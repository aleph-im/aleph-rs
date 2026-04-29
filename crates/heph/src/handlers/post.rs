use crate::db::Db;
use crate::db::posts::{PostRecord, get_post, insert_post, update_latest_amend};
use crate::handlers::credit_transfer;
use crate::handlers::{IncomingMessage, ProcessingError, ProcessingResult};
use aleph_sdk::credit_transfer::CREDIT_TRANSFER_POST_TYPE;
use aleph_types::message::MessageContent;

/// Process a POST message: insert into the posts table, handling amend logic.
/// For `aleph_credit_transfer` posts, the post insert and the credit apply
/// run in a single SQL transaction so partial state is impossible.
pub fn process_post(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
) -> ProcessingResult<()> {
    let post_content = match &content.content {
        aleph_types::message::MessageContentEnum::Post(p) => p,
        _ => {
            return Err(ProcessingError::InternalError(
                "process_post called with non-POST content".into(),
            ));
        }
    };

    let item_hash = msg.item_hash.to_string();
    let address = content.address.as_str().to_string();
    let time = content.time.as_f64();
    let channel = msg.channel.as_ref().and_then(|c| {
        serde_json::to_value(c)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    });

    let content_json = post_content
        .content
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    if post_content.is_amend() {
        // Amend path — unchanged from the previous implementation.
        let ref_hash = post_content.reference.as_deref().unwrap_or("").to_string();

        if ref_hash.is_empty() {
            return Err(ProcessingError::PostAmendNoTarget(
                "amend message has an empty ref".into(),
            ));
        }

        let target = db
            .with_conn(|conn| get_post(conn, &ref_hash))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?
            .ok_or_else(|| {
                ProcessingError::PostAmendTargetNotFound(format!(
                    "target post {ref_hash} not found"
                ))
            })?;

        if target.post_type == "amend" {
            return Err(ProcessingError::PostAmendAmend(format!(
                "target post {ref_hash} is itself an amend"
            )));
        }

        if target.address != address {
            return Err(ProcessingError::PermissionDenied(format!(
                "amend address {address} does not match original post address {}",
                target.address
            )));
        }

        let original_hash = ref_hash.clone();

        let record = PostRecord {
            item_hash: item_hash.clone(),
            address,
            post_type: "amend".to_string(),
            ref_: Some(ref_hash.clone()),
            content: content_json,
            channel,
            time,
            original_item_hash: Some(original_hash.clone()),
            latest_amend: None,
        };

        db.with_conn(|conn| insert_post(conn, &record))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        db.with_conn(|conn| update_latest_amend(conn, &original_hash, &item_hash, time))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        return Ok(());
    }

    // Regular (non-amend) post.
    let post_type = post_content.post_type_str().to_string();
    let record = PostRecord {
        item_hash: item_hash.clone(),
        address: address.clone(),
        post_type: post_type.clone(),
        ref_: None,
        content: content_json,
        channel,
        time,
        original_item_hash: None,
        latest_amend: None,
    };

    if post_type == CREDIT_TRANSFER_POST_TYPE {
        // Credit-transfer post: insert the post row AND apply the transfer in
        // a single transaction. Post insert first, so the recipient/sender
        // history rows can reference an existing post item_hash if any future
        // FK is added; the transaction keeps both halves atomic.
        let raw_credit_content = post_content
            .content
            .clone()
            .unwrap_or(serde_json::Value::Null);
        let sender = address.clone();
        let item_hash_for_apply = item_hash.clone();
        db.with_conn(|conn| -> ProcessingResult<()> {
            // `with_conn` hands us `&Connection`; `unchecked_transaction` is
            // the rusqlite-sanctioned way to start a transaction from an
            // immutable connection ref (vs. `transaction()` which needs
            // `&mut Connection`).
            let tx = conn
                .unchecked_transaction()
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
            insert_post(&tx, &record).map_err(|e| ProcessingError::InternalError(e.to_string()))?;
            credit_transfer::process_in_tx(&tx, &sender, &item_hash_for_apply, raw_credit_content)?;
            tx.commit()
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
            Ok(())
        })?;
    } else {
        db.with_conn(|conn| insert_post(conn, &record))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::db::balances::{get_credit_balance, set_credit_balance};
    use crate::db::posts::get_post;
    use crate::handlers::process_message;
    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::chain::Chain;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;

    fn make_post_content_str(addr: &str, time: f64) -> String {
        format!(
            r#"{{"type":"test","address":"{}","time":{},"content":{{"body":"Hello"}}}}"#,
            addr, time
        )
    }

    fn make_amend_content_str(addr: &str, time: f64, ref_hash: &str) -> String {
        format!(
            r#"{{"type":"amend","ref":"{}","address":"{}","time":{},"content":{{"body":"Amended"}}}}"#,
            ref_hash, addr, time
        )
    }

    fn sign_inline_message(
        key: &[u8; 32],
        msg_type: MessageType,
        item_content: String,
    ) -> IncomingMessage {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: msg_type,
            item_type: ItemType::Inline,
            item_content: item_content.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(0.0),
            channel: None,
        };
        let pending = sign_message(&account, unsigned).unwrap();
        IncomingMessage {
            chain: pending.chain,
            sender: pending.sender,
            signature: pending.signature,
            message_type: pending.message_type,
            item_type: pending.item_type,
            item_content: Some(pending.item_content),
            item_hash: pending.item_hash,
            time: pending.time,
            channel: pending.channel,
        }
    }

    /// Sign a POST message with explicit time embedded in content.
    fn sign_post(key: &[u8; 32], time: f64) -> IncomingMessage {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let addr = account.address().as_str().to_string();
        let ic = make_post_content_str(&addr, time);
        sign_inline_message(key, MessageType::Post, ic)
    }

    /// Sign an amend POST pointing to ref_hash.
    fn sign_amend(key: &[u8; 32], time: f64, ref_hash: &str) -> IncomingMessage {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let addr = account.address().as_str().to_string();
        let ic = make_amend_content_str(&addr, time, ref_hash);
        sign_inline_message(key, MessageType::Post, ic)
    }

    // -----------------------------------------------------------------------
    // Test: original post stored
    // -----------------------------------------------------------------------

    #[test]
    fn test_original_post_stored() {
        let key = [10u8; 32];
        let db = Db::open_in_memory().unwrap();
        let msg = sign_post(&key, 1_000.0);
        let hash = msg.item_hash.to_string();

        process_message(&db, &msg).expect("should process");

        let post = db
            .with_conn(|conn| get_post(conn, &hash))
            .unwrap()
            .expect("post should exist");
        assert_eq!(post.post_type, "test");
        assert!(post.latest_amend.is_none());
        assert!(post.original_item_hash.is_none());
    }

    // -----------------------------------------------------------------------
    // Test: amend updates latest_amend on original
    // -----------------------------------------------------------------------

    #[test]
    fn test_amend_updates_latest_amend() {
        let key = [11u8; 32];
        let db = Db::open_in_memory().unwrap();

        // Process the original post.
        let orig_msg = sign_post(&key, 1_000.0);
        let orig_hash = orig_msg.item_hash.to_string();
        process_message(&db, &orig_msg).expect("original should process");

        // Process an amend.
        let amend_msg = sign_amend(&key, 1_001.0, &orig_hash);
        let amend_hash = amend_msg.item_hash.to_string();
        process_message(&db, &amend_msg).expect("amend should process");

        // Check original's latest_amend.
        let orig = db
            .with_conn(|conn| get_post(conn, &orig_hash))
            .unwrap()
            .expect("original should exist");
        assert_eq!(orig.latest_amend, Some(amend_hash.clone()));

        // Check amend record.
        let amend = db
            .with_conn(|conn| get_post(conn, &amend_hash))
            .unwrap()
            .expect("amend should exist");
        assert_eq!(amend.post_type, "amend");
        assert_eq!(amend.original_item_hash, Some(orig_hash.clone()));
    }

    // -----------------------------------------------------------------------
    // Test: multiple amends — latest by timestamp wins
    // -----------------------------------------------------------------------

    #[test]
    fn test_multiple_amends_latest_wins() {
        let key = [12u8; 32];
        let db = Db::open_in_memory().unwrap();

        let orig_msg = sign_post(&key, 1_000.0);
        let orig_hash = orig_msg.item_hash.to_string();
        process_message(&db, &orig_msg).expect("original");

        let amend1_msg = sign_amend(&key, 1_001.0, &orig_hash);
        let amend1_hash = amend1_msg.item_hash.to_string();
        process_message(&db, &amend1_msg).expect("amend1");

        let amend2_msg = sign_amend(&key, 1_002.0, &orig_hash);
        let amend2_hash = amend2_msg.item_hash.to_string();
        process_message(&db, &amend2_msg).expect("amend2");

        // amend3 has an older timestamp — should not replace amend2.
        let amend3_msg = sign_amend(&key, 999.0, &orig_hash);
        process_message(&db, &amend3_msg).expect("amend3 (old)");

        let orig = db
            .with_conn(|conn| get_post(conn, &orig_hash))
            .unwrap()
            .unwrap();
        assert_eq!(
            orig.latest_amend,
            Some(amend2_hash),
            "amend2 should be latest"
        );
        let _ = amend1_hash; // suppress unused warning
    }

    // -----------------------------------------------------------------------
    // Test: amend pointing to non-existent post → PostAmendTargetNotFound
    // -----------------------------------------------------------------------

    #[test]
    fn test_amend_target_not_found() {
        let key = [13u8; 32];
        let db = Db::open_in_memory().unwrap();

        let amend_msg = sign_amend(&key, 1_001.0, "nonexistent_hash_abc");
        let result = process_message(&db, &amend_msg);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code(),
            101,
            "expected PostAmendTargetNotFound (101), got {:?}",
            err
        );
    }

    // -----------------------------------------------------------------------
    // Test: amend pointing to another amend → PostAmendAmend
    // -----------------------------------------------------------------------

    #[test]
    fn test_amend_of_amend() {
        let key = [14u8; 32];
        let db = Db::open_in_memory().unwrap();

        let orig_msg = sign_post(&key, 1_000.0);
        let orig_hash = orig_msg.item_hash.to_string();
        process_message(&db, &orig_msg).expect("original");

        let amend1_msg = sign_amend(&key, 1_001.0, &orig_hash);
        let amend1_hash = amend1_msg.item_hash.to_string();
        process_message(&db, &amend1_msg).expect("amend1");

        // Amend pointing to amend1 (not the original) should fail.
        let amend2_msg = sign_amend(&key, 1_002.0, &amend1_hash);
        let result = process_message(&db, &amend2_msg);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code(),
            102,
            "expected PostAmendAmend (102), got {:?}",
            err
        );
    }

    // -----------------------------------------------------------------------
    // Test: amend with different owner → PermissionDenied
    // -----------------------------------------------------------------------

    #[test]
    fn test_amend_different_owner_denied() {
        let key_a = [15u8; 32];
        let key_b = [16u8; 32];
        let db = Db::open_in_memory().unwrap();

        // key_a creates the original post.
        let orig_msg = sign_post(&key_a, 1_000.0);
        let orig_hash = orig_msg.item_hash.to_string();
        process_message(&db, &orig_msg).expect("original");

        // key_b tries to amend key_a's post.
        let amend_msg = sign_amend(&key_b, 1_001.0, &orig_hash);
        let result = process_message(&db, &amend_msg);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code(),
            2,
            "expected PermissionDenied (2), got {:?}",
            err
        );
    }

    // -----------------------------------------------------------------------
    // Credit-transfer dispatch tests
    // -----------------------------------------------------------------------

    /// Sign a credit-transfer POST inline message. The content embeds the sender's
    /// own address as `address` (per inline-message convention). The transfer entry
    /// targets `recipient`.
    fn sign_credit_transfer(
        key: &[u8; 32],
        time: f64,
        recipient: &str,
        amount: u64,
        expiration_unix: Option<i64>,
    ) -> IncomingMessage {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let sender = account.address().as_str().to_string();
        let exp_field = match expiration_unix {
            Some(e) => format!(r#","expiration":{e}"#),
            None => String::new(),
        };
        let item_content = format!(
            r#"{{"type":"aleph_credit_transfer","address":"{sender}","time":{time},"content":{{"transfer":{{"credits":[{{"address":"{recipient}","amount":{amount}{exp_field}}}]}}}}}}"#
        );
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: MessageType::Post,
            item_type: ItemType::Inline,
            item_content: item_content.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(time),
            channel: None,
        };
        let pending = sign_message(&account, unsigned).unwrap();
        IncomingMessage {
            chain: pending.chain,
            sender: pending.sender,
            signature: pending.signature,
            message_type: pending.message_type,
            item_type: pending.item_type,
            item_content: Some(pending.item_content),
            item_hash: pending.item_hash,
            time: pending.time,
            channel: pending.channel,
        }
    }

    #[test]
    fn credit_transfer_succeeds_and_updates_balances_and_history() {
        let db = Db::open_in_memory().unwrap();

        let sender_key = [40u8; 32];
        let sender_account = EvmAccount::new(Chain::Ethereum, &sender_key).unwrap();
        let sender = sender_account.address().as_str().to_string();
        db.with_conn(|c| set_credit_balance(c, &sender, 5_000))
            .unwrap();

        let recipient = "0x000000000000000000000000000000000000FACE";
        let msg = sign_credit_transfer(
            &sender_key,
            1_700_000_100.0,
            recipient,
            1_500,
            Some(1_798_761_599), // 2026-12-31T23:59:59Z
        );
        let item_hash = msg.item_hash.to_string();

        process_message(&db, &msg).expect("transfer should process");

        // Balances.
        assert_eq!(
            db.with_conn(|c| get_credit_balance(c, &sender)).unwrap(),
            Some(3_500)
        );
        assert_eq!(
            db.with_conn(|c| get_credit_balance(c, recipient)).unwrap(),
            Some(1_500)
        );

        // History rows: same message_hash on both legs.
        let count: i64 = db
            .with_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM credit_history WHERE message_hash = ?1",
                    [&item_hash],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn credit_transfer_with_insufficient_balance_rejects_and_rolls_back() {
        let db = Db::open_in_memory().unwrap();

        let sender_key = [41u8; 32];
        let sender_account = EvmAccount::new(Chain::Ethereum, &sender_key).unwrap();
        let sender = sender_account.address().as_str().to_string();
        db.with_conn(|c| set_credit_balance(c, &sender, 100))
            .unwrap();

        let recipient = "0x000000000000000000000000000000000000BEEF";
        let msg = sign_credit_transfer(&sender_key, 1_700_000_200.0, recipient, 200, None);
        let item_hash = msg.item_hash.to_string();

        let err = process_message(&db, &msg).unwrap_err();
        assert_eq!(err.error_code(), 6, "expected CreditInsufficient (6)");

        // Balances unchanged: sender still 100, recipient absent.
        assert_eq!(
            db.with_conn(|c| get_credit_balance(c, &sender)).unwrap(),
            Some(100)
        );
        assert_eq!(
            db.with_conn(|c| get_credit_balance(c, recipient)).unwrap(),
            None
        );

        // No post row, no history rows.
        let post_count: i64 = db
            .with_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM posts WHERE item_hash = ?1",
                    [&item_hash],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(post_count, 0);

        let history_count: i64 = db
            .with_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM credit_history WHERE message_hash = ?1",
                    [&item_hash],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(history_count, 0);
    }

    #[test]
    fn credit_transfer_self_transfer_rejected() {
        let db = Db::open_in_memory().unwrap();

        let sender_key = [42u8; 32];
        let sender_account = EvmAccount::new(Chain::Ethereum, &sender_key).unwrap();
        let sender = sender_account.address().as_str().to_string();
        db.with_conn(|c| set_credit_balance(c, &sender, 5_000))
            .unwrap();

        let msg = sign_credit_transfer(&sender_key, 1_700_000_300.0, &sender, 1, None);
        let err = process_message(&db, &msg).unwrap_err();
        assert_eq!(err.error_code(), 0);
        assert!(err.message().contains("sender and recipient must differ"));

        // Balance unchanged.
        assert_eq!(
            db.with_conn(|c| get_credit_balance(c, &sender)).unwrap(),
            Some(5_000)
        );
    }
}
