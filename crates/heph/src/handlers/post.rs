use crate::db::Db;
use crate::db::posts::{PostRecord, get_post, insert_post, update_latest_amend};
use crate::handlers::{IncomingMessage, ProcessingError, ProcessingResult};
use aleph_types::message::MessageContent;

/// Process a POST message: insert into the posts table, handling amend logic.
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

    // Serialize post content body to JSON.
    let content_json = post_content
        .content
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    if post_content.is_amend() {
        let ref_hash = post_content.reference.as_deref().unwrap_or("").to_string();

        if ref_hash.is_empty() {
            return Err(ProcessingError::PostAmendNoTarget(
                "amend message has an empty ref".into(),
            ));
        }

        // Look up the target post.
        let target = db
            .with_conn(|conn| get_post(conn, &ref_hash))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?
            .ok_or_else(|| {
                ProcessingError::PostAmendTargetNotFound(format!(
                    "target post {ref_hash} not found"
                ))
            })?;

        // Target must not itself be an amend.
        if target.post_type == "amend" {
            return Err(ProcessingError::PostAmendAmend(format!(
                "target post {ref_hash} is itself an amend"
            )));
        }

        // Address must match.
        if target.address != address {
            return Err(ProcessingError::PermissionDenied(format!(
                "amend address {address} does not match original post address {}",
                target.address
            )));
        }

        // The original is the target itself (target.original_item_hash is None for non-amends).
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

        // Update latest_amend on the original if this amend is newer.
        db.with_conn(|conn| update_latest_amend(conn, &original_hash, &item_hash, time))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    } else {
        // Regular (non-amend) post.
        let post_type = post_content.post_type_str().to_string();
        let record = PostRecord {
            item_hash,
            address,
            post_type,
            ref_: None,
            content: content_json,
            channel,
            time,
            original_item_hash: None,
            latest_amend: None,
        };

        db.with_conn(|conn| insert_post(conn, &record))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
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
}
