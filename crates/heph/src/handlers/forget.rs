//! FORGET message handler — spec section 3.6.

use crate::db::Db;
use crate::db::aggregates::{
    delete_aggregate, delete_aggregate_element, get_aggregate_elements, rebuild_aggregate,
};
use crate::db::files::{count_active_pins, delete_file_pin_by_message, insert_grace_period_pin};
use crate::db::messages::{get_message_by_hash, insert_forgotten, update_message_status};
use crate::db::posts::{delete_post, get_amends_for_post, get_post, refresh_latest_amend};
use crate::db::vms::{delete_account_costs, delete_vm, delete_vm_volumes};
use crate::handlers::{IncomingMessage, ProcessingError, ProcessingResult};
use aleph_types::message::MessageContent;
use aleph_types::message::MessageContentEnum;
use aleph_types::message::MessageStatus;
use std::time::{SystemTime, UNIX_EPOCH};

/// Grace period for unpinned files: 7 days in seconds.
const GRACE_PERIOD_SECS: f64 = 7.0 * 24.0 * 3600.0;

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Process a FORGET message: mark listed items as forgotten and cascade.
pub fn process_forget(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
) -> ProcessingResult<()> {
    let forget_content = match &content.content {
        MessageContentEnum::Forget(f) => f,
        _ => {
            return Err(ProcessingError::InternalError(
                "process_forget called with non-FORGET content".into(),
            ));
        }
    };

    let forget_hash = msg.item_hash.to_string();
    let reason = forget_content.reason().map(|s| s.to_string());

    for target_hash in forget_content.hashes() {
        let target_hash_str = target_hash.to_string();
        process_single_forget(
            db,
            msg,
            content,
            &forget_hash,
            &target_hash_str,
            reason.as_deref(),
        )?;
    }

    Ok(())
}

/// Forget a single target message hash.
fn process_single_forget(
    db: &Db,
    msg: &IncomingMessage,
    forget_content: &MessageContent,
    forget_hash: &str,
    target_hash: &str,
    reason: Option<&str>,
) -> ProcessingResult<()> {
    // Step 1: Validate target exists.
    let target = db
        .with_conn(|conn| get_message_by_hash(conn, target_hash))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?
        .ok_or_else(|| {
            ProcessingError::ForgetTargetNotFound(format!("target {target_hash} not found"))
        })?;

    // Cannot forget a FORGET message.
    if target.message_type.to_uppercase() == "FORGET" {
        return Err(ProcessingError::ForgetForget(format!(
            "cannot forget a FORGET message: {target_hash}"
        )));
    }

    // If already forgotten — idempotent: just record and continue.
    if target.status == "forgotten" {
        db.with_conn(|conn| insert_forgotten(conn, target_hash, forget_hash, reason))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
        return Ok(());
    }

    // Target must be "processed" or "removing".
    if target.status != "processed" && target.status != "removing" {
        return Err(ProcessingError::ForgetTargetNotFound(format!(
            "target {target_hash} has unexpected status: {}",
            target.status
        )));
    }

    // Step 2: Ownership check.
    let forget_address = forget_content.address.as_str();
    let target_owner = target.owner.as_deref().unwrap_or("");
    if forget_address.to_lowercase() != target_owner.to_lowercase() {
        return Err(ProcessingError::ForgetNotAllowed(format!(
            "forget address {} does not match target owner {}",
            forget_address, target_owner
        )));
    }

    // Step 3: VM dependency check for STORE targets.
    if target.message_type.to_uppercase() == "STORE" {
        let dep_count = db
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM vm_volumes v
                     JOIN vms m ON v.vm_hash = m.item_hash
                     WHERE v.ref_hash = ?1",
                    rusqlite::params![target_hash],
                    |row| row.get::<_, i64>(0),
                )
            })
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        if dep_count > 0 {
            return Err(ProcessingError::ForgetNotAllowed(format!(
                "target STORE {target_hash} is referenced by {dep_count} active VM(s)"
            )));
        }
    }

    // Step 4: Insert into forgotten_messages.
    db.with_conn(|conn| insert_forgotten(conn, target_hash, forget_hash, reason))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Step 5: Cascade by target type.
    let target_type = target.message_type.to_uppercase();
    match target_type.as_str() {
        "POST" => forget_post(db, msg, &target, target_hash, forget_hash, reason)?,
        "AGGREGATE" => forget_aggregate(db, &target, target_hash)?,
        "STORE" => forget_store(db, &target, target_hash)?,
        "PROGRAM" | "INSTANCE" => forget_vm(db, target_hash)?,
        _ => {}
    }

    // Step 6: Update message status to "forgotten".
    db.with_conn(|conn| update_message_status(conn, target_hash, MessageStatus::Forgotten))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    Ok(())
}

/// Cascade for POST messages.
fn forget_post(
    db: &Db,
    _msg: &IncomingMessage,
    target: &crate::db::messages::StoredMessage,
    target_hash: &str,
    forget_hash: &str,
    reason: Option<&str>,
) -> ProcessingResult<()> {
    // Determine if this is an original or an amend.
    let post = db
        .with_conn(|conn| get_post(conn, target_hash))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    if let Some(post_record) = post {
        if post_record.post_type == "amend" {
            // Amend: delete from posts, refresh latest_amend on original.
            db.with_conn(|conn| delete_post(conn, target_hash))
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

            if let Some(orig_hash) = &post_record.original_item_hash {
                db.with_conn(|conn| refresh_latest_amend(conn, orig_hash))
                    .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
            }
        } else {
            // Original post: find all amends, mark them as forgotten, delete from posts.
            let amend_hashes = db
                .with_conn(|conn| get_amends_for_post(conn, target_hash))
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

            for amend_hash in &amend_hashes {
                // Insert into forgotten_messages.
                db.with_conn(|conn| insert_forgotten(conn, amend_hash, forget_hash, reason))
                    .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
                // Delete from posts.
                db.with_conn(|conn| delete_post(conn, amend_hash))
                    .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
                // Update status to forgotten.
                db.with_conn(|conn| {
                    update_message_status(conn, amend_hash, MessageStatus::Forgotten)
                })
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
            }

            // Delete the original from posts.
            db.with_conn(|conn| delete_post(conn, target_hash))
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
        }
    }

    let _ = target; // suppress unused warning
    Ok(())
}

/// Cascade for AGGREGATE messages.
fn forget_aggregate(
    db: &Db,
    target: &crate::db::messages::StoredMessage,
    target_hash: &str,
) -> ProcessingResult<()> {
    // Get the address and key from the target's denormalized fields.
    let address = target.owner.as_deref().unwrap_or("");
    let key = target.content_key.as_deref().unwrap_or("");

    // Delete this element from aggregate_elements.
    db.with_conn(|conn| delete_aggregate_element(conn, target_hash))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Check if any elements remain.
    let remaining = db
        .with_conn(|conn| get_aggregate_elements(conn, address, key))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    if remaining.is_empty() {
        // Delete the aggregate summary row.
        db.with_conn(|conn| delete_aggregate(conn, address, key))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    } else {
        // Rebuild the aggregate from remaining elements.
        db.with_conn(|conn| rebuild_aggregate(conn, address, key))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    }

    Ok(())
}

/// Cascade for STORE messages.
fn forget_store(
    db: &Db,
    target: &crate::db::messages::StoredMessage,
    target_hash: &str,
) -> ProcessingResult<()> {
    let file_hash = target.content_item_hash.as_deref().unwrap_or("");

    // Delete the file pin for this message.
    db.with_conn(|conn| delete_file_pin_by_message(conn, target_hash))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // If no active pins remain, insert a grace-period pin.
    if !file_hash.is_empty() {
        let active_pins = db
            .with_conn(|conn| count_active_pins(conn, file_hash))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        if active_pins == 0 {
            let delete_by = now_secs() + GRACE_PERIOD_SECS;
            let owner = target.owner.as_deref().unwrap_or("");
            db.with_conn(|conn| insert_grace_period_pin(conn, file_hash, owner, delete_by))
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
        }
    }

    Ok(())
}

/// Cascade for PROGRAM/INSTANCE messages.
fn forget_vm(db: &Db, target_hash: &str) -> ProcessingResult<()> {
    // Delete vm_volumes first (FK dependency).
    db.with_conn(|conn| delete_vm_volumes(conn, target_hash))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Delete the VM record.
    db.with_conn(|conn| delete_vm(conn, target_hash))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Delete account costs.
    db.with_conn(|conn| delete_account_costs(conn, target_hash))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::Db;
    use crate::db::aggregates::get_aggregate;
    use crate::db::files::{count_active_pins, upsert_file};
    use crate::db::messages::get_message_status;
    use crate::db::posts::get_post;
    use crate::handlers::IncomingMessage;
    use crate::handlers::process_message;
    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::chain::Chain;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;

    fn addr_for_key(key: &[u8; 32]) -> String {
        EvmAccount::new(Chain::Ethereum, key)
            .unwrap()
            .address()
            .as_str()
            .to_string()
    }

    fn sign_inline(key: &[u8; 32], msg_type: MessageType, item_content: String) -> IncomingMessage {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: msg_type,
            item_type: ItemType::Inline,
            item_content: item_content.clone(),
            item_hash: item_hash.clone(),
            time: Timestamp::from(1_000.0),
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

    fn make_post_msg(key: &[u8; 32], time: f64) -> IncomingMessage {
        let addr = addr_for_key(key);
        let ic = format!(
            r#"{{"type":"test","address":"{}","time":{},"content":{{"body":"Hello"}}}}"#,
            addr, time
        );
        sign_inline(key, MessageType::Post, ic)
    }

    fn make_amend_msg(key: &[u8; 32], time: f64, ref_hash: &str) -> IncomingMessage {
        let addr = addr_for_key(key);
        let ic = format!(
            r#"{{"ref":"{}","address":"{}","time":{},"content":{{"body":"Amended"}}}}"#,
            ref_hash, addr, time
        );
        sign_inline(key, MessageType::Post, ic)
    }

    fn make_forget_msg(key: &[u8; 32], time: f64, target_hash: &str) -> IncomingMessage {
        let addr = addr_for_key(key);
        let ic = format!(
            r#"{{"hashes":["{}"],"address":"{}","time":{}}}"#,
            target_hash, addr, time
        );
        sign_inline(key, MessageType::Forget, ic)
    }

    fn make_aggregate_msg(
        key: &[u8; 32],
        agg_key: &str,
        content_json: &str,
        time: f64,
    ) -> IncomingMessage {
        let addr = addr_for_key(key);
        let ic = format!(
            r#"{{"key":"{}","address":"{}","time":{},"content":{}}}"#,
            agg_key, addr, time, content_json
        );
        sign_inline(key, MessageType::Aggregate, ic)
    }

    fn make_store_msg(key: &[u8; 32], file_hash: &str, time: f64) -> IncomingMessage {
        let addr = addr_for_key(key);
        let ic = format!(
            r#"{{"address":"{}","time":{},"item_type":"storage","item_hash":"{}"}}"#,
            addr, time, file_hash
        );
        sign_inline(key, MessageType::Store, ic)
    }

    // -----------------------------------------------------------------------
    // Test 1: Forget a POST → forgotten, gone from posts
    // -----------------------------------------------------------------------

    #[test]
    fn test_forget_post() {
        let key = [90u8; 32];
        let db = Db::open_in_memory().unwrap();

        let post_msg = make_post_msg(&key, 1_000.0);
        let post_hash = post_msg.item_hash.to_string();
        process_message(&db, &post_msg).expect("post should process");

        let forget_msg = make_forget_msg(&key, 1_001.0, &post_hash);
        process_message(&db, &forget_msg).expect("forget should process");

        // Post status should be "forgotten".
        let status = db
            .with_conn(|conn| get_message_status(conn, &post_hash))
            .unwrap();
        assert_eq!(status, Some("forgotten".to_string()));

        // Post should be gone from posts table.
        let post = db.with_conn(|conn| get_post(conn, &post_hash)).unwrap();
        assert!(post.is_none(), "post should be deleted from posts table");
    }

    // -----------------------------------------------------------------------
    // Test 2: Forget a POST original with amends → amends also forgotten
    // -----------------------------------------------------------------------

    #[test]
    fn test_forget_post_with_amends() {
        let key = [91u8; 32];
        let db = Db::open_in_memory().unwrap();

        let orig_msg = make_post_msg(&key, 1_000.0);
        let orig_hash = orig_msg.item_hash.to_string();
        process_message(&db, &orig_msg).expect("orig");

        let amend_msg = make_amend_msg(&key, 1_001.0, &orig_hash);
        let amend_hash = amend_msg.item_hash.to_string();
        process_message(&db, &amend_msg).expect("amend");

        let forget_msg = make_forget_msg(&key, 1_002.0, &orig_hash);
        process_message(&db, &forget_msg).expect("forget orig");

        // Both original and amend should be forgotten.
        let orig_status = db
            .with_conn(|conn| get_message_status(conn, &orig_hash))
            .unwrap();
        let amend_status = db
            .with_conn(|conn| get_message_status(conn, &amend_hash))
            .unwrap();
        assert_eq!(orig_status, Some("forgotten".to_string()));
        assert_eq!(amend_status, Some("forgotten".to_string()));
    }

    // -----------------------------------------------------------------------
    // Test 3: Forget an AGGREGATE element → element removed, aggregate rebuilt
    // -----------------------------------------------------------------------

    #[test]
    fn test_forget_aggregate_element() {
        let key = [92u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        let agg1 = make_aggregate_msg(&key, "data", r#"{"a":1,"b":2}"#, 1_000.0);
        let agg1_hash = agg1.item_hash.to_string();
        process_message(&db, &agg1).expect("agg1");

        let agg2 = make_aggregate_msg(&key, "data", r#"{"c":3}"#, 1_001.0);
        process_message(&db, &agg2).expect("agg2");

        // Forget agg1 — aggregate should rebuild with only agg2's content.
        let forget_msg = make_forget_msg(&key, 1_002.0, &agg1_hash);
        process_message(&db, &forget_msg).expect("forget agg1");

        let agg_record = db
            .with_conn(|conn| get_aggregate(conn, &addr, "data"))
            .unwrap()
            .expect("aggregate should still exist");

        let content: serde_json::Value = serde_json::from_str(&agg_record.content).unwrap();
        // After removing agg1, only agg2's content remains: {c:3}
        // a and b were from agg1 only, so they may or may not be present depending on
        // whether they were overwritten. Since agg1 was removed and agg2 had {c:3},
        // rebuilt aggregate should only have {c:3}.
        assert_eq!(content["c"], 3, "c should remain after rebuild");
        // a and b came only from agg1, so after forgetting agg1 they should be gone.
        assert!(
            content.get("a").is_none() || content["a"].is_null(),
            "a from forgotten agg1 should be gone, content: {:?}",
            content
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: Forget a STORE → pin removed, grace period inserted
    // -----------------------------------------------------------------------

    #[test]
    fn test_forget_store() {
        let key = [93u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        let file_hash = "a".repeat(64);

        // Upsert the file record first (store handler would normally do this).
        db.with_conn(|conn| upsert_file(conn, &file_hash, 1024, "file"))
            .unwrap();

        let store_msg = make_store_msg(&key, &file_hash, 1_000.0);
        let store_hash = store_msg.item_hash.to_string();
        process_message(&db, &store_msg).expect("store should process");

        // Verify pin exists.
        let pins_before = db
            .with_conn(|conn| count_active_pins(conn, &file_hash))
            .unwrap();
        assert_eq!(pins_before, 1, "should have 1 active pin before forget");

        let forget_msg = make_forget_msg(&key, 1_001.0, &store_hash);
        process_message(&db, &forget_msg).expect("forget should process");

        // Active pin should be gone.
        let pins_after = db
            .with_conn(|conn| count_active_pins(conn, &file_hash))
            .unwrap();
        assert_eq!(pins_after, 0, "active pin should be removed");

        // Grace period pin should exist.
        let total_pins: i64 = db.with_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM file_pins WHERE file_hash = ?1",
                rusqlite::params![&file_hash],
                |row| row.get(0),
            )
            .unwrap()
        });
        assert_eq!(total_pins, 1, "grace period pin should be inserted");

        let _ = (addr, store_hash);
    }

    // -----------------------------------------------------------------------
    // Test 5: Cannot forget a FORGET message → ForgetForget error
    // -----------------------------------------------------------------------

    #[test]
    fn test_cannot_forget_forget() {
        let key = [94u8; 32];
        let db = Db::open_in_memory().unwrap();

        // Create something to forget.
        let post_msg = make_post_msg(&key, 1_000.0);
        let post_hash = post_msg.item_hash.to_string();
        process_message(&db, &post_msg).expect("post");

        // First forget.
        let forget1 = make_forget_msg(&key, 1_001.0, &post_hash);
        let forget1_hash = forget1.item_hash.to_string();
        process_message(&db, &forget1).expect("first forget");

        // Try to forget the FORGET message itself.
        let forget2 = make_forget_msg(&key, 1_002.0, &forget1_hash);
        let result = process_message(&db, &forget2);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().error_code(),
            502,
            "expected ForgetForget (502)"
        );
    }

    // -----------------------------------------------------------------------
    // Test 6: Cannot forget message owned by different address → ForgetNotAllowed
    // -----------------------------------------------------------------------

    #[test]
    fn test_cannot_forget_other_owner() {
        let owner_key = [95u8; 32];
        let attacker_key = [96u8; 32];
        let db = Db::open_in_memory().unwrap();

        // Owner submits a post.
        let post_msg = make_post_msg(&owner_key, 1_000.0);
        let post_hash = post_msg.item_hash.to_string();
        process_message(&db, &post_msg).expect("post");

        // Attacker tries to forget owner's post.
        let forget_msg = make_forget_msg(&attacker_key, 1_001.0, &post_hash);
        let result = process_message(&db, &forget_msg);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().error_code(),
            503,
            "expected ForgetNotAllowed (503)"
        );
    }
}
