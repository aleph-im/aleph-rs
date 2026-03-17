use crate::db::Db;
use crate::db::aggregates::{
    count_aggregate_elements, get_aggregate, get_aggregate_elements, insert_aggregate_element,
    mark_aggregate_dirty, update_aggregate, upsert_aggregate,
};
use crate::handlers::{IncomingMessage, ProcessingError, ProcessingResult};
use aleph_types::message::MessageContent;

/// Threshold above which we mark dirty instead of rebuilding.
const DIRTY_THRESHOLD: i64 = 1000;

/// Shallow-merge two JSON objects: patch keys overwrite base keys at the top level only.
fn shallow_merge(base: &serde_json::Value, patch: &serde_json::Value) -> serde_json::Value {
    let mut result = base.as_object().cloned().unwrap_or_default();
    if let Some(patch_obj) = patch.as_object() {
        for (k, v) in patch_obj {
            result.insert(k.clone(), v.clone());
        }
    }
    serde_json::Value::Object(result)
}

/// Rebuild the merged aggregate from all elements ordered by time ASC, item_hash ASC.
fn rebuild_from_elements(
    elements: &[crate::db::aggregates::AggregateElementRecord],
) -> serde_json::Value {
    let mut merged = serde_json::Value::Object(serde_json::Map::new());
    for elem in elements {
        if let Ok(patch) = serde_json::from_str::<serde_json::Value>(&elem.content) {
            merged = shallow_merge(&merged, &patch);
        }
    }
    merged
}

/// Process an AGGREGATE message: insert element and update merged state.
pub fn process_aggregate(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
) -> ProcessingResult<()> {
    let agg_content = match &content.content {
        aleph_types::message::MessageContentEnum::Aggregate(a) => a,
        _ => {
            return Err(ProcessingError::InternalError(
                "process_aggregate called with non-AGGREGATE content".into(),
            ));
        }
    };

    let item_hash = msg.item_hash.to_string();
    let address = content.address.as_str().to_string();
    let key = agg_content.key().to_string();
    let time = content.time.as_f64();

    // Serialize this element's content map to JSON.
    let elem_content_json = serde_json::to_string(&agg_content.content)
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Step 1: Insert the element (idempotent).
    db.with_conn(|conn| {
        insert_aggregate_element(conn, &item_hash, &address, &key, &elem_content_json, time)
    })
    .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Step 2: Try to insert a new aggregate row.
    let inserted = db
        .with_conn(|conn| {
            upsert_aggregate(
                conn,
                &address,
                &key,
                &elem_content_json,
                time,
                Some(&item_hash),
            )
        })
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    if inserted {
        // New aggregate — nothing more to do.
        return Ok(());
    }

    // Existing aggregate — fetch current state.
    let existing = db
        .with_conn(|conn| get_aggregate(conn, &address, &key))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?
        .ok_or_else(|| {
            ProcessingError::InternalError("aggregate disappeared after upsert".into())
        })?;

    // If dirty, skip update (will be rebuilt on next read).
    if existing.dirty {
        return Ok(());
    }

    if time >= existing.time {
        // In-order or same-time: shallow merge new element onto existing.
        let existing_val: serde_json::Value = serde_json::from_str(&existing.content)
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
        let patch_val: serde_json::Value = serde_json::from_str(&elem_content_json)
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
        let merged = shallow_merge(&existing_val, &patch_val);
        let merged_json = serde_json::to_string(&merged)
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        db.with_conn(|conn| {
            update_aggregate(
                conn,
                &address,
                &key,
                &merged_json,
                time,
                Some(&item_hash),
                false,
            )
        })
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    } else {
        // Out-of-order: element has an earlier timestamp than current state.
        // Check element count to decide between rebuild and dirty.
        let count = db
            .with_conn(|conn| count_aggregate_elements(conn, &address, &key))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        if count > DIRTY_THRESHOLD {
            db.with_conn(|conn| mark_aggregate_dirty(conn, &address, &key))
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
        } else {
            // Full rebuild from all elements.
            let elements = db
                .with_conn(|conn| get_aggregate_elements(conn, &address, &key))
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
            let rebuilt = rebuild_from_elements(&elements);
            let rebuilt_json = serde_json::to_string(&rebuilt)
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

            // The new time should reflect the latest element's time.
            let latest_time = elements
                .iter()
                .map(|e| e.time)
                .fold(f64::NEG_INFINITY, f64::max);
            let last_hash = elements
                .last()
                .map(|e| e.item_hash.as_str())
                .unwrap_or(&item_hash);

            db.with_conn(|conn| {
                update_aggregate(
                    conn,
                    &address,
                    &key,
                    &rebuilt_json,
                    latest_time,
                    Some(last_hash),
                    false,
                )
            })
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::Db;
    use crate::db::aggregates::get_aggregate;
    use crate::handlers::IncomingMessage;
    use crate::handlers::process_message;
    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::chain::Chain;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;

    fn sign_aggregate(
        key: &[u8; 32],
        agg_key: &str,
        content_json: &str,
        time: f64,
    ) -> IncomingMessage {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let addr = account.address().as_str().to_string();
        let ic = format!(
            r#"{{"key":"{}","address":"{}","time":{},"content":{}}}"#,
            agg_key, addr, time, content_json
        );
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(ic.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: MessageType::Aggregate,
            item_type: ItemType::Inline,
            item_content: ic.clone(),
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

    fn get_content(db: &Db, addr: &str, key: &str) -> serde_json::Value {
        let agg = db
            .with_conn(|conn| get_aggregate(conn, addr, key))
            .unwrap()
            .expect("aggregate should exist");
        serde_json::from_str(&agg.content).unwrap()
    }

    fn addr_for_key(key: &[u8; 32]) -> String {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        account.address().as_str().to_string()
    }

    // -----------------------------------------------------------------------
    // Test: first aggregate creates entry
    // -----------------------------------------------------------------------

    #[test]
    fn test_first_aggregate_creates_entry() {
        let key = [20u8; 32];
        let db = Db::open_in_memory().unwrap();
        let addr = addr_for_key(&key);

        let msg = sign_aggregate(&key, "profile", r#"{"name":"Alice"}"#, 1000.0);
        process_message(&db, &msg).expect("should process");

        let content = get_content(&db, &addr, "profile");
        assert_eq!(content["name"], "Alice");
    }

    // -----------------------------------------------------------------------
    // Test: second with newer time — shallow merge
    // -----------------------------------------------------------------------

    #[test]
    fn test_second_aggregate_shallow_merge() {
        let key = [21u8; 32];
        let db = Db::open_in_memory().unwrap();
        let addr = addr_for_key(&key);

        let msg1 = sign_aggregate(
            &key,
            "profile",
            r#"{"name":"Alice","city":"Paris"}"#,
            1000.0,
        );
        process_message(&db, &msg1).expect("msg1");

        let msg2 = sign_aggregate(&key, "profile", r#"{"name":"Bob","age":30}"#, 1001.0);
        process_message(&db, &msg2).expect("msg2");

        let content = get_content(&db, &addr, "profile");
        // "name" should be overwritten to Bob, "city" should be preserved, "age" added.
        assert_eq!(content["name"], "Bob");
        assert_eq!(content["city"], "Paris");
        assert_eq!(content["age"], 30);
    }

    // -----------------------------------------------------------------------
    // Test: out-of-order element triggers rebuild
    // -----------------------------------------------------------------------

    #[test]
    fn test_out_of_order_triggers_rebuild() {
        let key = [22u8; 32];
        let db = Db::open_in_memory().unwrap();
        let addr = addr_for_key(&key);

        let msg1 = sign_aggregate(&key, "data", r#"{"a":1}"#, 1000.0);
        process_message(&db, &msg1).expect("msg1");

        let msg2 = sign_aggregate(&key, "data", r#"{"a":2,"b":2}"#, 1002.0);
        process_message(&db, &msg2).expect("msg2");

        // msg3 has time 1001 (between msg1 and msg2) — triggers rebuild.
        let msg3 = sign_aggregate(&key, "data", r#"{"a":3,"c":3}"#, 1001.0);
        process_message(&db, &msg3).expect("msg3");

        // After rebuild, elements in order: msg1(t=1000), msg3(t=1001), msg2(t=1002).
        // Merge: {a:1} -> {a:3,c:3} -> {a:2,b:2,c:3}
        let content = get_content(&db, &addr, "data");
        assert_eq!(content["a"], 2);
        assert_eq!(content["b"], 2);
        assert_eq!(content["c"], 3);
    }

    // -----------------------------------------------------------------------
    // Test: multiple elements for same key merge correctly
    // -----------------------------------------------------------------------

    #[test]
    fn test_multiple_elements_merge() {
        let key = [23u8; 32];
        let db = Db::open_in_memory().unwrap();
        let addr = addr_for_key(&key);

        let msg1 = sign_aggregate(&key, "cfg", r#"{"a":1}"#, 1000.0);
        let msg2 = sign_aggregate(&key, "cfg", r#"{"b":2}"#, 1001.0);
        let msg3 = sign_aggregate(&key, "cfg", r#"{"c":3}"#, 1002.0);
        let msg4 = sign_aggregate(&key, "cfg", r#"{"a":10}"#, 1003.0);

        for msg in &[msg1, msg2, msg3, msg4] {
            process_message(&db, msg).expect("should process");
        }

        let content = get_content(&db, &addr, "cfg");
        assert_eq!(content["a"], 10); // overwritten
        assert_eq!(content["b"], 2);
        assert_eq!(content["c"], 3);
    }
}
