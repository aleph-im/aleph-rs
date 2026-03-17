use crate::db::Db;
use crate::db::files::{InsertFilePin, insert_file_pin, upsert_file, upsert_file_tag};
use crate::db::messages::get_message_by_hash;
use crate::handlers::{IncomingMessage, ProcessingError, ProcessingResult};
use aleph_types::message::MessageContent;
use aleph_types::message::MessageContentEnum;

/// Process a STORE message (spec section 3.3).
pub fn process_store(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
) -> ProcessingResult<()> {
    // Extract StoreContent.
    let store_content = match &content.content {
        MessageContentEnum::Store(s) => s,
        _ => {
            return Err(ProcessingError::InternalError(
                "process_store called with non-STORE content".into(),
            ));
        }
    };

    let file_hash = store_content.file_hash().to_string();
    let owner = content.address.as_str().to_string();
    let item_hash = msg.item_hash.to_string();
    let time = content.time.as_f64();

    // Step 3 — validate ref if present.
    let ref_str: Option<String> = if let Some(raw_ref) = &store_content.reference {
        let ref_value = raw_ref.to_string();

        if is_item_hash(&ref_value) {
            // Looks like a message hash — look up the target.
            let target = db
                .with_conn(|conn| get_message_by_hash(conn, &ref_value))
                .map_err(|e| ProcessingError::InternalError(e.to_string()))?
                .ok_or_else(|| {
                    ProcessingError::StoreRefNotFound(format!("ref {ref_value} not found"))
                })?;

            // Target must be a STORE message.
            if target.message_type.to_uppercase() != "STORE" {
                return Err(ProcessingError::StoreRefNotFound(format!(
                    "ref {ref_value} is not a STORE message (got {})",
                    target.message_type
                )));
            }

            // Target must not itself have a ref (no chains).
            if target.content_ref.is_some() {
                return Err(ProcessingError::StoreUpdateUpdate(format!(
                    "ref {ref_value} is itself a STORE update (ref-of-ref not allowed)"
                )));
            }

            Some(ref_value)
        } else {
            // User-defined string ref — allowed as-is.
            Some(ref_value)
        }
    } else {
        None
    };

    // Step 4 — upsert file record.
    let size_bytes = store_content.size.map(|s| s.count() as i64).unwrap_or(0);
    db.with_conn(|conn| upsert_file(conn, &file_hash, size_bytes, "file"))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Step 5 — insert file pin (type = "message").
    let size_opt = store_content.size.map(|s| s.count() as i64);
    db.with_conn(|conn| {
        insert_file_pin(
            conn,
            &InsertFilePin {
                file_hash: &file_hash,
                owner: &owner,
                pin_type: "message",
                message_hash: Some(&item_hash),
                size: size_opt,
                content_type: store_content.content_type.as_deref(),
                ref_: ref_str.as_deref(),
            },
        )
    })
    .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // Step 6 — create/update file tag.
    let owner_lower = owner.to_lowercase();
    let tag = if let Some(ref r) = ref_str {
        format!("{owner_lower}:{r}")
    } else {
        format!("{owner_lower}:{file_hash}")
    };

    db.with_conn(|conn| upsert_file_tag(conn, &tag, &owner_lower, &file_hash, time))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    Ok(())
}

/// Return `true` if `s` looks like a 64-character lowercase hex string (item hash).
fn is_item_hash(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::db::files::{count_active_pins, get_file, get_file_tag};
    use crate::handlers::process_message;
    use aleph_types::account::{Account, EvmAccount, sign_message};
    use aleph_types::chain::Chain;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::message::unsigned::UnsignedMessage;
    use aleph_types::timestamp::Timestamp;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn sign_store_message(key: &[u8; 32], time: f64, item_content: String) -> IncomingMessage {
        let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
        let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
        let unsigned = UnsignedMessage {
            message_type: MessageType::Store,
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

    fn make_store_content(addr: &str, time: f64, file_hash: &str) -> String {
        format!(
            r#"{{"address":"{}","time":{},"item_type":"storage","item_hash":"{}"}}"#,
            addr, time, file_hash
        )
    }

    fn make_store_content_with_ref(addr: &str, time: f64, file_hash: &str, ref_: &str) -> String {
        format!(
            r#"{{"address":"{}","time":{},"item_type":"storage","item_hash":"{}","ref":"{}"}}"#,
            addr, time, file_hash, ref_
        )
    }

    /// A fake 64-char hex "file hash".
    fn fake_file_hash(n: u8) -> String {
        format!("{:0>64}", format!("{:x}", n))
    }

    fn addr_for_key(key: &[u8; 32]) -> String {
        EvmAccount::new(Chain::Ethereum, key)
            .unwrap()
            .address()
            .as_str()
            .to_string()
    }

    // -----------------------------------------------------------------------
    // Test 1: Basic STORE creates file record and file pin
    // -----------------------------------------------------------------------

    #[test]
    fn test_basic_store_creates_file_and_pin() {
        let key = [20u8; 32];
        let addr = addr_for_key(&key);
        let fh = fake_file_hash(1);
        let ic = make_store_content(&addr, 1_000.0, &fh);
        let msg = sign_store_message(&key, 1_000.0, ic);

        let db = Db::open_in_memory().unwrap();
        process_message(&db, &msg).expect("should process");

        let item_hash = msg.item_hash.to_string();
        db.with_conn(|conn| {
            let rec = get_file(conn, &fh)
                .unwrap()
                .expect("file record should exist");
            assert_eq!(rec.hash, fh);
            assert_eq!(rec.file_type, "file");

            let count = count_active_pins(conn, &fh).unwrap();
            assert_eq!(count, 1);
        });
        let _ = item_hash;
    }

    // -----------------------------------------------------------------------
    // Test 2: STORE with ref to existing STORE works
    // -----------------------------------------------------------------------

    #[test]
    fn test_store_with_ref_to_existing_store() {
        let key = [21u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        // First STORE (no ref).
        let fh1 = fake_file_hash(2);
        let ic1 = make_store_content(&addr, 1_000.0, &fh1);
        let msg1 = sign_store_message(&key, 1_000.0, ic1);
        let hash1 = msg1.item_hash.to_string();
        process_message(&db, &msg1).expect("first store");

        // Second STORE with ref pointing to first.
        let fh2 = fake_file_hash(3);
        let ic2 = make_store_content_with_ref(&addr, 1_001.0, &fh2, &hash1);
        let msg2 = sign_store_message(&key, 1_001.0, ic2);
        let result = process_message(&db, &msg2);
        assert!(
            result.is_ok(),
            "store with valid ref should succeed: {:?}",
            result
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: STORE with ref to non-STORE message fails with StoreRefNotFound
    // -----------------------------------------------------------------------

    #[test]
    fn test_store_ref_to_non_store_fails() {
        let key = [22u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        // Submit a POST message.
        let post_ic = format!(
            r#"{{"type":"test","address":"{}","time":1000.0,"content":{{"body":"Hi"}}}}"#,
            addr
        );
        let post_hash = ItemHash::Native(AlephItemHash::from_bytes(post_ic.as_bytes()));
        let unsigned_post = UnsignedMessage {
            message_type: MessageType::Post,
            item_type: ItemType::Inline,
            item_content: post_ic.clone(),
            item_hash: post_hash.clone(),
            time: Timestamp::from(1000.0),
            channel: None,
        };
        let account = EvmAccount::new(Chain::Ethereum, &key).unwrap();
        let pending_post = sign_message(&account, unsigned_post).unwrap();
        let post_msg = IncomingMessage {
            chain: pending_post.chain,
            sender: pending_post.sender,
            signature: pending_post.signature,
            message_type: pending_post.message_type,
            item_type: pending_post.item_type,
            item_content: Some(pending_post.item_content),
            item_hash: pending_post.item_hash,
            time: pending_post.time,
            channel: pending_post.channel,
        };
        let post_hash_str = post_msg.item_hash.to_string();
        process_message(&db, &post_msg).expect("post should process");

        // Now STORE with ref pointing to the POST.
        let fh = fake_file_hash(4);
        let ic = make_store_content_with_ref(&addr, 1_001.0, &fh, &post_hash_str);
        let store_msg = sign_store_message(&key, 1_001.0, ic);
        let result = process_message(&db, &store_msg);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code(),
            200,
            "expected StoreRefNotFound (200), got {:?}",
            err
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: STORE with ref to STORE that has its own ref → StoreUpdateUpdate
    // -----------------------------------------------------------------------

    #[test]
    fn test_store_ref_to_store_with_ref_fails() {
        let key = [23u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        // STORE 1 — no ref.
        let fh1 = fake_file_hash(5);
        let ic1 = make_store_content(&addr, 1_000.0, &fh1);
        let msg1 = sign_store_message(&key, 1_000.0, ic1);
        let hash1 = msg1.item_hash.to_string();
        process_message(&db, &msg1).expect("store 1");

        // STORE 2 — ref → STORE 1.
        let fh2 = fake_file_hash(6);
        let ic2 = make_store_content_with_ref(&addr, 1_001.0, &fh2, &hash1);
        let msg2 = sign_store_message(&key, 1_001.0, ic2);
        let hash2 = msg2.item_hash.to_string();
        process_message(&db, &msg2).expect("store 2 with ref to store 1");

        // STORE 3 — ref → STORE 2 (which has a ref) → should fail.
        let fh3 = fake_file_hash(7);
        let ic3 = make_store_content_with_ref(&addr, 1_002.0, &fh3, &hash2);
        let msg3 = sign_store_message(&key, 1_002.0, ic3);
        let result = process_message(&db, &msg3);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code(),
            201,
            "expected StoreUpdateUpdate (201), got {:?}",
            err
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: User-defined string ref creates tag
    // -----------------------------------------------------------------------

    #[test]
    fn test_user_defined_ref_creates_tag() {
        let key = [24u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        let fh = fake_file_hash(8);
        let ic = make_store_content_with_ref(&addr, 1_000.0, &fh, "my-custom-ref");
        let msg = sign_store_message(&key, 1_000.0, ic);
        process_message(&db, &msg).expect("should process");

        let owner_lower = addr.to_lowercase();
        let tag_key = format!("{owner_lower}:my-custom-ref");

        db.with_conn(|conn| {
            let tag = get_file_tag(conn, &tag_key)
                .unwrap()
                .expect("tag should exist");
            assert_eq!(tag.file_hash, fh);
        });
    }

    // -----------------------------------------------------------------------
    // Test 6: File tag tracks latest version (newer timestamp wins)
    // -----------------------------------------------------------------------

    #[test]
    fn test_file_tag_newer_wins() {
        let key = [25u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        let fh1 = fake_file_hash(9);
        let ic1 = make_store_content_with_ref(&addr, 1_000.0, &fh1, "versioned-ref");
        let msg1 = sign_store_message(&key, 1_000.0, ic1);
        process_message(&db, &msg1).expect("v1");

        let fh2 = fake_file_hash(10);
        let ic2 = make_store_content_with_ref(&addr, 1_001.0, &fh2, "versioned-ref");
        let msg2 = sign_store_message(&key, 1_001.0, ic2);
        process_message(&db, &msg2).expect("v2");

        let owner_lower = addr.to_lowercase();
        let tag_key = format!("{owner_lower}:versioned-ref");

        db.with_conn(|conn| {
            let tag = get_file_tag(conn, &tag_key).unwrap().unwrap();
            assert_eq!(tag.file_hash, fh2, "newer tag should win");
        });

        // Now submit an older-timestamped message.
        let fh_old = fake_file_hash(11);
        let ic_old = make_store_content_with_ref(&addr, 500.0, &fh_old, "versioned-ref");
        let msg_old = sign_store_message(&key, 500.0, ic_old);
        process_message(&db, &msg_old).expect("old");

        db.with_conn(|conn| {
            let tag = get_file_tag(conn, &tag_key).unwrap().unwrap();
            assert_eq!(tag.file_hash, fh2, "older update should not replace");
        });
    }

    // -----------------------------------------------------------------------
    // Test 7: FileStore write/read round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn test_file_store_round_trip() {
        use crate::files::FileStore;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let store = FileStore::new(dir.path()).unwrap();

        let data = b"Hello, Aleph STORE test!";
        let hash = store.write(data).unwrap();
        assert_eq!(hash.len(), 64);
        assert!(store.exists(&hash));
        assert_eq!(store.read(&hash).unwrap(), data);
        assert_eq!(store.size(&hash).unwrap(), data.len() as u64);
    }
}
