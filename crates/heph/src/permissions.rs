//! Permission system — spec section 6.
//!
//! Checks whether a message sender is authorized to act on behalf of content.address.

use crate::db::Db;
use crate::db::aggregates::get_aggregate;
use crate::db::messages::get_message_by_hash;
use crate::handlers::{IncomingMessage, ProcessingError, ProcessingResult};
use aleph_types::message::MessageContent;
use aleph_types::message::MessageContentEnum;
use aleph_types::message::MessageType;

/// Check whether the message sender is authorized to act on behalf of `content.address`.
///
/// Algorithm (spec section 6):
/// 1. Direct ownership: if sender == content.address → ALLOW.
/// 2. POST amend special case: validate the original owner matches, then delegate.
/// 3. Otherwise: delegate to the security aggregate check.
pub fn check_sender_authorization(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
) -> ProcessingResult<()> {
    let sender = msg.sender.as_str();
    let address = content.address.as_str();

    // 1. Direct ownership (case-insensitive).
    if sender.to_lowercase() == address.to_lowercase() {
        return Ok(());
    }

    // 2. POST amend special case.
    if msg.message_type == MessageType::Post
        && let MessageContentEnum::Post(post) = &content.content
        && post.is_amend()
    {
        // Extract the ref hash via serialization.
        let post_val = serde_json::to_value(&post.post_type)
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
        let ref_hash = post_val
            .get("ref")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if ref_hash.is_empty() {
            return Err(ProcessingError::PermissionDenied(
                "amend has empty ref".into(),
            ));
        }

        // Look up the original message.
        let original = db
            .with_conn(|conn| get_message_by_hash(conn, &ref_hash))
            .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        if let Some(orig_msg) = original {
            // The original's owner must match content.address.
            let original_address = orig_msg.owner.unwrap_or_default();
            if address.to_lowercase() != original_address.to_lowercase() {
                return Err(ProcessingError::PermissionDenied(format!(
                    "amend content.address {} does not match original owner {}",
                    address, original_address
                )));
            }
            return check_delegated(db, msg, content, sender, &original_address);
        }
        // Original not found — fall through to delegated check using content.address.
        return check_delegated(db, msg, content, sender, address);
    }

    // 3. Delegated check.
    check_delegated(db, msg, content, sender, address)
}

/// Check if `sender` is listed in the `security` aggregate of `owner_address`.
fn check_delegated(
    db: &Db,
    msg: &IncomingMessage,
    content: &MessageContent,
    sender: &str,
    owner_address: &str,
) -> ProcessingResult<()> {
    // Fetch the security aggregate for the owner.
    let aggregate = db
        .with_conn(|conn| get_aggregate(conn, owner_address, "security"))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    let aggregate = match aggregate {
        Some(a) => a,
        None => {
            return Err(ProcessingError::PermissionDenied(format!(
                "no security aggregate for {owner_address}"
            )));
        }
    };

    // Parse the aggregate content.
    let agg_value: serde_json::Value = serde_json::from_str(&aggregate.content).map_err(|e| {
        ProcessingError::InternalError(format!("security aggregate is not valid JSON: {e}"))
    })?;

    let authorizations = match agg_value.get("authorizations").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => {
            return Err(ProcessingError::PermissionDenied(format!(
                "security aggregate for {owner_address} has no authorizations array"
            )));
        }
    };

    // Extract channel string for comparison.
    let channel_str: Option<String> = msg.channel.as_ref().and_then(|c| {
        serde_json::to_value(c)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    });

    // Extract content-specific type for post_types / aggregate_keys filters.
    let post_type_str: Option<String> = if msg.message_type == MessageType::Post {
        if let MessageContentEnum::Post(post) = &content.content {
            Some(post.post_type_str().to_string())
        } else {
            None
        }
    } else {
        None
    };

    let aggregate_key_str: Option<String> = if msg.message_type == MessageType::Aggregate {
        if let MessageContentEnum::Aggregate(agg) = &content.content {
            Some(agg.key().to_string())
        } else {
            None
        }
    } else {
        None
    };

    let msg_type_str = msg.message_type.to_string(); // e.g. "POST"
    let chain_str = msg.chain.to_string(); // e.g. "ETH"

    for auth in authorizations {
        // Check address match (case-insensitive).
        let auth_addr = auth.get("address").and_then(|v| v.as_str()).unwrap_or("");
        if auth_addr.to_lowercase() != sender.to_lowercase() {
            continue;
        }

        // Check chain filter (if set).
        if let Some(auth_chain) = auth.get("chain").and_then(|v| v.as_str())
            && !auth_chain.is_empty()
            && auth_chain.to_uppercase() != chain_str.to_uppercase()
        {
            continue;
        }

        // Check channels filter (if non-empty).
        if let Some(channels_arr) = auth.get("channels").and_then(|v| v.as_array())
            && !channels_arr.is_empty()
        {
            let channels: Vec<String> = channels_arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            let msg_channel = channel_str.as_deref().unwrap_or("");
            if !channels.iter().any(|c| c == msg_channel) {
                continue;
            }
        }

        // Check types filter (if non-empty).
        if let Some(types_arr) = auth.get("types").and_then(|v| v.as_array())
            && !types_arr.is_empty()
        {
            let types: Vec<String> = types_arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_uppercase()))
                .collect();
            if !types.contains(&msg_type_str) {
                continue;
            }
        }

        // Check post_types filter (POST only).
        if msg.message_type == MessageType::Post
            && let Some(post_types_arr) = auth.get("post_types").and_then(|v| v.as_array())
            && !post_types_arr.is_empty()
        {
            let post_types: Vec<String> = post_types_arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            let pt = post_type_str.as_deref().unwrap_or("");
            if !post_types.iter().any(|t| t == pt) {
                continue;
            }
        }

        // Check aggregate_keys filter (AGGREGATE only).
        if msg.message_type == MessageType::Aggregate
            && let Some(agg_keys_arr) = auth.get("aggregate_keys").and_then(|v| v.as_array())
            && !agg_keys_arr.is_empty()
        {
            let agg_keys: Vec<String> = agg_keys_arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            let ak = aggregate_key_str.as_deref().unwrap_or("");
            if !agg_keys.iter().any(|k| k == ak) {
                continue;
            }
        }

        // All filters passed — first match wins.
        return Ok(());
    }

    Err(ProcessingError::PermissionDenied(format!(
        "sender {sender} is not authorized to act for {owner_address}"
    )))
}

#[cfg(test)]
mod tests {
    use crate::db::Db;
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

    /// Submit a security aggregate for `owner_key` that authorizes `delegate_addr`.
    fn submit_security_aggregate(db: &Db, owner_key: &[u8; 32], authorizations_json: &str) {
        let addr = addr_for_key(owner_key);
        let content = format!(
            r#"{{"key":"security","address":"{}","time":1000.0,"content":{{"authorizations":{}}}}}"#,
            addr, authorizations_json
        );
        let msg = sign_inline(owner_key, MessageType::Aggregate, content);
        process_message(db, &msg).expect("security aggregate should process");
    }

    // -----------------------------------------------------------------------
    // Test 1: Self-authorization (sender == content.address) passes
    // -----------------------------------------------------------------------

    #[test]
    fn test_self_authorization_passes() {
        let key = [70u8; 32];
        let addr = addr_for_key(&key);
        let db = Db::open_in_memory().unwrap();

        let content = format!(
            r#"{{"type":"test","address":"{}","time":1000.0,"content":{{"body":"Hi"}}}}"#,
            addr
        );
        let msg = sign_inline(&key, MessageType::Post, content);
        let result = process_message(&db, &msg);
        assert!(
            result.is_ok(),
            "self-authorization should pass: {:?}",
            result
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: No security aggregate → DENY
    // -----------------------------------------------------------------------

    #[test]
    fn test_no_security_aggregate_deny() {
        let owner_key = [71u8; 32];
        let delegate_key = [72u8; 32];
        let owner_addr = addr_for_key(&owner_key);
        let db = Db::open_in_memory().unwrap();

        // Delegate tries to submit a POST on behalf of owner — no security aggregate.
        let content = format!(
            r#"{{"type":"test","address":"{}","time":1000.0,"content":{{"body":"Hi"}}}}"#,
            owner_addr
        );
        let msg = sign_inline(&delegate_key, MessageType::Post, content);
        let result = process_message(&db, &msg);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().error_code(),
            2,
            "expected PermissionDenied (2)"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: Basic delegation with matching auth entry → ALLOW
    // -----------------------------------------------------------------------

    #[test]
    fn test_basic_delegation_allow() {
        let owner_key = [73u8; 32];
        let delegate_key = [74u8; 32];
        let owner_addr = addr_for_key(&owner_key);
        let delegate_addr = addr_for_key(&delegate_key);
        let db = Db::open_in_memory().unwrap();

        // Submit security aggregate for owner authorizing delegate.
        let auths = format!(r#"[{{"address":"{}"}}]"#, delegate_addr);
        submit_security_aggregate(&db, &owner_key, &auths);

        // Delegate sends a POST on behalf of owner.
        let content = format!(
            r#"{{"type":"test","address":"{}","time":1001.0,"content":{{"body":"Hi"}}}}"#,
            owner_addr
        );
        let msg = sign_inline(&delegate_key, MessageType::Post, content);
        let result = process_message(&db, &msg);
        assert!(result.is_ok(), "delegation should be allowed: {:?}", result);
    }

    // -----------------------------------------------------------------------
    // Test 4: Chain filter mismatch → DENY
    // -----------------------------------------------------------------------

    #[test]
    fn test_chain_filter_mismatch_deny() {
        let owner_key = [75u8; 32];
        let delegate_key = [76u8; 32];
        let owner_addr = addr_for_key(&owner_key);
        let delegate_addr = addr_for_key(&delegate_key);
        let db = Db::open_in_memory().unwrap();

        // Authorize delegate only for SOL chain.
        let auths = format!(r#"[{{"address":"{}","chain":"SOL"}}]"#, delegate_addr);
        submit_security_aggregate(&db, &owner_key, &auths);

        // Delegate sends on ETH chain — should fail.
        let content = format!(
            r#"{{"type":"test","address":"{}","time":1001.0,"content":{{"body":"Hi"}}}}"#,
            owner_addr
        );
        let msg = sign_inline(&delegate_key, MessageType::Post, content);
        // msg.chain is ETH (from sign_inline)
        let result = process_message(&db, &msg);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().error_code(),
            2,
            "expected PermissionDenied (2)"
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: Type filter allows POST → ALLOW for POST
    // -----------------------------------------------------------------------

    #[test]
    fn test_type_filter_post_allow() {
        let owner_key = [77u8; 32];
        let delegate_key = [78u8; 32];
        let owner_addr = addr_for_key(&owner_key);
        let delegate_addr = addr_for_key(&delegate_key);
        let db = Db::open_in_memory().unwrap();

        // Authorize delegate only for POST messages.
        let auths = format!(r#"[{{"address":"{}","types":["POST"]}}]"#, delegate_addr);
        submit_security_aggregate(&db, &owner_key, &auths);

        let content = format!(
            r#"{{"type":"test","address":"{}","time":1001.0,"content":{{"body":"Hi"}}}}"#,
            owner_addr
        );
        let msg = sign_inline(&delegate_key, MessageType::Post, content);
        let result = process_message(&db, &msg);
        assert!(
            result.is_ok(),
            "POST should be allowed by type filter: {:?}",
            result
        );
    }

    // -----------------------------------------------------------------------
    // Test 6: Empty filters allow all
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_filters_allow_all() {
        let owner_key = [79u8; 32];
        let delegate_key = [80u8; 32];
        let owner_addr = addr_for_key(&owner_key);
        let delegate_addr = addr_for_key(&delegate_key);
        let db = Db::open_in_memory().unwrap();

        // Authorize with completely empty filters.
        let auths = format!(
            r#"[{{"address":"{}","chain":null,"channels":[],"types":[]}}]"#,
            delegate_addr
        );
        submit_security_aggregate(&db, &owner_key, &auths);

        // Try POST.
        let content = format!(
            r#"{{"type":"test","address":"{}","time":1001.0,"content":{{"body":"Hi"}}}}"#,
            owner_addr
        );
        let msg = sign_inline(&delegate_key, MessageType::Post, content);
        let result = process_message(&db, &msg);
        assert!(
            result.is_ok(),
            "empty filters should allow all: {:?}",
            result
        );
    }

    // -----------------------------------------------------------------------
    // Test 7: AGGREGATE type filter deny
    // -----------------------------------------------------------------------

    #[test]
    fn test_type_filter_denies_aggregate() {
        let owner_key = [81u8; 32];
        let delegate_key = [82u8; 32];
        let owner_addr = addr_for_key(&owner_key);
        let delegate_addr = addr_for_key(&delegate_key);
        let db = Db::open_in_memory().unwrap();

        // Authorize delegate only for POST — not AGGREGATE.
        let auths = format!(r#"[{{"address":"{}","types":["POST"]}}]"#, delegate_addr);
        submit_security_aggregate(&db, &owner_key, &auths);

        // Delegate tries to submit an AGGREGATE on behalf of owner.
        let content = format!(
            r#"{{"key":"profile","address":"{}","time":1001.0,"content":{{"name":"Bob"}}}}"#,
            owner_addr
        );
        let msg = sign_inline(&delegate_key, MessageType::Aggregate, content);
        let result = process_message(&db, &msg);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().error_code(),
            2,
            "expected PermissionDenied (2)"
        );
    }
}
