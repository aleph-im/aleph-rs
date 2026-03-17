use crate::handlers::{IncomingMessage, ProcessingError, ProcessingResult};
use aleph_types::item_hash::{AlephItemHash, ItemHash};
use aleph_types::message::MessageContent;
use aleph_types::message::item_type::ItemType;

/// Maximum allowed size for inline item_content.
pub const MAX_INLINE_SIZE: usize = 200_000;

/// Validate the format of an incoming message per spec section 5.2.
///
/// Returns the parsed `MessageContent` on success so the caller doesn't have to
/// deserialize twice.
///
/// Validation steps:
/// 1. Message type is one of the 6 supported types (enforced by `MessageType` enum, so
///    a deserialization failure catches this already; we check explicitly here for clarity).
/// 2. item_type / item_content consistency:
///    - `inline` requires `item_content` to be present.
///    - non-`inline` forbids `item_content` (must be `None`).
/// 3. Hash verification: for inline messages `sha256(item_content bytes)` must match `item_hash`.
/// 4. Content must be valid JSON.
/// 5. Inline content size must be <= 200,000 bytes.
/// 6. Content schema: item_content must deserialize to the type declared by `message_type`.
pub fn validate_format(msg: &IncomingMessage) -> ProcessingResult<MessageContent> {
    match msg.item_type {
        ItemType::Inline => validate_inline(msg),
        ItemType::Storage | ItemType::Ipfs => validate_non_inline(msg),
    }
}

fn validate_inline(msg: &IncomingMessage) -> ProcessingResult<MessageContent> {
    // Step 2 — item_content must be present for inline.
    let item_content = msg.item_content.as_deref().ok_or_else(|| {
        ProcessingError::InvalidFormat("item_content is required for inline messages".into())
    })?;

    // Step 5 — size check (on bytes, not chars).
    let content_bytes = item_content.as_bytes();
    if content_bytes.len() > MAX_INLINE_SIZE {
        return Err(ProcessingError::InvalidFormat(format!(
            "item_content size {} exceeds maximum of {} bytes",
            content_bytes.len(),
            MAX_INLINE_SIZE
        )));
    }

    // Step 3 — hash verification.
    let computed = AlephItemHash::from_bytes(content_bytes);
    let expected = ItemHash::Native(computed);
    if expected != msg.item_hash {
        return Err(ProcessingError::InvalidFormat(format!(
            "item_hash mismatch: expected {expected}, got {}",
            msg.item_hash
        )));
    }

    // Step 4 + 6 — parse as JSON, then deserialize into the declared content type.
    let content =
        MessageContent::deserialize_with_type(msg.message_type, content_bytes).map_err(|e| {
            ProcessingError::InvalidFormat(format!(
                "content does not match declared type {:?}: {e}",
                msg.message_type
            ))
        })?;

    Ok(content)
}

fn validate_non_inline(msg: &IncomingMessage) -> ProcessingResult<MessageContent> {
    // Step 2 — item_content must NOT be present for non-inline.
    if msg.item_content.is_some() {
        return Err(ProcessingError::InvalidFormat(
            "item_content must not be present for non-inline messages".into(),
        ));
    }

    // For non-inline (storage / IPFS) messages the actual content lives elsewhere.
    // We cannot parse or hash-verify the content here; the caller is responsible
    // for fetching and verifying it before processing.  Return a placeholder
    // error so callers know they need to fetch first.
    Err(ProcessingError::ContentUnavailable(
        "non-inline message content must be fetched before validation".into(),
    ))
}

/// Validate a non-inline message after the content has been fetched externally.
///
/// `raw_content` is the raw bytes fetched from storage / IPFS.  The `item_hash`
/// in `msg` should match the SHA-256 of these bytes (for storage messages) or
/// the CID bytes (for IPFS messages — currently unimplemented).
pub fn validate_fetched_content(
    msg: &IncomingMessage,
    raw_content: &[u8],
) -> ProcessingResult<MessageContent> {
    // For storage messages: verify SHA-256.
    if msg.item_type == ItemType::Storage {
        let computed = AlephItemHash::from_bytes(raw_content);
        let expected = ItemHash::Native(computed);
        if expected != msg.item_hash {
            return Err(ProcessingError::InvalidFormat(format!(
                "item_hash mismatch for storage content: expected {expected}, got {}",
                msg.item_hash
            )));
        }
    }
    // For IPFS: CID verification is out of scope for now.

    let content =
        MessageContent::deserialize_with_type(msg.message_type, raw_content).map_err(|e| {
            ProcessingError::InvalidFormat(format!(
                "fetched content does not match declared type {:?}: {e}",
                msg.message_type
            ))
        })?;

    Ok(content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::address;
    use aleph_types::chain::{Chain, Signature};
    use aleph_types::channel::Channel;
    use aleph_types::item_hash::AlephItemHash;
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use aleph_types::timestamp::Timestamp;

    /// Build a minimal valid inline POST message whose item_content hashes to item_hash.
    fn make_inline_post(item_content: &str) -> IncomingMessage {
        let computed = AlephItemHash::from_bytes(item_content.as_bytes());
        IncomingMessage {
            chain: Chain::Ethereum,
            sender: address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"),
            signature: Signature::from("0xdeadbeef".to_string()),
            message_type: MessageType::Post,
            item_type: ItemType::Inline,
            item_content: Some(item_content.to_string()),
            item_hash: ItemHash::Native(computed),
            time: Timestamp::from(1762515431.653),
            channel: Some(Channel::from("TEST".to_string())),
        }
    }

    /// Minimal valid POST item_content (matches the JSON fields PostContent expects).
    fn valid_post_content() -> String {
        r#"{"type":"test","address":"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef","time":1762515431.653,"content":{"body":"Hello"}}"#.to_string()
    }

    // -----------------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------------

    #[test]
    fn test_valid_inline_message_passes() {
        let ic = valid_post_content();
        let msg = make_inline_post(&ic);
        let result = validate_format(&msg);
        assert!(result.is_ok(), "expected Ok but got {:?}", result);
    }

    // -----------------------------------------------------------------------
    // Inline consistency
    // -----------------------------------------------------------------------

    #[test]
    fn test_missing_item_content_for_inline_fails() {
        let ic = valid_post_content();
        let mut msg = make_inline_post(&ic);
        msg.item_content = None;
        let err = validate_format(&msg).unwrap_err();
        assert_eq!(err.error_code(), 0, "expected INVALID_FORMAT");
        assert!(err.message().contains("item_content is required"));
    }

    #[test]
    fn test_item_content_present_for_storage_fails() {
        let ic = valid_post_content();
        let computed = AlephItemHash::from_bytes(ic.as_bytes());
        let mut msg = make_inline_post(&ic);
        msg.item_type = ItemType::Storage;
        msg.item_content = Some(ic.clone()); // forbidden for non-inline
        msg.item_hash = ItemHash::Native(computed);
        let err = validate_format(&msg).unwrap_err();
        assert_eq!(err.error_code(), 0, "expected INVALID_FORMAT");
        assert!(err.message().contains("must not be present"));
    }

    #[test]
    fn test_item_content_present_for_ipfs_fails() {
        let ic = valid_post_content();
        let computed = AlephItemHash::from_bytes(ic.as_bytes());
        let mut msg = make_inline_post(&ic);
        msg.item_type = ItemType::Ipfs;
        msg.item_content = Some(ic.clone());
        msg.item_hash = ItemHash::Native(computed);
        let err = validate_format(&msg).unwrap_err();
        assert_eq!(err.error_code(), 0, "expected INVALID_FORMAT");
    }

    // -----------------------------------------------------------------------
    // Hash verification
    // -----------------------------------------------------------------------

    #[test]
    fn test_wrong_item_hash_fails() {
        let ic = valid_post_content();
        let mut msg = make_inline_post(&ic);
        // Overwrite the hash with all-zeros
        let wrong_hash = AlephItemHash::new([0u8; 32]);
        msg.item_hash = ItemHash::Native(wrong_hash);
        let err = validate_format(&msg).unwrap_err();
        assert_eq!(err.error_code(), 0, "expected INVALID_FORMAT");
        assert!(err.message().contains("item_hash mismatch"));
    }

    // -----------------------------------------------------------------------
    // Size limit
    // -----------------------------------------------------------------------

    #[test]
    fn test_oversized_content_fails() {
        // Build a string that exceeds 200 KB by wrapping it in a valid JSON structure.
        // We need the overall item_content to exceed MAX_INLINE_SIZE bytes.
        let padding = "x".repeat(MAX_INLINE_SIZE + 1);
        let ic = format!(
            r#"{{"type":"test","address":"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef","time":1000.0,"content":{{"data":"{}"}}}}"#,
            padding
        );
        let msg = make_inline_post(&ic);
        let err = validate_format(&msg).unwrap_err();
        assert_eq!(err.error_code(), 0, "expected INVALID_FORMAT");
        assert!(err.message().contains("exceeds maximum"));
    }

    // -----------------------------------------------------------------------
    // JSON / schema validation
    // -----------------------------------------------------------------------

    #[test]
    fn test_invalid_json_content_fails() {
        // Build a string with a correct hash but non-JSON content.
        let bad_content = "not json at all {{{";
        let computed = AlephItemHash::from_bytes(bad_content.as_bytes());
        let msg = IncomingMessage {
            chain: Chain::Ethereum,
            sender: address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"),
            signature: Signature::from("0xdeadbeef".to_string()),
            message_type: MessageType::Post,
            item_type: ItemType::Inline,
            item_content: Some(bad_content.to_string()),
            item_hash: ItemHash::Native(computed),
            time: Timestamp::from(1000.0),
            channel: None,
        };
        let err = validate_format(&msg).unwrap_err();
        assert_eq!(err.error_code(), 0, "expected INVALID_FORMAT");
    }

    #[test]
    fn test_wrong_content_schema_fails() {
        // A valid JSON object but not a valid PostContent (missing required fields).
        let bad_schema = r#"{"completely":"wrong","structure":42}"#;
        let computed = AlephItemHash::from_bytes(bad_schema.as_bytes());
        let msg = IncomingMessage {
            chain: Chain::Ethereum,
            sender: address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"),
            signature: Signature::from("0xdeadbeef".to_string()),
            message_type: MessageType::Aggregate,
            item_type: ItemType::Inline,
            item_content: Some(bad_schema.to_string()),
            item_hash: ItemHash::Native(computed),
            time: Timestamp::from(1000.0),
            channel: None,
        };
        let err = validate_format(&msg).unwrap_err();
        assert_eq!(err.error_code(), 0, "expected INVALID_FORMAT");
    }

    // -----------------------------------------------------------------------
    // Non-inline path returns ContentUnavailable
    // -----------------------------------------------------------------------

    #[test]
    fn test_storage_without_item_content_returns_content_unavailable() {
        let ic = valid_post_content();
        let computed = AlephItemHash::from_bytes(ic.as_bytes());
        let msg = IncomingMessage {
            chain: Chain::Ethereum,
            sender: address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"),
            signature: Signature::from("0xdeadbeef".to_string()),
            message_type: MessageType::Aggregate,
            item_type: ItemType::Storage,
            item_content: None,
            item_hash: ItemHash::Native(computed),
            time: Timestamp::from(1000.0),
            channel: None,
        };
        let err = validate_format(&msg).unwrap_err();
        assert_eq!(err.error_code(), 3, "expected CONTENT_UNAVAILABLE");
    }

    // -----------------------------------------------------------------------
    // validate_fetched_content
    // -----------------------------------------------------------------------

    #[test]
    fn test_validate_fetched_content_valid() {
        let ic = valid_post_content();
        let computed = AlephItemHash::from_bytes(ic.as_bytes());
        let msg = IncomingMessage {
            chain: Chain::Ethereum,
            sender: address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"),
            signature: Signature::from("0xdeadbeef".to_string()),
            message_type: MessageType::Post,
            item_type: ItemType::Storage,
            item_content: None,
            item_hash: ItemHash::Native(computed),
            time: Timestamp::from(1000.0),
            channel: None,
        };
        let result = validate_fetched_content(&msg, ic.as_bytes());
        assert!(result.is_ok(), "expected Ok but got {:?}", result);
    }

    #[test]
    fn test_validate_fetched_content_hash_mismatch() {
        let ic = valid_post_content();
        let wrong_hash = AlephItemHash::new([0u8; 32]);
        let msg = IncomingMessage {
            chain: Chain::Ethereum,
            sender: address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"),
            signature: Signature::from("0xdeadbeef".to_string()),
            message_type: MessageType::Post,
            item_type: ItemType::Storage,
            item_content: None,
            item_hash: ItemHash::Native(wrong_hash),
            time: Timestamp::from(1000.0),
            channel: None,
        };
        let err = validate_fetched_content(&msg, ic.as_bytes()).unwrap_err();
        assert_eq!(err.error_code(), 0, "expected INVALID_FORMAT");
        assert!(err.message().contains("item_hash mismatch"));
    }
}
