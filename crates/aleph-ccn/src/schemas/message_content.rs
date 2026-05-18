//! Mirrors `src/aleph/schemas/message_content.py`.
//!
//! Wrapper types describing where the content of a message came from and how
//! it is currently held (raw bytes, async stream, or already-decoded JSON).

use serde::{Deserialize, Serialize};

/// Source of a message's content. Mirrors `ContentSource` from
/// `aleph/schemas/message_content.py`.
///
/// Note: this is intentionally separate from `aleph_types::message::ContentSource`,
/// which models `item_type`/`item_content` for wire messages. This enum records
/// where the *node* actually found the content (DB, P2P fetch, IPFS, or inline
/// alongside the message envelope).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ContentSource {
    #[serde(rename = "DB")]
    Db,
    #[serde(rename = "P2P")]
    P2p,
    #[serde(rename = "IPFS")]
    Ipfs,
    #[serde(rename = "inline")]
    Inline,
}

/// Common base for resolved content: hash plus optional source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredContent {
    pub hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<ContentSource>,
}

/// Raw content (bytes). Python `RawContent` extends `StoredContent`.
#[derive(Debug, Clone, PartialEq)]
pub struct RawContent {
    pub hash: String,
    pub source: Option<ContentSource>,
    pub value: Vec<u8>,
}

impl RawContent {
    pub fn new(hash: String, source: Option<ContentSource>, value: Vec<u8>) -> Self {
        Self {
            hash,
            source,
            value,
        }
    }

    pub fn len(&self) -> usize {
        self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }
}

/// Decoded message content. Python `MessageContent` extends `StoredContent`
/// and carries both a parsed `value` (any JSON-decodable Python object) and
/// the original `raw_value` (bytes or str).
#[derive(Debug, Clone, PartialEq)]
pub struct MessageContent {
    pub hash: String,
    pub source: Option<ContentSource>,
    pub value: serde_json::Value,
    pub raw_value: RawValue,
}

impl MessageContent {
    pub fn new(
        hash: String,
        source: Option<ContentSource>,
        value: serde_json::Value,
        raw_value: RawValue,
    ) -> Self {
        Self {
            hash,
            source,
            value,
            raw_value,
        }
    }
}

/// `bytes | str` raw representation of a content payload.
#[derive(Debug, Clone, PartialEq)]
pub enum RawValue {
    Bytes(Vec<u8>),
    Str(String),
}

impl RawValue {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            RawValue::Bytes(b) => b.as_slice(),
            RawValue::Str(s) => s.as_bytes(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_source_roundtrip() {
        for (variant, expected) in [
            (ContentSource::Db, "\"DB\""),
            (ContentSource::P2p, "\"P2P\""),
            (ContentSource::Ipfs, "\"IPFS\""),
            (ContentSource::Inline, "\"inline\""),
        ] {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            let parsed: ContentSource = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_stored_content_roundtrip() {
        let json = serde_json::json!({"hash": "abc", "source": "DB"});
        let parsed: StoredContent = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(parsed.hash, "abc");
        assert_eq!(parsed.source, Some(ContentSource::Db));
        let back = serde_json::to_value(&parsed).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn test_raw_content_len() {
        let raw = RawContent::new("h".into(), None, vec![1, 2, 3]);
        assert_eq!(raw.len(), 3);
        assert!(!raw.is_empty());
    }

    #[test]
    fn test_raw_value_helpers() {
        let v = RawValue::Bytes(b"hello".to_vec());
        assert_eq!(v.as_bytes(), b"hello");
        assert_eq!(v.len(), 5);
        let v = RawValue::Str("world".to_string());
        assert_eq!(v.as_bytes(), b"world");
        assert!(!v.is_empty());
    }
}
