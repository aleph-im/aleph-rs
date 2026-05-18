//! Mirrors `src/aleph/schemas/base_messages.py`.
//!
//! Generic base shape shared by all Aleph message representations. Python
//! defines `AlephBaseMessage[MType, ContentType]`; we mirror it with a
//! Rust generic struct. The `MType`/`ContentType` parameters carry the
//! type information used by the wrapping enums (post / aggregate / …).

use std::marker::PhantomData;

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Marker trait identifying valid concrete message-type tags.
pub trait MessageTypeTag {
    fn message_type() -> MessageType;
}

/// Aleph base message struct. Mirrors `AlephBaseMessage` in pyaleph; the
/// generic `MType`/`ContentType` parameters are phantom and serve to thread
/// the type variant through wrapping enums.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlephBaseMessage<MType, ContentType> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<Chain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub message_type: Option<MessageType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    pub item_type: ItemType,
    pub item_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default = "Option::default", skip_serializing_if = "Option::is_none")]
    pub content: Option<ContentType>,
    #[serde(skip)]
    _mtype: PhantomData<MType>,
}

/// Errors raised by the structural validators ported from Python.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BaseMessageError {
    #[error("Could not determine item type")]
    MissingItemType,
    #[error("Could not determine item hash")]
    MissingItemHash,
    #[error("Could not find inline item content")]
    MissingInlineContent,
    #[error("Expected {expected:?} based on hash but item type is {actual:?}.")]
    ItemTypeMismatch {
        expected: ItemType,
        actual: ItemType,
    },
    #[error("'item_hash' does not match 'sha256(item_content)', expecting {expected}")]
    ItemHashMismatch { expected: String },
    #[error("Unknown item type: '{0:?}'")]
    UnknownItemType(ItemType),
    #[error("Unexpected hash type: '{0}'")]
    UnknownHashType(String),
}

/// Mirrors `item_type_from_hash` in `aleph.utils`:
/// - 64-char hex string => `Storage`
/// - non-empty otherwise => `Ipfs`
/// - empty => error
pub fn item_type_from_hash(hash: &str) -> Result<ItemType, BaseMessageError> {
    if hash.is_empty() {
        return Err(BaseMessageError::UnknownHashType(hash.to_string()));
    }
    let is_hex_hash = hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit());
    if is_hex_hash {
        Ok(ItemType::Storage)
    } else {
        Ok(ItemType::Ipfs)
    }
}

impl<MType, ContentType> AlephBaseMessage<MType, ContentType> {
    /// Mirror of `base_message_validator_check_item_type` + `check_item_hash`.
    pub fn validate_item_layout(&self) -> Result<(), BaseMessageError> {
        // check_item_type: for non-inline types, the hash must match
        // `item_type_from_hash(item_hash)`.
        if self.item_type != ItemType::Inline {
            if self.item_hash.is_empty() {
                return Err(BaseMessageError::MissingItemHash);
            }
            let expected = item_type_from_hash(&self.item_hash)?;
            if expected != self.item_type {
                return Err(BaseMessageError::ItemTypeMismatch {
                    expected,
                    actual: self.item_type,
                });
            }
        }

        // check_item_hash: for inline types, item_hash must equal sha256(item_content).
        match self.item_type {
            ItemType::Inline => {
                let item_content = self
                    .item_content
                    .as_deref()
                    .ok_or(BaseMessageError::MissingInlineContent)?;
                let mut h = Sha256::new();
                h.update(item_content.as_bytes());
                let computed = format!("{:x}", h.finalize());
                if self.item_hash != computed {
                    return Err(BaseMessageError::ItemHashMismatch { expected: computed });
                }
            }
            ItemType::Storage | ItemType::Ipfs => {}
        }
        Ok(())
    }
}

impl<MType, ContentType> Default for AlephBaseMessage<MType, ContentType> {
    fn default() -> Self {
        Self {
            sender: None,
            chain: None,
            signature: None,
            message_type: None,
            item_content: None,
            item_type: ItemType::Inline,
            item_hash: String::new(),
            time: None,
            channel: None,
            content: None,
            _mtype: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct DummyContent {
        value: String,
    }

    #[test]
    fn test_item_type_from_hash_storage() {
        let h = "a".repeat(64);
        assert_eq!(item_type_from_hash(&h).unwrap(), ItemType::Storage);
    }

    #[test]
    fn test_item_type_from_hash_ipfs() {
        assert_eq!(
            item_type_from_hash("QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8").unwrap(),
            ItemType::Ipfs
        );
    }

    #[test]
    fn test_aleph_base_message_inline_validates() {
        let item_content = "{\"value\":\"hi\"}";
        let mut h = Sha256::new();
        h.update(item_content.as_bytes());
        let hash_hex = format!("{:x}", h.finalize());

        let msg = AlephBaseMessage::<(), DummyContent> {
            item_type: ItemType::Inline,
            item_content: Some(item_content.to_string()),
            item_hash: hash_hex.clone(),
            ..Default::default()
        };
        msg.validate_item_layout().unwrap();
    }

    #[test]
    fn test_aleph_base_message_inline_hash_mismatch() {
        let msg = AlephBaseMessage::<(), DummyContent> {
            item_type: ItemType::Inline,
            item_content: Some("content".to_string()),
            item_hash: "wronghash".to_string(),
            ..Default::default()
        };
        assert!(msg.validate_item_layout().is_err());
    }

    #[test]
    fn test_aleph_base_message_storage_type_mismatch() {
        let msg = AlephBaseMessage::<(), DummyContent> {
            item_type: ItemType::Storage,
            item_hash: "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8".to_string(),
            ..Default::default()
        };
        // The hash looks like IPFS so it should error.
        assert!(msg.validate_item_layout().is_err());
    }

    #[test]
    fn test_aleph_base_message_roundtrip_serde() {
        let json = serde_json::json!({
            "item_type": "storage",
            "item_hash": "a".repeat(64),
        });
        let msg: AlephBaseMessage<(), DummyContent> = serde_json::from_value(json).unwrap();
        msg.validate_item_layout().unwrap();
        let back = serde_json::to_value(&msg).unwrap();
        assert_eq!(back["item_type"], "storage");
    }
}
