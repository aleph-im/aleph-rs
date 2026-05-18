//! Mirrors `src/aleph/schemas/cost_estimation_messages.py`.
//!
//! Cost-estimation request payloads. These extend the wire content shapes with
//! an `estimated_size_mib` field so callers of `/price/estimate/*` can request
//! a quote without uploading real volume payloads first.

use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::base::{Encoding, Interface};
use aleph_types::message::execution::volume::{BaseVolume, EphemeralVolume, PersistentVolume};
use aleph_types::message::{InstanceContent, MessageType, ProgramContent, StoreContent};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::schemas::pending_messages::PendingMessageError;

/// Immutable volume reference, augmented with an optional estimated size.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationImmutableVolume {
    #[serde(flatten)]
    pub base: BaseVolume,
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    #[serde(default)]
    pub use_latest: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_size_mib: Option<u64>,
}

/// Volume types accepted in cost-estimation requests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CostEstimationMachineVolume {
    Immutable(CostEstimationImmutableVolume),
    Ephemeral(EphemeralVolume),
    Persistent(PersistentVolume),
}

/// Instance content for cost estimation. `time` and `allow_amend` are kept
/// optional so callers of `/price/estimate/instance` don't have to supply them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationInstanceContent {
    #[serde(flatten)]
    pub base: InstanceContent,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<CostEstimationMachineVolume>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<f64>,
    #[serde(default)]
    pub allow_amend: bool,
}

/// Code content with optional estimated size. Mirrors
/// `CostEstimationCodeContent` in pyaleph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationCodeContent {
    pub encoding: Encoding,
    pub entrypoint: String,
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<Interface>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(default)]
    pub use_latest: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_size_mib: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationFunctionRuntime {
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    #[serde(default)]
    pub use_latest: bool,
    pub comment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_size_mib: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationDataContent {
    pub encoding: Encoding,
    pub mount: PathBuf,
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_latest: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_size_mib: Option<u64>,
}

/// Program content for cost estimation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationProgramContent {
    #[serde(flatten)]
    pub base: ProgramContent,
    pub code: CostEstimationCodeContent,
    pub runtime: CostEstimationFunctionRuntime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<CostEstimationDataContent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<CostEstimationMachineVolume>,
}

/// Store content with optional estimated size.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationStoreContent {
    #[serde(flatten)]
    pub base: StoreContent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_size_mib: Option<u64>,
}

/// Cost-estimation content variants. Mirrors `CostEstimationContent`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CostEstimationContent {
    Instance(CostEstimationInstanceContent),
    Program(CostEstimationProgramContent),
    Store(CostEstimationStoreContent),
}

/// Cost-estimation message variants. Each variant carries a typed content and
/// the message envelope. Mirrors `CostEstimationMessage`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CostEstimationMessage {
    Instance(CostEstimationInstanceMessage),
    Program(CostEstimationProgramMessage),
    Store(CostEstimationStoreMessage),
}

impl CostEstimationMessage {
    pub fn message_type(&self) -> MessageType {
        match self {
            CostEstimationMessage::Instance(_) => MessageType::Instance,
            CostEstimationMessage::Program(_) => MessageType::Program,
            CostEstimationMessage::Store(_) => MessageType::Store,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationInstanceMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<aleph_types::chain::Chain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    pub item_type: aleph_types::message::item_type::ItemType,
    pub item_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<CostEstimationInstanceContent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationProgramMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<aleph_types::chain::Chain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    pub item_type: aleph_types::message::item_type::ItemType,
    pub item_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<CostEstimationProgramContent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEstimationStoreMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<aleph_types::chain::Chain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    pub item_type: aleph_types::message::item_type::ItemType,
    pub item_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<CostEstimationStoreContent>,
}

/// Errors returned by [`parse_message`]. Mirrors Python `InvalidMessageFormat`.
#[derive(Debug, thiserror::Error)]
pub enum CostEstimationParseError {
    #[error("Message is not a dictionary")]
    NotAnObject,
    #[error("Invalid message_type: '{0}'")]
    InvalidMessageType(String),
    #[error("Unsupported cost-estimation message type: {0}")]
    UnsupportedType(MessageType),
    #[error("{0}")]
    Validation(String),
    #[error(transparent)]
    Pending(#[from] PendingMessageError),
}

/// Mirrors `parse_message` in `cost_estimation_messages.py`.
pub fn parse_message(
    value: serde_json::Value,
) -> Result<CostEstimationMessage, CostEstimationParseError> {
    let map = match value.as_object() {
        Some(m) => m,
        None => return Err(CostEstimationParseError::NotAnObject),
    };

    let raw_type = map
        .get("type")
        .cloned()
        .ok_or_else(|| CostEstimationParseError::InvalidMessageType("(missing)".to_string()))?;
    let message_type: MessageType = serde_json::from_value(raw_type.clone())
        .map_err(|_| CostEstimationParseError::InvalidMessageType(raw_type.to_string()))?;

    let mut clone = value.clone();
    if let Some(obj) = clone.as_object_mut() {
        if let Some(item_content) = obj.get("item_content").and_then(|v| v.as_str()) {
            let parsed: serde_json::Value = serde_json::from_str(item_content)
                .map_err(|e| CostEstimationParseError::Validation(format!("item_content: {e}")))?;
            obj.insert("content".to_string(), parsed);
        }
    }

    let msg = match message_type {
        MessageType::Instance => CostEstimationMessage::Instance(
            serde_json::from_value(clone)
                .map_err(|e| CostEstimationParseError::Validation(e.to_string()))?,
        ),
        MessageType::Program => CostEstimationMessage::Program(
            serde_json::from_value(clone)
                .map_err(|e| CostEstimationParseError::Validation(e.to_string()))?,
        ),
        MessageType::Store => CostEstimationMessage::Store(
            serde_json::from_value(clone)
                .map_err(|e| CostEstimationParseError::Validation(e.to_string()))?,
        ),
        other => return Err(CostEstimationParseError::UnsupportedType(other)),
    };
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_estimation_store_content_roundtrip() {
        let json = serde_json::json!({
            "item_type": "storage",
            "item_hash": "a".repeat(64),
            "estimated_size_mib": 100
        });
        let parsed: CostEstimationStoreContent = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.estimated_size_mib, Some(100));
    }

    #[test]
    fn test_cost_estimation_message_parse_unsupported_type() {
        let json = serde_json::json!({
            "type": "POST",
            "item_type": "inline",
            "item_hash": "ignored",
            "item_content": "{}"
        });
        let err = parse_message(json).unwrap_err();
        assert!(matches!(err, CostEstimationParseError::UnsupportedType(_)));
    }

    #[test]
    fn test_cost_estimation_message_parse_invalid_type() {
        let json = serde_json::json!({
            "type": "INVALID",
            "item_type": "inline",
            "item_hash": "ignored",
            "item_content": "{}"
        });
        let err = parse_message(json).unwrap_err();
        assert!(matches!(
            err,
            CostEstimationParseError::InvalidMessageType(_)
        ));
    }

    #[test]
    fn test_cost_estimation_message_not_object() {
        let err = parse_message(serde_json::json!(123)).unwrap_err();
        assert!(matches!(err, CostEstimationParseError::NotAnObject));
    }
}
