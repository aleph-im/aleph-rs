//! Mirrors `src/aleph/schemas/api/messages.py`.
//!
//! Wire shapes returned by `/api/v0/messages*`. Mirrors the Python
//! `AlephMessage` / `MessageWithStatus` / `MessageListResponse` Pydantic
//! definitions.

use std::marker::PhantomData;

use aleph_types::chain::Chain;
use aleph_types::message::item_type::ItemType;
use aleph_types::message::{
    AggregateContent, ForgetContent, InstanceContent, MessageType, PostContent, ProgramContent,
    StoreContent,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::message_status::{ErrorCode, MessageStatus, RemovedMessageReason};

/// On-chain confirmation. Mirrors Python `MessageConfirmation` in this module.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageConfirmation {
    pub chain: Chain,
    pub height: i64,
    pub hash: String,
}

/// Time field serialised as a UNIX timestamp (mirrors the Python
/// `@field_serializer("time")` that returns `dt.timestamp()`).
fn serialize_time_as_timestamp<S: serde::Serializer>(
    dt: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let secs = dt.timestamp() as f64 + (dt.timestamp_subsec_nanos() as f64) / 1_000_000_000.0;
    serializer.serialize_f64(secs)
}

fn deserialize_time_from_timestamp<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<DateTime<Utc>, D::Error> {
    let v = serde_json::Value::deserialize(deserializer)?;
    if let Some(f) = v.as_f64() {
        let secs = f.floor() as i64;
        let ns = ((f - f.floor()) * 1_000_000_000.0).round() as u32;
        return chrono::TimeZone::timestamp_opt(&Utc, secs, ns)
            .single()
            .ok_or_else(|| serde::de::Error::custom("invalid timestamp"));
    }
    if let Some(s) = v.as_str() {
        if let Ok(f) = s.parse::<f64>() {
            let secs = f.floor() as i64;
            let ns = ((f - f.floor()) * 1_000_000_000.0).round() as u32;
            return chrono::TimeZone::timestamp_opt(&Utc, secs, ns)
                .single()
                .ok_or_else(|| serde::de::Error::custom("invalid timestamp"));
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(dt.with_timezone(&Utc));
        }
    }
    Err(serde::de::Error::custom(
        "time must be a number or RFC-3339 string",
    ))
}

/// Generic base message. Mirrors `BaseMessage[MType, ContentType]`.
///
/// The `type` field is the discriminator on the wrapping `AlephMessage` enum,
/// so we deliberately omit it from this struct — serde reads/writes it once
/// at the enum level.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseMessage<MType, ContentType> {
    pub sender: String,
    pub chain: Chain,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    pub item_type: ItemType,
    pub item_hash: String,
    #[serde(
        serialize_with = "serialize_time_as_timestamp",
        deserialize_with = "deserialize_time_from_timestamp"
    )]
    pub time: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub content: ContentType,
    #[serde(default)]
    pub confirmed: bool,
    #[serde(default)]
    pub confirmations: Vec<MessageConfirmation>,
    #[serde(skip)]
    _mtype: PhantomData<MType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Aggregate;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Forget;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Instance;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Post;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Program;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Store;

pub type AggregateMessage = BaseMessage<Aggregate, AggregateContent>;
pub type ForgetMessage = BaseMessage<Forget, ForgetContent>;
pub type InstanceMessage = BaseMessage<Instance, InstanceContent>;
pub type PostMessage = BaseMessage<Post, PostContent>;
pub type ProgramMessage = BaseMessage<Program, ProgramContent>;
pub type StoreMessage = BaseMessage<Store, StoreContent>;

/// Discriminated union over message types — mirrors `AlephMessage`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AlephMessage {
    #[serde(rename = "AGGREGATE")]
    Aggregate(AggregateMessage),
    #[serde(rename = "FORGET")]
    Forget(ForgetMessage),
    #[serde(rename = "INSTANCE")]
    Instance(InstanceMessage),
    #[serde(rename = "POST")]
    Post(PostMessage),
    #[serde(rename = "PROGRAM")]
    Program(ProgramMessage),
    #[serde(rename = "STORE")]
    Store(StoreMessage),
}

impl AlephMessage {
    pub fn message_type(&self) -> MessageType {
        match self {
            AlephMessage::Aggregate(_) => MessageType::Aggregate,
            AlephMessage::Forget(_) => MessageType::Forget,
            AlephMessage::Instance(_) => MessageType::Instance,
            AlephMessage::Post(_) => MessageType::Post,
            AlephMessage::Program(_) => MessageType::Program,
            AlephMessage::Store(_) => MessageType::Store,
        }
    }

    pub fn item_hash(&self) -> &str {
        match self {
            AlephMessage::Aggregate(m) => &m.item_hash,
            AlephMessage::Forget(m) => &m.item_hash,
            AlephMessage::Instance(m) => &m.item_hash,
            AlephMessage::Post(m) => &m.item_hash,
            AlephMessage::Program(m) => &m.item_hash,
            AlephMessage::Store(m) => &m.item_hash,
        }
    }
}

/// Build an `AlephMessage` from raw JSON, dispatched by the `type` field.
/// Mirrors `format_message_dict`.
pub fn format_message_dict(value: serde_json::Value) -> Result<AlephMessage, String> {
    serde_json::from_value(value).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Status types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseMessageStatus {
    pub status: MessageStatus,
    pub item_hash: String,
    pub reception_time: DateTime<Utc>,
}

/// Pending wire representation — different from the validation-time
/// `pending_messages::BasePendingMessage`; this one is purely a formatting
/// shape (no smart validation).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingMessage {
    pub sender: String,
    pub chain: Chain,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    pub item_type: ItemType,
    pub item_hash: String,
    pub time: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<serde_json::Value>,
    pub reception_time: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingMessageStatus {
    #[serde(default = "MessageStatus::pending_default")]
    pub status: MessageStatus,
    pub item_hash: String,
    pub reception_time: DateTime<Utc>,
    pub messages: Vec<PendingMessage>,
}

impl MessageStatus {
    fn pending_default() -> MessageStatus {
        MessageStatus::Pending
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessedMessageStatus {
    #[serde(default = "ProcessedMessageStatus::default_status")]
    pub status: MessageStatus,
    pub item_hash: String,
    pub reception_time: DateTime<Utc>,
    pub message: AlephMessage,
}

impl ProcessedMessageStatus {
    fn default_status() -> MessageStatus {
        MessageStatus::Processed
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemovingMessageStatus {
    #[serde(default = "RemovingMessageStatus::default_status")]
    pub status: MessageStatus,
    pub item_hash: String,
    pub reception_time: DateTime<Utc>,
    pub message: AlephMessage,
    pub reason: RemovedMessageReason,
}

impl RemovingMessageStatus {
    fn default_status() -> MessageStatus {
        MessageStatus::Removing
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemovedMessageStatus {
    #[serde(default = "RemovedMessageStatus::default_status")]
    pub status: MessageStatus,
    pub item_hash: String,
    pub reception_time: DateTime<Utc>,
    pub message: AlephMessage,
    pub reason: RemovedMessageReason,
}

impl RemovedMessageStatus {
    fn default_status() -> MessageStatus {
        MessageStatus::Removed
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForgottenMessage {
    pub sender: String,
    pub chain: Chain,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub item_type: ItemType,
    pub item_hash: String,
    pub time: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForgottenMessageStatus {
    #[serde(default = "ForgottenMessageStatus::default_status")]
    pub status: MessageStatus,
    pub item_hash: String,
    pub reception_time: DateTime<Utc>,
    pub message: ForgottenMessage,
    pub forgotten_by: Vec<String>,
}

impl ForgottenMessageStatus {
    fn default_status() -> MessageStatus {
        MessageStatus::Forgotten
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RejectedMessageStatus {
    #[serde(default = "RejectedMessageStatus::default_status")]
    pub status: MessageStatus,
    pub item_hash: String,
    pub reception_time: DateTime<Utc>,
    pub message: serde_json::Map<String, serde_json::Value>,
    pub error_code: ErrorCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl RejectedMessageStatus {
    fn default_status() -> MessageStatus {
        MessageStatus::Rejected
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageStatusInfo {
    pub status: MessageStatus,
    pub item_hash: String,
    pub reception_time: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageHashes {
    pub status: MessageStatus,
    pub item_hash: String,
    pub reception_time: DateTime<Utc>,
}

/// Union of message status responses — mirrors `MessageWithStatus`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageWithStatus {
    Pending(PendingMessageStatus),
    Processed(ProcessedMessageStatus),
    Forgotten(ForgottenMessageStatus),
    Rejected(RejectedMessageStatus),
    Removing(RemovingMessageStatus),
    Removed(RemovedMessageStatus),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageListResponse {
    pub messages: Vec<AlephMessage>,
    pub pagination_page: i64,
    pub pagination_total: i64,
    pub pagination_per_page: i64,
    #[serde(default = "MessageListResponse::default_pagination_item")]
    pub pagination_item: String,
    #[serde(
        serialize_with = "serialize_time_as_timestamp",
        deserialize_with = "deserialize_time_from_timestamp"
    )]
    pub time: DateTime<Utc>,
}

impl MessageListResponse {
    fn default_pagination_item() -> String {
        "messages".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_aggregate_message_json() -> serde_json::Value {
        serde_json::json!({
            "sender": "0xa",
            "chain": "ETH",
            "type": "AGGREGATE",
            "item_type": "storage",
            "item_hash": "a".repeat(64),
            "time": 1700000000.0,
            "content": {
                "key": "my_key",
                "address": "0xa",
                "time": 1700000000.0,
                "content": {"foo": "bar"}
            },
            "confirmed": false,
            "confirmations": []
        })
    }

    #[test]
    fn test_aleph_message_aggregate_roundtrip() {
        let json = make_aggregate_message_json();
        let parsed: AlephMessage = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(parsed.message_type(), MessageType::Aggregate);
        let back = serde_json::to_value(&parsed).unwrap();
        // round-trip should preserve the data
        assert_eq!(back["type"], "AGGREGATE");
        assert_eq!(back["item_hash"], "a".repeat(64));
    }

    #[test]
    fn test_message_confirmation_roundtrip() {
        let mc = MessageConfirmation {
            chain: Chain::Ethereum,
            height: 100,
            hash: "0xdead".into(),
        };
        let json = serde_json::to_value(&mc).unwrap();
        let back: MessageConfirmation = serde_json::from_value(json).unwrap();
        assert_eq!(back, mc);
    }

    #[test]
    fn test_processed_message_status_roundtrip() {
        let json = serde_json::json!({
            "status": "processed",
            "item_hash": "a".repeat(64),
            "reception_time": "2024-01-01T00:00:00Z",
            "message": make_aggregate_message_json()
        });
        let parsed: ProcessedMessageStatus = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.status, MessageStatus::Processed);
    }

    #[test]
    fn test_forgotten_message_status_roundtrip() {
        let json = serde_json::json!({
            "status": "forgotten",
            "item_hash": "h".to_string(),
            "reception_time": "2024-01-01T00:00:00Z",
            "message": {
                "sender": "0xa",
                "chain": "ETH",
                "type": "POST",
                "item_type": "storage",
                "item_hash": "h",
                "time": "2024-01-01T00:00:00Z"
            },
            "forgotten_by": ["0xforget"]
        });
        let parsed: ForgottenMessageStatus = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.forgotten_by, vec!["0xforget".to_string()]);
        assert_eq!(parsed.status, MessageStatus::Forgotten);
    }

    #[test]
    fn test_rejected_message_status_roundtrip() {
        let json = serde_json::json!({
            "status": "rejected",
            "item_hash": "h",
            "reception_time": "2024-01-01T00:00:00Z",
            "message": {"foo": "bar"},
            "error_code": 0
        });
        let parsed: RejectedMessageStatus = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.error_code, ErrorCode::InvalidFormat);
    }

    #[test]
    fn test_message_list_response_roundtrip() {
        let resp = MessageListResponse {
            messages: vec![],
            pagination_page: 1,
            pagination_total: 0,
            pagination_per_page: 20,
            pagination_item: "messages".into(),
            time: Utc.timestamp_opt(1700000000, 0).unwrap(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json["time"].is_number());
        let back: MessageListResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back.pagination_item, "messages");
        assert_eq!(back.time.timestamp(), 1700000000);
    }

    #[test]
    fn test_format_message_dict() {
        let json = make_aggregate_message_json();
        let m = format_message_dict(json).unwrap();
        assert_eq!(m.message_type(), MessageType::Aggregate);
    }
}
