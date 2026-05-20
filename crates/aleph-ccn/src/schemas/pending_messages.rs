//! Mirrors `src/aleph/schemas/pending_messages.py`.
//!
//! Schemas representing raw messages received from users on the Aleph network
//! before they are processed/stored. Each message variant carries a typed
//! `content` field decoded from the JSON `item_content` payload.

use std::marker::PhantomData;

use aleph_types::chain::Chain;
use aleph_types::message::item_type::ItemType;
use aleph_types::message::{
    AggregateContent, ForgetContent, InstanceContent, MessageType, PostContent, ProgramContent,
    StoreContent,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::schemas::base_messages::{BaseMessageError, item_type_from_hash};

pub const MAX_INLINE_SIZE: usize = 200_000;

/// Errors raised while parsing pending messages, mirroring `InvalidMessageFormat`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PendingMessageError {
    #[error("Message is not a dictionary")]
    NotAnObject,
    #[error("Invalid message_type: '{0}'")]
    InvalidMessageType(String),
    #[error("Could not determine item hash")]
    MissingItemHash,
    #[error("Item content not specified for inline item type")]
    MissingInlineContent,
    #[error("Message too long")]
    InlineContentTooLong,
    #[error("Message content is not valid JSON data")]
    InvalidJson,
    #[error("{0:?} messages cannot define item_content")]
    UnexpectedItemContent(ItemType),
    #[error("Unexpected hash type: '{0}'")]
    UnknownHashType(String),
    #[error("Missing required field: '{0}'")]
    MissingField(&'static str),
    #[error("Field validation error: {0}")]
    FieldError(String),
    #[error(transparent)]
    BaseMessage(#[from] BaseMessageError),
}

/// Resolved item_type plus parsed content for a pending message. Mirrors
/// `base_pending_message_load_content`.
struct LoadedContent {
    item_type: ItemType,
    item_content: Option<String>,
    /// `content` is `None` for storage/ipfs item types (filled in later by the
    /// CCN) and `Some(value)` for inline items.
    content: Option<serde_json::Value>,
}

fn load_content(
    item_hash: Option<&str>,
    input_item_type: Option<ItemType>,
    item_content: Option<String>,
) -> Result<LoadedContent, PendingMessageError> {
    let item_hash = item_hash.ok_or(PendingMessageError::MissingItemHash)?;

    let default_item_type = if item_content.is_none() {
        item_type_from_hash(item_hash)
            .map_err(|_| PendingMessageError::UnknownHashType(item_hash.to_string()))?
    } else {
        ItemType::Inline
    };

    let item_type = input_item_type.unwrap_or(default_item_type);

    if item_type == ItemType::Inline {
        let raw = item_content
            .as_deref()
            .ok_or(PendingMessageError::MissingInlineContent)?;

        if raw.len() > MAX_INLINE_SIZE {
            return Err(PendingMessageError::InlineContentTooLong);
        }

        let parsed = serde_json::from_str::<serde_json::Value>(raw)
            .map_err(|_| PendingMessageError::InvalidJson)?;

        Ok(LoadedContent {
            item_type,
            item_content,
            content: Some(parsed),
        })
    } else {
        if item_content.is_some() {
            return Err(PendingMessageError::UnexpectedItemContent(item_type));
        }
        Ok(LoadedContent {
            item_type,
            item_content,
            content: None,
        })
    }
}

/// Mirrors `base_pending_message_validator_check_time` from pyaleph. Accepts
/// either a UNIX epoch (float / int) or an RFC-3339 datetime string.
fn parse_time_value(value: &serde_json::Value) -> Result<DateTime<Utc>, PendingMessageError> {
    use chrono::TimeZone;
    if let Some(secs) = value.as_f64() {
        let s = secs.floor() as i64;
        let ns = ((secs - secs.floor()) * 1_000_000_000.0).round() as u32;
        return chrono::Utc
            .timestamp_opt(s, ns)
            .single()
            .ok_or_else(|| PendingMessageError::FieldError("invalid timestamp".to_string()));
    }
    if let Some(s) = value.as_str() {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(dt.with_timezone(&Utc));
        }
        if let Ok(f) = s.parse::<f64>() {
            let secs = f.floor() as i64;
            let ns = ((f - f.floor()) * 1_000_000_000.0).round() as u32;
            return chrono::Utc
                .timestamp_opt(secs, ns)
                .single()
                .ok_or_else(|| PendingMessageError::FieldError("invalid timestamp".to_string()));
        }
    }
    Err(PendingMessageError::FieldError(
        "time field must be a number or RFC-3339 string".to_string(),
    ))
}

/// A pending (unprocessed) Aleph message. Generic over the concrete content type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BasePendingMessageStruct<MType, ContentType> {
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
    #[serde(default = "Option::default", skip_serializing_if = "Option::is_none")]
    pub content: Option<ContentType>,
    #[serde(skip)]
    _mtype: PhantomData<MType>,
}

impl<MType, ContentType> BasePendingMessageStruct<MType, ContentType> {
    pub fn new(
        sender: String,
        chain: Chain,
        signature: Option<String>,
        message_type: MessageType,
        item_content: Option<String>,
        item_type: ItemType,
        item_hash: String,
        time: DateTime<Utc>,
        channel: Option<String>,
        content: Option<ContentType>,
    ) -> Self {
        Self {
            sender,
            chain,
            signature,
            message_type,
            item_content,
            item_type,
            item_hash,
            time,
            channel,
            content,
            _mtype: PhantomData,
        }
    }
}

// -- concrete typed pending messages, one per MessageType ---------------------

pub type PendingAggregateMessage = BasePendingMessageStruct<Aggregate, AggregateContent>;
pub type PendingForgetMessage = BasePendingMessageStruct<Forget, ForgetContent>;
pub type PendingInstanceMessage = BasePendingMessageStruct<Instance, InstanceContent>;
pub type PendingPostMessage = BasePendingMessageStruct<Post, PostContent>;
pub type PendingProgramMessage = BasePendingMessageStruct<Program, ProgramContent>;
pub type PendingStoreMessage = BasePendingMessageStruct<Store, StoreContent>;

/// Marker types to thread MessageType into a generic.
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

/// Pending store message restricted to inline item types — Python
/// `PendingInlineStoreMessage`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingInlineStoreMessage {
    pub sender: String,
    pub chain: Chain,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub item_content: String,
    pub item_type: ItemType,
    pub item_hash: String,
    pub time: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<StoreContent>,
}

impl PendingInlineStoreMessage {
    pub fn validate(&self) -> Result<(), PendingMessageError> {
        if self.message_type != MessageType::Store {
            return Err(PendingMessageError::FieldError(
                "type must be STORE".to_string(),
            ));
        }
        if self.item_type != ItemType::Inline {
            return Err(PendingMessageError::FieldError(
                "item_type must be inline".to_string(),
            ));
        }
        Ok(())
    }
}

/// Tagged enum carrying the result of `parse_message`. Variants follow the
/// MessageType discriminator.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum BasePendingMessage {
    Aggregate(PendingAggregateMessage),
    Forget(PendingForgetMessage),
    Instance(PendingInstanceMessage),
    Post(PendingPostMessage),
    Program(PendingProgramMessage),
    Store(PendingStoreMessage),
}

impl BasePendingMessage {
    pub fn message_type(&self) -> MessageType {
        match self {
            BasePendingMessage::Aggregate(_) => MessageType::Aggregate,
            BasePendingMessage::Forget(_) => MessageType::Forget,
            BasePendingMessage::Instance(_) => MessageType::Instance,
            BasePendingMessage::Post(_) => MessageType::Post,
            BasePendingMessage::Program(_) => MessageType::Program,
            BasePendingMessage::Store(_) => MessageType::Store,
        }
    }

    pub fn item_hash(&self) -> &str {
        match self {
            BasePendingMessage::Aggregate(m) => &m.item_hash,
            BasePendingMessage::Forget(m) => &m.item_hash,
            BasePendingMessage::Instance(m) => &m.item_hash,
            BasePendingMessage::Post(m) => &m.item_hash,
            BasePendingMessage::Program(m) => &m.item_hash,
            BasePendingMessage::Store(m) => &m.item_hash,
        }
    }

    pub fn sender(&self) -> &str {
        match self {
            BasePendingMessage::Aggregate(m) => &m.sender,
            BasePendingMessage::Forget(m) => &m.sender,
            BasePendingMessage::Instance(m) => &m.sender,
            BasePendingMessage::Post(m) => &m.sender,
            BasePendingMessage::Program(m) => &m.sender,
            BasePendingMessage::Store(m) => &m.sender,
        }
    }

    pub fn item_type(&self) -> ItemType {
        match self {
            BasePendingMessage::Aggregate(m) => m.item_type,
            BasePendingMessage::Forget(m) => m.item_type,
            BasePendingMessage::Instance(m) => m.item_type,
            BasePendingMessage::Post(m) => m.item_type,
            BasePendingMessage::Program(m) => m.item_type,
            BasePendingMessage::Store(m) => m.item_type,
        }
    }
}

fn get_str_field(
    map: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<String, PendingMessageError> {
    map.get(field)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or(PendingMessageError::MissingField(field))
}

fn get_optional_str(
    map: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<String> {
    map.get(field).and_then(|v| {
        if v.is_null() {
            None
        } else {
            v.as_str().map(|s| s.to_string())
        }
    })
}

fn get_chain(
    map: &serde_json::Map<String, serde_json::Value>,
) -> Result<Chain, PendingMessageError> {
    let v = map
        .get("chain")
        .ok_or(PendingMessageError::MissingField("chain"))?;
    serde_json::from_value::<Chain>(v.clone())
        .map_err(|e| PendingMessageError::FieldError(format!("chain: {e}")))
}

fn get_item_type(
    map: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<ItemType>, PendingMessageError> {
    match map.get("item_type") {
        Some(v) if !v.is_null() => serde_json::from_value::<ItemType>(v.clone())
            .map(Some)
            .map_err(|e| PendingMessageError::FieldError(format!("item_type: {e}"))),
        _ => Ok(None),
    }
}

fn assemble<MType, ContentType>(
    map: serde_json::Map<String, serde_json::Value>,
    message_type: MessageType,
) -> Result<BasePendingMessageStruct<MType, ContentType>, PendingMessageError>
where
    ContentType: for<'de> Deserialize<'de>,
{
    let sender = get_str_field(&map, "sender")?;
    let chain = get_chain(&map)?;
    let signature = get_optional_str(&map, "signature");
    let item_hash = get_str_field(&map, "item_hash")?;
    let channel = get_optional_str(&map, "channel");
    let item_content = get_optional_str(&map, "item_content");
    let input_item_type = get_item_type(&map)?;
    let time_value = map
        .get("time")
        .cloned()
        .ok_or(PendingMessageError::MissingField("time"))?;
    let time = parse_time_value(&time_value)?;

    let loaded = load_content(Some(&item_hash), input_item_type, item_content)?;

    let content = match &loaded.content {
        Some(v) => Some(
            serde_json::from_value::<ContentType>(v.clone())
                .map_err(|e| PendingMessageError::FieldError(format!("content: {e}")))?,
        ),
        None => None,
    };

    Ok(BasePendingMessageStruct {
        sender,
        chain,
        signature,
        message_type,
        item_content: loaded.item_content,
        item_type: loaded.item_type,
        item_hash,
        time,
        channel,
        content,
        _mtype: PhantomData,
    })
}

/// Selects the correct variant based on `type` and parses the message body.
/// Mirrors `parse_message` in `pending_messages.py`.
pub fn parse_message(value: serde_json::Value) -> Result<BasePendingMessage, PendingMessageError> {
    let map = match value {
        serde_json::Value::Object(m) => m,
        _ => return Err(PendingMessageError::NotAnObject),
    };

    let raw_type = map
        .get("type")
        .cloned()
        .ok_or(PendingMessageError::MissingField("type"))?;

    let message_type: MessageType = serde_json::from_value(raw_type.clone())
        .map_err(|_| PendingMessageError::InvalidMessageType(raw_type.to_string()))?;

    let pending = match message_type {
        MessageType::Aggregate => BasePendingMessage::Aggregate(assemble(map, message_type)?),
        MessageType::Forget => BasePendingMessage::Forget(assemble(map, message_type)?),
        MessageType::Instance => BasePendingMessage::Instance(assemble(map, message_type)?),
        MessageType::Post => BasePendingMessage::Post(assemble(map, message_type)?),
        MessageType::Program => BasePendingMessage::Program(assemble(map, message_type)?),
        MessageType::Store => BasePendingMessage::Store(assemble(map, message_type)?),
    };
    Ok(pending)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha_hex(input: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(input.as_bytes());
        format!("{:x}", h.finalize())
    }

    #[test]
    fn test_parse_post_message_inline() {
        let inner = serde_json::json!({
            "address": "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10",
            "time": 1700000000.0,
            "type": "amend",
            "ref": "abc",
            "content": {"body": "Hello"}
        });
        let inner_str = serde_json::to_string(&inner).unwrap();
        let hash = sha_hex(&inner_str);

        let msg_json = serde_json::json!({
            "sender": "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10",
            "chain": "ETH",
            "signature": "0xdead",
            "type": "POST",
            "item_type": "inline",
            "item_content": inner_str,
            "item_hash": hash,
            "time": 1700000000.0,
            "channel": "TEST"
        });

        let parsed = parse_message(msg_json).unwrap();
        assert_eq!(parsed.message_type(), MessageType::Post);
        let post = match parsed {
            BasePendingMessage::Post(p) => p,
            _ => panic!("expected post"),
        };
        assert_eq!(post.sender, "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");
        assert_eq!(post.chain, Chain::Ethereum);
        let content = post.content.as_ref().unwrap();
        assert!(content.is_amend());
        assert_eq!(post.item_type, ItemType::Inline);
    }

    #[test]
    fn test_parse_message_invalid_type_rejected() {
        let json = serde_json::json!({
            "sender": "x",
            "chain": "ETH",
            "type": "BOGUS",
            "item_type": "inline",
            "item_content": "{}",
            "item_hash": "ignored",
            "time": 0.0
        });
        let err = parse_message(json).unwrap_err();
        assert!(matches!(err, PendingMessageError::InvalidMessageType(_)));
    }

    #[test]
    fn test_parse_message_non_object_rejected() {
        let err = parse_message(serde_json::json!([])).unwrap_err();
        assert!(matches!(err, PendingMessageError::NotAnObject));
    }

    #[test]
    fn test_parse_message_storage_does_not_load_content() {
        let json = serde_json::json!({
            "sender": "x",
            "chain": "ETH",
            "type": "AGGREGATE",
            "item_type": "storage",
            "item_hash": "a".repeat(64),
            "time": 0.0
        });
        let parsed = parse_message(json).unwrap();
        match parsed {
            BasePendingMessage::Aggregate(m) => {
                assert!(m.content.is_none());
                assert_eq!(m.item_type, ItemType::Storage);
            }
            _ => panic!("expected aggregate"),
        }
    }

    #[test]
    fn test_parse_message_inline_too_long_rejected() {
        let big = "0".repeat(MAX_INLINE_SIZE + 1);
        let json = serde_json::json!({
            "sender": "x",
            "chain": "ETH",
            "type": "AGGREGATE",
            "item_type": "inline",
            "item_content": big,
            "item_hash": "ignored",
            "time": 0.0
        });
        let err = parse_message(json).unwrap_err();
        assert!(matches!(err, PendingMessageError::InlineContentTooLong));
    }

    #[test]
    fn test_inline_store_validate() {
        let m = PendingInlineStoreMessage {
            sender: "x".into(),
            chain: Chain::Ethereum,
            signature: None,
            message_type: MessageType::Store,
            item_content: "{}".into(),
            item_type: ItemType::Inline,
            item_hash: "h".into(),
            time: Utc::now(),
            channel: None,
            content: None,
        };
        m.validate().unwrap();
    }

    #[test]
    fn test_inline_store_validate_wrong_type() {
        let m = PendingInlineStoreMessage {
            sender: "x".into(),
            chain: Chain::Ethereum,
            signature: None,
            message_type: MessageType::Post,
            item_content: "{}".into(),
            item_type: ItemType::Inline,
            item_hash: "h".into(),
            time: Utc::now(),
            channel: None,
            content: None,
        };
        assert!(m.validate().is_err());
    }
}
