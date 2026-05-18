//! Processed message table (`messages`), plus tag extraction and
//! `message_confirmations`, `forgotten_messages`, `rejected_messages`,
//! `error_codes`, `message_status`.
//!
//! Mirrors `src/aleph/db/models/messages.py`.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use crate::toolkit::timestamp::{timestamp_to_datetime, utc_now};
use crate::types::channel::Channel;
use crate::types::message_status::{ErrorCode, MessageStatus};

use super::pending_messages::PendingMessageDb;

fn chain_from_text(s: &str) -> Chain {
    serde_json::from_value::<Chain>(serde_json::Value::String(s.to_string()))
        .unwrap_or_else(|_| panic!("unknown Chain in DB: {s}"))
}

fn message_type_from_text(s: &str) -> MessageType {
    serde_json::from_value::<MessageType>(serde_json::Value::String(s.to_string()))
        .unwrap_or_else(|_| panic!("unknown MessageType in DB: {s}"))
}

fn item_type_from_text(s: &str) -> ItemType {
    serde_json::from_value::<ItemType>(serde_json::Value::String(s.to_string()))
        .unwrap_or_else(|_| panic!("unknown ItemType in DB: {s}"))
}

fn status_from_text(s: &str) -> MessageStatus {
    serde_json::from_value::<MessageStatus>(serde_json::Value::String(s.to_string()))
        .unwrap_or_else(|_| panic!("unknown MessageStatus in DB: {s}"))
}

/// Column names that exist on the `messages` table but are NOT part of the
/// canonical aleph-message wire format. API responses and any payload bound
/// for an aleph-message validator must strip these — pydantic models
/// (`PostMessage` etc.) use `extra="forbid"` and reject stray columns.
///
/// Mirrors Python `MessageDb.DENORMALIZED_COLUMNS`.
pub const DENORMALIZED_COLUMNS: &[&str] = &[
    "status",
    "reception_time",
    "owner",
    "content_type",
    "content_ref",
    "content_key",
    "content_item_hash",
    "first_confirmed_at",
    "first_confirmed_height",
    "payment_type",
    "tags",
];

/// Pull the tag list out of a content payload.
///
/// Tags live in different keys depending on the message type:
///
/// * `POST` + `AGGREGATE`: `content -> 'content' -> 'tags'`
/// * `STORE`:              `content -> 'tags'`
/// * `INSTANCE`/`PROGRAM`: `content -> 'metadata' -> 'tags'`
///
/// Returns `None` when the message carries no tags so the caller can leave
/// the column NULL, distinguishable from an explicitly empty list. Mirrors
/// Python `extract_tags`.
pub fn extract_tags(message_type: MessageType, content_dict: &Value) -> Option<Vec<String>> {
    let tags = match message_type {
        MessageType::Post | MessageType::Aggregate => {
            content_dict.get("content").and_then(|v| v.get("tags"))
        }
        MessageType::Store => content_dict.get("tags"),
        MessageType::Instance | MessageType::Program => {
            content_dict.get("metadata").and_then(|v| v.get("tags"))
        }
        MessageType::Forget => None,
    };

    let arr = tags?.as_array()?;
    if arr.is_empty() {
        return None;
    }
    let cleaned: Vec<String> = arr
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

/// Same as [`extract_tags`] but accepting the message type as a string. Used
/// when the type only has its wire-format value handy. Mirrors the Python
/// fallback that calls `MessageType(message_type)` on a string.
pub fn extract_tags_from_str(message_type: &str, content_dict: &Value) -> Option<Vec<String>> {
    let mt = serde_json::from_value::<MessageType>(Value::String(message_type.to_string())).ok()?;
    extract_tags(mt, content_dict)
}

/// Row of the `message_status` table (legacy).
#[derive(Debug, Clone)]
pub struct MessageStatusDb {
    pub item_hash: String,
    pub status: MessageStatus,
    pub reception_time: DateTime<Utc>,
}

impl MessageStatusDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let status_s: String = row.get("status");
        Self {
            item_hash: row.get("item_hash"),
            status: status_from_text(&status_s),
            reception_time: row.get("reception_time"),
        }
    }
}

/// Row of the `message_confirmations` association table.
#[derive(Debug, Clone)]
pub struct MessageConfirmationDb {
    pub id: i32,
    pub item_hash: String,
    pub tx_hash: String,
}

impl MessageConfirmationDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            id: row.get("id"),
            item_hash: row.get("item_hash"),
            tx_hash: row.get("tx_hash"),
        }
    }
}

/// Row of the `messages` table.
#[derive(Debug, Clone)]
pub struct MessageDb {
    pub item_hash: String,
    pub r#type: MessageType,
    pub chain: Chain,
    pub sender: String,
    pub signature: Option<String>,
    pub item_type: ItemType,
    pub item_content: Option<String>,
    pub content: Value,
    pub time: DateTime<Utc>,
    pub channel: Option<Channel>,
    pub size: i32,
    // Denormalized columns
    pub status_value: MessageStatus,
    pub reception_time: DateTime<Utc>,
    pub owner: Option<String>,
    pub content_type: Option<String>,
    pub content_ref: Option<String>,
    pub content_key: Option<String>,
    pub first_confirmed_at: Option<DateTime<Utc>>,
    pub first_confirmed_height: Option<i64>,
    pub payment_type: Option<String>,
    pub content_item_hash: Option<String>,
    pub tags: Option<Vec<String>>,
}

impl MessageDb {
    /// Build a [`MessageDb`] from a database row.
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let type_s: String = row.get("type");
        let chain_s: String = row.get("chain");
        let item_type_s: String = row.get("item_type");
        let status_s: String = row.get("status");
        let channel: Option<String> = row.get("channel");

        Self {
            item_hash: row.get("item_hash"),
            r#type: message_type_from_text(&type_s),
            chain: chain_from_text(&chain_s),
            sender: row.get("sender"),
            signature: row.get("signature"),
            item_type: item_type_from_text(&item_type_s),
            item_content: row.get("item_content"),
            content: row.get("content"),
            time: row.get("time"),
            channel: channel.map(Channel::from),
            size: row.get("size"),
            status_value: status_from_text(&status_s),
            reception_time: row.get("reception_time"),
            owner: row.get("owner"),
            content_type: row.get("content_type"),
            content_ref: row.get("content_ref"),
            content_key: row.get("content_key"),
            first_confirmed_at: row.get("first_confirmed_at"),
            first_confirmed_height: row.get("first_confirmed_height"),
            payment_type: row.get("payment_type"),
            content_item_hash: row.get("content_item_hash"),
            tags: row.get("tags"),
        }
    }

    /// Coerce content fields the way Python's `_coerce_content` does. Fills
    /// `address` / `time` from the pending message when missing.
    pub fn coerce_content(pending_message: &PendingMessageDb, content_dict: &mut Value) {
        let obj = match content_dict.as_object_mut() {
            Some(m) => m,
            None => return,
        };
        if obj.get("address").map(|v| v.is_null()).unwrap_or(true) {
            obj.insert(
                "address".to_string(),
                Value::String(pending_message.sender.clone()),
            );
        }
        if obj.get("time").map(|v| v.is_null()).unwrap_or(true) {
            // Convert chrono DateTime<Utc> back to a POSIX timestamp with sub-second precision.
            let ts = pending_message.time.timestamp() as f64
                + (pending_message.time.timestamp_subsec_nanos() as f64) / 1_000_000_000.0;
            obj.insert(
                "time".to_string(),
                serde_json::Number::from_f64(ts)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
            );
        }
    }

    /// Build a [`MessageDb`] from a [`PendingMessageDb`] plus the decoded
    /// content payload. Mirrors Python `MessageDb.from_pending_message`.
    ///
    /// The payment type derivation mirrors Python: `credit` if `payment.type
    /// == "credit"`, `superfluid` if `payment.type == "superfluid"`, else
    /// `hold` whenever a `payment` block is present.
    pub fn from_pending_message(
        pending_message: &PendingMessageDb,
        content_dict: &Value,
        content_size: i32,
        reception_time: Option<DateTime<Utc>>,
    ) -> Self {
        let reception_time = reception_time.unwrap_or(pending_message.reception_time);

        let mut content = content_dict.clone();
        Self::coerce_content(pending_message, &mut content);

        let payment_type = match content.get("payment") {
            Some(p) if p.is_object() => {
                let p_type = p.get("type").and_then(|v| v.as_str()).unwrap_or("");
                let resolved = match p_type {
                    "credit" => "credit",
                    "superfluid" => "superfluid",
                    _ => "hold",
                };
                Some(resolved.to_string())
            }
            _ => None,
        };

        let owner = content
            .get("address")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let content_type = content
            .get("type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let content_ref = content
            .get("ref")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let content_key = content
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let content_item_hash = content
            .get("item_hash")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = extract_tags(pending_message.r#type, &content);

        Self {
            item_hash: pending_message.item_hash.clone(),
            r#type: pending_message.r#type,
            chain: pending_message.chain.clone(),
            sender: pending_message.sender.clone(),
            signature: pending_message.signature.clone(),
            item_type: pending_message.item_type,
            item_content: pending_message.item_content.clone(),
            content,
            time: pending_message.time,
            channel: pending_message.channel.clone(),
            size: content_size,
            status_value: MessageStatus::Processed,
            reception_time,
            owner,
            content_type,
            content_ref,
            content_key,
            content_item_hash,
            payment_type,
            tags,
            first_confirmed_at: None,
            first_confirmed_height: None,
        }
    }

    /// Build a [`MessageDb`] from the JSON-dict form returned by the API.
    /// Mirrors Python `MessageDb.from_message_dict`. Denormalized columns
    /// are auto-populated from the JSONB `content` (mirrors Python
    /// `__init__` defaults).
    pub fn from_message_dict(message_dict: &Value) -> Self {
        let item_hash = message_dict
            .get("item_hash")
            .and_then(|v| v.as_str())
            .expect("item_hash field")
            .to_string();
        let type_s = message_dict
            .get("type")
            .and_then(|v| v.as_str())
            .expect("type field");
        let chain_s = message_dict
            .get("chain")
            .and_then(|v| v.as_str())
            .expect("chain field");
        let sender = message_dict
            .get("sender")
            .and_then(|v| v.as_str())
            .expect("sender field")
            .to_string();
        let signature = message_dict
            .get("signature")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let item_type_s = message_dict
            .get("item_type")
            .and_then(|v| v.as_str())
            .unwrap_or("inline");
        let item_content = message_dict
            .get("item_content")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let content = message_dict
            .get("content")
            .cloned()
            .unwrap_or(Value::Object(Map::new()));
        let time_f = message_dict
            .get("time")
            .and_then(|v| v.as_f64())
            .or_else(|| {
                message_dict
                    .get("time")
                    .and_then(|v| v.as_i64())
                    .map(|i| i as f64)
            })
            .expect("time field");
        let channel = message_dict
            .get("channel")
            .and_then(|v| v.as_str())
            .map(|s| Channel::from(s.to_string()));
        let size = message_dict
            .get("size")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;

        let r#type = message_type_from_text(type_s);
        let chain = chain_from_text(chain_s);
        let item_type = item_type_from_text(item_type_s);

        // Apply the same defaults the Python __init__ does for denormalized
        // columns derived from the JSONB content.
        let (owner, content_type, content_ref, content_key, content_item_hash, payment_type, tags) =
            if let Some(obj) = content.as_object() {
                let payment_type = obj.get("payment").and_then(|p| {
                    if let Some(t) = p.get("type").and_then(|v| v.as_str()) {
                        if t.is_empty() {
                            None
                        } else {
                            Some(t.to_string())
                        }
                    } else {
                        None
                    }
                });
                (
                    obj.get("address")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    obj.get("type")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    obj.get("ref")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    obj.get("key")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    obj.get("item_hash")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    payment_type,
                    extract_tags(r#type, &content),
                )
            } else {
                (None, None, None, None, None, None, None)
            };

        Self {
            item_hash,
            r#type,
            chain,
            sender,
            signature,
            item_type,
            item_content,
            content,
            time: timestamp_to_datetime(time_f),
            channel,
            size,
            status_value: MessageStatus::Processed,
            reception_time: utc_now(),
            owner,
            content_type,
            content_ref,
            content_key,
            content_item_hash,
            payment_type,
            tags,
            first_confirmed_at: None,
            first_confirmed_height: None,
        }
    }
}

/// Row of the `forgotten_messages` table.
#[derive(Debug, Clone)]
pub struct ForgottenMessageDb {
    pub item_hash: String,
    pub r#type: MessageType,
    pub chain: Chain,
    pub sender: String,
    pub signature: Option<String>,
    pub item_type: ItemType,
    pub time: DateTime<Utc>,
    pub channel: Option<Channel>,
    pub forgotten_by: Vec<String>,
}

impl ForgottenMessageDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let type_s: String = row.get("type");
        let chain_s: String = row.get("chain");
        let item_type_s: String = row.get("item_type");
        let channel: Option<String> = row.get("channel");
        Self {
            item_hash: row.get("item_hash"),
            r#type: message_type_from_text(&type_s),
            chain: chain_from_text(&chain_s),
            sender: row.get("sender"),
            signature: row.get("signature"),
            item_type: item_type_from_text(&item_type_s),
            time: row.get("time"),
            channel: channel.map(Channel::from),
            forgotten_by: row.get("forgotten_by"),
        }
    }
}

/// Row of the `error_codes` table.
#[derive(Debug, Clone)]
pub struct ErrorCodeDb {
    pub code: i32,
    pub description: String,
}

impl ErrorCodeDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            code: row.get("code"),
            description: row.get("description"),
        }
    }
}

/// Row of the `rejected_messages` table.
#[derive(Debug, Clone)]
pub struct RejectedMessageDb {
    pub item_hash: String,
    pub message: Value,
    pub error_code: ErrorCode,
    pub details: Option<Value>,
    pub traceback: Option<String>,
    pub tx_hash: Option<String>,
}

impl RejectedMessageDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let code: i32 = row.get("error_code");
        let error_code =
            ErrorCode::try_from(code).unwrap_or_else(|_| panic!("unknown ErrorCode: {code}"));
        Self {
            item_hash: row.get("item_hash"),
            message: row.get("message"),
            error_code,
            details: row.get("details"),
            traceback: row.get("traceback"),
            tx_hash: row.get("tx_hash"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn denormalized_columns_match_python() {
        // Sanity-check the set matches the Python frozenset exactly.
        let expected = [
            "status",
            "reception_time",
            "owner",
            "content_type",
            "content_ref",
            "content_key",
            "content_item_hash",
            "first_confirmed_at",
            "first_confirmed_height",
            "payment_type",
            "tags",
        ];
        assert_eq!(DENORMALIZED_COLUMNS.len(), expected.len());
        for col in expected {
            assert!(DENORMALIZED_COLUMNS.contains(&col), "missing: {col}");
        }
    }

    #[test]
    fn extract_tags_post_aggregate_path() {
        // POST + AGGREGATE: tags live at content.content.tags
        let post = json!({"content": {"tags": ["a", "b"]}});
        assert_eq!(
            extract_tags(MessageType::Post, &post),
            Some(vec!["a".into(), "b".into()])
        );
        let agg = json!({"content": {"tags": ["x"]}});
        assert_eq!(
            extract_tags(MessageType::Aggregate, &agg),
            Some(vec!["x".into()])
        );

        // Missing inner content -> None
        assert!(extract_tags(MessageType::Post, &json!({})).is_none());
        // Empty list -> None
        assert!(extract_tags(MessageType::Post, &json!({"content": {"tags": []}})).is_none());
    }

    #[test]
    fn extract_tags_store_path() {
        // STORE: tags live at content.tags
        let store = json!({"tags": ["s1", "s2"]});
        assert_eq!(
            extract_tags(MessageType::Store, &store),
            Some(vec!["s1".into(), "s2".into()])
        );
        // No tags
        assert!(extract_tags(MessageType::Store, &json!({})).is_none());
    }

    #[test]
    fn extract_tags_instance_program_path() {
        // INSTANCE / PROGRAM: tags live at content.metadata.tags
        let inst = json!({"metadata": {"tags": ["k8s", "gpu"]}});
        assert_eq!(
            extract_tags(MessageType::Instance, &inst),
            Some(vec!["k8s".into(), "gpu".into()])
        );
        let prog = json!({"metadata": {"tags": ["py"]}});
        assert_eq!(
            extract_tags(MessageType::Program, &prog),
            Some(vec!["py".into()])
        );
        // No metadata
        assert!(extract_tags(MessageType::Instance, &json!({})).is_none());
        // Metadata isn't a dict
        assert!(extract_tags(MessageType::Instance, &json!({"metadata": []})).is_none());
    }

    #[test]
    fn extract_tags_filters_non_strings() {
        let c = json!({"tags": ["good", 1, true, "ok"]});
        assert_eq!(
            extract_tags(MessageType::Store, &c),
            Some(vec!["good".into(), "ok".into()])
        );
        let only_bad = json!({"tags": [1, 2, 3]});
        assert!(extract_tags(MessageType::Store, &only_bad).is_none());
    }

    #[test]
    fn extract_tags_forget_returns_none() {
        let c = json!({"content": {"tags": ["x"]}, "tags": ["y"], "metadata": {"tags": ["z"]}});
        assert!(extract_tags(MessageType::Forget, &c).is_none());
    }

    #[test]
    fn extract_tags_from_str_unknown_type() {
        let c = json!({"tags": ["a"]});
        assert!(extract_tags_from_str("INVALID", &c).is_none());
        assert_eq!(extract_tags_from_str("STORE", &c), Some(vec!["a".into()]));
    }

    #[test]
    fn from_message_dict_derives_denormalized_fields() {
        let payload = json!({
            "item_hash": "deadbeef",
            "type": "POST",
            "chain": "ETH",
            "sender": "0xabc",
            "signature": "0xsig",
            "item_type": "inline",
            "time": 1_700_000_000.0,
            "channel": "TEST",
            "size": 123,
            "content": {
                "address": "0xowner",
                "type": "amend",
                "ref": "0xref",
                "content": {"tags": ["t1", "t2"]},
                "payment": {"type": "credit"},
            },
        });
        let m = MessageDb::from_message_dict(&payload);
        assert_eq!(m.item_hash, "deadbeef");
        assert_eq!(m.r#type, MessageType::Post);
        assert_eq!(m.chain, Chain::Ethereum);
        assert_eq!(m.size, 123);
        assert_eq!(m.owner.as_deref(), Some("0xowner"));
        assert_eq!(m.content_type.as_deref(), Some("amend"));
        assert_eq!(m.content_ref.as_deref(), Some("0xref"));
        assert_eq!(m.payment_type.as_deref(), Some("credit"));
        assert_eq!(m.tags, Some(vec!["t1".into(), "t2".into()]));
        assert_eq!(m.status_value, MessageStatus::Processed);
    }

    #[test]
    fn from_pending_message_coerces_address_and_time() {
        use crate::types::message_status::MessageOrigin;

        let now = Utc::now();
        let pending = PendingMessageDb {
            id: 0,
            item_hash: "deadbeef".into(),
            r#type: MessageType::Post,
            chain: Chain::Ethereum,
            sender: "0xsender".into(),
            signature: Some("0xsig".into()),
            item_type: ItemType::Inline,
            item_content: Some("{}".into()),
            content: None,
            time: now,
            channel: None,
            reception_time: now,
            check_message: true,
            next_attempt: now,
            retries: 0,
            tx_hash: None,
            fetched: false,
            origin: Some(
                serde_json::to_value(MessageOrigin::P2p)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string(),
            ),
        };

        // content_dict omits address/time -> should be back-filled from pending
        let content = json!({"content": {"tags": ["t"]}, "payment": {"type": "superfluid"}});
        let m = MessageDb::from_pending_message(&pending, &content, 100, None);
        assert_eq!(m.size, 100);
        assert_eq!(m.payment_type.as_deref(), Some("superfluid"));
        assert_eq!(m.tags, Some(vec!["t".into()]));
        // address coerced
        assert_eq!(
            m.content.get("address").and_then(|v| v.as_str()),
            Some("0xsender")
        );
        // time coerced
        assert!(m.content.get("time").and_then(|v| v.as_f64()).is_some());
        assert_eq!(m.owner.as_deref(), Some("0xsender"));
        assert_eq!(m.status_value, MessageStatus::Processed);
    }

    #[test]
    fn from_pending_message_payment_type_hold_when_neither_credit_nor_stream() {
        use chrono::TimeZone;
        let t = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let pending = PendingMessageDb {
            id: 0,
            item_hash: "h".into(),
            r#type: MessageType::Store,
            chain: Chain::Ethereum,
            sender: "0xsender".into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content: None,
            time: t,
            channel: None,
            reception_time: t,
            check_message: false,
            next_attempt: t,
            retries: 0,
            tx_hash: None,
            fetched: true,
            origin: None,
        };
        let content = json!({"payment": {"type": "whatever"}, "tags": ["x"]});
        let m = MessageDb::from_pending_message(&pending, &content, 10, None);
        assert_eq!(m.payment_type.as_deref(), Some("hold"));
        assert_eq!(m.tags, Some(vec!["x".into()]));
    }

    #[test]
    fn from_pending_message_no_payment_keeps_none() {
        use chrono::TimeZone;
        let t = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let pending = PendingMessageDb {
            id: 0,
            item_hash: "h".into(),
            r#type: MessageType::Aggregate,
            chain: Chain::Ethereum,
            sender: "0xsender".into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content: None,
            time: t,
            channel: None,
            reception_time: t,
            check_message: false,
            next_attempt: t,
            retries: 0,
            tx_hash: None,
            fetched: true,
            origin: None,
        };
        let content = json!({"content": {"tags": ["a"]}});
        let m = MessageDb::from_pending_message(&pending, &content, 10, None);
        assert!(m.payment_type.is_none());
        assert_eq!(m.tags, Some(vec!["a".into()]));
    }
}
