//! Pending message table (`pending_messages`).
//!
//! Mirrors `src/aleph/db/models/pending_messages.py`.

use std::cmp::min;

use chrono::{DateTime, Utc};
use serde_json::Value;

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use crate::toolkit::timestamp::{timestamp_to_datetime, utc_now};
use crate::types::channel::Channel;
use crate::types::message_status::MessageOrigin;

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

/// Returns the datetime for the first attempt of a pending message.
///
/// Mirrors `_default_first_attempt_datetime`: if the message time field is in
/// the past, we use it to process historical messages in order. If it's in
/// the future, default to the current time so a forged future-timestamp can't
/// manipulate execution order.
pub fn default_first_attempt_datetime(message_time: DateTime<Utc>) -> DateTime<Utc> {
    min(message_time, utc_now())
}

/// Row of the `pending_messages` table.
#[derive(Debug, Clone)]
pub struct PendingMessageDb {
    pub id: i64,
    pub item_hash: String,
    pub r#type: MessageType,
    pub chain: Chain,
    pub sender: String,
    pub signature: Option<String>,
    pub item_type: ItemType,
    pub item_content: Option<String>,
    pub content: Option<Value>,
    pub time: DateTime<Utc>,
    pub channel: Option<Channel>,
    pub reception_time: DateTime<Utc>,
    pub check_message: bool,
    pub next_attempt: DateTime<Utc>,
    pub retries: i32,
    pub tx_hash: Option<String>,
    pub fetched: bool,
    pub origin: Option<String>,
}

impl PendingMessageDb {
    /// Build a [`PendingMessageDb`] from a database row.
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let type_s: String = row.get("type");
        let chain_s: String = row.get("chain");
        let item_type_s: String = row.get("item_type");
        let channel: Option<String> = row.get("channel");
        Self {
            id: row.get("id"),
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
            reception_time: row.get("reception_time"),
            check_message: row.get("check_message"),
            next_attempt: row.get("next_attempt"),
            retries: row.get("retries"),
            tx_hash: row.get("tx_hash"),
            fetched: row.get("fetched"),
            origin: row.get("origin"),
        }
    }

    /// Build a [`PendingMessageDb`] from a raw API/wire message dict.
    /// Mirrors Python `PendingMessageDb.from_message_dict`.
    pub fn from_message_dict(
        message_dict: &Value,
        reception_time: DateTime<Utc>,
        fetched: bool,
        tx_hash: Option<String>,
        check_message: bool,
        origin: Option<MessageOrigin>,
    ) -> Self {
        let item_hash = message_dict
            .get("item_hash")
            .and_then(|v| v.as_str())
            .expect("item_hash field")
            .to_string();

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
        let message_time = timestamp_to_datetime(time_f);

        let type_s = message_dict
            .get("type")
            .and_then(|v| v.as_str())
            .expect("type field");
        let chain_s = message_dict
            .get("chain")
            .and_then(|v| v.as_str())
            .expect("chain field");
        let item_type_s = message_dict
            .get("item_type")
            .and_then(|v| v.as_str())
            .unwrap_or("inline");
        let sender = message_dict
            .get("sender")
            .and_then(|v| v.as_str())
            .expect("sender field")
            .to_string();
        let signature = message_dict
            .get("signature")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let item_content = message_dict
            .get("item_content")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let channel = message_dict
            .get("channel")
            .and_then(|v| v.as_str())
            .map(|s| Channel::from(s.to_string()));

        Self {
            id: 0,
            item_hash,
            r#type: message_type_from_text(type_s),
            chain: chain_from_text(chain_s),
            sender,
            signature,
            item_type: item_type_from_text(item_type_s),
            item_content,
            content: None,
            time: message_time,
            channel,
            check_message,
            fetched,
            next_attempt: default_first_attempt_datetime(message_time),
            retries: 0,
            tx_hash,
            reception_time,
            origin: Some(origin.unwrap_or(MessageOrigin::P2p).as_str().to_string()),
        }
    }
}

/// Helper trait used only locally to map [`MessageOrigin`] to its
/// stringified representation. Mirrors the Python `MessageOrigin.value`.
trait MessageOriginStr {
    fn as_str(self) -> &'static str;
}

impl MessageOriginStr for MessageOrigin {
    fn as_str(self) -> &'static str {
        match self {
            MessageOrigin::Onchain => "onchain",
            MessageOrigin::P2p => "p2p",
            MessageOrigin::Ipfs => "ipfs",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_first_attempt_past_time() {
        let past = Utc::now() - chrono::Duration::hours(1);
        let first = default_first_attempt_datetime(past);
        assert_eq!(first, past);
    }

    #[test]
    fn default_first_attempt_future_time_clamps() {
        let future = Utc::now() + chrono::Duration::hours(1);
        let before = Utc::now();
        let first = default_first_attempt_datetime(future);
        assert!(first >= before);
        assert!(first < future);
    }

    #[test]
    fn from_message_dict_basic() {
        let payload = json!({
            "item_hash": "deadbeef",
            "type": "POST",
            "chain": "ETH",
            "sender": "0xabc",
            "signature": "0xsig",
            "item_type": "inline",
            "item_content": "{}",
            "time": 1_700_000_000.0,
            "channel": "TEST",
        });
        let reception = Utc::now();
        let p = PendingMessageDb::from_message_dict(
            &payload,
            reception,
            false,
            None,
            true,
            Some(MessageOrigin::P2p),
        );
        assert_eq!(p.item_hash, "deadbeef");
        assert_eq!(p.r#type, MessageType::Post);
        assert_eq!(p.chain, Chain::Ethereum);
        assert_eq!(p.item_type, ItemType::Inline);
        assert_eq!(p.retries, 0);
        assert_eq!(p.origin.as_deref(), Some("p2p"));
        assert_eq!(p.reception_time, reception);
    }
}
