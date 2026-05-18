//! Mirrors `src/aleph/schemas/chains/sync_events.py`.

use aleph_types::chain::Chain;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;
use chrono::{DateTime, Utc};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

use crate::types::chain_sync::ChainSyncProtocol;
use crate::types::channel::Channel;

/// Mirrors `OnChainMessage`. The `time` field accepts either a UNIX timestamp
/// or a datetime; we normalise to `f64` seconds, matching `check_time`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnChainMessage {
    pub sender: String,
    pub chain: Chain,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    pub item_type: ItemType,
    pub item_hash: ItemHash,
    #[serde(deserialize_with = "deserialize_time")]
    pub time: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<Channel>,
}

fn deserialize_time<'de, D: Deserializer<'de>>(de: D) -> Result<f64, D::Error> {
    let value = serde_json::Value::deserialize(de)?;
    if let Some(f) = value.as_f64() {
        return Ok(f);
    }
    if let Some(s) = value.as_str() {
        // Try as float string first, then RFC-3339.
        if let Ok(f) = s.parse::<f64>() {
            return Ok(f);
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            let dt = dt.with_timezone(&Utc);
            return Ok(dt.timestamp() as f64 + dt.timestamp_subsec_nanos() as f64 / 1e9);
        }
    }
    // Object form: chrono produces RFC-3339 strings, but be tolerant of structured datetimes.
    Err(serde::de::Error::custom(
        "time must be a number or RFC-3339 string",
    ))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnChainContent {
    pub messages: Vec<OnChainMessage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnChainSyncEventPayload {
    pub protocol: ChainSyncProtocol,
    pub version: i64,
    pub content: OnChainContent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OffChainSyncEventPayload {
    pub protocol: ChainSyncProtocol,
    pub version: i64,
    pub content: String,
}

/// Discriminated union of sync event payloads. Mirrors the `Annotated[Union[...]]`
/// with `discriminator="protocol"` field in pyaleph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "protocol")]
pub enum SyncEventPayload {
    #[serde(rename = "aleph")]
    OnChain {
        version: i64,
        content: OnChainContent,
    },
    #[serde(rename = "aleph-offchain")]
    OffChain { version: i64, content: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_on_chain_sync_event_payload_roundtrip() {
        let json = serde_json::json!({
            "protocol": "aleph",
            "version": 1,
            "content": {
                "messages": [{
                    "sender": "0xa",
                    "chain": "ETH",
                    "type": "POST",
                    "item_type": "storage",
                    "item_hash": "a".repeat(64),
                    "time": 1700000000.0
                }]
            }
        });
        let parsed: SyncEventPayload = serde_json::from_value(json).unwrap();
        match parsed {
            SyncEventPayload::OnChain { version, content } => {
                assert_eq!(version, 1);
                assert_eq!(content.messages.len(), 1);
                assert_eq!(content.messages[0].time, 1700000000.0);
            }
            _ => panic!("expected on-chain"),
        }
    }

    #[test]
    fn test_off_chain_sync_event_payload_roundtrip() {
        let json = serde_json::json!({
            "protocol": "aleph-offchain",
            "version": 1,
            "content": "QmHash"
        });
        let parsed: SyncEventPayload = serde_json::from_value(json).unwrap();
        match parsed {
            SyncEventPayload::OffChain { version, content } => {
                assert_eq!(version, 1);
                assert_eq!(content, "QmHash");
            }
            _ => panic!("expected off-chain"),
        }
    }

    #[test]
    fn test_on_chain_message_time_from_rfc3339() {
        let json = serde_json::json!({
            "sender": "0xa",
            "chain": "ETH",
            "type": "POST",
            "item_type": "storage",
            "item_hash": "a".repeat(64),
            "time": "2024-01-01T00:00:00Z"
        });
        let parsed: OnChainMessage = serde_json::from_value(json).unwrap();
        assert!(parsed.time > 0.0);
    }
}
