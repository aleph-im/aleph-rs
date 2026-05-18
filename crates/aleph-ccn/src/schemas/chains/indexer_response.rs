//! Mirrors `src/aleph/schemas/chains/indexer_response.py`.

use chrono::{DateTime, Utc};
use serde::de::{Deserializer, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexerBlockchain {
    Bsc,
    Ethereum,
    Solana,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntityType {
    Block,
    Transaction,
    Log,
    State,
}

/// A pair of `(start, end)` datetimes. Mirrors `DateTimeRange`. Accepts either
/// `"<start>/<end>"` strings or 2-tuples.
#[derive(Debug, Clone, PartialEq)]
pub struct DateTimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl Serialize for DateTimeRange {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut tup = serializer.serialize_tuple(2)?;
        tup.serialize_element(&self.start)?;
        tup.serialize_element(&self.end)?;
        tup.end()
    }
}

impl<'de> Deserialize<'de> for DateTimeRange {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = DateTimeRange;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a 2-element tuple of datetimes or a 'start/end' RFC-3339 string")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                let (start_s, end_s) = v
                    .split_once('/')
                    .ok_or_else(|| E::custom("expected 'start/end' format"))?;
                let start = DateTime::parse_from_rfc3339(start_s)
                    .map_err(E::custom)?
                    .with_timezone(&Utc);
                let end = DateTime::parse_from_rfc3339(end_s)
                    .map_err(E::custom)?
                    .with_timezone(&Utc);
                Ok(DateTimeRange { start, end })
            }

            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
                self.visit_str(&v)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let start: DateTime<Utc> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("missing start datetime"))?;
                let end: DateTime<Utc> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("missing end datetime"))?;
                Ok(DateTimeRange { start, end })
            }
        }

        deserializer.deserialize_any(V)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountEntityState {
    pub blockchain: IndexerBlockchain,
    #[serde(rename = "type")]
    pub entity_type: EntityType,
    pub indexer: String,
    pub account: String,
    #[serde(rename = "completeHistory")]
    pub complete_history: bool,
    pub progress: f64,
    pub pending: Vec<DateTimeRange>,
    pub processed: Vec<DateTimeRange>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerAccountStateResponseData {
    pub state: Vec<AccountEntityState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerAccountStateResponse {
    pub data: IndexerAccountStateResponseData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerEvent {
    pub id: String,
    pub timestamp: f64,
    pub address: String,
    pub height: i64,
    pub transaction: String,
}

impl IndexerEvent {
    /// `timestamp / 1000` — Python `timestamp_seconds` property.
    pub fn timestamp_seconds(&self) -> f64 {
        self.timestamp / 1000.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageEvent {
    #[serde(flatten)]
    pub event: IndexerEvent,
    #[serde(rename = "type")]
    pub message_type: String,
    pub content: String,
}

impl MessageEvent {
    pub fn timestamp_seconds(&self) -> f64 {
        self.event.timestamp_seconds()
    }

    pub fn address(&self) -> &str {
        &self.event.address
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncEvent {
    #[serde(flatten)]
    pub event: IndexerEvent,
    pub message: String,
}

impl SyncEvent {
    pub fn timestamp_seconds(&self) -> f64 {
        self.event.timestamp_seconds()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerEventResponseData {
    #[serde(rename = "messageEvents", alias = "message_events", default)]
    pub message_events: Vec<MessageEvent>,
    #[serde(rename = "syncEvents", alias = "sync_events", default)]
    pub sync_events: Vec<SyncEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerEventResponse {
    pub data: IndexerEventResponseData,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blockchain_roundtrip() {
        for (variant, expected) in [
            (IndexerBlockchain::Bsc, "\"bsc\""),
            (IndexerBlockchain::Ethereum, "\"ethereum\""),
            (IndexerBlockchain::Solana, "\"solana\""),
        ] {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            let parsed: IndexerBlockchain = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_datetime_range_from_str() {
        let json = "\"2024-01-01T00:00:00Z/2024-01-02T00:00:00Z\"";
        let parsed: DateTimeRange = serde_json::from_str(json).unwrap();
        assert!(parsed.start < parsed.end);
    }

    #[test]
    fn test_indexer_event_timestamp_seconds() {
        let e = IndexerEvent {
            id: "x".into(),
            timestamp: 1000.0,
            address: "a".into(),
            height: 0,
            transaction: "t".into(),
        };
        assert_eq!(e.timestamp_seconds(), 1.0);
    }

    #[test]
    fn test_indexer_event_response_roundtrip() {
        let json = serde_json::json!({
            "data": {
                "messageEvents": [{
                    "id": "id1",
                    "timestamp": 1000.0,
                    "address": "0xa",
                    "height": 1,
                    "transaction": "tx1",
                    "type": "MESSAGE",
                    "content": "..."
                }],
                "syncEvents": []
            }
        });
        let parsed: IndexerEventResponse = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.data.message_events.len(), 1);
        assert_eq!(parsed.data.sync_events.len(), 0);
    }
}
