//! Mirrors `src/aleph/schemas/chains/tezos_indexer_response.py`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncStatus {
    Synced,
    InProgress,
    Down,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerStatus {
    #[serde(rename = "oldestBlock", alias = "oldest_block")]
    pub oldest_block: String,
    #[serde(rename = "recentBlock", alias = "recent_block")]
    pub recent_block: String,
    pub status: SyncStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerStats {
    #[serde(rename = "totalEvents", alias = "total_events")]
    pub total_events: i64,
}

/// Tezos indexer event. Mirrors `IndexerEvent[PayloadType]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerEvent<PayloadType> {
    pub source: String,
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "blockLevel", alias = "block_level")]
    pub block_level: i64,
    #[serde(rename = "operationHash", alias = "operation_hash")]
    pub operation_hash: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub payload: PayloadType,
}

/// Mirrors Tezos `MessageEventPayload`. Field aliases follow the Python
/// `populate_by_name=True` setup.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageEventPayload {
    pub timestamp: f64,
    pub addr: String,
    #[serde(rename = "msgtype", alias = "message_type")]
    pub message_type: String,
    #[serde(rename = "msgcontent", alias = "message_content")]
    pub message_content: String,
}

impl MessageEventPayload {
    pub fn address(&self) -> &str {
        &self.addr
    }

    pub fn event_type(&self) -> &str {
        &self.message_type
    }

    pub fn content(&self) -> &str {
        &self.message_content
    }

    pub fn timestamp_seconds(&self) -> f64 {
        self.timestamp
    }
}

pub type IndexerMessageEvent = IndexerEvent<MessageEventPayload>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerResponseData<IndexerEventType> {
    #[serde(rename = "indexStatus", alias = "index_status")]
    pub index_status: IndexerStatus,
    pub stats: IndexerStats,
    pub events: Vec<IndexerEventType>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexerResponse<IndexerEventType> {
    pub data: IndexerResponseData<IndexerEventType>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_status_roundtrip() {
        for (variant, expected) in [
            (SyncStatus::Synced, "\"synced\""),
            (SyncStatus::InProgress, "\"in_progress\""),
            (SyncStatus::Down, "\"down\""),
        ] {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            let parsed: SyncStatus = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn test_indexer_status_roundtrip() {
        let json = serde_json::json!({
            "oldestBlock": "0",
            "recentBlock": "1000",
            "status": "synced"
        });
        let parsed: IndexerStatus = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.oldest_block, "0");
        assert_eq!(parsed.recent_block, "1000");
        assert_eq!(parsed.status, SyncStatus::Synced);
    }

    #[test]
    fn test_indexer_response_roundtrip() {
        let json = serde_json::json!({
            "data": {
                "indexStatus": {
                    "oldestBlock": "0",
                    "recentBlock": "100",
                    "status": "in_progress"
                },
                "stats": {"totalEvents": 10},
                "events": [{
                    "source": "tz1",
                    "timestamp": "2024-01-01T00:00:00Z",
                    "blockLevel": 5,
                    "operationHash": "op1",
                    "type": "msg",
                    "payload": {
                        "timestamp": 1700000000.0,
                        "addr": "tz1xxx",
                        "msgtype": "POST",
                        "msgcontent": "..."
                    }
                }]
            }
        });
        let parsed: IndexerResponse<IndexerMessageEvent> = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.data.stats.total_events, 10);
        assert_eq!(parsed.data.events[0].payload.address(), "tz1xxx");
        assert_eq!(parsed.data.events[0].payload.event_type(), "POST");
    }
}
