//! Chain sync tables (`chains_sync_status`, `indexer_sync_status`, `chain_txs`).
//!
//! Mirrors `src/aleph/db/models/chains.py`.

use chrono::{DateTime, Utc};
use serde_json::Value;

use aleph_types::chain::Chain;

use crate::toolkit::range::Range;
use crate::toolkit::timestamp::timestamp_to_datetime;
use crate::types::chain_sync::{ChainEventType, ChainSyncProtocol};
use crate::{AlephError, AlephResult};

fn chain_from_text(s: &str) -> Chain {
    try_chain_from_text(s).unwrap_or_else(|_| panic!("unknown Chain in DB: {s}"))
}

fn try_chain_from_text(s: &str) -> AlephResult<Chain> {
    serde_json::from_value::<Chain>(serde_json::Value::String(s.to_string()))
        .map_err(|_| AlephError::InvalidMessage(format!("unknown Chain in DB: {s}")))
}

fn event_type_from_text(s: &str) -> ChainEventType {
    try_event_type_from_text(s).unwrap_or_else(|_| panic!("unknown ChainEventType in DB: {s}"))
}

fn try_event_type_from_text(s: &str) -> AlephResult<ChainEventType> {
    serde_json::from_value::<ChainEventType>(serde_json::Value::String(s.to_string()))
        .map_err(|_| AlephError::InvalidMessage(format!("unknown ChainEventType in DB: {s}")))
}

fn sync_protocol_from_text(s: &str) -> ChainSyncProtocol {
    try_sync_protocol_from_text(s)
        .unwrap_or_else(|_| panic!("unknown ChainSyncProtocol in DB: {s}"))
}

fn try_sync_protocol_from_text(s: &str) -> AlephResult<ChainSyncProtocol> {
    serde_json::from_value::<ChainSyncProtocol>(serde_json::Value::String(s.to_string()))
        .map_err(|_| AlephError::InvalidMessage(format!("unknown ChainSyncProtocol in DB: {s}")))
}

/// Row of the `chains_sync_status` table.
#[derive(Debug, Clone)]
pub struct ChainSyncStatusDb {
    pub chain: Chain,
    pub r#type: ChainEventType,
    pub height: i32,
    pub last_update: DateTime<Utc>,
}

impl ChainSyncStatusDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self::try_from_row(row).expect("valid ChainSyncStatusDb row")
    }

    pub fn try_from_row(row: &tokio_postgres::Row) -> AlephResult<Self> {
        let chain_s: String = row.get("chain");
        let type_s: String = row.get("type");
        Ok(Self {
            chain: try_chain_from_text(&chain_s)?,
            r#type: try_event_type_from_text(&type_s)?,
            height: row.get("height"),
            last_update: row.get("last_update"),
        })
    }
}

/// Row of the `indexer_sync_status` table.
#[derive(Debug, Clone)]
pub struct IndexerSyncStatusDb {
    pub chain: Chain,
    pub event_type: ChainEventType,
    pub start_block_datetime: DateTime<Utc>,
    pub end_block_datetime: DateTime<Utc>,
    pub start_included: bool,
    pub end_included: bool,
    pub last_updated: DateTime<Utc>,
}

impl IndexerSyncStatusDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self::try_from_row(row).expect("valid IndexerSyncStatusDb row")
    }

    pub fn try_from_row(row: &tokio_postgres::Row) -> AlephResult<Self> {
        let chain_s: String = row.get("chain");
        let event_s: String = row.get("event_type");
        Ok(Self {
            chain: try_chain_from_text(&chain_s)?,
            event_type: try_event_type_from_text(&event_s)?,
            start_block_datetime: row.get("start_block_datetime"),
            end_block_datetime: row.get("end_block_datetime"),
            start_included: row.get("start_included"),
            end_included: row.get("end_included"),
            last_updated: row.get("last_updated"),
        })
    }

    /// Convert this row to its [`Range`] representation. Mirrors Python's
    /// `IndexerSyncStatusDb.to_range`.
    pub fn to_range(&self) -> Range<DateTime<Utc>> {
        Range::new(
            self.start_block_datetime,
            self.end_block_datetime,
            self.start_included,
            self.end_included,
        )
        .expect("indexer sync range bounds valid")
    }
}

/// Row of the `chain_txs` table.
#[derive(Debug, Clone)]
pub struct ChainTxDb {
    pub hash: String,
    pub chain: Chain,
    pub height: i32,
    pub datetime: DateTime<Utc>,
    pub publisher: String,
    pub protocol: ChainSyncProtocol,
    pub protocol_version: i32,
    pub content: Value,
}

impl ChainTxDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self::try_from_row(row).expect("valid ChainTxDb row")
    }

    pub fn try_from_row(row: &tokio_postgres::Row) -> AlephResult<Self> {
        let chain_s: String = row.get("chain");
        let protocol_s: String = row.get("protocol");
        Ok(Self {
            hash: row.get("hash"),
            chain: try_chain_from_text(&chain_s)?,
            height: row.get("height"),
            datetime: row.get("datetime"),
            publisher: row.get("publisher"),
            protocol: try_sync_protocol_from_text(&protocol_s)?,
            protocol_version: row.get("protocol_version"),
            content: row.get("content"),
        })
    }

    /// Build a `ChainTxDb` from a raw dict-like payload. Mirrors Python
    /// `ChainTxDb.from_dict`.
    ///
    /// Expects the same keys as Python: `hash`, `chain`, `height`, `time`,
    /// `publisher`. Defaults match the Python signature.
    pub fn from_dict(
        tx_dict: &Value,
        protocol: ChainSyncProtocol,
        protocol_version: i32,
        content: Value,
    ) -> Self {
        let hash = tx_dict
            .get("hash")
            .and_then(|v| v.as_str())
            .expect("hash field")
            .to_string();
        let chain_s = tx_dict
            .get("chain")
            .and_then(|v| v.as_str())
            .expect("chain field");
        let height = tx_dict
            .get("height")
            .and_then(|v| v.as_i64())
            .expect("height field");
        let time_f = tx_dict
            .get("time")
            .and_then(|v| v.as_f64())
            .or_else(|| {
                tx_dict
                    .get("time")
                    .and_then(|v| v.as_i64())
                    .map(|i| i as f64)
            })
            .expect("time field");
        let publisher = tx_dict
            .get("publisher")
            .and_then(|v| v.as_str())
            .expect("publisher field")
            .to_string();
        Self {
            hash,
            chain: chain_from_text(chain_s),
            height: i32::try_from(height).expect("height fits in i32"),
            datetime: timestamp_to_datetime(time_f),
            publisher,
            protocol,
            protocol_version,
            content,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn chain_sync_status_construct() {
        let row = ChainSyncStatusDb {
            chain: Chain::Ethereum,
            r#type: ChainEventType::Message,
            height: 100,
            last_update: Utc::now(),
        };
        assert_eq!(row.height, 100);
        assert_eq!(row.r#type, ChainEventType::Message);
    }

    #[test]
    fn indexer_sync_to_range() {
        let now = Utc::now();
        let later = now + chrono::Duration::seconds(60);
        let row = IndexerSyncStatusDb {
            chain: Chain::Ethereum,
            event_type: ChainEventType::Sync,
            start_block_datetime: now,
            end_block_datetime: later,
            start_included: true,
            end_included: false,
            last_updated: Utc::now(),
        };
        let r = row.to_range();
        assert_eq!(r.lower, now);
        assert_eq!(r.upper, later);
        assert!(r.lower_inc);
        assert!(!r.upper_inc);
    }

    #[test]
    fn invalid_db_enums_return_errors() {
        assert!(try_chain_from_text("NOPE").is_err());
        assert!(try_event_type_from_text("NOPE").is_err());
        assert!(try_sync_protocol_from_text("NOPE").is_err());
    }

    #[test]
    fn chain_tx_from_dict_default() {
        let payload = json!({
            "hash": "0xdead",
            "chain": "ETH",
            "height": 42,
            "time": 1_700_000_000.0,
            "publisher": "0xpub",
        });
        let tx = ChainTxDb::from_dict(
            &payload,
            ChainSyncProtocol::OnChainSync,
            1,
            Value::String(String::new()),
        );
        assert_eq!(tx.hash, "0xdead");
        assert_eq!(tx.chain, Chain::Ethereum);
        assert_eq!(tx.height, 42);
        assert_eq!(tx.protocol, ChainSyncProtocol::OnChainSync);
        assert_eq!(tx.protocol_version, 1);
    }
}
