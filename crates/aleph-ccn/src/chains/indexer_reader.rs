//! Multi-chain indexer reader. Mirrors `aleph/chains/indexer_reader.py`.
//!
//! Talks to `https://multichain.api.aleph.cloud/` via GraphQL to pull
//! `SyncEvent` / `MessageEvent` entries for a given smart contract address.

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use aleph_types::chain::Chain;

use crate::AlephResult;
use crate::chains::chain_data_service::{PendingChainTx, PendingTxPublisher};
use crate::db::DbPool;
use crate::db::accessors::chains::{IndexerMultiRange, update_indexer_multirange};
use crate::toolkit::range::{MultiRange, Range};
use crate::toolkit::timestamp::timestamp_to_datetime;
use crate::types::chain_sync::{ChainEventType, ChainSyncProtocol};

/// Indexer blockchain identifiers — must match the indexer's GraphQL enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexerBlockchain {
    Ethereum,
    Bsc,
    Solana,
}

impl IndexerBlockchain {
    pub fn as_str(&self) -> &'static str {
        match self {
            IndexerBlockchain::Ethereum => "ethereum",
            IndexerBlockchain::Bsc => "bsc",
            IndexerBlockchain::Solana => "solana",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityType {
    Log,
}

impl EntityType {
    fn as_str(&self) -> &'static str {
        match self {
            EntityType::Log => "log",
        }
    }
}

/// Maps an Aleph `Chain` to its indexer blockchain name. Returns `None`
/// for chains not served by the multichain indexer (Tezos / NULS / DOT).
pub fn chain_to_blockchain(chain: &Chain) -> Option<IndexerBlockchain> {
    match chain {
        Chain::Bsc => Some(IndexerBlockchain::Bsc),
        Chain::Ethereum => Some(IndexerBlockchain::Ethereum),
        Chain::Sol => Some(IndexerBlockchain::Solana),
        _ => None,
    }
}

/// Builds the `accountState(...)` GraphQL query used to discover what
/// block ranges the indexer has already processed.
pub fn make_account_state_query(
    blockchain: IndexerBlockchain,
    accounts: &[String],
    type_: EntityType,
) -> String {
    let accounts_str = accounts
        .iter()
        .map(|a| format!("\"{a}\""))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "{{\n  state: accountState(\n    blockchain: \"{}\"\n    account: [{}]\n    type: {}\n  ) {{\n    blockchain\n    type\n    indexer\n    account\n    completeHistory\n    progress\n    pending\n    processed\n  }}\n}}",
        blockchain.as_str(),
        accounts_str,
        type_.as_str()
    )
}

/// Builds an `events(...)` GraphQL query.
pub fn make_events_query(
    event_type: ChainEventType,
    blockchain: IndexerBlockchain,
    datetime_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    block_range: Option<(u64, u64)>,
    limit: u32,
) -> String {
    let event_type_str = match event_type {
        ChainEventType::Message => "messageEvents",
        ChainEventType::Sync => "syncEvents",
    };

    let mut params = vec![
        format!("blockchain: \"{}\"", blockchain.as_str()),
        format!("limit: {limit}"),
        "reverse: false".to_string(),
    ];

    if let Some((s, e)) = block_range {
        params.push(format!("startHeight: {s}"));
        params.push(format!("endHeight: {e}"));
    }
    if let Some((s, e)) = datetime_range {
        params.push(format!("startDate: {}", (s.timestamp_millis()) as f64));
        params.push(format!("endDate: {}", (e.timestamp_millis()) as f64));
    }

    // The shape of the fields fragment matches MessageEvent/SyncEvent schemas.
    let fields = match event_type {
        ChainEventType::Message => "transaction\naddress\nheight\ntimestamp\ntype\ncontent",
        ChainEventType::Sync => "transaction\naddress\nheight\ntimestamp\nmessage",
    };

    format!(
        "{{\n  {event_type_str}({}) {{\n    {fields}\n  }}\n}}",
        params.join(", ")
    )
}

#[derive(Debug, Clone, Deserialize)]
pub struct MessageEvent {
    pub transaction: String,
    pub address: String,
    pub height: u64,
    pub timestamp: f64,
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncEvent {
    pub transaction: String,
    pub address: String,
    pub height: u64,
    pub timestamp: f64,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct IndexerEventData {
    #[serde(rename = "messageEvents", default)]
    pub message_events: Vec<MessageEvent>,
    #[serde(rename = "syncEvents", default)]
    pub sync_events: Vec<SyncEvent>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexerResponse<T> {
    pub data: T,
}

/// `AlephIndexerReader` mirrors the Python class but operates statelessly:
/// the caller passes the URL/contract on each call.
pub struct AlephIndexerReader {
    pub chain: Chain,
    pub blockchain: IndexerBlockchain,
    http: reqwest::Client,
}

impl AlephIndexerReader {
    pub fn new(chain: Chain) -> Self {
        let blockchain = chain_to_blockchain(&chain).unwrap_or(IndexerBlockchain::Ethereum);
        Self {
            chain,
            blockchain,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
        }
    }

    pub async fn fetch_events(
        &self,
        indexer_url: &str,
        event_type: ChainEventType,
        datetime_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
        block_range: Option<(u64, u64)>,
        limit: u32,
    ) -> AlephResult<IndexerEventData> {
        let query = make_events_query(
            event_type,
            self.blockchain,
            datetime_range,
            block_range,
            limit,
        );
        let body = serde_json::json!({ "query": query });
        let resp = self.http.post(indexer_url).json(&body).send().await?;
        let resp: IndexerResponse<IndexerEventData> = resp.json().await?;
        Ok(resp.data)
    }

    /// Project a single indexer event into a `PendingChainTx`.
    pub fn message_event_to_tx(&self, ev: &MessageEvent) -> PendingChainTx {
        let dt = timestamp_to_datetime(ev.timestamp / 1000.0);
        let content = serde_json::json!({
            "transaction": ev.transaction,
            "address": ev.address,
            "height": ev.height,
            "timestamp": ev.timestamp,
            "type": ev.r#type,
            "content": ev.content,
        });
        PendingChainTx {
            hash: ev.transaction.clone(),
            chain: self.chain.clone(),
            height: ev.height,
            datetime: dt,
            publisher: ev.address.clone(),
            protocol: ChainSyncProtocol::SmartContract,
            protocol_version: 1,
            content,
        }
    }

    /// Project a sync indexer event into a `PendingChainTx`.
    pub fn sync_event_to_tx(&self, ev: &SyncEvent) -> PendingChainTx {
        let dt = timestamp_to_datetime(ev.timestamp / 1000.0);
        let parsed: serde_json::Value =
            serde_json::from_str(&ev.message).unwrap_or(serde_json::Value::Null);
        let protocol_str = parsed
            .get("protocol")
            .and_then(|v| v.as_str())
            .unwrap_or("aleph");
        let protocol = match protocol_str {
            "aleph" => ChainSyncProtocol::OnChainSync,
            "aleph-offchain" => ChainSyncProtocol::OffChainSync,
            _ => ChainSyncProtocol::OnChainSync,
        };
        let version = parsed.get("version").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
        let content = parsed
            .get("content")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        PendingChainTx {
            hash: ev.transaction.clone(),
            chain: self.chain.clone(),
            height: ev.height,
            datetime: dt,
            publisher: ev.address.clone(),
            protocol,
            protocol_version: version,
            content,
        }
    }

    /// Pull all new events for the configured smart contract and persist
    /// the corresponding pending-tx rows. Mirrors
    /// `AlephIndexerReader.fetch_new_events`.
    pub async fn fetch_new_events(
        &self,
        pool: &DbPool,
        publisher: &PendingTxPublisher,
        indexer_url: &str,
        event_type: ChainEventType,
        last_height: u64,
    ) -> AlephResult<Vec<PendingChainTx>> {
        let mut results: Vec<PendingChainTx> = Vec::new();
        let mut cursor_block: u64 = last_height;
        let limit: u32 = 1000;
        let upper_bound: u64 = u64::MAX / 2;

        loop {
            let data = self
                .fetch_events(
                    indexer_url,
                    event_type,
                    None,
                    Some((cursor_block, upper_bound)),
                    limit,
                )
                .await?;

            let nb = match event_type {
                ChainEventType::Message => data.message_events.len(),
                ChainEventType::Sync => data.sync_events.len(),
            };

            if nb == 0 {
                break;
            }

            let mut last_dt: Option<DateTime<Utc>> = None;
            let mut last_block: u64 = cursor_block;

            match event_type {
                ChainEventType::Message => {
                    for ev in &data.message_events {
                        let tx = self.message_event_to_tx(ev);
                        last_dt = Some(tx.datetime);
                        last_block = tx.height.max(last_block);
                        publisher.publish(&tx).await?;
                        results.push(tx);
                    }
                }
                ChainEventType::Sync => {
                    for ev in &data.sync_events {
                        let tx = self.sync_event_to_tx(ev);
                        last_dt = Some(tx.datetime);
                        last_block = tx.height.max(last_block);
                        publisher.publish(&tx).await?;
                        results.push(tx);
                    }
                }
            };

            // Persist range progress.
            if let Some(end_dt) = last_dt {
                let start_dt = timestamp_to_datetime(0.0);
                if let Ok(range) = Range::new(start_dt, end_dt, true, true) {
                    let mut mr: MultiRange<DateTime<Utc>> = MultiRange::default();
                    mr.add_range(range);
                    let imr = IndexerMultiRange {
                        chain: self.chain.clone(),
                        event_type,
                        datetime_multirange: mr,
                    };
                    let client = pool
                        .get()
                        .await
                        .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
                    update_indexer_multirange(&**client, &imr).await?;
                }
            }

            if (nb as u32) < limit {
                break;
            }
            cursor_block = last_block.saturating_add(1);
        }

        Ok(results)
    }

    /// Run the indexer loop until cancelled. Mirrors `AlephIndexerReader.fetcher`
    /// with DB-backed range persistence.
    pub async fn run(
        &self,
        pool: DbPool,
        publisher: Arc<PendingTxPublisher>,
        indexer_url: String,
        event_type: ChainEventType,
    ) -> AlephResult<()> {
        loop {
            if let Err(e) = self
                .fetch_new_events(&pool, &publisher, &indexer_url, event_type, 0)
                .await
            {
                tracing::warn!(error = %e, "indexer run: fetch_new_events failed");
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    /// Loop forever fetching events. Mirrors `AlephIndexerReader.fetcher`.
    pub async fn fetcher(
        &self,
        indexer_url: &str,
        _smart_contract_address: &str,
        event_type: ChainEventType,
    ) -> AlephResult<()> {
        let mut next_after: DateTime<Utc> = Utc.timestamp_opt(0, 0).single().unwrap();
        loop {
            let now = Utc::now();
            match self
                .fetch_events(indexer_url, event_type, Some((next_after, now)), None, 1000)
                .await
            {
                Ok(data) => {
                    let count = data.message_events.len() + data.sync_events.len();
                    tracing::info!(
                        chain = %self.chain,
                        ?event_type,
                        count,
                        "indexer: fetched events"
                    );
                    next_after = now;
                }
                Err(e) => {
                    tracing::error!(error = %e, "indexer fetch failed; retrying");
                }
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_state_query_contains_blockchain_and_account() {
        let q = make_account_state_query(
            IndexerBlockchain::Ethereum,
            &["0xdeadbeef".to_string()],
            EntityType::Log,
        );
        assert!(q.contains("blockchain: \"ethereum\""));
        assert!(q.contains("0xdeadbeef"));
        assert!(q.contains("type: log"));
    }

    #[test]
    fn events_query_uses_message_for_message_type() {
        let q = make_events_query(
            ChainEventType::Message,
            IndexerBlockchain::Bsc,
            None,
            Some((1, 2)),
            500,
        );
        assert!(q.contains("messageEvents"));
        assert!(q.contains("blockchain: \"bsc\""));
        assert!(q.contains("startHeight: 1"));
        assert!(q.contains("endHeight: 2"));
    }

    #[test]
    fn events_query_sync() {
        let q = make_events_query(
            ChainEventType::Sync,
            IndexerBlockchain::Ethereum,
            None,
            Some((1, 2)),
            10,
        );
        assert!(q.contains("syncEvents"));
        assert!(q.contains("message"));
    }

    #[test]
    fn chain_mapping_recognises_supported_chains() {
        assert_eq!(
            chain_to_blockchain(&Chain::Ethereum),
            Some(IndexerBlockchain::Ethereum)
        );
        assert_eq!(
            chain_to_blockchain(&Chain::Bsc),
            Some(IndexerBlockchain::Bsc)
        );
        assert_eq!(
            chain_to_blockchain(&Chain::Sol),
            Some(IndexerBlockchain::Solana)
        );
        assert!(chain_to_blockchain(&Chain::Tezos).is_none());
    }

    #[tokio::test]
    async fn fetch_events_decodes_message_response() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        let body = serde_json::json!({
            "data": {
                "messageEvents": [
                    {
                        "transaction": "0xabc",
                        "address": "0xpub",
                        "height": 42,
                        "timestamp": 1700000000000.0_f64,
                        "type": "STORE_IPFS",
                        "content": "QmHash"
                    }
                ]
            }
        });
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&mock)
            .await;

        let reader = AlephIndexerReader::new(Chain::Ethereum);
        let data = reader
            .fetch_events(
                &mock.uri(),
                ChainEventType::Message,
                None,
                Some((0, 1000)),
                100,
            )
            .await
            .unwrap();

        assert_eq!(data.message_events.len(), 1);
        assert_eq!(data.message_events[0].height, 42);
        assert_eq!(data.message_events[0].transaction, "0xabc");
    }

    #[test]
    fn message_event_to_tx_uses_smart_contract_protocol() {
        let reader = AlephIndexerReader::new(Chain::Ethereum);
        let ev = MessageEvent {
            transaction: "0xabc".into(),
            address: "0xpub".into(),
            height: 10,
            timestamp: 1700000000000.0,
            r#type: Some("STORE_IPFS".into()),
            content: Some("QmHash".into()),
        };
        let tx = reader.message_event_to_tx(&ev);
        assert_eq!(tx.protocol, ChainSyncProtocol::SmartContract);
        assert_eq!(tx.chain, Chain::Ethereum);
        assert_eq!(tx.height, 10);
    }

    #[test]
    fn sync_event_to_tx_uses_inline_protocol() {
        let reader = AlephIndexerReader::new(Chain::Bsc);
        let ev = SyncEvent {
            transaction: "0xtx".into(),
            address: "0xemit".into(),
            height: 7,
            timestamp: 1700000000000.0,
            message: "{\"protocol\":\"aleph-offchain\",\"version\":1,\"content\":\"QmCID\"}".into(),
        };
        let tx = reader.sync_event_to_tx(&ev);
        assert_eq!(tx.protocol, ChainSyncProtocol::OffChainSync);
        assert_eq!(tx.protocol_version, 1);
        assert_eq!(tx.content, serde_json::Value::String("QmCID".into()));
    }
}
