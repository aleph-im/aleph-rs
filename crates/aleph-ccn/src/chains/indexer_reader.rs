//! Multi-chain indexer reader. Mirrors `aleph/chains/indexer_reader.py`.
//!
//! Talks to `https://multichain.api.aleph.cloud/` via GraphQL to pull
//! `SyncEvent` / `MessageEvent` entries for a given smart contract address.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use aleph_types::chain::Chain;

use crate::AlephResult;
use crate::chains::chain_data_service::{PendingChainTx, PendingTxPublisher};
use crate::db::DbPool;
use crate::db::accessors::chains::{
    add_indexer_range, get_missing_indexer_datetime_multirange,
};
use crate::schemas::chains::indexer_response::DateTimeRange;
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

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AccountStateData {
    #[serde(default)]
    pub state: Vec<AccountState>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountState {
    #[serde(default)]
    pub processed: Vec<DateTimeRange>,
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

    pub async fn fetch_account_state(
        &self,
        indexer_url: &str,
        accounts: &[String],
    ) -> AlephResult<AccountStateData> {
        let query = make_account_state_query(self.blockchain, accounts, EntityType::Log);
        let body = serde_json::json!({ "query": query });
        let resp = self.http.post(indexer_url).json(&body).send().await?;
        let resp: IndexerResponse<AccountStateData> = resp.json().await?;
        Ok(resp.data)
    }

    /// Project a single indexer event into a `PendingChainTx`.
    pub fn message_event_to_tx(&self, ev: &MessageEvent) -> Option<PendingChainTx> {
        let msg_type = ev.r#type.as_deref()?.to_string();
        let content_str = ev.content.as_deref()?.to_string();
        let dt = timestamp_to_datetime(ev.timestamp / 1000.0);
        let content = serde_json::json!({
            "transaction": ev.transaction,
            "address": ev.address,
            "height": ev.height,
            "timestamp": ev.timestamp,
            "type": msg_type,
            "content": content_str,
        });
        Some(PendingChainTx {
            hash: ev.transaction.clone(),
            chain: self.chain.clone(),
            height: ev.height,
            datetime: dt,
            publisher: ev.address.clone(),
            protocol: ChainSyncProtocol::SmartContract,
            protocol_version: 1,
            content,
        })
    }

    /// Project a sync indexer event into a `PendingChainTx`.
    pub fn sync_event_to_tx(&self, ev: &SyncEvent) -> Option<PendingChainTx> {
        let dt = timestamp_to_datetime(ev.timestamp / 1000.0);
        let parsed: serde_json::Value = serde_json::from_str(&ev.message).ok()?;
        let protocol_str = parsed.get("protocol").and_then(|v| v.as_str())?;
        let protocol = match protocol_str {
            "aleph" => ChainSyncProtocol::OnChainSync,
            "aleph-offchain" => ChainSyncProtocol::OffChainSync,
            _ => return None,
        };
        let version = parsed.get("version").and_then(|v| v.as_u64())? as u32;
        let content = parsed.get("content").cloned()?;
        Some(PendingChainTx {
            hash: ev.transaction.clone(),
            chain: self.chain.clone(),
            height: ev.height,
            datetime: dt,
            publisher: ev.address.clone(),
            protocol,
            protocol_version: version,
            content,
        })
    }

    async fn fetch_range(
        &self,
        pool: &DbPool,
        publisher: &PendingTxPublisher,
        indexer_url: &str,
        event_type: ChainEventType,
        datetime_range: Range<DateTime<Utc>>,
    ) -> AlephResult<Vec<PendingChainTx>> {
        let mut results: Vec<PendingChainTx> = Vec::new();
        let mut start_datetime = datetime_range.lower;
        let end_datetime = datetime_range.upper;
        let limit: u32 = 1000;

        loop {
            let data = self
                .fetch_events(
                    indexer_url,
                    event_type,
                    Some((start_datetime, end_datetime)),
                    None,
                    limit,
                )
                .await?;

            let nb = match event_type {
                ChainEventType::Message => data.message_events.len(),
                ChainEventType::Sync => data.sync_events.len(),
            };

            if nb == 0 {
                let synced_range =
                    Range::new(start_datetime, end_datetime, true, true).expect("range bounds");
                let mut client = pool
                    .get()
                    .await
                    .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
                add_indexer_range(&mut client, self.chain.clone(), event_type, synced_range)
                    .await?;
                break;
            }

            let mut last_dt: DateTime<Utc> = start_datetime;

            match event_type {
                ChainEventType::Message => {
                    for ev in &data.message_events {
                        let Some(tx) = self.message_event_to_tx(ev) else {
                            tracing::warn!(
                                transaction = %ev.transaction,
                                "skipping malformed smart-contract indexer event"
                            );
                            continue;
                        };
                        last_dt = tx.datetime;
                        publisher.publish(&tx).await?;
                        results.push(tx);
                    }
                }
                ChainEventType::Sync => {
                    for ev in &data.sync_events {
                        let Some(tx) = self.sync_event_to_tx(ev) else {
                            tracing::warn!(
                                transaction = %ev.transaction,
                                "skipping malformed sync indexer event"
                            );
                            continue;
                        };
                        last_dt = tx.datetime;
                        publisher.publish(&tx).await?;
                        results.push(tx);
                    }
                }
            };

            // Persist range progress.
            let synced_upper = if (nb as u32) >= limit {
                last_dt
            } else {
                end_datetime
            };
            let synced_range =
                Range::new(start_datetime, synced_upper, true, (nb as u32) < limit)
                    .expect("range bounds");
            let mut client = pool
                .get()
                .await
                .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
            add_indexer_range(
                &mut client,
                self.chain.clone(),
                event_type,
                synced_range.clone(),
            )
            .await?;

            if (nb as u32) < limit {
                break;
            }
            start_datetime = synced_range.upper;
        }

        Ok(results)
    }

    /// Pull all missing processed indexer ranges for the configured smart
    /// contract and persist the corresponding pending-tx rows. Mirrors
    /// `AlephIndexerReader.fetch_new_events`.
    pub async fn fetch_new_events(
        &self,
        pool: &DbPool,
        publisher: &PendingTxPublisher,
        indexer_url: &str,
        smart_contract_address: &str,
        event_type: ChainEventType,
    ) -> AlephResult<Vec<PendingChainTx>> {
        let account_state = self
            .fetch_account_state(indexer_url, &[smart_contract_address.to_string()])
            .await?;
        let Some(state) = account_state.state.first() else {
            tracing::warn!(
                account = smart_contract_address,
                "No account data found. Is the indexer up to date?"
            );
            return Ok(Vec::new());
        };

        let mut indexer_multirange: MultiRange<DateTime<Utc>> = MultiRange::default();
        for processed in &state.processed {
            let range = Range::new(processed.start, processed.end, true, true)
                .map_err(|e| crate::AlephError::Chain(format!("invalid indexer range: {e}")))?;
            indexer_multirange.add_range(range);
        }

        let client = pool
            .get()
            .await
            .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
        let missing = get_missing_indexer_datetime_multirange(
            &**client,
            self.chain.clone(),
            event_type,
            &indexer_multirange,
        )
        .await?;
        drop(client);

        let mut results = Vec::new();
        for range_to_sync in missing.iter() {
            let mut txs = self
                .fetch_range(
                    pool,
                    publisher,
                    indexer_url,
                    event_type,
                    range_to_sync.clone(),
                )
                .await?;
            results.append(&mut txs);
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
        smart_contract_address: String,
        event_type: ChainEventType,
    ) -> AlephResult<()> {
        loop {
            if let Err(e) = self
                .fetch_new_events(
                    &pool,
                    &publisher,
                    &indexer_url,
                    &smart_contract_address,
                    event_type,
                )
                .await
            {
                tracing::warn!(error = %e, "indexer run: fetch_new_events failed");
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    /// Fallback entry point for connectors built without DB pool + publisher.
    ///
    /// The real fetch loop ([`AlephIndexerReader::run`]) requires a `DbPool`
    /// (to persist synced ranges) and a `PendingTxPublisher` (to publish
    /// fetched events). Without those, any events fetched here would be
    /// silently discarded and no range progress would be recorded, so this
    /// fails loudly instead of running a no-op discard loop.
    pub async fn fetcher(
        &self,
        _indexer_url: &str,
        _smart_contract_address: &str,
        _event_type: ChainEventType,
    ) -> AlephResult<()> {
        Err(crate::AlephError::Chain(
            "indexer reader requires DbPool + publisher".to_string(),
        ))
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
        let tx = reader.message_event_to_tx(&ev).unwrap();
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
        let tx = reader.sync_event_to_tx(&ev).unwrap();
        assert_eq!(tx.protocol, ChainSyncProtocol::OffChainSync);
        assert_eq!(tx.protocol_version, 1);
        assert_eq!(tx.content, serde_json::Value::String("QmCID".into()));
    }

    #[test]
    fn sync_event_to_tx_rejects_unknown_protocol() {
        let reader = AlephIndexerReader::new(Chain::Bsc);
        let ev = SyncEvent {
            transaction: "0xtx".into(),
            address: "0xemit".into(),
            height: 7,
            timestamp: 1700000000000.0,
            message: "{\"protocol\":\"unknown\",\"version\":1,\"content\":{}}".into(),
        };
        assert!(reader.sync_event_to_tx(&ev).is_none());
    }
}
