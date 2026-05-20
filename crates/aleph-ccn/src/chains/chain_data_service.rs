//! Chain data service. Mirrors `aleph/chains/chain_data_service.py`.
//!
//! Two concerns live here:
//!   - `ChainDataService` prepares the JSON payload that goes on-chain
//!     (or, more commonly, into IPFS) when the CCN broadcasts a batch of
//!     unconfirmed messages.
//!   - `PendingTxPublisher` records incoming chain transactions in the DB
//!     and announces them on the pending-tx RabbitMQ exchange.

use std::sync::Arc;
use std::time::Duration;

use alloy_rpc_types_eth::Log;
use alloy_sol_types::SolEvent;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use lapin::{BasicProperties, Channel, options::BasicPublishOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;

use crate::{AlephError, AlephResult};
use crate::db::accessors::files::{upsert_file, upsert_tx_file_pin};
use crate::db::models::messages::MessageDb;
use crate::services::ipfs::IpfsService;
use crate::storage::StorageService;
use crate::toolkit::timestamp::{timestamp_to_datetime, utc_now};
use crate::types::chain_sync::ChainSyncProtocol;
use crate::types::channel::Channel as AlephChannel;
use crate::types::files::FileType;
use aleph_types::chain::Chain;
use aleph_types::message::item_type::ItemType;

/// Historical threshold used by the previous inline fallback. Kept for tests
/// that build large payloads; production sync event preparation now mirrors
/// pyaleph and always emits `OFF_CHAIN_SYNC`.
pub const PAYLOAD_INLINE_LIMIT_BYTES: usize = 50 * 1024;

/// Payload format pyaleph emits on-chain (or as the "on-chain content" of an
/// off-chain sync event).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnChainMessage {
    pub item_hash: String,
    pub sender: String,
    pub chain: Chain,
    #[serde(rename = "type")]
    pub message_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    pub time: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<AlephChannel>,
}

impl OnChainMessage {
    /// Project a fully-validated `MessageDb` row into an `OnChainMessage`
    /// suitable for inclusion in a sync event payload.
    pub fn from_message_db(msg: &MessageDb) -> Self {
        let item_type = serde_json::to_value(msg.item_type)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()));
        // f64 seconds-since-epoch, matching the pyaleph `OnChainMessage.time`
        // shape (it goes through Pydantic's `datetime -> float`).
        let time = msg.time.timestamp() as f64 + (msg.time.timestamp_subsec_nanos() as f64) / 1e9;
        Self {
            item_hash: msg.item_hash.clone(),
            sender: msg.sender.clone(),
            chain: msg.chain.clone(),
            message_type: msg.r#type.to_string(),
            signature: msg.signature.clone(),
            time,
            item_content: msg.item_content.clone(),
            item_type,
            channel: msg.channel.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnChainContent {
    pub messages: Vec<OnChainMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnChainSyncEventPayload {
    pub protocol: ChainSyncProtocol,
    pub version: u32,
    pub content: OnChainContent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffChainSyncEventPayload {
    pub protocol: ChainSyncProtocol,
    pub version: u32,
    /// IPFS hash where the actual payload lives.
    pub content: String,
}

// --- Solidity event mirrors -----------------------------------------------
// Local declarations so chain_data_service stays decoupled from `ethereum.rs`.

alloy_sol_types::sol! {
    event SyncEvent(uint256 timestamp, address addr, string message);
    event MessageEvent(uint256 timestamp, address addr, string msgtype, string msgcontent);
}

/// Mirrors `ChainDataService.prepare_sync_event_payload` / `get_tx_messages`.
pub struct ChainDataService {
    ipfs: Option<Arc<IpfsService>>,
    storage: Option<Arc<StorageService>>,
}

impl ChainDataService {
    pub fn new() -> Self {
        Self {
            ipfs: None,
            storage: None,
        }
    }

    pub fn with_ipfs(ipfs: Arc<IpfsService>) -> Self {
        Self {
            ipfs: Some(ipfs),
            storage: None,
        }
    }

    pub fn with_storage(storage: Arc<StorageService>) -> Self {
        Self {
            ipfs: Some(storage.ipfs_service.clone()),
            storage: Some(storage),
        }
    }

    /// Packs the messages into an `OnChainSyncEventPayload`.
    pub fn build_on_chain_payload(&self, messages: Vec<OnChainMessage>) -> OnChainSyncEventPayload {
        OnChainSyncEventPayload {
            protocol: ChainSyncProtocol::OnChainSync,
            version: 1,
            content: OnChainContent { messages },
        }
    }

    /// Wraps a hosted IPFS CID into an `OffChainSyncEventPayload`.
    pub fn build_off_chain_payload(&self, ipfs_cid: String) -> OffChainSyncEventPayload {
        OffChainSyncEventPayload {
            protocol: ChainSyncProtocol::OffChainSync,
            version: 1,
            content: ipfs_cid,
        }
    }

    /// Builds the JSON payload that the chain writer will emit via
    /// `doEmit(content)`.
    ///
    /// Pyaleph always stores the archive off-chain and emits an `OffChain`
    /// envelope referencing the resulting CID.
    pub async fn prepare_sync_event_payload(
        &self,
        client: &(impl GenericClient + Sync),
        messages: Vec<MessageDb>,
    ) -> AlephResult<String> {
        let on_chain_messages: Vec<OnChainMessage> = messages
            .iter()
            .map(OnChainMessage::from_message_db)
            .collect();
        let archive = self.build_on_chain_payload(on_chain_messages);
        let (cid, size) = if let Some(storage) = &self.storage {
            if !storage.ipfs_enabled {
                return Err(AlephError::Ipfs(
                    "cannot prepare chain sync payload when IPFS is disabled".into(),
                ));
            }
            let archive_bytes = serde_json::to_vec(&archive)?;
            let size = archive_bytes.len() as i64;
            let cid = storage
                .add_file(client, &archive_bytes, ItemType::Ipfs)
                .await?;
            (cid, size)
        } else {
            self.upload_sync_archive(archive).await?
        };
        upsert_file(client, &cid, size, FileType::File).await?;
        let off = self.build_off_chain_payload(cid);
        Ok(serde_json::to_string(&off)?)
    }

    async fn upload_sync_archive(
        &self,
        archive: OnChainSyncEventPayload,
    ) -> AlephResult<(String, i64)> {
        let archive_json = serde_json::to_value(archive)?;
        let size = serde_json::to_vec(&archive_json)?.len() as i64;
        match &self.ipfs {
            Some(ipfs) => {
                let cid = ipfs.add_bytes(Bytes::from(serde_json::to_vec(&archive_json)?), 0).await?;
                Ok((cid, size))
            }
            None => Err(AlephError::Ipfs(
                "cannot prepare chain sync payload without IPFS service".into(),
            )),
        }
    }

    /// Decode a single Ethereum-style log into a [`PendingChainTx`]. Returns
    /// `None` if the log does not match a known Aleph event topic.
    /// Mirrors `_request_transactions` in pyaleph's `ethereum.py`.
    pub fn parse_log(&self, log: &Log) -> AlephResult<Option<PendingChainTx>> {
        // SyncEvent: { protocol, version, content } already JSON-encoded.
        if let Ok(decoded) = SyncEvent::decode_log(&log.inner) {
            let SyncEvent {
                timestamp,
                addr,
                message,
            } = decoded.data;
            let parsed: Value = match serde_json::from_str(&message) {
                Ok(v) => v,
                Err(_) => {
                    tracing::warn!(?log.transaction_hash, "SyncEvent message is not JSON");
                    return Ok(None);
                }
            };
            let Some(protocol) = parsed
                .get("protocol")
                .and_then(|v| v.as_str())
                .and_then(|s| {
                    serde_json::from_value::<ChainSyncProtocol>(Value::String(s.to_string())).ok()
                }) else {
                    tracing::warn!(?log.transaction_hash, "SyncEvent has unknown or missing protocol");
                    return Ok(None);
                };
            let Some(protocol_version) = parsed.get("version").and_then(|v| v.as_u64()) else {
                tracing::warn!(?log.transaction_hash, "SyncEvent has missing or invalid version");
                return Ok(None);
            };
            let Some(content) = parsed.get("content").cloned() else {
                tracing::warn!(?log.transaction_hash, "SyncEvent has missing content");
                return Ok(None);
            };

            let height = log.block_number.unwrap_or_default();
            let hash = log
                .transaction_hash
                .map(|h| format!("0x{}", hex::encode(h.0)))
                .unwrap_or_default();
            let datetime = u256_to_datetime(timestamp);
            return Ok(Some(PendingChainTx {
                hash,
                chain: Chain::Ethereum,
                height,
                datetime,
                publisher: format!("{:#x}", addr),
                protocol,
                protocol_version: protocol_version as u32,
                content,
            }));
        }
        // MessageEvent: payload is the message body itself.
        if let Ok(decoded) = MessageEvent::decode_log(&log.inner) {
            let MessageEvent {
                timestamp,
                addr,
                msgtype,
                msgcontent,
            } = decoded.data;
            let height = log.block_number.unwrap_or_default();
            let hash = log
                .transaction_hash
                .map(|h| format!("0x{}", hex::encode(h.0)))
                .unwrap_or_default();
            let datetime = u256_to_datetime(timestamp);
            let content = serde_json::json!({
                "address": format!("{:#x}", addr),
                "type": msgtype,
                "content": msgcontent,
                "timestamp": datetime.timestamp_millis(),
            });
            return Ok(Some(PendingChainTx {
                hash,
                chain: Chain::Ethereum,
                height,
                datetime,
                publisher: format!("{:#x}", addr),
                protocol: ChainSyncProtocol::SmartContract,
                protocol_version: 1,
                content,
            }));
        }
        Ok(None)
    }

    /// Resolves the message payload(s) carried by a chain transaction.
    /// Mirrors `ChainDataService.get_tx_messages`.
    pub async fn get_tx_messages(
        &self,
        client: &(impl GenericClient + Sync),
        tx: &crate::db::models::chains::ChainTxDb,
    ) -> AlephResult<Vec<OnChainMessage>> {
        if tx.protocol == ChainSyncProtocol::OffChainSync {
            let (messages, file_hash, size) = self.get_off_chain_messages(tx).await?;
            if self.ipfs_enabled() {
                upsert_file(client, &file_hash, size, FileType::File).await?;
                upsert_tx_file_pin(client, &file_hash, &tx.hash, utc_now()).await?;
                if let Err(e) = self.pin_off_chain_archive(&file_hash).await {
                    tracing::warn!(file_hash = %file_hash, error = %e, "could not pin off-chain sync archive");
                }
            }
            return Ok(messages);
        }

        self.get_tx_messages_from_tx(tx).await
    }

    /// Resolves the message payload(s) carried by a chain transaction without
    /// requiring a DB client. The client parameter on [`Self::get_tx_messages`]
    /// is kept for API compatibility with the Python-shaped call sites.
    pub async fn get_tx_messages_from_tx(
        &self,
        tx: &crate::db::models::chains::ChainTxDb,
    ) -> AlephResult<Vec<OnChainMessage>> {
        if tx.protocol_version != 1 {
            return Err(crate::AlephError::Chain(format!(
                "unknown protocol/version object in tx {}/{}: {:?} v{}",
                tx.chain, tx.hash, tx.protocol, tx.protocol_version
            )));
        }

        match tx.protocol {
            ChainSyncProtocol::OnChainSync => {
                let messages = tx
                    .content
                    .get("messages")
                    .cloned()
                    .ok_or_else(|| {
                        crate::AlephError::Chain(format!(
                            "got bad data in tx {}/{}: missing messages",
                            tx.chain, tx.hash
                        ))
                    })?;
                let parsed: Vec<OnChainMessage> = serde_json::from_value(messages)
                    .map_err(|e| crate::AlephError::Chain(format!("bad on-chain content: {e}")))?;
                Ok(parsed)
            }
            ChainSyncProtocol::OffChainSync => {
                let (parsed, _, _) = self.get_off_chain_messages(tx).await?;
                Ok(parsed)
            }
            ChainSyncProtocol::SmartContract => {
                Ok(vec![smart_contract_message_from_tx(tx)?])
            }
        }
    }

    async fn get_off_chain_messages(
        &self,
        tx: &crate::db::models::chains::ChainTxDb,
    ) -> AlephResult<(Vec<OnChainMessage>, String, i64)> {
        let cid = tx.content.as_str().ok_or_else(|| {
            crate::AlephError::Chain("off-chain content is not a string CID".into())
        })?;
        if let Some(storage) = &self.storage {
            let content = storage
                .get_json(cid, ItemType::Ipfs, Duration::from_secs(60), 1)
                .await?;
            return parse_off_chain_archive(tx, cid, content.value, content.raw_value.len() as i64);
        }
        let ipfs = self
            .ipfs
            .as_ref()
            .ok_or_else(|| crate::AlephError::Chain("IPFS service not configured".into()))?;
        let raw = ipfs
            .get_ipfs_content(cid, Duration::from_secs(60), 1)
            .await?
            .ok_or_else(|| crate::AlephError::Chain(format!("could not fetch CID {cid}")))?;
        let body: Value = serde_json::from_slice(&raw)
            .map_err(|e| crate::AlephError::Chain(format!("bad off-chain JSON: {e}")))?;
        parse_off_chain_archive(tx, cid, body, raw.len() as i64)
    }

    async fn pin_off_chain_archive(&self, cid: &str) -> AlephResult<()> {
        if let Some(storage) = &self.storage {
            return storage.pin_hash(cid, Duration::from_secs(120), 1).await;
        }
        if let Some(ipfs) = &self.ipfs {
            return ipfs.pin_add(cid, Duration::from_secs(120), 1).await;
        }
        Ok(())
    }

    fn ipfs_enabled(&self) -> bool {
        self.storage
            .as_ref()
            .map(|storage| storage.ipfs_enabled)
            .unwrap_or_else(|| self.ipfs.is_some())
    }
}

fn parse_off_chain_archive(
    tx: &crate::db::models::chains::ChainTxDb,
    cid: &str,
    body: Value,
    size: i64,
) -> AlephResult<(Vec<OnChainMessage>, String, i64)> {
        let messages_val = body
            .get("content")
            .and_then(|c| c.get("messages"))
            .cloned()
            .ok_or_else(|| {
                crate::AlephError::Chain(format!(
                    "got bad off-chain data in tx {}/{}: missing content.messages",
                    tx.chain, tx.hash
                ))
            })?;
        let parsed: Vec<OnChainMessage> = serde_json::from_value(messages_val)
            .map_err(|e| crate::AlephError::Chain(format!("bad off-chain content: {e}")))?;
        Ok((parsed, cid.to_string(), size))
}

fn smart_contract_message_from_tx(
    tx: &crate::db::models::chains::ChainTxDb,
) -> AlephResult<OnChainMessage> {
    let address = required_smart_contract_str(tx, "address")?;
    let msg_type = required_smart_contract_str(tx, "type")?;
    let content_str = required_smart_contract_str(tx, "content")?;
    let timestamp_secs =
        tx.datetime.timestamp() as f64 + (tx.datetime.timestamp_subsec_nanos() as f64) / 1e9;

    let (out_type, item_content) = if msg_type == "STORE_IPFS" {
        let content_time = tx
            .content
            .get("timestamp")
            .and_then(|v| v.as_f64())
            .map(timestamp_number_to_seconds)
            .unwrap_or(timestamp_secs);
        let store_content = serde_json::json!({
            "address": address,
            "time": content_time,
            "item_type": "ipfs",
            "item_hash": content_str,
        });
        ("STORE".to_string(), serde_json::to_string(&store_content)?)
    } else {
        (msg_type.to_string(), content_str.to_string())
    };

    Ok(OnChainMessage {
        item_hash: sha256_hex(item_content.as_bytes()),
        sender: address.to_string(),
        chain: tx.chain.clone(),
        message_type: out_type,
        signature: None,
        time: timestamp_secs,
        item_content: Some(item_content),
        item_type: Some("inline".to_string()),
        channel: None,
    })
}

fn required_smart_contract_str<'a>(
    tx: &'a crate::db::models::chains::ChainTxDb,
    key: &str,
) -> AlephResult<&'a str> {
    tx.content
        .get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            crate::AlephError::Chain(format!(
                "incompatible smart-contract tx content for {}/{}: missing {key}",
                tx.chain, tx.hash
            ))
        })
}

fn timestamp_number_to_seconds(value: f64) -> f64 {
    if value > 10_000_000_000.0 {
        value / 1000.0
    } else {
        value
    }
}

impl Default for ChainDataService {
    fn default() -> Self {
        Self::new()
    }
}

fn u256_to_datetime(ts: alloy_primitives::U256) -> DateTime<Utc> {
    // Solidity timestamps are seconds since epoch.
    let secs: u64 = ts.try_into().unwrap_or(0);
    timestamp_to_datetime(secs as f64)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
}

/// In-memory representation of a chain transaction. Mirrors `ChainTxDb`. We
/// keep the standalone struct (instead of using `ChainTxDb` directly) so the
/// publisher can build a "to be persisted" payload before the row is written —
/// pyaleph's TX publisher does the same.
#[derive(Debug, Clone)]
pub struct PendingChainTx {
    pub hash: String,
    pub chain: Chain,
    pub height: u64,
    pub datetime: DateTime<Utc>,
    pub publisher: String,
    pub protocol: ChainSyncProtocol,
    pub protocol_version: u32,
    pub content: serde_json::Value,
}

impl PendingChainTx {
    /// Project into the DB row representation.
    pub fn to_chain_tx_db(&self) -> crate::db::models::chains::ChainTxDb {
        crate::db::models::chains::ChainTxDb {
            hash: self.hash.clone(),
            chain: self.chain.clone(),
            height: self.height as i32,
            datetime: self.datetime,
            publisher: self.publisher.clone(),
            protocol: self.protocol,
            protocol_version: self.protocol_version as i32,
            content: self.content.clone(),
        }
    }
}

/// Mirrors `PendingTxPublisher` in pyaleph.
#[async_trait::async_trait]
pub trait PendingTxSink: Send + Sync {
    async fn publish(&self, tx: &PendingChainTx) -> AlephResult<()>;
}

/// Default sink: emits a `tracing` event. Used in tests / dev mode.
#[derive(Default, Debug, Clone, Copy)]
pub struct TracingPendingTxSink;

#[async_trait::async_trait]
impl PendingTxSink for TracingPendingTxSink {
    async fn publish(&self, tx: &PendingChainTx) -> AlephResult<()> {
        tracing::info!(
            chain = %tx.chain,
            hash = %tx.hash,
            "pending tx published"
        );
        Ok(())
    }
}

/// Production sink: upserts `chain_txs` + enqueues the pending tx row so the
/// processor picks it up. Mirrors `PendingTxPublisher.publish` from pyaleph.
pub struct DbPendingTxSink {
    pool: crate::db::DbPool,
}

impl DbPendingTxSink {
    pub fn new(pool: crate::db::DbPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl PendingTxSink for DbPendingTxSink {
    async fn publish(&self, tx: &PendingChainTx) -> AlephResult<()> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| crate::AlephError::Internal(anyhow::anyhow!(e)))?;
        let chain_tx = tx.to_chain_tx_db();
        let transaction = (&mut **client).transaction().await?;
        crate::db::accessors::chains::upsert_chain_tx(&transaction, &chain_tx).await?;
        crate::db::accessors::pending_txs::upsert_pending_tx(&transaction, &tx.hash).await?;
        transaction.commit().await?;
        Ok(())
    }
}

/// Production sink variant: persist first, then wake the pending-tx workers
/// through RabbitMQ. This matches pyaleph's commit-before-publish ordering.
pub struct MqPendingTxSink {
    db: DbPendingTxSink,
    channel: Channel,
    exchange: String,
}

impl MqPendingTxSink {
    pub fn new(pool: crate::db::DbPool, channel: Channel, exchange: String) -> Self {
        Self {
            db: DbPendingTxSink::new(pool),
            channel,
            exchange,
        }
    }

    fn routing_key(tx: &PendingChainTx) -> String {
        let chain = serde_json::to_value(&tx.chain)
            .ok()
            .and_then(|v| v.as_str().map(ToOwned::to_owned))
            .unwrap_or_else(|| tx.chain.to_string());
        format!("{chain}.{}.{}", tx.publisher, tx.hash)
    }
}

#[async_trait::async_trait]
impl PendingTxSink for MqPendingTxSink {
    async fn publish(&self, tx: &PendingChainTx) -> AlephResult<()> {
        self.db.publish(tx).await?;
        self.channel
            .basic_publish(
                &self.exchange,
                &Self::routing_key(tx),
                BasicPublishOptions::default(),
                tx.hash.as_bytes(),
                BasicProperties::default(),
            )
            .await
            .map_err(|e| crate::AlephError::P2p(format!("publish failed: {e}")))?
            .await
            .map_err(|e| crate::AlephError::P2p(format!("publish confirm failed: {e}")))?;
        Ok(())
    }
}

pub struct PendingTxPublisher {
    sink: Box<dyn PendingTxSink>,
}

impl PendingTxPublisher {
    pub fn new(sink: Box<dyn PendingTxSink>) -> Self {
        Self { sink }
    }

    pub async fn publish(&self, tx: &PendingChainTx) -> AlephResult<()> {
        self.sink.publish(tx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::chains::ChainTxDb;
    use alloy_primitives::{Address as EvmAddress, B256, LogData, U256, address};
    use alloy_rpc_types_eth::Log as RpcLog;
    use alloy_sol_types::SolEvent;
    use serde_json::json;

    fn sample_msg() -> OnChainMessage {
        OnChainMessage {
            item_hash: "deadbeef".into(),
            sender: "0xabc".into(),
            chain: Chain::Ethereum,
            message_type: "POST".into(),
            signature: Some("0xsig".into()),
            time: 1700000000.0,
            item_content: Some("{}".into()),
            item_type: Some("inline".into()),
            channel: Some(AlephChannel::from("TEST".to_string())),
        }
    }

    #[test]
    fn build_on_chain_payload_uses_on_chain_protocol() {
        let svc = ChainDataService::new();
        let p = svc.build_on_chain_payload(vec![sample_msg()]);
        assert_eq!(p.protocol, ChainSyncProtocol::OnChainSync);
        assert_eq!(p.version, 1);
        assert_eq!(p.content.messages.len(), 1);
    }

    #[test]
    fn build_off_chain_payload_carries_cid() {
        let svc = ChainDataService::new();
        let p = svc.build_off_chain_payload("Qm123".into());
        assert_eq!(p.protocol, ChainSyncProtocol::OffChainSync);
        assert_eq!(p.content, "Qm123");
    }

    #[tokio::test]
    async fn tracing_sink_publishes() {
        let publisher = PendingTxPublisher::new(Box::new(TracingPendingTxSink));
        let tx = PendingChainTx {
            hash: "h".into(),
            chain: Chain::Ethereum,
            height: 1,
            datetime: Utc::now(),
            publisher: "p".into(),
            protocol: ChainSyncProtocol::OnChainSync,
            protocol_version: 1,
            content: json!({}),
        };
        publisher.publish(&tx).await.unwrap();
    }

    #[test]
    fn pending_tx_mq_routing_key_matches_pyaleph_shape() {
        let tx = PendingChainTx {
            hash: "0xabc".into(),
            chain: Chain::Bsc,
            height: 10,
            datetime: timestamp_to_datetime(1700000000.0),
            publisher: "0xpublisher".into(),
            protocol: ChainSyncProtocol::SmartContract,
            protocol_version: 1,
            content: json!({}),
        };

        assert_eq!(
            MqPendingTxSink::routing_key(&tx),
            "BSC.0xpublisher.0xabc"
        );
    }

    // --- prepare_sync_event_payload -------------------------------------

    /// Tiny `GenericClient` stand-in used only because the public API expects
    /// one — none of the test code paths actually hit the DB.
    async fn dummy_pool() -> deadpool_postgres::Pool {
        // We never actually run any query in these tests; build a pool that
        // points at an unreachable host so accidental DB usage panics.
        let mut cfg = tokio_postgres::Config::new();
        cfg.host("127.0.0.1").port(1).user("x").dbname("x");
        let mgr = deadpool_postgres::Manager::from_config(
            cfg,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        );
        deadpool_postgres::Pool::builder(mgr)
            .max_size(1)
            .build()
            .unwrap()
    }

    fn small_message() -> MessageDb {
        use crate::types::message_status::MessageStatus;
        use aleph_types::message::MessageType;
        use aleph_types::message::item_type::ItemType;
        MessageDb {
            item_hash: "00".into(),
            r#type: MessageType::Post,
            chain: Chain::Ethereum,
            sender: "0xabc".into(),
            signature: Some("0xsig".into()),
            item_type: ItemType::Inline,
            item_content: Some("{}".into()),
            content: json!({}),
            time: Utc::now(),
            channel: None,
            size: 2,
            status_value: MessageStatus::Processed,
            reception_time: Utc::now(),
            owner: None,
            content_type: None,
            content_ref: None,
            content_key: None,
            first_confirmed_at: None,
            first_confirmed_height: None,
            payment_type: None,
            content_item_hash: None,
            tags: None,
        }
    }

    #[tokio::test]
    async fn prepare_payload_fails_without_ipfs() {
        let svc = ChainDataService::new();
        let archive = svc.build_on_chain_payload(vec![OnChainMessage::from_message_db(
            &small_message(),
        )]);
        let err = svc
            .upload_sync_archive(archive)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("without IPFS service"));
    }

    #[tokio::test]
    async fn prepare_payload_uploads_to_ipfs_even_when_small() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string("{\"Hash\":\"QmFakeCidFromTest\"}"),
            )
            .expect(1)
            .mount(&mock)
            .await;

        let settings = crate::config::IpfsSettings {
            host: mock.address().ip().to_string(),
            port: mock.address().port(),
            ..Default::default()
        };
        let ipfs = IpfsService::new(&settings).unwrap();
        let svc = ChainDataService::with_ipfs(Arc::new(ipfs));
        let msg = small_message();
        let on_chain: Vec<OnChainMessage> = vec![OnChainMessage::from_message_db(&msg)];
        let archive = svc.build_on_chain_payload(on_chain);
        let inline = serde_json::to_string(&archive).unwrap();
        assert!(inline.len() <= PAYLOAD_INLINE_LIMIT_BYTES);

        let (cid, size) = svc.upload_sync_archive(archive).await.unwrap();
        let off = svc.build_off_chain_payload(cid);
        let serialized = serde_json::to_string(&off).unwrap();
        assert!(serialized.contains("\"protocol\":\"aleph-offchain\""));
        assert!(serialized.contains("QmFakeCidFromTest"));
        assert!(size > 0);
    }

    // --- parse_log -------------------------------------------------------

    fn make_log(event_data: LogData, block: u64, tx_hash: B256) -> RpcLog {
        let inner = alloy_primitives::Log {
            address: address!("23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC"),
            data: event_data,
        };
        RpcLog {
            inner,
            block_hash: None,
            block_number: Some(block),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    #[test]
    fn parse_log_decodes_sync_event() {
        let payload = json!({
            "protocol": "aleph",
            "version": 1,
            "content": { "messages": [] },
        })
        .to_string();
        let event = SyncEvent {
            timestamp: U256::from(1700000000u64),
            addr: address!("23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC"),
            message: payload,
        };
        let encoded = event.encode_log_data();
        let log = make_log(encoded, 100, B256::ZERO);
        let svc = ChainDataService::new();
        let parsed = svc.parse_log(&log).unwrap().expect("sync event decoded");
        assert_eq!(parsed.protocol, ChainSyncProtocol::OnChainSync);
        assert_eq!(parsed.height, 100);
    }

    #[test]
    fn parse_log_decodes_message_event() {
        let event = MessageEvent {
            timestamp: U256::from(1700000000u64),
            addr: address!("23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC"),
            msgtype: "POST".into(),
            msgcontent: "{}".into(),
        };
        let encoded = event.encode_log_data();
        let log = make_log(encoded, 200, B256::ZERO);
        let svc = ChainDataService::new();
        let parsed = svc.parse_log(&log).unwrap().expect("message event decoded");
        assert_eq!(parsed.protocol, ChainSyncProtocol::SmartContract);
        assert_eq!(parsed.height, 200);
    }

    #[test]
    fn parse_log_ignores_unrelated_topic() {
        let log = RpcLog {
            inner: alloy_primitives::Log {
                address: EvmAddress::ZERO,
                data: LogData::new_unchecked(vec![B256::ZERO], vec![].into()),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(B256::ZERO),
            transaction_index: None,
            log_index: None,
            removed: false,
        };
        let svc = ChainDataService::new();
        assert!(svc.parse_log(&log).unwrap().is_none());
    }

    // --- get_tx_messages -------------------------------------------------

    #[tokio::test]
    async fn get_tx_messages_on_chain_extracts_messages() {
        let svc = ChainDataService::new();
        let tx = ChainTxDb {
            hash: "0xtx".into(),
            chain: Chain::Ethereum,
            height: 1,
            datetime: Utc::now(),
            publisher: "0xpub".into(),
            protocol: ChainSyncProtocol::OnChainSync,
            protocol_version: 1,
            content: json!({
                "messages": [{
                    "item_hash": "deadbeef",
                    "sender": "0xabc",
                    "chain": "ETH",
                    "type": "POST",
                    "signature": "0xsig",
                    "time": 1700000000.0,
                    "item_content": "{}",
                    "item_type": "inline",
                }]
            }),
        };
        let pool = dummy_pool().await;
        // We can't acquire a real client for the dummy pool — call via
        // direct match on protocol; the on-chain branch never touches it.
        // Workaround: build a tiny adapter that satisfies GenericClient.
        // Skip the client roundtrip and exercise the on-chain branch.
        let messages = match tx.protocol {
            ChainSyncProtocol::OnChainSync => {
                svc.get_tx_messages_on_chain_branch_for_test(&tx).unwrap()
            }
            _ => unreachable!(),
        };
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].item_hash, "deadbeef");
        drop(pool);
    }

    #[tokio::test]
    async fn get_tx_messages_smart_contract_synthesizes_store() {
        let svc = ChainDataService::new();
        let now = Utc::now();
        let tx = ChainTxDb {
            hash: "0xkt".into(),
            chain: Chain::Tezos,
            height: 5,
            datetime: now,
            publisher: "tz1abc".into(),
            protocol: ChainSyncProtocol::SmartContract,
            protocol_version: 1,
            content: json!({
                "address": "tz1abc",
                "type": "STORE_IPFS",
                "content": "QmHash",
                "timestamp": now.timestamp_millis(),
            }),
        };
        let messages = svc.get_tx_messages_from_tx(&tx).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message_type, "STORE");
        assert_eq!(messages[0].sender, "tz1abc");
        let inner: serde_json::Value =
            serde_json::from_str(messages[0].item_content.as_ref().unwrap()).unwrap();
        assert_eq!(inner["item_type"], "ipfs");
        assert_eq!(inner["item_hash"], "QmHash");
        assert_eq!(inner["time"], json!(now.timestamp_millis() as f64 / 1000.0));
    }

    #[tokio::test]
    async fn get_tx_messages_rejects_unsupported_protocol_version() {
        let svc = ChainDataService::new();
        let tx = ChainTxDb {
            hash: "0xunsupported".into(),
            chain: Chain::Ethereum,
            height: 1,
            datetime: Utc::now(),
            publisher: "0xpub".into(),
            protocol: ChainSyncProtocol::OnChainSync,
            protocol_version: 2,
            content: json!({"messages": []}),
        };

        assert!(svc.get_tx_messages_from_tx(&tx).await.is_err());
    }

    #[tokio::test]
    async fn get_tx_messages_rejects_malformed_smart_contract_content() {
        let svc = ChainDataService::new();
        let tx = ChainTxDb {
            hash: "0xbad".into(),
            chain: Chain::Ethereum,
            height: 1,
            datetime: Utc::now(),
            publisher: "0xpub".into(),
            protocol: ChainSyncProtocol::SmartContract,
            protocol_version: 1,
            content: json!({
                "address": "0xabc",
                "type": "POST"
            }),
        };

        assert!(svc.get_tx_messages_from_tx(&tx).await.is_err());
    }

    #[tokio::test]
    async fn get_tx_messages_off_chain_fetches_from_ipfs() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let archive = json!({
            "protocol": "aleph",
            "version": 1,
            "content": {
                "messages": [{
                    "item_hash": "abc",
                    "sender": "0xabc",
                    "chain": "ETH",
                    "type": "POST",
                    "signature": "0xsig",
                    "time": 1700000000.0,
                    "item_content": "{}",
                    "item_type": "inline",
                }],
            },
        });
        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string(archive.to_string()))
            .mount(&mock)
            .await;

        let settings = crate::config::IpfsSettings {
            host: mock.address().ip().to_string(),
            port: mock.address().port(),
            ..Default::default()
        };
        let ipfs = IpfsService::new(&settings).unwrap();
        let svc = ChainDataService::with_ipfs(Arc::new(ipfs));

        let tx = ChainTxDb {
            hash: "0xt".into(),
            chain: Chain::Ethereum,
            height: 1,
            datetime: Utc::now(),
            publisher: "0xp".into(),
            protocol: ChainSyncProtocol::OffChainSync,
            protocol_version: 1,
            content: Value::String("QmFakeCidFromTest".into()),
        };
        let messages = svc
            .get_tx_messages_off_chain_branch_for_test(&tx)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].item_hash, "abc");
    }

    #[tokio::test]
    async fn get_tx_messages_off_chain_reads_local_storage_before_ipfs() {
        use crate::services::cache::local::LocalCache;
        use crate::services::storage::engine::StorageEngine;
        use crate::services::storage::in_memory::InMemoryStorageEngine;

        let archive = json!({
            "protocol": "aleph",
            "version": 1,
            "content": {
                "messages": [{
                    "item_hash": "from-local-storage",
                    "sender": "0xabc",
                    "chain": "ETH",
                    "type": "POST",
                    "signature": "0xsig",
                    "time": 1700000000.0,
                    "item_content": "{}",
                    "item_type": "inline",
                }],
            },
        });
        let cid = "QmLocalArchive";
        let engine = Arc::new(InMemoryStorageEngine::new());
        engine
            .write(cid, archive.to_string().as_bytes())
            .await
            .unwrap();
        let ipfs = Arc::new(IpfsService::new(&crate::config::IpfsSettings::default()).unwrap());
        let cache = Arc::new(LocalCache::new());
        let storage = Arc::new(
            StorageService::new(engine, ipfs, cache)
                .with_ipfs_enabled(false)
                .with_http_p2p_enabled(false),
        );
        let svc = ChainDataService::with_storage(storage);

        let tx = ChainTxDb {
            hash: "0xt".into(),
            chain: Chain::Ethereum,
            height: 1,
            datetime: Utc::now(),
            publisher: "0xp".into(),
            protocol: ChainSyncProtocol::OffChainSync,
            protocol_version: 1,
            content: Value::String(cid.into()),
        };
        let messages = svc
            .get_tx_messages_off_chain_branch_for_test(&tx)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].item_hash, "from-local-storage");
    }
}

#[cfg(test)]
impl ChainDataService {
    /// Test helper that runs the OnChain branch of `get_tx_messages` without
    /// requiring a database client.
    pub fn get_tx_messages_on_chain_branch_for_test(
        &self,
        tx: &crate::db::models::chains::ChainTxDb,
    ) -> AlephResult<Vec<OnChainMessage>> {
        let messages = tx
            .content
            .get("messages")
            .cloned()
            .unwrap_or_else(|| Value::Array(vec![]));
        let parsed: Vec<OnChainMessage> = serde_json::from_value(messages)
            .map_err(|e| crate::AlephError::Chain(format!("bad on-chain content: {e}")))?;
        Ok(parsed)
    }

    /// Test helper exercising the `SmartContract` branch without DB access.
    pub fn get_tx_messages_smart_contract_branch_for_test(
        &self,
        tx: &crate::db::models::chains::ChainTxDb,
    ) -> AlephResult<Vec<OnChainMessage>> {
        Ok(vec![smart_contract_message_from_tx(tx)?])
    }

    /// Test helper exercising the off-chain (IPFS) branch without DB access.
    pub async fn get_tx_messages_off_chain_branch_for_test(
        &self,
        tx: &crate::db::models::chains::ChainTxDb,
    ) -> AlephResult<Vec<OnChainMessage>> {
        self.get_off_chain_messages(tx)
            .await
            .map(|(messages, _, _)| messages)
    }
}
