//! Ethereum connector. Mirrors `aleph/chains/ethereum.py`.
//!
//! The signature verifier is identical to `EvmVerifier` — pyaleph keeps the
//! class as a placeholder for future Ethereum-specific tweaks.
//!
//! `EthereumConnector` wires up the on-chain fetcher and packer using
//! `alloy_provider`. The smart-contract ABI is embedded via `include_str!`
//! from `src/chains/assets/ethereum_sc_abi.json`.

use std::sync::Arc;
use std::time::Duration;

use alloy_network::{EthereumWallet, TransactionBuilder};
use alloy_primitives::{Address as EvmAddress, Bytes, U256};
use alloy_provider::{DynProvider, Provider, ProviderBuilder};
use alloy_rpc_types_eth::{BlockNumberOrTag, Filter, Log, TransactionRequest};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolEvent, sol};
use async_trait::async_trait;
use chrono::Utc;

use super::abc::{ChainReader, ChainWriter, Verifier};
use super::chain_data_service::{ChainDataService, PendingTxPublisher};
use super::evm::EvmVerifier;
use crate::AlephError;
use crate::AlephResult;
use crate::config::Settings;
use crate::db::DbPool;
use crate::db::accessors::chains::{get_last_height, upsert_chain_sync_status};
use crate::types::chain_sync::{ChainEventType, ChainSyncProtocol};
use aleph_types::chain::Chain;

/// Embedded Ethereum sync contract ABI.
pub const SYNC_CONTRACT_ABI: &str = include_str!("assets/ethereum_sc_abi.json");

// Solidity event declarations — these match `assets/ethereum_sc_abi.json` and
// let us decode logs with strongly typed structs.
sol! {
    /// Emitted on every Aleph sync push.
    event SyncEvent(uint256 timestamp, address addr, string message);

    /// Emitted by the smart contract for each individual Aleph message.
    event MessageEvent(uint256 timestamp, address addr, string msgtype, string msgcontent);

    /// `doEmit(string)` is the sync-publication entry-point on the Aleph SC.
    function doEmit(string content) external;
}

/// Re-exports `EvmVerifier` — Python keeps `EthereumVerifier(EVMVerifier)`
/// as a subclass placeholder.
#[derive(Default, Debug, Clone, Copy)]
pub struct EthereumVerifier(pub EvmVerifier);

#[async_trait]
impl Verifier for EthereumVerifier {
    async fn verify_signature(
        &self,
        message: &dyn super::abc::PendingMessageView,
    ) -> AlephResult<bool> {
        self.0.verify_signature(message).await
    }
}

/// Top-level Ethereum connector. Holds the provider, contract address, and
/// configuration values needed by the fetcher / packer loops.
pub struct EthereumConnector {
    pub provider: Arc<DynProvider>,
    pub contract_address: EvmAddress,
    pub authorized_emitters: Vec<EvmAddress>,
    pub max_gas_price: u64,
    pub start_height: u64,
    pub max_block_range: u64,
    pub pool: Option<DbPool>,
    pub chain_data_service: Arc<ChainDataService>,
    pub pending_tx_publisher: Arc<PendingTxPublisher>,
}

impl EthereumConnector {
    /// Builds the connector from the runtime `Settings`. Mirrors
    /// `EthereumConnector.new` in pyaleph.
    pub async fn new(cfg: &Settings) -> AlephResult<Self> {
        let url: url::Url =
            cfg.ethereum.api_url.parse().map_err(|e: url::ParseError| {
                AlephError::Config(format!("ethereum.api_url: {e}"))
            })?;
        let provider = ProviderBuilder::new().connect_http(url).erased();

        let sync_contract = cfg
            .ethereum
            .sync_contract
            .as_deref()
            .ok_or_else(|| AlephError::Config("ethereum.sync_contract is required".into()))?;
        let contract_address: EvmAddress = sync_contract
            .parse()
            .map_err(|e| AlephError::Config(format!("ethereum.sync_contract: {e}")))?;

        let authorized_emitters: Vec<EvmAddress> = cfg
            .ethereum
            .authorized_emitters
            .iter()
            .map(|s| {
                s.parse::<EvmAddress>()
                    .map_err(|e| AlephError::Config(format!("ethereum.authorized_emitters: {e}")))
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            provider: Arc::new(provider),
            contract_address,
            authorized_emitters,
            max_gas_price: cfg.ethereum.max_gas_price,
            start_height: cfg.ethereum.start_height,
            max_block_range: cfg.ethereum.max_block_range,
            pool: None,
            chain_data_service: Arc::new(ChainDataService::new()),
            pending_tx_publisher: Arc::new(PendingTxPublisher::new(Box::new(
                super::chain_data_service::TracingPendingTxSink,
            ))),
        })
    }

    /// Wire up DB + chain data service + pending tx publisher. Used by the
    /// `ChainConnector` orchestrator once those services are available.
    pub fn with_services(
        mut self,
        pool: DbPool,
        chain_data_service: Arc<ChainDataService>,
        pending_tx_publisher: Arc<PendingTxPublisher>,
    ) -> Self {
        self.pool = Some(pool);
        self.chain_data_service = chain_data_service;
        self.pending_tx_publisher = pending_tx_publisher;
        self
    }

    /// Returns the SyncEvent topic hash used to filter logs.
    pub fn sync_event_topic() -> alloy_primitives::FixedBytes<32> {
        SyncEvent::SIGNATURE_HASH
    }

    /// Pulls logs for any Aleph event (SyncEvent + MessageEvent) in
    /// `[from_block, to_block]`.
    pub async fn fetch_logs(
        &self,
        from_block: u64,
        to_block: BlockNumberOrTag,
    ) -> AlephResult<Vec<Log>> {
        let filter = Filter::new()
            .address(self.contract_address)
            .from_block(from_block)
            .to_block(to_block);

        self.provider
            .get_logs(&filter)
            .await
            .map_err(|e| AlephError::Chain(format!("eth get_logs: {e}")))
    }

    /// Pulls logs for `SyncEvent` in `[from_block, to_block]`.
    pub async fn fetch_sync_logs(
        &self,
        from_block: u64,
        to_block: BlockNumberOrTag,
    ) -> AlephResult<Vec<Log>> {
        let filter = Filter::new()
            .address(self.contract_address)
            .event_signature(Self::sync_event_topic())
            .from_block(from_block)
            .to_block(to_block);

        self.provider
            .get_logs(&filter)
            .await
            .map_err(|e| AlephError::Chain(format!("eth get_logs: {e}")))
    }

    /// Decodes the SyncEvent payload from a raw log.
    pub fn decode_sync_event(log: &Log) -> Option<SyncEvent> {
        SyncEvent::decode_log(&log.inner).ok().map(|d| d.data)
    }

    /// Polling interval helper — yields after `delay`.
    async fn sleep(delay: Duration) {
        tokio::time::sleep(delay).await;
    }

    /// Drain unconfirmed messages from the DB into a Vec, respecting the
    /// configured `max_unconfirmed_messages` cap. Mirrors the pagination loop
    /// in pyaleph's `packer()`.
    async fn collect_unconfirmed_messages(
        &self,
        max_unconfirmed: usize,
    ) -> AlephResult<Vec<crate::db::models::messages::MessageDb>> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| AlephError::Chain("ethereum connector missing DbPool".into()))?;
        let client = pool
            .get()
            .await
            .map_err(|e| AlephError::Pool(format!("pool acquire: {e}")))?;
        let mut all: Vec<crate::db::models::messages::MessageDb> = Vec::new();
        let mut offset = 0i64;
        let batch_size = 500i64;
        loop {
            let batch = crate::db::accessors::messages::get_unconfirmed_messages(
                &**client, batch_size, offset,
            )
            .await?;
            if batch.is_empty() {
                break;
            }
            offset += batch.len() as i64;
            let len = batch.len();
            all.extend(batch);
            if all.len() >= max_unconfirmed || (len as i64) < batch_size {
                break;
            }
        }
        if all.len() > max_unconfirmed {
            all.truncate(max_unconfirmed);
        }
        Ok(all)
    }
}

#[async_trait]
impl ChainReader for EthereumConnector {
    async fn fetcher(&self, cfg: &Settings) -> AlephResult<()> {
        let poll = Duration::from_secs(cfg.ethereum.message_delay.max(1));
        let authorized: std::collections::HashSet<EvmAddress> =
            self.authorized_emitters.iter().copied().collect();

        // Resume from the last persisted height when available. Mirrors
        // pyaleph's `get_last_height()` boot path.
        let mut next_block = self.read_persisted_start_height().await;

        loop {
            // Fetch logs in `[next_block, latest]` with a progressive shrink
            // loop. Some RPCs respond with `-32005 TooManyLogsInRange` (or
            // similar provider-specific codes) when the range is too wide; in
            // that case we halve the requested range and retry until either
            // the batch succeeds or the range drops to a single block.
            let latest = match self.provider.get_block_number().await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(error = %e, "ETH fetcher: eth_blockNumber failed");
                    Self::sleep(poll).await;
                    continue;
                }
            };
            if next_block > latest {
                Self::sleep(poll).await;
                continue;
            }

            let mut range = self.max_block_range.max(1);
            let mut window_start = next_block;
            while window_start <= latest {
                let window_end = window_start
                    .saturating_add(range.saturating_sub(1))
                    .min(latest);
                let logs_res = self
                    .fetch_logs(window_start, BlockNumberOrTag::Number(window_end))
                    .await;
                let logs = match logs_res {
                    Ok(l) => l,
                    Err(e) => {
                        let msg = e.to_string();
                        // Provider-specific signals: alchemy returns -32005,
                        // infura/quicknode return "query returned more than ...
                        // logs" / "block range is too wide". Treat all of them
                        // as TooManyLogsInRange and halve the window.
                        if msg.contains("-32005")
                            || msg.to_lowercase().contains("too many")
                            || msg.to_lowercase().contains("range is too wide")
                            || msg.to_lowercase().contains("query returned more")
                        {
                            if range <= 1 {
                                tracing::error!(
                                    "ETH fetcher: stuck at block {window_start}: {e}"
                                );
                                break;
                            }
                            range /= 2;
                            tracing::info!(
                                window_start, window_end, new_range = range,
                                "ETH fetcher: shrinking block range after TooManyLogsInRange"
                            );
                            continue;
                        }
                        tracing::warn!(error = %e, "ETH fetcher: get_logs failed");
                        break;
                    }
                };
                self.publish_window_logs(&logs, &authorized).await?;
                // Persist the sync progress so subsequent restarts pick up
                // where we left off. Mirrors `upsert_chain_sync_status` in
                // pyaleph's `_request_transactions`.
                next_block = window_end.saturating_add(1);
                self.write_persisted_progress(window_end).await;
                window_start = next_block;
                // After a success, gradually expand back to the configured
                // ceiling (capped at `max_block_range`).
                range = (range.saturating_mul(2)).min(self.max_block_range.max(1));
            }
            Self::sleep(poll).await;
        }
    }
}

impl EthereumConnector {
    async fn publish_window_logs(
        &self,
        logs: &[Log],
        authorized: &std::collections::HashSet<EvmAddress>,
    ) -> AlephResult<()> {
        for log in logs {
            let pending = match self.chain_data_service.parse_log(log) {
                Ok(Some(p)) => p,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(error = %e, "ETH fetcher: parse_log error");
                    continue;
                }
            };
            if !authorized.is_empty() && pending.protocol != ChainSyncProtocol::SmartContract {
                let publisher_addr = pending.publisher.parse::<EvmAddress>().ok();
                if let Some(addr) = publisher_addr {
                    if !authorized.contains(&addr) {
                        tracing::trace!(%pending.publisher, "ETH: unauthorized emitter");
                        continue;
                    }
                }
            }
            self.pending_tx_publisher.publish(&pending).await?;
        }
        Ok(())
    }

    async fn read_persisted_start_height(&self) -> u64 {
        let Some(pool) = self.pool.as_ref() else {
            return self.start_height;
        };
        let Ok(client) = pool.get().await else {
            return self.start_height;
        };
        match get_last_height(&**client, Chain::Ethereum, ChainEventType::Sync).await {
            Ok(Some(h)) if (h as u64) > self.start_height => h.saturating_add(1) as u64,
            _ => self.start_height,
        }
    }

    async fn write_persisted_progress(&self, height: u64) {
        let Some(pool) = self.pool.as_ref() else { return };
        let Ok(client) = pool.get().await else { return };
        // `chains_sync_status.height` is i32 in the schema; truncate but never
        // wrap below zero.
        let height_i32 = i32::try_from(height).unwrap_or(i32::MAX);
        if let Err(e) = upsert_chain_sync_status(
            &**client,
            Chain::Ethereum,
            ChainEventType::Sync,
            height_i32,
            Utc::now(),
        )
        .await
        {
            tracing::warn!(error = %e, "ETH fetcher: upsert_chain_sync_status failed");
        }
    }
}

#[async_trait]
impl ChainWriter for EthereumConnector {
    async fn packer(&self, cfg: &Settings) -> AlephResult<()> {
        if !cfg.ethereum.packing_node {
            tracing::info!("ETH packing disabled (config.ethereum.packing_node = false)");
            return Ok(());
        }
        let pk = cfg.ethereum.private_key.as_deref().ok_or_else(|| {
            AlephError::Config("ethereum.packing_node requires ethereum.private_key".into())
        })?;
        let signer: PrivateKeySigner = pk
            .parse()
            .map_err(|e| AlephError::Config(format!("ethereum.private_key: {e}")))?;
        let wallet = EthereumWallet::from(signer.clone());
        let from_addr = signer.address();
        tracing::info!(%from_addr, "ETH packer started");

        let chain_id = cfg.ethereum.chain_id;
        let commit_delay = Duration::from_secs(cfg.ethereum.commit_delay);
        let max_gas_price = U256::from(self.max_gas_price);
        let max_unconfirmed = cfg.aleph.jobs.max_unconfirmed_messages as usize;
        let receipt_timeout = Duration::from_secs(30);

        loop {
            // Get current gas price.
            let gas_price = self
                .provider
                .get_gas_price()
                .await
                .map_err(|e| AlephError::Chain(format!("eth_gasPrice: {e}")))?;
            let gas_price_u256 = U256::from(gas_price);
            if gas_price_u256 > max_gas_price {
                tracing::warn!(
                    %gas_price_u256,
                    %max_gas_price,
                    "ETH packer: gas price too high; sleeping",
                );
                Self::sleep(commit_delay).await;
                continue;
            }

            // Drain unconfirmed messages.
            let messages = match self.collect_unconfirmed_messages(max_unconfirmed).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "ETH packer: failed to collect unconfirmed");
                    Self::sleep(commit_delay).await;
                    continue;
                }
            };

            if messages.is_empty() {
                Self::sleep(commit_delay).await;
                continue;
            }

            tracing::info!(count = messages.len(), "ETH packer: broadcasting batch");

            // Build payload (inline or off-chain CID) via the chain data service.
            let pool = match &self.pool {
                Some(p) => p,
                None => {
                    tracing::error!("ETH packer: missing DbPool; skipping");
                    Self::sleep(commit_delay).await;
                    continue;
                }
            };
            let payload = {
                let client = match pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(error = %e, "ETH packer: pool acquire failed");
                        Self::sleep(commit_delay).await;
                        continue;
                    }
                };
                match self
                    .chain_data_service
                    .prepare_sync_event_payload(&**client, messages)
                    .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(error = %e, "ETH packer: prepare payload failed");
                        Self::sleep(commit_delay).await;
                        continue;
                    }
                }
            };

            let call = doEmitCall { content: payload };
            let calldata: Bytes = call.abi_encode().into();

            let nonce = match self.provider.get_transaction_count(from_addr).await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(error = %e, "ETH packer: get_transaction_count failed");
                    Self::sleep(commit_delay).await;
                    continue;
                }
            };

            let tx = TransactionRequest::default()
                .with_from(from_addr)
                .with_to(self.contract_address)
                .with_input(calldata)
                .with_nonce(nonce)
                .with_chain_id(chain_id)
                .with_gas_price((gas_price * 11) / 10);

            let gas_limit = match self.provider.estimate_gas(tx.clone()).await {
                Ok(gas) => gas,
                Err(e) => {
                    tracing::warn!(error = %e, "ETH packer: estimate_gas failed");
                    Self::sleep(commit_delay).await;
                    continue;
                }
            };

            let signed_tx = match tx.with_gas_limit(gas_limit).build(&wallet).await {
                Ok(envelope) => envelope,
                Err(e) => {
                    tracing::warn!(error = %e, "ETH packer: local signing failed");
                    Self::sleep(commit_delay).await;
                    continue;
                }
            };

            // pyaleph signs locally with the configured private key and
            // broadcasts the raw transaction.
            let pending_tx = match self.provider.send_tx_envelope(signed_tx).await {
                Ok(pending) => pending,
                Err(e) => {
                    tracing::warn!(error = %e, "ETH packer: broadcast failed");
                    Self::sleep(commit_delay).await;
                    continue;
                }
            };
            let tx_hash = pending_tx.tx_hash().to_string();
            tracing::info!(%tx_hash, "ETH packer: broadcast tx");

            // Wait for receipt with timeout. We poll get_transaction_receipt
            // rather than rely on the alloy "watcher" feature so the loop
            // remains responsive to cancellation.
            let receipt_fut = async {
                loop {
                    match self
                        .provider
                        .get_transaction_receipt(*pending_tx.tx_hash())
                        .await
                    {
                        Ok(Some(r)) => break Ok::<_, AlephError>(Some(r)),
                        Ok(None) => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "ETH packer: get_transaction_receipt failed");
                            break Ok(None);
                        }
                    }
                }
            };
            match tokio::time::timeout(receipt_timeout, receipt_fut).await {
                Ok(Ok(Some(r))) => {
                    tracing::info!(tx_hash = %tx_hash, block = r.block_number, "ETH packer: receipt")
                }
                Ok(Ok(None)) => tracing::warn!(%tx_hash, "ETH packer: no receipt yet"),
                Ok(Err(e)) => tracing::warn!(error = %e, "ETH packer: receipt error"),
                Err(_) => tracing::warn!(%tx_hash, "ETH packer: receipt timed out"),
            }

            Self::sleep(commit_delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::abc::SimplePendingMessage;
    use crate::chains::chain_data_service::{PendingChainTx, PendingTxSink};
    use alloy_primitives::{B256, address};
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;
    use url::Url;

    struct FailingPendingTxSink;

    #[async_trait::async_trait]
    impl PendingTxSink for FailingPendingTxSink {
        async fn publish(&self, _tx: &PendingChainTx) -> AlephResult<()> {
            Err(AlephError::P2p("forced publish failure".into()))
        }
    }

    fn make_sync_log(block: u64) -> Log {
        let payload = serde_json::json!({
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
        Log {
            inner: alloy_primitives::Log {
                address: address!("23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC"),
                data: event.encode_log_data(),
            },
            block_hash: None,
            block_number: Some(block),
            block_timestamp: None,
            transaction_hash: Some(B256::ZERO),
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    fn test_connector_with_sink(sink: Box<dyn PendingTxSink>) -> EthereumConnector {
        let url: Url = "http://127.0.0.1:1".parse().unwrap();
        EthereumConnector {
            provider: Arc::new(ProviderBuilder::new().connect_http(url).erased()),
            contract_address: address!("23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC"),
            authorized_emitters: Vec::new(),
            max_gas_price: 0,
            start_height: 0,
            max_block_range: 100,
            pool: None,
            chain_data_service: Arc::new(ChainDataService::new()),
            pending_tx_publisher: Arc::new(PendingTxPublisher::new(sink)),
        }
    }

    #[test]
    fn sync_event_topic_is_keccak() {
        // Just ensure the constant builds and is non-zero.
        let topic = EthereumConnector::sync_event_topic();
        assert_ne!(topic.0, [0u8; 32]);
    }

    #[test]
    fn embedded_abi_is_valid_json() {
        let parsed: serde_json::Value = serde_json::from_str(SYNC_CONTRACT_ABI).unwrap();
        assert!(parsed.is_array());
    }

    #[tokio::test]
    async fn ethereum_verifier_delegates_to_evm() {
        let msg = SimplePendingMessage {
            chain: Chain::Ethereum,
            sender: "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".into(),
            message_type: MessageType::Post,
            item_hash: "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c".into(),
            signature: Some("0x636728dbea33fa6064f099045421b980dff3c71932cd89c2bf42387ebb6f53890a24e13f16678463876224772b90838c2b9557cd8fc2cdae45509ce8cb89e3fd1b".into()),
            time_seconds: 1762515431.653,
        };
        let v = EthereumVerifier::default();
        assert!(v.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn publish_window_logs_returns_publish_failure() {
        let connector = test_connector_with_sink(Box::new(FailingPendingTxSink));
        let logs = vec![make_sync_log(10)];
        let authorized = std::collections::HashSet::new();

        let err = connector
            .publish_window_logs(&logs, &authorized)
            .await
            .unwrap_err();

        assert!(format!("{err:?}").contains("forced publish failure"));
    }

    #[tokio::test]
    async fn packer_transaction_is_locally_signed() {
        let signer: PrivateKeySigner =
            "0x59c6995e998f97a5a0044966f094538b28132d1b90e7d6f7b8e3f4ec8e3f5f6b"
                .parse()
                .unwrap();
        let from_addr = signer.address();
        let wallet = EthereumWallet::from(signer);
        let contract_address: EvmAddress = "0x0000000000000000000000000000000000000001"
            .parse()
            .unwrap();

        let calldata: Bytes = doEmitCall {
            content: "{\"protocol\":\"aleph-offchain\",\"version\":1,\"content\":\"cid\"}".into(),
        }
        .abi_encode()
        .into();
        let tx = TransactionRequest::default()
            .with_from(from_addr)
            .with_to(contract_address)
            .with_input(calldata)
            .with_nonce(7)
            .with_chain_id(31337)
            .with_gas_price(1_000_000_000)
            .with_gas_limit(100_000);

        let envelope = tx.build(&wallet).await.unwrap();
        assert_ne!(*envelope.tx_hash(), alloy_primitives::B256::ZERO);
    }
}
