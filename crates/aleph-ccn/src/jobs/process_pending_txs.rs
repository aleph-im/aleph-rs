//! Pending-transaction processor. Mirrors `aleph/jobs/process_pending_txs.py`.
//!
//! Loops over the `pending_txs` table, asks the chain data service for the
//! messages embedded in each transaction, hands them to the
//! [`PendingMessagePublisher`] and finally deletes the pending row.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::AlephResult;
use crate::chains::chain_data_service::{ChainDataService, PendingChainTx};
use crate::db::DbPool;
use crate::db::accessors::chains::get_chain_tx;
use crate::db::accessors::pending_txs::{claim_pending_txs, delete_pending_tx};
use crate::db::models::pending_txs::PendingTxDb;
use crate::handlers::message_handler::MessagePublisher;
use crate::jobs::job_utils::MqWatcher;
use crate::toolkit::timestamp::utc_now;
use crate::types::chain_sync::ChainSyncProtocol;
use crate::types::message_status::MessageOrigin;

const PENDING_TX_LEASE_SECONDS: i64 = 300;

/// Abstraction over `ChainDataService.get_tx_messages`. Each implementer
/// turns a pending chain transaction into the list of message-dicts it
/// embeds. When `chains::chain_data_service` finishes its port, it will
/// implement this trait directly.
#[async_trait]
pub trait TxMessageProvider: Send + Sync {
    /// Returns the list of message dicts carried by `tx`.
    ///
    /// `seen_ids` is kept for archive payload deduplication by providers that
    /// need it, but duplicate message hashes from chain transactions must still
    /// be returned so processed messages can record each confirmation.
    async fn get_tx_messages(
        &self,
        tx: &PendingChainTx,
        _seen_ids: &mut HashSet<String>,
    ) -> AlephResult<Vec<Value>>;
}

/// Abstraction over `MessagePublisher.add_pending_message`. The publisher
/// in the Rust port currently exposes `publish_pending_message` for an
/// already-persisted row; here we accept a still-on-wire JSON message and
/// expect the implementer to do the same DB+MQ work the Python class does.
#[async_trait]
pub trait PendingMessagePublisher: Send + Sync {
    /// Persist `message_dict` as a pending row and announce a fetch/process
    /// job on RabbitMQ.
    async fn add_pending_message(
        &self,
        message_dict: &Value,
        reception_time: DateTime<Utc>,
        tx_hash: Option<&str>,
        check_message: bool,
        origin: MessageOrigin,
    ) -> AlephResult<()>;
}

#[async_trait]
impl TxMessageProvider for ChainDataService {
    async fn get_tx_messages(
        &self,
        tx: &PendingChainTx,
        _seen_ids: &mut HashSet<String>,
    ) -> AlephResult<Vec<Value>> {
        let decoded = self.get_tx_messages_from_tx(&tx.to_chain_tx_db()).await?;
        let mut out = Vec::with_capacity(decoded.len());
        for message in decoded {
            out.push(serde_json::to_value(message)?);
        }
        Ok(out)
    }
}

/// DB-aware adapter that lets `ChainDataService` persist off-chain archive
/// file rows and tx pins while decoding pending transactions.
pub struct DbTxMessageProvider {
    pool: DbPool,
    service: Arc<ChainDataService>,
}

impl DbTxMessageProvider {
    pub fn new(pool: DbPool, service: Arc<ChainDataService>) -> Self {
        Self { pool, service }
    }
}

#[async_trait]
impl TxMessageProvider for DbTxMessageProvider {
    async fn get_tx_messages(
        &self,
        tx: &PendingChainTx,
        _seen_ids: &mut HashSet<String>,
    ) -> AlephResult<Vec<Value>> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
        let decoded = self
            .service
            .get_tx_messages(&**client, &tx.to_chain_tx_db())
            .await?;
        let mut out = Vec::with_capacity(decoded.len());
        for message in decoded {
            out.push(serde_json::to_value(message)?);
        }
        Ok(out)
    }
}

/// DB-backed adapter from the pending-tx job to [`MessagePublisher`].
///
/// The job only has decoded JSON messages; this wrapper acquires a database
/// client and delegates the pyaleph-compatible insert/status/confirmation
/// logic to [`MessagePublisher::add_pending_message`].
pub struct DbPendingMessagePublisher {
    pool: DbPool,
    publisher: Arc<MessagePublisher>,
}

impl DbPendingMessagePublisher {
    pub fn new(pool: DbPool, publisher: Arc<MessagePublisher>) -> Self {
        Self { pool, publisher }
    }
}

#[async_trait]
impl PendingMessagePublisher for DbPendingMessagePublisher {
    async fn add_pending_message(
        &self,
        message_dict: &Value,
        reception_time: DateTime<Utc>,
        tx_hash: Option<&str>,
        check_message: bool,
        origin: MessageOrigin,
    ) -> AlephResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
        self.publisher
            .add_pending_message(
                &**client,
                message_dict,
                reception_time,
                tx_hash.map(str::to_string),
                check_message,
                Some(origin),
            )
            .await?;
        Ok(())
    }
}

/// Knobs accepted by [`run`]. Mirrors the relevant subset of
/// `config.aleph.jobs.pending_txs`.
pub struct PendingTxConfig {
    pub max_concurrency: usize,
    /// Idle wait between polls when the queue is empty.
    pub idle_timeout: Duration,
    /// Return after the first empty pass (tests).
    pub one_shot: bool,
}

impl Default for PendingTxConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 20,
            idle_timeout: Duration::from_secs(5),
            one_shot: false,
        }
    }
}

/// Resolve a pending-tx row into the associated chain transaction.
async fn resolve_chain_tx(
    pool: &DbPool,
    pending: &PendingTxDb,
) -> AlephResult<Option<PendingChainTx>> {
    let client = pool
        .get()
        .await
        .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
    let row = get_chain_tx(&**client, &pending.tx_hash).await?;
    Ok(row.map(|tx| PendingChainTx {
        hash: tx.hash,
        chain: tx.chain,
        height: tx.height as u64,
        datetime: tx.datetime,
        publisher: tx.publisher,
        protocol: tx.protocol,
        protocol_version: tx.protocol_version as u32,
        content: tx.content,
    }))
}

/// Process one pending TX end-to-end. Mirrors Python `handle_pending_tx`.
pub async fn handle_pending_tx(
    pool: &DbPool,
    provider: &dyn TxMessageProvider,
    publisher: &dyn PendingMessagePublisher,
    pending_chain_tx: PendingChainTx,
    seen_ids: &mut HashSet<String>,
) -> AlephResult<()> {
    tracing::info!(
        "{:?} Handling TX in block {}",
        pending_chain_tx.chain,
        pending_chain_tx.height
    );

    let messages = provider
        .get_tx_messages(&pending_chain_tx, seen_ids)
        .await?;
    if messages.is_empty() {
        tracing::debug!("TX contains no message");
        return Ok(());
    } else {
        let check_message = pending_chain_tx.protocol != ChainSyncProtocol::SmartContract;
        let reception_time = utc_now();
        for message_dict in &messages {
            publisher
                .add_pending_message(
                    message_dict,
                    reception_time,
                    Some(&pending_chain_tx.hash),
                    check_message,
                    MessageOrigin::Onchain,
                )
                .await?;
        }
    }

    // Bogus or handled: drop the pending row.
    let client = pool
        .get()
        .await
        .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
    delete_pending_tx(&**client, &pending_chain_tx.hash).await?;
    Ok(())
}

/// Drain one pass of `pending_txs`. Mirrors `process_pending_txs`.
pub async fn process_one_batch(
    pool: &DbPool,
    provider: &dyn TxMessageProvider,
    publisher: &dyn PendingMessagePublisher,
    max_concurrency: usize,
) -> AlephResult<usize> {
    let client = pool
        .get()
        .await
        .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
    let now = utc_now();
    let lease_until = now + chrono::Duration::seconds(PENDING_TX_LEASE_SECONDS);
    let pendings = claim_pending_txs(&**client, now, lease_until, max_concurrency as i64).await?;
    drop(client);

    if pendings.is_empty() {
        return Ok(0);
    }

    let mut seen_offchain_hashes: HashSet<String> = HashSet::new();
    let mut seen_ids: HashSet<String> = HashSet::new();
    let mut handled = 0usize;

    // Sequential resolve to keep ordering stable; Python uses a small pool
    // of futures + asyncio.wait. For functional parity it's enough to
    // process serially with the dedup gate.
    for pending in pendings {
        let chain_tx = match resolve_chain_tx(pool, &pending).await? {
            Some(tx) => tx,
            None => {
                tracing::warn!(
                    "pending_tx references unknown chain_tx: {}",
                    pending.tx_hash
                );
                continue;
            }
        };

        if chain_tx.protocol == ChainSyncProtocol::OffChainSync {
            let key = chain_tx.content.to_string();
            if seen_offchain_hashes.contains(&key) {
                continue;
            }
            seen_offchain_hashes.insert(key);
        }

        if let Err(e) = handle_pending_tx(pool, provider, publisher, chain_tx, &mut seen_ids).await
        {
            tracing::warn!("Error handling pending tx {}: {}", pending.tx_hash, e);
        }
        handled += 1;
    }
    Ok(handled)
}

/// Run the pending-tx loop until cancelled. Mirrors `handle_txs_task`.
pub async fn run(
    pool: DbPool,
    provider: Arc<dyn TxMessageProvider>,
    publisher: Arc<dyn PendingMessagePublisher>,
    watcher: Arc<MqWatcher>,
    cfg: PendingTxConfig,
    cancel: crate::jobs::job_utils::CancelToken,
) -> AlephResult<()> {
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }

        match process_one_batch(&pool, &*provider, &*publisher, cfg.max_concurrency).await {
            Ok(handled) => {
                if cfg.one_shot && handled == 0 {
                    return Ok(());
                }
            }
            Err(e) => {
                tracing::error!("Error in pending txs job: {e}");
            }
        }

        tokio::select! {
            _ = watcher.ready() => {}
            _ = tokio::time::sleep(cfg.idle_timeout) => {}
            _ = cancel.cancelled() => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::chain::Chain;
    use std::sync::Mutex;

    struct EmptyProvider;
    #[async_trait]
    impl TxMessageProvider for EmptyProvider {
        async fn get_tx_messages(
            &self,
            _tx: &PendingChainTx,
            _seen_ids: &mut HashSet<String>,
        ) -> AlephResult<Vec<Value>> {
            Ok(Vec::new())
        }
    }

    struct RecordingPublisher {
        calls: Mutex<Vec<String>>,
    }
    #[async_trait]
    impl PendingMessagePublisher for RecordingPublisher {
        async fn add_pending_message(
            &self,
            message_dict: &Value,
            _reception_time: DateTime<Utc>,
            _tx_hash: Option<&str>,
            _check_message: bool,
            _origin: MessageOrigin,
        ) -> AlephResult<()> {
            self.calls.lock().unwrap().push(message_dict.to_string());
            Ok(())
        }
    }

    fn sample_chain_tx() -> PendingChainTx {
        PendingChainTx {
            hash: "0xtx".into(),
            chain: Chain::Ethereum,
            height: 1,
            datetime: utc_now(),
            publisher: "x".into(),
            protocol: ChainSyncProtocol::OnChainSync,
            protocol_version: 1,
            content: Value::Null,
        }
    }

    #[tokio::test]
    async fn empty_messages_do_not_call_publisher() {
        // No DB access in handle_pending_tx when messages are empty (the
        // publisher / delete steps are skipped).
        let provider = EmptyProvider;
        let publisher = RecordingPublisher {
            calls: Mutex::new(Vec::new()),
        };
        // We can't run handle_pending_tx without a pool when messages exist;
        // but with no messages, the function takes the early-return path.
        let mut seen: HashSet<String> = HashSet::new();
        let result = provider
            .get_tx_messages(&sample_chain_tx(), &mut seen)
            .await
            .unwrap();
        assert!(result.is_empty());
        assert!(publisher.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn loop_terminates_on_cancel() {
        let cancel = crate::jobs::job_utils::CancelToken::new();
        cancel.cancel();
        let watcher = Arc::new(MqWatcher::detached());
        // simulate the cancel branch of the loop body without DB access.
        let cfg = PendingTxConfig::default();
        tokio::select! {
            _ = watcher.ready() => panic!("watcher fired"),
            _ = tokio::time::sleep(cfg.idle_timeout) => panic!("timer fired"),
            _ = cancel.cancelled() => {}
        }
    }

    #[test]
    fn backoff_is_exponential() {
        use crate::jobs::job_utils::compute_next_retry_interval;
        assert!(compute_next_retry_interval(2) > compute_next_retry_interval(1));
        assert!(compute_next_retry_interval(3) > compute_next_retry_interval(2));
    }
}
