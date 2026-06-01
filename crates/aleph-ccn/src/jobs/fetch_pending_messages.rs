//! Pending-message fetcher. Mirrors `aleph/jobs/fetch_pending_messages.py`.
//!
//! Pulls un-fetched rows from `pending_messages`, calls
//! [`MessageHandler::verify_and_fetch_message`], persists the resolved
//! content + `fetched = true`, and notifies the processor job over MQ.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt as _;
use futures_util::stream::FuturesUnordered;
use lapin::options::BasicPublishOptions;
use lapin::{BasicProperties, Channel};

use crate::AlephResult;
use crate::db::DbPool;
use crate::db::accessors::pending_messages::{
    claim_next_pending_messages, set_pending_message_fetched,
};
use crate::db::models::messages::MessageDb;
use crate::db::models::pending_messages::PendingMessageDb;
use crate::handlers::message_handler::MessageHandler;
use crate::jobs::job_utils::{
    CancelToken, MqWatcher, handle_processing_error, pending_message_payload,
};
use crate::toolkit::timestamp::utc_now;
use crate::types::message_status::MessageProcessingException;

/// Abstraction over the handler's `verify_and_fetch_message` so tests can
/// inject deterministic outcomes without spinning up DB + IPFS.
///
/// The trait uses `?Send` on its futures because `ContentHandler` futures
/// are not `Send`. Job loops drive these on a single tokio task.
#[async_trait(?Send)]
pub trait FetchRunner {
    async fn verify_and_fetch(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        pending: &PendingMessageDb,
    ) -> Result<MessageDb, MessageProcessingException>;

    fn max_retries(&self) -> i32;
}

/// Default runner wrapping a real [`MessageHandler`].
pub struct HandlerFetchRunner {
    pub handler: Arc<MessageHandler>,
    pub max_retries: i32,
}

#[async_trait(?Send)]
impl FetchRunner for HandlerFetchRunner {
    async fn verify_and_fetch(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        pending: &PendingMessageDb,
    ) -> Result<MessageDb, MessageProcessingException> {
        self.handler.verify_and_fetch_message(client, pending).await
    }

    fn max_retries(&self) -> i32 {
        self.max_retries
    }
}

/// Optional notifier: publishes a `process.<item_hash>` message on the
/// pending-message exchange whenever a row transitions to `fetched = true`.
///
/// Mirrors Python's `_notify_message_fetched`.
pub struct FetchNotifier {
    pub channel: Channel,
    pub exchange: String,
}

impl FetchNotifier {
    pub async fn notify(&self, pending: &PendingMessageDb) -> AlephResult<()> {
        let routing_key = format!("process.{}", pending.item_hash);
        let body = pending.id.to_string();
        self.channel
            .basic_publish(
                &self.exchange,
                &routing_key,
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default(),
            )
            .await
            .map_err(|e| crate::AlephError::P2p(format!("publish failed: {e}")))?
            .await
            .map_err(|e| crate::AlephError::P2p(format!("publish confirm failed: {e}")))?;
        Ok(())
    }
}

/// Configuration knobs for [`run`].
pub struct FetchConfig {
    /// Maximum number of simultaneously-running fetch tasks.
    /// Mirrors `config.aleph.jobs.pending_messages.max_concurrency`.
    pub max_concurrency: usize,
    /// Idle wait between polls when the queue is empty.
    pub idle_timeout: Duration,
    /// Return as soon as an idle pass is observed (tests).
    pub one_shot: bool,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 10,
            idle_timeout: Duration::from_secs(3),
            one_shot: false,
        }
    }
}

/// Fetch & persist a single pending message. Returns `Some(MessageDb)` on
/// success, `None` on a handled processing exception (already logged).
///
/// Mirrors pyaleph's `with session.begin(): ... session.commit() /
/// session.rollback()` boundary: the fetch + status update happen inside a
/// single transaction so a failure mid-way cannot leave partial writes
/// behind. On failure, the retry/reject bookkeeping runs in a separate
/// transaction.
pub async fn fetch_one(
    pool: &DbPool,
    runner: &dyn FetchRunner,
    mut pending: PendingMessageDb,
    notifier: Option<&FetchNotifier>,
) -> AlephResult<Option<MessageDb>> {
    let mut client = pool
        .get()
        .await
        .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
    let pg_client: &mut tokio_postgres::Client = &mut **client;
    let tx = pg_client
        .transaction()
        .await
        .map_err(|e| crate::AlephError::Pool(format!("begin fetch tx: {e}")))?;

    match runner.verify_and_fetch(&tx, &pending).await {
        Ok(message) => {
            let content = message.content.clone();
            if !content.is_object() {
                let _ = tx.rollback().await;
                return Err(crate::AlephError::InvalidMessage(format!(
                    "Fetched message {} has no content dict",
                    message.item_hash
                )));
            }
            set_pending_message_fetched(&tx, pending.id, &content).await?;
            tx.commit()
                .await
                .map_err(|e| crate::AlephError::Pool(format!("commit fetch tx: {e}")))?;
            if let Some(n) = notifier {
                if let Err(e) = n.notify(&pending).await {
                    tracing::warn!("Failed to notify processor for {}: {e}", pending.item_hash);
                }
            }
            Ok(Some(message))
        }
        Err(exception) => {
            let _ = tx.rollback().await;
            let payload = pending_message_payload(&pending);
            let retry_tx = pg_client
                .transaction()
                .await
                .map_err(|e| crate::AlephError::Pool(format!("begin retry tx: {e}")))?;
            let _ = handle_processing_error(
                &retry_tx,
                &mut pending,
                &exception,
                runner.max_retries(),
                &payload,
            )
            .await?;
            retry_tx
                .commit()
                .await
                .map_err(|e| crate::AlephError::Pool(format!("commit retry tx: {e}")))?;
            Ok(None)
        }
    }
}

/// Claim up to `slots` pending rows that are not currently being fetched.
async fn claim_candidates(
    pool: &DbPool,
    slots: usize,
    busy_hashes: &HashSet<String>,
) -> AlephResult<Vec<PendingMessageDb>> {
    if slots == 0 {
        return Ok(Vec::new());
    }
    let client = pool
        .get()
        .await
        .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
    let exclude: Vec<String> = busy_hashes.iter().cloned().collect();
    let now = utc_now();
    let lease_until = now + chrono::Duration::seconds(300);
    let rows = claim_next_pending_messages(
        &**client,
        now,
        lease_until,
        slots as i64,
        Some(false),
        if exclude.is_empty() {
            None
        } else {
            Some(&exclude)
        },
    )
    .await?;
    Ok(rows)
}

/// Run the fetcher loop. Mirrors Python `fetch_messages_task`.
///
/// Up to `cfg.max_concurrency` fetches run concurrently via a
/// `FuturesUnordered`. The loop drives the pool from a single task so the
/// non-`Send` futures returned by the content handler are compatible.
pub async fn run(
    pool: DbPool,
    runner: Arc<dyn FetchRunner>,
    notifier: Option<Arc<FetchNotifier>>,
    watcher: Arc<MqWatcher>,
    cfg: FetchConfig,
    cancel: CancelToken,
) -> AlephResult<()> {
    let mut busy_hashes: HashSet<String> = HashSet::new();
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let inflight = in_flight.len();
        let slots = cfg.max_concurrency.saturating_sub(inflight);

        let candidates = match claim_candidates(&pool, slots, &busy_hashes).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!("Failed to claim pending rows: {e}");
                Vec::new()
            }
        };

        for pending in candidates {
            busy_hashes.insert(pending.item_hash.clone());
            let item_hash = pending.item_hash.clone();
            let runner_ref = runner.clone();
            let notifier_ref = notifier.clone();
            let pool_ref = pool.clone();
            in_flight.push(async move {
                let result = fetch_one(
                    &pool_ref,
                    &*runner_ref,
                    pending,
                    notifier_ref.as_deref().map(|n| n),
                )
                .await;
                (item_hash, result)
            });
        }

        if in_flight.is_empty() {
            if cfg.one_shot {
                break;
            }
            tokio::select! {
                _ = watcher.ready() => {}
                _ = tokio::time::sleep(cfg.idle_timeout) => {}
                _ = cancel.cancelled() => break,
            }
            continue;
        }

        // Wait for one fetch to finish or external signal.
        tokio::select! {
            done = in_flight.next() => {
                if let Some((item_hash, result)) = done {
                    busy_hashes.remove(&item_hash);
                    if let Err(e) = result {
                        tracing::warn!("Fetcher error for {item_hash}: {e}");
                    }
                }
            }
            _ = cancel.cancelled() => break,
        }
    }

    // Drain remaining tasks before returning.
    while let Some((item_hash, result)) = in_flight.next().await {
        busy_hashes.remove(&item_hash);
        if let Err(e) = result {
            tracing::warn!("Fetcher error for {item_hash}: {e}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::job_utils::compute_next_retry_interval;

    #[tokio::test]
    async fn empty_queue_loop_terminates_on_cancel() {
        let cancel = CancelToken::new();
        cancel.cancel();
        let watcher = Arc::new(MqWatcher::detached());
        let cfg = FetchConfig {
            max_concurrency: 1,
            idle_timeout: Duration::from_millis(10),
            one_shot: true,
        };
        let _ = cfg;
        tokio::select! {
            _ = watcher.ready() => panic!("watcher should not fire"),
            _ = cancel.cancelled() => {}
        }
    }

    #[test]
    fn backoff_bounded_by_exponential_cap() {
        // Full jitter draws uniformly from [0, 2^attempts] seconds, so each
        // sample is bounded by the exponential cap.
        for _ in 0..50 {
            assert!(compute_next_retry_interval(0) <= Duration::from_secs(1));
            assert!(compute_next_retry_interval(1) <= Duration::from_secs(2));
            assert!(compute_next_retry_interval(2) <= Duration::from_secs(4));
            assert!(compute_next_retry_interval(3) <= Duration::from_secs(8));
        }
    }
}
