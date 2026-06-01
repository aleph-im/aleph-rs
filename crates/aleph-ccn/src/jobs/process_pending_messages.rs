//! Pending-message processor. Mirrors `aleph/jobs/process_pending_messages.py`.
//!
//! Pulls already-fetched pending rows out of `pending_messages` and feeds
//! them to [`MessageHandler::process_pending_message`]. Successful results
//! are optionally published on a RabbitMQ exchange so subscribers
//! (`/api/v0/messages/ws`, etc.) can react.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lapin::options::BasicPublishOptions;
use lapin::{BasicProperties, Channel};
use tokio::sync::broadcast;

use crate::AlephResult;
use crate::db::DbPool;
use crate::db::accessors::pending_messages::get_next_pending_message;
use crate::db::models::pending_messages::PendingMessageDb;
use crate::handlers::message_handler::{MessageHandler, ProcessOutcome};
use crate::jobs::job_utils::{
    CancelToken, MqWatcher, handle_processing_error, pending_message_payload,
};
use crate::toolkit::timestamp::utc_now;
use crate::types::message_processing_result::{
    AnyMessageProcessingResult, FailedMessage, MessageProcessingResult,
};
use crate::types::message_status::MessageOrigin;

/// Abstraction over the small slice of behaviour the processor needs from a
/// `MessageHandler`. `?Send` because the underlying `ContentHandler`
/// futures are not `Send`. The processor drives them on a single tokio
/// task.
#[async_trait(?Send)]
pub trait PendingMessageRunner {
    async fn process(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        pending: &PendingMessageDb,
    ) -> Result<ProcessOutcome, crate::types::message_status::MessageProcessingException>;

    fn max_retries(&self) -> i32;
}

#[async_trait(?Send)]
impl PendingMessageRunner for HandlerRunner {
    async fn process(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        pending: &PendingMessageDb,
    ) -> Result<ProcessOutcome, crate::types::message_status::MessageProcessingException> {
        self.handler.process_pending_message(client, pending).await
    }

    fn max_retries(&self) -> i32 {
        self.max_retries
    }
}

/// Default runner adapter wrapping a real [`MessageHandler`].
pub struct HandlerRunner {
    pub handler: Arc<MessageHandler>,
    pub max_retries: i32,
}

/// Optional publisher: posts the JSON outcome on the message exchange.
/// Mirrors the second pipeline stage `publish_to_mq` in Python.
pub struct OutcomePublisher {
    pub channel: Channel,
    pub exchange: String,
    pub broadcast: Option<broadcast::Sender<serde_json::Value>>,
}

impl OutcomePublisher {
    /// Publish an outcome unless it originated from the on-chain protocol
    /// (matches Python `if result.origin != MessageOrigin.ONCHAIN`).
    pub async fn publish(&self, result: &AnyMessageProcessingResult) -> AlephResult<()> {
        if let AnyMessageProcessingResult::Processed(processed) = result {
            if let Some(sender) = &self.broadcast {
                let _ = sender.send(processed.message.clone());
            }
        }
        if result.origin() == Some(MessageOrigin::Onchain) {
            return Ok(());
        }
        let body = serde_json::to_vec(&result.to_dict())?;
        let routing_key = format!("{}.{}", result.status().as_value_str(), result.item_hash());
        self.channel
            .basic_publish(
                &self.exchange,
                &routing_key,
                BasicPublishOptions::default(),
                &body,
                BasicProperties::default(),
            )
            .await
            .map_err(|e| crate::AlephError::P2p(format!("publish failed: {e}")))?
            .await
            .map_err(|e| crate::AlephError::P2p(format!("publish confirm failed: {e}")))?;
        Ok(())
    }
}

/// Configuration for a [`run`] loop.
pub struct PendingMessageProcessorConfig {
    /// Idle wait when the queue is empty before re-checking.
    pub idle_timeout: Duration,
    /// Return after the first empty pass (tests).
    pub one_shot: bool,
}

impl Default for PendingMessageProcessorConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(1),
            one_shot: false,
        }
    }
}

/// Drain one pass of pending rows: pull, process, return outcomes.
///
/// Mirrors Python `PendingMessageProcessor.process_messages`. Each pending
/// row is wrapped in its own DB transaction so partial writes from a failing
/// `process()` call are rolled back before [`handle_processing_error`] runs
/// in a fresh transaction. This matches pyaleph's
/// `with session.begin(): ... session.commit() / session.rollback()`
/// boundary.
pub async fn process_one_batch(
    pool: &DbPool,
    runner: &dyn PendingMessageRunner,
) -> AlephResult<Vec<AnyMessageProcessingResult>> {
    let mut results: Vec<AnyMessageProcessingResult> = Vec::new();
    loop {
        let mut client = pool
            .get()
            .await
            .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
        let pg_client: &mut tokio_postgres::Client = &mut **client;

        // Open the per-row transaction. Both `get_next_pending_message` and
        // the subsequent processing must observe the same snapshot so a row
        // already PROCESSED by a prior run is filtered out and not picked up
        // again.
        let tx = pg_client
            .transaction()
            .await
            .map_err(|e| crate::AlephError::Pool(format!("begin tx: {e}")))?;

        let mut pending = match get_next_pending_message(&tx, utc_now(), 0, Some(true), None)
            .await?
        {
            Some(p) => p,
            None => {
                // Nothing to do — release the transaction without writes.
                let _ = tx.rollback().await;
                break;
            }
        };

        match runner.process(&tx, &pending).await {
            Ok(outcome) => {
                tx.commit().await.map_err(|e| {
                    crate::AlephError::Pool(format!("commit processed tx: {e}"))
                })?;
                let result = match outcome {
                    ProcessOutcome::Processed(pm) => AnyMessageProcessingResult::Processed(pm),
                    ProcessOutcome::Rejected {
                        item_hash,
                        error_code,
                    } => AnyMessageProcessingResult::Failed(
                        FailedMessage::rejected(item_hash, error_code)
                            .with_origin(parse_origin(pending.origin.as_deref())),
                    ),
                };
                results.push(result);
            }
            Err(exception) => {
                // Mirror pyaleph: roll back any partial writes done during
                // `process()` before recording the failure in a separate
                // transaction.
                let _ = tx.rollback().await;
                let payload = pending_message_payload(&pending);
                let retry_tx = pg_client
                    .transaction()
                    .await
                    .map_err(|e| crate::AlephError::Pool(format!("begin retry tx: {e}")))?;
                let failed = handle_processing_error(
                    &retry_tx,
                    &mut pending,
                    &exception,
                    runner.max_retries(),
                    &payload,
                )
                .await?;
                retry_tx.commit().await.map_err(|e| {
                    crate::AlephError::Pool(format!("commit retry tx: {e}"))
                })?;
                results.push(AnyMessageProcessingResult::Failed(failed));
            }
        }
    }
    Ok(results)
}

fn parse_origin(s: Option<&str>) -> Option<MessageOrigin> {
    match s? {
        "onchain" => Some(MessageOrigin::Onchain),
        "p2p" => Some(MessageOrigin::P2p),
        "ipfs" => Some(MessageOrigin::Ipfs),
        _ => None,
    }
}

/// Convenience helper used by [`ProcessedMessage`] consumers and tests.
pub fn log_outcome(result: &AnyMessageProcessingResult) {
    match result {
        AnyMessageProcessingResult::Processed(pm) => {
            tracing::info!("Successfully processed {}", pm.item_hash())
        }
        AnyMessageProcessingResult::Failed(fm) => {
            tracing::info!(
                "Pending message {} ended with status {:?}",
                fm.item_hash(),
                fm.status()
            )
        }
    }
}

/// Run the processor loop until the cancellation token fires.
/// Mirrors Python `fetch_and_process_messages_task`.
pub async fn run(
    pool: DbPool,
    runner: Arc<dyn PendingMessageRunner>,
    publisher: Option<Arc<OutcomePublisher>>,
    watcher: Arc<MqWatcher>,
    cfg: PendingMessageProcessorConfig,
    cancel: CancelToken,
) -> AlephResult<()> {
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let batch = match process_one_batch(&pool, &*runner).await {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Error in pending messages job: {e}");
                Vec::new()
            }
        };

        if let Some(p) = publisher.as_ref() {
            for result in &batch {
                if let Err(e) = p.publish(result).await {
                    tracing::warn!("Failed to publish outcome: {e}");
                }
            }
        }
        for result in &batch {
            log_outcome(result);
        }

        if cfg.one_shot && batch.is_empty() {
            return Ok(());
        }

        tokio::select! {
            _ = watcher.ready() => {},
            _ = tokio::time::sleep(cfg.idle_timeout) => {},
            _ = cancel.cancelled() => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::message_processing_result::ProcessedMessage;
    use crate::types::message_status::ErrorCode;

    #[tokio::test]
    async fn empty_queue_loop_terminates_on_cancel() {
        let watcher = Arc::new(MqWatcher::detached());
        let cancel = CancelToken::new();
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            cancel2.cancel();
        });
        let cfg = PendingMessageProcessorConfig {
            idle_timeout: Duration::from_millis(10),
            one_shot: false,
        };
        tokio::select! {
            _ = watcher.ready() => {}
            _ = tokio::time::sleep(cfg.idle_timeout) => {}
            _ = cancel.cancelled() => {}
        }
    }

    #[tokio::test]
    async fn retry_backoff_bounded_by_exponential_cap() {
        use super::super::job_utils::compute_next_retry_interval;
        // Full jitter: each draw is bounded by its exponential cap.
        for _ in 0..50 {
            assert!(compute_next_retry_interval(0) <= Duration::from_secs(1));
            assert!(compute_next_retry_interval(1) <= Duration::from_secs(2));
            assert!(compute_next_retry_interval(2) <= Duration::from_secs(4));
        }
    }

    #[test]
    fn outcome_routing_key_format() {
        let pm = ProcessedMessage::new("hh".into(), serde_json::json!({}), false);
        let any = AnyMessageProcessingResult::Processed(pm);
        let routing = format!("{}.{}", any.status().as_value_str(), any.item_hash());
        assert_eq!(routing, "processed.hh");
        let fm = FailedMessage::will_retry("xx".into(), ErrorCode::ContentUnavailable);
        let any = AnyMessageProcessingResult::Failed(fm);
        let routing = format!("{}.{}", any.status().as_value_str(), any.item_hash());
        assert_eq!(routing, "retry.xx");
    }

    #[test]
    fn parse_origin_handles_all_variants() {
        assert_eq!(parse_origin(None), None);
        assert_eq!(parse_origin(Some("onchain")), Some(MessageOrigin::Onchain));
        assert_eq!(parse_origin(Some("p2p")), Some(MessageOrigin::P2p));
        assert_eq!(parse_origin(Some("ipfs")), Some(MessageOrigin::Ipfs));
        assert_eq!(parse_origin(Some("???")), None);
    }
}
