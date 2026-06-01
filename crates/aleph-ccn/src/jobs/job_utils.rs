//! Shared utilities for the pending-message/pending-tx jobs. Mirrors
//! `aleph/jobs/job_utils.py`.
//!
//! The Python module exposes:
//! * `compute_next_retry_interval(attempts) -> timedelta` — exponential
//!   backoff capped at 5 minutes.
//! * `schedule_next_attempt(session, pending_message)` — increments retries
//!   and rolls `next_attempt` forward.
//! * `MqWatcher` — async context manager wrapping a RabbitMQ queue iterator
//!   into a `ready()` event.
//! * `MessageJob` — base class with `handle_processing_error()` that turns a
//!   raised exception into either a rejection (via
//!   `reject_existing_pending_message`) or a retry (via
//!   `schedule_next_attempt`).
//!
//! All of those have direct Rust equivalents below.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use lapin::Channel;
use lapin::options::{BasicConsumeOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::AlephResult;
use crate::db::accessors::messages::reject_existing_pending_message;
use crate::db::accessors::pending_messages::set_next_retry;
use crate::db::models::pending_messages::PendingMessageDb;
use crate::toolkit::rabbitmq::declare_topic_exchange;
use crate::toolkit::timestamp::utc_now;
use crate::types::message_processing_result::FailedMessage;
use crate::types::message_status::MessageProcessingException;

/// Hard cap on retry backoff (seconds). Mirrors `MAX_RETRY_INTERVAL = 300`.
pub const MAX_RETRY_INTERVAL: u64 = 300;

/// Lightweight cancellation token. Mirrors the small slice of
/// `tokio_util::sync::CancellationToken` we need — checked + awaitable.
#[derive(Clone, Default)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the token cancelled and wake all waiters.
    pub fn cancel(&self) {
        if !self.flag.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    /// Whether `cancel()` has been called.
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Future that resolves when `cancel()` is called.
    ///
    /// `notify_waiters` only wakes tasks already parked on `notified()`, so
    /// we re-check the flag in a loop to handle the race where cancel was
    /// invoked before we registered.
    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            if self.is_cancelled() {
                return;
            }
            notified.await;
            if self.is_cancelled() {
                return;
            }
        }
    }
}

/// Compute the time interval before the next retry attempt.
///
/// Uses exponential backoff with full jitter: the interval is drawn
/// uniformly from `[0, min(2^attempts, MAX_RETRY_INTERVAL)]` seconds. The
/// jitter decorrelates the next-attempt time across nodes that failed the
/// same attempt at roughly the same moment, so a retry storm does not
/// re-converge into the same instant. Mirrors Python
/// `compute_next_retry_interval`.
pub fn compute_next_retry_interval(attempts: i32) -> Duration {
    use rand::Rng as _;
    let cap = if attempts < 0 {
        1u64
    } else if attempts >= 9 {
        // 2^9 = 512 > 300, so anything from 9 onwards saturates.
        MAX_RETRY_INTERVAL
    } else {
        1u64 << (attempts as u32)
    }
    .min(MAX_RETRY_INTERVAL);
    let seconds = rand::thread_rng().gen_range(0.0..=cap as f64);
    Duration::from_secs_f64(seconds)
}

/// Schedule the next attempt time for a failed pending message.
///
/// The next attempt is set relative to "now" (not the previous attempt
/// time), exactly like Python's `schedule_next_attempt`. Persists the new
/// timestamp/retry count to the DB and updates the in-memory copy so the
/// caller can observe the new values.
pub async fn schedule_next_attempt(
    client: &impl tokio_postgres::GenericClient,
    pending_message: &mut PendingMessageDb,
) -> AlephResult<()> {
    let next_attempt = utc_now()
        + chrono::Duration::from_std(compute_next_retry_interval(pending_message.retries))
            .unwrap_or_else(|_| chrono::Duration::seconds(MAX_RETRY_INTERVAL as i64));
    set_next_retry(client, pending_message.id, next_attempt).await?;
    pending_message.next_attempt = next_attempt;
    pending_message.retries += 1;
    Ok(())
}

/// Watches a RabbitMQ queue and fires a [`Notify`] whenever a delivery is
/// observed. Mirrors Python's `MqWatcher` (an async context manager wrapping
/// a queue iterator + asyncio.Event).
pub struct MqWatcher {
    notify: Arc<Notify>,
    task: Option<JoinHandle<()>>,
}

impl MqWatcher {
    /// Build a watcher with no underlying consumer. `notify_ready()` calls
    /// `notify_one()` directly. Used by the in-process tests and by callers
    /// that drive readiness on a timer.
    pub fn detached() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            task: None,
        }
    }

    /// Spawn a background task that wakes [`Self::ready`] every time the
    /// underlying RabbitMQ queue receives a message. Mirrors the
    /// `__aenter__` body of Python's `MqWatcher`.
    pub fn spawn(channel: Channel, queue: String, routing_key: String) -> AlephResult<Self> {
        use futures_util::StreamExt as _;
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();
        let task = tokio::spawn(async move {
            let mut retry_delay = Duration::from_secs(1);
            loop {
                let consumer = channel
                    .basic_consume(
                        &queue,
                        &format!("watcher-{}", routing_key),
                        BasicConsumeOptions {
                            no_ack: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await;

                let mut consumer = match consumer {
                    Ok(c) => {
                        retry_delay = Duration::from_secs(1);
                        c
                    }
                    Err(e) => {
                        tracing::error!(
                            "MqWatcher consumer setup failed; retrying in {:?}: {e}",
                            retry_delay
                        );
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
                        continue;
                    }
                };

                while let Some(delivery) = consumer.next().await {
                    match delivery {
                        Ok(_) => notify_clone.notify_one(),
                        Err(e) => {
                            tracing::warn!(
                                "MqWatcher delivery error; recreating consumer in {:?}: {e}",
                                retry_delay
                            );
                            break;
                        }
                    }
                }

                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
            }
        });
        Ok(Self {
            notify,
            task: Some(task),
        })
    }

    /// Awaits the next "queue had a message" signal. Mirrors Python
    /// `MqWatcher.ready()`. Returns immediately if a notification was sent
    /// while there was no waiter.
    pub async fn ready(&self) {
        self.notify.notified().await;
    }

    /// Manually fire a readiness notification. Used by tests and by the
    /// in-process scheduler.
    pub fn notify_ready(&self) {
        self.notify.notify_one();
    }
}

impl Drop for MqWatcher {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

/// Declare the `pending_tx_queue` topic-bound queue and return its name.
/// Mirrors Python `make_pending_tx_queue`.
pub async fn make_pending_tx_queue(
    channel: &Channel,
    pending_tx_exchange: &str,
) -> AlephResult<String> {
    declare_topic_exchange(channel, pending_tx_exchange).await?;
    let queue_name = "pending-tx-queue".to_string();
    channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions {
                // pyaleph uses aio_pika's default `durable=False`.
                durable: false,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| crate::AlephError::P2p(format!("queue_declare failed: {e}")))?;
    channel
        .queue_bind(
            &queue_name,
            pending_tx_exchange,
            "#",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|e| crate::AlephError::P2p(format!("queue_bind failed: {e}")))?;
    Ok(queue_name)
}

/// Declare a per-pattern pending-message queue and return its name. Mirrors
/// Python `make_pending_message_queue`.
pub async fn make_pending_message_queue(
    channel: &Channel,
    pending_message_exchange: &str,
    routing_key: &str,
) -> AlephResult<String> {
    declare_topic_exchange(channel, pending_message_exchange).await?;
    let suffix: String = routing_key
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let queue_name = format!("pending-message-queue-{suffix}");
    channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions {
                // pyaleph uses aio_pika's default `durable=False`.
                durable: false,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| crate::AlephError::P2p(format!("queue_declare failed: {e}")))?;
    channel
        .queue_bind(
            &queue_name,
            pending_message_exchange,
            routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(|e| crate::AlephError::P2p(format!("queue_bind failed: {e}")))?;
    Ok(queue_name)
}

/// Convert a `MessageProcessingException` raised during fetch/process into a
/// `FailedMessage` result and persist the side-effects.
///
/// Mirrors Python `MessageJob.handle_processing_error`. The Rust port keeps
/// the same two-arm logic:
///   * `InvalidMessageException`/non-retry → reject (mark pending row as
///     rejected, delete from `pending_messages`).
///   * retry-eligible + retries < max_retries → schedule next attempt.
///   * retry-eligible + retries ≥ max_retries → reject (too many retries).
pub async fn handle_processing_error(
    client: &impl tokio_postgres::GenericClient,
    pending_message: &mut PendingMessageDb,
    exception: &MessageProcessingException,
    max_retries: i32,
    pending_message_dict: &serde_json::Value,
) -> AlephResult<FailedMessage> {
    if !exception.is_retry() {
        return reject(client, pending_message, exception, pending_message_dict).await;
    }
    // Retryable: handle MessageContentUnavailable specially by clearing the
    // `fetched` flag so the fetcher picks the message up again.
    if matches!(
        exception,
        MessageProcessingException::MessageContentUnavailable { .. }
    ) {
        client
            .execute(
                "UPDATE pending_messages SET fetched = FALSE WHERE id = $1",
                &[&pending_message.id],
            )
            .await?;
        pending_message.fetched = false;
    }

    if pending_message.retries >= max_retries {
        tracing::warn!(
            "Rejecting pending message: {} - too many retries",
            pending_message.item_hash
        );
        return reject(client, pending_message, exception, pending_message_dict).await;
    }

    tracing::warn!(
        "Message {} marked for retry: {:?}",
        pending_message.item_hash,
        exception
    );
    schedule_next_attempt(client, pending_message).await?;
    let origin = parse_origin(pending_message.origin.as_deref());
    Ok(
        FailedMessage::will_retry(pending_message.item_hash.clone(), exception.error_code())
            .with_origin(origin),
    )
}

async fn reject(
    client: &impl tokio_postgres::GenericClient,
    pending_message: &PendingMessageDb,
    exception: &MessageProcessingException,
    pending_message_dict: &serde_json::Value,
) -> AlephResult<FailedMessage> {
    let rejected =
        reject_existing_pending_message(client, pending_message, pending_message_dict, exception)
            .await?;
    let error_code = match rejected {
        Some(r) => r.error_code,
        None => exception.error_code(),
    };
    let origin = parse_origin(pending_message.origin.as_deref());
    Ok(FailedMessage::rejected(pending_message.item_hash.clone(), error_code).with_origin(origin))
}

fn parse_origin(s: Option<&str>) -> Option<crate::types::message_status::MessageOrigin> {
    use crate::types::message_status::MessageOrigin;
    match s? {
        "onchain" => Some(MessageOrigin::Onchain),
        "p2p" => Some(MessageOrigin::P2p),
        "ipfs" => Some(MessageOrigin::Ipfs),
        _ => None,
    }
}

/// Helper struct returned by [`pending_message_payload`] used to serialise
/// the pending row into the JSON form `reject_existing_pending_message`
/// stores in `rejected_messages.message`.
pub fn pending_message_payload(pending_message: &PendingMessageDb) -> serde_json::Value {
    // Python's `to_dict` serialises the `time` column as the ISO 8601 string
    // produced by `datetime.isoformat()`. We mirror that exactly.
    serde_json::json!({
        "item_hash": pending_message.item_hash,
        "type": pending_message.r#type.to_string(),
        "chain": pending_message.chain,
        "sender": pending_message.sender,
        "signature": pending_message.signature,
        "item_type": pending_message.item_type,
        "item_content": pending_message.item_content,
        "time": crate::toolkit::cursor::datetime_isoformat(pending_message.time),
        "channel": pending_message.channel,
    })
}

/// Compute the absolute datetime for the next retry. Mirrors the small
/// helper used by `MessagePublisher` plus tests.
pub fn next_retry_datetime(now: DateTime<Utc>, attempts: i32) -> DateTime<Utc> {
    now + chrono::Duration::from_std(compute_next_retry_interval(attempts))
        .unwrap_or_else(|_| chrono::Duration::seconds(MAX_RETRY_INTERVAL as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_interval_zero_attempts_bounded_by_one_second() {
        // First retry must fall within [0, 1] seconds (base = 2^0 = 1).
        for _ in 0..50 {
            let delay = compute_next_retry_interval(0);
            assert!(delay <= Duration::from_secs(1));
        }
    }

    #[test]
    fn retry_interval_bounded_by_exponential_cap() {
        // Each draw stays within [0, min(2^attempts, MAX_RETRY_INTERVAL)].
        // attempts=9 is the first value where 2^attempts (512) exceeds the
        // cap (300), so it doubles as a boundary check.
        for attempts in [1, 2, 5, 8, 9] {
            let cap_seconds = (1u64 << attempts).min(MAX_RETRY_INTERVAL);
            let cap = Duration::from_secs(cap_seconds);
            for _ in 0..50 {
                let delay = compute_next_retry_interval(attempts);
                assert!(delay <= cap, "attempts={attempts}: {delay:?} > {cap:?}");
            }
        }
    }

    #[test]
    fn retry_interval_caps_at_max() {
        // Large attempt counts cannot exceed MAX_RETRY_INTERVAL.
        let cap = Duration::from_secs(MAX_RETRY_INTERVAL);
        for _ in 0..50 {
            assert!(compute_next_retry_interval(20) <= cap);
            assert!(compute_next_retry_interval(100) <= cap);
        }
    }

    #[test]
    fn retry_interval_is_jittered() {
        // Successive calls at the same attempt count produce distinct values
        // (the jitter actually decorrelates retries).
        let samples: std::collections::HashSet<u128> = (0..50)
            .map(|_| compute_next_retry_interval(5).as_nanos())
            .collect();
        assert!(samples.len() >= 5);
    }

    #[test]
    fn next_retry_datetime_uses_backoff() {
        // attempts=2 caps the jitter window at 2^2 = 4 seconds.
        let now = Utc::now();
        let later = next_retry_datetime(now, 2);
        let delta = (later - now).num_seconds();
        assert!((0..=4).contains(&delta), "delta={delta}");
    }

    #[test]
    fn parse_origin_variants() {
        use crate::types::message_status::MessageOrigin;
        assert_eq!(parse_origin(None), None);
        assert_eq!(parse_origin(Some("onchain")), Some(MessageOrigin::Onchain));
        assert_eq!(parse_origin(Some("p2p")), Some(MessageOrigin::P2p));
        assert_eq!(parse_origin(Some("ipfs")), Some(MessageOrigin::Ipfs));
        assert_eq!(parse_origin(Some("zzz")), None);
    }

    #[tokio::test]
    async fn mq_watcher_detached_signals_on_demand() {
        let w = MqWatcher::detached();
        w.notify_ready();
        // ready() should resolve immediately because notify_one was called
        // before any waiter — Notify queues the wakeup.
        tokio::time::timeout(Duration::from_millis(100), w.ready())
            .await
            .expect("ready() did not resolve");
    }

    #[tokio::test]
    async fn mq_watcher_blocks_when_no_signal() {
        let w = MqWatcher::detached();
        let res = tokio::time::timeout(Duration::from_millis(50), w.ready()).await;
        assert!(res.is_err(), "ready() should block without a signal");
    }
}
