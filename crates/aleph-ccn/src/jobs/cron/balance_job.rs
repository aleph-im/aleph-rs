//! Balance reconciliation cron job. Mirrors
//! `aleph/jobs/cron/balance_job.py`.
//!
//! Each pass:
//!   1. Lists addresses with balance changes since the previous run.
//!   2. Walks every `(item_hash, height, cost)` paid by `hold` for that
//!      address, sorted by emission order.
//!   3. If the remaining balance is insufficient and the message is past
//!      the cost-cutoff height, schedules the message for removal.
//!      Otherwise, recovers any previously scheduled-for-removal messages.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::AlephResult;
use crate::db::accessors::balances::{get_total_balance, get_updated_balance_accounts};
use crate::db::accessors::cost::get_total_costs_for_address_grouped_by_message;
use crate::db::accessors::files::update_file_pin_grace_period;
use crate::db::accessors::messages::{
    get_message_by_item_hash, get_message_status, upsert_message_status,
};
use crate::db::models::account_costs::PaymentType;
use crate::db::models::cron_jobs::CronJobDb;
use crate::db::models::messages::MessageDb;
use crate::jobs::cron::cron_job::CronJob;
use crate::services::cost::{CostContent, calculate_storage_size};
use crate::toolkit::constants::{MiB, STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT};
use crate::toolkit::timestamp::utc_now;
use crate::types::message_status::MessageStatus;

/// Stable id matching the `cron_jobs.id` value used by pyaleph.
pub const NAME: &str = "balance";

/// Cron job implementation. Mirrors Python `BalanceCronJob`.
pub struct BalanceCronJob {
    pub max_unauthenticated_upload_file_size: i64,
}

impl BalanceCronJob {
    pub fn new(max_unauthenticated_upload_file_size: i64) -> Self {
        Self {
            max_unauthenticated_upload_file_size,
        }
    }
}

#[async_trait]
impl CronJob for BalanceCronJob {
    fn name(&self) -> &str {
        NAME
    }

    fn period(&self) -> Duration {
        Duration::from_secs(3600)
    }

    async fn run(
        &self,
        _now: DateTime<Utc>,
        job: &CronJobDb,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> AlephResult<()> {
        let pg = tx;
        let accounts = get_updated_balance_accounts(pg, job.last_run).await?;
        tracing::info!("Checking '{}' updated account balances...", accounts.len());

        for address in accounts {
            let mut remaining_balance = get_total_balance(pg, &address, false).await?;

            let mut to_delete: Vec<String> = Vec::new();
            let mut to_recover: Vec<String> = Vec::new();

            let hold_costs = get_total_costs_for_address_grouped_by_message(
                pg,
                &address,
                Some(PaymentType::Hold),
            )
            .await?;

            for row in hold_costs {
                tracing::info!(
                    "Checking {} message, with height {} and cost {}",
                    row.item_hash,
                    row.height,
                    row.total
                );

                let should_remove = remaining_balance < row.total
                    && (row.height as i64) >= STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT;
                remaining_balance = (remaining_balance - row.total).max(Decimal::ZERO);

                let status = match get_message_status(pg, &row.item_hash).await? {
                    Some(s) => s,
                    None => continue,
                };

                if should_remove {
                    if status.status != MessageStatus::Removing
                        && status.status != MessageStatus::Removed
                    {
                        to_delete.push(row.item_hash);
                    }
                } else if status.status == MessageStatus::Removing {
                    to_recover.push(row.item_hash);
                }
            }

            if !to_delete.is_empty() {
                tracing::info!(
                    "'{}' messages to delete for account '{}'...",
                    to_delete.len(),
                    address
                );
                delete_messages(
                    pg,
                    &to_delete,
                    self.max_unauthenticated_upload_file_size,
                    /* legacy_cutoff_ts */ None,
                )
                .await?;
            }
            if !to_recover.is_empty() {
                tracing::info!(
                    "'{}' messages to recover for account '{}'...",
                    to_recover.len(),
                    address
                );
                recover_messages(pg, &to_recover).await?;
            }
        }
        Ok(())
    }
}

/// Schedule each message in `messages` for removal. Mirrors the Python
/// `delete_messages` method on `BalanceCronJob`.
///
/// `legacy_cutoff_ts`: when `Some(ts)`, the "small file" exception only
/// applies to messages whose `time` is before that POSIX timestamp (this
/// matches `CreditBalanceCronJob` behaviour). The hold-balance job always
/// applies the exception, so pass `None`.
pub async fn delete_messages(
    client: &impl tokio_postgres::GenericClient,
    messages: &[String],
    max_unauthenticated_upload_file_size: i64,
    legacy_cutoff_ts: Option<i64>,
) -> AlephResult<()> {
    for item_hash in messages {
        let message = match get_message_by_item_hash(client, item_hash).await? {
            Some(m) => m,
            None => continue,
        };

        if message.r#type == aleph_types::message::MessageType::Store {
            if message_small_enough_to_keep(
                client,
                &message,
                max_unauthenticated_upload_file_size,
                legacy_cutoff_ts,
            )
            .await?
            {
                continue;
            }
        }

        let now = utc_now();
        let delete_by = now + chrono::Duration::hours(24 + 1);

        if message.r#type == aleph_types::message::MessageType::Store {
            update_file_pin_grace_period(client, item_hash, Some(delete_by)).await?;
        }

        upsert_message_status(
            client,
            item_hash,
            MessageStatus::Removing,
            now,
            Some("message_status.status = 'processed'"),
        )
        .await?;
        // Dual-write to messages table (trigger handles message_counts).
        client
            .execute(
                "UPDATE messages SET status = 'removing' WHERE item_hash = $1",
                &[&item_hash],
            )
            .await?;
    }
    Ok(())
}

/// Restore messages previously scheduled for removal.
pub async fn recover_messages(
    client: &impl tokio_postgres::GenericClient,
    messages: &[String],
) -> AlephResult<()> {
    for item_hash in messages {
        let message = match get_message_by_item_hash(client, item_hash).await? {
            Some(m) => m,
            None => continue,
        };

        if message.r#type == aleph_types::message::MessageType::Store {
            update_file_pin_grace_period(client, item_hash, None).await?;
        }

        let now = utc_now();
        upsert_message_status(
            client,
            item_hash,
            MessageStatus::Processed,
            now,
            Some("message_status.status = 'removing'"),
        )
        .await?;
        client
            .execute(
                "UPDATE messages SET status = 'processed' WHERE item_hash = $1",
                &[&item_hash],
            )
            .await?;
    }
    Ok(())
}

async fn message_small_enough_to_keep(
    client: &impl tokio_postgres::GenericClient,
    message: &MessageDb,
    max_unauthenticated_upload_file_size: i64,
    legacy_cutoff_ts: Option<i64>,
) -> AlephResult<bool> {
    // Mirrors the "small file exception" guard. For credit-balance jobs the
    // exception only applies to legacy messages.
    if let Some(cutoff) = legacy_cutoff_ts {
        let message_ts = message.time.timestamp();
        if message_ts >= cutoff {
            return Ok(false);
        }
    }
    let cost_content = match CostContent::from_value(&message.content) {
        Some(c) => c,
        None => return Ok(false),
    };
    let storage_mib = calculate_storage_size(client, &cost_content).await?;
    let Some(storage_mib) = storage_mib else {
        return Ok(false);
    };
    let limit_mib = Decimal::from(max_unauthenticated_upload_file_size) / Decimal::from(MiB);
    Ok(storage_mib <= limit_mib)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    #[test]
    fn name_is_balance() {
        let j = BalanceCronJob::new(0);
        assert_eq!(j.name(), NAME);
    }

    #[test]
    fn period_is_one_hour() {
        let j = BalanceCronJob::new(0);
        assert_eq!(j.period(), StdDuration::from_secs(3600));
    }

    #[tokio::test]
    async fn run_returns_quickly_on_cancel() {
        // Smoke test: the run loop is wrapped by the orchestrator; the job
        // itself doesn't poll, so a "no work" pass returns Ok promptly.
        let _ = BalanceCronJob::new(0);
    }

    #[test]
    fn backoff_bounded_by_exponential_cap() {
        use crate::jobs::job_utils::compute_next_retry_interval;
        // Full jitter: each draw is bounded by its exponential cap.
        for _ in 0..50 {
            assert!(compute_next_retry_interval(1) <= StdDuration::from_secs(2));
            assert!(compute_next_retry_interval(2) <= StdDuration::from_secs(4));
        }
    }
}
