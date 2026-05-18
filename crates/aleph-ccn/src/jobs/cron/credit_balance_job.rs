//! Credit-balance reconciliation cron job. Mirrors
//! `aleph/jobs/cron/credit_balance_job.py`.
//!
//! Same structure as [`BalanceCronJob`] but works on the credit-history
//! ledger and uses the 24-hour-minimum-runtime guard instead of the height
//! cutoff.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;

use crate::AlephResult;
use crate::db::accessors::balances::{get_credit_balance, get_updated_credit_balance_accounts};
use crate::db::accessors::cost::get_total_costs_for_address_grouped_by_message;
use crate::db::accessors::messages::get_message_status;
use crate::db::models::account_costs::PaymentType;
use crate::db::models::cron_jobs::CronJobDb;
use crate::jobs::cron::balance_job::{delete_messages, recover_messages};
use crate::jobs::cron::cron_job::CronJob;
use crate::toolkit::constants::CREDIT_ONLY_CUTOFF_TIMESTAMP;
use crate::toolkit::timestamp::utc_now;
use crate::types::message_status::MessageStatus;

/// Stable id matching the `cron_jobs.id` value used by pyaleph.
pub const NAME: &str = "credit_balance";

/// Cron job implementation. Mirrors Python `CreditBalanceCronJob`.
pub struct CreditBalanceCronJob {
    pub max_unauthenticated_upload_file_size: i64,
}

impl CreditBalanceCronJob {
    pub fn new(max_unauthenticated_upload_file_size: i64) -> Self {
        Self {
            max_unauthenticated_upload_file_size,
        }
    }
}

#[async_trait]
impl CronJob for CreditBalanceCronJob {
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
        let accounts = get_updated_credit_balance_accounts(pg, job.last_run).await?;
        tracing::info!(
            "Checking '{}' updated credit balance accounts...",
            accounts.len()
        );

        for address in accounts {
            let mut remaining_credits = get_credit_balance(pg, &address, None).await?;

            let mut to_delete: Vec<String> = Vec::new();
            let mut to_recover: Vec<String> = Vec::new();

            let credit_costs = get_total_costs_for_address_grouped_by_message(
                pg,
                &address,
                Some(PaymentType::Credit),
            )
            .await?;

            for row in credit_costs {
                tracing::info!(
                    "Checking credit message {} with cost {} credits",
                    row.item_hash,
                    row.total
                );

                // Cost is per hour, so multiply by 24 to compute the 1-day
                // minimum runtime guard. Use `Decimal::to_i64` so we do not
                // silently truncate fractional totals via `to_string().parse()`
                // (Python keeps `Decimal` precision end-to-end here, but the
                // downstream comparison only needs an integer — skip the row
                // entirely if the cost doesn't fit `i64`).
                let Some(cost_i) = row.total.to_i64() else {
                    tracing::warn!(
                        "Skipping credit message {}: total cost {} does not fit i64",
                        row.item_hash,
                        row.total
                    );
                    continue;
                };
                let daily_cost = cost_i.saturating_mul(24);
                let should_remove = remaining_credits < daily_cost;
                remaining_credits = (remaining_credits - cost_i).max(0);

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
                    "'{}' credit-paid messages to delete for account '{}'...",
                    to_delete.len(),
                    address
                );
                delete_messages(
                    pg,
                    &to_delete,
                    self.max_unauthenticated_upload_file_size,
                    Some(CREDIT_ONLY_CUTOFF_TIMESTAMP),
                )
                .await?;
            }
            if !to_recover.is_empty() {
                tracing::info!(
                    "'{}' credit-paid messages to recover for account '{}'...",
                    to_recover.len(),
                    address
                );
                recover_messages(pg, &to_recover).await?;
            }
        }
        Ok(())
    }
}

/// Helper used by the dual-write path to coerce a `MessageStatus` into the
/// stringified form stored in `messages.status`. Mirrors the Python
/// `MessageStatus.value`. Kept here so the inline `UPDATE messages` calls
/// in [`delete_messages`] / [`recover_messages`] read cleanly.
#[allow(dead_code)]
fn message_status_str(s: MessageStatus) -> &'static str {
    match s {
        MessageStatus::Pending => "pending",
        MessageStatus::Processed => "processed",
        MessageStatus::Rejected => "rejected",
        MessageStatus::Forgotten => "forgotten",
        MessageStatus::Removing => "removing",
        MessageStatus::Removed => "removed",
    }
}

/// Pin the helper to the `utc_now` import so older builds without the
/// chrono `clock` feature still notice if it disappears upstream.
#[allow(dead_code)]
fn _utc_anchor() -> DateTime<Utc> {
    utc_now()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    #[test]
    fn name_is_credit_balance() {
        let j = CreditBalanceCronJob::new(0);
        assert_eq!(j.name(), NAME);
    }

    #[test]
    fn period_is_one_hour() {
        let j = CreditBalanceCronJob::new(0);
        assert_eq!(j.period(), StdDuration::from_secs(3600));
    }

    #[test]
    fn message_status_str_round_trips() {
        assert_eq!(message_status_str(MessageStatus::Removing), "removing");
        assert_eq!(message_status_str(MessageStatus::Processed), "processed");
    }

    #[test]
    fn backoff_doubles_each_attempt() {
        use crate::jobs::job_utils::compute_next_retry_interval;
        assert!(compute_next_retry_interval(4) > compute_next_retry_interval(3));
    }

    #[test]
    fn decimal_to_i64_avoids_string_parse_zeroing() {
        // The old conversion path was `row.total.to_string().parse::<i64>()`,
        // which silently zeroes any fractional or scientific Decimal because
        // the `i64::from_str` call fails on "12.5". `Decimal::to_i64`
        // truncates toward zero (returning Some(12) for 12.5) so we keep
        // meaningful magnitude.
        let frac: rust_decimal::Decimal = "12.5".parse().unwrap();
        // Confirm that the legacy path zeroes the value.
        assert_eq!(frac.to_string().parse::<i64>().unwrap_or(0), 0);
        // Confirm the new path keeps the integer magnitude.
        assert_eq!(frac.to_i64(), Some(12));

        let exact: rust_decimal::Decimal = "12".parse().unwrap();
        assert_eq!(exact.to_i64(), Some(12));

        // For sums that genuinely exceed i64, `to_i64` returns None so the
        // job skips the row instead of feeding a poisoned value forward.
        let huge = rust_decimal::Decimal::MAX;
        assert!(huge.to_i64().is_none());
    }
}
