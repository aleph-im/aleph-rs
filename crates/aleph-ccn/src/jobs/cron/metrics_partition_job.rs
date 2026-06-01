//! Cron job that maintains monthly partitions of `crn_metrics` and
//! `ccn_metrics`. Mirrors `aleph/jobs/cron/metrics_partition_job.py`.
//!
//! Two responsibilities per run:
//!
//! 1. Pre-create the next `lookahead_months` worth of monthly partitions
//!    if they don't already exist. This guarantees there's always a real
//!    partition ready for incoming scoring posts, so writes never have to
//!    fall back to the DEFAULT catch-all partition.
//!
//! 2. Detach + drop partitions whose upper bound is older than the
//!    retention cutoff (`retention_months` ago). DETACH first so the parent
//!    table only briefly holds an ACCESS EXCLUSIVE lock; the subsequent DROP
//!    only touches the (now-standalone) child table.
//!
//! Both operations are idempotent. A run that finds the next partition
//! already present and nothing past the cutoff is a no-op.
//!
//! The DEFAULT partition is left untouched. If it ever contains rows the
//! cron logs a warning (operational signal that the lookahead is too short
//! or that out-of-range data is arriving).

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};

use crate::AlephResult;
use crate::db::models::cron_jobs::CronJobDb;
use crate::jobs::cron::cron_job::CronJob;
use crate::toolkit::partitions::{add_months, month_floor, monthly_bounds, partition_name, ts_literal};

/// Stable id matching the `cron_jobs.id` value used by pyaleph.
pub const NAME: &str = "metrics_partition";

const PARTITIONED_TABLES: [&str; 2] = ["crn_metrics", "ccn_metrics"];

/// Cron job implementation. Mirrors Python `MetricsPartitionCronJob`.
pub struct MetricsPartitionCronJob {
    /// Drop partitions whose upper bound is older than `now - retention_months`.
    pub retention_months: i32,
    /// Ensure partitions exist up to and including `now + lookahead_months`.
    pub lookahead_months: i32,
}

impl MetricsPartitionCronJob {
    pub fn new(retention_months: i32, lookahead_months: i32) -> Self {
        Self {
            retention_months,
            lookahead_months,
        }
    }
}

#[async_trait]
impl CronJob for MetricsPartitionCronJob {
    fn name(&self) -> &str {
        NAME
    }

    fn period(&self) -> Duration {
        Duration::from_secs(86400)
    }

    async fn run(
        &self,
        now: DateTime<Utc>,
        _job: &CronJobDb,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> AlephResult<()> {
        let now_month = month_floor(now);
        let cutoff = add_months(now_month, -self.retention_months);
        // Lookahead is inclusive: ensure partition for now_month + N exists,
        // so range becomes [..., now_month + N + 1).
        let lookahead_upper = add_months(now_month, self.lookahead_months + 1);

        for table in PARTITIONED_TABLES {
            ensure_partitions(tx, table, now_month, lookahead_upper).await?;
            drop_past_cutoff(tx, table, cutoff).await?;
            warn_if_default_has_rows(tx, table).await?;
        }
        Ok(())
    }
}

/// Create any missing monthly partitions in `[start, end_exclusive)`.
async fn ensure_partitions(
    tx: &tokio_postgres::Transaction<'_>,
    table: &str,
    start: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
) -> AlephResult<()> {
    let existing = list_partitions(tx, table).await?;
    let existing_names: std::collections::HashSet<&str> =
        existing.iter().map(|(name, _)| name.as_str()).collect();
    for (lower, upper) in monthly_bounds(start, end_exclusive) {
        let name = partition_name(table, lower);
        if existing_names.contains(name.as_str()) {
            continue;
        }
        tracing::info!(
            "Creating partition {} on {} for [{}, {})",
            name,
            table,
            lower.to_rfc3339(),
            upper.to_rfc3339()
        );
        let sql = format!(
            "CREATE TABLE {name} PARTITION OF {table} \
             FOR VALUES FROM ('{}') TO ('{}')",
            ts_literal(lower),
            ts_literal(upper)
        );
        tx.execute(&sql, &[]).await?;
    }
    Ok(())
}

/// DETACH + DROP partitions whose upper bound is `<= cutoff`.
///
/// DETACH briefly takes ACCESS EXCLUSIVE on the parent, then the DROP only
/// touches the now-standalone child. Metrics tables are not on a
/// latency-sensitive read path so plain DETACH is fine; CONCURRENTLY would
/// require autocommit, which the cron's transactional session doesn't offer.
async fn drop_past_cutoff(
    tx: &tokio_postgres::Transaction<'_>,
    table: &str,
    cutoff: DateTime<Utc>,
) -> AlephResult<()> {
    for (name, bounds) in list_partitions(tx, table).await? {
        // The DEFAULT partition has no bounds. Skip.
        let Some((_lower, upper)) = bounds else {
            continue;
        };
        if upper <= cutoff {
            tracing::info!(
                "Dropping partition {} on {} (upper={} <= cutoff={})",
                name,
                table,
                upper.to_rfc3339(),
                cutoff.to_rfc3339()
            );
            tx.execute(&format!("ALTER TABLE {table} DETACH PARTITION {name}"), &[])
                .await?;
            tx.execute(&format!("DROP TABLE {name}"), &[]).await?;
        }
    }
    Ok(())
}

async fn warn_if_default_has_rows(
    tx: &tokio_postgres::Transaction<'_>,
    table: &str,
) -> AlephResult<()> {
    let default_name = format!("{table}_default");
    let row = tx
        .query_one(&format!("SELECT count(*) FROM {default_name}"), &[])
        .await?;
    let count: i64 = row.get(0);
    if count > 0 {
        tracing::warn!(
            "DEFAULT partition {} holds {} rows. Lookahead may be too short, \
             or out-of-range timestamps are arriving.",
            default_name,
            count
        );
    }
    Ok(())
}

/// Return `(child_name, Some((lower, upper)))` for every existing partition
/// of `parent`. The DEFAULT partition appears with `(name, None)`.
async fn list_partitions(
    tx: &tokio_postgres::Transaction<'_>,
    parent: &str,
) -> AlephResult<Vec<(String, Option<(DateTime<Utc>, DateTime<Utc>)>)>> {
    let rows = tx
        .query(
            "SELECT c.relname AS child_name, \
                    pg_get_expr(c.relpartbound, c.oid) AS bound_expr \
             FROM pg_inherits i \
             JOIN pg_class p ON p.oid = i.inhparent \
             JOIN pg_class c ON c.oid = i.inhrelid \
             WHERE p.relname = $1",
            &[&parent],
        )
        .await?;

    let mut out = Vec::new();
    for row in rows {
        let name: String = row.get("child_name");
        let expr: Option<String> = row.get("bound_expr");
        out.push((name, parse_bound_expr(expr.as_deref())));
    }
    Ok(out)
}

/// Parse `pg_get_expr` output for a RANGE partition.
///
/// Examples:
/// * `FOR VALUES FROM ('2026-05-01 00:00:00+00') TO ('2026-06-01 00:00:00+00')`
/// * `DEFAULT`
fn parse_bound_expr(expr: Option<&str>) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let expr = expr?;
    if expr.contains("DEFAULT") {
        return None;
    }
    // The expression is well-formed Postgres output; parse the two quoted
    // timestamps in order.
    let parts: Vec<&str> = expr.split('\'').collect();
    if parts.len() < 5 {
        return None;
    }
    let lower = parse_pg_timestamp(parts[1])?;
    let upper = parse_pg_timestamp(parts[3])?;
    Some((lower, upper))
}

fn parse_pg_timestamp(s: &str) -> Option<DateTime<Utc>> {
    // Postgres emits e.g. "2026-05-01 00:00:00+00". Try RFC3339 (with a 'T')
    // first, then the space-separated forms.
    if let Ok(dt) = DateTime::parse_from_rfc3339(&s.replace(' ', "T")) {
        return Some(dt.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%#z", "%Y-%m-%d %H:%M:%S%z"] {
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
    }
    // No timezone: assume UTC.
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Utc.from_utc_datetime(&naive).into();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_is_metrics_partition() {
        let j = MetricsPartitionCronJob::new(12, 1);
        assert_eq!(j.name(), NAME);
    }

    #[test]
    fn period_is_one_day() {
        let j = MetricsPartitionCronJob::new(12, 1);
        assert_eq!(j.period(), Duration::from_secs(86400));
    }

    #[test]
    fn parse_default_partition_yields_none() {
        assert!(parse_bound_expr(Some("DEFAULT")).is_none());
        assert!(parse_bound_expr(None).is_none());
    }

    #[test]
    fn parse_range_partition_bounds() {
        let expr = "FOR VALUES FROM ('2026-05-01 00:00:00+00') TO ('2026-06-01 00:00:00+00')";
        let (lower, upper) = parse_bound_expr(Some(expr)).expect("parsed");
        assert_eq!(
            lower,
            Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).single().unwrap()
        );
        assert_eq!(
            upper,
            Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).single().unwrap()
        );
    }
}
