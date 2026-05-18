//! Cron job records (`cron_jobs` table).
//!
//! Mirrors `src/aleph/db/models/cron_jobs.py`.

use chrono::{DateTime, Utc};

/// Row of the `cron_jobs` table.
#[derive(Debug, Clone)]
pub struct CronJobDb {
    pub id: String,
    /// Run interval, in seconds.
    pub interval: i32,
    pub last_run: DateTime<Utc>,
}

impl CronJobDb {
    /// Build a [`CronJobDb`] from a database row.
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            id: row.get("id"),
            interval: row.get("interval"),
            last_run: row.get("last_run"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_job_construct() {
        let job = CronJobDb {
            id: "balances".into(),
            interval: 3600,
            last_run: Utc::now(),
        };
        assert_eq!(job.id, "balances");
        assert_eq!(job.interval, 3600);
    }
}
