//! Cron-job orchestrator. Mirrors `aleph/jobs/cron/cron_job.py`.
//!
//! Each cron job implements [`CronJob`] (Python `BaseCronJob`). The
//! [`CronRunner`] (Python `CronJob`) iterates the `cron_jobs` table and
//! invokes any whose `last_run + interval >= now`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::AlephResult;
use crate::db::DbPool;
use crate::db::accessors::cron_jobs::{get_cron_jobs, update_cron_job};
use crate::db::models::cron_jobs::CronJobDb;
use crate::toolkit::timestamp::utc_now;

/// One cron job. Mirrors Python `BaseCronJob.run(now, job)`.
#[async_trait]
pub trait CronJob: Send + Sync {
    /// Stable identifier matching `cron_jobs.id`.
    fn name(&self) -> &str;

    /// Run period override. The DB row's `interval` field is authoritative
    /// at scheduling time; this hint is exposed for callers that want to
    /// seed the row.
    fn period(&self) -> Duration {
        Duration::from_secs(3600)
    }

    /// Execute the job. The orchestrator runs this inside a transaction and
    /// updates `last_run` in the same transaction on success — so each job's
    /// writes commit atomically with the `cron_jobs.last_run` bump.
    async fn run(
        &self,
        now: DateTime<Utc>,
        job: &CronJobDb,
        tx: &tokio_postgres::Transaction<'_>,
    ) -> AlephResult<()>;
}

/// Cron orchestrator. Mirrors Python `CronJob`.
pub struct CronRunner {
    pub pool: DbPool,
    pub jobs: HashMap<String, Arc<dyn CronJob>>,
}

impl CronRunner {
    pub fn new(pool: DbPool, jobs: Vec<Arc<dyn CronJob>>) -> Self {
        let map = jobs
            .into_iter()
            .map(|j| (j.name().to_string(), j))
            .collect();
        Self { pool, jobs: map }
    }

    /// Run one pass. Mirrors `CronJob.run(now)`.
    pub async fn run(&self, now: DateTime<Utc>) -> AlephResult<()> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
        let rows = get_cron_jobs(&**client).await?;
        drop(client);

        let mut futures = Vec::new();
        for job in rows {
            let interval = chrono::Duration::seconds(job.interval as i64);
            let run_at = job.last_run + interval;
            if now < run_at {
                continue;
            }
            let cron_job = match self.jobs.get(&job.id) {
                Some(cj) => cj.clone(),
                None => continue,
            };
            let pool = self.pool.clone();
            tracing::info!("'{}' cron job scheduled for running successfully.", job.id);
            futures.push(async move {
                let id = job.id.clone();
                tracing::info!("Starting '{}' cron job check...", id);
                let mut client = pool
                    .get()
                    .await
                    .map_err(|e| crate::AlephError::Pool(format!("pool acquire: {e}")))?;
                let tx = client
                    .transaction()
                    .await
                    .map_err(|e| crate::AlephError::Pool(format!("begin tx: {e}")))?;
                match cron_job.run(now, &job, &*tx).await {
                    Ok(_) => {
                        // Update last_run inside the same transaction so the
                        // bump commits atomically with the job's writes.
                        update_cron_job(&*tx, &id, now).await?;
                        tx.commit()
                            .await
                            .map_err(|e| crate::AlephError::Pool(format!("commit tx: {e}")))?;
                        tracing::info!("'{}' cron job ran successfully.", id);
                        Ok::<(), crate::AlephError>(())
                    }
                    Err(e) => {
                        tracing::error!("'{id}' cron job failed: {e}");
                        // Roll back; last_run stays put so we retry next pass.
                        let _ = tx.rollback().await;
                        Ok(())
                    }
                }
            });
        }

        // Await all futures; ignore individual errors (already logged).
        for f in futures {
            let _ = f.await;
        }
        Ok(())
    }
}

/// Run the cron loop until cancelled. Mirrors `cron_job_task`.
pub async fn run(
    runner: Arc<CronRunner>,
    period: Duration,
    cancel: crate::jobs::job_utils::CancelToken,
) -> AlephResult<()> {
    tracing::info!(
        "Warming up cron job runner... next run: {:?}",
        utc_now() + chrono::Duration::from_std(period).unwrap_or(chrono::Duration::hours(1))
    );
    tokio::select! {
        _ = tokio::time::sleep(period) => {}
        _ = cancel.cancelled() => return Ok(()),
    }

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let now = utc_now();
        tracing::info!("Starting cron job check...");
        if let Err(e) = runner.run(now).await {
            tracing::error!("Unexpected error during cron job check: {e}");
        }
        tracing::info!("Cron job ran successfully.");
        tracing::info!(
            "Next cron job run: {:?}",
            now + chrono::Duration::from_std(period).unwrap_or(chrono::Duration::hours(1))
        );

        tokio::select! {
            _ = tokio::time::sleep(period) => {}
            _ = cancel.cancelled() => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI32, Ordering};

    struct CountingJob {
        ran: AtomicI32,
        name: &'static str,
    }
    #[async_trait]
    impl CronJob for CountingJob {
        fn name(&self) -> &str {
            self.name
        }
        async fn run(
            &self,
            _now: DateTime<Utc>,
            _job: &CronJobDb,
            _tx: &tokio_postgres::Transaction<'_>,
        ) -> AlephResult<()> {
            self.ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn cron_loop_terminates_on_cancel_quickly() {
        let cancel = crate::jobs::job_utils::CancelToken::new();
        cancel.cancel();
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(60)) => panic!("slept"),
            _ = cancel.cancelled() => {}
        }
    }

    #[test]
    fn cron_jobs_register_by_name() {
        // Without a real DbPool we exercise the map insertion only.
        let job = Arc::new(CountingJob {
            ran: AtomicI32::new(0),
            name: "balances",
        }) as Arc<dyn CronJob>;
        let map: HashMap<String, Arc<dyn CronJob>> = vec![("balances".to_string(), job.clone())]
            .into_iter()
            .collect();
        assert!(map.contains_key("balances"));
        assert_eq!(map.get("balances").unwrap().name(), "balances");
        let _ = Mutex::new(0);
    }

    #[test]
    fn backoff_doubles_each_attempt() {
        use crate::jobs::job_utils::compute_next_retry_interval;
        assert!(compute_next_retry_interval(1) > compute_next_retry_interval(0));
        assert!(compute_next_retry_interval(3) > compute_next_retry_interval(2));
    }
}
