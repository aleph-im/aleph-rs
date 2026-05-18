//! Ports `tests/jobs/test_cron_job.py`.
//!
//! The Python test parametrises a CronRunner with a `BalanceCronJob` and
//! exercises it across three `now` values. We mirror those three calls and
//! assert the run completes without error.

mod common;

use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};

use aleph_ccn::jobs::cron::balance_job::BalanceCronJob;
use aleph_ccn::jobs::cron::cron_job::{CronJob, CronRunner};

use common::{start_postgres};

async fn seed_cron_row(pool: &aleph_ccn::db::DbPool, id: &str) {
    let client = pool.get().await.unwrap();
    let now = Utc::now() - Duration::hours(1);
    client
        .execute(
            "INSERT INTO cron_jobs(id, interval, last_run) VALUES ($1, 1, $2) \
             ON CONFLICT (id) DO UPDATE SET last_run = EXCLUDED.last_run",
            &[&id.to_string(), &now],
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn balance_cron_job_runs_for_multiple_datetimes() {
    let pg = start_postgres().await;
    seed_cron_row(&pg.pool, "balance").await;
    let job: Arc<dyn CronJob> = Arc::new(BalanceCronJob::new(0));
    let runner = CronRunner::new(pg.pool.clone(), vec![job]);

    let nows = [
        Utc.with_ymd_and_hms(2040, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 6, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
    ];
    for now in nows {
        runner.run(now).await.unwrap();
    }
}
