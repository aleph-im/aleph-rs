//! `cron_jobs` accessors. Mirrors `aleph/db/accessors/cron_jobs.py`.

use chrono::{DateTime, Utc};
use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::db::models::cron_jobs::CronJobDb;

/// Fetch all rows from `cron_jobs`.
pub async fn get_cron_jobs(client: &impl GenericClient) -> AlephResult<Vec<CronJobDb>> {
    let rows = client
        .query("SELECT id, interval, last_run FROM cron_jobs", &[])
        .await?;
    Ok(rows.iter().map(CronJobDb::from_row).collect())
}

/// Fetch a single cron job by id.
pub async fn get_cron_job(client: &impl GenericClient, id: &str) -> AlephResult<Option<CronJobDb>> {
    let row = client
        .query_opt(
            "SELECT id, interval, last_run FROM cron_jobs WHERE id = $1",
            &[&id],
        )
        .await?;
    Ok(row.as_ref().map(CronJobDb::from_row))
}

/// Update the `last_run` column for the given job id.
pub async fn update_cron_job(
    client: &impl GenericClient,
    id: &str,
    last_run: DateTime<Utc>,
) -> AlephResult<()> {
    client
        .execute(
            "UPDATE cron_jobs SET last_run = $1 WHERE id = $2",
            &[&last_run, &id],
        )
        .await?;
    Ok(())
}

/// Delete the cron job with the given id.
pub async fn delete_cron_job(client: &impl GenericClient, id: &str) -> AlephResult<()> {
    client
        .execute("DELETE FROM cron_jobs WHERE id = $1", &[&id])
        .await?;
    Ok(())
}
