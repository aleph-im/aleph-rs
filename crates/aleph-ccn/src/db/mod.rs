//! Database connection + migrations. Mirrors `aleph/db/connection.py`
//! and `deployment/migrations/`.
//!
//! Uses `tokio-postgres` + `deadpool-postgres` for a pooled async client and
//! `refinery` for embedded SQL migrations.

pub mod accessors;
pub mod models;

use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

use crate::AlephError;
use crate::AlephResult;
use crate::config::PostgresSettings;

pub type DbPool = Pool;

pub async fn connect(cfg: &PostgresSettings) -> AlephResult<DbPool> {
    let mut pg_cfg = tokio_postgres::Config::new();
    pg_cfg
        .host(&cfg.host)
        .port(cfg.port)
        .user(&cfg.user)
        .password(&cfg.password)
        .dbname(&cfg.database)
        .connect_timeout(Duration::from_secs(10));

    let mgr_cfg = ManagerConfig {
        recycling_method: if cfg.pool_pre_ping {
            RecyclingMethod::Verified
        } else {
            RecyclingMethod::Fast
        },
    };
    let mgr = Manager::from_config(pg_cfg, NoTls, mgr_cfg);
    let pool = Pool::builder(mgr)
        .max_size(cfg.pool_size as usize)
        .build()
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    Ok(pool)
}

mod embedded {
    refinery::embed_migrations!("migrations");
}

pub async fn migrate(pool: &DbPool) -> AlephResult<()> {
    let mut client = pool
        .get()
        .await
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    let report = embedded::migrations::runner()
        .run_async(&mut **client)
        .await
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    tracing::info!("applied {} migration(s)", report.applied_migrations().len());
    Ok(())
}
