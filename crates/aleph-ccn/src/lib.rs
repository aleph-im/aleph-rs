//! aleph-ccn — Rust port of pyaleph (Aleph.im Core Channel Node).
//!
//! Modules mirror `aleph/<name>` in the Python tree. The goal is iso-functional
//! parity with `pyaleph`. We deliberately avoid stubs — if a module is declared
//! here, it ships with a real implementation.

pub mod chains;
pub mod config;
pub mod db;
pub mod error;
pub mod handlers;
pub mod jobs;
pub mod network;
pub mod permissions;
pub mod repair;
pub mod schemas;
pub mod services;
pub mod storage;
pub mod toolkit;
pub mod types;
pub mod web;

pub use error::{AlephError, AlephResult};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn run(cfg: config::Settings) -> AlephResult<()> {
    let pool = db::connect(&cfg.postgres).await?;
    db::migrate(&pool).await?;
    tracing::info!("database ready");
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    Ok(())
}
