//! Database accessor functions. Mirrors `aleph/db/accessors/*.py`.
//!
//! Each submodule takes a connection or `&DbPool` and operates on a specific
//! table.

pub mod address_stats;
pub mod aggregates;
pub mod authorizations;
pub mod balances;
pub mod chains;
pub mod cost;
pub mod cron_jobs;
pub mod files;
pub mod messages;
pub mod metrics;
pub mod peers;
pub mod pending_messages;
pub mod pending_txs;
pub mod posts;
pub mod vms;
