//! Database row types. Mirrors `aleph/db/models/*.py`.
//!
//! Each module exposes a pure data struct and (where relevant) helper builders.
//! Mutations live in `crate::db::accessors`.

pub mod account_costs;
pub mod aggregates;
pub mod balances;
pub mod chains;
pub mod cron_jobs;
pub mod files;
pub mod message_counts;
pub mod messages;
pub mod metrics;
pub mod peers;
pub mod pending_messages;
pub mod pending_txs;
pub mod posts;
pub mod vms;
