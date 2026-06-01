//! Persisted node scoring metrics (`crn_metrics` / `ccn_metrics` tables).
//!
//! Mirrors `src/aleph/db/models/metrics.py`.
//!
//! `crn_metrics` and `ccn_metrics` are RANGE-partitioned on `measured_at`
//! at the Postgres level. Partition DDL lives in migration V0061; the row
//! types here only describe the logical parent table. The PK is composite
//! `(id, measured_at)` because Postgres requires the partition key to be
//! part of any PK on a partitioned table; `id` is still an IDENTITY column.

use chrono::{DateTime, Utc};

/// Row of the `crn_metrics` table.
#[derive(Debug, Clone)]
pub struct CrnMetricDb {
    pub item_hash: String,
    pub measured_at: DateTime<Utc>,
    pub node_id: String,
    pub base_latency: Option<f64>,
    pub base_latency_ipv4: Option<f64>,
    pub full_check_latency: Option<f64>,
    pub diagnostic_vm_latency: Option<f64>,
}

/// Row of the `ccn_metrics` table.
#[derive(Debug, Clone)]
pub struct CcnMetricDb {
    pub item_hash: String,
    pub measured_at: DateTime<Utc>,
    pub node_id: String,
    pub base_latency: Option<f64>,
    pub base_latency_ipv4: Option<f64>,
    pub metrics_latency: Option<f64>,
    pub aggregate_latency: Option<f64>,
    pub file_download_latency: Option<f64>,
    pub pending_messages: Option<i32>,
    pub eth_height_remaining: Option<i32>,
}
