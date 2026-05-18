//! Metric view accessors. Mirrors `aleph/db/accessors/metrics.py`.
//!
//! Queries the `ccn_metric_view` and `crn_metric_view` Postgres views
//! created by migration V0021.

use std::time::{SystemTime, UNIX_EPOCH};

use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::types::sort_order::SortOrder;

/// Result of [`query_metric_ccn`] — column-wise lists matching the Python
/// transpose-into-dict shape.
#[derive(Debug, Clone, Default)]
pub struct CcnMetricResult {
    pub item_hash: Vec<String>,
    pub measured_at: Vec<Option<f64>>,
    pub base_latency: Vec<Option<f64>>,
    pub base_latency_ipv4: Vec<Option<f64>>,
    pub metrics_latency: Vec<Option<f64>>,
    pub aggregate_latency: Vec<Option<f64>>,
    pub file_download_latency: Vec<Option<f64>>,
    pub pending_messages: Vec<Option<i32>>,
    pub eth_height_remaining: Vec<Option<i32>>,
}

/// Result of [`query_metric_crn`].
#[derive(Debug, Clone, Default)]
pub struct CrnMetricResult {
    pub item_hash: Vec<String>,
    pub measured_at: Vec<Option<f64>>,
    pub base_latency: Vec<Option<f64>>,
    pub base_latency_ipv4: Vec<Option<f64>>,
    pub full_check_latency: Vec<Option<f64>>,
    pub diagnostic_vm_latency: Vec<Option<f64>>,
}

fn now_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn resolve_window(start_date: Option<f64>, end_date: Option<f64>) -> Option<f64> {
    let two_weeks = 60.0 * 60.0 * 24.0 * 14.0;
    match (start_date, end_date) {
        (None, None) => Some(now_seconds() - two_weeks),
        (None, Some(end)) => Some(end - two_weeks),
        (Some(s), _) => Some(s),
    }
}

fn append_filters(
    sql: &mut String,
    params: &mut Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
    node_id: Option<&str>,
    start_date: Option<f64>,
    end_date: Option<f64>,
) {
    if let Some(nid) = node_id {
        params.push(Box::new(nid.to_string()));
        sql.push_str(&format!(" AND node_id = ${}", params.len()));
    }
    if let Some(s) = start_date {
        params.push(Box::new(s));
        sql.push_str(&format!(" AND measured_at >= ${}", params.len()));
    }
    if let Some(e) = end_date {
        params.push(Box::new(e));
        sql.push_str(&format!(" AND measured_at <= ${}", params.len()));
    }
}

/// Query the `ccn_metric_view`. Mirrors `query_metric_ccn`.
pub async fn query_metric_ccn(
    client: &impl GenericClient,
    node_id: Option<&str>,
    start_date: Option<f64>,
    end_date: Option<f64>,
    sort_order: Option<SortOrder>,
) -> AlephResult<CcnMetricResult> {
    let start_date = resolve_window(start_date, end_date);
    let mut sql = String::from(
        "SELECT item_hash, measured_at, base_latency, base_latency_ipv4, metrics_latency, \
                aggregate_latency, file_download_latency, pending_messages, eth_height_remaining \
         FROM ccn_metric_view WHERE 1=1",
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    append_filters(&mut sql, &mut params, node_id, start_date, end_date);
    if let Some(order) = sort_order {
        sql.push_str(&format!(" ORDER BY measured_at {}", order.to_sql()));
    }
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    let mut out = CcnMetricResult::default();
    for r in rows {
        out.item_hash.push(r.get::<_, String>("item_hash"));
        out.measured_at.push(r.try_get("measured_at").ok());
        out.base_latency.push(r.try_get("base_latency").ok());
        out.base_latency_ipv4
            .push(r.try_get("base_latency_ipv4").ok());
        out.metrics_latency.push(r.try_get("metrics_latency").ok());
        out.aggregate_latency
            .push(r.try_get("aggregate_latency").ok());
        out.file_download_latency
            .push(r.try_get("file_download_latency").ok());
        out.pending_messages
            .push(r.try_get("pending_messages").ok());
        out.eth_height_remaining
            .push(r.try_get("eth_height_remaining").ok());
    }
    Ok(out)
}

/// Query the `crn_metric_view`. Mirrors `query_metric_crn`.
pub async fn query_metric_crn(
    client: &impl GenericClient,
    node_id: &str,
    start_date: Option<f64>,
    end_date: Option<f64>,
    sort_order: Option<SortOrder>,
) -> AlephResult<CrnMetricResult> {
    let start_date = resolve_window(start_date, end_date);
    let mut sql = String::from(
        "SELECT item_hash, measured_at, base_latency, base_latency_ipv4, full_check_latency, \
                diagnostic_vm_latency \
         FROM crn_metric_view WHERE 1=1",
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    append_filters(&mut sql, &mut params, Some(node_id), start_date, end_date);
    if let Some(order) = sort_order {
        sql.push_str(&format!(" ORDER BY measured_at {}", order.to_sql()));
    }
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    let mut out = CrnMetricResult::default();
    for r in rows {
        out.item_hash.push(r.get::<_, String>("item_hash"));
        out.measured_at.push(r.try_get("measured_at").ok());
        out.base_latency.push(r.try_get("base_latency").ok());
        out.base_latency_ipv4
            .push(r.try_get("base_latency_ipv4").ok());
        out.full_check_latency
            .push(r.try_get("full_check_latency").ok());
        out.diagnostic_vm_latency
            .push(r.try_get("diagnostic_vm_latency").ok());
    }
    Ok(out)
}
