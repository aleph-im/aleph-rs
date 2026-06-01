//! Metric accessors. Mirrors `aleph/db/accessors/metrics.py`.
//!
//! Queries the persisted `crn_metrics` / `ccn_metrics` tables (migration
//! V0061), which replace the on-the-fly JSON-unnesting views. Scoring
//! payloads carry `measured_at` as a Unix epoch number; the DB column is
//! `TIMESTAMPTZ` so the partition key is a real time. The API contract
//! still serializes `measured_at` as epoch seconds, so the query layer
//! converts back to `f64` on read.

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
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

fn epoch_to_datetime(epoch: f64) -> Option<DateTime<Utc>> {
    let secs = epoch.floor() as i64;
    let nsecs = ((epoch - epoch.floor()) * 1_000_000_000.0).round() as u32;
    Utc.timestamp_opt(secs, nsecs).single()
}

fn datetime_to_epoch(dt: DateTime<Utc>) -> f64 {
    dt.timestamp() as f64 + (dt.timestamp_subsec_nanos() as f64) / 1_000_000_000.0
}

fn measured_at_opt(row: &tokio_postgres::Row) -> Option<f64> {
    row.try_get::<_, DateTime<Utc>>("measured_at")
        .ok()
        .map(datetime_to_epoch)
}

fn append_filters(
    sql: &mut String,
    params: &mut Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
    node_id: Option<&str>,
    start_dt: Option<DateTime<Utc>>,
    end_dt: Option<DateTime<Utc>>,
) {
    if let Some(nid) = node_id {
        params.push(Box::new(nid.to_string()));
        sql.push_str(&format!(" AND node_id = ${}", params.len()));
    }
    if let Some(s) = start_dt {
        params.push(Box::new(s));
        sql.push_str(&format!(" AND measured_at >= ${}", params.len()));
    }
    if let Some(e) = end_dt {
        params.push(Box::new(e));
        sql.push_str(&format!(" AND measured_at <= ${}", params.len()));
    }
}

/// Query the persisted `ccn_metrics` table. Mirrors `query_metric_ccn`.
pub async fn query_metric_ccn(
    client: &impl GenericClient,
    node_id: Option<&str>,
    start_date: Option<f64>,
    end_date: Option<f64>,
    sort_order: Option<SortOrder>,
) -> AlephResult<CcnMetricResult> {
    let start_date = resolve_window(start_date, end_date);
    let start_dt = start_date.and_then(epoch_to_datetime);
    let end_dt = end_date.and_then(epoch_to_datetime);

    let mut sql = String::from(
        "SELECT item_hash, measured_at, base_latency, base_latency_ipv4, metrics_latency, \
                aggregate_latency, file_download_latency, pending_messages, eth_height_remaining \
         FROM ccn_metrics WHERE 1=1",
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    append_filters(&mut sql, &mut params, node_id, start_dt, end_dt);
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
        out.measured_at.push(measured_at_opt(&r));
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

/// Query the persisted `crn_metrics` table. Mirrors `query_metric_crn`.
pub async fn query_metric_crn(
    client: &impl GenericClient,
    node_id: &str,
    start_date: Option<f64>,
    end_date: Option<f64>,
    sort_order: Option<SortOrder>,
) -> AlephResult<CrnMetricResult> {
    let start_date = resolve_window(start_date, end_date);
    let start_dt = start_date.and_then(epoch_to_datetime);
    let end_dt = end_date.and_then(epoch_to_datetime);

    let mut sql = String::from(
        "SELECT item_hash, measured_at, base_latency, base_latency_ipv4, full_check_latency, \
                diagnostic_vm_latency \
         FROM crn_metrics WHERE 1=1",
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    append_filters(&mut sql, &mut params, Some(node_id), start_dt, end_dt);
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
        out.measured_at.push(measured_at_opt(&r));
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

fn coerce_float(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn coerce_int(value: Option<&Value>) -> Option<i32> {
    match value {
        Some(Value::Number(n)) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .and_then(|v| i32::try_from(v).ok()),
        Some(Value::String(s)) => s
            .parse::<i64>()
            .ok()
            .and_then(|v| i32::try_from(v).ok()),
        _ => None,
    }
}

/// Scoring payloads carry `measured_at` as a Unix epoch number. Return
/// `None` on anything that isn't a usable timestamp. Mirrors Python
/// `_coerce_measured_at`.
fn coerce_measured_at(value: Option<&Value>) -> Option<DateTime<Utc>> {
    coerce_float(value).and_then(epoch_to_datetime)
}

fn entry_node_id(entry: &Value) -> Option<String> {
    match entry.get("node_id") {
        // Reject missing-or-empty node_id: an empty string is unusable for
        // the (node_id, measured_at) lookups the API serves.
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(v) if !v.is_null() => {
            let s = v.to_string();
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        }
        _ => None,
    }
}

/// Insert scoring metrics rows derived from a POST message's inner content.
/// Mirrors `insert_node_metrics`. `content` is the inner `content.content`
/// object of the POST message.
pub async fn insert_node_metrics(
    client: &impl GenericClient,
    item_hash: &str,
    content: &Value,
) -> AlephResult<()> {
    let metrics = match content.get("metrics") {
        Some(Value::Object(m)) => m,
        _ => return Ok(()),
    };

    if let Some(Value::Array(crn_array)) = metrics.get("crn") {
        for entry in crn_array {
            if !entry.is_object() {
                continue;
            }
            let (Some(node_id), Some(measured_at)) =
                (entry_node_id(entry), coerce_measured_at(entry.get("measured_at")))
            else {
                continue;
            };
            client
                .execute(
                    "INSERT INTO crn_metrics \
                       (item_hash, node_id, measured_at, base_latency, base_latency_ipv4, \
                        full_check_latency, diagnostic_vm_latency) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &item_hash,
                        &node_id,
                        &measured_at,
                        &coerce_float(entry.get("base_latency")),
                        &coerce_float(entry.get("base_latency_ipv4")),
                        &coerce_float(entry.get("full_check_latency")),
                        &coerce_float(entry.get("diagnostic_vm_latency")),
                    ],
                )
                .await?;
        }
    }

    if let Some(Value::Array(ccn_array)) = metrics.get("ccn") {
        for entry in ccn_array {
            if !entry.is_object() {
                continue;
            }
            let (Some(node_id), Some(measured_at)) =
                (entry_node_id(entry), coerce_measured_at(entry.get("measured_at")))
            else {
                continue;
            };
            client
                .execute(
                    "INSERT INTO ccn_metrics \
                       (item_hash, node_id, measured_at, base_latency, base_latency_ipv4, \
                        metrics_latency, aggregate_latency, file_download_latency, \
                        pending_messages, eth_height_remaining) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                    &[
                        &item_hash,
                        &node_id,
                        &measured_at,
                        &coerce_float(entry.get("base_latency")),
                        &coerce_float(entry.get("base_latency_ipv4")),
                        &coerce_float(entry.get("metrics_latency")),
                        &coerce_float(entry.get("aggregate_latency")),
                        &coerce_float(entry.get("file_download_latency")),
                        &coerce_int(entry.get("pending_messages")),
                        &coerce_int(entry.get("eth_height_remaining")),
                    ],
                )
                .await?;
        }
    }

    Ok(())
}
