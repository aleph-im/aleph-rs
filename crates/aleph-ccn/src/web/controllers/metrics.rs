//! Mirrors `aleph/web/controllers/metrics.py`.
//!
//! Builds the Prometheus-style metrics dataclass and renders it either as
//! Prometheus text or JSON.

use axum::http::StatusCode;
use axum::response::Response;
use serde::Serialize;
use serde_json::{Value, json};

use crate::db::accessors::chains::get_last_height;
use crate::db::accessors::messages::count_matching_messages_fast;
use crate::types::chain_sync::ChainEventType;
use crate::web::AppState;
use crate::web::controllers::error::WebResult;
use crate::web::controllers::utils::{get_db, json_text_response};

#[derive(Debug, Clone, Serialize)]
pub struct BuildInfo {
    pub python_version: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Metrics {
    pub pyaleph_build_info: BuildInfo,
    pub pyaleph_status_peers_total: i64,
    pub pyaleph_status_sync_messages_total: i64,
    pub pyaleph_status_sync_permanent_files_total: i64,
    pub pyaleph_status_sync_pending_messages_total: i64,
    pub pyaleph_status_sync_pending_txs_total: i64,
    pub pyaleph_status_chain_eth_last_committed_height: Option<i64>,
    pub pyaleph_processing_pending_messages_tasks_total: Option<i64>,
    pub pyaleph_status_sync_messages_reference_total: Option<i64>,
    pub pyaleph_status_sync_messages_remaining_total: Option<i64>,
    pub pyaleph_status_chain_eth_height_reference_total: Option<i64>,
    pub pyaleph_status_chain_eth_height_remaining_total: Option<i64>,
    pub pyaleph_ws_messages_connections_active: i64,
    pub pyaleph_ws_messages_connections_max: i64,
    pub pyaleph_ws_status_connections_active: i64,
    pub pyaleph_ws_status_connections_max: i64,
    pub pyaleph_ws_messages_broadcast_total: i64,
    pub pyaleph_ws_messages_connections_rejected_total: i64,
    pub pyaleph_ws_status_connections_rejected_total: i64,
    pub pyaleph_ws_broadcaster_consumer_restarts_total: i64,
    pub pyaleph_store_fetch_ipfs_total: i64,
    pub pyaleph_store_fetch_ipfs_failed_total: i64,
    pub pyaleph_store_fetch_ipfs_duration_ms_sum: i64,
    pub pyaleph_store_fetch_storage_total: i64,
    pub pyaleph_store_fetch_storage_failed_total: i64,
    pub pyaleph_store_fetch_storage_duration_ms_sum: i64,
}

/// Fetch metrics from the database (no Redis-backed WS counters).
pub async fn get_metrics(state: &AppState) -> WebResult<Metrics> {
    let client = get_db(state).await?;

    let n_pending_messages: i64 = {
        let row = client
            .query_one("SELECT COUNT(id) FROM pending_messages", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let n_pending_txs: i64 = {
        let row = client
            .query_one("SELECT COUNT(tx_hash) FROM pending_txs", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let n_synced_messages = count_matching_messages_fast(&**client, None, None, None, None)
        .await?
        .unwrap_or(0);
    let n_peers: i64 = {
        let row = client
            .query_one("SELECT COUNT(peer_id) FROM peers", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let n_file_pins: i64 = {
        let row = client
            .query_one("SELECT COUNT(id) FROM file_pins", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let eth_last_committed_height = get_last_height(
        &**client,
        aleph_types::chain::Chain::Ethereum,
        ChainEventType::Sync,
    )
    .await?
    .map(|v| v as i64);

    Ok(Metrics {
        pyaleph_build_info: BuildInfo {
            python_version: "n/a".to_string(),
            version: crate::VERSION.to_string(),
        },
        pyaleph_status_peers_total: n_peers,
        pyaleph_status_sync_messages_total: n_synced_messages,
        pyaleph_status_sync_permanent_files_total: n_file_pins,
        pyaleph_status_sync_pending_messages_total: n_pending_messages,
        pyaleph_status_sync_pending_txs_total: n_pending_txs,
        pyaleph_status_chain_eth_last_committed_height: eth_last_committed_height,
        pyaleph_processing_pending_messages_tasks_total: None,
        pyaleph_status_sync_messages_reference_total: None,
        pyaleph_status_sync_messages_remaining_total: None,
        pyaleph_status_chain_eth_height_reference_total: None,
        pyaleph_status_chain_eth_height_remaining_total: None,
        pyaleph_ws_messages_connections_active: 0,
        pyaleph_ws_messages_connections_max: state.config.websocket.max_message_connections as i64,
        pyaleph_ws_status_connections_active: 0,
        pyaleph_ws_status_connections_max: state.config.websocket.max_status_connections as i64,
        pyaleph_ws_messages_broadcast_total: 0,
        pyaleph_ws_messages_connections_rejected_total: 0,
        pyaleph_ws_status_connections_rejected_total: 0,
        pyaleph_ws_broadcaster_consumer_restarts_total: 0,
        // STORE file-fetch counters live in Redis (written by the message
        // processing workers via the node cache). The web layer does not yet
        // hold a Redis handle — like the WS counters above, these are reported
        // as 0 here. Mirrors the fields added in pyaleph #1164.
        pyaleph_store_fetch_ipfs_total: 0,
        pyaleph_store_fetch_ipfs_failed_total: 0,
        pyaleph_store_fetch_ipfs_duration_ms_sum: 0,
        pyaleph_store_fetch_storage_total: 0,
        pyaleph_store_fetch_storage_failed_total: 0,
        pyaleph_store_fetch_storage_duration_ms_sum: 0,
    })
}

/// Render a JSON dict as a string of Prometheus label pairs `{k=v,...}`.
fn format_dict_for_prometheus(map: &serde_json::Map<String, Value>) -> String {
    let mut parts = Vec::new();
    for (key, value) in map {
        if value.is_null() {
            continue;
        }
        parts.push(format!("{key}={value}"));
    }
    format!("{{{}}}", parts.join(","))
}

/// Format a metrics dataclass as Prometheus text.
pub fn format_metrics_prometheus(m: &Metrics) -> String {
    let value = serde_json::to_value(m).expect("metrics serializable");
    let map = match value.as_object() {
        Some(m) => m,
        None => return String::new(),
    };
    let mut lines = Vec::new();
    for (key, value) in map {
        if value.is_null() {
            continue;
        }
        if let Some(obj) = value.as_object() {
            lines.push(format!("{key}{} 1", format_dict_for_prometheus(obj)));
        } else {
            lines.push(format!("{key} {value}"));
        }
    }
    lines.join("\n")
}

/// `/metrics` — Prometheus text format.
pub async fn metrics_handler(state: axum::extract::State<AppState>) -> WebResult<Response> {
    let m = get_metrics(&state.0).await?;
    let body = format_metrics_prometheus(&m);
    let resp = Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "text/plain")
        .body(axum::body::Body::from(body))
        .expect("response");
    Ok(resp)
}

/// `/metrics.json` — JSON variant.
pub async fn metrics_json_handler(state: axum::extract::State<AppState>) -> WebResult<Response> {
    let m = get_metrics(&state.0).await?;
    let body = serde_json::to_string(&m).expect("metrics serializable");
    Ok(json_text_response(StatusCode::OK, body))
}

/// `/api/v0/core/{node_id}/metrics`.
pub async fn ccn_metric_handler(
    state: axum::extract::State<AppState>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    if node_id.is_empty() {
        return Err(crate::web::controllers::error::WebError::Unprocessable(
            "node_id must be specified.".into(),
        ));
    }
    let start_date = q.get("start_date").and_then(|v| v.parse::<f64>().ok());
    let end_date = q.get("end_date").and_then(|v| v.parse::<f64>().ok());
    let sort_order = q
        .get("sort")
        .and_then(|s| crate::types::sort_order::SortOrder::parse_for_metrics(s));
    let client = get_db(&state.0).await?;
    let res = crate::db::accessors::metrics::query_metric_ccn(
        &**client,
        Some(&node_id),
        start_date,
        end_date,
        sort_order,
    )
    .await?;
    if res.item_hash.is_empty() {
        return Err(crate::web::controllers::error::WebError::NotFound(
            "node not found".into(),
        ));
    }
    let body = json!({
        "metrics": {
            "item_hash": res.item_hash,
            "measured_at": res.measured_at,
            "base_latency": res.base_latency,
            "base_latency_ipv4": res.base_latency_ipv4,
            "metrics_latency": res.metrics_latency,
            "aggregate_latency": res.aggregate_latency,
            "file_download_latency": res.file_download_latency,
            "pending_messages": res.pending_messages,
            "eth_height_remaining": res.eth_height_remaining,
        }
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}

/// `/api/v0/compute/{node_id}/metrics`.
pub async fn crn_metric_handler(
    state: axum::extract::State<AppState>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    if node_id.is_empty() {
        return Err(crate::web::controllers::error::WebError::Unprocessable(
            "node_id must be specified.".into(),
        ));
    }
    let start_date = q.get("start_date").and_then(|v| v.parse::<f64>().ok());
    let end_date = q.get("end_date").and_then(|v| v.parse::<f64>().ok());
    let sort_order = q
        .get("sort")
        .and_then(|s| crate::types::sort_order::SortOrder::parse_for_metrics(s));
    let client = get_db(&state.0).await?;
    let res = crate::db::accessors::metrics::query_metric_crn(
        &**client, &node_id, start_date, end_date, sort_order,
    )
    .await?;
    if res.item_hash.is_empty() {
        return Err(crate::web::controllers::error::WebError::NotFound(
            "node not found".into(),
        ));
    }
    let body = json!({
        "metrics": {
            "item_hash": res.item_hash,
            "measured_at": res.measured_at,
            "base_latency": res.base_latency,
            "base_latency_ipv4": res.base_latency_ipv4,
            "full_check_latency": res.full_check_latency,
            "diagnostic_vm_latency": res.diagnostic_vm_latency,
        }
    });
    Ok(json_text_response(StatusCode::OK, body.to_string()))
}
