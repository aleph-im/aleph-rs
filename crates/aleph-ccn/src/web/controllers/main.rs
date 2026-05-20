//! Mirrors `aleph/web/controllers/main.py`.
//!
//! Index, status WebSocket, /metrics, /metrics.json, and per-node metrics
//! endpoints. The WebSocket implementation broadcasts the latest metrics on a
//! polling interval; the broadcaster lives entirely inside the handler so the
//! controller is stateless from the AppState's point of view.

use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{CloseCode, CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;

/// RFC 6455 `1013 Try Again Later` — emitted when the WS connection cap is hit.
const WS_TRY_AGAIN_LATER: CloseCode = 1013;

use crate::web::AppState;
use crate::web::controllers::error::WebResult;
use crate::web::controllers::metrics::{
    ccn_metric_handler, crn_metric_handler, get_metrics, metrics_handler, metrics_json_handler,
};
use crate::web::controllers::utils::json_text_response;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/", get(index))
        .route("/metrics", get(metrics_handler))
        .route("/metrics.json", get(metrics_json_handler))
        .route("/api/v0/core/{node_id}/metrics", get(ccn_metric_handler))
        .route("/api/v0/compute/{node_id}/metrics", get(crn_metric_handler))
        .route("/api/ws0/status", get(status_ws))
}

async fn index(State(_state): State<AppState>) -> WebResult<Response> {
    Ok(json_text_response(StatusCode::OK, "aleph-ccn".to_string()))
}

/// Hold a status websocket open, sending an updated metrics payload every
/// `poll_interval`. Mirrors the Python `StatusBroadcaster`.
async fn status_ws(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_status_ws(socket, state))
}

async fn handle_status_ws(mut socket: WebSocket, state: AppState) {
    // Enforce the global connection cap up-front. Beyond capacity we send a
    // 1013 close frame (matching pyaleph's `WSCloseCode.TRY_AGAIN_LATER`).
    let cap = state.config.websocket.max_status_connections;
    let active = state.ws_status_active.clone();
    let prev = active.fetch_add(1, Ordering::SeqCst);
    if prev >= cap {
        active.fetch_sub(1, Ordering::SeqCst);
        let _ = socket
            .send(Message::Close(Some(CloseFrame {
                code: WS_TRY_AGAIN_LATER,
                reason: "Too many connections".into(),
            })))
            .await;
        return;
    }
    struct ActiveGuard(std::sync::Arc<std::sync::atomic::AtomicU32>);
    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let _guard = ActiveGuard(active);

    // Diff-only emission: keep the previous payload as a JSON value so we can
    // compute a per-field diff and send only the deltas. The very first frame
    // is always the full snapshot.
    let mut last_value: Option<serde_json::Value> = None;
    let poll = Duration::from_secs(state.config.websocket.heartbeat.max(1));
    let mut interval = tokio::time::interval(poll);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let metrics = match get_metrics(&state).await {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(?e, "status_ws: failed to compute metrics");
                        continue;
                    }
                };
                let value = serde_json::to_value(&metrics).unwrap_or(serde_json::Value::Null);
                let payload = match &last_value {
                    None => serde_json::to_string(&value).unwrap_or_default(),
                    Some(prev) => {
                        let diff = diff_metrics(prev, &value);
                        if diff.as_object().map(|m| m.is_empty()).unwrap_or(true) {
                            // No-op heartbeat: still send a Ping so the
                            // connection stays alive.
                            if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                                break;
                            }
                            continue;
                        }
                        serde_json::to_string(&diff).unwrap_or_default()
                    }
                };
                if socket.send(Message::Text(payload.into())).await.is_err() {
                    break;
                }
                last_value = Some(value);
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                _ => {}
            }
        }
    }
}

/// Compute a shallow per-field diff between two metrics snapshots. Only fields
/// whose value changed are included in the output object.
fn diff_metrics(prev: &serde_json::Value, next: &serde_json::Value) -> serde_json::Value {
    let (Some(p), Some(n)) = (prev.as_object(), next.as_object()) else {
        return next.clone();
    };
    let mut out = serde_json::Map::new();
    for (k, v) in n {
        if p.get(k) != Some(v) {
            out.insert(k.clone(), v.clone());
        }
    }
    serde_json::Value::Object(out)
}

// Silence unused imports if path-extracting handlers aren't called directly.
#[allow(dead_code)]
fn _types() {
    let _ = (
        std::marker::PhantomData::<Path<String>>,
        std::marker::PhantomData::<Query<std::collections::HashMap<String, String>>>,
    );
    // Force the import-of-trait usage; concrete types implement it.
    fn _into_response<T: IntoResponse>(t: T) -> axum::response::Response {
        t.into_response()
    }
}
