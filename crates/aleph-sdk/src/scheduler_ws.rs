//! WebSocket subscriber for the scheduler's real-time event stream
//! (`/api/v1/ws`).
//!
//! Used by `aleph instance create/start --wait` to learn the moment a VM is
//! scheduled (or rejected) instead of polling. The stream is filtered
//! server-side by `vm_hash` and carries no networking data, so callers still
//! poll the allocated CRN for the actual reachable IP once a `Scheduled` event
//! arrives.
//!
//! The connection loop mirrors [`crate::ws`]: the initial connect is awaited so
//! the caller can fall back to polling when the endpoint is unavailable, and a
//! background task reconnects with exponential backoff until the receiver is
//! dropped. tungstenite answers server pings automatically, so the heartbeat
//! needs no handling here.

use futures_util::StreamExt;
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use url::Url;

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;
const CHANNEL_BUFFER_SIZE: usize = 100;

#[derive(Debug, thiserror::Error)]
pub enum SchedulerWsError {
    /// The base URL had a scheme with no websocket equivalent.
    #[error("cannot derive a websocket scheme from the scheduler URL")]
    BadScheme,
    /// The websocket handshake / TCP connect failed.
    #[error("scheduler websocket connect failed")]
    Connect(#[source] Box<tokio_tungstenite::tungstenite::Error>),
}

/// A scheduling event relevant to waiting on a specific VM. Any other event
/// kind maps to [`VmSchedulingEvent::Other`] so new scheduler events never
/// break the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VmSchedulingEvent {
    /// The VM was assigned to a node.
    Scheduled { vm_hash: String, node_hash: String },
    /// The VM was removed from its node (deleted, payment failed, ...).
    Unscheduled { vm_hash: String, reason: String },
    /// The VM cannot be placed on any node.
    Unschedulable { vm_hash: String, reason: String },
    /// Any other event kind; callers ignore it.
    Other,
}

/// Wire envelope. Only `event` is needed; `id`, `timestamp`, `sequence` and
/// `source` are ignored (serde drops unknown fields by default).
#[derive(Deserialize)]
struct EventEnvelope {
    event: serde_json::Value,
}

/// Parse the externally-tagged `event` object (e.g. `{"VmScheduled": {...}}`)
/// into a [`VmSchedulingEvent`]. Tolerant of unknown variants and missing
/// fields so a scheduler schema change cannot crash the wait.
fn parse_event(event: &serde_json::Value) -> VmSchedulingEvent {
    let Some((kind, body)) = event.as_object().and_then(|m| m.iter().next()) else {
        return VmSchedulingEvent::Other;
    };
    let field = |k: &str| {
        body.get(k)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    };
    match kind.as_str() {
        "VmScheduled" => VmSchedulingEvent::Scheduled {
            vm_hash: field("vm_hash"),
            node_hash: field("node_hash"),
        },
        "VmUnscheduled" => VmSchedulingEvent::Unscheduled {
            vm_hash: field("vm_hash"),
            reason: reason_string(body.get("reason")),
        },
        "VmUnschedulable" => VmSchedulingEvent::Unschedulable {
            vm_hash: field("vm_hash"),
            reason: reason_string(body.get("reason")),
        },
        _ => VmSchedulingEvent::Other,
    }
}

/// Reasons are unit-enum strings (e.g. `"NoSuitableNode"`), but stay lenient if
/// the scheduler ever sends an object or omits the field.
fn reason_string(v: Option<&serde_json::Value>) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

fn build_ws_url(base_url: &Url, vm_hash: &str) -> Result<Url, SchedulerWsError> {
    let scheme = match base_url.scheme() {
        "https" => "wss",
        "http" => "ws",
        s => s,
    };
    let mut url = base_url.clone();
    url.set_scheme(scheme)
        .map_err(|_| SchedulerWsError::BadScheme)?;
    url.set_path("/api/v1/ws");
    url.query_pairs_mut()
        .clear()
        .append_pair("vm_hash", vm_hash);
    Ok(url)
}

/// Subscribe to scheduling events for one VM.
///
/// The returned receiver yields parsed events (filtered to the kinds in
/// [`VmSchedulingEvent`], dropping `Other`). The background task reconnects
/// with backoff if the socket drops, and exits when the receiver is dropped.
/// The initial connect is awaited, so an `Err` here is the caller's cue to fall
/// back to HTTP polling.
pub async fn subscribe_vm(
    base_url: &Url,
    vm_hash: &str,
) -> Result<mpsc::Receiver<VmSchedulingEvent>, SchedulerWsError> {
    let ws_url = build_ws_url(base_url, vm_hash)?;
    let (ws_stream, _) = connect_async(ws_url.as_str())
        .await
        .map_err(|e| SchedulerWsError::Connect(Box::new(e)))?;

    let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
    tokio::spawn(run_ws_loop(ws_url, ws_stream, tx));
    Ok(rx)
}

async fn run_ws_loop(
    ws_url: Url,
    initial_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    tx: mpsc::Sender<VmSchedulingEvent>,
) {
    let mut ws_stream = initial_stream;
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        let (_, mut read) = ws_stream.split();

        while let Some(msg) = read.next().await {
            match msg {
                Ok(WsMessage::Text(text)) => {
                    backoff_ms = INITIAL_BACKOFF_MS;
                    if let Ok(env) = serde_json::from_str::<EventEnvelope>(&text) {
                        let event = parse_event(&env.event);
                        // Drop `Other` to keep the channel quiet; the server
                        // already filters by vm_hash so everything here is ours.
                        if !matches!(event, VmSchedulingEvent::Other)
                            && tx.send(event).await.is_err()
                        {
                            return; // receiver dropped
                        }
                    }
                }
                Ok(WsMessage::Close(_)) => break,
                // Ping/pong/binary: ignored (tungstenite answers pings itself).
                Ok(_) => {}
                Err(_) => break, // stream error: reconnect
            }
        }

        if tx.is_closed() {
            return;
        }

        // Reconnect with exponential backoff until the socket comes back or the
        // receiver goes away.
        loop {
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            if tx.is_closed() {
                return;
            }
            match connect_async(ws_url.as_str()).await {
                Ok((new_stream, _)) => {
                    ws_stream = new_stream;
                    break;
                }
                Err(_) => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event_of(value: serde_json::Value) -> VmSchedulingEvent {
        let env: EventEnvelope = serde_json::from_value(json!({ "event": value })).unwrap();
        parse_event(&env.event)
    }

    #[test]
    fn parses_vm_scheduled() {
        let ev = event_of(json!({
            "VmScheduled": {
                "vm_hash": "abc123",
                "node_hash": "def456",
                "vm_type": "instance",
                "payment_type": null
            }
        }));
        assert_eq!(
            ev,
            VmSchedulingEvent::Scheduled {
                vm_hash: "abc123".into(),
                node_hash: "def456".into(),
            }
        );
    }

    #[test]
    fn parses_vm_unschedulable_reason() {
        let ev = event_of(json!({
            "VmUnschedulable": { "vm_hash": "abc123", "reason": "NoSuitableNode" }
        }));
        assert_eq!(
            ev,
            VmSchedulingEvent::Unschedulable {
                vm_hash: "abc123".into(),
                reason: "NoSuitableNode".into(),
            }
        );
    }

    #[test]
    fn parses_vm_unscheduled_reason() {
        let ev = event_of(json!({
            "VmUnscheduled": { "vm_hash": "abc123", "node_hash": "def456", "reason": "PaymentFailed" }
        }));
        assert_eq!(
            ev,
            VmSchedulingEvent::Unscheduled {
                vm_hash: "abc123".into(),
                reason: "PaymentFailed".into(),
            }
        );
    }

    #[test]
    fn unknown_variant_maps_to_other() {
        let ev = event_of(json!({ "VmMigrated": { "vm_hash": "abc123" } }));
        assert_eq!(ev, VmSchedulingEvent::Other);
    }

    #[test]
    fn missing_fields_are_lenient() {
        let ev = event_of(json!({ "VmScheduled": {} }));
        assert_eq!(
            ev,
            VmSchedulingEvent::Scheduled {
                vm_hash: String::new(),
                node_hash: String::new(),
            }
        );
    }

    #[test]
    fn build_ws_url_converts_scheme_and_sets_filter() {
        let base = Url::parse("https://scheduler.api.aleph.cloud/").unwrap();
        let url = build_ws_url(&base, "abc123").unwrap();
        assert_eq!(url.scheme(), "wss");
        assert_eq!(url.path(), "/api/v1/ws");
        assert_eq!(url.query(), Some("vm_hash=abc123"));
    }

    #[test]
    fn build_ws_url_http_becomes_ws() {
        let base = Url::parse("http://localhost:8081/").unwrap();
        let url = build_ws_url(&base, "x").unwrap();
        assert_eq!(url.scheme(), "ws");
    }
}
