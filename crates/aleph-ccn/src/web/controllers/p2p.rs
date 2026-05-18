//! Mirrors `aleph/web/controllers/p2p.py`.

use axum::Router;
use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde::Deserialize;
use serde_json::Value;

use crate::db::accessors::pending_messages::insert_pending_message;
use crate::db::models::pending_messages::PendingMessageDb;
use crate::schemas::pending_messages::parse_message as parse_pending_message;
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{get_db, json_text_response};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v0/p2p/pubsub/pub", post(pub_json))
        .route("/api/v0/ipfs/pubsub/pub", post(pub_json))
        .route("/api/v0/messages", post(pub_message))
}

#[derive(Debug, Deserialize)]
struct PubJsonRequest {
    topic: Option<String>,
    data: Option<Value>,
}

async fn pub_json(
    State(state): State<AppState>,
    Json(body): Json<PubJsonRequest>,
) -> WebResult<Response> {
    let topic = body
        .topic
        .as_deref()
        .ok_or_else(|| WebError::Unprocessable("'topic' is required".into()))?;
    let message_topic = &state.config.aleph.queue_topic;
    if topic != message_topic {
        return Err(WebError::Forbidden(format!(
            "Unauthorized P2P topic: {topic}. Use {message_topic}."
        )));
    }
    let data = match body.data {
        Some(Value::String(s)) => s,
        _ => {
            return Err(WebError::Unprocessable(
                "'data': expected a serialized JSON string.".into(),
            ));
        }
    };
    let parsed: Value = serde_json::from_str(&data)
        .map_err(|_| WebError::Unprocessable("'data': must be deserializable as JSON.".into()))?;
    let pending = parse_pending_message(parsed.clone())
        .map_err(|e| WebError::Unprocessable(e.to_string()))?;

    // Insert the pending row so the processor picks it up.
    let pending_db = pending_message_to_db(&pending, &parsed, "p2p")?;
    let client = get_db(&state).await?;
    insert_pending_message(&**client, &pending_db).await?;

    // Best-effort live publish on IPFS pubsub if enabled.
    if let Some(ipfs) = &state.ipfs_service {
        let _ = ipfs
            .pubsub_publish(&state.config.ipfs.alive_topic, &data)
            .await;
    }

    let resp = serde_json::json!({
        "status": "success",
        "failed": serde_json::Value::Array(Vec::new()),
    });
    Ok(json_text_response(StatusCode::OK, resp.to_string()))
}

#[derive(Debug, Deserialize)]
struct PubMessageRequest {
    #[serde(default)]
    sync: bool,
    message: Value,
}

async fn pub_message(
    State(state): State<AppState>,
    Json(body): Json<PubMessageRequest>,
) -> WebResult<Response> {
    let pending = parse_pending_message(body.message.clone())
        .map_err(|e| WebError::Unprocessable(e.to_string()))?;

    let pending_db = pending_message_to_db(&pending, &body.message, "api")?;
    let client = get_db(&state).await?;
    insert_pending_message(&**client, &pending_db).await?;

    // Sync mode: pyaleph waits for the processor to publish the outcome on a
    // RabbitMQ topic. Mimic this with a single-row poll on `messages` /
    // `rejected_messages` with a short timeout.
    let message_status = if body.sync {
        wait_for_processed(&client, &pending_db.item_hash).await
    } else {
        "pending".to_string()
    };

    let resp = serde_json::json!({
        "publication_status": { "status": "success", "failed": serde_json::Value::Array(Vec::new()) },
        "message_status": message_status,
    });
    let code = if message_status == "pending" {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok(json_text_response(code, resp.to_string()))
}

/// Build a `PendingMessageDb` from a parsed pending-message variant + the raw
/// dict received over the wire.
fn pending_message_to_db(
    _pending: &crate::schemas::pending_messages::BasePendingMessage,
    raw: &Value,
    origin: &str,
) -> WebResult<PendingMessageDb> {
    use crate::types::message_status::MessageOrigin;
    let origin_enum = match origin {
        "p2p" => Some(MessageOrigin::P2p),
        "ipfs" => Some(MessageOrigin::Ipfs),
        _ => Some(MessageOrigin::Onchain),
    };
    Ok(PendingMessageDb::from_message_dict(
        raw,
        chrono::Utc::now(),
        false,
        None,
        true,
        origin_enum,
    ))
}

/// Poll `messages` / `rejected_messages` for up to ~5s. Returns `"processed"`,
/// `"rejected"`, or `"pending"` if we time out.
async fn wait_for_processed(client: &deadpool_postgres::Object, item_hash: &str) -> String {
    use std::time::Duration;
    for _ in 0..50 {
        if let Ok(row) = client
            .query_opt(
                "SELECT status FROM message_status WHERE item_hash = $1",
                &[&item_hash],
            )
            .await
        {
            if let Some(row) = row {
                let status: String = row.get(0);
                let lower = status.to_lowercase();
                if lower == "processed" || lower == "rejected" || lower == "forgotten" {
                    return lower;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    "pending".into()
}
