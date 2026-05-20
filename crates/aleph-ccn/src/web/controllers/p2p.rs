//! Mirrors `aleph/web/controllers/p2p.py`.

use axum::Router;
use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use serde::Deserialize;
use serde_json::Value;

use crate::schemas::pending_messages::parse_message as parse_pending_message;
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};
use crate::web::controllers::utils::{
    broadcast_and_process_message, json_text_response, publish_on_p2p_topics,
};

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
    let _pending = parse_pending_message(parsed.clone())
        .map_err(|e| WebError::Unprocessable(e.to_string()))?;

    let failed = publish_on_p2p_topics(&state, topic, &data).await;
    let publication_status = match failed.len() {
        0 => "success",
        1 => "warning",
        _ => "error",
    };

    let resp = serde_json::json!({
        "status": publication_status,
        "failed": failed,
    });
    let status_code = if publication_status == "error" {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::OK
    };
    Ok(json_text_response(status_code, resp.to_string()))
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
    let client = crate::web::controllers::utils::get_db(&state).await?;
    let (code, resp) =
        broadcast_and_process_message(&state, &**client, &body.message, body.sync).await?;
    Ok(json_text_response(code, resp.to_string()))
}
