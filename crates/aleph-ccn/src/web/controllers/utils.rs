//! Shared helpers for HTTP controllers. Mirrors `aleph/web/controllers/utils.py`.

use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use deadpool_postgres::Object;
use serde_json::{Value, json};
use tokio::time::{Duration, timeout};

use crate::AlephError;
use crate::schemas::pending_messages::parse_message as parse_pending_message;
use crate::types::message_status::{MessageOrigin, MessageProcessingStatus, MessageStatus};
use crate::web::AppState;
use crate::web::controllers::error::{WebError, WebResult};

/// Default page size for paginated message listings.
pub const DEFAULT_MESSAGES_PER_PAGE: i64 = 20;

/// Maximum allowed pagination when a cursor is in play.
pub const CURSOR_MAX_PAGINATION: i64 = 200;

/// HTTP status for a message processing result.
pub fn processing_status_to_http_status(status: MessageProcessingStatus) -> StatusCode {
    match status {
        MessageProcessingStatus::ProcessedNewMessage
        | MessageProcessingStatus::ProcessedConfirmation => StatusCode::OK,
        MessageProcessingStatus::FailedWillRetry => StatusCode::ACCEPTED,
        MessageProcessingStatus::FailedRejected => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

/// HTTP status for a message-level status.
pub fn message_status_to_http_status(status: MessageStatus) -> StatusCode {
    match status {
        MessageStatus::Pending => StatusCode::ACCEPTED,
        MessageStatus::Processed => StatusCode::OK,
        MessageStatus::Rejected => StatusCode::UNPROCESSABLE_ENTITY,
        MessageStatus::Forgotten => StatusCode::GONE,
        MessageStatus::Removing => StatusCode::OK,
        MessageStatus::Removed => StatusCode::GONE,
    }
}

/// Validate cursor + pagination relationship. Mirrors `validate_cursor_pagination`.
pub fn validate_cursor_pagination(cursor: Option<&str>, pagination: i64) -> Result<i64, WebError> {
    if cursor.is_none() {
        return Ok(pagination);
    }
    if pagination == 0 {
        return Err(WebError::Unprocessable(
            "pagination=0 is not allowed with cursor-based pagination".into(),
        ));
    }
    Ok(pagination.min(CURSOR_MAX_PAGINATION))
}

/// Convert a DateTime<Utc> to a POSIX timestamp (float seconds with sub-second).
pub fn datetime_to_timestamp(dt: DateTime<Utc>) -> f64 {
    dt.timestamp() as f64 + (dt.timestamp_subsec_nanos() as f64) / 1_000_000_000.0
}

/// Look up an item hash path parameter.
pub fn parse_item_hash(s: &str) -> WebResult<&str> {
    if s.is_empty() {
        return Err(WebError::BadRequest(format!("Invalid message hash: {s}")));
    }
    Ok(s)
}

/// Render a serializable response as `application/json`.
pub fn json_response<T: serde::Serialize>(status: StatusCode, value: &T) -> Response {
    let body = serde_json::to_string(value).unwrap_or_default();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("response builder")
}

/// Render raw JSON text as `application/json`.
pub fn json_text_response(status: StatusCode, text: String) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(text))
        .expect("response builder")
}

/// Get a pooled DB client.
pub async fn get_db(state: &AppState) -> WebResult<Object> {
    state
        .pool
        .get()
        .await
        .map_err(|e| WebError::Internal(format!("pool: {e}")))
}

/// Publish a pending message on the configured P2P transports, then enqueue it
/// locally for fetch/process. Mirrors pyaleph `broadcast_and_process_message`.
pub async fn broadcast_and_process_message(
    state: &AppState,
    client: &(impl tokio_postgres::GenericClient + Sync),
    message_dict: &Value,
    sync: bool,
) -> WebResult<(StatusCode, Value)> {
    let pending = parse_pending_message(message_dict.clone())
        .map_err(|e| WebError::Unprocessable(e.to_string()))?;
    let payload = serde_json::to_string(message_dict)
        .map_err(|e| WebError::Internal(format!("message serialization: {e}")))?;
    let topic = state.config.aleph.queue_topic.clone();

    let failed_publications = publish_on_p2p_topics(state, &topic, &payload).await;
    let publication_status = match failed_publications.len() {
        0 => "success",
        1 => "warning",
        _ => "error",
    };
    if publication_status == "error" {
        return Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "publication_status": {
                    "status": publication_status,
                    "failed": failed_publications,
                },
                "message_status": Value::Null,
            }),
        ));
    }

    let pending_row = state
        .message_publisher
        .add_pending_message(
            client,
            message_dict,
            chrono::Utc::now(),
            None,
            true,
            Some(MessageOrigin::P2p),
        )
        .await?;

    let message_status = if sync && pending_row.is_some() {
        wait_for_message_terminal_status(client, pending.item_hash()).await
    } else {
        MessageStatus::Pending
    };
    let status_code = message_status_to_http_status(message_status);
    Ok((
        status_code,
        json!({
            "publication_status": {
                "status": publication_status,
                "failed": failed_publications,
            },
            "message_status": message_status,
        }),
    ))
}

pub async fn publish_on_p2p_topics(
    state: &AppState,
    topic: &str,
    payload: &str,
) -> Vec<&'static str> {
    let mut failed = Vec::new();
    if let Some(ipfs) = &state.ipfs_service {
        match timeout(Duration::from_secs(10), ipfs.pubsub_publish(topic, payload)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(?e, "failed to publish message on IPFS pubsub");
                failed.push("ipfs");
            }
            Err(_) => {
                tracing::warn!("timed out publishing message on IPFS pubsub");
                failed.push("ipfs");
            }
        }
    }
    if let Some(p2p) = &state.p2p_client {
        match timeout(
            Duration::from_secs(10),
            p2p.publish(Bytes::from(payload.to_owned()), topic, true),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(?e, "failed to publish message on P2P pubsub");
                failed.push("p2p");
            }
            Err(_) => {
                tracing::warn!("timed out publishing message on P2P pubsub");
                failed.push("p2p");
            }
        }
    } else {
        failed.push("p2p");
    }
    failed
}

async fn wait_for_message_terminal_status(
    client: &(impl tokio_postgres::GenericClient + Sync),
    item_hash: &str,
) -> MessageStatus {
    for _ in 0..300 {
        if let Ok(Some(row)) = client
            .query_opt(
                "SELECT status FROM message_status WHERE item_hash = $1",
                &[&item_hash],
            )
            .await
        {
            let status: String = row.get(0);
            match status.as_str() {
                "processed" => return MessageStatus::Processed,
                "rejected" => return MessageStatus::Rejected,
                "forgotten" => return MessageStatus::Forgotten,
                "removing" => return MessageStatus::Removing,
                "removed" => return MessageStatus::Removed,
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    MessageStatus::Pending
}

/// Map any AlephError to a WebError.
pub fn map_aleph(err: AlephError) -> WebError {
    err.into()
}
