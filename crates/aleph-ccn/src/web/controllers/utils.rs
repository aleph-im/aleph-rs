//! Shared helpers for HTTP controllers. Mirrors `aleph/web/controllers/utils.py`.

use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::Response;
use chrono::{DateTime, Utc};
use deadpool_postgres::Object;

use crate::AlephError;
use crate::types::message_status::{MessageProcessingStatus, MessageStatus};
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

/// Map any AlephError to a WebError.
pub fn map_aleph(err: AlephError) -> WebError {
    err.into()
}
