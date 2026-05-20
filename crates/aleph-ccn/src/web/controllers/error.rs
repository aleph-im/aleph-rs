//! HTTP error helpers shared across controllers.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use deadpool_postgres::PoolError;

use crate::AlephError;

/// Web-layer error. Maps cleanly to HTTP responses.
#[derive(Debug)]
pub enum WebError {
    /// 102 Processing — message accepted asynchronously but not yet processed.
    Processing(String),
    /// 400 Bad Request — invalid params or malformed input.
    BadRequest(String),
    /// 402 Payment Required.
    PaymentRequired(String),
    /// 403 Forbidden.
    Forbidden(String),
    /// 404 Not Found.
    NotFound(String),
    /// 410 Gone (FORGOTTEN/REMOVED).
    Gone(String),
    /// 502 Bad Gateway.
    BadGateway(String),
    /// 504 Gateway Timeout.
    GatewayTimeout(String),
    /// 413 Payload Too Large.
    PayloadTooLarge(String),
    /// 422 Unprocessable Entity — pydantic-style validation error.
    Unprocessable(String),
    /// 500 Internal Server Error.
    Internal(String),
}

impl WebError {
    pub fn status(&self) -> StatusCode {
        match self {
            WebError::Processing(_) => StatusCode::PROCESSING,
            WebError::BadRequest(_) => StatusCode::BAD_REQUEST,
            WebError::PaymentRequired(_) => StatusCode::PAYMENT_REQUIRED,
            WebError::Forbidden(_) => StatusCode::FORBIDDEN,
            WebError::NotFound(_) => StatusCode::NOT_FOUND,
            WebError::Gone(_) => StatusCode::GONE,
            WebError::BadGateway(_) => StatusCode::BAD_GATEWAY,
            WebError::GatewayTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
            WebError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            WebError::Unprocessable(_) => StatusCode::UNPROCESSABLE_ENTITY,
            WebError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn body(&self) -> String {
        match self {
            WebError::Processing(s)
            | WebError::BadRequest(s)
            | WebError::PaymentRequired(s)
            | WebError::Forbidden(s)
            | WebError::NotFound(s)
            | WebError::Gone(s)
            | WebError::BadGateway(s)
            | WebError::GatewayTimeout(s)
            | WebError::PayloadTooLarge(s)
            | WebError::Unprocessable(s)
            | WebError::Internal(s) => s.clone(),
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        (self.status(), self.body()).into_response()
    }
}

impl From<AlephError> for WebError {
    fn from(value: AlephError) -> Self {
        match value {
            AlephError::NotFound(s) => WebError::NotFound(s),
            AlephError::InvalidMessage(s) => WebError::Unprocessable(s),
            AlephError::InvalidSignature => WebError::Forbidden("invalid signature".to_string()),
            AlephError::InvalidItemHash(s) => {
                WebError::BadRequest(format!("invalid item hash: {s}"))
            }
            AlephError::ContentTooLarge { actual, limit } => {
                WebError::PayloadTooLarge(format!("size {actual} exceeds limit {limit}"))
            }
            AlephError::Unauthorized(s) => WebError::Forbidden(s),
            AlephError::Json(e) => WebError::Unprocessable(e.to_string()),
            AlephError::Yaml(e) => WebError::Internal(e.to_string()),
            AlephError::Hex(e) => WebError::BadRequest(e.to_string()),
            AlephError::Http(e) => WebError::Internal(e.to_string()),
            AlephError::Storage(s) => WebError::Internal(format!("storage: {s}")),
            AlephError::P2p(s) => WebError::Internal(format!("p2p: {s}")),
            AlephError::Ipfs(s) => WebError::Internal(format!("ipfs: {s}")),
            AlephError::Chain(s) => WebError::Internal(format!("chain: {s}")),
            AlephError::Db(e) => WebError::Internal(format!("db: {e}")),
            AlephError::Pool(s) => WebError::Internal(format!("pool: {s}")),
            AlephError::Io(e) => WebError::Internal(format!("io: {e}")),
            AlephError::Config(s) => WebError::Internal(format!("config: {s}")),
            AlephError::Migrate(s) => WebError::Internal(format!("migrate: {s}")),
            AlephError::Rejected { code, reason } => {
                WebError::Unprocessable(format!("rejected ({code}): {reason}"))
            }
            AlephError::Internal(e) => WebError::Internal(e.to_string()),
        }
    }
}

impl From<PoolError> for WebError {
    fn from(value: PoolError) -> Self {
        WebError::Internal(format!("pool: {value}"))
    }
}

impl From<tokio_postgres::Error> for WebError {
    fn from(value: tokio_postgres::Error) -> Self {
        WebError::Internal(format!("db: {value}"))
    }
}

impl From<serde_json::Error> for WebError {
    fn from(value: serde_json::Error) -> Self {
        WebError::Unprocessable(value.to_string())
    }
}

/// Convenience: build a 422 body that mirrors Pydantic's `e.json()` shape.
pub fn validation_error(msg: impl Into<String>) -> WebError {
    WebError::Unprocessable(msg.into())
}

pub type WebResult<T> = Result<T, WebError>;
