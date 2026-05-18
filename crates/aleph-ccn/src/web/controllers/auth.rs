//! Mirrors the `require_auth_token` decorator in `aleph/toolkit/ecdsa.py`
//! (used through `@require_auth_token` on the price-recalculation routes).
//!
//! The decorator reads the `X-Auth-Token` header, then calls
//! `verify_auth_token(token, public_key, max_token_age)`. Missing header ->
//! 401 with body `"Missing X-Auth-Token header"`. Invalid / expired token ->
//! 401 with body `"Invalid or expired authentication token"`. Both responses
//! carry `WWW-Authenticate: Bearer`, matching the aiohttp behaviour.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::toolkit::ecdsa::verify_auth_token;
use crate::web::AppState;

/// HTTP header name carrying the auth token. Matches pyaleph.
pub const AUTH_HEADER: &str = "X-Auth-Token";

/// Build a 401 response with the pyaleph body text and a `WWW-Authenticate`
/// challenge header.
fn unauthorized(body: &'static str) -> Response {
    let mut resp = (StatusCode::UNAUTHORIZED, body).into_response();
    resp.headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    resp
}

/// Axum middleware: enforce a valid `X-Auth-Token` header. Use with
/// `axum::middleware::from_fn_with_state(state, require_auth_token)`.
pub async fn require_auth_token(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(value) = request.headers().get(AUTH_HEADER) else {
        return unauthorized("Missing X-Auth-Token header");
    };
    let Ok(token) = value.to_str() else {
        return unauthorized("Invalid or expired authentication token");
    };
    let public_key = &state.config.aleph.auth.public_key;
    let max_age = state.config.aleph.auth.max_token_age;
    if !verify_auth_token(public_key, token, max_age) {
        return unauthorized("Invalid or expired authentication token");
    }
    next.run(request).await
}
