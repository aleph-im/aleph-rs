//! Integration test for the `X-Auth-Token` middleware on the
//! `/api/v0/price/(re|{item_hash}/re)calculate` endpoints.
//!
//! Mirrors the python-side decorator behaviour:
//!  * No header  -> 401, body "Missing X-Auth-Token header".
//!  * Bad token  -> 401, body "Invalid or expired authentication token".
//!  * Expired    -> 401 (same body).
//!  * Valid token -> reaches the handler.
//!
//! We can't drive the real recalculate handlers without a live Postgres
//! pool (the `dummy_state` pool blocks forever on `get()`), so for the
//! "valid token" assertions we attach the same middleware to a no-op
//! handler under `/__test/echo`. This exercises the production
//! `require_auth_token` middleware against the production `AppState`
//! shape — the recalculate routes themselves are covered by the 401
//! assertions, which short-circuit before touching the pool.

mod common;

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use axum::{Router, http::StatusCode as ASC};
use http::{Request, StatusCode};
use tower::ServiceExt;

use aleph_ccn::config::Settings;
use aleph_ccn::toolkit::ecdsa::{create_auth_token, create_auth_token_at, generate_key_pair};
use aleph_ccn::web::AppState;
use aleph_ccn::web::controllers::auth::require_auth_token;

fn state_with_keypair() -> (AppState, String) {
    let (priv_hex, pub_hex) = generate_key_pair();
    let base = common::dummy_state();
    let mut settings: Settings = (*base.config).clone();
    settings.aleph.auth.public_key = pub_hex;
    settings.aleph.auth.max_token_age = 300;
    let state = AppState {
        config: Arc::new(settings),
        ..base
    };
    (state, priv_hex)
}

/// Trivial handler that runs *after* the auth middleware. Returns 200/ok
/// to prove the middleware admitted the request.
async fn echo_handler() -> &'static str {
    "ok"
}

/// Build a router with the production middleware layered on top of two
/// fake routes that mirror the protected prices paths. The middleware
/// itself, `require_auth_token`, is the same code that protects
/// `/api/v0/price/recalculate` in the real router.
fn test_router(state: AppState) -> Router {
    Router::new()
        .route("/api/v0/price/recalculate", post(echo_handler))
        .route(
            "/api/v0/price/{item_hash}/recalculate",
            post(echo_handler),
        )
        .route_layer(from_fn_with_state(state.clone(), require_auth_token))
        .route("/api/v0/costs", post(echo_handler)) // unprotected
        .with_state(state)
}

async fn body_text(resp: axum::http::Response<Body>) -> (StatusCode, String) {
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 4096).await.unwrap_or_default();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn recalculate_without_auth_header_returns_401() {
    let (state, _priv) = state_with_keypair();
    let app = test_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/price/recalculate")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_text(response).await;
    assert_eq!(status, ASC::UNAUTHORIZED);
    assert_eq!(body, "Missing X-Auth-Token header");
}

#[tokio::test]
async fn recalculate_with_invalid_token_returns_401() {
    let (state, _priv) = state_with_keypair();
    let app = test_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/price/recalculate")
                .header("X-Auth-Token", "not-a-real-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_text(response).await;
    assert_eq!(status, ASC::UNAUTHORIZED);
    assert_eq!(body, "Invalid or expired authentication token");
}

#[tokio::test]
async fn recalculate_with_token_signed_by_wrong_key_returns_401() {
    let (state, _priv) = state_with_keypair();
    let app = test_router(state);
    let (other_priv, _other_pub) = generate_key_pair();
    let token = create_auth_token(&other_priv).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/price/recalculate")
                .header("X-Auth-Token", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_text(response).await;
    assert_eq!(status, ASC::UNAUTHORIZED);
    assert_eq!(body, "Invalid or expired authentication token");
}

#[tokio::test]
async fn recalculate_with_expired_token_returns_401() {
    let (state, priv_hex) = state_with_keypair();
    let app = test_router(state);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let token = create_auth_token_at(&priv_hex, now - 600).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/price/recalculate")
                .header("X-Auth-Token", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_text(response).await;
    assert_eq!(status, ASC::UNAUTHORIZED);
    assert_eq!(body, "Invalid or expired authentication token");
}

#[tokio::test]
async fn recalculate_with_valid_token_reaches_handler() {
    let (state, priv_hex) = state_with_keypair();
    let app = test_router(state);
    let token = create_auth_token(&priv_hex).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/price/recalculate")
                .header("X-Auth-Token", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_text(response).await;
    assert_eq!(status, ASC::OK);
    assert_eq!(body, "ok");
}

#[tokio::test]
async fn recalculate_with_hash_without_auth_header_returns_401() {
    let (state, _priv) = state_with_keypair();
    let app = test_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/price/0123456789abcdef/recalculate")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_text(response).await;
    assert_eq!(status, ASC::UNAUTHORIZED);
    assert_eq!(body, "Missing X-Auth-Token header");
}

#[tokio::test]
async fn recalculate_with_hash_valid_token_reaches_handler() {
    let (state, priv_hex) = state_with_keypair();
    let app = test_router(state);
    let token = create_auth_token(&priv_hex).unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/price/0123456789abcdef/recalculate")
                .header("X-Auth-Token", token)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_text(response).await;
    assert_eq!(status, ASC::OK);
    assert_eq!(body, "ok");
}

#[tokio::test]
async fn public_endpoint_skips_auth_middleware() {
    // `/api/v0/costs` is registered *after* `route_layer`, so the middleware
    // does not apply. Hitting it without a token should succeed.
    let (state, _priv) = state_with_keypair();
    let app = test_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/costs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_text(response).await;
    assert_eq!(status, ASC::OK);
    assert_eq!(body, "ok");
}

#[tokio::test]
async fn production_router_blocks_recalculate_without_token() {
    // Sanity-check: the *real* production `build_router` also rejects
    // unauthenticated requests at the auth middleware (no DB needed since
    // the 401 short-circuits before the handler).
    let (state, _priv) = state_with_keypair();
    let app = aleph_ccn::web::build_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/price/recalculate")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = body_text(response).await;
    assert_eq!(status, ASC::UNAUTHORIZED);
    assert_eq!(body, "Missing X-Auth-Token header");
}
