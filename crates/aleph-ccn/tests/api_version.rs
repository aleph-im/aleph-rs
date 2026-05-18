//! Ports `tests/api/test_version.py`.
//!
//! The Python test simply hits `/api/v0/version` and asserts the response
//! payload's `version` field matches the package version. The Rust router
//! exposes the same endpoint under `aleph_ccn::web::build_router`.

mod common;

use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn version_endpoint_returns_pkg_version() {
    let state = common::dummy_state();
    let app = common::build_app(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/version")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 2048).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["version"], aleph_ccn::VERSION);
}

#[tokio::test]
async fn root_endpoint_returns_marker_text() {
    let state = common::dummy_state();
    let app = common::build_app(state);
    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 64).await.unwrap();
    assert_eq!(std::str::from_utf8(&bytes).unwrap(), "aleph-ccn");
}

#[tokio::test]
async fn missing_route_returns_404() {
    let state = common::dummy_state();
    let app = common::build_app(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/this-does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
