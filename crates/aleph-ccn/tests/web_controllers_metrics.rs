//! Ports `tests/web/controllers/test_metrics.py`. The original suite is a pure
//! unit test of `format_dict_for_prometheus` / `format_dataclass_for_prometheus`.
//! The Rust port verifies the HTTP-level `/metrics`, `/metrics.json`, and `/`
//! endpoints (the production analogues of those helpers).

mod common;

use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use tower::ServiceExt;

use common::{make_app_state, start_postgres};

async fn get_full(app: axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

#[tokio::test]
async fn metrics_text_format_returns_prometheus_text() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get_full(app, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("pyaleph_") || text.contains("aleph_"));
}

#[tokio::test]
async fn metrics_json_returns_json_object() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get_full(app, "/metrics.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v.is_object());
}

#[tokio::test]
async fn index_endpoint_returns_200() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get_full(app, "/").await;
    assert_eq!(status, StatusCode::OK);
}
