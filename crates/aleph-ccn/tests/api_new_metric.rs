//! Ports `tests/api/test_new_metric.py`. Without the metric fixture data (a
//! large JSON file), we exercise the validation + 404 paths of the production
//! handler.

mod common;

use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use tower::ServiceExt;

use common::{make_app_state, start_postgres};

async fn get(app: axum::Router, uri: &str) -> StatusCode {
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let _ = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    status
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ccn_metric_unknown_returns_404() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let status = get(app, "/api/v0/core/this_is_not_a_node/metrics").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn crn_metric_unknown_returns_404() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let status = get(app, "/api/v0/compute/this_is_not_a_node/metrics").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ccn_metric_with_end_date_unknown_404() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let status = get(
        app,
        "/api/v0/core/unknown_node/metrics?end_date=1701261023",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ccn_metric_with_start_date_unknown_404() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let status = get(
        app,
        "/api/v0/core/unknown_node/metrics?start_date=1701261023",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ccn_metric_sort_param_accepted() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let status = get(app, "/api/v0/core/unknown/metrics?sort=DESC").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn crn_metric_sort_param_accepted() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let status = get(app, "/api/v0/compute/unknown/metrics?sort=ASC").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn metrics_json_endpoint_returns_200() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let status = get(app, "/metrics.json").await;
    assert_eq!(status, StatusCode::OK);
}
