//! Ports `tests/api/test_ipfs.py`. The Python suite mocks the IPFS service via
//! `mocker.AsyncMock`. The Rust integration tests run without a live IPFS
//! daemon, so most endpoints return 403 ("IPFS is disabled"). That is the
//! contractually-correct behavior for an unconfigured node and is exercised
//! here. Endpoints that need an actively-pinning IPFS service (multipart upload,
//! pubsub) are skipped — those paths are covered by the unit tests around
//! `services::ipfs` in the production crate.

mod common;

use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{make_app_state, start_postgres};

async fn post_multipart(
    app: axum::Router,
    uri: &str,
    boundary: &str,
    body: Vec<u8>,
) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(body))
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

async fn post_json(app: axum::Router, uri: &str, body: Value) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

fn build_multipart(boundary: &str, parts: &[(&str, &str, &[u8])]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    for (name, filename, data) in parts {
        buf.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        if filename.is_empty() {
            buf.extend_from_slice(
                format!("content-disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
            );
        } else {
            buf.extend_from_slice(
                format!(
                    "content-disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\n\r\n"
                )
                .as_bytes(),
            );
        }
        buf.extend_from_slice(data);
        buf.extend_from_slice(b"\r\n");
    }
    buf.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    buf
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_file_disabled_returns_403() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let boundary = "----alephTest";
    let body = build_multipart(boundary, &[("file", "f.txt", b"hi there")]);
    let (status, _) = post_multipart(app, "/api/v0/ipfs/add_file", boundary, body).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_json_disabled_returns_403() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = post_json(app, "/api/v0/ipfs/add_json", json!({"name": "test"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_pubsub_pub_invalid_topic_returns_403() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = post_json(
        app,
        "/api/v0/ipfs/pubsub/pub",
        json!({"topic": "wrong-topic", "data": "{}"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_pubsub_pub_missing_topic_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = post_json(app, "/api/v0/ipfs/pubsub/pub", json!({"data": "{}"})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_pubsub_pub_data_not_string_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    // Use the canonical queue topic from default settings: "ALEPH-TEST" per Settings::default
    let topic = aleph_ccn::config::Settings::default().aleph.queue_topic;
    let (status, _) = post_json(
        app,
        "/api/v0/ipfs/pubsub/pub",
        json!({"topic": topic, "data": {"obj": 1}}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_pubsub_pub_invalid_json_data_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let topic = aleph_ccn::config::Settings::default().aleph.queue_topic;
    let (status, _) = post_json(
        app,
        "/api/v0/ipfs/pubsub/pub",
        json!({"topic": topic, "data": "not-valid-json{"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_file_missing_file_part_disabled_returns_403() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let boundary = "----alephTest";
    let body = build_multipart(boundary, &[("metadata", "", b"{}")]);
    // Even with no `file`, the controller short-circuits on `Forbidden` since
    // ipfs_service is None.
    let (status, _) = post_multipart(app, "/api/v0/ipfs/add_file", boundary, body).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_get_hash_unknown_returns_4xx() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/storage/QmDoesNotExist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // 404 if the hash format is recognised, 400 if it's malformed.
    assert!(response.status().is_client_error());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_count_unknown_hash_is_zero() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/storage/count/QmDoesNotExist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    // Endpoint returns the raw count as a JSON number.
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v.as_i64(), Some(0));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_metadata_unknown_returns_404() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/storage/metadata/QmDoesNotExist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_pubsub_pub_validates_pending_message_shape() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let topic = aleph_ccn::config::Settings::default().aleph.queue_topic;
    // Missing item_content -> validation error
    let bad_msg = json!({
        "chain": "ETH",
        "sender": "0x123",
        "type": "STORE",
        "channel": "TEST",
        "signature": "0x00",
        "time": 1.0,
        "item_type": "inline",
        "item_hash": "0".repeat(64),
    });
    let (status, _) = post_json(
        app,
        "/api/v0/ipfs/pubsub/pub",
        json!({"topic": topic, "data": bad_msg.to_string()}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}
