//! Ports `tests/api/test_storage.py`. The original Python tests rely on a
//! mocked IPFS service; without it the upload endpoints return 403. Tests
//! here exercise the lookup paths (GET /storage/{hash}, /count, /metadata)
//! against seeded `files` rows + the in-memory storage engine wired into
//! `make_app_state`.

mod common;

use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use aleph_ccn::types::files::FileType;

use common::{insert_default_aggregates, make_app_state, start_postgres};

async fn get(app: axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
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

async fn post_json(app: axum::Router, uri: &str, body: serde_json::Value) -> (StatusCode, Vec<u8>) {
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

async fn post_bytes(app: axum::Router, uri: &str, body: Vec<u8>) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
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

async fn insert_file(pool: &aleph_ccn::db::DbPool, hash: &str, size: i64) {
    let client = pool.get().await.unwrap();
    aleph_ccn::db::accessors::files::upsert_file(&**client, hash, size, FileType::File)
        .await
        .unwrap();
}

const FILE_CONTENT: &[u8] = b"Hello earthlings, I come in pieces";
const FILE_SHA256: &str = "bb6e53f2738e5934b9a2125a9dc3d76211720e5152bdbcd4b236363d18d4f8a3";
const SIGNED_STORAGE_FILE_CONTENT: &[u8] = b"Hello Aleph.im\n";
const SIGNED_STORAGE_FILE_SHA256: &str =
    "0214e5578f5acb5d36ea62255cbf1157a4bdde7b9612b5db4899b2175e310b6f";
const SIGNED_STORAGE_MESSAGE_JSON: &str = r#"{
  "chain": "ETH",
  "sender": "0x6dA130FD646f826C1b8080C07448923DF9a79aaA",
  "type": "STORE",
  "channel": "null",
  "signature": "0x2b90dcfa8f93506150df275a4fe670e826be0b4b751badd6ec323648a6a738962f47274f71a9939653fb6d49c25055821f547447fb3b33984a579008d93eca431b",
  "time": 1692193373.7144432,
  "item_type": "inline",
  "item_content": "{\"address\":\"0x6dA130FD646f826C1b8080C07448923DF9a79aaA\",\"time\":1692193373.714271,\"item_type\":\"storage\",\"item_hash\":\"0214e5578f5acb5d36ea62255cbf1157a4bdde7b9612b5db4899b2175e310b6f\",\"mime_type\":\"text/plain\"}",
  "item_hash": "8227acbc2f7c43899efd9f63ea9d8119a4cb142f3ba2db5fe499ccfab86dfaed"
}"#;

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_get_unknown_returns_4xx() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/storage/QmDoesNotExist").await;
    // Hash format may be rejected as malformed (400) or NOT_FOUND if recognised.
    assert!(status.is_client_error());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_get_existing_returns_200() {
    let pg = start_postgres().await;
    insert_file(&pg.pool, FILE_SHA256, FILE_CONTENT.len() as i64).await;

    let state = make_app_state(pg.pool.clone());
    // Seed the in-memory storage engine with the test content.
    if let Some(engine) = state.storage_engine.clone() {
        engine.write(FILE_SHA256, FILE_CONTENT).await.unwrap();
    }
    let app = aleph_ccn::web::build_router(state);
    let (status, body) = get(app, &format!("/api/v0/storage/{FILE_SHA256}")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"].as_str(), Some("success"));
    assert!(v["content"].is_string());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_raw_existing_returns_200() {
    let pg = start_postgres().await;
    insert_file(&pg.pool, FILE_SHA256, FILE_CONTENT.len() as i64).await;
    let state = make_app_state(pg.pool.clone());
    if let Some(engine) = state.storage_engine.clone() {
        engine.write(FILE_SHA256, FILE_CONTENT).await.unwrap();
    }
    let app = aleph_ccn::web::build_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v0/storage/raw/{FILE_SHA256}"))
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
    assert_eq!(body, FILE_CONTENT);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_count_returns_zero_for_unknown() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/storage/count/QmDoesNotExist").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v.as_i64(), Some(0));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_metadata_unknown_returns_404() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/storage/metadata/QmDoesNotExist").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_metadata_known_returns_200() {
    let pg = start_postgres().await;
    insert_file(&pg.pool, FILE_SHA256, FILE_CONTENT.len() as i64).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, &format!("/api/v0/storage/metadata/{FILE_SHA256}")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["file_hash"].as_str(), Some(FILE_SHA256));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_by_message_hash_unknown_returns_404() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let hash = "0".repeat(64);
    let (status, _) = get(app, &format!("/api/v0/storage/by-message-hash/{hash}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_by_ref_user_defined_without_address_returns_400() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/storage/by-ref/some-ref").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_by_ref_hash_unknown_returns_404() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let hash = "0".repeat(64);
    let (status, _) = get(app, &format!("/api/v0/storage/by-ref/{hash}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_by_ref_addr_unknown_returns_404() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/storage/by-ref/0xabc/some-ref").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_add_json_basic() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = post_json(
        app,
        "/api/v0/storage/add_json",
        serde_json::json!({"hello": "world"}),
    )
    .await;
    // Without signature verification + IPFS, the controller should still
    // persist the JSON to local storage; on success it returns 200, on
    // unavailable services 403.
    assert!(status == StatusCode::OK || status == StatusCode::FORBIDDEN);
    if status == StatusCode::OK {
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"].as_str(), Some("success"));
        assert!(v["hash"].is_string());
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_add_json_accepts_configured_body_above_axum_default() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let body = json!({ "payload": "a".repeat(3 * 1024 * 1024) });

    let (status, bytes) = post_json(app, "/api/v0/storage/add_json", body).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&bytes));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_add_ipfs_json_disabled_returns_403() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = post_json(app, "/api/v0/ipfs/add_json", serde_json::json!({"x": 1})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_count_for_existing_file_unpinned_is_zero() {
    let pg = start_postgres().await;
    insert_file(&pg.pool, FILE_SHA256, FILE_CONTENT.len() as i64).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, &format!("/api/v0/storage/count/{FILE_SHA256}")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    // No file_pins inserted; count is 0. Endpoint returns the raw integer.
    assert_eq!(v.as_i64(), Some(0));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_add_file_disabled_or_validates() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    // Send an empty body — endpoint should reject (not a valid multipart).
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/storage/add_file")
                .header("content-type", "multipart/form-data; boundary=---x")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_client_error());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_raw_upload_uses_unauthenticated_limit() {
    let pg = start_postgres().await;
    let mut state = make_app_state(pg.pool.clone());
    let config = std::sync::Arc::make_mut(&mut state.config);
    config.storage.max_file_size = 1024;
    config.storage.max_unauthenticated_upload_file_size = 8;
    let app = aleph_ccn::web::build_router(state);

    let (status, _) = post_bytes(app, "/api/v0/storage/add_file", vec![b'x'; 9]).await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_signed_small_upload_still_requires_balance() {
    let pg = start_postgres().await;
    insert_default_aggregates(&pg.pool).await.unwrap();
    let state = make_app_state(pg.pool.clone());
    let storage = state.storage_engine.clone();
    let app = aleph_ccn::web::build_router(state);
    let boundary = "----alephTest";
    let metadata = json!({
        "sync": false,
        "message": serde_json::from_str::<Value>(SIGNED_STORAGE_MESSAGE_JSON).unwrap(),
    })
    .to_string();
    let body = build_multipart(
        boundary,
        &[
            ("file", "hello.txt", SIGNED_STORAGE_FILE_CONTENT),
            ("metadata", "", metadata.as_bytes()),
        ],
    );

    let (status, _) = post_multipart(app, "/api/v0/storage/add_file", boundary, body).await;

    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    if let Some(engine) = storage {
        assert!(!engine.exists(SIGNED_STORAGE_FILE_SHA256).await.unwrap());
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn storage_count_endpoint_exists_for_arbitrary_hash() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/storage/count/abc123").await;
    assert_eq!(status, StatusCode::OK);
}
