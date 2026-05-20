//! Ports `tests/api/test_ipfs.py`. The Python suite mocks the IPFS service via
//! `mocker.AsyncMock`. The Rust integration tests run without a live IPFS
//! daemon, so most endpoints return 403 ("IPFS is disabled"). That is the
//! contractually-correct behavior for an unconfigured node and is exercised
//! here. Endpoints that need an actively-pinning IPFS service (multipart upload,
//! pubsub) are skipped — those paths are covered by the unit tests around
//! `services::ipfs` in the production crate.

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use cid::Cid;
use http::{Request, StatusCode};
use multihash_codetable::{Code, MultihashDigest};
use serde_json::{Value, json};
use tower::ServiceExt;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

use aleph_ccn::config::IpfsSettings;
use aleph_ccn::db::accessors::files::get_file;
use aleph_ccn::services::ipfs::IpfsService;

use common::{make_app_state, start_postgres};

const SIGNED_STORE_MESSAGE_JSON: &str = r#"{
  "chain": "ETH",
  "channel": "TEST",
  "sender": "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106",
  "type": "STORE",
  "time": 1652794362.573859,
  "item_type": "inline",
  "item_content": "{\"address\":\"0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106\",\"time\":1652794362.5736332,\"item_type\":\"storage\",\"item_hash\":\"5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21\",\"mime_type\":\"text/plain\"}",
  "item_hash": "f6fc4884e3ec3624bd3f60a3c37abf83a130777086061b1a373e659f2bab4d06",
  "signature": "0x7b87c29388a7a452353f9cae8718b66158fb5bdc93f032964226745ee04919092550791b93f79e5ee1981f2d9d6e5ac0cae0d28b68bb63fe0fcbd79015a6f3ea1b"
}"#;

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

async fn post_raw_json(app: axum::Router, uri: &str, body: &str) -> (StatusCode, Vec<u8>) {
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

fn ipfs_enabled_state(pool: aleph_ccn::db::DbPool, server_uri: &str) -> aleph_ccn::web::AppState {
    let mut state = make_app_state(pool);
    let config = Arc::make_mut(&mut state.config);
    config.ipfs.stat_timeout = 1;
    let url = url::Url::parse(server_uri).unwrap();
    let mut settings = IpfsSettings::default();
    settings.host = url.host_str().unwrap().to_string();
    settings.port = url.port().unwrap();
    settings.scheme = url.scheme().to_string();
    state.ipfs_service = Some(Arc::new(IpfsService::new(&settings).unwrap()));
    state
}

async fn file_and_grace_pin_count(pool: &aleph_ccn::db::DbPool, hash: &str) -> (i64, i64) {
    let client = pool.get().await.unwrap();
    let file_size = client
        .query_one("SELECT size FROM files WHERE hash = $1", &[&hash])
        .await
        .unwrap()
        .get::<_, i64>("size");
    let pin_count = client
        .query_one(
            "SELECT COUNT(*)::BIGINT AS count FROM file_pins WHERE file_hash = $1 AND type = 'grace_period'",
            &[&hash],
        )
        .await
        .unwrap()
        .get::<_, i64>("count");
    (file_size, pin_count)
}

fn car_varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return out;
        }
    }
}

fn build_carv1(root: Cid, version: u8, roots_count: usize) -> Vec<u8> {
    let cid_bytes = root.to_bytes();
    let mut header = Vec::new();
    header.push(0xa2);
    header.extend_from_slice(b"\x65roots");
    header.push(0x80 | (roots_count as u8));
    for _ in 0..roots_count {
        header.push(0xd8);
        header.push(0x2a);
        header.push(0x58);
        header.push((cid_bytes.len() + 1) as u8);
        header.push(0x00);
        header.extend_from_slice(&cid_bytes);
    }
    header.extend_from_slice(b"\x67version");
    header.push(version);

    let mut car = car_varint(header.len() as u64);
    car.extend_from_slice(&header);
    car.extend_from_slice(b"blocks-not-read");
    car
}

fn sample_car(version: u8, roots_count: usize) -> Vec<u8> {
    let cid = Cid::new_v1(0x55, Code::Sha2_256.digest(b"root"));
    build_carv1(cid, version, roots_count)
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
async fn ipfs_add_json_stores_canonical_bytes_locally() {
    let pg = start_postgres().await;
    let server = wiremock::MockServer::start().await;
    let cid = "QmJsonaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("cid-version", "0"))
        .and(query_param("pin", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Name\":\"\",\"Hash\":\"{cid}\",\"Size\":\"1\"}}\n"
        )))
        .expect(1)
        .mount(&server)
        .await;

    let state = ipfs_enabled_state(pg.pool.clone(), &server.uri());
    let storage = state.storage_engine.clone().expect("storage engine");
    let app = aleph_ccn::web::build_router(state);
    let raw_body = "{\n  \"z\": 1,\n  \"a\": [true, false]\n}";
    let parsed: Value = serde_json::from_str(raw_body).unwrap();
    let canonical = serde_json::to_vec(&parsed).unwrap();

    let (status, body) = post_raw_json(app, "/api/v0/ipfs/add_json", raw_body).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        storage.read(cid).await.unwrap().as_deref(),
        Some(canonical.as_slice())
    );
    let client = pg.pool.get().await.unwrap();
    let file = get_file(&**client, cid).await.unwrap().expect("file row");
    assert_eq!(file.size, canonical.len() as i64);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_file_stat_timeout_returns_504_and_grace_pins() {
    let pg = start_postgres().await;
    let server = wiremock::MockServer::start().await;
    let cid = "QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("cid-version", "0"))
        .and(query_param("pin", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Name\":\"file\",\"Hash\":\"{cid}\",\"Size\":\"11\"}}\n"
        )))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{cid}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(2))
                .set_body_json(json!({"Type": "file", "Size": 11})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let app = aleph_ccn::web::build_router(ipfs_enabled_state(pg.pool.clone(), &server.uri()));
    let boundary = "----alephTest";
    let body = build_multipart(boundary, &[("file", "f.txt", b"hello world")]);

    let (status, body) = post_multipart(app, "/api/v0/ipfs/add_file", boundary, body).await;

    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("Timeout"), "{body}");
    assert_eq!(file_and_grace_pin_count(&pg.pool, cid).await, (11, 1));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_file_rejects_storage_metadata_before_kubo_add() {
    let pg = start_postgres().await;
    let server = wiremock::MockServer::start().await;
    let app = aleph_ccn::web::build_router(ipfs_enabled_state(pg.pool.clone(), &server.uri()));
    let boundary = "----alephTest";
    let metadata = json!({
        "sync": false,
        "message": serde_json::from_str::<Value>(SIGNED_STORE_MESSAGE_JSON).unwrap(),
    })
    .to_string();
    let body = build_multipart(
        boundary,
        &[
            ("file", "f.txt", b"hello world"),
            ("metadata", "", metadata.as_bytes()),
        ],
    );

    let (status, body) = post_multipart(app, "/api/v0/ipfs/add_file", boundary, body).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("Unsupported STORE item type"), "{body}");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_car_disabled_returns_403() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let boundary = "----alephTest";
    let body = build_multipart(boundary, &[("file", "archive.car", b"not-a-car")]);
    let (status, _) = post_multipart(app, "/api/v0/ipfs/add_car", boundary, body).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_car_missing_metadata_returns_422_before_kubo() {
    let pg = start_postgres().await;
    let server = wiremock::MockServer::start().await;
    let app = aleph_ccn::web::build_router(ipfs_enabled_state(pg.pool.clone(), &server.uri()));
    let boundary = "----alephTest";
    let car = sample_car(1, 1);
    let body = build_multipart(boundary, &[("file", "archive.car", &car)]);

    let (status, body) = post_multipart(app, "/api/v0/ipfs/add_car", boundary, body).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("metadata is required"), "{body}");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_car_missing_file_returns_422_before_kubo() {
    let pg = start_postgres().await;
    let server = wiremock::MockServer::start().await;
    let app = aleph_ccn::web::build_router(ipfs_enabled_state(pg.pool.clone(), &server.uri()));
    let boundary = "----alephTest";
    let metadata = json!({"message": {}, "sync": false}).to_string();
    let body = build_multipart(boundary, &[("metadata", "", metadata.as_bytes())]);

    let (status, body) = post_multipart(app, "/api/v0/ipfs/add_car", boundary, body).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("Missing 'file'"), "{body}");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_car_rejects_unsupported_car_version_before_kubo() {
    let pg = start_postgres().await;
    let server = wiremock::MockServer::start().await;
    let app = aleph_ccn::web::build_router(ipfs_enabled_state(pg.pool.clone(), &server.uri()));
    let boundary = "----alephTest";
    let car = sample_car(2, 1);
    let metadata = json!({"message": {}, "sync": false}).to_string();
    let body = build_multipart(
        boundary,
        &[
            ("file", "archive.car", &car),
            ("metadata", "", metadata.as_bytes()),
        ],
    );

    let (status, body) = post_multipart(app, "/api/v0/ipfs/add_car", boundary, body).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("unsupported CAR version"), "{body}");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn ipfs_add_car_rejects_multiple_roots_before_kubo() {
    let pg = start_postgres().await;
    let server = wiremock::MockServer::start().await;
    let app = aleph_ccn::web::build_router(ipfs_enabled_state(pg.pool.clone(), &server.uri()));
    let boundary = "----alephTest";
    let car = sample_car(1, 2);
    let metadata = json!({"message": {}, "sync": false}).to_string();
    let body = build_multipart(
        boundary,
        &[
            ("file", "archive.car", &car),
            ("metadata", "", metadata.as_bytes()),
        ],
    );

    let (status, body) = post_multipart(app, "/api/v0/ipfs/add_car", boundary, body).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("expected exactly 1 root"), "{body}");
    assert!(server.received_requests().await.unwrap().is_empty());
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
