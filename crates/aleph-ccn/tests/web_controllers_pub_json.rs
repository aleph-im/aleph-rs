//! Ports `tests/web/controllers/test_pub_json.py`. The /api/v0/ipfs/pubsub/pub
//! endpoint shares its handler with /api/v0/p2p/pubsub/pub. The tests here
//! cover valid + invalid Aleph-message payloads.

mod common;

use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{make_app_state, start_postgres};

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

const VALID_STORE_MSG: &str = r#"{
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

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn pub_valid_aleph_message_returns_success() {
    let pg = start_postgres().await;
    let topic = aleph_ccn::config::Settings::default().aleph.queue_topic;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = post_json(
        app,
        "/api/v0/ipfs/pubsub/pub",
        json!({"topic": topic, "data": VALID_STORE_MSG}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"].as_str(), Some("success"));
    assert!(v["failed"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn pub_invalid_aleph_message_returns_422() {
    let pg = start_postgres().await;
    let topic = aleph_ccn::config::Settings::default().aleph.queue_topic;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = post_json(
        app,
        "/api/v0/ipfs/pubsub/pub",
        json!({
            "topic": topic,
            "data": serde_json::to_string(&json!({"header": "garbage", "type": "STORE"})).unwrap(),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}
