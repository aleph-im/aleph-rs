//! Ports `tests/api/test_p2p.py`.

mod common;

use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{make_app_state, start_postgres};

const P2P_PUB_URI: &str = "/api/v0/p2p/pubsub/pub";

const MESSAGE_DICT_JSON: &str = r#"{
  "chain": "NULS2",
  "item_hash": "4bbcfe7c4775492c2e602d322d68f558891468927b5e0d6cb89ff880134f323e",
  "sender": "NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB",
  "type": "STORE",
  "channel": "MYALEPH",
  "item_content": "{\"address\":\"NULSd6Hgbhr42Dm5nEgf6foEUT5bgwHesZQJB\",\"item_type\":\"ipfs\",\"item_hash\":\"QmUDS8mpQmpPyptyUEedHxHMkxo7ueRRiAvrpgvJMpjXwW\",\"time\":1577325086.513}",
  "item_type": "inline",
  "signature": "G7/xlWoMjjOr1NBN4SiZ8USYYVM9Q3JHXChR9hPw9/YSItfAplshWysqYDkvmBZiwbICG0IVB3ilMPJ/ZVgPNlk=",
  "time": 1608297193.717
}"#;

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

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn pubsub_pub_valid_message_succeeds() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let topic = aleph_ccn::config::Settings::default().aleph.queue_topic;
    let (status, body) = post_json(
        app,
        P2P_PUB_URI,
        json!({"topic": topic, "data": MESSAGE_DICT_JSON}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"].as_str(), Some("success"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn pubsub_pub_errors() {
    let pg = start_postgres().await;
    let topic = aleph_ccn::config::Settings::default().aleph.queue_topic;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    // Invalid topic
    let (status, _) = post_json(
        app.clone(),
        P2P_PUB_URI,
        json!({"topic": "random-topic", "data": MESSAGE_DICT_JSON}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // data is an object (not a stringified JSON)
    let (status, _) = post_json(
        app.clone(),
        P2P_PUB_URI,
        json!({"topic": topic, "data": {"obj": 1}}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // truncated JSON
    let truncated = &MESSAGE_DICT_JSON[..MESSAGE_DICT_JSON.len() - 2];
    let (status, _) = post_json(
        app.clone(),
        P2P_PUB_URI,
        json!({"topic": topic, "data": truncated}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Missing item_content
    let mut v: Value = serde_json::from_str(MESSAGE_DICT_JSON).unwrap();
    if let Some(obj) = v.as_object_mut() {
        obj.remove("item_content");
    }
    let (status, _) = post_json(
        app,
        P2P_PUB_URI,
        json!({"topic": topic, "data": v.to_string()}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn post_message_pending_returns_202() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let message: Value = serde_json::from_str(MESSAGE_DICT_JSON).unwrap();
    let (status, body) = post_json(
        app,
        "/api/v0/messages",
        json!({"sync": false, "message": message}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["message_status"].as_str(), Some("pending"));
    assert_eq!(v["publication_status"]["status"].as_str(), Some("success"));
}
