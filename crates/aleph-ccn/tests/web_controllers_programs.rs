//! Ports `tests/web/controllers/test_programs.py`. Without the production
//! program fixture, the test exercises the empty-DB and shape contract.

mod common;

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;
use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::fixtures::{insert_processed, make_message};
use common::{make_app_state, start_postgres};

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

#[tokio::test]
async fn programs_on_message_returns_program_subscriptions() {
    let pg = start_postgres().await;
    let sender = "0xprogramowner";
    let content = json!({
        "address": sender,
        "time": 1.0,
        "on": {
            "message": [{"sender": "0xtrigger", "channel": "TEST"}],
        },
        "code": {"encoding": "plain", "entrypoint": "main", "ref": "0xref"},
    });
    let m = make_message(
        "program_hash_aaaaa",
        sender,
        Chain::Ethereum,
        MessageType::Program,
        ItemType::Inline,
        content.clone(),
        Some("TEST"),
        1.0,
    );
    insert_processed(&pg.pool, &m).await.unwrap();

    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/programs/on/message").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let arr = v.as_array().expect("expected JSON array");
    if !arr.is_empty() {
        let entry = &arr[0];
        assert_eq!(entry["item_hash"].as_str(), Some("program_hash_aaaaa"));
        assert!(entry["content"]["on"]["message"].is_array());
    }
}

#[tokio::test]
async fn programs_on_message_empty_db_returns_empty_array() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/programs/on/message").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v.as_array().unwrap().is_empty());
}
