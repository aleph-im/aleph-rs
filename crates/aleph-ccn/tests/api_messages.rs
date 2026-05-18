//! Ports `tests/api/test_get_message.py` and `tests/api/test_list_messages.py`.
//!
//! These tests require a live Postgres + migrations applied; we spin up a
//! per-test container via `testcontainers`. They're marked `#[ignore]` so the
//! default `cargo test` run stays fast and hermetic.

mod common;

use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

use aleph_ccn::db::accessors::messages::upsert_rejected_message;
use aleph_ccn::toolkit::timestamp::utc_now;
use aleph_ccn::types::message_status::{ErrorCode, MessageStatus};

use common::fixtures::{fixture_messages, fixture_messages_with_status};
use common::{
    build_messages_app, insert_processed_message, make_app_state, start_postgres,
};

/// Hit a GET endpoint on the app router and return (status, body bytes).
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
#[ignore = "requires docker; run with --ignored"]
async fn list_messages_returns_all_fixture_rows() {
    let pg = start_postgres().await;
    for m in fixture_messages() {
        insert_processed_message(&pg.pool, m).await.unwrap();
    }

    let app = build_messages_app(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/messages.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let messages = v["messages"].as_array().unwrap();
    assert_eq!(messages.len(), fixture_messages().len());
    assert_eq!(v["pagination_total"].as_i64(), Some(messages.len() as i64));
    assert_eq!(v["pagination_page"].as_i64(), Some(1));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn list_messages_filter_by_channel() {
    let pg = start_postgres().await;
    for m in fixture_messages() {
        insert_processed_message(&pg.pool, m).await.unwrap();
    }
    let app = build_messages_app(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/messages.json?channels=unit-tests").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let messages = v["messages"].as_array().unwrap();
    assert!(!messages.is_empty());
    for m in messages {
        assert_eq!(m["channel"].as_str(), Some("unit-tests"));
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn list_messages_filter_by_chain() {
    let pg = start_postgres().await;
    for m in fixture_messages() {
        insert_processed_message(&pg.pool, m).await.unwrap();
    }
    let app = build_messages_app(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/messages.json?chains=ETH").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let messages = v["messages"].as_array().unwrap();
    assert_eq!(messages.len(), fixture_messages().len());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn list_messages_filter_by_hashes() {
    let pg = start_postgres().await;
    let inserted = fixture_messages();
    for m in inserted.clone() {
        insert_processed_message(&pg.pool, m).await.unwrap();
    }
    let target_hashes = [&inserted[0].item_hash, &inserted[1].item_hash];
    let q = format!(
        "/api/v0/messages.json?hashes={},{}",
        target_hashes[0], target_hashes[1]
    );
    let app = build_messages_app(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, &q).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["messages"].as_array().unwrap().len(), 2);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn list_messages_pagination_validates_page() {
    let pg = start_postgres().await;
    let app = build_messages_app(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/messages.json?page=0").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn list_messages_filter_unknown_channel_is_empty() {
    let pg = start_postgres().await;
    for m in fixture_messages() {
        insert_processed_message(&pg.pool, m).await.unwrap();
    }
    let app = build_messages_app(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/messages.json?channels=none-pizza-with-left-beef").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["messages"].as_array().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Per-message lookup (mirrors test_get_message.py)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_processed_message_status() {
    let pg = start_postgres().await;
    let f = fixture_messages_with_status();
    for m in f.processed.clone() {
        insert_processed_message(&pg.pool, m).await.unwrap();
    }
    let app = build_messages_app(make_app_state(pg.pool.clone()));
    for processed in f.processed {
        let uri = format!("/api/v0/messages/{}", processed.item_hash);
        let (status, body) = get(app.clone(), &uri).await;
        assert_eq!(status, StatusCode::OK);
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"].as_str(), Some("processed"));
        assert_eq!(v["item_hash"].as_str(), Some(processed.item_hash.as_str()));
        assert_eq!(
            v["message"]["sender"].as_str(),
            Some(processed.sender.as_str())
        );
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_unknown_message_returns_404() {
    let pg = start_postgres().await;
    let app = build_messages_app(make_app_state(pg.pool.clone()));
    let (status, _) = get(
        app,
        "/api/v0/messages/0000000000000000000000000000000000000000000000000000000000000000",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_rejected_message_status() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let f = fixture_messages_with_status();
    let rj = &f.rejected[0];
    // Insert via the actual accessor — exercises real SQL.
    upsert_rejected_message(
        &**client,
        &rj.item_hash,
        &rj.message,
        ErrorCode::ForgetNoTarget.as_i32(),
        rj.details.as_ref(),
        rj.traceback.as_deref(),
        rj.tx_hash.as_deref(),
    )
    .await
    .unwrap();
    aleph_ccn::db::accessors::messages::upsert_message_status(
        &**client,
        &rj.item_hash,
        MessageStatus::Rejected,
        utc_now(),
        None,
    )
    .await
    .unwrap();
    drop(client);

    let app = build_messages_app(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/messages/{}", rj.item_hash);
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"].as_str(), Some("rejected"));
    assert_eq!(
        v["error_code"].as_i64(),
        Some(ErrorCode::ForgetNoTarget.as_i32() as i64)
    );
    assert!(v.get("traceback").is_none());
}
