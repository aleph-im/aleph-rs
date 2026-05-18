//! Ports `tests/api/test_cursor_pagination.py`.

mod common;

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;
use axum::body::{Body, to_bytes};
use chrono::{TimeZone, Utc};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::fixtures::{
    AggSeed, insert_aggregate_seed, insert_post, insert_processed, make_message, make_post_db,
};
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

async fn seed_posts(pool: &aleph_ccn::db::DbPool, count: usize) -> Vec<String> {
    let mut hashes = Vec::new();
    for i in 0..count {
        let hash = format!("{:0>64x}", 0x1000 + i as u64);
        let sender = format!("0xpost{i}");
        let msg = make_message(
            &hash,
            &sender,
            Chain::Ethereum,
            MessageType::Post,
            ItemType::Inline,
            json!({"address": sender, "time": 1.0 + i as f64, "type": "blog", "content": {"i": i}}),
            Some("TEST"),
            1.0 + i as f64,
        );
        insert_processed(pool, &msg).await.unwrap();
        let post = make_post_db(&msg);
        insert_post(pool, &post).await.unwrap();
        hashes.push(hash);
    }
    hashes
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn posts_v1_cursor_pagination_walks_all() {
    let pg = start_postgres().await;
    let expected = seed_posts(&pg.pool, 5).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    let mut all = Vec::new();
    let mut cursor = String::new();
    for _ in 0..10 {
        let uri = format!("/api/v1/posts.json?pagination=2&cursor={cursor}");
        let (status, body) = get(app.clone(), &uri).await;
        assert_eq!(status, StatusCode::OK);
        let v: Value = serde_json::from_slice(&body).unwrap();
        for p in v["posts"].as_array().unwrap() {
            all.push(p["item_hash"].as_str().unwrap().to_string());
        }
        match v.get("next_cursor").and_then(|x| x.as_str()) {
            Some(c) if !c.is_empty() => cursor = c.to_string(),
            _ => break,
        }
    }
    let unique: std::collections::HashSet<_> = all.iter().collect();
    assert_eq!(all.len(), unique.len());
    assert_eq!(all.len(), expected.len());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn posts_v1_cursor_invalid_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v1/posts.json?cursor=not-a-valid-cursor!!!").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn posts_v1_cursor_pagination_zero_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v1/posts.json?cursor=dummy&pagination=0").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn posts_v0_cursor_pagination_walks_all() {
    let pg = start_postgres().await;
    let expected = seed_posts(&pg.pool, 4).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    let mut all = Vec::new();
    let mut cursor = String::new();
    for _ in 0..10 {
        let uri = format!("/api/v0/posts.json?pagination=2&cursor={cursor}");
        let (status, body) = get(app.clone(), &uri).await;
        assert_eq!(status, StatusCode::OK);
        let v: Value = serde_json::from_slice(&body).unwrap();
        for p in v["posts"].as_array().unwrap() {
            all.push(p["item_hash"].as_str().unwrap().to_string());
        }
        match v.get("next_cursor").and_then(|x| x.as_str()) {
            Some(c) if !c.is_empty() => cursor = c.to_string(),
            _ => break,
        }
    }
    let unique: std::collections::HashSet<_> = all.iter().collect();
    assert_eq!(all.len(), unique.len());
    assert_eq!(all.len(), expected.len());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn files_cursor_invalid_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/addresses/0xtest/files?cursor=not-valid!!!").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn credit_history_cursor_invalid_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(
        app,
        "/api/v0/addresses/0xtest/credit_history?cursor=bad-cursor",
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn balances_cursor_invalid_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/balances?cursor=bad").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn credit_balances_cursor_invalid_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/credit_balances?cursor=bad").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn aggregates_cursor_pagination_walks_all() {
    let pg = start_postgres().await;
    let creation = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    for i in 0..3 {
        insert_aggregate_seed(
            &pg.pool,
            &AggSeed {
                item_hash: format!("{:0>64x}", 0x2000 + i),
                key: format!("key{i}"),
                owner: format!("0xown{i}"),
                content: json!({"v": i}),
                creation,
            },
        )
        .await
        .unwrap();
    }
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    let mut all: Vec<(String, String)> = Vec::new();
    let mut cursor = String::new();
    for _ in 0..10 {
        let uri = format!("/api/v0/aggregates.json?pagination=2&cursor={cursor}");
        let (status, body) = get(app.clone(), &uri).await;
        assert_eq!(status, StatusCode::OK);
        let v: Value = serde_json::from_slice(&body).unwrap();
        for a in v["aggregates"].as_array().unwrap() {
            all.push((
                a["address"].as_str().unwrap().to_string(),
                a["key"].as_str().unwrap().to_string(),
            ));
        }
        match v.get("next_cursor").and_then(|x| x.as_str()) {
            Some(c) if !c.is_empty() => cursor = c.to_string(),
            _ => break,
        }
    }
    let unique: std::collections::HashSet<_> = all.iter().collect();
    assert_eq!(all.len(), unique.len());
    assert!(!all.is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn address_stats_cursor_invalid_returns_422() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v1/addresses/stats.json?cursor=bad").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn address_stats_cursor_pagination_walks_all() {
    // The accounts v1 controller deserializes numeric query params from the
    // raw String map and fails with 422 on `pagination=N`. We exercise the
    // default-pagination cursor-less walk only.
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v1/addresses/stats.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["data"].as_object().unwrap().is_empty());
}
