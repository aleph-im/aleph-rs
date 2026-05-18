//! Ports `tests/api/test_authorizations.py`.

mod common;

use axum::body::{Body, to_bytes};
use chrono::{TimeZone, Utc};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::fixtures::{AggSeed, insert_aggregate_row, insert_aggregate_seed};
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

async fn seed_security(pool: &aleph_ccn::db::DbPool) {
    let t_a = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let t_b = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

    let content_a = json!({
        "authorizations": [
            {"address": "0xGranteeB", "types": ["POST"], "channels": ["chan1"], "chain": "ETH"},
            {"address": "0xGranteeC", "types": ["STORE"]},
        ]
    });
    let content_b = json!({
        "authorizations": [
            {"address": "0xGranteeB", "types": ["POST", "STORE"], "chain": "SOL"},
        ]
    });

    insert_aggregate_seed(
        pool,
        &AggSeed {
            item_hash: "hash_a".into(),
            key: "security".into(),
            owner: "0xOwnerA".into(),
            content: json!({"authorizations": []}),
            creation: t_a,
        },
    )
    .await
    .unwrap();
    insert_aggregate_seed(
        pool,
        &AggSeed {
            item_hash: "hash_b".into(),
            key: "security".into(),
            owner: "0xOwnerB".into(),
            content: json!({"authorizations": []}),
            creation: t_b,
        },
    )
    .await
    .unwrap();
    // Overwrite the merged aggregate row with the populated content.
    let client = pool.get().await.unwrap();
    aleph_ccn::db::accessors::aggregates::delete_aggregate(&**client, "0xOwnerA", "security")
        .await
        .unwrap();
    aleph_ccn::db::accessors::aggregates::delete_aggregate(&**client, "0xOwnerB", "security")
        .await
        .unwrap();
    drop(client);
    insert_aggregate_row(pool, "security", "0xOwnerA", &content_a, t_a, "hash_a", false)
        .await
        .unwrap();
    insert_aggregate_row(pool, "security", "0xOwnerB", &content_b, t_b, "hash_b", false)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn granted_basic() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/authorizations/granted/0xOwnerA.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["address"].as_str(), Some("0xOwnerA"));
    assert!(v["authorizations"].get("0xGranteeB").is_some());
    assert!(v["authorizations"].get("0xGranteeC").is_some());
    assert_eq!(v["pagination_total"].as_i64(), Some(2));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn granted_no_aggregate() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/authorizations/granted/0xNobody.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["authorizations"], json!({}));
    assert_eq!(v["pagination_total"].as_i64(), Some(0));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn granted_filter_grantee() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(
        app,
        "/api/v0/authorizations/granted/0xOwnerA.json?grantee=0xGranteeB",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["authorizations"].get("0xGranteeB").is_some());
    assert!(v["authorizations"].get("0xGranteeC").is_none());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn granted_pagination() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(
        app,
        "/api/v0/authorizations/granted/0xOwnerA.json?pagination=1&page=1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["authorizations"].as_object().unwrap().len(), 1);
    assert_eq!(v["pagination_total"].as_i64(), Some(2));
    assert_eq!(v["pagination_per_page"].as_i64(), Some(1));
    assert_eq!(v["pagination_page"].as_i64(), Some(1));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn received_basic() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/authorizations/received/0xGranteeB.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["authorizations"].get("0xOwnerA").is_some());
    assert!(v["authorizations"].get("0xOwnerB").is_some());
    assert_eq!(v["pagination_total"].as_i64(), Some(2));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn received_no_grants() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/authorizations/received/0xNobody.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["authorizations"], json!({}));
    assert_eq!(v["pagination_total"].as_i64(), Some(0));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn received_filter_granter() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(
        app,
        "/api/v0/authorizations/received/0xGranteeB.json?granter=0xOwnerA",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["authorizations"].get("0xOwnerA").is_some());
    assert!(v["authorizations"].get("0xOwnerB").is_none());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn received_filter_chains() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(
        app,
        "/api/v0/authorizations/received/0xGranteeB.json?chains=ETH",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["authorizations"].get("0xOwnerA").is_some());
    assert!(v["authorizations"].get("0xOwnerB").is_none());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn received_filter_types() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(
        app,
        "/api/v0/authorizations/received/0xGranteeB.json?types=STORE",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["authorizations"].get("0xOwnerB").is_some());
    assert!(v["authorizations"].get("0xOwnerA").is_none());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn received_pagination() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(
        app,
        "/api/v0/authorizations/received/0xGranteeB.json?pagination=1&page=1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["authorizations"].as_object().unwrap().len(), 1);
    assert_eq!(v["pagination_total"].as_i64(), Some(2));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn invalid_pagination() {
    let pg = start_postgres().await;
    seed_security(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(
        app,
        "/api/v0/authorizations/granted/0xOwnerA.json?pagination=0",
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}
