//! Ports `tests/api/test_balance.py`. The original Python tests rely on a live
//! message processor pipeline to produce locked-amount values; the Rust port
//! exercises the same endpoints with seeded balance + cost rows instead.

mod common;

use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

use common::{insert_user_balance, make_app_state, start_postgres};

const ACCOUNT: &str = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";

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
async fn get_balance_default_aggregates_balance_for_address() {
    let pg = start_postgres().await;
    insert_user_balance(&pg.pool, ACCOUNT, 100_000).await.unwrap();

    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{ACCOUNT}/balance");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();

    // Full JSON shape — mirrors `test_get_balance.test_get_balance_*` in the
    // Python suite which assert the response carries address/chain/dapp keys
    // alongside the numeric balance/locked fields.
    assert_eq!(v["address"].as_str(), Some(ACCOUNT));
    // chain defaults to ETH when not filtered.
    let chain = v["chain"].as_str();
    assert!(
        chain == Some("ETH") || chain.is_none(),
        "chain must be ETH or absent, got {chain:?}",
    );
    // dapp is null on the default aggregate-balance response (no dapp filter).
    assert!(
        v["dapp"].is_null() || !v.as_object().unwrap().contains_key("dapp"),
        "dapp must be null or absent on aggregate balance response, got {:?}",
        v["dapp"],
    );
    // locked_amount must always be present so the consumer can compute available
    // balance without additional API calls.
    assert!(
        v.get("locked_amount").is_some(),
        "locked_amount field must be present",
    );

    let balance: f64 = v["balance"].as_str().unwrap().parse().unwrap();
    assert!((balance - 100_000.0).abs() < 1e-6);
    assert!(v["details"].is_object());
    let eth: f64 = v["details"]["ETH"].as_str().unwrap().parse().unwrap();
    assert!((eth - 100_000.0).abs() < 1e-6);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_balance_with_chain_filter() {
    let pg = start_postgres().await;
    insert_user_balance(&pg.pool, ACCOUNT, 50_000).await.unwrap();

    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{ACCOUNT}/balance?chain=ETH");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let bal: f64 = v["balance"].as_str().unwrap().parse().unwrap();
    assert!((bal - 50_000.0).abs() < 1e-6);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_balance_with_no_balance_returns_zero() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{ACCOUNT}/balance");
    let (status, body) = get(app.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let bal: f64 = v["balance"].as_str().unwrap().parse().unwrap();
    assert_eq!(bal, 0.0);
    let locked: f64 = v["locked_amount"].as_str().unwrap().parse().unwrap();
    assert_eq!(locked, 0.0);

    let uri = format!("/api/v0/addresses/{ACCOUNT}/balance?chain=ETH");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let bal: f64 = v["balance"].as_str().unwrap().parse().unwrap();
    assert_eq!(bal, 0.0);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_balances_endpoint() {
    let pg = start_postgres().await;
    insert_user_balance(&pg.pool, ACCOUNT, 12345).await.unwrap();
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/balances").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["balances"].as_array().unwrap().len() > 0);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_balances_with_chains_filter() {
    let pg = start_postgres().await;
    insert_user_balance(&pg.pool, ACCOUNT, 100).await.unwrap();
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/balances?chains=ETH").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["balances"].as_array().unwrap().len() > 0);
}
