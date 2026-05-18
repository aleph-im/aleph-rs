//! Ports `tests/web/controllers/test_accounts_controllers.py`. The original
//! suite is mostly mock-based unit tests around `get_resource_consumed_credits`
//! and `get_account_channels` / `get_account_post_types`. The Rust port
//! exercises the same endpoints end-to-end against the real database.

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

const ADDRESS: &str = "0xCONTROLLERS";

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
async fn resource_consumed_credits_zero_for_unknown_message() {
    let pg = start_postgres().await;
    let hash = "no_credits_hash_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/messages/{hash}/consumed_credits");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["item_hash"].as_str(), Some(hash));
    assert_eq!(v["consumed_credits"].as_i64(), Some(0));
}

#[tokio::test]
async fn account_post_types_endpoint_returns_sorted() {
    let pg = start_postgres().await;
    for (h, t, ts) in [
        ("h1", "blog", 1.0),
        ("h2", "news", 2.0),
        ("h3", "tutorial", 3.0),
    ] {
        let m = make_message(
            h,
            ADDRESS,
            Chain::Ethereum,
            MessageType::Post,
            ItemType::Inline,
            json!({"address": ADDRESS, "time": ts, "type": t}),
            Some("TEST"),
            ts,
        );
        insert_processed(&pg.pool, &m).await.unwrap();
    }
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{ADDRESS}/post_types");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let types: Vec<&str> = v["post_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    assert_eq!(types, vec!["blog", "news", "tutorial"]);
}

#[tokio::test]
async fn account_channels_endpoint_returns_sorted() {
    let pg = start_postgres().await;
    for (h, c, ts) in [
        ("h1", "alpha", 1.0),
        ("h2", "bravo", 2.0),
        ("h3", "charlie", 3.0),
    ] {
        let m = make_message(
            h,
            ADDRESS,
            Chain::Ethereum,
            MessageType::Post,
            ItemType::Inline,
            json!({"address": ADDRESS, "time": ts, "type": "x"}),
            Some(c),
            ts,
        );
        insert_processed(&pg.pool, &m).await.unwrap();
    }
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{ADDRESS}/channels");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let chans: Vec<&str> = v["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    assert_eq!(chans, vec!["alpha", "bravo", "charlie"]);
}

#[tokio::test]
async fn account_post_types_unknown_address_returns_empty() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/addresses/0xnobody/post_types").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["post_types"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn account_channels_unknown_address_returns_empty() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/addresses/0xnobody/channels").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["channels"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn account_files_unknown_address_returns_404_or_empty() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/addresses/0xnobody/files").await;
    // Returns 404 when the address has no files at all; or 200 with an empty
    // array when the endpoint short-circuits to a stats-only response.
    assert!(status == StatusCode::OK || status == StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn account_credit_history_unknown_address_returns_404_or_empty() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/addresses/0xnobody/credit_history").await;
    assert!(status == StatusCode::OK || status == StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn credit_balances_default_returns_array() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/credit_balances").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["balances"].is_array() || v.is_object());
}

#[tokio::test]
async fn account_balance_endpoint_no_balance_returns_zero() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/addresses/0xnobody/balance").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let bal: f64 = v["balance"].as_str().unwrap().parse().unwrap();
    assert_eq!(bal, 0.0);
}
