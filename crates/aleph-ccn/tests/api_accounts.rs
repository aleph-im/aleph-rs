//! Ports `tests/api/test_accounts.py`. Hits the production router via
//! `aleph_ccn::web::build_router`.

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

const TEST_ADDRESS: &str = "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba";

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

fn post_msg(item_hash: &str, sender: &str, ptype: Option<&str>, time_sec: f64) -> aleph_ccn::db::models::messages::MessageDb {
    let inner = json!({
        "address": sender,
        "time": time_sec,
        "type": ptype,
        "content": {"title": "x"},
    });
    make_message(
        item_hash,
        sender,
        Chain::Ethereum,
        MessageType::Post,
        ItemType::Inline,
        inner,
        Some("TEST"),
        time_sec,
    )
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn account_post_types_basic() {
    let pg = start_postgres().await;

    let msgs = vec![
        post_msg("hash1", TEST_ADDRESS, Some("blog"), 1_652_126_646.5),
        post_msg("hash2", TEST_ADDRESS, Some("blog"), 1_652_126_647.5),
        post_msg("hash3", TEST_ADDRESS, Some("news"), 1_652_126_648.5),
        post_msg("hash4", TEST_ADDRESS, Some("tutorial"), 1_652_126_649.5),
        // null type filtered out by accessor
        post_msg("hash_null", TEST_ADDRESS, None, 1_652_126_650.0),
        // From different address
        post_msg("hash6", "0xDifferentAddress", Some("blog"), 1_652_126_651.5),
    ];
    for m in &msgs {
        insert_processed(&pg.pool, m).await.unwrap();
    }
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    let uri = format!("/api/v0/addresses/{TEST_ADDRESS}/post_types");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["address"].as_str(), Some(TEST_ADDRESS));
    let types: Vec<String> = v["post_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    let mut sorted = types.clone();
    sorted.sort();
    assert_eq!(types, sorted);
    let s: std::collections::HashSet<&str> = types.iter().map(|s| s.as_str()).collect();
    assert!(s.contains("blog") && s.contains("news") && s.contains("tutorial"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn account_post_types_empty() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{TEST_ADDRESS}/post_types");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["address"].as_str(), Some(TEST_ADDRESS));
    assert!(v["post_types"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn account_post_types_different_address() {
    let pg = start_postgres().await;
    let other = "0xDifferentAddress";
    let msgs = vec![
        post_msg("hash1", TEST_ADDRESS, Some("blog"), 1_652_126_646.5),
        post_msg("hash6", other, Some("blog"), 1_652_126_651.5),
    ];
    for m in &msgs {
        insert_processed(&pg.pool, m).await.unwrap();
    }
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{other}/post_types");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["address"].as_str(), Some(other));
    let types: Vec<&str> = v["post_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    assert_eq!(types, vec!["blog"]);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn account_post_types_single() {
    let pg = start_postgres().await;
    let address = "0xSingleTypeAddress";
    let msgs = vec![
        post_msg("single1", address, Some("single"), 1_652_126_646.5),
        post_msg("single2", address, Some("single"), 1_652_126_647.5),
    ];
    for m in &msgs {
        insert_processed(&pg.pool, m).await.unwrap();
    }
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{address}/post_types");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let types: Vec<&str> = v["post_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    assert_eq!(types, vec!["single"]);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn account_channels_basic() {
    let pg = start_postgres().await;

    let channel_msg = |hash: &str, sender: &str, channel: Option<&str>, time_sec: f64| -> aleph_ccn::db::models::messages::MessageDb {
        let inner = json!({
            "address": sender,
            "time": time_sec,
            "type": "blog",
        });
        make_message(
            hash,
            sender,
            Chain::Ethereum,
            MessageType::Post,
            ItemType::Inline,
            inner,
            channel,
            time_sec,
        )
    };

    let msgs = vec![
        channel_msg("c1", TEST_ADDRESS, Some("channel1"), 1.0),
        channel_msg("c2", TEST_ADDRESS, Some("channel1"), 2.0),
        channel_msg("c3", TEST_ADDRESS, Some("channel2"), 3.0),
        channel_msg("c4", TEST_ADDRESS, Some("channel3"), 4.0),
        channel_msg("c5", TEST_ADDRESS, None, 5.0),
        channel_msg("c6", "0xDifferentAddress", Some("other_channel"), 6.0),
    ];
    for m in &msgs {
        insert_processed(&pg.pool, m).await.unwrap();
    }
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{TEST_ADDRESS}/channels");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let chans: Vec<&str> = v["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    let s: std::collections::HashSet<_> = chans.iter().copied().collect();
    assert!(s.contains("channel1") && s.contains("channel2") && s.contains("channel3"));
    let mut sorted = chans.clone();
    sorted.sort();
    assert_eq!(chans, sorted);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn account_channels_empty() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{TEST_ADDRESS}/channels");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["channels"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn account_channels_only_null() {
    let pg = start_postgres().await;
    let address = "0xNullChannelsAddress";
    let m = make_message(
        "null1",
        address,
        Chain::Ethereum,
        MessageType::Post,
        ItemType::Inline,
        json!({"address": address, "time": 1_652_126_646.5, "type": "blog"}),
        None,
        1_652_126_646.5,
    );
    insert_processed(&pg.pool, &m).await.unwrap();
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{address}/channels");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["channels"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn account_channels_different_address() {
    let pg = start_postgres().await;
    let other = "0xDifferentAddress";
    let m = make_message(
        "c6",
        other,
        Chain::Ethereum,
        MessageType::Post,
        ItemType::Inline,
        json!({"address": other, "time": 1.0, "type": "blog"}),
        Some("other_channel"),
        1.0,
    );
    insert_processed(&pg.pool, &m).await.unwrap();
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{other}/channels");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let chans: Vec<&str> = v["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    assert_eq!(chans, vec!["other_channel"]);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn account_channels_single() {
    let pg = start_postgres().await;
    let address = "0xSingleChannelAddress";
    let m1 = make_message(
        "single1",
        address,
        Chain::Ethereum,
        MessageType::Post,
        ItemType::Inline,
        json!({"address": address, "time": 1.0, "type": "blog"}),
        Some("single_channel"),
        1.0,
    );
    let m2 = make_message(
        "single2",
        address,
        Chain::Ethereum,
        MessageType::Aggregate,
        ItemType::Inline,
        json!({"address": address, "time": 2.0, "key": "test"}),
        Some("single_channel"),
        2.0,
    );
    insert_processed(&pg.pool, &m1).await.unwrap();
    insert_processed(&pg.pool, &m2).await.unwrap();
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/addresses/{address}/channels");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let chans: Vec<&str> = v["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect();
    assert_eq!(chans, vec!["single_channel"]);
}
