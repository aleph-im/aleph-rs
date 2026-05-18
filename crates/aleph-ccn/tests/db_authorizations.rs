//! Ports `tests/db/test_authorizations.py`.

mod common;

use std::collections::HashMap;

use chrono::{TimeZone, Utc};
use serde_json::{Value, json};

use aleph_ccn::db::accessors::authorizations::{
    AuthFilter, filter_authorizations, get_granted_authorizations, get_received_authorizations,
    paginate_authorizations,
};

use common::fixtures::{insert_aggregate_element_row, insert_aggregate_row};
use common::{start_postgres};

async fn seed_security_aggregates(pg: &aleph_ccn::db::DbPool) {
    let creation = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let creation_b = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
    let creation_e = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();

    insert_aggregate_element_row(
        pg,
        "hash_a",
        "security",
        "0xOwnerA",
        &json!({"authorizations": []}),
        creation,
    )
    .await
    .unwrap();
    insert_aggregate_element_row(
        pg,
        "hash_b",
        "security",
        "0xOwnerB",
        &json!({"authorizations": []}),
        creation_b,
    )
    .await
    .unwrap();
    insert_aggregate_element_row(
        pg,
        "hash_empty",
        "security",
        "0xOwnerEmpty",
        &json!({}),
        creation_e,
    )
    .await
    .unwrap();

    let owner_a = json!({
        "authorizations": [
            {"address": "0xGranteeB", "types": ["POST"], "channels": ["chan1"], "chain": "ETH"},
            {"address": "0xGranteeC", "types": ["STORE"]},
        ]
    });
    insert_aggregate_row(pg, "security", "0xOwnerA", &owner_a, creation, "hash_a", false)
        .await
        .unwrap();

    let owner_b = json!({
        "authorizations": [
            {"address": "0xOwnerA", "types": ["POST", "STORE"]},
            {"address": "0xGranteeB", "types": ["AGGREGATE"]},
        ]
    });
    insert_aggregate_row(pg, "security", "0xOwnerB", &owner_b, creation_b, "hash_b", false)
        .await
        .unwrap();

    insert_aggregate_row(
        pg,
        "security",
        "0xOwnerEmpty",
        &json!({}),
        creation_e,
        "hash_empty",
        false,
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn granted_authorizations_returns_raw_content() {
    let pg = start_postgres().await;
    seed_security_aggregates(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();
    let result = get_granted_authorizations(&**client, "0xOwnerA")
        .await
        .unwrap()
        .expect("aggregate exists");
    let auths = result["authorizations"].as_array().unwrap();
    assert_eq!(auths.len(), 2);
    assert_eq!(auths[0]["address"], "0xGranteeB");
    assert_eq!(auths[1]["address"], "0xGranteeC");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn granted_authorizations_missing_owner_returns_none() {
    let pg = start_postgres().await;
    seed_security_aggregates(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();
    let result = get_granted_authorizations(&**client, "0xNobody").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn received_authorizations_strips_address_field() {
    let pg = start_postgres().await;
    seed_security_aggregates(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();
    let received = get_received_authorizations(&**client, "0xOwnerA")
        .await
        .unwrap();
    assert_eq!(received.len(), 1);
    let (owner, auths) = &received[0];
    assert_eq!(owner, "0xOwnerB");
    assert_eq!(auths.len(), 1);
    assert_eq!(auths[0]["types"], json!(["POST", "STORE"]));
    assert!(auths[0].get("address").is_none());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn received_authorizations_multiple_granters() {
    let pg = start_postgres().await;
    seed_security_aggregates(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();
    let received = get_received_authorizations(&**client, "0xGranteeB")
        .await
        .unwrap();
    assert_eq!(received.len(), 2);
    let map: HashMap<String, Vec<Value>> = received.into_iter().collect();
    assert_eq!(map["0xOwnerA"][0]["types"], json!(["POST"]));
    assert_eq!(map["0xOwnerB"][0]["types"], json!(["AGGREGATE"]));
    assert!(map["0xOwnerA"][0].get("address").is_none());
    assert!(map["0xOwnerB"][0].get("address").is_none());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn received_authorizations_none() {
    let pg = start_postgres().await;
    seed_security_aggregates(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();
    let received = get_received_authorizations(&**client, "0xNobody").await.unwrap();
    assert!(received.is_empty());
}

// ------------------------------------------------------------------
// Pure-function filter / pagination tests (no DB needed but kept here
// to mirror the Python file structure).
// ------------------------------------------------------------------

fn sample_authorizations() -> HashMap<String, Vec<Value>> {
    let mut m = HashMap::new();
    m.insert(
        "0xGranterA".into(),
        vec![
            json!({"types": ["POST"], "channels": ["chan1", "chan2"], "chain": "ETH", "post_types": ["amend"], "aggregate_keys": []}),
            json!({"types": ["STORE"], "channels": ["chan3"], "chain": "SOL"}),
        ],
    );
    m.insert(
        "0xGranterB".into(),
        vec![json!({"types": ["POST", "STORE"]})],
    );
    m.insert(
        "0xGranterC".into(),
        vec![json!({"types": ["AGGREGATE"], "aggregate_keys": ["key1"]})],
    );
    m
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn filter_by_types() {
    let _pg = start_postgres().await;
    let m = sample_authorizations();
    let types = vec!["POST".to_string()];
    let f = AuthFilter {
        types: Some(&types),
        ..Default::default()
    };
    let out = filter_authorizations(&m, &f);
    assert!(out.contains_key("0xGranterA"));
    assert!(out.contains_key("0xGranterB"));
    assert!(!out.contains_key("0xGranterC"));
    assert_eq!(out["0xGranterA"].len(), 1);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn filter_by_channels() {
    let _pg = start_postgres().await;
    let m = sample_authorizations();
    let chans = vec!["chan1".to_string()];
    let f = AuthFilter {
        channels: Some(&chans),
        ..Default::default()
    };
    let out = filter_authorizations(&m, &f);
    assert!(out.contains_key("0xGranterA"));
    assert!(out.contains_key("0xGranterB"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn filter_by_chains() {
    let _pg = start_postgres().await;
    let m = sample_authorizations();
    let chains = vec!["ETH".to_string()];
    let f = AuthFilter {
        chains: Some(&chains),
        ..Default::default()
    };
    let out = filter_authorizations(&m, &f);
    assert!(out.contains_key("0xGranterA"));
    assert_eq!(out["0xGranterA"].len(), 1);
    assert!(out.contains_key("0xGranterB"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn filter_by_post_types() {
    let _pg = start_postgres().await;
    let m = sample_authorizations();
    let post_types = vec!["amend".to_string()];
    let f = AuthFilter {
        post_types: Some(&post_types),
        ..Default::default()
    };
    let out = filter_authorizations(&m, &f);
    assert!(out.contains_key("0xGranterA"));
    assert_eq!(out["0xGranterA"].len(), 2);
    assert!(out.contains_key("0xGranterB"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn filter_by_aggregate_keys() {
    let _pg = start_postgres().await;
    let m = sample_authorizations();
    let agg_keys = vec!["key1".to_string()];
    let f = AuthFilter {
        aggregate_keys: Some(&agg_keys),
        ..Default::default()
    };
    let out = filter_authorizations(&m, &f);
    assert!(out.contains_key("0xGranterC"));
    assert!(out.contains_key("0xGranterB"));
    assert!(out.contains_key("0xGranterA"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn filter_no_filters_returns_all() {
    let _pg = start_postgres().await;
    let m = sample_authorizations();
    let f = AuthFilter::default();
    let out = filter_authorizations(&m, &f);
    assert_eq!(out, m);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn filter_combined_types_and_chains() {
    let _pg = start_postgres().await;
    let m = sample_authorizations();
    let types = vec!["POST".to_string()];
    let chains = vec!["ETH".to_string()];
    let f = AuthFilter {
        types: Some(&types),
        chains: Some(&chains),
        ..Default::default()
    };
    let out = filter_authorizations(&m, &f);
    assert!(out.contains_key("0xGranterA"));
    assert_eq!(out["0xGranterA"].len(), 1);
    assert!(out.contains_key("0xGranterB"));
    assert!(!out.contains_key("0xGranterC"));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn paginate_first_page() {
    let _pg = start_postgres().await;
    let mut data = HashMap::new();
    for i in 0..5 {
        data.insert(format!("0xAddr{i}"), vec![json!({"types": ["POST"]})]);
    }
    let (page, total) = paginate_authorizations(&data, 1, 2);
    assert_eq!(total, 5);
    assert_eq!(page.len(), 2);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn paginate_second_page() {
    let _pg = start_postgres().await;
    let mut data = HashMap::new();
    for i in 0..5 {
        data.insert(format!("0xAddr{i}"), vec![json!({"types": ["POST"]})]);
    }
    let (page, total) = paginate_authorizations(&data, 2, 2);
    assert_eq!(total, 5);
    assert_eq!(page.len(), 2);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn paginate_last_page() {
    let _pg = start_postgres().await;
    let mut data = HashMap::new();
    for i in 0..5 {
        data.insert(format!("0xAddr{i}"), vec![json!({"types": ["POST"]})]);
    }
    let (page, total) = paginate_authorizations(&data, 3, 2);
    assert_eq!(total, 5);
    assert_eq!(page.len(), 1);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn paginate_empty() {
    let _pg = start_postgres().await;
    let data: HashMap<String, Vec<Value>> = HashMap::new();
    let (page, total) = paginate_authorizations(&data, 1, 20);
    assert_eq!(total, 0);
    assert!(page.is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn paginate_beyond_range() {
    let _pg = start_postgres().await;
    let mut data = HashMap::new();
    data.insert("0xAddr0".into(), vec![json!({"types": ["POST"]})]);
    let (page, total) = paginate_authorizations(&data, 5, 20);
    assert_eq!(total, 1);
    assert!(page.is_empty());
}
