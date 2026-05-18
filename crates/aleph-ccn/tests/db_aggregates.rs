//! Ports `tests/db/test_aggregates.py`.

mod common;

use std::collections::HashSet;

use chrono::{TimeZone, Utc};
use serde_json::json;

use aleph_ccn::db::accessors::aggregates::{
    get_aggregate_by_key, get_aggregate_content_keys, refresh_aggregate,
};

use common::fixtures::{insert_aggregate_element_row, insert_aggregate_row};
use common::{start_postgres};

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregate_by_key_returns_row() {
    let pg = start_postgres().await;
    let creation = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
    insert_aggregate_element_row(&pg.pool, "1234", "key", "Me", &json!({}), creation)
        .await
        .unwrap();
    insert_aggregate_row(
        &pg.pool,
        "key",
        "Me",
        &json!({"a": 1, "b": 2}),
        creation,
        "1234",
        false,
    )
    .await
    .unwrap();

    let client = pg.pool.get().await.unwrap();
    let agg = get_aggregate_by_key(&**client, "Me", "key", true)
        .await
        .unwrap()
        .expect("aggregate present");
    assert_eq!(agg.key, "key");
    assert_eq!(agg.owner, "Me");
    assert_eq!(agg.content, json!({"a": 1, "b": 2}));
    assert_eq!(agg.last_revision_hash, "1234");

    // with_content=false returns 'null'::jsonb
    let agg = get_aggregate_by_key(&**client, "Me", "key", false)
        .await
        .unwrap()
        .expect("aggregate present");
    assert!(agg.content.is_null());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregate_by_key_no_data_returns_none() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let agg = get_aggregate_by_key(&**client, "owner", "key", true)
        .await
        .unwrap();
    assert!(agg.is_none());
}

fn aggregate_fixture_elements() -> Vec<(String, serde_json::Value, chrono::DateTime<Utc>)> {
    vec![
        (
            "1".into(),
            json!({"a": "alien"}),
            Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
        ),
        (
            "2".into(),
            json!({"b": "batman"}),
            Utc.with_ymd_and_hms(2022, 1, 2, 0, 0, 0).unwrap(),
        ),
        (
            "3".into(),
            json!({"a": "aleph"}),
            Utc.with_ymd_and_hms(2022, 1, 3, 0, 0, 0).unwrap(),
        ),
        (
            "4".into(),
            json!({"c": "chianti"}),
            Utc.with_ymd_and_hms(2022, 1, 4, 0, 0, 0).unwrap(),
        ),
    ]
}

async fn seed_elements(pg: &aleph_ccn::db::DbPool) {
    for (item_hash, content, creation) in aggregate_fixture_elements() {
        insert_aggregate_element_row(pg, &item_hash, "key", "me", &content, creation)
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn refresh_aggregate_insert_path_builds_merged_content() {
    let pg = start_postgres().await;
    seed_elements(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();
    refresh_aggregate(&**client, "me", "key").await.unwrap();
    let agg = get_aggregate_by_key(&**client, "me", "key", true)
        .await
        .unwrap()
        .expect("aggregate");
    assert_eq!(agg.last_revision_hash, "4");
    assert_eq!(agg.dirty, false);
    assert_eq!(
        agg.content,
        json!({"a": "aleph", "b": "batman", "c": "chianti"})
    );
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn refresh_aggregate_update_path_overwrites_dirty_row() {
    let pg = start_postgres().await;
    seed_elements(&pg.pool).await;

    // Pre-existing dirty row pointing at the first element only.
    let creation = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
    insert_aggregate_row(
        &pg.pool,
        "key",
        "me",
        &json!({"a": "alien"}),
        creation,
        "1",
        true,
    )
    .await
    .unwrap();

    let client = pg.pool.get().await.unwrap();
    refresh_aggregate(&**client, "me", "key").await.unwrap();
    let agg = get_aggregate_by_key(&**client, "me", "key", true)
        .await
        .unwrap()
        .expect("aggregate");
    assert_eq!(agg.last_revision_hash, "4");
    assert_eq!(agg.dirty, false);
    assert_eq!(
        agg.content,
        json!({"a": "aleph", "b": "batman", "c": "chianti"})
    );
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn refresh_aggregate_update_no_op_keeps_same_state() {
    let pg = start_postgres().await;
    seed_elements(&pg.pool).await;
    let creation_final = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
    insert_aggregate_row(
        &pg.pool,
        "key",
        "me",
        &json!({"a": "aleph", "b": "batman", "c": "chianti"}),
        creation_final,
        "4",
        false,
    )
    .await
    .unwrap();

    let client = pg.pool.get().await.unwrap();
    refresh_aggregate(&**client, "me", "key").await.unwrap();
    let agg = get_aggregate_by_key(&**client, "me", "key", true)
        .await
        .unwrap()
        .expect("aggregate");
    assert_eq!(agg.last_revision_hash, "4");
    assert!(!agg.dirty);
    assert_eq!(
        agg.content,
        json!({"a": "aleph", "b": "batman", "c": "chianti"})
    );
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_content_keys_returns_top_level_json_keys() {
    let pg = start_postgres().await;
    seed_elements(&pg.pool).await;
    let creation = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
    insert_aggregate_row(
        &pg.pool,
        "key",
        "me",
        &json!({"a": "aleph", "b": "batman", "c": "chianti"}),
        creation,
        "4",
        false,
    )
    .await
    .unwrap();

    let client = pg.pool.get().await.unwrap();
    let keys = get_aggregate_content_keys(&**client, "me", "key").await.unwrap();
    let set: HashSet<String> = keys.into_iter().collect();
    let expected: HashSet<String> = ["a", "b", "c"].into_iter().map(String::from).collect();
    assert_eq!(set, expected);

    // No-match.
    let empty = get_aggregate_content_keys(&**client, "no-one", "not-a-key")
        .await
        .unwrap();
    assert!(empty.is_empty());
}
