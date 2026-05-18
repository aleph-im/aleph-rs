//! Ports `tests/db/test_posts.py`.

mod common;

use chrono::{TimeZone, Utc};
use serde_json::json;

use aleph_ccn::db::accessors::posts::{
    count_matching_posts, delete_post, get_matching_posts, get_original_post, get_post,
    refresh_latest_amend, PostFilters,
};
use aleph_ccn::db::models::posts::PostDb;
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::sort_order::{SortBy, SortOrder};

use common::fixtures::{insert_post, set_post_latest_amend};
use common::{start_postgres};

fn original_post() -> PostDb {
    PostDb {
        item_hash: "285e8bce91cdcf595b711687906f35e53a61198da9df56b4f86e5686151ee41d".into(),
        owner: "0xabadbabe".into(),
        r#type: Some("my-post-type".into()),
        r#ref: None,
        amends: None,
        channel: Some(Channel::from("MY-CHANNEL".to_string())),
        content: json!({"body": "Hello, world!"}),
        creation_datetime: Utc
            .with_ymd_and_hms(2022, 11, 11, 11, 11, 11)
            .unwrap(),
        latest_amend: None,
        tags: None,
    }
}

fn first_amend_post() -> PostDb {
    PostDb {
        item_hash: "cf315f88a3ab02a49df43114463ded42c266aa32c0168ee2bf231fda9e930ffb".into(),
        owner: "0xabadbabe".into(),
        r#type: Some("amend".into()),
        r#ref: Some(original_post().item_hash),
        amends: Some(original_post().item_hash),
        channel: original_post().channel,
        content: json!({"body": "Goodbye blue sky"}),
        creation_datetime: Utc.with_ymd_and_hms(2022, 12, 6, 0, 0, 0).unwrap(),
        latest_amend: None,
        tags: None,
    }
}

fn second_amend_post() -> PostDb {
    PostDb {
        item_hash: "5189605b437e0a8808acb318f0fd3e73b3682c44a3f93e2d4d58c35c03caf2a2".into(),
        owner: "0xabadbabe".into(),
        r#type: Some("amend".into()),
        r#ref: Some(original_post().item_hash),
        amends: Some(original_post().item_hash),
        channel: original_post().channel,
        content: json!({"body": "Gutentag!"}),
        creation_datetime: Utc.with_ymd_and_hms(2022, 12, 25, 0, 0, 0).unwrap(),
        latest_amend: None,
        tags: None,
    }
}

fn post_from_second_user() -> PostDb {
    PostDb {
        item_hash: "4fc575ac98f3c69e543792758f62fc0c4d3a1b422281a7425e545d979012c065".into(),
        owner: "0xdeadbabe".into(),
        r#type: Some("great-posts".into()),
        r#ref: None,
        amends: None,
        channel: Some(Channel::from("ALEPH-POSTS".to_string())),
        content: json!({"body": "You're my favorite customer"}),
        creation_datetime: Utc.with_ymd_and_hms(2022, 10, 12, 0, 0, 0).unwrap(),
        latest_amend: None,
        tags: None,
    }
}

#[tokio::test]
async fn get_post_no_amend_returns_original_content() {
    let pg = start_postgres().await;
    let orig = original_post();
    insert_post(&pg.pool, &orig).await.unwrap();
    let client = pg.pool.get().await.unwrap();
    let m = get_post(&**client, &orig.item_hash).await.unwrap().expect("post");
    assert_eq!(m.item_hash, orig.item_hash);
    assert_eq!(m.content, orig.content);
    assert_eq!(m.last_updated, orig.creation_datetime);
    assert_eq!(m.created, orig.creation_datetime);
}

#[tokio::test]
async fn get_post_with_one_amend_returns_amend_for_original_hash() {
    let pg = start_postgres().await;
    let orig = original_post();
    let amend = first_amend_post();
    insert_post(&pg.pool, &orig).await.unwrap();
    insert_post(&pg.pool, &amend).await.unwrap();
    set_post_latest_amend(&pg.pool, &orig.item_hash, Some(&amend.item_hash))
        .await
        .unwrap();

    let client = pg.pool.get().await.unwrap();
    let m = get_post(&**client, &orig.item_hash)
        .await
        .unwrap()
        .expect("post");
    assert_eq!(m.item_hash, amend.item_hash);
    assert_eq!(m.original_item_hash, orig.item_hash);
    assert_eq!(m.content, amend.content);
    assert_eq!(m.last_updated, amend.creation_datetime);

    // querying by amend hash returns None
    let none = get_post(&**client, &amend.item_hash).await.unwrap();
    assert!(none.is_none());
}

#[tokio::test]
async fn get_post_with_two_amends_returns_latest() {
    let pg = start_postgres().await;
    let orig = original_post();
    let a1 = first_amend_post();
    let a2 = second_amend_post();
    insert_post(&pg.pool, &orig).await.unwrap();
    insert_post(&pg.pool, &a1).await.unwrap();
    insert_post(&pg.pool, &a2).await.unwrap();
    set_post_latest_amend(&pg.pool, &orig.item_hash, Some(&a2.item_hash))
        .await
        .unwrap();

    let client = pg.pool.get().await.unwrap();
    let m = get_post(&**client, &orig.item_hash)
        .await
        .unwrap()
        .expect("post");
    assert_eq!(m.item_hash, a2.item_hash);
    assert_eq!(m.content, a2.content);
}

async fn seed_two_users(pg: &aleph_ccn::db::DbPool) -> (PostDb, PostDb, PostDb) {
    let orig = original_post();
    let amend = first_amend_post();
    let other = post_from_second_user();
    insert_post(pg, &orig).await.unwrap();
    insert_post(pg, &amend).await.unwrap();
    insert_post(pg, &other).await.unwrap();
    set_post_latest_amend(pg, &orig.item_hash, Some(&amend.item_hash))
        .await
        .unwrap();
    (orig, amend, other)
}

#[tokio::test]
async fn get_matching_posts_returns_all_and_counts_two() {
    let pg = start_postgres().await;
    let (orig, amend, other) = seed_two_users(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();

    let mut filters = PostFilters::default();
    filters.sort_by = Some(SortBy::Time);
    filters.sort_order = Some(SortOrder::Descending);
    filters.pagination = 100;
    filters.page = 1;
    let rows = get_matching_posts(&**client, &filters).await.unwrap();
    assert_eq!(rows.len(), 2);
    let count = count_matching_posts(&**client, None).await.unwrap();
    assert_eq!(count, 2);

    // by hash
    let mut by_hash = filters.clone();
    by_hash.hashes = Some(vec![orig.item_hash.clone()]);
    let rows = get_matching_posts(&**client, &by_hash).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item_hash, amend.item_hash);
    assert_eq!(rows[0].original_item_hash, orig.item_hash);

    let c = count_matching_posts(&**client, Some(&by_hash)).await.unwrap();
    assert_eq!(c, 1);

    // by owner
    let mut by_addr = filters.clone();
    by_addr.addresses = Some(vec![other.owner.clone()]);
    let rows = get_matching_posts(&**client, &by_addr).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].original_item_hash, other.item_hash);

    // by channel
    let mut by_chan = filters.clone();
    by_chan.channels = Some(vec![{
        let ch = other.channel.as_ref().unwrap();
        serde_json::to_value(ch)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default()
    }]);
    let rows = get_matching_posts(&**client, &by_chan).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].original_item_hash, other.item_hash);
}

#[tokio::test]
async fn get_matching_posts_time_filters() {
    let pg = start_postgres().await;
    let (_orig, amend, _other) = seed_two_users(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();
    let start = amend.creation_datetime;
    let end = start + chrono::Duration::days(1);
    let mut filters = PostFilters::default();
    filters.sort_by = Some(SortBy::Time);
    filters.sort_order = Some(SortOrder::Descending);
    filters.pagination = 100;
    filters.page = 1;
    filters.start_date = Some(start.timestamp() as f64);
    filters.end_date = Some(end.timestamp() as f64);
    let rows = get_matching_posts(&**client, &filters).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item_hash, amend.item_hash);
}

#[tokio::test]
async fn get_matching_posts_sort_order() {
    let pg = start_postgres().await;
    let (orig, amend, other) = seed_two_users(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();
    let mut filters = PostFilters::default();
    filters.sort_by = Some(SortBy::Time);
    filters.sort_order = Some(SortOrder::Ascending);
    filters.pagination = 100;
    filters.page = 1;
    let asc = get_matching_posts(&**client, &filters).await.unwrap();
    assert_eq!(asc[0].original_item_hash, other.item_hash);
    assert_eq!(asc[1].original_item_hash, orig.item_hash);
    assert_eq!(asc[1].item_hash, amend.item_hash);

    filters.sort_order = Some(SortOrder::Descending);
    let desc = get_matching_posts(&**client, &filters).await.unwrap();
    assert_eq!(desc[0].original_item_hash, orig.item_hash);
    assert_eq!(desc[1].original_item_hash, other.item_hash);
}

#[tokio::test]
async fn get_matching_posts_empty() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let mut filters = PostFilters::default();
    filters.sort_by = Some(SortBy::Time);
    filters.sort_order = Some(SortOrder::Descending);
    filters.pagination = 100;
    filters.page = 1;
    let rows = get_matching_posts(&**client, &filters).await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn refresh_latest_amend_updates_to_last_amend_and_handles_deletion() {
    let pg = start_postgres().await;
    let orig = original_post();
    let a1 = first_amend_post();
    let a2 = second_amend_post();
    insert_post(&pg.pool, &orig).await.unwrap();
    insert_post(&pg.pool, &a1).await.unwrap();
    insert_post(&pg.pool, &a2).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    refresh_latest_amend(&**client, &orig.item_hash).await.unwrap();
    let updated = get_original_post(&**client, &orig.item_hash)
        .await
        .unwrap()
        .expect("orig");
    assert_eq!(updated.latest_amend.as_deref(), Some(a2.item_hash.as_str()));

    delete_post(&**client, &a2.item_hash).await.unwrap();
    refresh_latest_amend(&**client, &orig.item_hash).await.unwrap();
    let updated = get_original_post(&**client, &orig.item_hash)
        .await
        .unwrap()
        .expect("orig");
    assert_eq!(updated.latest_amend.as_deref(), Some(a1.item_hash.as_str()));

    delete_post(&**client, &a1.item_hash).await.unwrap();
    refresh_latest_amend(&**client, &orig.item_hash).await.unwrap();
    let updated = get_original_post(&**client, &orig.item_hash)
        .await
        .unwrap()
        .expect("orig");
    assert!(updated.latest_amend.is_none());
}
