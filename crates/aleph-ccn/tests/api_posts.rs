//! Ports `tests/api/test_posts.py`.

mod common;

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;
use axum::body::{Body, to_bytes};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::fixtures::{insert_post, insert_processed, make_message, make_post_db};
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

fn make_post_message(
    hash: &str,
    sender: &str,
    ptype: Option<&str>,
    pref: Option<&str>,
    tags: Option<&[&str]>,
    time_sec: f64,
) -> aleph_ccn::db::models::messages::MessageDb {
    let mut content = json!({
        "address": sender,
        "time": time_sec,
        "type": ptype,
        "content": {
            "title": format!("{hash} content"),
            "tags": tags.map(|ts| ts.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap_or_default(),
        },
    });
    if let Some(r) = pref {
        content["ref"] = Value::String(r.to_string());
    }
    make_message(
        hash,
        sender,
        Chain::Ethereum,
        MessageType::Post,
        ItemType::Inline,
        content,
        Some("TEST"),
        time_sec,
    )
}

async fn seed_basic_posts(pool: &aleph_ccn::db::DbPool) -> Vec<String> {
    let mut hashes = Vec::new();
    for i in 0..3 {
        let hash = format!("post_hash_{i}");
        let sender = format!("0xpost{i}");
        let m = make_post_message(&hash, &sender, Some("blog"), None, None, 1.0 + i as f64);
        insert_processed(pool, &m).await.unwrap();
        let p = make_post_db(&m);
        insert_post(pool, &p).await.unwrap();
        hashes.push(hash);
    }
    hashes
}

#[tokio::test]
async fn get_posts_returns_all() {
    let pg = start_postgres().await;
    let hashes = seed_basic_posts(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v1/posts.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let posts = v["posts"].as_array().unwrap();
    assert_eq!(posts.len(), hashes.len());
    for p in posts {
        assert!(p.get("_id").is_none());
        let h = p["item_hash"].as_str().unwrap();
        assert!(hashes.iter().any(|x| x == h));
    }
}

#[tokio::test]
async fn get_posts_filter_by_ref() {
    let pg = start_postgres().await;
    let _ = seed_basic_posts(&pg.pool).await;

    let ref_hash = "ref_target_hash";
    let sender = "0xrefowner";
    let m = make_post_message(
        "refpost_hash",
        sender,
        Some("custom"),
        Some(ref_hash),
        Some(&["mainnet"]),
        10.0,
    );
    insert_processed(&pg.pool, &m).await.unwrap();
    let p = make_post_db(&m);
    insert_post(&pg.pool, &p).await.unwrap();
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    let uri = format!("/api/v0/posts.json?refs={ref_hash}");
    let (status, body) = get(app.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["posts"].as_array().unwrap().len(), 1);
    assert_eq!(v["pagination_total"].as_i64(), Some(1));

    let (status, body) = get(app, "/api/v0/posts.json?refs=not-a-ref").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["posts"].as_array().unwrap().len(), 0);
    assert_eq!(v["pagination_total"].as_i64(), Some(0));
}

#[tokio::test]
async fn get_posts_filter_by_tags() {
    let pg = start_postgres().await;

    let sender = "0xtagowner";
    let m = make_post_message(
        "tag_hash",
        sender,
        Some("blog"),
        None,
        Some(&["mainnet", "alpha"]),
        20.0,
    );
    insert_processed(&pg.pool, &m).await.unwrap();
    let p = make_post_db(&m);
    insert_post(&pg.pool, &p).await.unwrap();
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    let (status, body) = get(app.clone(), "/api/v0/posts.json?tags=mainnet").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["posts"].as_array().unwrap().len(), 1);

    let (status, body) = get(app.clone(), "/api/v0/posts.json?tags=not-a-tag").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["posts"].as_array().unwrap().len(), 0);

    let (status, body) = get(app, "/api/v0/posts.json?tags=mainnet,alpha").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["posts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn get_amended_post_replaces_original() {
    let pg = start_postgres().await;
    let sender = "0xamend";

    // Original post
    let original_hash = "orig_hash";
    let original_msg = make_post_message(original_hash, sender, None, Some("custom-ref"), None, 1.0);
    insert_processed(&pg.pool, &original_msg).await.unwrap();
    let mut original_post = make_post_db(&original_msg);

    // Amend post — type=amend, ref=original_hash
    let amend_hash = "amend_hash";
    let amend_msg = make_post_message(
        amend_hash,
        sender,
        Some("amend"),
        Some(original_hash),
        None,
        2.0,
    );
    insert_processed(&pg.pool, &amend_msg).await.unwrap();
    let amend_post = make_post_db(&amend_msg);

    // Insert original first, then amend, then update original.latest_amend
    original_post.latest_amend = Some(amend_hash.into());
    insert_post(&pg.pool, &original_post).await.unwrap();
    insert_post(&pg.pool, &amend_post).await.unwrap();

    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/posts.json?refs=custom-ref").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let posts = v["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 1);
    let post = &posts[0];
    assert_eq!(post["item_hash"].as_str(), Some(amend_hash));
    assert_eq!(post["original_item_hash"].as_str(), Some(original_hash));
}

#[tokio::test]
async fn get_posts_empty_db_returns_empty() {
    let pg = start_postgres().await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v1/posts.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["posts"].as_array().unwrap().is_empty());
}
