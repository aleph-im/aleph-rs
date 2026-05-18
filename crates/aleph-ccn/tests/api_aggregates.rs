//! Ports `tests/api/test_aggregates.py`.

mod common;

use axum::body::{Body, to_bytes};
use chrono::{TimeZone, Utc};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::fixtures::{AggSeed, insert_aggregate_seed};
use common::{make_app_state, start_postgres};

const ADDRESS_1: &str = "0x720F319A9c3226dCDd7D8C49163D79EDa1084E98";
const ADDRESS_2: &str = "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4";

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

async fn seed_all(pool: &aleph_ccn::db::DbPool) {
    let t1 = Utc.with_ymd_and_hms(2022, 2, 14, 12, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2022, 2, 14, 12, 5, 0).unwrap();
    let t3 = Utc.with_ymd_and_hms(2022, 3, 25, 12, 0, 0).unwrap();
    let t4 = Utc.with_ymd_and_hms(2022, 2, 14, 11, 0, 0).unwrap();
    let t5 = Utc.with_ymd_and_hms(2022, 2, 14, 10, 0, 0).unwrap();

    let seeds = vec![
        // ADDRESS_1 test_reference {a:1,b:2}
        AggSeed {
            item_hash: "53c2b16aa84b10878982a2920844625546f5db32337ecd9dd15928095a30381c".into(),
            key: "test_reference".into(),
            owner: ADDRESS_1.into(),
            content: json!({"a": 1, "b": 2}),
            creation: t1,
        },
        // ADDRESS_1 test_reference {c:3,d:4} (later revision)
        AggSeed {
            item_hash: "0022ed09d16a1c3d6cbb3c7e2645657ebaa0382eba65be06264b106f528b85bf".into(),
            key: "test_reference".into(),
            owner: ADDRESS_1.into(),
            content: json!({"c": 3, "d": 4}),
            creation: t2,
        },
        // ADDRESS_2 test_reference {c:3,d:4}
        AggSeed {
            item_hash: "a87004aa03f8ae63d2c4bbe84b93b9ce70ca6482ce36c82ab0b0f689fc273f34".into(),
            key: "test_reference".into(),
            owner: ADDRESS_2.into(),
            content: json!({"c": 3, "d": 4}),
            creation: t3,
        },
        // ADDRESS_1 test_target {a:1, b:2}
        AggSeed {
            item_hash: "f875631a6c4a70ce44143bdd9a64861a5ce6f68e2267a00979ff0ad399a6c780".into(),
            key: "test_target".into(),
            owner: ADDRESS_1.into(),
            content: json!({"a": 1, "b": 2}),
            creation: t4,
        },
        // ADDRESS_1 test_key {a:1, b:2}
        AggSeed {
            item_hash: "8c83e020b1f0d3e3a8b9ba2c41be7c4fbe9c0cb6c7e74a1aac111a111d1a111a".into(),
            key: "test_key".into(),
            owner: ADDRESS_1.into(),
            content: json!({"a": 1, "b": 2}),
            creation: t5,
        },
    ];
    for s in &seeds {
        insert_aggregate_seed(pool, s).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_no_update() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/aggregates/{ADDRESS_2}.json?with_info=false");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["address"].as_str(), Some(ADDRESS_2));
    assert_eq!(v["data"]["test_reference"], json!({"c": 3, "d": 4}));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_with_info() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/aggregates/{ADDRESS_1}.json?with_info=true");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["test_key"], json!({"a": 1, "b": 2}));
    assert_eq!(v["data"]["test_target"], json!({"a": 1, "b": 2}));
    assert_eq!(
        v["data"]["test_reference"],
        json!({"a": 1, "b": 2, "c": 3, "d": 4})
    );
    assert!(v["info"]["test_reference"].is_object());

    // ISO-8601 shape parity — the Python suite asserts every `creation_datetime`
    // is rendered with microsecond precision and an explicit `+00:00` offset
    // (PEP-8601 UTC marker). Format matches `datetime.isoformat()` of a
    // tz-aware UTC datetime, e.g. `2025-01-31T00:00:00.000000+00:00`.
    let info = v["info"].as_object().expect("info must be present with with_info=true");
    for (key, entry) in info {
        let cd = entry["creation_datetime"]
            .as_str()
            .unwrap_or_else(|| panic!("info[{key}].creation_datetime must be a string"));
        // Must end with `+00:00` (UTC), not `Z`.
        assert!(
            cd.ends_with("+00:00"),
            "info[{key}].creation_datetime = {cd:?} must end with `+00:00`",
        );
        // Must include microsecond precision: the `T` literal + at least 6 digits
        // of fractional seconds before the offset.
        // Layout: YYYY-MM-DDTHH:MM:SS.ffffff+00:00 -> total length 32.
        assert_eq!(
            cd.len(),
            32,
            "info[{key}].creation_datetime = {cd:?} should be 32 chars (YYYY-MM-DDTHH:MM:SS.ffffff+00:00)",
        );
        // Sanity: parseable as RFC3339.
        chrono::DateTime::parse_from_rfc3339(cd).unwrap_or_else(|e| {
            panic!("info[{key}].creation_datetime = {cd:?} not RFC3339: {e}")
        });
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_filter_by_key() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    let uri = format!("/api/v0/aggregates/{ADDRESS_1}.json?keys=test_target&with_info=false");
    let (status, body) = get(app.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["test_target"], json!({"a": 1, "b": 2}));

    let uri = format!(
        "/api/v0/aggregates/{ADDRESS_1}.json?keys=test_target,test_reference&with_info=false"
    );
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert!(v["data"].get("test_key").is_none());
    assert!(v["data"]["test_target"].is_object());
    assert!(v["data"]["test_reference"].is_object());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_invalid_address_404() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, _) = get(app, "/api/v0/aggregates/unknown.json?with_info=false").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_value_only() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri =
        format!("/api/v0/aggregates/{ADDRESS_1}.json?keys=test_target&with_info=false&value_only=1");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v, json!({"a": 1, "b": 2}));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_list_basic() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/aggregates.json").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["aggregates"].as_array().unwrap().len(), 4);
    assert_eq!(v["pagination_total"].as_i64(), Some(4));
    assert_eq!(v["pagination_per_page"].as_i64(), Some(20));
    assert_eq!(v["pagination_page"].as_i64(), Some(1));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_list_filter_keys() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(app, "/api/v0/aggregates.json?keys=test_reference").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let agg = v["aggregates"].as_array().unwrap();
    assert_eq!(agg.len(), 2);
    for a in agg {
        assert_eq!(a["key"].as_str(), Some("test_reference"));
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_list_filter_addresses() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/aggregates.json?addresses={ADDRESS_2}");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let agg = v["aggregates"].as_array().unwrap();
    assert_eq!(agg.len(), 1);
    assert_eq!(agg[0]["address"].as_str(), Some(ADDRESS_2));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_list_pagination() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    let (status, body) = get(app.clone(), "/api/v0/aggregates.json?pagination=2&page=1").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["aggregates"].as_array().unwrap().len(), 2);
    assert_eq!(v["pagination_total"].as_i64(), Some(4));
    assert_eq!(v["pagination_page"].as_i64(), Some(1));

    let (status, body) = get(app, "/api/v0/aggregates.json?pagination=2&page=2").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["aggregates"].as_array().unwrap().len(), 2);
    assert_eq!(v["pagination_page"].as_i64(), Some(2));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_list_pagination_limits() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));

    let (s, _) = get(app.clone(), "/api/v0/aggregates.json?pagination=0").await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);

    let (s, _) = get(app.clone(), "/api/v0/aggregates.json?pagination=501").await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);

    let (s, _) = get(app.clone(), "/api/v0/aggregates.json?pagination=1").await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = get(app.clone(), "/api/v0/aggregates.json?pagination=500").await;
    assert_eq!(s, StatusCode::OK);

    let (s, _) = get(app, "/api/v0/aggregates.json?page=0").await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_list_sort_creation_asc() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(
        app,
        "/api/v0/aggregates.json?sortBy=creation_time&sortOrder=1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let arr = v["aggregates"].as_array().unwrap();
    for w in arr.windows(2) {
        assert!(w[0]["created"].as_str().unwrap() <= w[1]["created"].as_str().unwrap());
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_aggregates_list_sort_last_modified_desc() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let (status, body) = get(
        app,
        "/api/v0/aggregates.json?sortBy=last_modified&sortOrder=-1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let arr = v["aggregates"].as_array().unwrap();
    for w in arr.windows(2) {
        assert!(w[0]["last_updated"].as_str().unwrap() >= w[1]["last_updated"].as_str().unwrap());
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn dirty_aggregate_is_refreshed_on_read() {
    let pg = start_postgres().await;
    seed_all(&pg.pool).await;
    // Mark ADDRESS_1's test_reference as dirty so the address aggregates read
    // re-runs `refresh_aggregate` before responding.
    let client = pg.pool.get().await.unwrap();
    aleph_ccn::db::accessors::aggregates::mark_aggregate_as_dirty(
        &**client,
        "test_reference",
        ADDRESS_1,
    )
    .await
    .unwrap();
    drop(client);

    let app = aleph_ccn::web::build_router(make_app_state(pg.pool.clone()));
    let uri = format!("/api/v0/aggregates/{ADDRESS_1}.json?with_info=false");
    let (status, body) = get(app, &uri).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    // After refresh, merge result remains the same merged content.
    assert_eq!(
        v["data"]["test_reference"],
        json!({"a": 1, "b": 2, "c": 3, "d": 4})
    );
}
