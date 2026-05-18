//! Ports `tests/db/test_chains.py`.

mod common;

use chrono::{TimeZone, Utc};

use aleph_ccn::db::accessors::chains::{
    IndexerMultiRange, get_chain_sync_status, get_indexer_multirange, get_last_height,
    get_missing_indexer_datetime_multirange, update_indexer_multirange,
    upsert_chain_sync_status,
};
use aleph_ccn::toolkit::range::{MultiRange, Range};
use aleph_ccn::types::chain_sync::ChainEventType;
use aleph_types::chain::Chain;

use common::{start_postgres};

#[tokio::test]
async fn get_last_height_returns_inserted_value() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let updated = Utc.with_ymd_and_hms(2022, 10, 1, 0, 0, 0).unwrap();
    upsert_chain_sync_status(&**client, Chain::Ethereum, ChainEventType::Sync, 123, updated)
        .await
        .unwrap();

    let height = get_last_height(&**client, Chain::Ethereum, ChainEventType::Sync)
        .await
        .unwrap();
    assert_eq!(height, Some(123));
}

#[tokio::test]
async fn get_last_height_none_when_no_data() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let height = get_last_height(&**client, Chain::Nuls2, ChainEventType::Sync)
        .await
        .unwrap();
    assert!(height.is_none());
}

#[tokio::test]
async fn upsert_chain_sync_status_inserts_row() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let updated = Utc.with_ymd_and_hms(2022, 11, 1, 0, 0, 0).unwrap();
    upsert_chain_sync_status(&**client, Chain::Ethereum, ChainEventType::Sync, 10, updated)
        .await
        .unwrap();

    let row = get_chain_sync_status(&**client, Chain::Ethereum, ChainEventType::Sync)
        .await
        .unwrap()
        .expect("row");
    assert_eq!(row.chain, Chain::Ethereum);
    assert_eq!(row.r#type, ChainEventType::Sync);
    assert_eq!(row.height, 10);
    assert_eq!(row.last_update, updated);
}

#[tokio::test]
async fn upsert_chain_sync_status_replaces_row() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let updated_old = Utc.with_ymd_and_hms(2023, 2, 6, 0, 0, 0).unwrap();
    upsert_chain_sync_status(&**client, Chain::Tezos, ChainEventType::Sync, 1000, updated_old)
        .await
        .unwrap();

    let updated_new = Utc.with_ymd_and_hms(2023, 2, 7, 0, 0, 0).unwrap();
    upsert_chain_sync_status(&**client, Chain::Tezos, ChainEventType::Sync, 1001, updated_new)
        .await
        .unwrap();

    let row = get_chain_sync_status(&**client, Chain::Tezos, ChainEventType::Sync)
        .await
        .unwrap()
        .expect("row");
    assert_eq!(row.height, 1001);
    assert_eq!(row.last_update, updated_new);
}

fn build_fixture_multirange() -> IndexerMultiRange {
    let mr = MultiRange::new(vec![
        Range::new(
            Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2021, 1, 1, 0, 0, 0).unwrap(),
            true,
            true,
        )
        .unwrap(),
        Range::new(
            Utc.with_ymd_and_hms(2021, 6, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
            true,
            true,
        )
        .unwrap(),
    ]);
    IndexerMultiRange {
        chain: Chain::Ethereum,
        event_type: ChainEventType::Sync,
        datetime_multirange: mr,
    }
}

#[tokio::test]
async fn get_indexer_multirange_round_trips() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let fixture = build_fixture_multirange();
    update_indexer_multirange(&**client, &fixture).await.unwrap();

    let db = get_indexer_multirange(&**client, fixture.chain.clone(), fixture.event_type)
        .await
        .unwrap();
    assert_eq!(db.chain, fixture.chain);
    assert_eq!(db.event_type, fixture.event_type);
    assert_eq!(db.datetime_multirange, fixture.datetime_multirange);
}

#[tokio::test]
async fn update_indexer_multirange_overwrites_existing() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();

    // First write
    let initial = build_fixture_multirange();
    update_indexer_multirange(&**client, &initial).await.unwrap();
    // Replace with a single-range version.
    let replaced = IndexerMultiRange {
        chain: Chain::Ethereum,
        event_type: ChainEventType::Sync,
        datetime_multirange: MultiRange::new(vec![Range::new(
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            true,
            false,
        )
        .unwrap()]),
    };
    update_indexer_multirange(&**client, &replaced).await.unwrap();
    let db = get_indexer_multirange(&**client, Chain::Ethereum, ChainEventType::Sync)
        .await
        .unwrap();
    assert_eq!(db.datetime_multirange, replaced.datetime_multirange);
}

#[tokio::test]
async fn get_missing_indexer_datetime_multirange_returns_gaps() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();

    let fixture = build_fixture_multirange();
    update_indexer_multirange(&**client, &fixture).await.unwrap();

    let wider = MultiRange::new(vec![Range::new(
        Utc.with_ymd_and_hms(2019, 1, 1, 0, 0, 0).unwrap(),
        Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
        true,
        true,
    )
    .unwrap()]);

    let missing = get_missing_indexer_datetime_multirange(
        &**client,
        Chain::Ethereum,
        ChainEventType::Sync,
        &wider,
    )
    .await
    .unwrap();
    // 3 gaps, matching the Python expectation.
    assert_eq!(missing.len(), 3);
}
