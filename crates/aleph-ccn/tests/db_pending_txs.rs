//! Ports `tests/db/test_pending_txs.py`.

mod common;

use chrono::{TimeZone, Utc};

use aleph_ccn::db::accessors::pending_txs::{count_pending_txs, get_pending_txs};

use common::fixtures::{insert_chain_tx_row, insert_pending_tx_row};
use common::{start_postgres};

async fn seed_fixture_txs(pg_pool: &aleph_ccn::db::DbPool) {
    let rows: &[(&str, &str, i32, &str)] = &[
        ("1", "ETH", 1200, "2022-01-01"),
        ("2", "SOL", 30000000, "2022-01-02"),
        ("3", "ETH", 1202, "2022-01-03"),
    ];
    for (hash, chain, height, date) in rows {
        let parts: Vec<i32> = date.split('-').map(|s| s.parse().unwrap()).collect();
        let dt = Utc
            .with_ymd_and_hms(parts[0], parts[1] as u32, parts[2] as u32, 0, 0, 0)
            .unwrap();
        insert_chain_tx_row(
            pg_pool,
            hash,
            chain,
            *height,
            dt,
            "0xpub",
            "aleph-offchain",
            1,
            &serde_json::Value::String("c".into()),
        )
        .await
        .unwrap();
        insert_pending_tx_row(pg_pool, hash).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_pending_txs_orders_by_chain_tx_datetime() {
    let pg = start_postgres().await;
    seed_fixture_txs(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();

    let rows = get_pending_txs(&**client, 100).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].tx_hash, "1");
    assert_eq!(rows[1].tx_hash, "2");
    assert_eq!(rows[2].tx_hash, "3");

    let limit_1 = get_pending_txs(&**client, 1).await.unwrap();
    assert_eq!(limit_1.len(), 1);
    assert_eq!(limit_1[0].tx_hash, "1");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn count_pending_txs_returns_total() {
    let pg = start_postgres().await;
    seed_fixture_txs(&pg.pool).await;
    let client = pg.pool.get().await.unwrap();
    let n = count_pending_txs(&**client, None).await.unwrap();
    assert_eq!(n, 3);
}
