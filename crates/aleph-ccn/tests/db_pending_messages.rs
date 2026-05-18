//! Ports `tests/db/test_pending_messages_db.py`.

mod common;

use chrono::{TimeZone, Utc};

use aleph_ccn::db::accessors::pending_messages::{
    count_pending_messages, get_next_pending_messages, insert_pending_message,
};
use aleph_ccn::db::models::pending_messages::PendingMessageDb;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::fixtures::insert_chain_tx_row;
use common::{start_postgres};

fn fixture_pending_messages() -> Vec<(PendingMessageDb, Option<&'static str>, Option<&'static str>)>
{
    // (pending, chain_tx_hash_to_seed, chain_for_tx)
    vec![
        (
            PendingMessageDb {
                id: 0,
                item_hash: "448b3c6f6455e6f4216b01b43522bddc3564a14c04799ed0ce8af4857c7ba15f".into(),
                r#type: MessageType::Forget,
                chain: Chain::Ethereum,
                sender: "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4".into(),
                signature: Some("0x3619c0".into()),
                item_type: ItemType::Inline,
                item_content: Some("{\"hashes\":[]}".into()),
                content: None,
                time: Utc.with_ymd_and_hms(2022, 10, 7, 17, 5, 0).unwrap(),
                channel: None,
                reception_time: Utc.with_ymd_and_hms(2022, 10, 7, 17, 5, 10).unwrap(),
                check_message: true,
                next_attempt: Utc.with_ymd_and_hms(2022, 10, 7, 17, 5, 0).unwrap(),
                retries: 0,
                tx_hash: Some("0x1234".into()),
                fetched: true,
                origin: None,
            },
            Some("0x1234"),
            Some("ETH"),
        ),
        (
            PendingMessageDb {
                id: 0,
                item_hash: "53c2b16aa84b10878982a2920844625546f5db32337ecd9dd15928095a30381c".into(),
                r#type: MessageType::Aggregate,
                chain: Chain::Ethereum,
                sender: "0x51A58800b26AA1451aaA803d1746687cB88E0501".into(),
                signature: Some("0x06b1cf".into()),
                item_type: ItemType::Inline,
                item_content: Some("{}".into()),
                content: None,
                time: Utc.with_ymd_and_hms(2022, 10, 7, 22, 10, 0).unwrap(),
                channel: None,
                reception_time: Utc.with_ymd_and_hms(2022, 10, 7, 22, 10, 10).unwrap(),
                check_message: true,
                next_attempt: Utc.with_ymd_and_hms(2022, 10, 7, 22, 10, 0).unwrap(),
                retries: 3,
                tx_hash: None,
                fetched: true,
                origin: None,
            },
            None,
            None,
        ),
        (
            PendingMessageDb {
                id: 0,
                item_hash: "588ac154509de449b0915844fa1117c72b9136eaaabd078494ea5c5c39cd14b2".into(),
                r#type: MessageType::Store,
                chain: Chain::Sol,
                sender: "BCma9zC6WmtCzS95sPauUGKMQmhAqe6eRboUmRZF1gR3".into(),
                signature: Some("0xabc".into()),
                item_type: ItemType::Inline,
                item_content: Some("{}".into()),
                content: None,
                time: Utc.with_ymd_and_hms(2022, 10, 7, 21, 53, 0).unwrap(),
                channel: None,
                reception_time: Utc.with_ymd_and_hms(2022, 10, 7, 21, 53, 10).unwrap(),
                check_message: true,
                next_attempt: Utc.with_ymd_and_hms(2022, 10, 7, 21, 53, 0).unwrap(),
                retries: 0,
                tx_hash: Some("0x4321".into()),
                fetched: true,
                origin: None,
            },
            Some("0x4321"),
            Some("TEZOS"),
        ),
    ]
}

#[tokio::test]
async fn count_pending_messages_filters_by_chain() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let fixtures = fixture_pending_messages();
    for (pm, tx_hash, chain) in fixtures.iter() {
        if let (Some(h), Some(c)) = (tx_hash, chain) {
            insert_chain_tx_row(
                &pg.pool,
                h,
                c,
                100,
                Utc.with_ymd_and_hms(2022, 10, 7, 17, 4, 0).unwrap(),
                "0xpub",
                "aleph-offchain",
                1,
                &serde_json::Value::String("Qm".into()),
            )
            .await
            .unwrap();
        }
        insert_pending_message(&**client, pm).await.unwrap();
    }

    let count_all = count_pending_messages(&**client, None).await.unwrap();
    assert_eq!(count_all, 3);

    let count_eth = count_pending_messages(&**client, Some(Chain::Ethereum))
        .await
        .unwrap();
    assert_eq!(count_eth, 1);

    let count_tezos = count_pending_messages(&**client, Some(Chain::Tezos))
        .await
        .unwrap();
    assert_eq!(count_tezos, 1);

    let count_sol = count_pending_messages(&**client, Some(Chain::Sol))
        .await
        .unwrap();
    assert_eq!(count_sol, 0);
}

#[tokio::test]
async fn get_pending_messages_orders_and_excludes() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let mut ids: Vec<(i64, &str)> = Vec::new();
    let fixtures = fixture_pending_messages();
    for (pm, tx_hash, chain) in fixtures.iter() {
        if let (Some(h), Some(c)) = (tx_hash, chain) {
            insert_chain_tx_row(
                &pg.pool,
                h,
                c,
                100,
                Utc.with_ymd_and_hms(2022, 10, 7, 17, 4, 0).unwrap(),
                "0xpub",
                "aleph-offchain",
                1,
                &serde_json::Value::String("Qm".into()),
            )
            .await
            .unwrap();
        }
        let assigned = insert_pending_message(&**client, pm).await.unwrap();
        ids.push((assigned, pm.item_hash.as_str()));
    }

    let max_attempt = fixtures
        .iter()
        .map(|(p, _, _)| p.next_attempt)
        .max()
        .unwrap();
    let rows = get_next_pending_messages(&**client, max_attempt, 100, 0, None, None)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    // Ordered by next_attempt ASC. Two rows share 17:05 — tie-breaker is row
    // insertion order via id ASC, but we only assert chronological correctness:
    // every consecutive pair has non-decreasing next_attempt.
    for w in rows.windows(2) {
        assert!(w[0].next_attempt <= w[1].next_attempt);
    }

    // exclude one hash
    let excluded = "588ac154509de449b0915844fa1117c72b9136eaaabd078494ea5c5c39cd14b2";
    let excludes = vec![excluded.to_string()];
    let filtered = get_next_pending_messages(&**client, max_attempt, 100, 0, None, Some(&excludes))
        .await
        .unwrap();
    assert_eq!(filtered.len(), 2);
    assert!(filtered.iter().all(|m| m.item_hash != excluded));
}
