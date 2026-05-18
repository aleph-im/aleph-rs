//! Ports `tests/db/test_peers.py`.

mod common;

use chrono::{TimeZone, Utc};

use aleph_ccn::db::accessors::peers::{get_all_addresses_by_peer_type, upsert_peer};
use aleph_ccn::db::models::peers::PeerType;

use common::{start_postgres};

#[tokio::test]
async fn get_all_addresses_by_peer_type_returns_correct_addresses() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let peer_id = "some-peer-id";
    let last_seen = Utc.with_ymd_and_hms(2022, 10, 1, 0, 0, 0).unwrap();

    // Insert all three types with the same peer_id.
    for (ptype, addr) in [
        (PeerType::Http, "http://127.0.0.1:4024"),
        (PeerType::P2p, "/ip4/127.0.0.1/tcp/4025"),
        (PeerType::Ipfs, "http://127.0.0.1:4001"),
    ] {
        upsert_peer(&**client, peer_id, ptype, addr, PeerType::P2p, Some(last_seen))
            .await
            .unwrap();
    }

    let http = get_all_addresses_by_peer_type(&**client, PeerType::Http, None)
        .await
        .unwrap();
    let p2p = get_all_addresses_by_peer_type(&**client, PeerType::P2p, None)
        .await
        .unwrap();
    let ipfs = get_all_addresses_by_peer_type(&**client, PeerType::Ipfs, None)
        .await
        .unwrap();

    assert_eq!(http, vec!["http://127.0.0.1:4024".to_string()]);
    assert_eq!(p2p, vec!["/ip4/127.0.0.1/tcp/4025".to_string()]);
    assert_eq!(ipfs, vec!["http://127.0.0.1:4001".to_string()]);

    // last_seen >= filter
    let recent = get_all_addresses_by_peer_type(&**client, PeerType::P2p, Some(last_seen))
        .await
        .unwrap();
    assert_eq!(recent, vec!["/ip4/127.0.0.1/tcp/4025".to_string()]);
    let old = get_all_addresses_by_peer_type(
        &**client,
        PeerType::P2p,
        Some(last_seen + chrono::Duration::days(1)),
    )
    .await
    .unwrap();
    assert!(old.is_empty());
}

#[tokio::test]
async fn get_all_addresses_by_peer_type_no_match_each_type() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    for ptype in [PeerType::Http, PeerType::P2p, PeerType::Ipfs] {
        let rows = get_all_addresses_by_peer_type(&**client, ptype, None)
            .await
            .unwrap();
        assert!(rows.is_empty(), "expected empty for {ptype:?}");
    }
}

#[tokio::test]
async fn upsert_peer_insert() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let last_seen = Utc.with_ymd_and_hms(2022, 10, 1, 0, 0, 0).unwrap();
    upsert_peer(
        &**client,
        "peer-id",
        PeerType::Http,
        "http://127.0.0.1:4024",
        PeerType::Ipfs,
        Some(last_seen),
    )
    .await
    .unwrap();
    let row = client
        .query_one(
            "SELECT peer_id, peer_type, address, source, last_seen FROM peers \
             WHERE peer_id = $1 AND peer_type = $2",
            &[&"peer-id", &"HTTP"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("address"), "http://127.0.0.1:4024");
    assert_eq!(row.get::<_, String>("source"), "IPFS");
}

#[tokio::test]
async fn upsert_peer_replace() {
    let pg = start_postgres().await;
    let client = pg.pool.get().await.unwrap();
    let last_seen = Utc.with_ymd_and_hms(2022, 10, 1, 0, 0, 0).unwrap();
    upsert_peer(
        &**client,
        "peer-id",
        PeerType::Http,
        "http://127.0.0.1:4024",
        PeerType::P2p,
        Some(last_seen),
    )
    .await
    .unwrap();

    let new_last = Utc.with_ymd_and_hms(2022, 10, 2, 0, 0, 0).unwrap();
    upsert_peer(
        &**client,
        "peer-id",
        PeerType::Http,
        "http://0.0.0.0:4024",
        PeerType::Ipfs,
        Some(new_last),
    )
    .await
    .unwrap();

    let row = client
        .query_one(
            "SELECT peer_id, peer_type, address, source, last_seen FROM peers \
             WHERE peer_id = $1 AND peer_type = $2",
            &[&"peer-id", &"HTTP"],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("address"), "http://0.0.0.0:4024");
    assert_eq!(row.get::<_, String>("source"), "IPFS");
    assert_eq!(row.get::<_, chrono::DateTime<Utc>>("last_seen"), new_last);
}
