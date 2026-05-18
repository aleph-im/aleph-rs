//! Ports `tests/db/test_messages.py`.

mod common;

use chrono::{TimeZone, Utc};
use serde_json::json;

use aleph_ccn::db::accessors::messages::{
    append_to_forgotten_by, count_matching_messages, forget_message, get_distinct_channels,
    get_forgotten_message, get_message_by_item_hash, get_message_status, get_unconfirmed_messages,
    message_exists, upsert_confirmation, upsert_message, upsert_message_status, MessageFilters,
};
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::message_status::MessageStatus;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::fixtures::{insert_chain_tx_row, insert_confirmation_row};
use common::{insert_processed_message, start_postgres};

fn fixture_message() -> MessageDb {
    let sender = "0x51A58800b26AA1451aaA803d1746687cB88E0500";
    let content = json!({
        "address": sender,
        "key": "my-aggregate",
        "time": 1664999873,
        "content": {"easy": "as", "a-b": "c"},
    });
    MessageDb {
        item_hash: "aea68aac5f4dc6e6b813fc5de9e6c17d3ef1b03e77eace15398405260baf3ce4".into(),
        chain: Chain::Ethereum,
        sender: sender.into(),
        signature: Some("0xfeed".into()),
        r#type: MessageType::Aggregate,
        item_content: Some(serde_json::to_string(&content).unwrap()),
        content,
        item_type: ItemType::Inline,
        size: 2000,
        time: Utc.with_ymd_and_hms(2022, 10, 5, 17, 17, 52).unwrap(),
        channel: Some(Channel::from("CHANEL-N5".to_string())),
        status_value: MessageStatus::Processed,
        reception_time: Utc.with_ymd_and_hms(2022, 10, 5, 17, 17, 52).unwrap(),
        owner: Some(sender.into()),
        content_type: None,
        content_ref: None,
        content_key: Some("my-aggregate".into()),
        content_item_hash: None,
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_message_returns_inserted_row() {
    let pg = start_postgres().await;
    let m = fixture_message();
    insert_processed_message(&pg.pool, m.clone()).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    let got = get_message_by_item_hash(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("present");
    assert_eq!(got.item_hash, m.item_hash);
    assert_eq!(got.sender, m.sender);
    assert_eq!(got.r#type, m.r#type);
    assert_eq!(got.channel, m.channel);
    assert!(got.first_confirmed_at.is_none());
    assert!(got.first_confirmed_height.is_none());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn message_exists_returns_expected_bool() {
    let pg = start_postgres().await;
    let m = fixture_message();
    let client = pg.pool.get().await.unwrap();
    assert!(!message_exists(&**client, &m.item_hash).await.unwrap());
    drop(client);
    insert_processed_message(&pg.pool, m.clone()).await.unwrap();
    let client = pg.pool.get().await.unwrap();
    assert!(message_exists(&**client, &m.item_hash).await.unwrap());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn message_count_returns_one_after_insert() {
    let pg = start_postgres().await;
    let m = fixture_message();
    insert_processed_message(&pg.pool, m).await.unwrap();
    let client = pg.pool.get().await.unwrap();
    let count = count_matching_messages(&**client, &MessageFilters::new())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn upsert_confirmation_populates_denormalized_columns() {
    let pg = start_postgres().await;
    let m = fixture_message();
    insert_processed_message(&pg.pool, m.clone()).await.unwrap();
    let tx_datetime = Utc.with_ymd_and_hms(2022, 10, 1, 0, 0, 0).unwrap();
    insert_chain_tx_row(
        &pg.pool,
        "0xdeadbeef",
        "ETH",
        1000,
        tx_datetime,
        "0xabadbabe",
        "aleph-offchain",
        1,
        &serde_json::Value::String("Qm".into()),
    )
    .await
    .unwrap();

    let client = pg.pool.get().await.unwrap();
    upsert_confirmation(&**client, &m.item_hash, "0xdeadbeef")
        .await
        .unwrap();

    let row = get_message_by_item_hash(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("row");
    assert_eq!(row.first_confirmed_at, Some(tx_datetime));
    assert_eq!(row.first_confirmed_height, Some(1000));

    // Upsert is idempotent.
    upsert_confirmation(&**client, &m.item_hash, "0xdeadbeef")
        .await
        .unwrap();
    let n: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM message_confirmations WHERE item_hash = $1",
            &[&m.item_hash],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 1);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn confirmation_trigger_keeps_earliest_for_first_confirmed_columns() {
    let pg = start_postgres().await;
    let m = fixture_message();
    insert_processed_message(&pg.pool, m.clone()).await.unwrap();
    let early = Utc.with_ymd_and_hms(2022, 9, 1, 0, 0, 0).unwrap();
    let late = Utc.with_ymd_and_hms(2022, 11, 1, 0, 0, 0).unwrap();
    insert_chain_tx_row(
        &pg.pool,
        "0xearly",
        "ETH",
        500,
        early,
        "0xpub1",
        "aleph-offchain",
        1,
        &serde_json::Value::String("c".into()),
    )
    .await
    .unwrap();
    insert_chain_tx_row(
        &pg.pool,
        "0xlate",
        "ETH",
        900,
        late,
        "0xpub2",
        "aleph-offchain",
        1,
        &serde_json::Value::String("c".into()),
    )
    .await
    .unwrap();

    let client = pg.pool.get().await.unwrap();
    upsert_confirmation(&**client, &m.item_hash, "0xlate")
        .await
        .unwrap();
    let row = get_message_by_item_hash(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("row");
    assert_eq!(row.first_confirmed_at, Some(late));

    upsert_confirmation(&**client, &m.item_hash, "0xearly")
        .await
        .unwrap();
    let row = get_message_by_item_hash(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("row");
    assert_eq!(row.first_confirmed_at, Some(early));
    assert_eq!(row.first_confirmed_height, Some(500));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn upsert_message_keeps_earliest_time() {
    let pg = start_postgres().await;
    let m = fixture_message();
    insert_processed_message(&pg.pool, m.clone()).await.unwrap();

    let mut earlier = m.clone();
    earlier.time = m.time - chrono::Duration::seconds(1);
    let client = pg.pool.get().await.unwrap();
    upsert_message(&**client, &earlier).await.unwrap();

    let row = get_message_by_item_hash(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("row");
    assert_eq!(row.time, earlier.time);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_unconfirmed_messages_returns_signature_present_and_no_confirmation() {
    let pg = start_postgres().await;
    let m = fixture_message();
    insert_processed_message(&pg.pool, m.clone()).await.unwrap();

    let client = pg.pool.get().await.unwrap();
    let rows = get_unconfirmed_messages(&**client, 100, 0).await.unwrap();
    assert_eq!(rows.len(), 1);

    // After confirmation, none.
    let tx_datetime = Utc.with_ymd_and_hms(2022, 10, 6, 0, 0, 0).unwrap();
    insert_chain_tx_row(
        &pg.pool,
        "1234",
        "SOL",
        8000,
        tx_datetime,
        "0xabadbabe",
        "aleph-offchain",
        1,
        &serde_json::Value::String("c".into()),
    )
    .await
    .unwrap();
    insert_confirmation_row(&pg.pool, &m.item_hash, "1234")
        .await
        .unwrap();
    let rows = get_unconfirmed_messages(&**client, 100, 0).await.unwrap();
    assert!(rows.is_empty());

    let zero = get_unconfirmed_messages(&**client, 0, 0).await.unwrap();
    assert!(zero.is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_unconfirmed_messages_skips_trusted_messages_without_signature() {
    let pg = start_postgres().await;
    let mut m = fixture_message();
    m.signature = None;
    insert_processed_message(&pg.pool, m).await.unwrap();
    let client = pg.pool.get().await.unwrap();
    let rows = get_unconfirmed_messages(&**client, 100, 0).await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn get_distinct_channels_returns_single_channel() {
    let pg = start_postgres().await;
    let m = fixture_message();
    insert_processed_message(&pg.pool, m.clone()).await.unwrap();
    let client = pg.pool.get().await.unwrap();
    let channels = get_distinct_channels(&**client).await.unwrap();
    assert_eq!(channels, vec![m.channel.clone()]);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn forget_message_marks_status_and_records_metadata() {
    let pg = start_postgres().await;
    let m = fixture_message();
    insert_processed_message(&pg.pool, m.clone()).await.unwrap();

    let forget_hash = "d06251c954d4c75476c749e80b8f2a4962d20282b28b3e237e30b0a76157df2d";
    let client = pg.pool.get().await.unwrap();
    forget_message(&**client, &m.item_hash, forget_hash)
        .await
        .unwrap();

    let status = get_message_status(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("status");
    assert_eq!(status.status, MessageStatus::Forgotten);

    let row = get_message_by_item_hash(&**client, &m.item_hash).await.unwrap();
    assert!(row.is_none());

    let fg = get_forgotten_message(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("forgotten");
    assert_eq!(fg.item_hash, m.item_hash);
    assert_eq!(fg.forgotten_by, vec![forget_hash.to_string()]);

    let new_hash = "2aa1f44199181110e0c6b79ccc5e40ceaf20eac791dcfcd1b4f8f2f32b2d8502";
    append_to_forgotten_by(&**client, &m.item_hash, new_hash)
        .await
        .unwrap();
    let fg = get_forgotten_message(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("forgotten");
    assert_eq!(
        fg.forgotten_by,
        vec![forget_hash.to_string(), new_hash.to_string()]
    );
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn forget_message_cascades_confirmations() {
    let pg = start_postgres().await;
    let m = fixture_message();
    insert_processed_message(&pg.pool, m.clone()).await.unwrap();
    insert_chain_tx_row(
        &pg.pool,
        "0xdeadbeef",
        "ETH",
        1000,
        Utc.with_ymd_and_hms(2022, 10, 1, 0, 0, 0).unwrap(),
        "0xpub",
        "aleph-offchain",
        1,
        &serde_json::Value::String("c".into()),
    )
    .await
    .unwrap();
    insert_confirmation_row(&pg.pool, &m.item_hash, "0xdeadbeef")
        .await
        .unwrap();

    let forget_hash = "d06251c954d4c75476c749e80b8f2a4962d20282b28b3e237e30b0a76157df2d";
    let client = pg.pool.get().await.unwrap();
    let pre: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM message_confirmations WHERE item_hash = $1",
            &[&m.item_hash],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(pre, 1);

    forget_message(&**client, &m.item_hash, forget_hash)
        .await
        .unwrap();

    let row = get_message_by_item_hash(&**client, &m.item_hash).await.unwrap();
    assert!(row.is_none());
    let post: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM message_confirmations WHERE item_hash = $1",
            &[&m.item_hash],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(post, 0);

    let fg = get_forgotten_message(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("forgotten");
    assert_eq!(fg.forgotten_by, vec![forget_hash.to_string()]);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn payment_type_persisted_after_upsert() {
    let pg = start_postgres().await;
    // Build a STORE message with payment.type=credit and check that
    // upsert_message+denormalization populates payment_type='credit'.
    let item_hash = "b81dcc3aa4827c693bc65d8ca1041387960cb4f4323e8be1984b604748ff02a8";
    let sender = "0xB6B5358493AF8159B17506C5cC85df69193444BC";
    let content = json!({
        "address": sender,
        "time": 1771337941.575_f64,
        "item_type": "ipfs",
        "item_hash": "QmePTEmasKHQQYdK3maUhrMJ7nxftSTFKeAGP7JweeiNrf",
        "payment": {"chain": "ETH", "type": "credit"},
    });
    let m = MessageDb {
        item_hash: item_hash.into(),
        r#type: MessageType::Store,
        chain: Chain::Ethereum,
        sender: sender.into(),
        signature: Some("0x54c".into()),
        item_type: ItemType::Inline,
        item_content: Some(serde_json::to_string(&content).unwrap()),
        content,
        time: Utc.with_ymd_and_hms(2026, 2, 17, 0, 0, 0).unwrap(),
        channel: Some(Channel::from("ALEPH-CLOUDSOLUTIONS".to_string())),
        size: 189,
        status_value: MessageStatus::Processed,
        reception_time: Utc.with_ymd_and_hms(2026, 2, 17, 0, 0, 0).unwrap(),
        owner: Some(sender.into()),
        content_type: None,
        content_ref: None,
        content_key: None,
        content_item_hash: Some("QmePTEmasKHQQYdK3maUhrMJ7nxftSTFKeAGP7JweeiNrf".into()),
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: Some("credit".into()),
        tags: None,
    };
    let client = pg.pool.get().await.unwrap();
    upsert_message(&**client, &m).await.unwrap();
    upsert_message_status(&**client, &m.item_hash, MessageStatus::Processed, m.reception_time, None)
        .await
        .unwrap();
    let fetched = get_message_by_item_hash(&**client, &m.item_hash)
        .await
        .unwrap()
        .expect("row");
    assert_eq!(fetched.payment_type.as_deref(), Some("credit"));
}
