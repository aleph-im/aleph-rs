//! Ports `tests/message_processing/test_process_aggregates.py`.
//!
//! The Rust tests drive [`AggregateMessageHandler::process`] directly,
//! mirroring how the Python pipeline reaches the same code path: build a
//! `MessageDb`, run the handler, assert on `aggregates` and
//! `aggregate_elements` rows.

mod common;

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde_json::{Value, json};

use aleph_ccn::db::accessors::aggregates::{get_aggregate_by_key, get_aggregate_elements};
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::handlers::content::aggregate::AggregateMessageHandler;
use aleph_ccn::handlers::content::content_handler::ContentHandler;
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::message_status::MessageStatus;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::{start_postgres};

fn aggregate_message(item_hash: &str, owner: &str, key: &str, time: f64, content: Value) -> MessageDb {
    let content_dict = json!({
        "address": owner,
        "time": time,
        "key": key,
        "content": content,
    });
    MessageDb {
        item_hash: item_hash.into(),
        r#type: MessageType::Aggregate,
        chain: Chain::Ethereum,
        sender: owner.into(),
        signature: Some("0xsig".into()),
        item_type: ItemType::Inline,
        item_content: Some(serde_json::to_string(&content_dict).unwrap()),
        content: content_dict,
        time: Utc.timestamp_opt(time as i64, 0).unwrap(),
        channel: Some(Channel::from("INTEGRATION_TESTS".to_string())),
        size: 0,
        status_value: MessageStatus::Processed,
        reception_time: Utc.timestamp_opt(time as i64, 0).unwrap(),
        owner: Some(owner.into()),
        content_type: None,
        content_ref: None,
        content_key: Some(key.into()),
        content_item_hash: None,
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    }
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn process_aggregate_first_element_inserts_row_and_element() {
    let pg = start_postgres().await;
    let handler = AggregateMessageHandler::new();
    let owner = "0x720F319A9c3226dCDd7D8C49163D79EDa1084E98";
    let key = "first-aggregate";
    let item_hash = "a87004aa03f8ae63d2c4bbe84b93b9ce70ca6482ce36c82ab0b0f689fc273f34";

    let msg = aggregate_message(item_hash, owner, key, 1_700_000_000.0, json!({"a": 1, "b": "x"}));

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        handler.process(&*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let elements = get_aggregate_elements(&**client, owner, key).await.unwrap();
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0].item_hash, item_hash);
    assert_eq!(elements[0].content, json!({"a": 1, "b": "x"}));

    let agg = get_aggregate_by_key(&**client, owner, key, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(agg.key, key);
    assert_eq!(agg.owner, owner);
    assert_eq!(agg.content, json!({"a": 1, "b": "x"}));
    assert_eq!(agg.last_revision_hash, item_hash);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn process_aggregates_in_order_merges_content() {
    let pg = start_postgres().await;
    let handler = AggregateMessageHandler::new();
    let owner = "0x720F319A9c3226dCDd7D8C49163D79EDa1084E98";
    let key = "test_reference";
    let original = aggregate_message(
        "53c2b16aa84b10878982a2920844625546f5db32337ecd9dd15928095a30381c",
        owner,
        key,
        1_644_857_371.0,
        json!({"a": 1, "c": 2}),
    );
    let update = aggregate_message(
        "0022ed09d16a1c3d6cbb3c7e2645657ebaa0382eba65be06264b106f528b85bf",
        owner,
        key,
        1_644_857_704.0,
        json!({"c": 3, "d": 4}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        handler.process(&*tx, &[original.clone()]).await.unwrap();
        handler.process(&*tx, &[update.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let agg = get_aggregate_by_key(&**client, owner, key, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(agg.content, json!({"a": 1, "c": 3, "d": 4}));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn process_aggregates_reverse_order_still_merges() {
    let pg = start_postgres().await;
    let handler = AggregateMessageHandler::new();
    let owner = "0x720F319A9c3226dCDd7D8C49163D79EDa1084E98";
    let key = "test_reverse";
    let original = aggregate_message(
        "53c2b16aa84b10878982a2920844625546f5db32337ecd9dd15928095a30381c",
        owner,
        key,
        1_644_857_371.0,
        json!({"a": 1, "c": 2}),
    );
    let update = aggregate_message(
        "0022ed09d16a1c3d6cbb3c7e2645657ebaa0382eba65be06264b106f528b85bf",
        owner,
        key,
        1_644_857_704.0,
        json!({"c": 3, "d": 4}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        // Insert the update first, then the older one — the handler should
        // still produce the same merged content via the "out of order
        // refresh" path.
        handler.process(&*tx, &[update.clone()]).await.unwrap();
        handler.process(&*tx, &[original.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let agg = get_aggregate_by_key(&**client, owner, key, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(agg.content, json!({"a": 1, "c": 3, "d": 4}));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn delete_aggregate_one_element_removes_row_and_element() {
    let pg = start_postgres().await;
    let handler = AggregateMessageHandler::new();
    let owner = "0xme";
    let key = "my-aggregate";
    let msg = aggregate_message(
        "d73d50b2d2c670d4c6c8e03ad0e4e2145642375f92784c68539a3400e0e4e242",
        owner,
        key,
        1_672_531_200.0,
        json!({"Hello": "world"}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        handler.process(&*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }
    // Sanity: aggregate exists.
    {
        let client = pg.pool.get().await.unwrap();
        assert!(get_aggregate_by_key(&**client, owner, key, true)
            .await
            .unwrap()
            .is_some());
    }

    // Forget the single element. The handler deletes the aggregate, removes
    // the element, then refreshes (now a no-op).
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        let _ = ContentHandler::forget_message(&handler, &*tx, &msg)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    assert!(get_aggregate_by_key(&**client, owner, key, true)
        .await
        .unwrap()
        .is_none());
    let elements = get_aggregate_elements(&**client, owner, key).await.unwrap();
    assert!(elements.is_empty());
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn delete_aggregate_first_element_keeps_last() {
    let pg = start_postgres().await;
    let handler = AggregateMessageHandler::new();
    let owner = "0xme";
    let key = "my-aggregate-two";

    let first = aggregate_message(
        "d73d50b2d2c670d4c6c8e03ad0e4e2145642375f92784c68539a3400e0e4e242",
        owner,
        key,
        1_672_531_200.0,
        json!({"Hello": "world"}),
    );
    let last = aggregate_message(
        "37a2ca64f9fdd35a2aac386003179c594b3f0963e064c0663ff84368bc3c1bb5",
        owner,
        key,
        1_672_617_600.0,
        json!({"Goodbye": "blue sky"}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        handler.process(&*tx, &[first.clone()]).await.unwrap();
        handler.process(&*tx, &[last.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    // Forget the first element.
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        let _ = ContentHandler::forget_message(&handler, &*tx, &first)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let agg = get_aggregate_by_key(&**client, owner, key, true)
        .await
        .unwrap()
        .unwrap();
    // The element kept is `last`.
    assert_eq!(agg.last_revision_hash, last.item_hash);
    assert_eq!(agg.content, json!({"Goodbye": "blue sky"}));

    let elements = get_aggregate_elements(&**client, owner, key).await.unwrap();
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0].item_hash, last.item_hash);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn delete_aggregate_last_element_keeps_first() {
    let pg = start_postgres().await;
    let handler = AggregateMessageHandler::new();
    let owner = "0xme";
    let key = "my-aggregate-three";

    let first = aggregate_message(
        "d73d50b2d2c670d4c6c8e03ad0e4e2145642375f92784c68539a3400e0e4e242",
        owner,
        key,
        1_672_531_200.0,
        json!({"Hello": "world"}),
    );
    let last = aggregate_message(
        "37a2ca64f9fdd35a2aac386003179c594b3f0963e064c0663ff84368bc3c1bb5",
        owner,
        key,
        1_672_617_600.0,
        json!({"Goodbye": "blue sky"}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        handler.process(&*tx, &[first.clone()]).await.unwrap();
        handler.process(&*tx, &[last.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        let _ = ContentHandler::forget_message(&handler, &*tx, &last)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let agg = get_aggregate_by_key(&**client, owner, key, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(agg.last_revision_hash, first.item_hash);
    assert_eq!(agg.content, json!({"Hello": "world"}));

    let elements = get_aggregate_elements(&**client, owner, key).await.unwrap();
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0].item_hash, first.item_hash);
}

#[test]
fn aggregate_handler_default_constructible() {
    // Smoke test: matches the Python `AggregateMessageHandler()` instantiation
    // sanity path inside the unit tests.
    let _ = Arc::new(AggregateMessageHandler::default());
}
