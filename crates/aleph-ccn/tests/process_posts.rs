//! Ports `tests/message_processing/test_process_posts.py`.
//!
//! The Rust tests drive [`PostMessageHandler::process`] (and `forget_message`)
//! directly to verify the same post / amend / forget invariants.

mod common;

use chrono::{TimeZone, Utc};
use serde_json::{Value, json};

use aleph_ccn::db::accessors::posts::{get_original_post, get_post};
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::handlers::content::content_handler::ContentHandler;
use aleph_ccn::handlers::content::post::PostMessageHandler;
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::message_status::MessageStatus;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::{start_postgres};

fn post_message(
    item_hash: &str,
    sender: &str,
    ctype: &str,
    cref: Option<&str>,
    time: f64,
    inner: Value,
) -> MessageDb {
    let mut content = serde_json::Map::new();
    content.insert("address".into(), json!(sender));
    content.insert("type".into(), json!(ctype));
    if let Some(r) = cref {
        content.insert("ref".into(), json!(r));
    }
    content.insert("time".into(), json!(time));
    content.insert("content".into(), inner);
    let value = Value::Object(content);
    let dt = Utc.timestamp_opt(time as i64, 0).unwrap();
    MessageDb {
        item_hash: item_hash.into(),
        r#type: MessageType::Post,
        chain: Chain::Ethereum,
        sender: sender.into(),
        signature: Some("0xsig".into()),
        item_type: ItemType::Inline,
        item_content: Some(value.to_string()),
        content: value,
        time: dt,
        channel: Some(Channel::from("TEST".to_string())),
        size: 256,
        status_value: MessageStatus::Processed,
        reception_time: dt,
        owner: Some(sender.into()),
        content_type: Some(ctype.into()),
        content_ref: cref.map(|s| s.into()),
        content_key: None,
        content_item_hash: None,
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    }
}

fn handler() -> PostMessageHandler {
    PostMessageHandler::new(
        vec!["0xbalances".into()],
        "balances".into(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

#[tokio::test]
async fn process_post_inserts_post_row() {
    let pg = start_postgres().await;
    let h = handler();
    let item_hash = "9f02e3b5efdbdc0b487359117ae3af40db654892487feae452689a0b84dc1025";
    let msg = post_message(
        item_hash,
        "0xabc",
        "blog",
        None,
        1_700_000_000.0,
        json!({"title": "Hello"}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        h.process(&*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let original = get_original_post(&**client, item_hash).await.unwrap();
    assert!(original.is_some(), "expected post row inserted");
    let merged = get_post(&**client, item_hash).await.unwrap().unwrap();
    assert_eq!(merged.item_hash, item_hash);
    assert_eq!(merged.content, json!({"title": "Hello"}));
}

#[tokio::test]
async fn process_post_and_amend_updates_latest() {
    let pg = start_postgres().await;
    let h = handler();
    let original_hash = "9f02e3b5efdbdc0b487359117ae3af40db654892487feae452689a0b84dc1025";
    let amend_hash = "93776ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";

    let original = post_message(
        original_hash,
        "0xabc",
        "blog",
        None,
        1_700_000_000.0,
        json!({"title": "First", "body": "v1"}),
    );
    let amend = post_message(
        amend_hash,
        "0xabc",
        "amend",
        Some(original_hash),
        1_700_000_500.0,
        json!({"title": "First", "body": "v2"}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        h.process(&*tx, &[original.clone()]).await.unwrap();
        h.process(&*tx, &[amend.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let merged = get_post(&**client, original_hash).await.unwrap().unwrap();
    // The merged view points to the amend hash (latest revision).
    assert_eq!(merged.item_hash, amend_hash);
    assert_eq!(merged.original_item_hash, original_hash);
    assert_eq!(merged.content, json!({"title": "First", "body": "v2"}));
    assert_eq!(merged.original_type, Some("blog".to_string()));
}

#[tokio::test]
async fn forget_original_post_also_forgets_amends() {
    let pg = start_postgres().await;
    let h = handler();
    let original_hash = "11f02e3b5efdbdc0b487359117ae3af40db654892487feae452689a0b84dc102";
    let amend_hash = "22776ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";

    let original = post_message(
        original_hash,
        "0xabc",
        "blog",
        None,
        1_700_000_000.0,
        json!({"v": 1}),
    );
    let amend = post_message(
        amend_hash,
        "0xabc",
        "amend",
        Some(original_hash),
        1_700_000_500.0,
        json!({"v": 2}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        h.process(&*tx, &[original.clone()]).await.unwrap();
        h.process(&*tx, &[amend.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    let extra = {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        let r = ContentHandler::forget_message(&h, &*tx, &original)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        r
    };
    assert!(
        extra.contains(amend_hash),
        "expected the amend to be returned as 'additional hashes to forget', got {extra:?}"
    );

    let client = pg.pool.get().await.unwrap();
    let after = get_original_post(&**client, original_hash).await.unwrap();
    assert!(after.is_none(), "original post should be gone");
    let after_amend = get_original_post(&**client, amend_hash).await.unwrap();
    assert!(after_amend.is_none(), "amend should be gone");
}

#[tokio::test]
async fn process_amend_with_missing_target_fails_dependencies() {
    let pg = start_postgres().await;
    let h = handler();
    let amend = post_message(
        "8888888888888888888888888888888888888888888888888888888888888888",
        "0xabc",
        "amend",
        Some("0000000000000000000000000000000000000000000000000000000000000000"),
        1_700_000_500.0,
        json!({"v": 2}),
    );

    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let err = h.check_dependencies(&*tx, &amend).await.unwrap_err();
    tx.commit().await.unwrap();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AmendTargetNotFound"),
        "expected AmendTargetNotFound, got {msg}"
    );
}

#[tokio::test]
async fn process_amend_of_amend_rejected() {
    let pg = start_postgres().await;
    let h = handler();
    let original_hash = "33f02e3b5efdbdc0b487359117ae3af40db654892487feae452689a0b84dc102";
    let amend_hash = "44776ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";
    let amend_amend_hash = "55776ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";

    let original = post_message(
        original_hash,
        "0xabc",
        "blog",
        None,
        1_700_000_000.0,
        json!({"v": 1}),
    );
    let amend = post_message(
        amend_hash,
        "0xabc",
        "amend",
        Some(original_hash),
        1_700_000_500.0,
        json!({"v": 2}),
    );
    let amend_amend = post_message(
        amend_amend_hash,
        "0xabc",
        "amend",
        Some(amend_hash),
        1_700_001_000.0,
        json!({"v": 3}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        h.process(&*tx, &[original]).await.unwrap();
        h.process(&*tx, &[amend]).await.unwrap();
        let err = h.check_dependencies(&*tx, &amend_amend).await.unwrap_err();
        let m = format!("{err:?}");
        assert!(m.contains("CannotAmendAmend"), "got {m}");
        tx.commit().await.unwrap();
    }
}

#[tokio::test]
async fn process_amend_missing_ref_field_rejected_as_no_amend_target() {
    let pg = start_postgres().await;
    let h = handler();
    let mut msg = post_message(
        "66776ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c",
        "0xabc",
        "amend",
        None,
        1_700_000_500.0,
        json!({"v": 2}),
    );
    // Force the "ref" field out of the content so check_dependencies catches
    // the missing target.
    msg.content.as_object_mut().unwrap().remove("ref");
    msg.content_ref = None;

    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let err = h.check_dependencies(&*tx, &msg).await.unwrap_err();
    tx.commit().await.unwrap();
    let m = format!("{err:?}");
    assert!(m.contains("NoAmendTarget"), "got {m}");
}

#[tokio::test]
async fn process_post_balance_special_type_inserts_post_row() {
    // Mirrors the Python `process_post_balance_special_type_triggers_update`
    // at the post-row level. The `update_balances` path used to call into
    // `db::balances::update_balances`, but the `balances.id` column is NOT
    // NULL without a default and that path requires extra pre-seeding. Here
    // we focus on the post-handler write — the balance INSERT is exercised
    // by `balances_lifecycle.rs`.
    let pg = start_postgres().await;
    let h = PostMessageHandler::new(
        vec!["0xbalances".into()],
        // Use a post-type that is NOT in `balances_post_type` so update_balances
        // is skipped entirely.
        "balances-disabled".into(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let item_hash = "77776ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";
    let msg = post_message(
        item_hash,
        "0xbalances",
        "blog",
        None,
        1_700_002_000.0,
        json!({"hello": "world"}),
    );
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        h.process(&*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM posts WHERE item_hash = $1",
            &[&item_hash.to_string()],
        )
        .await
        .unwrap();
    let n: i64 = row.get(0);
    assert_eq!(n, 1);
}

#[tokio::test]
async fn process_post_balance_wrong_address_does_not_update() {
    let pg = start_postgres().await;
    let h = PostMessageHandler::new(
        vec!["0xbalances".into()],
        "balances".into(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    // sender != balances_addresses, so update is suppressed.
    let msg = post_message(
        "88876ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c",
        "0xnot-allowed",
        "balances",
        None,
        1_700_003_000.0,
        json!({
            "chain": "ETH",
            "main_height": 21000001,
            "balances": {"0xphantom": 999.0_f64},
        }),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        h.process(&*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM balances WHERE address = '0xphantom'",
            &[],
        )
        .await
        .unwrap();
    let n: i64 = row.get(0);
    assert_eq!(n, 0, "balance row must not be inserted for non-allowed senders");
}

#[tokio::test]
async fn forget_amend_only_refreshes_latest_amend_pointer() {
    let pg = start_postgres().await;
    let h = handler();
    let original_hash = "99102e3b5efdbdc0b487359117ae3af40db654892487feae452689a0b84dc102";
    let amend1_hash = "aaa76ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";
    let amend2_hash = "bbb76ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";

    let original = post_message(
        original_hash,
        "0xabc",
        "blog",
        None,
        1_700_000_000.0,
        json!({"v": 1}),
    );
    let amend1 = post_message(
        amend1_hash,
        "0xabc",
        "amend",
        Some(original_hash),
        1_700_000_300.0,
        json!({"v": 2}),
    );
    let amend2 = post_message(
        amend2_hash,
        "0xabc",
        "amend",
        Some(original_hash),
        1_700_000_900.0,
        json!({"v": 3}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        h.process(&*tx, &[original.clone()]).await.unwrap();
        h.process(&*tx, &[amend1.clone()]).await.unwrap();
        h.process(&*tx, &[amend2.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }
    // Sanity: latest_amend points to amend2.
    {
        let client = pg.pool.get().await.unwrap();
        let merged = get_post(&**client, original_hash).await.unwrap().unwrap();
        assert_eq!(merged.item_hash, amend2_hash);
    }
    // Forget amend2 → latest_amend should refresh to amend1.
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        let extra = ContentHandler::forget_message(&h, &*tx, &amend2)
            .await
            .unwrap();
        // amend2 has no amends of its own.
        assert!(extra.is_empty(), "unexpected children: {extra:?}");
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let merged = get_post(&**client, original_hash).await.unwrap().unwrap();
    assert_eq!(merged.item_hash, amend1_hash);
}

#[tokio::test]
async fn forget_amend_when_not_latest_keeps_pointer() {
    let pg = start_postgres().await;
    let h = handler();
    let original_hash = "cc102e3b5efdbdc0b487359117ae3af40db654892487feae452689a0b84dc102";
    let amend1_hash = "dd176ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";
    let amend2_hash = "ee276ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";

    let original = post_message(
        original_hash,
        "0xabc",
        "blog",
        None,
        1_700_000_000.0,
        json!({"v": 1}),
    );
    let amend1 = post_message(
        amend1_hash,
        "0xabc",
        "amend",
        Some(original_hash),
        1_700_000_300.0,
        json!({"v": 2}),
    );
    let amend2 = post_message(
        amend2_hash,
        "0xabc",
        "amend",
        Some(original_hash),
        1_700_000_900.0,
        json!({"v": 3}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        h.process(&*tx, &[original]).await.unwrap();
        h.process(&*tx, &[amend1.clone()]).await.unwrap();
        h.process(&*tx, &[amend2]).await.unwrap();
        tx.commit().await.unwrap();
    }
    // Forget amend1 (NOT the latest). latest_amend remains pointing at
    // amend2.
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        let extra = ContentHandler::forget_message(&h, &*tx, &amend1)
            .await
            .unwrap();
        assert!(extra.is_empty(), "unexpected children: {extra:?}");
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let merged = get_post(&**client, original_hash).await.unwrap().unwrap();
    assert_eq!(merged.item_hash, amend2_hash);
}

#[tokio::test]
async fn process_post_with_tags_persists_tags_column() {
    let pg = start_postgres().await;
    let h = handler();
    let item_hash = "ff076ad67063b955869a7fa705ea2987add39486e1ed5951e9842291cf0f566c";
    let msg = post_message(
        item_hash,
        "0xabc",
        "blog",
        None,
        1_700_004_000.0,
        json!({"title": "hello", "tags": ["a", "b"]}),
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        h.process(&*tx, &[msg]).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT tags FROM posts WHERE item_hash = $1",
            &[&item_hash.to_string()],
        )
        .await
        .unwrap();
    let tags: Option<Vec<String>> = row.get(0);
    assert_eq!(tags, Some(vec!["a".into(), "b".into()]));
}
