//! Ports `tests/message_processing/test_process_forgets.py`.
//!
//! These tests drive [`ForgetMessageHandler`] directly. A FORGET message
//! targets:
//! - a POST → the post handler removes the row.
//! - a STORE → the store handler removes the file pin.
//! - a FORGET → rejected via `CannotForgetForgetMessage`.
//! - a missing item → `ForgetTargetNotFound`.

mod common;

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde_json::{Value, json};

use aleph_ccn::db::accessors::messages::upsert_message_status;
use aleph_ccn::db::accessors::posts::get_original_post;
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::handlers::content::aggregate::AggregateMessageHandler;
use aleph_ccn::handlers::content::content_handler::ContentHandler;
use aleph_ccn::handlers::content::forget::{ContentHandlerTable, ForgetMessageHandler};
use aleph_ccn::handlers::content::post::PostMessageHandler;
use aleph_ccn::handlers::content::store::StoreMessageHandler;
use aleph_ccn::handlers::content::vm::VmMessageHandler;
use aleph_ccn::services::storage::in_memory::InMemoryStorageEngine;
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::message_status::{MessageStatus};
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::{insert_processed_message, start_postgres};

fn build_forget_handler() -> ForgetMessageHandler {
    use aleph_ccn::services::storage::engine::StorageEngine;
    let storage: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::default());
    let vm: Arc<dyn ContentHandler> = Arc::new(VmMessageHandler::new());
    let post: Arc<dyn ContentHandler> = Arc::new(PostMessageHandler::new(
        vec!["nope".into()],
        "no-balances-in-tests".into(),
        vec!["nope".into()],
        vec!["no-balances-in-tests".into()],
        vec!["nope".into()],
    ));
    let store: Arc<dyn ContentHandler> = Arc::new(StoreMessageHandler::new(
        storage, None, 24, 25 * 1024 * 1024, false, false, 5, Vec::new(),
    ));
    let aggregate: Arc<dyn ContentHandler> = Arc::new(AggregateMessageHandler::new());
    let table: ContentHandlerTable = vec![
        (MessageType::Aggregate, aggregate),
        (MessageType::Instance, vm.clone()),
        (MessageType::Post, post),
        (MessageType::Program, vm),
        (MessageType::Store, store),
    ];
    ForgetMessageHandler::new(table)
}

fn post_message(item_hash: &str, sender: &str, ctype: &str, time: f64, inner: Value) -> MessageDb {
    let content = json!({
        "address": sender,
        "type": ctype,
        "time": time,
        "content": inner,
    });
    let dt = Utc.timestamp_opt(time as i64, 0).unwrap();
    MessageDb {
        item_hash: item_hash.into(),
        r#type: MessageType::Post,
        chain: Chain::Ethereum,
        sender: sender.into(),
        signature: Some("0xsig".into()),
        item_type: ItemType::Inline,
        item_content: Some(content.to_string()),
        content,
        time: dt,
        channel: Some(Channel::from("TEST".to_string())),
        size: 256,
        status_value: MessageStatus::Processed,
        reception_time: dt,
        owner: Some(sender.into()),
        content_type: Some(ctype.into()),
        content_ref: None,
        content_key: None,
        content_item_hash: None,
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    }
}

fn forget_message(item_hash: &str, sender: &str, hashes: &[&str], time: f64) -> MessageDb {
    let content = json!({
        "address": sender,
        "time": time,
        "hashes": hashes,
    });
    let dt = Utc.timestamp_opt(time as i64, 0).unwrap();
    MessageDb {
        item_hash: item_hash.into(),
        r#type: MessageType::Forget,
        chain: Chain::Ethereum,
        sender: sender.into(),
        signature: Some("0xsig".into()),
        item_type: ItemType::Inline,
        item_content: Some(content.to_string()),
        content,
        time: dt,
        channel: Some(Channel::from("TEST".to_string())),
        size: 256,
        status_value: MessageStatus::Processed,
        reception_time: dt,
        owner: Some(sender.into()),
        content_type: None,
        content_ref: None,
        content_key: None,
        content_item_hash: None,
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    }
}

async fn insert_post(pool: &aleph_ccn::db::DbPool, msg: &MessageDb) {
    // Use the post handler's `process` to seed a real post row.
    let h = PostMessageHandler::new(
        vec!["nope".into()],
        "no-balances".into(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let mut client = pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    h.process(&*tx, std::slice::from_ref(msg)).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn forget_post_deletes_row_and_marks_status_forgotten() {
    let pg = start_postgres().await;
    let sender = "0x696879aE4F6d8DaDD5b8F1cbb1e663B89b08f106";
    let target_hash = "fc1e7b1edc2348eb78303fb1342e31e5ad3820249629032d37f8223e754a5f8e";
    let forget_hash = "431a0d2f79ecfa859949d2a09f67068ce7ebd4eb777d179ad958be6c79abc66b";

    let target = post_message(target_hash, sender, "test", 1_652_786_281.0, json!({"body": "destroy me"}));
    insert_processed_message(&pg.pool, target.clone()).await.unwrap();
    insert_post(&pg.pool, &target).await;

    let forget = forget_message(forget_hash, sender, &[target_hash], 1_652_786_534.0);

    let forget_handler = build_forget_handler();
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    ContentHandler::process(&forget_handler, &*tx, &[forget])
        .await
        .unwrap();
    // The post handler's `forget_message` should have deleted the row.
    let p = get_original_post(&*tx, target_hash).await.unwrap();
    tx.commit().await.unwrap();
    assert!(p.is_none(), "post should be gone after forget");
}

#[tokio::test]
async fn forget_with_no_targets_yields_no_target() {
    let pg = start_postgres().await;
    let forget = forget_message(
        "ee10000000000000000000000000000000000000000000000000000000000001",
        "0xabc",
        &[], // empty `hashes`
        1_700_000_000.0,
    );
    let h = build_forget_handler();
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let err = h.check_dependencies(&*tx, &forget).await.unwrap_err();
    tx.commit().await.unwrap();
    let m = format!("{err:?}");
    assert!(m.contains("NoForgetTarget"), "got {m}");
}

#[tokio::test]
async fn forget_unknown_target_hash_yields_target_not_found() {
    let pg = start_postgres().await;
    let forget = forget_message(
        "ee20000000000000000000000000000000000000000000000000000000000001",
        "0xabc",
        &["7777777777777777777777777777777777777777777777777777777777777777"],
        1_700_000_500.0,
    );
    let h = build_forget_handler();
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let err = h.check_dependencies(&*tx, &forget).await.unwrap_err();
    tx.commit().await.unwrap();
    let m = format!("{err:?}");
    assert!(m.contains("ForgetTargetNotFound"), "got {m}");
}

#[tokio::test]
async fn forget_forget_message_is_rejected() {
    let pg = start_postgres().await;
    let sender = "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef";
    let target_hash = "e3b24727335e34016247c0d37e2b0203bb8c2d76deddafc1700b4cf0e13845c5";
    let forget_hash = "884dd713e94fa0350239b67e65eecaa54361df8af0e3f6d0e42e0f8de059e15a";

    // Insert the target as a processed FORGET message.
    let mut target = post_message(target_hash, sender, "test", 1_645_794_065.0, json!({}));
    target.r#type = MessageType::Forget;
    target.content = json!({
        "address": sender,
        "time": 1_645_794_065.0,
        "hashes": ["QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF"],
    });
    insert_processed_message(&pg.pool, target).await.unwrap();

    // FORGET targeting that FORGET should be rejected.
    let forget = forget_message(forget_hash, sender, &[target_hash], 1_639_058_312.0);
    let h = build_forget_handler();
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let err = ContentHandler::process(&h, &*tx, &[forget])
        .await
        .unwrap_err();
    tx.commit().await.unwrap();
    let m = format!("{err:?}");
    assert!(m.contains("CannotForgetForgetMessage"), "got {m}");
}

#[tokio::test]
async fn forget_target_with_status_forgotten_appends_to_forgotten_by() {
    let pg = start_postgres().await;
    let sender = "0xabc1";
    let target_hash = "aa00000000000000000000000000000000000000000000000000000000000001";
    let target = post_message(target_hash, sender, "test", 1_700_000_000.0, json!({"v": 1}));
    insert_processed_message(&pg.pool, target.clone()).await.unwrap();
    // Move it to FORGOTTEN.
    {
        let client = pg.pool.get().await.unwrap();
        upsert_message_status(&**client, target_hash, MessageStatus::Forgotten, Utc::now(), None)
            .await
            .unwrap();
        // Seed the forgotten row so append_to_forgotten_by has somewhere to
        // write.
        client
            .execute(
                "INSERT INTO forgotten_messages(item_hash, type, chain, sender, signature, \
                                                  item_type, time, channel, forgotten_by) \
                 VALUES ($1, 'POST', 'ETH', $2, '0xsig', 'inline', NOW(), 'TEST', $3::varchar[]) \
                 ON CONFLICT DO NOTHING",
                &[&target_hash.to_string(), &sender.to_string(), &Vec::<String>::new()],
            )
            .await
            .unwrap();
    }
    let forget_hash = "bb00000000000000000000000000000000000000000000000000000000000001";
    let forget = forget_message(forget_hash, sender, &[target_hash], 1_700_001_000.0);
    let h = build_forget_handler();
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    ContentHandler::process(&h, &*tx, &[forget])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // The forgotten row should now include the new forget hash in its
    // forgotten_by column.
    let row = client
        .query_one(
            "SELECT forgotten_by FROM forgotten_messages WHERE item_hash = $1",
            &[&target_hash.to_string()],
        )
        .await
        .unwrap();
    let arr: Vec<String> = row.get(0);
    assert!(arr.contains(&forget_hash.to_string()), "expected {forget_hash} in {arr:?}");
}
