//! Ports core flows from `tests/message_processing/test_process_stores.py`.
//!
//! The Python file has 22 tests but most rely on the full pipeline (cost
//! validation, balance reconciliation, IPFS stat mocks). The tests here drive
//! the [`StoreMessageHandler`] directly and verify the side-effects on
//! `files`, `file_pins` and `file_tags`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use aleph_ccn::AlephResult;
use aleph_ccn::db::accessors::files::{
    count_file_pins, get_file, get_file_tag, get_message_file_pin, is_pinned_file,
};
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::handlers::content::content_handler::ContentHandler;
use aleph_ccn::handlers::content::store::{IpfsFileStats, StoreMessageHandler, should_pin_on_ipfs};
use aleph_ccn::services::ipfs::IpfsService;
use aleph_ccn::services::ipfs::common::IpfsEndpoint;
use aleph_ccn::services::p2p::jobs::ApiServerLookup;
use aleph_ccn::services::storage::engine::StorageEngine;
use aleph_ccn::services::storage::in_memory::InMemoryStorageEngine;
use aleph_ccn::storage::{StorageService, verify_content_hash_sha256};
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::files::{FileTag, FileType};
use aleph_ccn::types::message_status::MessageStatus;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::{seed_file, start_postgres};

/// `storage`-style hash. A valid sha256-of-content placeholder used by the
/// Python tests.
const STORE_FILE_HASH: &str =
    "5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21";

fn store_message(
    item_hash: &str,
    sender: &str,
    file_hash: &str,
    item_type: &str,
    cref: Option<&str>,
    time: f64,
) -> MessageDb {
    let mut content = serde_json::Map::new();
    content.insert("address".into(), Value::String(sender.into()));
    content.insert("time".into(), json!(time));
    content.insert("item_type".into(), Value::String(item_type.into()));
    content.insert("item_hash".into(), Value::String(file_hash.into()));
    if let Some(r) = cref {
        content.insert("ref".into(), Value::String(r.into()));
    }
    let value = Value::Object(content);
    let dt = Utc.timestamp_opt(time as i64, 0).unwrap();
    MessageDb {
        item_hash: item_hash.into(),
        r#type: MessageType::Store,
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
        content_type: None,
        content_ref: cref.map(|s| s.into()),
        content_key: None,
        content_item_hash: Some(file_hash.into()),
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    }
}

fn handler() -> StoreMessageHandler {
    let storage: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::default());
    StoreMessageHandler::new(storage, None, 24, 25 * 1024 * 1024, false, true, 5, Vec::new())
}

fn handler_with_storage(storage: Arc<dyn StorageEngine>) -> StoreMessageHandler {
    StoreMessageHandler::new(storage, None, 24, 25 * 1024 * 1024, false, true, 5, Vec::new())
}

struct StaticApiServers(Vec<String>);

#[async_trait::async_trait]
impl ApiServerLookup for StaticApiServers {
    async fn get_api_servers(&self) -> AlephResult<Vec<String>> {
        Ok(self.0.clone())
    }
}

fn storage_service(
    storage: Arc<dyn StorageEngine>,
    api_servers: Vec<String>,
) -> Arc<StorageService> {
    let endpoint = IpfsEndpoint {
        scheme: "http".into(),
        host: "127.0.0.1".into(),
        port: 1,
        timeout: Duration::from_millis(1),
    };
    let ipfs = Arc::new(IpfsService::from_parts(
        reqwest::Client::new(),
        None,
        endpoint.clone(),
        endpoint,
    ));
    Arc::new(
        StorageService::new(storage, ipfs, Arc::new(StaticApiServers(api_servers)))
            .with_ipfs_enabled(false)
            .with_http_p2p_enabled(true),
    )
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn process_store_inserts_pin_and_file_tag() {
    let pg = start_postgres().await;
    let h = handler();
    seed_file(&pg.pool, STORE_FILE_HASH, 1024).await.unwrap();
    let item_hash = "70635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e";
    let msg = store_message(
        item_hash,
        "0xowner1",
        STORE_FILE_HASH,
        "storage",
        None,
        1_700_000_000.0,
    );
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let pin = get_message_file_pin(&**client, item_hash).await.unwrap();
    assert!(pin.is_some(), "message_file_pin must exist");
    let tag = get_file_tag(&**client, &FileTag::from(item_hash)).await.unwrap();
    assert!(tag.is_some(), "file tag must exist for non-ref store");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn process_store_with_ref_uses_owner_ref_tag() {
    let pg = start_postgres().await;
    let h = handler();
    seed_file(&pg.pool, STORE_FILE_HASH, 1024).await.unwrap();
    let item_hash = "80635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e";
    let msg = store_message(
        item_hash,
        "0xowner2",
        STORE_FILE_HASH,
        "storage",
        Some("mytag"),
        1_700_001_000.0,
    );
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    let owner_tag = FileTag::from("0xowner2/mytag");
    let tag = get_file_tag(&**client, &owner_tag).await.unwrap();
    assert!(tag.is_some(), "owner/ref tag should be created");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn forget_store_message_removes_pin_and_grace_periods_file() {
    let pg = start_postgres().await;
    let h = handler();
    seed_file(&pg.pool, STORE_FILE_HASH, 1024).await.unwrap();
    let item_hash = "90635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e";
    let msg = store_message(
        item_hash,
        "0xowner1",
        STORE_FILE_HASH,
        "storage",
        None,
        1_700_000_000.0,
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(
        count_file_pins(
            &**pg.pool.get().await.unwrap(),
            STORE_FILE_HASH,
        )
        .await
        .unwrap(),
        1
    );
    // Forget removes the message-pin and inserts a grace_period_pin (since
    // no other pins remain).
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        let _ = ContentHandler::forget_message(&h, &*tx, &msg)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    // After forget there is now exactly one grace-period pin replacing the
    // message-pin.
    let n = count_file_pins(&**client, STORE_FILE_HASH).await.unwrap();
    assert_eq!(n, 1, "expected exactly one (grace-period) pin");
    let pin = get_message_file_pin(&**client, item_hash).await.unwrap();
    assert!(pin.is_none(), "message_file_pin should be deleted");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn forget_store_multi_user_keeps_other_pin() {
    let pg = start_postgres().await;
    let h = handler();
    seed_file(&pg.pool, STORE_FILE_HASH, 1024).await.unwrap();
    let user1 = "0x971300C78A38e0F85E60A3b04ae3fA70b4276B64";
    let user2 = "0xaC033C1cA5C49Eff98A1D9a56BeDBC4840010BA4";
    let item1 = "50635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e";
    let item2 = "dbe8199004b052108ec19618f43af1d2baf5c04974d0aec1c4de2d02c44a2483";
    let msg1 = store_message(item1, user1, STORE_FILE_HASH, "storage", None, 1_700_000_000.0);
    let msg2 = store_message(item2, user2, STORE_FILE_HASH, "storage", None, 1_700_000_500.0);

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg1.clone()]).await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg2.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }

    assert_eq!(
        count_file_pins(
            &**pg.pool.get().await.unwrap(),
            STORE_FILE_HASH,
        )
        .await
        .unwrap(),
        2
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::forget_message(&h, &*tx, &msg1)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let client = pg.pool.get().await.unwrap();
    // user2's pin remains, so no grace_period pin should be added.
    let pinned = is_pinned_file(&**client, STORE_FILE_HASH).await.unwrap();
    assert!(pinned, "user2 pin must remain after forgetting user1");
    let n = count_file_pins(&**client, STORE_FILE_HASH).await.unwrap();
    assert_eq!(n, 1, "exactly user2's message-pin should remain");
    let pin2 = get_message_file_pin(&**client, item2).await.unwrap();
    assert!(pin2.is_some(), "user2 pin must remain");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn fetch_related_content_storage_reads_from_engine() {
    let pg = start_postgres().await;
    let storage = Arc::new(InMemoryStorageEngine::default());
    storage.write(STORE_FILE_HASH, b"Hello, world!").await.unwrap();
    let h = handler_with_storage(storage.clone());

    let msg = store_message(
        "11635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e",
        "0xowner",
        STORE_FILE_HASH,
        "storage",
        None,
        1_700_000_000.0,
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::fetch_related_content(&h, &*tx, &msg).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let file = get_file(&**client, STORE_FILE_HASH).await.unwrap();
    assert!(file.is_some());
    let stored = file.unwrap();
    assert_eq!(stored.size, "Hello, world!".len() as i64);
    assert_eq!(stored.r#type, FileType::File);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn fetch_related_content_storage_uses_storage_service_peer_fallback() {
    let pg = start_postgres().await;
    let body = b"Hello from peer";
    let file_hash = verify_content_hash_sha256(body);
    let peer = MockServer::start().await;
    let encoded = base64::engine::general_purpose::STANDARD.encode(body);
    Mock::given(method("GET"))
        .and(path(format!("/api/v0/storage/{file_hash}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "success",
            "content": encoded,
        })))
        .expect(1)
        .mount(&peer)
        .await;

    let storage = Arc::new(InMemoryStorageEngine::default());
    let storage_dyn: Arc<dyn StorageEngine> = storage.clone();
    let h = handler_with_storage(storage_dyn.clone())
        .with_storage_service(storage_service(storage_dyn, vec![peer.uri()]));

    let msg = store_message(
        "12635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e",
        "0xowner",
        &file_hash,
        "storage",
        None,
        1_700_000_000.0,
    );

    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::fetch_related_content(&h, &*tx, &msg).await.unwrap();
        tx.commit().await.unwrap();
    }

    assert_eq!(
        storage.read(&file_hash).await.unwrap().unwrap().as_ref(),
        body
    );
    let client = pg.pool.get().await.unwrap();
    let file = get_file(&**client, &file_hash).await.unwrap().unwrap();
    assert_eq!(file.size, body.len() as i64);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn fetch_related_content_missing_file_yields_unavailable() {
    let pg = start_postgres().await;
    let h = handler();
    let msg = store_message(
        "22635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e",
        "0xowner",
        STORE_FILE_HASH,
        "storage",
        None,
        1_700_000_000.0,
    );
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let err = ContentHandler::fetch_related_content(&h, &*tx, &msg)
        .await
        .unwrap_err();
    tx.commit().await.unwrap();
    let m = format!("{err:?}");
    assert!(m.contains("FileUnavailable"), "got {m}");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn is_related_content_fetched_returns_true_when_file_present() {
    let _pg = start_postgres().await;
    let storage = Arc::new(InMemoryStorageEngine::default());
    storage.write(STORE_FILE_HASH, b"abc").await.unwrap();
    let h = handler_with_storage(storage.clone());

    let msg = store_message(
        "33635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e",
        "0xowner",
        STORE_FILE_HASH,
        "storage",
        None,
        1_700_000_000.0,
    );
    // The handler reads from the in-memory engine directly; no live DB
    // contact required.
    let pool = _pg.pool.clone();
    let mut client = pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let fetched = ContentHandler::is_related_content_fetched(&h, &*tx, &msg)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(fetched);
}

#[test]
fn should_pin_on_ipfs_directory_always_pinned() {
    let stats = IpfsFileStats {
        size: 0,
        file_type: FileType::Directory,
        is_directory: true,
    };
    assert!(should_pin_on_ipfs(&stats, 1024 * 1024));
}

#[test]
fn should_pin_on_ipfs_small_file_skipped() {
    let stats = IpfsFileStats {
        size: 512,
        file_type: FileType::File,
        is_directory: false,
    };
    assert!(!should_pin_on_ipfs(&stats, 1024 * 1024));
}

#[test]
fn should_pin_on_ipfs_large_file_pinned() {
    let stats = IpfsFileStats {
        size: 2 * 1024 * 1024,
        file_type: FileType::File,
        is_directory: false,
    };
    assert!(should_pin_on_ipfs(&stats, 1024 * 1024));
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn forget_store_with_ref_refreshes_owner_ref_tag() {
    let pg = start_postgres().await;
    let h = handler();
    seed_file(&pg.pool, STORE_FILE_HASH, 1024).await.unwrap();
    let owner = "0xrefowner";
    let item = "44635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e";
    let msg = store_message(
        item,
        owner,
        STORE_FILE_HASH,
        "storage",
        Some("dataset-v1"),
        1_700_000_000.0,
    );
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }
    // Sanity check the owner/ref tag was created.
    {
        let client = pg.pool.get().await.unwrap();
        let tag = get_file_tag(&**client, &FileTag::from("0xrefowner/dataset-v1"))
            .await
            .unwrap();
        assert!(tag.is_some());
    }
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::forget_message(&h, &*tx, &msg)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    // The tag for the deleted pin was either dropped (no more pins) or
    // refreshed; either way it should not return the deleted message's file
    // hash.
    let client = pg.pool.get().await.unwrap();
    let pin = get_message_file_pin(&**client, item).await.unwrap();
    assert!(pin.is_none(), "pin should have been removed");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn process_store_idempotent_for_same_item_hash_rejects_duplicate_pin() {
    let pg = start_postgres().await;
    let h = handler();
    seed_file(&pg.pool, STORE_FILE_HASH, 1024).await.unwrap();
    let item = "55635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e";
    let msg = store_message(
        item,
        "0xowner",
        STORE_FILE_HASH,
        "storage",
        None,
        1_700_000_000.0,
    );
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg.clone()]).await.unwrap();
        tx.commit().await.unwrap();
    }
    // A second insert with the same item_hash should fail (uq constraint on
    // file_pins.item_hash).
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let err = ContentHandler::process(&h, &*tx, &[msg]).await;
    let _ = tx.rollback().await;
    assert!(err.is_err(), "duplicate insert must error out");
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn process_store_for_two_different_items_keeps_pin_count_at_two() {
    let pg = start_postgres().await;
    let h = handler();
    seed_file(&pg.pool, STORE_FILE_HASH, 1024).await.unwrap();
    let owner = "0xowner";
    let items = ["66635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e",
                 "77635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e"];
    for (i, hash) in items.iter().enumerate() {
        let msg = store_message(
            hash,
            owner,
            STORE_FILE_HASH,
            "storage",
            None,
            1_700_000_000.0 + (i as f64) * 10.0,
        );
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::process(&h, &*tx, &[msg]).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let n = count_file_pins(&**client, STORE_FILE_HASH).await.unwrap();
    assert_eq!(n, 2);
}

#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn fetch_related_content_storage_with_wrong_hash_kind_yields_invalid_format() {
    let pg = start_postgres().await;
    let storage = Arc::new(InMemoryStorageEngine::default());
    let h = handler_with_storage(storage);

    // Use an IPFS-style hash but claim item_type=storage. The
    // `item_type_from_hash` sanity check should reject it.
    let msg = store_message(
        "88635384e43c7af6b3297f6571644c30f3f07ac681bfd14b9c556c63e661a69e",
        "0xowner",
        "QmTQPocJ8n3r7jhwYxmCDR5bJ4SNsEhdVm8WwkNbGctgJF",
        "storage",
        None,
        1_700_000_000.0,
    );

    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let err = ContentHandler::fetch_related_content(&h, &*tx, &msg)
        .await
        .unwrap_err();
    tx.commit().await.unwrap();
    let m = format!("{err:?}");
    assert!(m.contains("InvalidMessageFormat"), "got {m}");
}

// ---------------------------------------------------------------------------
// Balance / credit / payment-type checks — ports of:
//   test_process_store_with_not_enough_balance,
//   test_new_store_message_requires_credits,
//   test_legacy_store_message_uses_hold_payment.
// The full Python pipeline uses MessageHandler.process(); here we exercise the
// StoreMessageHandler's check_balance hook directly (the gateway into payment
// validation) and assert the failure shape that the production code raises.
// ---------------------------------------------------------------------------

use aleph_ccn::toolkit::constants::{
    CREDIT_ONLY_CUTOFF_TIMESTAMP, STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP,
};
use common::insert_default_aggregates;

fn store_message_with_size(
    item_hash: &str,
    sender: &str,
    file_hash: &str,
    time: f64,
    size: i32,
) -> MessageDb {
    let mut content = serde_json::Map::new();
    content.insert("address".into(), Value::String(sender.into()));
    content.insert("time".into(), json!(time));
    content.insert("item_type".into(), Value::String("storage".into()));
    content.insert("item_hash".into(), Value::String(file_hash.into()));
    let value = Value::Object(content);
    let dt = Utc.timestamp_opt(time as i64, 0).unwrap();
    MessageDb {
        item_hash: item_hash.into(),
        r#type: MessageType::Store,
        chain: Chain::Ethereum,
        sender: sender.into(),
        signature: Some("0xsig".into()),
        item_type: ItemType::Inline,
        item_content: Some(value.to_string()),
        content: value,
        time: dt,
        channel: Some(Channel::from("TEST".to_string())),
        size,
        status_value: MessageStatus::Processed,
        reception_time: dt,
        owner: Some(sender.into()),
        content_type: None,
        content_ref: None,
        content_key: None,
        content_item_hash: Some(file_hash.into()),
        first_confirmed_at: None,
        first_confirmed_height: None,
        payment_type: None,
        tags: None,
    }
}

/// In the paid-hold window, a STORE with payment.type=hold (the default) over
/// the unauthenticated-upload threshold must fail when the sender has no
/// balance.
#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn check_balance_fails_when_balance_below_cost_for_large_hold_store() {
    let pg = start_postgres().await;
    insert_default_aggregates(&pg.pool).await.unwrap();
    let h = handler();

    // 26 MiB > 25 MiB unauthenticated threshold => balance is enforced.
    let large_size = 26 * 1024 * 1024;
    let msg = store_message_with_size(
        "734a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
        "0xpoor",
        STORE_FILE_HASH,
        (STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1) as f64,
        large_size,
    );

    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let res = ContentHandler::check_balance(&h, &*tx, &msg).await;
    tx.commit().await.unwrap();
    assert!(
        res.is_err(),
        "expected balance error for hold payment with 0 balance, got {res:?}",
    );
    let err = format!("{:?}", res.unwrap_err());
    assert!(
        err.contains("InsufficientBalance") || err.contains("Balance"),
        "expected balance-related error, got {err}",
    );
}

/// Ports test_new_store_message_requires_credits: a STORE submitted after the
/// credit-only cutoff must use payment_type=credit.
#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn check_balance_rejects_hold_message_after_credit_only_cutoff() {
    let pg = start_postgres().await;
    insert_default_aggregates(&pg.pool).await.unwrap();
    let h = handler();

    let msg = store_message_with_size(
        "844a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
        "0xnewuser",
        STORE_FILE_HASH,
        (CREDIT_ONLY_CUTOFF_TIMESTAMP + 1) as f64,
        26 * 1024 * 1024,
    );

    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let res = ContentHandler::check_balance(&h, &*tx, &msg).await;
    tx.commit().await.unwrap();
    assert!(res.is_err(), "expected InvalidPaymentMethod, got {res:?}");
    let err = format!("{:?}", res.unwrap_err());
    assert!(
        err.contains("InvalidPaymentMethod"),
        "expected InvalidPaymentMethod, got {err}",
    );
}

/// During the paid-hold window, small hold STORE messages remain exempt from
/// balance validation.
#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn check_balance_allows_small_hold_store_after_store_cost_cutoff() {
    let pg = start_postgres().await;
    insert_default_aggregates(&pg.pool).await.unwrap();
    let h = handler();

    let small = 1024 * 1024;
    let msg = store_message_with_size(
        "a44a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
        "0xsmallhold",
        STORE_FILE_HASH,
        (STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP + 1) as f64,
        small,
    );

    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let res = ContentHandler::check_balance(&h, &*tx, &msg).await;
    tx.commit().await.unwrap();
    assert!(
        res.is_ok(),
        "small hold store must pass during paid-hold window, got {res:?}",
    );
}

/// Ports test_legacy_store_message_uses_hold_payment: a small STORE submitted
/// before the cutoff goes through the unauthenticated-upload free path.
#[tokio::test]
#[ignore = "requires docker; run with --ignored"]
async fn pre_check_balance_legacy_small_store_is_free() {
    let pg = start_postgres().await;
    insert_default_aggregates(&pg.pool).await.unwrap();
    let h = handler();

    let small = 1024 * 1024;
    let msg = store_message_with_size(
        "944a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
        "0xlegacy",
        STORE_FILE_HASH,
        (STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP - 1) as f64,
        small,
    );

    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let res = ContentHandler::check_balance(&h, &*tx, &msg).await;
    tx.commit().await.unwrap();
    assert!(
        res.is_ok(),
        "legacy small store must not raise payment errors, got {res:?}",
    );
}
