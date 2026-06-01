//! Ports `tests/storage/test_store_message.py`.
//!
//! Drives `StoreMessageHandler.fetch_related_content` against a pre-populated
//! in-memory storage engine, asserting that the `files` table is updated.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};

use aleph_ccn::db::accessors::files::get_file;
use aleph_ccn::db::models::messages::MessageDb;
use aleph_ccn::handlers::content::content_handler::ContentHandler;
use aleph_ccn::handlers::content::store::StoreMessageHandler;
use aleph_ccn::services::storage::engine::StorageEngine;
use aleph_ccn::services::storage::in_memory::InMemoryStorageEngine;
use aleph_ccn::types::channel::Channel;
use aleph_ccn::types::files::FileType;
use aleph_ccn::types::message_status::MessageStatus;
use aleph_types::chain::Chain;
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;

use common::{start_postgres};

fn store_message_storage(item_hash: &str, sender: &str, file_hash: &str) -> MessageDb {
    let value = json!({
        "address": sender,
        "time": 1_645_807_812.0_f64,
        "item_type": "storage",
        "item_hash": file_hash,
        "mime_type": "text/plain",
    });
    let dt = Utc.timestamp_opt(1_645_807_812, 0).unwrap();
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
        size: 200,
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

fn handler_with(storage: Arc<InMemoryStorageEngine>, store_files: bool) -> StoreMessageHandler {
    let dyn_engine: Arc<dyn StorageEngine> = storage;
    StoreMessageHandler::new(
        dyn_engine,
        None,
        24,
        25 * 1024 * 1024,
        false,
        store_files,
        5,
        0.0,
        Vec::new(),
    )
}

#[tokio::test]
async fn handle_new_storage_file_records_file_row() {
    let pg = start_postgres().await;
    let file_hash = "5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21";
    let storage = Arc::new(InMemoryStorageEngine::default());
    storage.write(file_hash, b"alea jacta est").await.unwrap();
    let h = handler_with(storage, true);

    let msg = store_message_storage(
        "7e4f914865028356704919810073ec5690ecc4bb0ee3bd6bdb24829fd532398f",
        "0x1772213F07b98eBf3e85CCf88Ac29482ff97d9B1",
        file_hash,
    );
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::fetch_related_content(&h, &*tx, &msg).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let file = get_file(&**client, file_hash).await.unwrap().unwrap();
    assert_eq!(file.size, b"alea jacta est".len() as i64);
    assert_eq!(file.r#type, FileType::File);
}

#[tokio::test]
async fn handle_storage_missing_file_is_unavailable() {
    let pg = start_postgres().await;
    let file_hash = "6ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21";
    let storage = Arc::new(InMemoryStorageEngine::default());
    // intentionally no write
    let h = handler_with(storage, true);
    let msg = store_message_storage(
        "8e4f914865028356704919810073ec5690ecc4bb0ee3bd6bdb24829fd532398f",
        "0x1772213F07b98eBf3e85CCf88Ac29482ff97d9B1",
        file_hash,
    );
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let err = ContentHandler::fetch_related_content(&h, &*tx, &msg)
        .await
        .unwrap_err();
    tx.commit().await.unwrap();
    assert!(format!("{err:?}").contains("FileUnavailable"));
}

#[tokio::test]
async fn is_related_content_fetched_false_when_missing() {
    let pg = start_postgres().await;
    let storage = Arc::new(InMemoryStorageEngine::default());
    let h = handler_with(storage, true);
    let msg = store_message_storage(
        "9e4f914865028356704919810073ec5690ecc4bb0ee3bd6bdb24829fd532398f",
        "0x1772213F07b98eBf3e85CCf88Ac29482ff97d9B1",
        "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    );
    let mut client = pg.pool.get().await.unwrap();
    let tx = client.transaction().await.unwrap();
    let fetched = ContentHandler::is_related_content_fetched(&h, &*tx, &msg)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(!fetched);
}

#[tokio::test]
async fn store_files_false_does_not_persist_bytes_locally() {
    let pg = start_postgres().await;
    let file_hash = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
    let storage = Arc::new(InMemoryStorageEngine::default());
    // Pre-write so the existence check passes — we still expect the file row to
    // be persisted via fetch_related_content's local-storage branch.
    storage.write(file_hash, b"x").await.unwrap();
    let h = handler_with(storage.clone(), false);
    let msg = store_message_storage(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xowner",
        file_hash,
    );
    {
        let mut client = pg.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();
        ContentHandler::fetch_related_content(&h, &*tx, &msg).await.unwrap();
        tx.commit().await.unwrap();
    }
    let client = pg.pool.get().await.unwrap();
    let file = get_file(&**client, file_hash).await.unwrap().unwrap();
    assert_eq!(file.size, 1);
}
