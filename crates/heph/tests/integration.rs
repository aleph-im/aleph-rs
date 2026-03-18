//! End-to-end integration tests for the heph HTTP server.
//!
//! These tests spin up a real actix-web server on a random OS-assigned port,
//! then exercise the full request/response cycle with a `reqwest` client.

use std::sync::Arc;

use actix_web::{App, HttpServer, web};
use aleph_types::account::{Account, EvmAccount, sign_message};
use aleph_types::chain::Chain;
use aleph_types::item_hash::{AlephItemHash, ItemHash};
use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;
use aleph_types::message::unsigned::UnsignedMessage;
use aleph_types::timestamp::Timestamp;

use heph::api::{AppState, configure_routes};
use heph::config::HephConfig;
use heph::db::Db;
use heph::files::FileStore;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Start the server on an OS-assigned port and return the base URL.
///
/// The server is constructed and run entirely inside a background OS thread
/// that owns its own actix/tokio runtime, avoiding Send constraints.
/// A `std::sync::mpsc::channel` carries the bound port back to the caller.
fn start_test_server() -> String {
    let db = Arc::new(Db::open_in_memory().unwrap());
    let tmpdir = tempfile::tempdir().unwrap();
    let file_store = Arc::new(FileStore::new(&tmpdir.keep().join("files")).unwrap());

    // Pre-seed a test account with a deterministic key.
    let key = [1u8; 32];
    let account = EvmAccount::new(Chain::Ethereum, &key).unwrap();
    let addr = account.address().as_str().to_string();
    db.with_conn(|c| heph::db::balances::set_credit_balance(c, &addr, 1_000_000_000))
        .unwrap();

    let config = HephConfig {
        port: 0,
        host: "127.0.0.1".to_string(),
        data_dir: None,
        accounts: vec![addr],
        balance: 1_000_000_000,
        log_level: "error".to_string(),
    };

    let (tx, rx) = std::sync::mpsc::channel::<u16>();

    // Build and run the server entirely inside the background thread.
    // This is necessary because actix-web's App / HttpServer use Rc internally
    // and are therefore !Send.
    std::thread::spawn(move || {
        let state = web::Data::new(AppState {
            db,
            file_store,
            config,
        });

        let sys = actix_web::rt::System::new();
        sys.block_on(async move {
            let server = HttpServer::new(move || {
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes)
            })
            .bind("127.0.0.1:0")
            .unwrap();

            let port = server.addrs()[0].port();
            // Send the port before we start serving.
            tx.send(port).unwrap();

            server.run().await.unwrap();
        });
    });

    // Wait for the port to come back and give the server a moment to accept.
    let port = rx.recv().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));

    format!("http://127.0.0.1:{port}")
}

/// Build a signed POST message and return (msg_json, item_hash_hex).
fn build_post_msg(key: &[u8; 32], time: f64) -> (serde_json::Value, String) {
    let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
    let addr = account.address().as_str().to_string();
    let item_content = format!(
        r#"{{"type":"test-post","address":"{}","time":{},"content":{{"body":"hello world"}}}}"#,
        addr, time
    );
    let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
    let unsigned = UnsignedMessage {
        message_type: MessageType::Post,
        item_type: ItemType::Inline,
        item_content: item_content.clone(),
        item_hash: item_hash.clone(),
        time: Timestamp::from(time),
        channel: None,
    };
    let pending = sign_message(&account, unsigned).unwrap();
    let hash_str = pending.item_hash.to_string();
    let msg = serde_json::json!({
        "chain": pending.chain,
        "sender": pending.sender.as_str(),
        "signature": pending.signature.as_str(),
        "type": "POST",
        "item_type": "inline",
        "item_content": pending.item_content,
        "item_hash": hash_str,
        "time": time,
    });
    (msg, hash_str)
}

/// Build a signed AGGREGATE message and return (msg_json, item_hash_hex).
fn build_aggregate_msg(
    key: &[u8; 32],
    agg_key: &str,
    content_json: &str,
    time: f64,
) -> (serde_json::Value, String) {
    let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
    let addr = account.address().as_str().to_string();
    let item_content = format!(
        r#"{{"key":"{}","address":"{}","time":{},"content":{}}}"#,
        agg_key, addr, time, content_json
    );
    let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
    let unsigned = UnsignedMessage {
        message_type: MessageType::Aggregate,
        item_type: ItemType::Inline,
        item_content: item_content.clone(),
        item_hash: item_hash.clone(),
        time: Timestamp::from(time),
        channel: None,
    };
    let pending = sign_message(&account, unsigned).unwrap();
    let hash_str = pending.item_hash.to_string();
    let msg = serde_json::json!({
        "chain": pending.chain,
        "sender": pending.sender.as_str(),
        "signature": pending.signature.as_str(),
        "type": "AGGREGATE",
        "item_type": "inline",
        "item_content": pending.item_content,
        "item_hash": hash_str,
        "time": time,
    });
    (msg, hash_str)
}

/// Build a signed FORGET message targeting `target_hash`.
fn build_forget_msg(key: &[u8; 32], target_hash: &str, time: f64) -> (serde_json::Value, String) {
    let account = EvmAccount::new(Chain::Ethereum, key).unwrap();
    let addr = account.address().as_str().to_string();
    let item_content = format!(
        r#"{{"hashes":["{}"],"address":"{}","time":{}}}"#,
        target_hash, addr, time
    );
    let item_hash = ItemHash::Native(AlephItemHash::from_bytes(item_content.as_bytes()));
    let unsigned = UnsignedMessage {
        message_type: MessageType::Forget,
        item_type: ItemType::Inline,
        item_content: item_content.clone(),
        item_hash: item_hash.clone(),
        time: Timestamp::from(time),
        channel: None,
    };
    let pending = sign_message(&account, unsigned).unwrap();
    let hash_str = pending.item_hash.to_string();
    let msg = serde_json::json!({
        "chain": pending.chain,
        "sender": pending.sender.as_str(),
        "signature": pending.signature.as_str(),
        "type": "FORGET",
        "item_type": "inline",
        "item_content": pending.item_content,
        "item_hash": hash_str,
        "time": time,
    });
    (msg, hash_str)
}

fn addr_for_key(key: &[u8; 32]) -> String {
    EvmAccount::new(Chain::Ethereum, key)
        .unwrap()
        .address()
        .as_str()
        .to_string()
}

// ---------------------------------------------------------------------------
// Full E2E test
// ---------------------------------------------------------------------------

/// Comprehensive end-to-end test covering:
///   a. Submit a POST message (sync)
///   b. Query it back via GET /api/v0/messages/{hash}
///   c. Submit an AGGREGATE message
///   d. Query aggregates via GET /api/v0/aggregates/{address}.json
///   e. Upload a file via POST /api/v0/storage/add_json
///   f. Download it via GET /api/v0/storage/raw/{hash}
///   g. Submit a FORGET for the POST
///   h. Verify the POST is now forgotten
#[tokio::test]
async fn test_full_e2e_flow() {
    let base_url = start_test_server();
    let client = reqwest::Client::new();

    let key = [1u8; 32];
    let addr = addr_for_key(&key);

    // ------------------------------------------------------------------
    // a. Submit a POST message (sync)
    // ------------------------------------------------------------------
    let (post_msg, post_hash) = build_post_msg(&key, 1_700_000_000.0);

    let resp = client
        .post(format!("{base_url}/api/v0/messages"))
        .json(&serde_json::json!({ "sync": true, "message": post_msg }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        200,
        "POST message should return 200 (sync)"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["message_status"], "processed");
    assert_eq!(body["publication_status"]["status"], "success");

    // ------------------------------------------------------------------
    // b. Query back via GET /api/v0/messages/{hash}
    // ------------------------------------------------------------------
    let resp = client
        .get(format!("{base_url}/api/v0/messages/{post_hash}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 200, "GET message should return 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "processed");
    assert_eq!(body["item_hash"], post_hash);
    assert_eq!(
        body["message"]["sender"].as_str().unwrap().to_lowercase(),
        addr.to_lowercase()
    );

    // ------------------------------------------------------------------
    // c. Submit an AGGREGATE message
    // ------------------------------------------------------------------
    let (agg_msg, _agg_hash) = build_aggregate_msg(
        &key,
        "profile",
        r#"{"name":"Alice","score":42}"#,
        1_700_000_001.0,
    );

    let resp = client
        .post(format!("{base_url}/api/v0/messages"))
        .json(&serde_json::json!({ "sync": true, "message": agg_msg }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        200,
        "AGGREGATE message should return 200"
    );

    // ------------------------------------------------------------------
    // d. Query aggregates via GET /api/v0/aggregates/{address}.json
    // ------------------------------------------------------------------
    let resp = client
        .get(format!("{base_url}/api/v0/aggregates/{addr}.json"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        200,
        "GET aggregates should return 200"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["address"].as_str().unwrap().to_lowercase(),
        addr.to_lowercase()
    );
    assert_eq!(body["data"]["profile"]["name"], "Alice");
    assert_eq!(body["data"]["profile"]["score"], 42);

    // ------------------------------------------------------------------
    // e. Upload a file via POST /api/v0/storage/add_json
    // ------------------------------------------------------------------
    let file_content = serde_json::json!({"stored": "data", "value": 123});
    let file_bytes = serde_json::to_vec(&file_content).unwrap();

    let resp = client
        .post(format!("{base_url}/api/v0/storage/add_json"))
        .header("content-type", "application/json")
        .body(file_bytes.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 200, "add_json should return 200");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "success");
    let file_hash = body["hash"].as_str().unwrap().to_string();
    assert_eq!(file_hash.len(), 64, "file hash should be 64 hex chars");

    // ------------------------------------------------------------------
    // f. Download via GET /api/v0/storage/raw/{hash}
    // ------------------------------------------------------------------
    let resp = client
        .get(format!("{base_url}/api/v0/storage/raw/{file_hash}"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        200,
        "GET raw storage should return 200"
    );
    let raw_bytes = resp.bytes().await.unwrap();
    assert_eq!(
        raw_bytes.to_vec(),
        file_bytes,
        "downloaded bytes should match uploaded bytes"
    );

    // ------------------------------------------------------------------
    // g. Submit a FORGET for the POST
    // ------------------------------------------------------------------
    let (forget_msg, _forget_hash) = build_forget_msg(&key, &post_hash, 1_700_000_002.0);

    let resp = client
        .post(format!("{base_url}/api/v0/messages"))
        .json(&serde_json::json!({ "sync": true, "message": forget_msg }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        200,
        "FORGET message should return 200"
    );

    // ------------------------------------------------------------------
    // h. Verify the POST is now forgotten
    // ------------------------------------------------------------------
    let resp = client
        .get(format!("{base_url}/api/v0/messages/{post_hash}"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        200,
        "GET forgotten message should return 200"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["status"], "forgotten",
        "POST message should have status 'forgotten' after FORGET"
    );
    assert_eq!(body["item_hash"], post_hash);
}

// ---------------------------------------------------------------------------
// Additional targeted tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_missing_message_returns_404() {
    let base_url = start_test_server();
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base_url}/api/v0/messages/nonexistent_hash_abc123"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test]
async fn test_async_post_returns_202() {
    let base_url = start_test_server();
    let client = reqwest::Client::new();

    let key = [2u8; 32];
    let (msg, _) = build_post_msg(&key, 1_700_000_010.0);

    let resp = client
        .post(format!("{base_url}/api/v0/messages"))
        .json(&serde_json::json!({ "sync": false, "message": msg }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 202, "async POST should return 202");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["message_status"], "pending");
}

#[tokio::test]
async fn test_storage_raw_404_for_nonexistent() {
    let base_url = start_test_server();
    let client = reqwest::Client::new();

    let nonexistent = "0".repeat(64);
    let resp = client
        .get(format!("{base_url}/api/v0/storage/raw/{nonexistent}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test]
async fn test_aggregate_404_for_unknown_address() {
    let base_url = start_test_server();
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base_url}/api/v0/aggregates/0xdeadbeefdeadbeef.json"
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 404);
}

/// Test submit_message with inline content (small message).
/// Uses the SDK's AlephClient + PostBuilder instead of raw HTTP.
#[tokio::test]
async fn test_submit_message_inline() {
    use aleph_sdk::client::{AlephClient, AlephMessageClient};
    use aleph_sdk::messages::PostBuilder;
    use aleph_types::message::MessageStatus;
    use url::Url;

    let base_url = start_test_server();
    let client = AlephClient::new(Url::parse(&base_url).unwrap());

    let key = [1u8; 32];
    let account = EvmAccount::new(Chain::Ethereum, &key).unwrap();

    let pending = PostBuilder::new(
        &account,
        "test-post",
        serde_json::json!({"body": "hello via SDK"}),
    )
    .unwrap()
    .build()
    .unwrap();

    assert_eq!(pending.item_type, ItemType::Inline);

    let resp = client.submit_message(&pending, true).await.unwrap();
    assert_eq!(resp.message_status, "processed");

    // Verify we can fetch the message back
    let fetched = client.get_message(&pending.item_hash).await.unwrap();
    assert_eq!(fetched.status(), MessageStatus::Processed);
}

/// Test submit_message with storage-routed content (large message).
/// Verifies that submit_message transparently uploads content before posting.
#[tokio::test]
async fn test_submit_message_storage() {
    use aleph_sdk::client::{AlephClient, AlephMessageClient, AlephStorageClient};
    use aleph_sdk::messages::PostBuilder;
    use aleph_types::message::MessageStatus;
    use url::Url;

    let base_url = start_test_server();
    let client = AlephClient::new(Url::parse(&base_url).unwrap());

    let key = [1u8; 32];
    let account = EvmAccount::new(Chain::Ethereum, &key).unwrap();

    // Create content large enough to exceed the 200KB inline cutoff
    let big_body = "x".repeat(210_000);
    let pending = PostBuilder::new(&account, "test-post", serde_json::json!({"body": big_body}))
        .unwrap()
        .build()
        .unwrap();

    assert_eq!(pending.item_type, ItemType::Storage);

    let resp = client.submit_message(&pending, true).await.unwrap();
    assert_eq!(resp.message_status, "processed");

    // Verify the content is retrievable from storage
    let download = client
        .download_file_by_hash(&pending.item_hash)
        .await
        .unwrap();
    let bytes = download.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), pending.item_content.as_bytes());

    // Verify we can fetch the message back
    let fetched = client.get_message(&pending.item_hash).await.unwrap();
    assert_eq!(fetched.status(), MessageStatus::Processed);
}

#[tokio::test]
#[ignore = "heph does not support IPFS upload; enable when IPFS endpoint is available"]
async fn test_submit_message_ipfs() {
    todo!()
}

/// Test file upload via the CLI handler (exercises the full upload → STORE → submit flow).
#[tokio::test]
async fn test_cli_file_upload() {
    use aleph_sdk::client::{AlephClient, AlephMessageClient, AlephStorageClient};
    use aleph_sdk::messages::StoreBuilder;
    use aleph_types::message::{MessageStatus, StorageEngine};
    use url::Url;

    let base_url = start_test_server();
    let client = AlephClient::new(Url::parse(&base_url).unwrap());

    let key = [1u8; 32];
    let account = EvmAccount::new(Chain::Ethereum, &key).unwrap();

    // Write a temp file
    let tmpdir = tempfile::tempdir().unwrap();
    let file_path = tmpdir.path().join("cli-upload-test.txt");
    let file_content = b"cli file upload integration test";
    std::fs::write(&file_path, file_content).unwrap();

    // Replicate the CLI handler logic: upload → build STORE → submit
    let file_hash = client.upload_file_to_storage(&file_path).await.unwrap();

    let pending = StoreBuilder::new(&account, file_hash.clone(), StorageEngine::Storage)
        .reference("my-test-ref")
        .build()
        .unwrap();

    assert_eq!(pending.message_type, MessageType::Store);

    let resp = client.post_message(&pending, true).await.unwrap();
    assert_eq!(resp.message_status, "processed");

    // Verify the STORE message was persisted
    let fetched = client.get_message(&pending.item_hash).await.unwrap();
    assert_eq!(fetched.status(), MessageStatus::Processed);

    // Verify file is downloadable by hash
    let download = client.download_file_by_hash(&file_hash).await.unwrap();
    let bytes = download.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), file_content);
}

/// Test creating a STORE message using the SDK's StoreBuilder.
/// Uploads a file, builds a STORE message, submits it, and verifies it can be retrieved.
#[tokio::test]
async fn test_store_builder_e2e() {
    use aleph_sdk::client::{AlephClient, AlephMessageClient, AlephStorageClient};
    use aleph_sdk::messages::StoreBuilder;
    use aleph_types::message::{MessageStatus, StorageEngine};
    use url::Url;

    let base_url = start_test_server();
    let client = AlephClient::new(Url::parse(&base_url).unwrap());

    let key = [1u8; 32];
    let account = EvmAccount::new(Chain::Ethereum, &key).unwrap();

    // Write a temp file to upload
    let tmpdir = tempfile::tempdir().unwrap();
    let file_path = tmpdir.path().join("test-upload.bin");
    let file_content = b"hello aleph store test!";
    std::fs::write(&file_path, file_content).unwrap();

    // Upload file and get local hash
    let file_hash = client.upload_file_to_storage(&file_path).await.unwrap();

    // Build and submit STORE message
    let pending = StoreBuilder::new(&account, file_hash.clone(), StorageEngine::Storage)
        .build()
        .unwrap();

    assert_eq!(pending.message_type, MessageType::Store);
    assert_eq!(pending.item_type, ItemType::Inline);

    let resp = client.post_message(&pending, true).await.unwrap();
    assert_eq!(resp.message_status, "processed");

    // Verify the message can be fetched back
    let fetched = client.get_message(&pending.item_hash).await.unwrap();
    assert_eq!(fetched.status(), MessageStatus::Processed);

    // Verify the uploaded file is downloadable
    let download = client.download_file_by_hash(&file_hash).await.unwrap();
    let bytes = download.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), file_content);
}

/// Test the create_store convenience method (upload + build + submit in one call).
#[tokio::test]
async fn test_create_store_convenience() {
    use aleph_sdk::client::{AlephClient, AlephMessageClient};
    use aleph_types::message::StorageEngine;
    use url::Url;

    let base_url = start_test_server();
    let client = AlephClient::new(Url::parse(&base_url).unwrap());

    let key = [1u8; 32];
    let account = EvmAccount::new(Chain::Ethereum, &key).unwrap();

    // Write a temp file
    let tmpdir = tempfile::tempdir().unwrap();
    let file_path = tmpdir.path().join("create-store-test.txt");
    std::fs::write(&file_path, b"create_store convenience test").unwrap();

    // One-call: upload + build + submit
    let resp = client
        .create_store(&account, &file_path, StorageEngine::Storage, true)
        .await
        .unwrap();
    assert_eq!(resp.message_status, "processed");
}
