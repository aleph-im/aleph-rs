//! Tests for AlephClient::upload_folder_to_ipfs_authenticated.

use aleph_sdk::client::{AlephClient, StorageError};
use aleph_sdk::folder_hash::hash_folder_root;
use aleph_sdk::ipfs::{UploadFolderOptions, collect_folder_files};
use aleph_sdk::messages::StoreBuilder;
use aleph_types::account::{Account, SignError};
use aleph_types::chain::{Address, Chain, Signature};
use aleph_types::item_hash::ItemHash;
use aleph_types::message::StorageEngine;
use aleph_types::message::pending::PendingMessage;
use std::path::Path;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// A separate fixture CID used for the "tampered" cross-mismatch test.
const TAMPERED_CID: &str = "bafybeibwzifw72ttrkqglhi64gn3stoyjs6t2vcyfzr67gqkogfgcyo3uy";

/// Minimal Account for test fixtures. Mirrors `ipfs_authenticated_upload.rs`.
struct TestAccount {
    address: Address,
}

impl TestAccount {
    fn new() -> Self {
        Self {
            address: Address::from("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".to_string()),
        }
    }
}

impl Account for TestAccount {
    fn chain(&self) -> Chain {
        Chain::Ethereum
    }
    fn address(&self) -> &Address {
        &self.address
    }
    fn sign_raw(&self, _buffer: &[u8]) -> Result<Signature, SignError> {
        Ok(Signature::from("0xDUMMY".to_string()))
    }
}

fn fake_pending_store_for(cid_str: &str) -> PendingMessage {
    let account = TestAccount::new();
    let hash: ItemHash = cid_str.parse().unwrap();
    StoreBuilder::new(&account, hash, StorageEngine::Ipfs)
        .build()
        .unwrap()
}

fn make_test_folder() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("a.txt"), b"hello").unwrap();
    std::fs::write(tmp.path().join("b.txt"), b"world").unwrap();
    tmp
}

fn build_store_message_for(folder: &Path) -> (PendingMessage, ItemHash) {
    let entries = collect_folder_files(folder, true).unwrap();
    let opts = UploadFolderOptions::default();
    let root = hash_folder_root(&entries, &opts).unwrap();
    let pending = fake_pending_store_for(&root.to_string());
    (pending, root)
}

#[tokio::test]
async fn upload_folder_authenticated_happy_path() {
    let server = MockServer::start().await;
    let folder = make_test_folder();
    let (pending, root) = build_store_message_for(folder.path());
    let root_str = root.to_string();

    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_car"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "success",
            "hash": &root_str,
            "size": 100,
        })))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let opts = UploadFolderOptions::default();
    let got = client
        .upload_folder_to_ipfs_authenticated(folder.path(), &pending, false, opts)
        .await
        .expect("upload should succeed");
    assert_eq!(got.to_string(), root_str);
}

#[tokio::test]
async fn upload_folder_authenticated_classifies_invalid_signature() {
    let server = MockServer::start().await;
    let folder = make_test_folder();
    let (pending, _) = build_store_message_for(folder.path());

    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_car"))
        .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let opts = UploadFolderOptions::default();
    let err = client
        .upload_folder_to_ipfs_authenticated(folder.path(), &pending, false, opts)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::InvalidSignature), "got {err:?}");
}

#[tokio::test]
async fn upload_folder_authenticated_classifies_ipfs_disabled() {
    let server = MockServer::start().await;
    let folder = make_test_folder();
    let (pending, _) = build_store_message_for(folder.path());

    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_car"))
        .respond_with(ResponseTemplate::new(403).set_body_string("IPFS is disabled on this node"))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let opts = UploadFolderOptions::default();
    let err = client
        .upload_folder_to_ipfs_authenticated(folder.path(), &pending, false, opts)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::IpfsDisabled), "got {err:?}");
}

#[tokio::test]
async fn upload_folder_authenticated_classifies_car_header_root_mismatch() {
    let server = MockServer::start().await;
    let folder = make_test_folder();
    let (pending, _) = build_store_message_for(folder.path());

    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_car"))
        .respond_with(
            ResponseTemplate::new(422).set_body_string("Root CID does not match (bafyA != bafyB)"),
        )
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let opts = UploadFolderOptions::default();
    let err = client
        .upload_folder_to_ipfs_authenticated(folder.path(), &pending, false, opts)
        .await
        .unwrap_err();
    match err {
        StorageError::CarHeaderRootMismatch {
            car_root,
            metadata_root,
        } => {
            assert_eq!(car_root, "bafyA");
            assert_eq!(metadata_root, "bafyB");
        }
        other => panic!("expected CarHeaderRootMismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn upload_folder_authenticated_classifies_imported_root_mismatch() {
    let server = MockServer::start().await;
    let folder = make_test_folder();
    let (pending, _) = build_store_message_for(folder.path());

    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_car"))
        .respond_with(ResponseTemplate::new(422).set_body_string(
            "Imported root does not match expected (bafyKubo != bafyExpected); CAR header declared a root that does not correspond to the imported DAG",
        ))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let opts = UploadFolderOptions::default();
    let err = client
        .upload_folder_to_ipfs_authenticated(folder.path(), &pending, false, opts)
        .await
        .unwrap_err();
    match err {
        StorageError::ImportedRootMismatch {
            kubo_root,
            expected_root,
        } => {
            assert_eq!(kubo_root, "bafyKubo");
            assert_eq!(expected_root, "bafyExpected");
        }
        other => panic!("expected ImportedRootMismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn upload_folder_authenticated_classifies_insufficient_balance() {
    let server = MockServer::start().await;
    let folder = make_test_folder();
    let (pending, _) = build_store_message_for(folder.path());

    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_car"))
        .respond_with(ResponseTemplate::new(402).set_body_string("Insufficient balance"))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let opts = UploadFolderOptions::default();
    let err = client
        .upload_folder_to_ipfs_authenticated(folder.path(), &pending, false, opts)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::InsufficientBalance),
        "got {err:?}"
    );
}

#[tokio::test]
async fn upload_folder_authenticated_classifies_too_large() {
    let server = MockServer::start().await;
    let folder = make_test_folder();
    let (pending, _) = build_store_message_for(folder.path());

    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_car"))
        .respond_with(ResponseTemplate::new(413).set_body_string("File too large"))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let opts = UploadFolderOptions::default();
    let err = client
        .upload_folder_to_ipfs_authenticated(folder.path(), &pending, false, opts)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::FileTooLarge), "got {err:?}");
}

#[tokio::test]
async fn upload_folder_authenticated_classifies_502_as_backend_unavailable() {
    let server = MockServer::start().await;
    let folder = make_test_folder();
    let (pending, _) = build_store_message_for(folder.path());

    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_car"))
        .respond_with(
            ResponseTemplate::new(502).set_body_string("Failed to import CAR into IPFS: kubo down"),
        )
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let opts = UploadFolderOptions::default();
    let err = client
        .upload_folder_to_ipfs_authenticated(folder.path(), &pending, false, opts)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::IpfsBackendUnavailable(ref s) if s.contains("kubo down")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn upload_folder_authenticated_rejects_local_root_metadata_mismatch() {
    // Caller signs a STORE message with a different CID than what the folder
    // hashes to. The SDK should fail with CidMismatch before contacting the
    // server.
    let server = MockServer::start().await; // unused; catches any accidental request
    let folder = make_test_folder();
    let pending = fake_pending_store_for(TAMPERED_CID);

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let opts = UploadFolderOptions::default();
    let err = client
        .upload_folder_to_ipfs_authenticated(folder.path(), &pending, false, opts)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::CidMismatch { .. }),
        "got {err:?}"
    );
    let reqs = server.received_requests().await.unwrap();
    assert!(
        reqs.is_empty(),
        "unexpected request reached the mock server"
    );
}
