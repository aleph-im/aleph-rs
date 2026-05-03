//! Integration tests for authenticated IPFS uploads against a mock CCN.
//!
//! These tests exercise `upload_to_ipfs` and `upload_file_to_ipfs` with a
//! signed STORE message in the multipart form, mirroring what
//! `aleph file upload --storage-engine ipfs` does at the CLI layer.

use aleph_sdk::client::{AlephClient, AlephStorageClient, StorageError};
use aleph_sdk::messages::StoreBuilder;
use aleph_types::account::{Account, SignError};
use aleph_types::chain::{Address, Chain, Signature};
use aleph_types::item_hash::ItemHash;
use aleph_types::message::StorageEngine;
use aleph_types::message::pending::PendingMessage;
use std::io::Write;
use tempfile::NamedTempFile;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// A real-shaped CIDv0 that parses as ItemHash::Ipfs, used for upload_to_ipfs
// tests that have no integrity check.
const FAKE_CID: &str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";

/// Minimal Account implementation for test fixtures.
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

fn fake_pending_message(hash_str: &str) -> PendingMessage {
    let account = TestAccount::new();
    let hash: ItemHash = hash_str.parse().unwrap();
    StoreBuilder::new(&account, hash, StorageEngine::Ipfs)
        .build()
        .unwrap()
}

fn small_temp_file(bytes: &[u8]) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(bytes).unwrap();
    f
}

#[tokio::test]
async fn upload_with_metadata_returns_cid() {
    let server = MockServer::start().await;
    let body = format!(r#"{{"status":"success","hash":"{FAKE_CID}","name":"upload","size":5}}"#);
    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let msg = fake_pending_message(FAKE_CID);
    let hash = client
        .upload_to_ipfs(b"hello", Some(&msg), false)
        .await
        .expect("upload should succeed");
    assert_eq!(hash.to_string(), FAKE_CID);

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let body = std::str::from_utf8(&reqs[0].body).expect("multipart body should be utf-8");
    assert!(
        body.contains("name=\"metadata\""),
        "metadata multipart part should be present: {body}"
    );
    assert!(
        body.contains("\"type\":\"STORE\""),
        "STORE message JSON should be present: {body}"
    );
    assert!(
        body.contains("\"sync\":false"),
        "sync flag should be serialized: {body}"
    );
}

#[tokio::test]
async fn upload_without_metadata_omits_part() {
    let server = MockServer::start().await;
    let body = format!(r#"{{"status":"success","hash":"{FAKE_CID}","name":"upload","size":5}}"#);
    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let hash = client
        .upload_to_ipfs(b"hello", None, false)
        .await
        .expect("unauthenticated upload should still succeed");
    assert_eq!(hash.to_string(), FAKE_CID);

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let body = std::str::from_utf8(&reqs[0].body).expect("multipart body should be utf-8");
    assert!(
        !body.contains("name=\"metadata\""),
        "metadata multipart part should be ABSENT: {body}"
    );
}

#[tokio::test]
async fn upload_402_maps_to_insufficient_balance() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_file"))
        .respond_with(ResponseTemplate::new(402))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let msg = fake_pending_message(FAKE_CID);
    let err = client
        .upload_to_ipfs(b"hello", Some(&msg), false)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::InsufficientBalance),
        "got {err:?}"
    );
}

#[tokio::test]
async fn upload_403_with_ipfs_disabled_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_file"))
        .respond_with(
            ResponseTemplate::new(403).set_body_string("403: IPFS is disabled on this node"),
        )
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let err = client
        .upload_to_ipfs(b"hello", None, false)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::IpfsDisabled), "got {err:?}");
}

#[tokio::test]
async fn upload_403_otherwise_maps_to_invalid_signature() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_file"))
        .respond_with(ResponseTemplate::new(403).set_body_string("403: Forbidden"))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let msg = fake_pending_message(FAKE_CID);
    let err = client
        .upload_to_ipfs(b"hello", Some(&msg), false)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::InvalidSignature), "got {err:?}");
}

#[tokio::test]
async fn upload_422_maps_to_invalid_metadata_with_body() {
    let server = MockServer::start().await;
    let reason = "File hash does not match (X != Y)";
    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_file"))
        .respond_with(ResponseTemplate::new(422).set_body_string(reason))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let msg = fake_pending_message(FAKE_CID);
    let err = client
        .upload_to_ipfs(b"hello", Some(&msg), false)
        .await
        .unwrap_err();
    match err {
        StorageError::InvalidMetadata(s) => assert_eq!(s, reason),
        other => panic!("expected InvalidMetadata, got {other:?}"),
    }
}

#[tokio::test]
async fn upload_413_maps_to_file_too_large() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_file"))
        .respond_with(ResponseTemplate::new(413))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    let err = client
        .upload_to_ipfs(b"hello", None, false)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::FileTooLarge), "got {err:?}");
}

#[tokio::test]
async fn upload_file_with_metadata_returns_cid() {
    let server = MockServer::start().await;
    let bytes: &[u8] = b"hello";
    // Compute the CID the SDK will compute locally — keeps this test
    // honest if Hasher::for_ipfs() ever changes (the unit tests in
    // verify.rs catch that first).
    let expected_cid = aleph_sdk::verify::compute_cid(bytes).to_string();

    let body = format!(
        r#"{{"status":"success","hash":"{expected_cid}","name":"upload","size":{}}}"#,
        bytes.len()
    );
    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;

    let f = small_temp_file(bytes);
    let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
    // The store message must reference the same CID for the local + server
    // hashes to match.
    let hash: ItemHash = expected_cid.parse().unwrap();
    let account = TestAccount::new();
    let msg = StoreBuilder::new(&account, hash, StorageEngine::Ipfs)
        .build()
        .unwrap();

    let result = client
        .upload_file_to_ipfs(f.path(), Some(&msg), true) // sync=true
        .await
        .expect("upload should succeed");
    assert_eq!(result.to_string(), expected_cid);

    // Round-trip verification of the metadata part.
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let body = std::str::from_utf8(&reqs[0].body).expect("multipart body should be utf-8");
    assert!(body.contains("name=\"metadata\""), "metadata part missing");
    assert!(
        body.contains("\"sync\":true"),
        "sync=true should be serialized"
    );
}
