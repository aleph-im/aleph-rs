//! Integration tests that run against a local heph instance.
//!
//! These tests require a running heph server. Set the `ALEPH_TEST_CCN_URL`
//! environment variable to the base URL (e.g. `http://127.0.0.1:4024`).
//!
//! All tests are `#[ignore]`d by default so that `cargo test` works without a
//! running heph.  CI runs them with `--include-ignored`.

use aleph_sdk::client::{AlephClient, AlephStorageClient};
use aleph_types::item_hash::ItemHash;
use memsizes::Bytes;
use url::Url;

#[cfg(feature = "account-evm")]
use aleph_sdk::client::{AlephMessageClient, AlephPostClient, MessageFilter, PostFilter};
#[cfg(feature = "account-evm")]
use aleph_sdk::messages::PostBuilder;
#[cfg(feature = "account-evm")]
use aleph_types::account::EvmAccount;
#[cfg(feature = "account-evm")]
use aleph_types::chain::Chain;
#[cfg(feature = "account-evm")]
use futures_util::StreamExt;

fn ccn_url() -> Url {
    let raw = std::env::var("ALEPH_TEST_CCN_URL")
        .expect("ALEPH_TEST_CCN_URL must be set to run integration tests");
    Url::parse(&raw).expect("ALEPH_TEST_CCN_URL must be a valid URL")
}

// ---------------------------------------------------------------------------
// Storage: upload then query size / download
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_upload_to_storage() {
    let client = AlephClient::new(ccn_url());
    let data = b"hello aleph storage";

    let hash = client
        .upload_to_storage(data)
        .await
        .expect("upload should succeed");

    assert!(matches!(hash, ItemHash::Native(_)));

    let size = client
        .get_file_size(&hash)
        .await
        .expect("file should exist");
    assert_eq!(size, Bytes::from(data.len() as u64));
}

#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_get_file_size() {
    let client = AlephClient::new(ccn_url());
    let data = b"size-check payload";

    let hash = client
        .upload_to_storage(data)
        .await
        .expect("upload should succeed");

    let size = client
        .get_file_size(&hash)
        .await
        .expect("should get file size");
    assert_eq!(size, Bytes::from(data.len() as u64));
}

#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_download_file_by_hash() {
    let client = AlephClient::new(ccn_url());
    let data = b"download-by-hash payload";

    let hash = client
        .upload_to_storage(data)
        .await
        .expect("upload should succeed");

    let download = client
        .download_file_by_hash(&hash)
        .await
        .expect("download should succeed");

    let content = download.bytes().await.expect("should read bytes");
    assert_eq!(content.len(), data.len());
    assert_eq!(&content[..], data);
}

#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_download_file_to_disk() {
    let client = AlephClient::new(ccn_url());
    let data = b"download-to-disk payload";

    let hash = client
        .upload_to_storage(data)
        .await
        .expect("upload should succeed");

    let tmp_dir = std::env::temp_dir();
    let tmp_file = tmp_dir.join("aleph-heph-test-download");

    let download = client
        .download_file_by_hash(&hash)
        .await
        .expect("download should succeed");

    download
        .to_file(&tmp_file)
        .await
        .expect("should write to file");

    let metadata = std::fs::metadata(&tmp_file).expect("file should exist");
    assert_eq!(metadata.len(), data.len() as u64);

    std::fs::remove_file(&tmp_file).ok();
}

#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_download_file_with_verification() {
    let client = AlephClient::new(ccn_url());
    let data = b"verified-download payload";

    let hash = client
        .upload_to_storage(data)
        .await
        .expect("upload should succeed");

    let download = client
        .download_file_by_hash(&hash)
        .await
        .expect("download should succeed");

    let content = download
        .with_verification()
        .bytes()
        .await
        .expect("verified download should succeed");
    assert_eq!(content.len(), data.len());
    assert_eq!(&content[..], data);
}

#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_download_file_to_disk_with_verification() {
    let client = AlephClient::new(ccn_url());
    let data = b"verified-disk-download payload";

    let hash = client
        .upload_to_storage(data)
        .await
        .expect("upload should succeed");

    let tmp_dir = std::env::temp_dir();
    let tmp_file = tmp_dir.join("aleph-heph-test-download-verified");

    let download = client
        .download_file_by_hash(&hash)
        .await
        .expect("download should succeed");

    download
        .with_verification()
        .to_file(&tmp_file)
        .await
        .expect("verified write to file should succeed");

    let metadata = std::fs::metadata(&tmp_file).expect("file should exist");
    assert_eq!(metadata.len(), data.len() as u64);

    std::fs::remove_file(&tmp_file).ok();
}

// ---------------------------------------------------------------------------
// Auto-paginating iterators
// ---------------------------------------------------------------------------

#[cfg(feature = "account-evm")]
const EVM_TEST_KEY: [u8; 32] = [
    0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3, 0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff, 0x94,
    0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfb, 0xba, 0x0f, 0x2d, 0x1d, 0xb7, 0x44, 0xce, 0x06,
];

#[cfg(feature = "account-evm")]
#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_get_messages_iterator_multi_page() {
    let client = AlephClient::new(ccn_url());
    let account = EvmAccount::new(Chain::Ethereum, &EVM_TEST_KEY).unwrap();

    // Post 3 messages with a unique tag so we can filter for them.
    let tag = format!("iter-test-{}", std::process::id());
    let mut posted_hashes = Vec::new();
    for i in 0..3 {
        let msg = PostBuilder::new(&account, &tag, serde_json::json!({"index": i}))
            .unwrap()
            .build()
            .unwrap();
        posted_hashes.push(msg.item_hash.clone());
        client
            .post_message(&msg, true)
            .await
            .expect("post_message should succeed");
    }

    // Iterate with pagination=2 to force at least 2 pages for 3 messages.
    let filter = MessageFilter {
        content_types: Some(vec![tag.clone()]),
        pagination: Some(2),
        ..Default::default()
    };
    let items: Vec<_> = client
        .get_messages_iterator(filter)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("iterator should not error");

    assert_eq!(items.len(), 3, "should get all 3 messages across pages");

    // Verify all posted hashes are present.
    for hash in &posted_hashes {
        assert!(
            items.iter().any(|m| &m.item_hash == hash),
            "message {hash} should be in iterator results"
        );
    }
}

#[cfg(feature = "account-evm")]
#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_get_messages_iterator_empty() {
    let client = AlephClient::new(ccn_url());

    let filter = MessageFilter {
        content_types: Some(vec!["nonexistent-type-that-matches-nothing".to_string()]),
        ..Default::default()
    };
    let items: Vec<_> = client
        .get_messages_iterator(filter)
        .collect::<Vec<_>>()
        .await;

    assert!(
        items.is_empty(),
        "should yield zero items for unmatched filter"
    );
}

#[cfg(feature = "account-evm")]
#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_get_posts_v0_iterator_multi_page() {
    let client = AlephClient::new(ccn_url());
    let account = EvmAccount::new(Chain::Ethereum, &EVM_TEST_KEY).unwrap();

    let tag = format!("posts-v0-iter-{}", std::process::id());
    let mut posted_hashes = Vec::new();
    for i in 0..3 {
        let msg = PostBuilder::new(&account, &tag, serde_json::json!({"index": i}))
            .unwrap()
            .build()
            .unwrap();
        posted_hashes.push(msg.item_hash.clone());
        client
            .post_message(&msg, true)
            .await
            .expect("post_message should succeed");
    }

    let filter = PostFilter {
        post_types: Some(vec![tag.clone()]),
        pagination: Some(2),
        ..Default::default()
    };
    let items: Vec<_> = client
        .get_posts_v0_iterator(filter)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("iterator should not error");

    assert_eq!(items.len(), 3, "should get all 3 posts across pages");

    // Verify all posted hashes are present.
    for hash in &posted_hashes {
        assert!(
            items.iter().any(|p| &p.original_item_hash == hash),
            "post {hash} should be in iterator results"
        );
    }
}

#[cfg(feature = "account-evm")]
#[tokio::test]
#[ignore = "requires a running heph instance"]
async fn test_get_posts_v1_iterator_multi_page() {
    let client = AlephClient::new(ccn_url());
    let account = EvmAccount::new(Chain::Ethereum, &EVM_TEST_KEY).unwrap();

    let tag = format!("posts-v1-iter-{}", std::process::id());
    let mut posted_hashes = Vec::new();
    for i in 0..3 {
        let msg = PostBuilder::new(&account, &tag, serde_json::json!({"index": i}))
            .unwrap()
            .build()
            .unwrap();
        posted_hashes.push(msg.item_hash.clone());
        client
            .post_message(&msg, true)
            .await
            .expect("post_message should succeed");
    }

    let filter = PostFilter {
        post_types: Some(vec![tag.clone()]),
        pagination: Some(2),
        ..Default::default()
    };
    let items: Vec<_> = client
        .get_posts_v1_iterator(filter)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("iterator should not error");

    assert_eq!(items.len(), 3, "should get all 3 posts across pages");

    // Verify all posted hashes are present.
    for hash in &posted_hashes {
        assert!(
            items.iter().any(|p| &p.original_item_hash == hash),
            "post {hash} should be in iterator results"
        );
    }
}
