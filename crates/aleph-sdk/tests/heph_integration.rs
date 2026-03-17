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
