//! Integration tests for IPFS folder uploads.

use aleph_sdk::client::AlephClient;
use aleph_sdk::ipfs::{CidVersion, UploadFolderOptions};
use std::fs;
use tempfile::TempDir;
use url::Url;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_temp_dir(files: &[(&str, &str)]) -> TempDir {
    let tmp = TempDir::new().unwrap();
    for (rel, content) in files {
        let abs = tmp.path().join(rel);
        if let Some(parent) = abs.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&abs, content).unwrap();
    }
    tmp
}

// 46-char CIDv0 strings (placeholder values — they only need to parse as Cid)
const FILE_CID: &str = "QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ROOT_CID: &str = "QmRoot1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn mock_body() -> String {
    format!(
        "{{\"Name\":\"hello.txt\",\"Hash\":\"{FILE_CID}\",\"Size\":\"5\"}}\n{{\"Name\":\"\",\"Hash\":\"{ROOT_CID}\",\"Size\":\"100\"}}\n"
    )
}

fn gateway_url(server_uri: &str) -> Url {
    // The SDK uses an absolute-path join (`/api/v0/...`), so the base URL's
    // trailing slash doesn't matter — but parsing as-is keeps it minimal.
    Url::parse(server_uri).unwrap()
}

#[tokio::test]
async fn upload_folder_returns_root_cid_from_ndjson() {
    let server = MockServer::start().await;
    let tmp = make_temp_dir(&[("hello.txt", "hello")]);

    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("wrap-with-directory", "true"))
        .and(query_param("cid-version", "1"))
        .and(query_param("raw-leaves", "true"))
        .and(query_param("pin", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(mock_body()))
        .expect(1)
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse("https://ccn.example.com").unwrap())
        .with_ipfs_gateway(gateway_url(&server.uri()));

    let hash = client
        .upload_folder_to_ipfs(tmp.path(), UploadFolderOptions::default())
        .await
        .expect("upload should succeed against the mock");

    assert_eq!(hash.to_string(), ROOT_CID);
}

#[tokio::test]
async fn upload_folder_v0_omits_raw_leaves() {
    let server = MockServer::start().await;
    let tmp = make_temp_dir(&[("a.txt", "a")]);

    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("cid-version", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_string(mock_body()))
        .expect(1)
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse("https://ccn.example.com").unwrap())
        .with_ipfs_gateway(gateway_url(&server.uri()));

    let opts = UploadFolderOptions {
        cid_version: CidVersion::V0,
        ..Default::default()
    };
    client
        .upload_folder_to_ipfs(tmp.path(), opts)
        .await
        .expect("upload should succeed against the mock");
}

#[tokio::test]
async fn upload_folder_rejects_empty_directory() {
    let tmp = TempDir::new().unwrap();
    let server = MockServer::start().await;

    let client = AlephClient::new(Url::parse("https://ccn.example.com").unwrap())
        .with_ipfs_gateway(gateway_url(&server.uri()));

    let err = client
        .upload_folder_to_ipfs(tmp.path(), UploadFolderOptions::default())
        .await
        .unwrap_err();

    assert!(format!("{err}").contains("empty folder"));
}

#[tokio::test]
async fn upload_folder_surfaces_403_as_ipfs_disabled() {
    let server = MockServer::start().await;
    let tmp = make_temp_dir(&[("a.txt", "a")]);

    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse("https://ccn.example.com").unwrap())
        .with_ipfs_gateway(gateway_url(&server.uri()));

    let err = client
        .upload_folder_to_ipfs(tmp.path(), UploadFolderOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, aleph_sdk::client::StorageError::IpfsDisabled));
}
