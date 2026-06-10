//! Integration tests for IPFS folder uploads.

use aleph_sdk::client::AlephClient;
use aleph_sdk::ipfs::UploadFolderOptions;
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

fn gateway_url(server_uri: &str) -> Url {
    // The SDK uses an absolute-path join (`/api/v0/...`), so the base URL's
    // trailing slash doesn't matter — but parsing as-is keeps it minimal.
    Url::parse(server_uri).unwrap()
}

#[tokio::test]
async fn upload_folder_returns_root_cid_when_gateway_matches_local() {
    let server = MockServer::start().await;
    let tmp = make_temp_dir(&[("hello.txt", "hello\n")]);

    // Compute what the local hasher will produce so the mock can echo it.
    let entries = aleph_sdk::ipfs::collect_folder_files(tmp.path(), true).unwrap();
    let opts = aleph_sdk::ipfs::UploadFolderOptions::default();
    let local_root =
        aleph_sdk::folder_hash::hash_folder_root(&entries, &opts).expect("local hash must succeed");
    let local_root_cid = match local_root {
        aleph_types::item_hash::ItemHash::Ipfs(c) => c.to_string(),
        _ => unreachable!(),
    };

    let body = format!(
        "{{\"Name\":\"hello.txt\",\"Hash\":\"QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"5\"}}\n\
         {{\"Name\":\"\",\"Hash\":\"{local_root_cid}\",\"Size\":\"100\"}}\n"
    );

    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("wrap-with-directory", "true"))
        .and(query_param("cid-version", "1"))
        .and(query_param("raw-leaves", "true"))
        .and(query_param("pin", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse("https://ccn.example.com").unwrap())
        .with_ipfs_gateway(gateway_url(&server.uri()));

    let hash = client
        .upload_folder_to_ipfs(tmp.path(), opts)
        .await
        .expect("upload should succeed against the mock");

    assert_eq!(hash.to_string(), local_root_cid);
}

#[tokio::test]
async fn upload_folder_v0_omits_raw_leaves_and_matches_local() {
    let server = MockServer::start().await;
    let tmp = make_temp_dir(&[("a.txt", "a")]);

    let entries = aleph_sdk::ipfs::collect_folder_files(tmp.path(), true).unwrap();
    let mut opts = aleph_sdk::ipfs::UploadFolderOptions::default();
    opts.cid_version = aleph_sdk::ipfs::CidVersion::V0;
    let local_root =
        aleph_sdk::folder_hash::hash_folder_root(&entries, &opts).expect("local hash must succeed");
    let local_root_cid = match local_root {
        aleph_types::item_hash::ItemHash::Ipfs(c) => c.to_string(),
        _ => unreachable!(),
    };

    let body = format!(
        "{{\"Name\":\"a.txt\",\"Hash\":\"QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"1\"}}\n\
         {{\"Name\":\"\",\"Hash\":\"{local_root_cid}\",\"Size\":\"100\"}}\n"
    );

    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("cid-version", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse("https://ccn.example.com").unwrap())
        .with_ipfs_gateway(gateway_url(&server.uri()));

    client
        .upload_folder_to_ipfs(tmp.path(), opts)
        .await
        .expect("upload should succeed against the mock");
}

#[tokio::test]
async fn upload_folder_errors_on_cid_mismatch() {
    let server = MockServer::start().await;
    let tmp = make_temp_dir(&[("hello.txt", "hello\n")]);

    // Mock returns a CID that will NOT match what we compute locally.
    let body = "{\"Name\":\"hello.txt\",\"Hash\":\"QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"5\"}\n\
                {\"Name\":\"\",\"Hash\":\"QmWrongRootaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"100\"}\n";

    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .expect(1)
        .mount(&server)
        .await;

    let client = AlephClient::new(Url::parse("https://ccn.example.com").unwrap())
        .with_ipfs_gateway(gateway_url(&server.uri()));

    let err = client
        .upload_folder_to_ipfs(tmp.path(), aleph_sdk::ipfs::UploadFolderOptions::default())
        .await
        .unwrap_err();

    match err {
        aleph_sdk::client::StorageError::CidMismatch { local, remote } => {
            assert_ne!(local, remote);
        }
        other => panic!("expected CidMismatch, got {other:?}"),
    }
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
        .expect(1)
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
