//! End-to-end tests for `aleph file upload`.
//!
//! - `file_upload_json_emits_submission_envelope`: storage engine, runs the
//!   compiled `aleph` binary against a real heph server.
//! - `ipfs_upload_uses_authenticated_request_and_no_separate_post`: IPFS
//!   engine, runs against a `wiremock::MockServer` (heph has no IPFS
//!   endpoint) and asserts the single-request authenticated shape.

use std::process::Command;
use std::sync::Arc;

use actix_web::{App, HttpServer, web};
use aleph_types::account::{Account, EvmAccount};
use aleph_types::chain::Chain;

use heph::api::{AppState, configure_routes};
use heph::config::HephConfig;
use heph::db::Db;
use heph::files::FileStore;

/// Deterministic test key. The corresponding address is pre-seeded with credits
/// so authenticated uploads clear the balance check.
const TEST_KEY: [u8; 32] = [1u8; 32];

/// Start a heph server on an OS-assigned port. Returns the base URL.
fn start_test_server() -> String {
    let db = Arc::new(Db::open_in_memory().unwrap());
    let tmpdir = tempfile::tempdir().unwrap();
    let file_store = Arc::new(FileStore::new(&tmpdir.keep().join("files")).unwrap());

    let account = EvmAccount::new(Chain::Ethereum, &TEST_KEY).unwrap();
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

    std::thread::spawn(move || {
        let state = web::Data::new(AppState {
            db,
            file_store,
            config,
            corechannel: std::sync::Mutex::new(heph::corechannel::CoreChannelState::new()),
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
            tx.send(port).unwrap();

            server.run().await.unwrap();
        });
    });

    let port = rx.recv().unwrap();
    format!("http://127.0.0.1:{port}/")
}

/// Regression guard for the #142 bug where the default (native storage) path
/// dropped the return value of `upload_file_to_storage` and emitted nothing
/// to stdout, breaking downstream tooling that calls `json.loads(stdout)`.
#[test]
fn file_upload_json_emits_submission_envelope() {
    let base_url = start_test_server();

    let tmpdir = tempfile::tempdir().unwrap();
    let file_path = tmpdir.path().join("upload.txt");
    let contents = b"aleph cli file upload integration test";
    std::fs::write(&file_path, contents).unwrap();

    let private_key_hex = hex::encode(TEST_KEY);

    let output = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "--ccn",
            &base_url,
            "--json",
            "file",
            "upload",
            "--storage-engine",
            "storage",
            "--private-key",
            &private_key_hex,
            "--chain",
            "eth",
            file_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to spawn aleph binary");

    assert!(
        output.status.success(),
        "aleph exited with {:?}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    assert!(
        !stdout.trim().is_empty(),
        "stdout was empty — regression from #142 (JSON output dropped on storage engine)"
    );

    let envelope: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should be valid JSON");

    let account = EvmAccount::new(Chain::Ethereum, &TEST_KEY).unwrap();
    let expected_sender = account.address().as_str().to_string();

    // Shape mirrors the IPFS branch's envelope (see common::print_submission_result).
    assert!(envelope["item_hash"].is_string(), "envelope: {envelope}");
    assert_eq!(envelope["type"], "STORE");
    assert_eq!(envelope["chain"], "ETH");
    assert_eq!(envelope["sender"], expected_sender);
    assert!(envelope["time"].is_number(), "envelope: {envelope}");
    assert!(envelope["explorer_url"].is_string(), "envelope: {envelope}");
    assert_eq!(envelope["publication_status"], "success");
    assert_eq!(envelope["message_status"], "processed");
}

#[tokio::test]
async fn ipfs_upload_uses_authenticated_request_and_no_separate_post() {
    use std::io::Write;
    use tempfile::NamedTempFile;
    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    let bytes: &[u8] = b"hello";
    let expected_cid = aleph_sdk::verify::compute_cid(bytes).to_string();

    let server = MockServer::start().await;

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

    // After the authenticated upload, the CLI fetches message status via
    // `GET /api/v0/messages/{hash}` to catch the case where the upload
    // succeeded HTTP-wise but the STORE message was rejected (e.g. insufficient
    // credits on pyaleph). Mock a Pending response here: the test's actual
    // intent is to assert the request shape below, not the specific status.
    // The simpler Pending payload avoids constructing a full Message JSON.
    let test_sender = EvmAccount::new(Chain::Ethereum, &TEST_KEY)
        .unwrap()
        .address()
        .as_str()
        .to_string();
    let pending_body = format!(
        r#"{{"status":"pending","messages":[{{"sender":"{test_sender}","chain":"ETH","signature":null,"item_type":"inline","item_content":"{{}}","type":"STORE","item_hash":"0000000000000000000000000000000000000000000000000000000000000000","time":"2026-05-19T00:00:00Z","channel":null,"content":null}}]}}"#
    );
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v0/messages/[^/]+$"))
        .respond_with(ResponseTemplate::new(200).set_body_string(pending_body))
        .expect(1)
        .mount(&server)
        .await;

    // Critical regression guard: the IPFS auth path must NOT submit a
    // separate /api/v0/messages POST. If it does, the test fails (expect(0)).
    Mock::given(method("POST"))
        .and(path("/api/v0/messages"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let mut tmp = NamedTempFile::new().unwrap();
    tmp.write_all(bytes).unwrap();
    let file_path = tmp.path().to_path_buf();

    let private_key_hex = hex::encode(TEST_KEY);
    let base_url = server.uri();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_aleph"))
            .args([
                "--ccn",
                &base_url,
                "--json",
                "file",
                "upload",
                "--storage-engine",
                "ipfs",
                "--private-key",
                &private_key_hex,
                "--chain",
                "eth",
                file_path.to_str().unwrap(),
            ])
            .output()
            .expect("failed to spawn aleph binary")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "aleph exited non-zero\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let envelope: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should be valid JSON");

    let account = EvmAccount::new(Chain::Ethereum, &TEST_KEY).unwrap();
    let expected_sender = Account::address(&account).as_str().to_string();

    assert!(envelope["item_hash"].is_string(), "envelope: {envelope}");
    assert_eq!(envelope["type"], "STORE");
    assert_eq!(envelope["chain"], "ETH");
    assert_eq!(envelope["sender"], expected_sender);
    assert!(envelope["time"].is_number(), "envelope: {envelope}");
    assert!(envelope["explorer_url"].is_string(), "envelope: {envelope}");
    assert_eq!(envelope["publication_status"], "success");
    // Mock returns Pending (see the GET mock above). The point of this test
    // is the request shape, not the specific message status string.
    assert_eq!(envelope["message_status"], "pending");

    // Verify the request shape: exactly one POST to /api/v0/ipfs/add_file
    // with both `file` and `metadata` parts in the multipart body.
    let reqs: Vec<Request> = server.received_requests().await.unwrap();
    let ipfs_reqs: Vec<&Request> = reqs
        .iter()
        .filter(|r| r.url.path() == "/api/v0/ipfs/add_file")
        .collect();
    assert_eq!(ipfs_reqs.len(), 1, "expected exactly one IPFS upload");
    let body = std::str::from_utf8(&ipfs_reqs[0].body).expect("multipart body should be utf-8");
    assert!(body.contains("name=\"file\""), "missing file part: {body}");
    assert!(
        body.contains("name=\"metadata\""),
        "missing metadata part: {body}"
    );
    assert!(
        body.contains("\"type\":\"STORE\""),
        "missing STORE type: {body}"
    );
    assert!(
        body.contains(&format!("\"sender\":\"{expected_sender}\"")),
        "metadata should contain expected sender: {body}"
    );
}

/// pyaleph's `/api/v0/ipfs/add_file` returns HTTP 200 even when the STORE
/// message is subsequently rejected (e.g. insufficient credits). The CLI must
/// follow up with a `GET /api/v0/messages/{hash}` and surface the rejection
/// as a non-zero exit, instead of lying with "Message processed".
#[tokio::test]
async fn ipfs_upload_surfaces_rejection_from_message_status() {
    use std::io::Write;
    use tempfile::NamedTempFile;
    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let bytes: &[u8] = b"hello";
    let expected_cid = aleph_sdk::verify::compute_cid(bytes).to_string();

    let server = MockServer::start().await;

    let upload_body = format!(
        r#"{{"status":"success","hash":"{expected_cid}","name":"upload","size":{}}}"#,
        bytes.len()
    );
    Mock::given(method("POST"))
        .and(path("/api/v0/ipfs/add_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(upload_body))
        .expect(1)
        .mount(&server)
        .await;

    // GET status returns Rejected with error_code 6 (insufficient credit
    // balance). This is the exact scenario the user hit in production.
    let test_sender = EvmAccount::new(Chain::Ethereum, &TEST_KEY)
        .unwrap()
        .address()
        .as_str()
        .to_string();
    let rejected_body = format!(
        r#"{{"status":"rejected","message":{{"sender":"{test_sender}","chain":"ETH","signature":null,"type":"STORE","item_type":"storage","item_content":null,"item_hash":"0000000000000000000000000000000000000000000000000000000000000000","time":1234567890.0,"channel":null,"content":null}},"error_code":6}}"#
    );
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v0/messages/[^/]+$"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rejected_body))
        .expect(1)
        .mount(&server)
        .await;

    let mut tmp = NamedTempFile::new().unwrap();
    tmp.write_all(bytes).unwrap();
    let file_path = tmp.path().to_path_buf();

    let private_key_hex = hex::encode(TEST_KEY);
    let base_url = server.uri();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_aleph"))
            .args([
                "--ccn",
                &base_url,
                "file",
                "upload",
                "--storage-engine",
                "ipfs",
                "--private-key",
                &private_key_hex,
                "--chain",
                "eth",
                file_path.to_str().unwrap(),
            ])
            .output()
            .expect("failed to spawn aleph binary")
    })
    .await
    .unwrap();

    assert!(
        !output.status.success(),
        "CLI must exit non-zero on rejected message; got exit {:?}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("rejected") && stderr.contains("6"),
        "stderr must mention rejection and error code 6, got:\n{stderr}"
    );
    assert!(
        stderr.contains("insufficient credit balance"),
        "stderr must describe error code 6 as 'insufficient credit balance', got:\n{stderr}"
    );
}
