//! End-to-end test for `aleph file upload --json`.
//!
//! Spawns a real heph server on an OS-assigned port, runs the compiled `aleph`
//! binary against it with `--json --storage-engine storage`, and asserts that
//! stdout is a parseable JSON envelope with the expected submission fields.
//!
//! Regression guard for the #142 bug where the default (native storage) path
//! dropped the return value of `upload_file_to_storage` and emitted nothing
//! to stdout, breaking downstream tooling that calls `json.loads(stdout)`.

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
            "--ccn-url",
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
