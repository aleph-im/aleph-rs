//! End-to-end CLI tests for the authenticated CAR upload path against
//! the heph stub at /api/v0/ipfs/add_car.

use std::process::Command;
use std::sync::Arc;

use actix_web::{App, HttpServer, web};
use aleph_types::account::{Account, EvmAccount};
use aleph_types::chain::Chain;
use heph::api::{AppState, configure_routes};
use heph::config::HephConfig;
use heph::db::Db;
use heph::files::FileStore;

/// Deterministic test key. The corresponding address is pre-seeded with
/// credits so authenticated uploads clear the balance check.
const TEST_KEY: [u8; 32] = [1u8; 32];

/// Start a heph server on an OS-assigned port. Returns the base URL.
///
/// Lifted verbatim from `file_upload_integration.rs`.
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

/// Happy path: the default `aleph file upload <dir>` routes to
/// /api/v0/ipfs/add_car (the authenticated CAR endpoint on the CCN) and
/// succeeds. Confirms that:
///   - the binary builds a valid CARv1 payload,
///   - the STORE message signature passes heph's verification,
///   - the response is parsed and a non-empty envelope is printed.
#[test]
fn dir_upload_default_uses_add_car_and_succeeds() {
    let base_url = start_test_server();
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("a.txt"), b"hello").unwrap();
    std::fs::write(tmpdir.path().join("b.txt"), b"world").unwrap();

    let private_key_hex = hex::encode(TEST_KEY);
    let output = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "--ccn",
            &base_url,
            "--json",
            "file",
            "upload",
            "--private-key",
            &private_key_hex,
            "--chain",
            "eth",
            tmpdir.path().to_str().unwrap(),
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

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        !stdout.trim().is_empty(),
        "expected non-empty stdout from JSON envelope"
    );

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
    assert_eq!(envelope["message_status"], "processed");
}

/// Smoke test: `--use-gateway-relay` takes a different code path from the
/// default authenticated CAR upload. The heph stub has no /api/v0/add
/// endpoint (only /api/v0/ipfs/add_car), so pointing --ipfs-gateway at the
/// same heph base URL forces a failure. A non-zero exit code confirms the
/// flag routed to the gateway path rather than add_car.
#[test]
fn dir_upload_with_use_gateway_relay_does_not_hit_add_car() {
    let base_url = start_test_server();
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("a.txt"), b"hello").unwrap();

    let private_key_hex = hex::encode(TEST_KEY);
    let output = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "--ccn",
            &base_url,
            "file",
            "upload",
            "--private-key",
            &private_key_hex,
            "--chain",
            "eth",
            "--use-gateway-relay",
            "--ipfs-gateway",
            &base_url,
            tmpdir.path().to_str().unwrap(),
        ])
        .output()
        .expect("failed to spawn aleph binary");

    // Heph has no /api/v0/add endpoint, so the gateway-relay path returns a
    // non-success HTTP status. The CLI propagates that as a non-zero exit.
    assert!(
        !output.status.success(),
        "expected gateway-relay path to fail against heph (no /api/v0/add endpoint)\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
