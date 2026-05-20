//! End-to-end tests for the `aleph website` CLI subcommands.
//!
//! Spins up a tiny actix-web mock CCN that serves canned aggregate fixtures
//! and a STORE message fixture, runs the compiled `aleph` binary against it,
//! and asserts that the JSON payload printed to stdout matches the expected
//! shape.

use std::process::Command;

use actix_web::{App, HttpResponse, HttpServer, web};
use serde::Deserialize;

const WEBSITES_AGGREGATE: &str = include_str!("../../../fixtures/websites/websites_aggregate.json");
const DOMAINS_AGGREGATE: &str = include_str!("../../../fixtures/websites/domains_aggregate.json");
const STORE_MESSAGE: &str = include_str!("../../../fixtures/websites/store_message.json");

const TEST_ADDRESS: &str = "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10";

#[derive(Deserialize)]
struct AggregatesQuery {
    keys: String,
}

/// Mock handler for `GET /api/v0/aggregates/{address}.json?keys=...`.
///
/// Routes each `keys` value to the matching fixture so a single test server
/// can satisfy both calls the CLI makes when listing or showing websites.
async fn aggregates_handler(
    _path: web::Path<String>,
    query: web::Query<AggregatesQuery>,
) -> HttpResponse {
    match query.keys.as_str() {
        "websites" => HttpResponse::Ok()
            .content_type("application/json")
            .body(WEBSITES_AGGREGATE),
        "domains" => HttpResponse::Ok()
            .content_type("application/json")
            .body(DOMAINS_AGGREGATE),
        other => HttpResponse::NotFound().body(format!("unexpected keys={other}")),
    }
}

/// Mock handler for `GET /api/v0/messages/{hash}`.
///
/// The CLI's `resolve_store_ipfs_cid` calls `AlephMessageClient::get_message`,
/// which expects a `MessageWithStatus<Message>` envelope (i.e. a `status` tag
/// alongside the message). The fixture is shaped to match that wire format.
async fn message_handler(_path: web::Path<String>) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("application/json")
        .body(STORE_MESSAGE)
}

/// Start the mock CCN on an OS-assigned port. Returns the base URL with a
/// trailing slash so it composes cleanly with `Url::join`.
///
/// Mounts both the aggregate route (used by `website list` and `website show`)
/// and the messages route (used by `website show` to resolve `volume_id` to
/// an IPFS CID via the underlying STORE message).
fn start_test_server() -> String {
    let (tx, rx) = std::sync::mpsc::channel::<u16>();

    std::thread::spawn(move || {
        let sys = actix_web::rt::System::new();
        sys.block_on(async move {
            let server = HttpServer::new(|| {
                App::new()
                    .route(
                        "/api/v0/aggregates/{address}.json",
                        web::get().to(aggregates_handler),
                    )
                    .route("/api/v0/messages/{hash}", web::get().to(message_handler))
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
fn website_list_json_round_trip() {
    let base_url = start_test_server();

    let output = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "--ccn",
            &base_url,
            "--json",
            "website",
            "list",
            "--address",
            TEST_ADDRESS,
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
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should be valid JSON");
    let arr = parsed.as_array().expect("output should be a JSON array");

    assert!(
        arr.iter().any(|v| v["name"] == "my-site"),
        "expected `my-site` in output: {stdout}"
    );
    // Soft-deleted aggregate entries (value == null) must be filtered out.
    assert!(
        arr.iter().all(|v| v["name"] != "deleted-site"),
        "soft-deleted `deleted-site` should not appear: {stdout}"
    );
}

#[test]
fn website_show_resolves_ipfs_cid() {
    let base_url = start_test_server();

    let output = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "--ccn",
            &base_url,
            "--json",
            "website",
            "show",
            "my-site",
            "--address",
            TEST_ADDRESS,
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
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should be valid JSON");

    assert_eq!(parsed["name"], "my-site", "stdout: {stdout}");
    assert_eq!(parsed["version"], 2, "stdout: {stdout}");
    assert_eq!(
        parsed["ipfs_cid"], "bafybeigfolderv2cidexampleexampleexampleexampleexa",
        "stdout: {stdout}"
    );
    let domains = parsed["domains"]
        .as_array()
        .expect("`domains` should be an array");
    assert_eq!(domains.len(), 1, "stdout: {stdout}");
    assert_eq!(domains[0], "site.example.com", "stdout: {stdout}");
}

/// Locks the public JSON contract of `aleph website deploy --dry-run --json`.
///
/// In `--dry-run --json` mode the inner STORE / aggregate submissions are
/// skipped and only the final `DeployOut` envelope reaches stdout. That
/// envelope is fully deterministic given its inputs (name, volume_id, resolved
/// IPFS CID, version, domains_attached) — no timestamps, no signatures — so
/// the snapshot needs no redactions and any drift in the wire shape will
/// surface as a snapshot diff for downstream tooling reviewers.
#[test]
fn website_deploy_dry_run_snapshot() {
    let base_url = start_test_server();

    // Single 64-hex `volume_id` matching the fixture's `item_hash`, so
    // `resolve_store_ipfs_cid` succeeds and the snapshot pins a real CID.
    let volume_id = "a".repeat(64);

    // tempdir with one file — the deploy code's `validate_folder` only checks
    // the path exists and is non-empty when `--volume-id` is absent. With a
    // volume_id supplied the folder is not uploaded, but the path argument is
    // still positional/required, so we point it at a real (empty-ish) dir.
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("index.html"), b"<html></html>").expect("write index.html");

    let output = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "--ccn",
            &base_url,
            "--json",
            "website",
            "deploy",
            "test-site",
            tmp.path().to_str().expect("tempdir path utf-8"),
            "--volume-id",
            &volume_id,
            "--private-key",
            "0x0101010101010101010101010101010101010101010101010101010101010101",
            "--chain",
            "eth",
            "--dry-run",
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
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout should be valid JSON");

    insta::assert_json_snapshot!(parsed);
}
