//! End-to-end tests for the `aleph domain` CLI subcommands.
//!
//! Spins up a tiny actix-web mock CCN that serves the canned `domains`
//! aggregate fixture, runs the compiled `aleph` binary against it, and
//! asserts that the JSON payload printed to stdout matches the expected
//! shape.

use std::process::Command;

use actix_web::{App, HttpResponse, HttpServer, web};
use serde::Deserialize;

const DOMAINS_AGGREGATE: &str = include_str!("../../../fixtures/websites/domains_aggregate.json");

const TEST_ADDRESS: &str = "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10";

#[derive(Deserialize)]
struct AggregatesQuery {
    keys: String,
}

/// Mock handler for `GET /api/v0/aggregates/{address}.json?keys=domains`.
///
/// Only the `domains` key is exercised by `aleph domain list`; any other
/// key returns 404 so a misrouted call is loud rather than silently empty.
async fn aggregates_handler(
    _path: web::Path<String>,
    query: web::Query<AggregatesQuery>,
) -> HttpResponse {
    match query.keys.as_str() {
        "domains" => HttpResponse::Ok()
            .content_type("application/json")
            .body(DOMAINS_AGGREGATE),
        other => HttpResponse::NotFound().body(format!("unexpected keys={other}")),
    }
}

/// Start the mock CCN on an OS-assigned port. Returns the base URL with a
/// trailing slash so it composes cleanly with `Url::join`.
fn start_test_server() -> String {
    let (tx, rx) = std::sync::mpsc::channel::<u16>();

    std::thread::spawn(move || {
        let sys = actix_web::rt::System::new();
        sys.block_on(async move {
            let server = HttpServer::new(|| {
                App::new().route(
                    "/api/v0/aggregates/{address}.json",
                    web::get().to(aggregates_handler),
                )
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
fn domain_list_json_round_trip() {
    let base_url = start_test_server();

    let output = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "--ccn-url",
            &base_url,
            "--json",
            "domain",
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

    assert_eq!(arr.len(), 2, "expected exactly two domains: {stdout}");

    let site = arr
        .iter()
        .find(|v| v["domain"] == "site.example.com")
        .unwrap_or_else(|| panic!("`site.example.com` not in output: {stdout}"));
    let site_message_id = site["message_id"]
        .as_str()
        .unwrap_or_else(|| panic!("`site.example.com` message_id should be a string: {stdout}"));
    assert!(
        !site_message_id.is_empty(),
        "`site.example.com` should have non-empty message_id: {stdout}"
    );

    let detached = arr
        .iter()
        .find(|v| v["domain"] == "detached.example.com")
        .unwrap_or_else(|| panic!("`detached.example.com` not in output: {stdout}"));
    let detached_message_id = detached["message_id"].as_str().unwrap_or_else(|| {
        panic!("`detached.example.com` message_id should be a string: {stdout}")
    });
    assert!(
        detached_message_id.is_empty(),
        "`detached.example.com` should have empty message_id: {stdout}"
    );
}
