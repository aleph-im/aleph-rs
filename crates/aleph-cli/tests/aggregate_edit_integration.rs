//! End-to-end tests for `aleph aggregate create/edit/unset`.
//!
//! Each test spins up a tiny actix-web mock CCN that answers the aggregate
//! fetch (`GET /api/v0/aggregates/{address}.json?keys=...`) with a canned
//! body, then runs the compiled `aleph` binary with `--dry-run --json` and
//! asserts on the AGGREGATE envelope (or the guard error) it prints.

use std::io::Write;
use std::process::Command;

use actix_web::{web, App, HttpResponse, HttpServer};
use serde::Deserialize;

const PRIVATE_KEY_HEX: &str = "4242424242424242424242424242424242424242424242424242424242424242";

#[derive(Deserialize)]
struct KeysQuery {
    #[allow(dead_code)]
    keys: String,
}

/// Start a mock CCN on an OS-assigned port that returns `body` for any
/// aggregate fetch. Returns the base URL with a trailing slash.
///
/// Follows the same `actix_web::rt::System` + thread + channel pattern as
/// `website_integration.rs::start_test_server`.
fn start_mock(body: &'static str) -> String {
    let (tx, rx) = std::sync::mpsc::channel::<u16>();

    std::thread::spawn(move || {
        let sys = actix_web::rt::System::new();
        sys.block_on(async move {
            let server = HttpServer::new(move || {
                App::new().route(
                    "/api/v0/aggregates/{address}.json",
                    web::get().to(move |_q: web::Query<KeysQuery>| async move {
                        HttpResponse::Ok()
                            .content_type("application/json")
                            .body(body)
                    }),
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

/// Run `aleph --ccn <ccn> --json aggregate <extra...> --private-key <KEY>
/// --chain eth --dry-run`, optionally appending `-y` and overriding EDITOR.
///
/// Global flags (`--ccn`, `--json`) come before the `aggregate` subcommand,
/// mirroring `website_integration.rs`. Identity/signing flags (`--private-key`,
/// `--chain`, `--dry-run`) come after the subcommand, mirroring
/// `program_dry_run.rs`.
///
/// `yes` controls whether `-y` is appended (only `edit` and `unset` accept it;
/// `create` does not have the flag).
fn run_aggregate(
    ccn: &str,
    extra: &[&str],
    yes: bool,
    editor: Option<&str>,
) -> std::process::Output {
    let bin = env!("CARGO_BIN_EXE_aleph");
    let mut cmd = Command::new(bin);
    cmd.args(["--ccn", ccn, "--json", "aggregate"])
        .args(extra)
        .args(["--private-key", PRIVATE_KEY_HEX, "--chain", "eth", "--dry-run"]);
    if yes {
        cmd.arg("-y");
    }
    if let Some(ed) = editor {
        cmd.env("EDITOR", ed).env_remove("VISUAL");
    }
    cmd.output().expect("failed to spawn aleph binary")
}

/// Parse the pretty-printed `PendingMessage` JSON from stdout and decode the
/// `item_content` field (which is an inline JSON string) into a `Value`.
///
/// The dry-run output serializes a `PendingMessage` whose `item_type` is
/// `"inline"` and whose `item_content` holds the content JSON as a string.
fn parse_item_content(stdout: &str) -> serde_json::Value {
    let envelope: serde_json::Value =
        serde_json::from_str(stdout).expect("stdout should be valid JSON");
    let raw = envelope["item_content"]
        .as_str()
        .expect("item_content should be a string");
    serde_json::from_str(raw).expect("item_content should be valid JSON")
}

#[test]
fn create_rejects_existing_key() {
    let ccn = start_mock(r#"{"data": {"mykey": {"a": 1}}}"#);
    let out = run_aggregate(
        &ccn,
        &["create", "--key", "mykey", "--content", "{\"a\":1}"],
        false,
        None,
    );
    assert!(!out.status.success(), "create should fail when key exists");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("already exists"),
        "expected 'already exists' guard, got: {stderr}"
    );
}

#[test]
fn create_emits_aggregate_envelope_when_absent() {
    let ccn = start_mock(r#"{"data": {}}"#);
    let out = run_aggregate(
        &ccn,
        &["create", "--key", "mykey", "--content", "{\"a\":1}"],
        false,
        None,
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("\"type\": \"AGGREGATE\""), "{stdout}");
    let content = parse_item_content(&stdout);
    assert_eq!(content["key"], "mykey", "{stdout}");
    assert_eq!(content["content"]["a"], 1, "{stdout}");
}

#[test]
fn edit_subkey_posts_single_value() {
    let ccn = start_mock(r#"{"data": {"mykey": {"a": 1}}}"#);
    let out = run_aggregate(
        &ccn,
        &["edit", "--key", "mykey", "--subkey", "b", "--content", "9"],
        true,
        None,
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let content = parse_item_content(&stdout);
    assert_eq!(content["content"]["b"], 9, "{stdout}");
    assert!(content["content"]["a"].is_null(), "{stdout}");
}

#[test]
fn edit_whole_content_nulls_removed_subkey() {
    let ccn = start_mock(r#"{"data": {"mykey": {"a": 1, "old": true}}}"#);
    let out = run_aggregate(
        &ccn,
        &["edit", "--key", "mykey", "--content", "{\"a\":2}"],
        true,
        None,
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let content = parse_item_content(&stdout);
    assert_eq!(content["content"]["a"], 2, "{stdout}");
    assert!(content["content"]["old"].is_null(), "{stdout}");
}

#[test]
#[cfg(unix)]
fn edit_interactive_uses_editor() {
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("fake-editor.sh");
    let mut f = std::fs::File::create(&script).unwrap();
    writeln!(f, "#!/bin/sh\ncat > \"$1\" <<'EOF'\n{{\"a\": 2}}\nEOF").unwrap();
    drop(f);
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let ccn = start_mock(r#"{"data": {"mykey": {"a": 1, "old": true}}}"#);
    let out = run_aggregate(
        &ccn,
        &["edit", "--key", "mykey"],
        true,
        Some(script.to_str().unwrap()),
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let content = parse_item_content(&stdout);
    assert_eq!(content["content"]["a"], 2, "{stdout}");
    assert!(content["content"]["old"].is_null(), "{stdout}");
}

#[test]
fn unset_posts_null_for_subkeys() {
    let ccn = start_mock(r#"{"data": {"mykey": {"a": 1, "b": 2}}}"#);
    let out = run_aggregate(
        &ccn,
        &["unset", "--key", "mykey", "--subkey", "a,b"],
        true,
        None,
    );
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let content = parse_item_content(&stdout);
    assert!(content["content"]["a"].is_null(), "{stdout}");
    assert!(content["content"]["b"].is_null(), "{stdout}");
}

#[test]
fn security_key_is_rejected_by_create() {
    let ccn = start_mock(r#"{"data": {}}"#);
    let out = run_aggregate(
        &ccn,
        &["create", "--key", "security", "--content", "{}"],
        false,
        None,
    );
    assert!(
        !out.status.success(),
        "expected failure for 'security' key, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("authorization"), "{stderr}");
}
