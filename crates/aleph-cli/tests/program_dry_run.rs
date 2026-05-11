//! End-to-end test for `aleph program create --dry-run --json`.
//!
//! Spawns the compiled `aleph` binary against a placeholder CCN URL (no HTTP
//! traffic happens because dry-run short-circuits before any submission) and
//! asserts that stdout contains both the STORE and PROGRAM envelopes the
//! create flow emits.

use std::process::Command;

use tempfile::tempdir;

#[test]
fn create_dry_run_emits_store_and_program_envelopes() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("main.py"), b"def app(): pass\n").unwrap();

    // Deterministic 32-byte private key. Hex-encoded to match the CLI's
    // `--private-key` flag.
    let private_key_hex = hex::encode([0x42u8; 32]);

    let bin = env!("CARGO_BIN_EXE_aleph");
    let out = Command::new(bin)
        .args([
            // The CLI requires --ccn or a configured network. Dry-run never
            // contacts the URL, so any well-formed value works here.
            "--ccn",
            "https://example.invalid/",
            "--json",
            "program",
            "create",
            src.to_str().unwrap(),
            "main:app",
            "--vcpus",
            "1",
            "--memory",
            "256MiB",
            "--private-key",
            &private_key_hex,
            "--chain",
            "eth",
            "--dry-run",
        ])
        .output()
        .expect("failed to spawn aleph binary");

    assert!(
        out.status.success(),
        "aleph exited with {:?}\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let stdout = String::from_utf8(out.stdout).expect("stdout not utf-8");

    assert!(
        stdout.contains("\"type\": \"STORE\""),
        "missing STORE envelope:\n{stdout}"
    );
    assert!(
        stdout.contains("\"type\": \"PROGRAM\""),
        "missing PROGRAM envelope:\n{stdout}"
    );
    assert!(
        stdout.contains("main:app"),
        "missing entrypoint in payload:\n{stdout}"
    );
}
