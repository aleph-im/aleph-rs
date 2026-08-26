//! Binary-level tests pinning that `aleph vprogram create --compose` fails
//! closed BEFORE any network or real tool use.
//!
//! `handle_create` validates the compose file and locates the local tools
//! (veritysetup, mkfs.ext4, podman) before it ever fetches the runtime
//! manifest. Every test here spawns the binary against
//! `https://example.invalid/`, which has no server behind it: if validation
//! did not run first and the CLI reached the network, the failure would
//! instead be a DNS/connection error mentioning `example.invalid`. Asserting
//! on the specific validation message (not just a non-zero exit) is what
//! proves the ordering.
//!
//! `compute_measurements` parses real OVMF SEV metadata, so a fixture bundle
//! that reaches that far cannot be faked cheaply here; full dry-run payload
//! coverage is out of scope (see the cross-repo e2e in the aleph-cvm plan).

use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::{TempDir, tempdir};

/// Deterministic 32-byte private key, hex-encoded to match `--private-key`.
fn private_key_hex() -> String {
    hex::encode([0x42u8; 32])
}

/// A syntactically valid (but not necessarily real) 64-hex item hash for
/// `--runtime`. Never dereferenced: every test here fails before the runtime
/// manifest fetch that would resolve it.
fn runtime_hash() -> String {
    "63f07193e6ee9d207b7d1fcf8286f9aee34e6f12f101d2ec77c1229f92964696".to_string()
}

/// Write no-op executable shims for `veritysetup`, `mkfs.ext4`, and `podman`
/// into a fresh temp dir so `Veritysetup::find()`, `MkfsExt4::find()`, and
/// `ContainerTool::find()` succeed on any machine, regardless of what is
/// actually installed. None of these tests reach far enough to invoke the
/// shims (they exist only to satisfy the `find()` probes), so the scripts
/// need no real behavior beyond exiting 0.
#[cfg(unix)]
fn tool_shim_dir() -> TempDir {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    for name in ["veritysetup", "mkfs.ext4", "podman"] {
        let path = dir.path().join(name);
        std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    dir
}

/// Build the `Command` common to every test: the compiled `aleph` binary,
/// PATH prepended with the tool shims, and the global flags every subcommand
/// here needs (`--ccn`, `--json`).
#[cfg(unix)]
fn base_command(shim_dir: &Path) -> Command {
    let bin = env!("CARGO_BIN_EXE_aleph");
    let mut cmd = Command::new(bin);
    let original_path = std::env::var("PATH").unwrap_or_default();
    cmd.env("PATH", format!("{}:{}", shim_dir.display(), original_path));
    cmd.args([
        // Dry-run and pre-network validation failures never contact this
        // URL; a well-formed placeholder is enough, and its unreachability
        // is what makes these tests prove validation ran first.
        "--ccn",
        "https://example.invalid/",
        "--json",
    ]);
    cmd
}

fn write_compose(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("docker-compose.yml");
    std::fs::write(&path, contents).unwrap();
    path
}

#[cfg(unix)]
#[test]
fn rejects_a_compose_file_with_ports_before_any_network() {
    let shims = tool_shim_dir();
    let work = tempdir().unwrap();
    let compose_path = write_compose(
        work.path(),
        "services:\n  \
         web:\n    \
         image: nginx:1.27\n    \
         network_mode: host\n    \
         ports:\n      - \"8080:8080\"\n",
    );
    let key = private_key_hex();

    let out = base_command(shims.path())
        .args([
            "vprogram",
            "create",
            "test-vprogram",
            "--compose",
            compose_path.to_str().unwrap(),
            "--runtime",
            &runtime_hash(),
            "--private-key",
            &key,
            "--chain",
            "eth",
            "--dry-run",
        ])
        .output()
        .expect("failed to spawn aleph binary");

    assert!(
        !out.status.success(),
        "expected a non-zero exit; stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("ports") && stderr.contains("8080"),
        "stderr did not mention the rejected `ports` key and its 8080 \
         entrypoint contract:\n{stderr}"
    );
    assert!(
        !stderr.contains("example.invalid"),
        "stderr mentions the placeholder CCN host, implying a network \
         attempt happened before validation:\n{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn rejects_missing_image_archive_path() {
    let shims = tool_shim_dir();
    let work = tempdir().unwrap();
    let compose_path = write_compose(
        work.path(),
        "services:\n  \
         web:\n    \
         image: nginx:1.27\n    \
         network_mode: host\n",
    );
    let key = private_key_hex();

    let out = base_command(shims.path())
        .args([
            "vprogram",
            "create",
            "test-vprogram",
            "--compose",
            compose_path.to_str().unwrap(),
            "--image-archive",
            "web=./does-not-exist.tar",
            "--runtime",
            &runtime_hash(),
            "--private-key",
            &key,
            "--chain",
            "eth",
            "--dry-run",
        ])
        .output()
        .expect("failed to spawn aleph binary");

    assert!(
        !out.status.success(),
        "expected a non-zero exit; stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("does-not-exist.tar"),
        "stderr did not mention the missing image archive path:\n{stderr}"
    );
    assert!(
        !stderr.contains("example.invalid"),
        "stderr mentions the placeholder CCN host, implying a network \
         attempt happened before validation:\n{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn requires_exactly_one_of_workload_and_compose() {
    let shims = tool_shim_dir();
    let key = private_key_hex();

    let out = base_command(shims.path())
        .args([
            "vprogram",
            "create",
            "test-vprogram",
            "--runtime",
            &runtime_hash(),
            "--private-key",
            &key,
            "--chain",
            "eth",
            "--dry-run",
        ])
        .output()
        .expect("failed to spawn aleph binary");

    assert_eq!(
        out.status.code(),
        Some(2),
        "expected clap's usage-error exit code 2; stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--workload") || stderr.contains("--compose"),
        "stderr did not mention the missing required flag:\n{stderr}"
    );
}
