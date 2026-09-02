//! Binary-level tests pinning that `aleph vprogram run` fails closed BEFORE
//! any network use: clap flag conflicts, the QEMU preflight, and the local
//! file gates. Same technique as vprogram_compose_cli.rs: the CCN URL is
//! `https://example.invalid/`, so reaching the network would surface as a
//! DNS error mentioning that host instead of the expected message.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::{TempDir, tempdir};

fn runtime_hash() -> String {
    "63f07193e6ee9d207b7d1fcf8286f9aee34e6f12f101d2ec77c1229f92964696".to_string()
}

/// No-op shims for every tool `run` probes, optionally including QEMU.
fn shim_dir(with_qemu: bool) -> TempDir {
    let dir = tempdir().unwrap();
    let mut names = vec!["veritysetup", "mkfs.ext4", "debugfs", "podman"];
    if with_qemu {
        names.push("qemu-system-x86_64");
    }
    for name in names {
        let path = dir.path().join(name);
        std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    dir
}

fn base_command(shims: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_aleph"));
    // PATH is ONLY the shim dir so a real qemu on the machine cannot leak in.
    cmd.env("PATH", shims);
    cmd.args(["--ccn", "https://example.invalid/", "vprogram", "run"]);
    cmd
}

fn workload_file(dir: &Path) -> PathBuf {
    let path = dir.join("workload.ext4");
    std::fs::write(&path, b"not really ext4").unwrap();
    path
}

#[test]
fn rejects_runtime_together_with_runtime_manifest() {
    let shims = shim_dir(true);
    let work = tempdir().unwrap();
    let workload = workload_file(work.path());
    let out = base_command(shims.path())
        .args(["--workload"])
        .arg(&workload)
        .args(["--runtime", &runtime_hash()])
        .args(["--runtime-manifest", "m.json", "--bundle", "b.tar.gz"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("cannot be used with"), "{stderr}");
}

#[test]
fn rejects_runtime_manifest_without_bundle() {
    let shims = shim_dir(true);
    let work = tempdir().unwrap();
    let workload = workload_file(work.path());
    let out = base_command(shims.path())
        .args(["--workload"])
        .arg(&workload)
        .args(["--runtime-manifest", "m.json"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("--bundle"), "{stderr}");
}

#[test]
fn fails_before_network_when_qemu_is_missing() {
    let shims = shim_dir(false);
    let work = tempdir().unwrap();
    let workload = workload_file(work.path());
    let out = base_command(shims.path())
        .args(["--workload"])
        .arg(&workload)
        .args(["--runtime", &runtime_hash()])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(stderr.contains("qemu-system-x86_64 not found"), "{stderr}");
    assert!(!stderr.contains("example.invalid"), "{stderr}");
}

#[test]
fn fails_on_a_missing_workload_before_network() {
    let shims = shim_dir(true);
    let out = base_command(shims.path())
        .args(["--workload", "/nonexistent/workload.ext4"])
        .args(["--runtime", &runtime_hash()])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(stderr.contains("workload image not found"), "{stderr}");
    assert!(!stderr.contains("example.invalid"), "{stderr}");
}

#[test]
fn fails_on_a_missing_local_manifest_before_network() {
    let shims = shim_dir(true);
    let work = tempdir().unwrap();
    let workload = workload_file(work.path());
    let out = base_command(shims.path())
        .args(["--workload"])
        .arg(&workload)
        .args([
            "--runtime-manifest",
            "/nonexistent/manifest.json",
            "--bundle",
            "/nonexistent/b.tar.gz",
        ])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(stderr.contains("reading runtime manifest"), "{stderr}");
    assert!(!stderr.contains("example.invalid"), "{stderr}");
}
