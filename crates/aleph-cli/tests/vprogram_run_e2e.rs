//! Real boot of a V-PROGRAM under plain QEMU through `aleph vprogram run
//! --check`. Local tool, not a CI gate: needs qemu-system-x86_64, KVM for a
//! sane boot time, a runtime built from aleph-vm dev-2.1 or newer, and a
//! workload. Skipped unless every variable below is set:
//!
//!   ALEPH_VPROGRAM_E2E=1
//!   ALEPH_VPROGRAM_E2E_MANIFEST=/path/to/manifest.json
//!   ALEPH_VPROGRAM_E2E_BUNDLE=/path/to/snp-image.tar.gz
//!   ALEPH_VPROGRAM_E2E_WORKLOAD=/path/to/workload.ext4
#![cfg(unix)]

use std::process::Command;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn run_check_boots_and_reaches_the_plain_agent() {
    let (Some(_), Some(manifest), Some(bundle), Some(workload)) = (
        env("ALEPH_VPROGRAM_E2E"),
        env("ALEPH_VPROGRAM_E2E_MANIFEST"),
        env("ALEPH_VPROGRAM_E2E_BUNDLE"),
        env("ALEPH_VPROGRAM_E2E_WORKLOAD"),
    ) else {
        eprintln!("skipping: ALEPH_VPROGRAM_E2E* not set");
        return;
    };
    let port = free_port().to_string();
    let out = Command::new(env!("CARGO_BIN_EXE_aleph"))
        .args([
            "--ccn",
            "https://example.invalid/",
            "vprogram",
            "run",
            "--check",
        ])
        .args(["--workload", &workload])
        .args(["--runtime-manifest", &manifest, "--bundle", &bundle])
        .args(["--port", &port, "--timeout", "300"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(stderr.contains("init: LOCAL MODE:"), "{stderr}");
    assert!(
        stderr.contains(&format!("workload reachable at http://127.0.0.1:{port}")),
        "{stderr}"
    );
}
