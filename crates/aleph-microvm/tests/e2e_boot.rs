//! End-to-end boot test. Requires Linux + KVM + firecracker and real artifacts.
//! Run with:
//!   ALEPH_MICROVM_E2E=1 \
//!   ALEPH_MICROVM_KERNEL=/path/vmlinux \
//!   ALEPH_MICROVM_ROOTFS=/path/rootfs.squashfs \
//!   ALEPH_MICROVM_CODE=/path/hello.zip \
//!   cargo test -p aleph-microvm --test e2e_boot -- --ignored --nocapture

use std::path::PathBuf;

use aleph_microvm::asgi::scope_from_parts;
use aleph_microvm::config::{Encoding, Interface, LocalVmConfig};
use aleph_microvm::protocol::{RunCodePayload, RunResponse};
use aleph_microvm::{preflight, LocalVm};

fn env_path(key: &str) -> Option<PathBuf> {
    std::env::var_os(key).map(PathBuf::from)
}

#[tokio::test]
#[ignore = "needs KVM + firecracker + artifacts; opt in with ALEPH_MICROVM_E2E=1"]
async fn boots_and_answers_http() {
    if std::env::var("ALEPH_MICROVM_E2E").as_deref() != Ok("1") {
        eprintln!("skipping: ALEPH_MICROVM_E2E not set");
        return;
    }
    let firecracker = preflight::check("firecracker").expect("preflight");
    let cfg = LocalVmConfig {
        kernel_path: env_path("ALEPH_MICROVM_KERNEL").expect("KERNEL"),
        rootfs_path: env_path("ALEPH_MICROVM_ROOTFS").expect("ROOTFS"),
        code_path: env_path("ALEPH_MICROVM_CODE").expect("CODE"),
        encoding: Encoding::Zip,
        interface: Interface::Asgi,
        entrypoint: "main:app".into(),
        vm_hash: "e2e".into(),
        vcpus: 1,
        mem_mib: 256,
        variables: vec![],
        volumes: vec![],
    };
    let dir = std::env::temp_dir().join("aleph-microvm-e2e");
    let vm = LocalVm::launch(&cfg, &firecracker, dir)
        .await
        .expect("launch");
    let scope = scope_from_parts("GET", "/", &[], vec![]);
    let payload = RunCodePayload { scope }.to_msgpack().unwrap();
    let raw = vm.channel.send_run(&payload).await.expect("run");
    let resp: RunResponse = rmp_serde::from_slice(&raw).unwrap();
    let ok = resp.into_success().expect("vm error");
    assert_eq!(ok.status, 200);
    vm.shutdown().await;
}
