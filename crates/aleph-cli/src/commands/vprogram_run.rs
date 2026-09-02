//! `aleph vprogram run`: boot a V-PROGRAM locally in plain QEMU.
//!
//! The guest side of this contract lives in aleph-vm `nix/init-common.sh`
//! (`start_attest_agent`): with the `aleph_local=1` cmdline token the
//! attest agent serves plain HTTP on its usual port, so a SLIRP port
//! forward to that port reaches the workload through the same proxy path
//! production uses.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use aleph_sdk::client::AlephClient;
use anyhow::{Result, bail};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

use super::vprogram::{ATTEST_PORT, LocalBuild, RuntimeSource, prepare_local_build};
use crate::cli::VProgramRunArgs;
use crate::qemu::{Accel, LocalBootSpec, Qemu, QemuProcess};

/// Printed before the boot so nobody mistakes a local run for a deployment.
const HONESTY_LINE: &str = "local mode: no SEV-SNP, SeaBIOS instead of the bundle OVMF, -cpu max, \
    user networking, plain HTTP; the launch measurement is not validated";
/// How often the forwarded port is probed once the init markers are all in.
const PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// Serial lines the guest init prints on the way up. Order differs between
/// the exec and compose flavors, so completion is "all seen", not a sequence.
const MARKER_VERITY_ROOT: &str = "init: mounting /dev/mapper/verity-root";
const MARKER_VERITY_WORKLOAD: &str = "init: mounting /dev/mapper/verity-workload";
const MARKER_LOCAL_MODE: &str = "init: LOCAL MODE:";
const MARKER_INIT_START: &str = "init: starting /sbin/init";
const MARKERS: [&str; 4] = [
    MARKER_VERITY_ROOT,
    MARKER_VERITY_WORKLOAD,
    MARKER_LOCAL_MODE,
    MARKER_INIT_START,
];
/// Seen when the firewall is up: with this but no LOCAL MODE line the
/// runtime predates local mode (its init ignores the token).
const MARKER_FIREWALL: &str = "init: firewall active";
const FATAL_PREFIX: &str = "init: FATAL:";
const TAIL_LINES: usize = 40;

pub(crate) enum ScanEvent {
    /// A fail-closed line from init; the VM is powering off.
    Fatal(String),
    /// Every marker has been seen; start probing the forwarded port.
    Complete,
    Continue,
}

pub(crate) struct LineScanner {
    seen: [bool; 4],
    saw_firewall: bool,
    tail: VecDeque<String>,
}

impl LineScanner {
    pub(crate) fn new() -> Self {
        Self {
            seen: [false; 4],
            saw_firewall: false,
            tail: VecDeque::with_capacity(TAIL_LINES),
        }
    }

    pub(crate) fn feed(&mut self, line: &str) -> ScanEvent {
        if self.tail.len() == TAIL_LINES {
            self.tail.pop_front();
        }
        self.tail.push_back(line.to_string());
        if line.contains(FATAL_PREFIX) {
            return ScanEvent::Fatal(line.to_string());
        }
        if line.contains(MARKER_FIREWALL) {
            self.saw_firewall = true;
        }
        let before = self.complete();
        for (i, marker) in MARKERS.iter().enumerate() {
            if line.contains(marker) {
                self.seen[i] = true;
            }
        }
        if !before && self.complete() {
            ScanEvent::Complete
        } else {
            ScanEvent::Continue
        }
    }

    pub(crate) fn complete(&self) -> bool {
        self.seen.iter().all(|&s| s)
    }

    /// Why the deadline passed, from what the serial log showed.
    pub(crate) fn timeout_diagnosis(
        &self,
        runtime: &str,
        guest_port: u16,
        timeout_secs: u64,
    ) -> String {
        if self.complete() {
            return format!("agent did not answer on tcp/{guest_port} within {timeout_secs}s");
        }
        if self.saw_firewall && !self.seen[2] {
            return format!(
                "runtime {runtime} predates local mode (no aleph_local support in its init); \
                 rebuild it from aleph-vm dev-2.1 or newer"
            );
        }
        let missing = MARKERS
            .iter()
            .zip(self.seen.iter())
            .find(|&(_, &seen)| !seen)
            .map(|(m, _)| *m)
            .unwrap_or("");
        format!("guest never printed {missing:?} within {timeout_secs}s")
    }

    pub(crate) fn tail(&self) -> Vec<String> {
        self.tail.iter().cloned().collect()
    }
}

/// True once ANY HTTP response comes back. A bare TCP connect is not
/// evidence: SLIRP accepts the host-side connection itself and only then
/// tries the guest, closing without a byte when nothing listens there yet.
pub(crate) async fn probe_http(url: &str) -> bool {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    client.get(url).send().await.is_ok()
}

pub async fn handle_run(
    aleph_client: &AlephClient,
    json: bool,
    args: VProgramRunArgs,
) -> Result<()> {
    // Preflight the hypervisor before any build or network work.
    let qemu = Qemu::find()?;
    let accel = Accel::detect();
    if accel == Accel::Tcg {
        eprintln!("warning: /dev/kvm is not usable; booting under TCG (software emulation, slow)");
    }
    // Clone rather than move: `args` is borrowed again by `boot_spec` below.
    let source = match (args.runtime_manifest.clone(), args.bundle.clone()) {
        (Some(manifest), Some(bundle)) => RuntimeSource::LocalFiles { manifest, bundle },
        _ => RuntimeSource::Network(args.build.runtime.clone()),
    };
    let build = prepare_local_build(aleph_client, json, &args.build, source).await?;
    let runtime_label = format!("{} {}", build.manifest.name, build.manifest.version);

    let spec = boot_spec(&build, &args, accel);
    if !json {
        eprintln!("{HONESTY_LINE}");
        eprintln!(
            "Booting {} vcpu(s), {} MiB, forwarding 127.0.0.1:{} -> guest tcp/{}...",
            spec.vcpus, spec.mem_mib, spec.host_port, spec.guest_port
        );
    }
    let mut vm = QemuProcess::spawn(&qemu, &spec.argv())?;
    let url = format!("http://127.0.0.1:{}/", args.port);
    let deadline = Instant::now() + Duration::from_secs(args.timeout);
    let outcome = wait_until_ready(&mut vm, &url, deadline, &runtime_label, args.timeout).await;

    match outcome {
        Err(e) => {
            vm.shutdown().await?;
            Err(e)
        }
        Ok(()) => {
            report_ready(&build, &args, json);
            if args.check {
                vm.shutdown().await?;
                return Ok(());
            }
            // Interactive: run until Ctrl-C (SIGINT reaches QEMU too) or the
            // guest powers off. A guest that powers off on its own means the
            // workload exited, which production treats as a failure.
            let status = vm.wait().await?;
            bail!("the VM powered off (workload exited; qemu {status})")
        }
    }
}

/// The QEMU invocation for this build: the bundle's kernel/initrd, the
/// materialized cmdline, and the read-only disks in the guest device order
/// production uses (platform pair, workload pair, then one pair per volume).
fn boot_spec(build: &LocalBuild, args: &VProgramRunArgs, accel: Accel) -> LocalBootSpec {
    let mut disks = vec![
        build.artifacts.platform_rootfs.clone(),
        build.artifacts.platform_hash_tree.clone(),
        build.workload.data.clone(),
        build.workload.hash_tree.clone(),
    ];
    for v in &build.volumes {
        disks.push(v.data.clone());
        disks.push(v.hash_tree.clone());
    }
    LocalBootSpec {
        kernel: build.artifacts.kernel.clone(),
        initrd: build.artifacts.initrd.clone(),
        cmdline: build.cmdline.clone(),
        disks,
        vcpus: args.build.vcpus,
        mem_mib: args.build.memory,
        host_port: args.port,
        guest_port: ATTEST_PORT,
        internet: !args.no_internet,
        accel,
    }
}

/// Stream the serial console to stderr, watch the init markers, then probe
/// the forwarded port until the guest's agent answers or `deadline` passes.
async fn wait_until_ready(
    vm: &mut QemuProcess,
    url: &str,
    deadline: Instant,
    runtime_label: &str,
    timeout_secs: u64,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    if let Some(stdout) = vm.take_stdout() {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("vm | {line}");
                let _ = tx.send(line);
            }
        });
    }
    if let Some(stderr) = vm.take_stderr() {
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("qemu | {line}");
            }
        });
    }
    drop(tx);

    let mut scanner = LineScanner::new();
    let mut probing = false;
    // One persistent ticker, always selected on, so the loop head (deadline
    // and `try_wait`) runs on its own schedule no matter what the console
    // does: a silent guest cannot park us in `rx.recv()` forever, and a
    // chatty one cannot keep resetting a per-iteration timer. `tick` is
    // cancel-safe and is not rearmed when the other branch wins.
    let mut ticker = tokio::time::interval(PROBE_INTERVAL);
    loop {
        if Instant::now() >= deadline {
            bail!(
                "{}\n--- last serial lines ---\n{}",
                scanner.timeout_diagnosis(runtime_label, ATTEST_PORT, timeout_secs),
                scanner.tail().join("\n")
            );
        }
        if let Some(status) = vm.try_wait()? {
            bail!(
                "qemu exited ({status}) before the guest was ready\n--- last serial lines ---\n{}",
                scanner.tail().join("\n")
            );
        }
        tokio::select! {
            line = rx.recv() => match line {
                Some(line) => match scanner.feed(&line) {
                    ScanEvent::Fatal(line) => bail!("guest init failed closed: {line}"),
                    ScanEvent::Complete => probing = true,
                    ScanEvent::Continue => {}
                },
                None => {
                    // Console closed: QEMU is exiting; the try_wait above
                    // reports it on the next turn.
                    tokio::time::sleep(PROBE_INTERVAL).await;
                }
            },
            _ = ticker.tick() => {
                if probing && probe_http(url).await {
                    return Ok(());
                }
            }
        }
    }
}

/// Announce the forwarded endpoint: a single JSON document on stdout with
/// `--json`, otherwise a human line on stderr so stdout stays the guest's.
fn report_ready(build: &LocalBuild, args: &VProgramRunArgs, json: bool) {
    let url = format!("http://127.0.0.1:{}", args.port);
    if json {
        println!(
            "{}",
            serde_json::json!({
                "status": "ready",
                "url": url,
                "runtime": { "name": build.manifest.name, "version": build.manifest.version },
            })
        );
    } else if args.check {
        eprintln!("workload reachable at {url}");
    } else {
        eprintln!("workload reachable at {url} (Ctrl-C to stop)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "init: mounting /dev/mapper/verity-root";
    const WORKLOAD: &str = "init: mounting /dev/mapper/verity-workload";
    const LOCAL: &str =
        "init: LOCAL MODE: attest agent serving plain HTTP without a TEE; tcp/8443 is unattested";
    const START: &str = "init: starting /sbin/init from /mnt/workload";
    const FIREWALL: &str =
        "init: firewall active (drop inbound except tcp/8443, loopback, and ND/PMTU icmpv6)";

    #[test]
    fn markers_complete_in_any_order() {
        // init.sh prints LOCAL MODE before the /sbin/init line, init-compose.sh
        // after it; the scanner must not care.
        let mut s = LineScanner::new();
        assert!(matches!(s.feed(ROOT), ScanEvent::Continue));
        assert!(matches!(s.feed(WORKLOAD), ScanEvent::Continue));
        assert!(matches!(s.feed(START), ScanEvent::Continue));
        assert!(!s.complete());
        assert!(matches!(s.feed(LOCAL), ScanEvent::Complete));
        assert!(s.complete());
    }

    #[test]
    fn fatal_line_wins_immediately() {
        let mut s = LineScanner::new();
        s.feed(ROOT);
        let ev = s.feed(
            "init: FATAL: dm-verity verification failed for the workload volume - it may be tampered",
        );
        match ev {
            ScanEvent::Fatal(line) => assert!(line.contains("workload volume")),
            _ => panic!("expected Fatal"),
        }
    }

    #[test]
    fn timeout_without_local_mode_blames_the_runtime_age() {
        let mut s = LineScanner::new();
        s.feed(ROOT);
        s.feed(WORKLOAD);
        s.feed(FIREWALL);
        s.feed(START);
        let msg = s.timeout_diagnosis("aleph-snp-attest 2026.07.08", 8443, 180);
        assert!(msg.contains("predates local mode"), "{msg}");
        assert!(msg.contains("aleph-snp-attest 2026.07.08"), "{msg}");
    }

    #[test]
    fn timeout_after_all_markers_blames_the_agent() {
        let mut s = LineScanner::new();
        for l in [ROOT, WORKLOAD, LOCAL, START] {
            s.feed(l);
        }
        let msg = s.timeout_diagnosis("x", 8443, 180);
        assert!(
            msg.contains("agent did not answer on tcp/8443 within 180s"),
            "{msg}"
        );
    }

    #[test]
    fn timeout_before_verity_names_the_missing_marker() {
        let mut s = LineScanner::new();
        s.feed("random kernel line");
        let msg = s.timeout_diagnosis("x", 8443, 180);
        assert!(msg.contains(ROOT), "{msg}");
    }

    #[test]
    fn tail_keeps_the_last_forty_lines() {
        let mut s = LineScanner::new();
        for i in 0..50 {
            s.feed(&format!("line {i}"));
        }
        let tail = s.tail();
        assert_eq!(tail.len(), 40);
        assert_eq!(tail[0], "line 10");
        assert_eq!(tail[39], "line 49");
    }

    #[tokio::test]
    async fn probe_http_accepts_any_http_response_and_rejects_a_bare_close() {
        use std::io::{Read, Write};
        // A listener that answers a 404: ready.
        let ok = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let ok_port = ok.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut s, _)) = ok.accept() {
                let mut buf = [0u8; 1024];
                let _ = s.read(&mut buf);
                let _ = s.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
            }
        });
        assert!(probe_http(&format!("http://127.0.0.1:{ok_port}/")).await);

        // A listener that accepts and closes without a byte (what SLIRP does
        // while the guest is not listening yet): not ready.
        let closer = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closer_port = closer.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((s, _)) = closer.accept() {
                drop(s);
            }
        });
        assert!(!probe_http(&format!("http://127.0.0.1:{closer_port}/")).await);
    }
}
