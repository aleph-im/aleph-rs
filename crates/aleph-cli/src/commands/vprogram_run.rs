//! `aleph vprogram run`: boot a V-PROGRAM locally in plain QEMU.
//!
//! The guest side of this contract lives in aleph-vm `nix/init-common.sh`
//! (`start_attest_agent`): with the `aleph_local=1` cmdline token the
//! attest agent serves plain HTTP on its usual port, so a SLIRP port
//! forward to that port reaches the workload through the same proxy path
//! production uses.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use aleph_sdk::client::AlephClient;
use aleph_sdk::vprogram::bundle::BundleArtifacts;
use anyhow::{Result, bail};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

use super::vprogram::{
    ATTEST_PORT, LocalBuild, RuntimeSource, VerityArtifact, prepare_local_build,
};
use crate::cli::VProgramRunArgs;
use crate::qemu::{Accel, LocalBootSpec, Qemu, QemuProcess};

/// Printed before the boot so nobody mistakes a local run for a deployment.
const HONESTY_LINE: &str = "local mode: no SEV-SNP, SeaBIOS instead of the bundle OVMF, -cpu max, \
    user networking, plain HTTP; the launch measurement is not validated";
/// How often the forwarded port is probed once the init markers are all in.
const PROBE_INTERVAL: Duration = Duration::from_millis(500);
/// Per-probe HTTP timeout.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

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
/// Indices into [`MARKERS`], i.e. into `LineScanner::seen`. A unit test pins
/// each to its marker so reordering `MARKERS` cannot silently point the
/// local-mode diagnosis at another marker.
const IDX_LOCAL_MODE: usize = 2;
const IDX_INIT_START: usize = 3;
/// Seen when the firewall is up: with this but no LOCAL MODE line the
/// runtime predates local mode (its init ignores the token).
const MARKER_FIREWALL: &str = "init: firewall active";
const FATAL_PREFIX: &str = "init: FATAL:";
const TAIL_LINES: usize = 40;
/// How many further serial lines after [`MARKER_INIT_START`] are allowed to go
/// by before a missing LOCAL MODE line is called conclusive. Both inits print
/// it either before the /sbin/init line or immediately after it, so a handful
/// of lines of slack is enough while leaving room for interleaved kernel
/// output.
const LOCAL_MODE_GRACE_LINES: usize = 5;

pub(crate) enum ScanEvent {
    /// A fail-closed line from init; the VM is powering off.
    Fatal(String),
    /// Every marker has been seen; start probing the forwarded port.
    Complete,
    /// The init got far enough that it would have announced local mode, and
    /// did not: this runtime predates the `aleph_local` token. Reported at
    /// once instead of after the full timeout.
    NoLocalMode,
    Continue,
}

pub(crate) struct LineScanner {
    seen: [bool; 4],
    saw_firewall: bool,
    /// Lines fed since [`MARKER_INIT_START`] was seen, `None` until then.
    since_init_start: Option<usize>,
    /// [`ScanEvent::NoLocalMode`] is emitted once, not on every later line.
    reported_no_local_mode: bool,
    tail: VecDeque<String>,
}

impl LineScanner {
    pub(crate) fn new() -> Self {
        Self {
            seen: [false; 4],
            saw_firewall: false,
            since_init_start: None,
            reported_no_local_mode: false,
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
            return ScanEvent::Complete;
        }
        self.since_init_start = match self.since_init_start {
            Some(n) => Some(n + 1),
            // The init-start line itself counts as zero lines elapsed.
            None if self.seen[IDX_INIT_START] => Some(0),
            None => None,
        };
        if !self.reported_no_local_mode
            && !self.seen[IDX_LOCAL_MODE]
            && self.saw_firewall
            && self
                .since_init_start
                .is_some_and(|n| n >= LOCAL_MODE_GRACE_LINES)
        {
            self.reported_no_local_mode = true;
            return ScanEvent::NoLocalMode;
        }
        ScanEvent::Continue
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
        if self.saw_firewall && !self.seen[IDX_LOCAL_MODE] {
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

/// The probe client: short timeout so a hung connection cannot outlive a
/// tick, and no redirect following since any response at all is the signal.
fn probe_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

/// True once ANY HTTP response comes back. A bare TCP connect is not
/// evidence: SLIRP accepts the host-side connection itself and only then
/// tries the guest, closing without a byte when nothing listens there yet.
pub(crate) async fn probe_http(client: &reqwest::Client, url: &str) -> bool {
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
        (None, None) => RuntimeSource::Network(args.build.runtime.clone()),
        (Some(_), None) | (None, Some(_)) => {
            unreachable!("clap: --runtime-manifest and --bundle require each other")
        }
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
            // Best effort: a shutdown io error must not replace the
            // diagnosis the caller actually needs to read.
            let _ = vm.shutdown().await;
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
    LocalBootSpec {
        kernel: build.artifacts.kernel.clone(),
        initrd: build.artifacts.initrd.clone(),
        cmdline: build.cmdline.clone(),
        disks: disk_order(&build.artifacts, &build.workload, &build.volumes),
        vcpus: args.build.vcpus,
        mem_mib: args.build.memory,
        host_port: args.port,
        guest_port: ATTEST_PORT,
        internet: !args.no_internet,
        accel,
    }
}

/// The read-only disks in the guest device order the runtime's init expects:
/// the platform pair (vda/vdb), the workload pair, then one pair per verified
/// volume in flag order. The cmdline's roothash slots are filled in the same
/// order, so getting this wrong silently boots the wrong device.
fn disk_order(
    artifacts: &BundleArtifacts,
    workload: &VerityArtifact,
    volumes: &[VerityArtifact],
) -> Vec<PathBuf> {
    let mut disks = Vec::with_capacity(4 + 2 * volumes.len());
    disks.push(artifacts.platform_rootfs.clone());
    disks.push(artifacts.platform_hash_tree.clone());
    disks.push(workload.data.clone());
    disks.push(workload.hash_tree.clone());
    for v in volumes {
        disks.push(v.data.clone());
        disks.push(v.hash_tree.clone());
    }
    disks
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
    // One client for every probe: rebuilding it per tick threw away the
    // connection pool and the TLS/DNS setup for no gain.
    let client = probe_client()?;
    // One persistent ticker, always selected on, so the loop head (deadline
    // and `try_wait`) runs on its own schedule no matter what the console
    // does: a silent guest cannot park us in `rx.recv()` forever, and a
    // chatty one cannot keep resetting a per-iteration timer. `tick` is
    // cancel-safe and is not rearmed when the other branch wins.
    let mut ticker = tokio::time::interval(PROBE_INTERVAL);
    // A probe can take longer than one interval; skipped ticks must not be
    // replayed back-to-back as a burst of probes.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
                Some(line) => {
                    // Bound first: an arm below reads `scanner` again.
                    let event = scanner.feed(&line);
                    match event {
                        ScanEvent::Fatal(line) => bail!("guest init failed closed: {line}"),
                        ScanEvent::Complete => probing = true,
                        // The init is past the point where it would have said
                        // so: give the diagnosis the deadline would have, now.
                        ScanEvent::NoLocalMode => bail!(
                            "{}\n--- last serial lines ---\n{}",
                            scanner.timeout_diagnosis(runtime_label, ATTEST_PORT, timeout_secs),
                            scanner.tail().join("\n")
                        ),
                        ScanEvent::Continue => {}
                    }
                }
                None => {
                    // Console closed: QEMU is exiting; the try_wait above
                    // reports it on the next turn.
                    tokio::time::sleep(PROBE_INTERVAL).await;
                }
            },
            _ = ticker.tick() => {
                if probing && probe_http(&client, url).await {
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
    fn marker_indices_point_at_their_markers() {
        assert_eq!(MARKERS[IDX_LOCAL_MODE], MARKER_LOCAL_MODE);
        assert_eq!(MARKERS[IDX_INIT_START], MARKER_INIT_START);
    }

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

    /// The init printed the firewall line and started /sbin/init without ever
    /// announcing local mode: it never will, so say so now instead of sitting
    /// out the whole timeout.
    #[test]
    fn a_missing_local_mode_line_is_called_once_the_init_has_moved_on() {
        let mut s = LineScanner::new();
        s.feed(ROOT);
        s.feed(WORKLOAD);
        s.feed(FIREWALL);
        assert!(matches!(s.feed(START), ScanEvent::Continue));
        for i in 0..(LOCAL_MODE_GRACE_LINES - 1) {
            assert!(matches!(s.feed(&format!("noise {i}")), ScanEvent::Continue));
        }
        assert!(matches!(
            s.feed("one line too many"),
            ScanEvent::NoLocalMode
        ));
        // Reported once, not again on every later line.
        assert!(matches!(s.feed("more noise"), ScanEvent::Continue));
    }

    #[test]
    fn a_local_mode_line_inside_the_grace_window_suppresses_the_verdict() {
        let mut s = LineScanner::new();
        s.feed(ROOT);
        s.feed(WORKLOAD);
        s.feed(FIREWALL);
        s.feed(START);
        s.feed("noise 0");
        // init-compose.sh prints LOCAL MODE just after the /sbin/init line.
        assert!(matches!(s.feed(LOCAL), ScanEvent::Complete));
        for i in 0..(LOCAL_MODE_GRACE_LINES + 5) {
            assert!(matches!(s.feed(&format!("noise {i}")), ScanEvent::Continue));
        }
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

    /// The guest's init addresses its disks by device order, and the cmdline's
    /// roothash slots are filled in that same order, so this vector is a wire
    /// contract rather than an implementation detail.
    #[test]
    fn disk_order_is_platform_then_workload_then_volumes_in_flag_order() {
        let artifacts = BundleArtifacts {
            ovmf: PathBuf::from("/bundle/ovmf"),
            kernel: PathBuf::from("/bundle/kernel"),
            initrd: PathBuf::from("/bundle/initrd"),
            platform_rootfs: PathBuf::from("/bundle/platform_rootfs"),
            platform_hash_tree: PathBuf::from("/bundle/platform_hash_tree"),
        };
        let verity = |name: &str| VerityArtifact {
            data: PathBuf::from(format!("/build/{name}.ext4")),
            hash_tree: PathBuf::from(format!("/build/{name}.verity")),
            root_hash: "0".repeat(64),
        };
        let volumes = [verity("vol0"), verity("vol1")];
        assert_eq!(
            disk_order(&artifacts, &verity("workload"), &volumes),
            vec![
                PathBuf::from("/bundle/platform_rootfs"),
                PathBuf::from("/bundle/platform_hash_tree"),
                PathBuf::from("/build/workload.ext4"),
                PathBuf::from("/build/workload.verity"),
                PathBuf::from("/build/vol0.ext4"),
                PathBuf::from("/build/vol0.verity"),
                PathBuf::from("/build/vol1.ext4"),
                PathBuf::from("/build/vol1.verity"),
            ]
        );
    }

    #[tokio::test]
    async fn probe_http_accepts_any_http_response_and_rejects_a_bare_close() {
        use std::io::{Read, Write};
        let client = probe_client().unwrap();
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
        assert!(probe_http(&client, &format!("http://127.0.0.1:{ok_port}/")).await);

        // A listener that accepts and closes without a byte (what SLIRP does
        // while the guest is not listening yet): not ready.
        let closer = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closer_port = closer.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((s, _)) = closer.accept() {
                drop(s);
            }
        });
        assert!(!probe_http(&client, &format!("http://127.0.0.1:{closer_port}/")).await);
    }
}
