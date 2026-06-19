//! Wait until a freshly created or started instance can actually accept SSH
//! logins (not merely until it has booted).
//!
//! The scheduler auto-dispatches instances, so `instance create --wait` never
//! notifies a CRN. The wait has two phases:
//!
//! 1. **Allocation** is observed over the scheduler's WebSocket
//!    (`/api/v1/ws?vm_hash=...`), which pushes a `Scheduled` event the moment a
//!    node is assigned, or an `Unschedulable`/`Unscheduled` event we surface as
//!    an immediate, reasoned failure instead of waiting out the timeout. If the
//!    socket is unavailable we fall back to HTTP polling, so behaviour is never
//!    worse than plain polling.
//! 2. **Reachability** is then polled from the allocated CRN. A CRN listing the
//!    VM with networking only means it is booting, so we actively probe the SSH
//!    port until sshd answers. The probe prefers IPv4 (host public IPv4 plus
//!    the forwarded SSH port), which works on virtually all nodes; some CRNs do
//!    not assign IPv6 yet. This phase has no WebSocket equivalent (the
//!    scheduler does not know VM IPs).
//!
//! The reachability poll mirrors `instance_backup::poll_until_complete`: it
//! takes an injectable `sleep` closure so unit tests run instantly, and a
//! `fetch` closure so the network access is mockable.

use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use aleph_sdk::crn::ActiveVmNetworking;
use aleph_sdk::scheduler::SchedulerClient;
use aleph_sdk::scheduler_ws::{VmSchedulingEvent, subscribe_vm};
use aleph_types::item_hash::ItemHash;
use tokio::io::AsyncReadExt;
use url::Url;

/// Interval between successive polls.
pub(crate) const WAIT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// In-VM SSH port probed for reachability. Instances created by the CLI run
/// sshd on 22, which the CRN forwards from a host port (see `mapped_ports`).
const SSH_GUEST_PORT: u16 = 22;

/// How long a single TCP connect attempt to the SSH port may take. A booting
/// VM refuses the connection immediately (fast retry); this bound only applies
/// when packets are dropped.
const SSH_PROBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait for sshd's banner once connected.
const SSH_PROBE_BANNER_TIMEOUT: Duration = Duration::from_secs(3);

/// Connectivity selected from a VM's CRN networking, applying the same rules
/// as `instance show --verbose`: IPv6 prefers the concrete address over the
/// subnet CIDR; the reachable IPv4 is the CRN host's public address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Connectivity {
    pub ipv6: Option<String>,
    pub ipv4: Option<String>,
    /// True when the SSH reachability probe succeeded over IPv4 (host public
    /// IPv4 + forwarded port). Some CRNs do not assign IPv6 yet, so when this
    /// is set the printed SSH hint recommends `-4`.
    pub reachable_via_ipv4: bool,
}

impl Connectivity {
    /// Build the display connectivity. Returns `None` when neither an IPv6 nor
    /// a host IPv4 is present, i.e. the VM has no usable networking yet.
    /// `reachable_via_ipv4` is set later by the reachability probe.
    fn from_networking(net: &ActiveVmNetworking) -> Option<Self> {
        let ipv6 = net.ipv6_ip.clone().or_else(|| net.ipv6_network.clone());
        let ipv4 = net.host_ipv4.clone();
        if ipv6.is_none() && ipv4.is_none() {
            return None;
        }
        Some(Self {
            ipv6,
            ipv4,
            reachable_via_ipv4: false,
        })
    }
}

/// Choose the endpoint to probe for SSH reachability, preferring IPv4.
///
/// IPv4 port-forwarding (the CRN host's public IPv4 plus the forwarded SSH
/// port) works on virtually all nodes, whereas some CRNs do not assign IPv6
/// yet, so a direct IPv6 connection can hang on those. Returns
/// `(ip, port, is_ipv4)`, or `None` when no probeable endpoint exists.
fn ssh_probe_target(net: &ActiveVmNetworking, guest_port: u16) -> Option<(IpAddr, u16, bool)> {
    if let (Some(host), Some(mapped)) =
        (net.host_ipv4.as_deref(), net.mapped_ports.get(&guest_port))
        && let Ok(ip) = host.parse::<IpAddr>()
    {
        return Some((ip, mapped.host, true));
    }
    if let Some(ip6) = net.ipv6_ip.as_deref()
        && let Ok(ip) = ip6.parse::<IpAddr>()
    {
        return Some((ip, guest_port, false));
    }
    None
}

/// sshd sends an identification string starting with `SSH-` as soon as a
/// connection opens. Seeing it confirms sshd is actually answering, not just
/// that the port is open.
fn is_ssh_banner(buf: &[u8]) -> bool {
    buf.starts_with(b"SSH-")
}

/// Probe SSH reachability: open a TCP connection and confirm sshd's banner.
/// This mirrors what an `ssh` attempt does, so a success means the VM is
/// genuinely ready to accept logins, not merely booting.
async fn probe_ssh(ip: IpAddr, port: u16) -> bool {
    let addr = SocketAddr::new(ip, port);
    let connect = tokio::time::timeout(
        SSH_PROBE_CONNECT_TIMEOUT,
        tokio::net::TcpStream::connect(addr),
    );
    let Ok(Ok(mut stream)) = connect.await else {
        return false;
    };
    let mut buf = [0u8; 4];
    match tokio::time::timeout(SSH_PROBE_BANNER_TIMEOUT, stream.read_exact(&mut buf)).await {
        Ok(Ok(_)) => is_ssh_banner(&buf),
        _ => false,
    }
}

/// Outcome of [`wait_until_ready`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WaitOutcome {
    Ready(Connectivity),
    /// The scheduler reported the VM cannot be placed (or was unscheduled).
    /// Carries a human-friendly reason. This is terminal: stop waiting.
    Failed(String),
    Timeout,
}

/// One snapshot of the readiness state, produced by the `fetch` closure.
#[derive(Debug, Clone)]
pub(crate) enum ReadyState {
    /// The CCN/scheduler has not allocated a node yet, or the CRN does not
    /// list the VM with usable networking yet. Keep waiting.
    Pending,
    /// The VM is reachable with the given connectivity.
    Ready(Connectivity),
}

/// Poll `fetch` until it reports [`ReadyState::Ready`], or until `timeout`
/// elapses. `sleep` lets tests inject a no-op delay. A single timeout budget
/// covers every phase (allocation, node lookup, CRN listing) because the
/// caller folds all of them into one `fetch`.
pub(crate) async fn poll_until_ready<F, Fut, S, SFut>(
    mut fetch: F,
    mut sleep: S,
    timeout: Duration,
    poll_interval: Duration,
) -> anyhow::Result<WaitOutcome>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<ReadyState>>,
    S: FnMut(Duration) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    let start = Instant::now();
    loop {
        match fetch().await? {
            ReadyState::Ready(conn) => return Ok(WaitOutcome::Ready(conn)),
            ReadyState::Pending => {
                if start.elapsed() >= timeout {
                    return Ok(WaitOutcome::Timeout);
                }
                sleep(poll_interval).await;
            }
        }
    }
}

/// Resolve the VM's allocated CRN and check whether it is reachable over SSH.
///
/// The CRN listing the VM with networking only means it is booting, not that
/// it accepts logins. So once networking is present we actively probe the SSH
/// port (preferring IPv4) and only report [`ReadyState::Ready`] when sshd
/// answers. Returns [`ReadyState::Pending`] when the VM is not allocated yet,
/// the node has no reachable address, the CRN does not (yet) list the VM with
/// usable networking, or SSH is not up yet. Errors only on hard failures
/// (malformed scheduler data, CRN HTTP errors).
async fn fetch_ready_state(
    scheduler: &SchedulerClient,
    http: &reqwest::Client,
    vm_id: &ItemHash,
) -> anyhow::Result<ReadyState> {
    let Some(vm) = scheduler.get_vm(vm_id).await? else {
        // Not found yet: the CCN may not have processed the message.
        return Ok(ReadyState::Pending);
    };
    let Some(node_hash) = vm.allocated_node else {
        return Ok(ReadyState::Pending);
    };
    let Some(node) = scheduler.get_node(&node_hash).await? else {
        return Ok(ReadyState::Pending);
    };
    let Some(addr) = node.address.as_deref() else {
        return Ok(ReadyState::Pending);
    };
    let crn_url = Url::parse(addr)?;

    let list = aleph_sdk::crn::fetch_active_vms(http, &crn_url).await?;
    let Some(entry) = list.0.get(vm_id) else {
        return Ok(ReadyState::Pending);
    };
    let Some(net) = entry.networking.as_ref() else {
        return Ok(ReadyState::Pending);
    };
    let Some(display) = Connectivity::from_networking(net) else {
        return Ok(ReadyState::Pending);
    };

    match ssh_probe_target(net, SSH_GUEST_PORT) {
        Some((ip, port, is_ipv4)) => {
            if probe_ssh(ip, port).await {
                Ok(ReadyState::Ready(Connectivity {
                    reachable_via_ipv4: is_ipv4,
                    ..display
                }))
            } else {
                Ok(ReadyState::Pending)
            }
        }
        // Networking exists but exposes no SSH endpoint to probe (no forwarded
        // port and no IPv6). We cannot verify reachability, so report ready on
        // a best-effort basis rather than waiting out the timeout.
        None => Ok(ReadyState::Ready(display)),
    }
}

/// Outcome of the allocation phase ([`wait_for_allocation`]).
enum AllocationOutcome {
    /// A node was assigned. Proceed to poll the CRN for reachability.
    Allocated,
    /// The scheduler rejected the VM. Terminal, with a friendly reason.
    Failed(String),
    /// The deadline passed before any allocation event.
    Timeout,
    /// The WebSocket could not be used; the caller should fall back to polling.
    Unavailable,
}

/// Map a scheduler reason code (e.g. `NoSuitableNode`) to a human sentence,
/// passing unknown codes through verbatim so we never hide information.
fn friendly_reason(raw: &str) -> String {
    match raw {
        "NoSuitableNode" => "no suitable node available".into(),
        "InsufficientResources" => "insufficient resources available".into(),
        "NoIpv6Node" => "no node with IPv6 connectivity available".into(),
        "PaymentFailed" => "payment failed".into(),
        "Deleted" => "the instance was deleted".into(),
        "" => "scheduler rejected the instance".into(),
        other => other.to_string(),
    }
}

/// Watch the scheduler WebSocket until the VM is scheduled, rejected, or the
/// deadline passes. Subscribes first, then does a one-shot REST check to catch
/// a VM that was already allocated (the stream replays no history), then reads
/// live events.
async fn wait_for_allocation(
    scheduler_url: &Url,
    vm_id: &ItemHash,
    deadline: Instant,
) -> AllocationOutcome {
    let vm_hash = vm_id.to_string();

    // Subscribe before the REST guard so an event landing between the two is
    // not lost (the stream is deltas-only, no snapshot on connect).
    let mut rx = match subscribe_vm(scheduler_url, &vm_hash).await {
        Ok(rx) => rx,
        Err(_) => return AllocationOutcome::Unavailable,
    };

    // Guard: the VM may already be allocated (or already rejected) before we
    // connected. Ignore transient errors here and let the stream drive.
    let scheduler = SchedulerClient::new(scheduler_url.clone());
    if let Ok(Some(vm)) = scheduler.get_vm(vm_id).await {
        if vm.allocated_node.is_some() {
            return AllocationOutcome::Allocated;
        }
        if vm.status == "unschedulable" {
            return AllocationOutcome::Failed(friendly_reason("NoSuitableNode"));
        }
    }

    loop {
        let now = Instant::now();
        if now >= deadline {
            return AllocationOutcome::Timeout;
        }
        match tokio::time::timeout(deadline - now, rx.recv()).await {
            Err(_) => return AllocationOutcome::Timeout,
            // Subscriber gave up (e.g. repeated reconnect failures): fall back.
            Ok(None) => return AllocationOutcome::Unavailable,
            Ok(Some(event)) => match event {
                // The server filters by vm_hash, but match defensively.
                VmSchedulingEvent::Scheduled { vm_hash: h, .. } if h == vm_hash => {
                    return AllocationOutcome::Allocated;
                }
                VmSchedulingEvent::Unschedulable { vm_hash: h, reason }
                | VmSchedulingEvent::Unscheduled { vm_hash: h, reason }
                    if h == vm_hash =>
                {
                    return AllocationOutcome::Failed(friendly_reason(&reason));
                }
                _ => continue,
            },
        }
    }
}

/// Drive the wait against the live scheduler/CRN. Phase 1 observes allocation
/// over the WebSocket (falling back to polling if unavailable); phase 2 polls
/// the allocated CRN for reachability, sleeping for real between attempts. Used
/// by `instance create --wait` and `instance start --wait`.
pub(crate) async fn wait_until_ready(
    scheduler_url: &Url,
    vm_id: &ItemHash,
    timeout: Duration,
) -> anyhow::Result<WaitOutcome> {
    let deadline = Instant::now() + timeout;

    match wait_for_allocation(scheduler_url, vm_id, deadline).await {
        AllocationOutcome::Failed(reason) => return Ok(WaitOutcome::Failed(reason)),
        AllocationOutcome::Timeout => return Ok(WaitOutcome::Timeout),
        // Allocated: proceed to the reachability poll. Unavailable: the poll
        // loop re-checks allocation itself, so this degrades to plain polling.
        AllocationOutcome::Allocated | AllocationOutcome::Unavailable => {}
    }

    let remaining = deadline.saturating_duration_since(Instant::now());
    let scheduler = SchedulerClient::new(scheduler_url.clone());
    let http = reqwest::Client::new();
    poll_until_ready(
        || fetch_ready_state(&scheduler, &http, vm_id),
        tokio::time::sleep,
        remaining,
        WAIT_POLL_INTERVAL,
    )
    .await
}

/// Report the wait outcome to the user and translate it into a process result.
/// `Ready`/`Timeout` succeed (the create/start itself already succeeded);
/// `Failed` returns an error so the command exits non-zero, since the instance
/// will not become reachable without intervention.
pub(crate) fn finish_wait(
    outcome: WaitOutcome,
    vm_id: &ItemHash,
    json: bool,
) -> anyhow::Result<()> {
    match outcome {
        WaitOutcome::Ready(conn) => {
            report_ready(&conn, vm_id, json);
            Ok(())
        }
        WaitOutcome::Timeout => {
            report_timeout(vm_id, json);
            Ok(())
        }
        WaitOutcome::Failed(reason) => {
            if json {
                let payload = serde_json::json!({
                    "ready": false,
                    "error": reason,
                });
                println!("{payload}");
            }
            Err(anyhow::anyhow!(
                "instance could not be scheduled: {reason} \
                 (check with `aleph instance show {vm_id} --verbose`)"
            ))
        }
    }
}

/// Report a successful wait to the user. Human output goes to stderr (so it
/// does not pollute `--json` consumers); the SSH hint references the item
/// hash. When `json` is set, the connectivity is merged into the caller's
/// JSON object instead.
pub(crate) fn report_ready(conn: &Connectivity, vm_id: &ItemHash, json: bool) {
    // Recommend `-4` when the VM was confirmed reachable over IPv4, since some
    // CRNs do not assign IPv6 and the default `instance ssh` path is IPv6.
    let ssh_hint = if conn.reachable_via_ipv4 {
        format!("aleph instance ssh {vm_id} -4")
    } else {
        format!("aleph instance ssh {vm_id}")
    };

    if json {
        let payload = serde_json::json!({
            "ready": true,
            "ipv6": conn.ipv6,
            "ipv4": conn.ipv4,
            "reachable_via": if conn.reachable_via_ipv4 { "ipv4" } else { "ipv6" },
        });
        println!("{payload}");
    } else {
        eprintln!("Instance ready.");
        if let Some(ipv6) = &conn.ipv6 {
            eprintln!("  IPv6: {ipv6}");
        }
        if let Some(ipv4) = &conn.ipv4 {
            eprintln!("  IPv4: {ipv4}");
        }
        eprintln!("  SSH:  {ssh_hint}");
    }
}

/// Report a wait timeout. The create/start itself succeeded; this only tells
/// the user the VM is not reachable yet.
pub(crate) fn report_timeout(vm_id: &ItemHash, json: bool) {
    if json {
        let payload = serde_json::json!({
            "ready": false,
            "ipv6": serde_json::Value::Null,
            "ipv4": serde_json::Value::Null,
        });
        println!("{payload}");
    } else {
        eprintln!(
            "warning: instance not reachable yet; \
             check with `aleph instance show {vm_id} --verbose`"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn conn(ipv6: &str) -> Connectivity {
        Connectivity {
            ipv6: Some(ipv6.to_string()),
            ipv4: None,
            reachable_via_ipv4: false,
        }
    }

    fn networking(
        host_ipv4: Option<&str>,
        ipv6_ip: Option<&str>,
        ssh_host_port: Option<u16>,
    ) -> ActiveVmNetworking {
        let mut mapped_ports = std::collections::BTreeMap::new();
        if let Some(host) = ssh_host_port {
            mapped_ports.insert(
                SSH_GUEST_PORT,
                aleph_sdk::crn::MappedPort {
                    host,
                    extra: Default::default(),
                },
            );
        }
        ActiveVmNetworking {
            mapped_ports,
            ipv6_ip: ipv6_ip.map(Into::into),
            ipv6_network: None,
            ipv4_ip: None,
            ipv4_network: None,
            host_ipv4: host_ipv4.map(Into::into),
        }
    }

    #[test]
    fn ssh_probe_target_prefers_ipv4_mapped_port() {
        let net = networking(Some("1.2.3.4"), Some("2a01::5"), Some(24221));
        let (ip, port, is_ipv4) = ssh_probe_target(&net, SSH_GUEST_PORT).unwrap();
        assert_eq!(ip.to_string(), "1.2.3.4");
        assert_eq!(port, 24221);
        assert!(is_ipv4);
    }

    #[test]
    fn ssh_probe_target_falls_back_to_ipv6() {
        // No forwarded SSH port, so IPv4 is not probeable; use direct IPv6.
        let net = networking(Some("1.2.3.4"), Some("2a01::5"), None);
        let (ip, port, is_ipv4) = ssh_probe_target(&net, SSH_GUEST_PORT).unwrap();
        assert_eq!(ip.to_string(), "2a01::5");
        assert_eq!(port, SSH_GUEST_PORT);
        assert!(!is_ipv4);
    }

    #[test]
    fn ssh_probe_target_none_when_no_endpoint() {
        let net = networking(Some("1.2.3.4"), None, None);
        assert!(ssh_probe_target(&net, SSH_GUEST_PORT).is_none());
    }

    #[test]
    fn ssh_banner_detection() {
        assert!(is_ssh_banner(b"SSH-"));
        assert!(is_ssh_banner(b"SSH-2.0-OpenSSH_9.6"));
        assert!(!is_ssh_banner(b"HTTP"));
        assert!(!is_ssh_banner(b"\x00\x00\x00\x00"));
    }

    #[tokio::test]
    async fn probe_ssh_succeeds_on_ssh_banner() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let _ = sock.write_all(b"SSH-2.0-OpenSSH_9.6\r\n").await;
            }
        });
        assert!(probe_ssh(addr.ip(), addr.port()).await);
    }

    #[tokio::test]
    async fn probe_ssh_fails_on_non_ssh_banner() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let _ = sock.write_all(b"HTTP/1.1 200 OK\r\n").await;
            }
        });
        assert!(!probe_ssh(addr.ip(), addr.port()).await);
    }

    #[tokio::test]
    async fn probe_ssh_fails_on_closed_port() {
        // Bind then drop to get a port nothing listens on.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        assert!(!probe_ssh(addr.ip(), addr.port()).await);
    }

    /// VM becomes ready after a few pending polls: the fetcher returns
    /// not-allocated, then allocated-without-networking, then networking.
    #[tokio::test]
    async fn becomes_ready_after_a_few_polls() {
        let states = RefCell::new(vec![
            ReadyState::Pending, // not allocated yet
            ReadyState::Pending, // allocated, no networking yet
            ReadyState::Ready(conn("2a01::1")),
        ]);
        let mut idx = 0usize;
        let slept = RefCell::new(0u32);

        let outcome = poll_until_ready(
            || {
                let s = states.borrow()[idx].clone();
                idx += 1;
                async move { Ok(s) }
            },
            |_d| {
                *slept.borrow_mut() += 1;
                async {}
            },
            Duration::from_secs(300),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        assert_eq!(outcome, WaitOutcome::Ready(conn("2a01::1")));
        // Slept once after each of the two pending polls.
        assert_eq!(*slept.borrow(), 2);
    }

    /// The VM never becomes ready, so the loop hits the timeout. A zero
    /// timeout means the first pending poll returns `Timeout` immediately.
    #[tokio::test]
    async fn hits_timeout_when_never_ready() {
        let slept = RefCell::new(0u32);
        let outcome = poll_until_ready(
            || async { Ok(ReadyState::Pending) },
            |_d| {
                *slept.borrow_mut() += 1;
                async {}
            },
            Duration::from_secs(0),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        assert_eq!(outcome, WaitOutcome::Timeout);
        // Elapsed >= 0 on the first pending poll, so we never slept.
        assert_eq!(*slept.borrow(), 0);
    }

    #[test]
    fn connectivity_prefers_ipv6_ip_over_network() {
        let net = ActiveVmNetworking {
            mapped_ports: Default::default(),
            ipv6_ip: Some("2a01::5".into()),
            ipv6_network: Some("2a01::0/124".into()),
            ipv4_ip: Some("172.16.0.2".into()),
            ipv4_network: Some("172.16.0.0/24".into()),
            host_ipv4: Some("1.2.3.4".into()),
        };
        let c = Connectivity::from_networking(&net).unwrap();
        assert_eq!(c.ipv6.as_deref(), Some("2a01::5"));
        assert_eq!(c.ipv4.as_deref(), Some("1.2.3.4"));
    }

    #[test]
    fn friendly_reason_maps_known_codes_and_passes_through() {
        assert_eq!(
            friendly_reason("NoSuitableNode"),
            "no suitable node available"
        );
        assert_eq!(
            friendly_reason("InsufficientResources"),
            "insufficient resources available"
        );
        // Unknown codes pass through verbatim so nothing is hidden.
        assert_eq!(friendly_reason("SomethingNew"), "SomethingNew");
        // Empty reason gets a sensible default.
        assert_eq!(friendly_reason(""), "scheduler rejected the instance");
    }

    #[test]
    fn connectivity_none_when_no_ip() {
        let net = ActiveVmNetworking {
            mapped_ports: Default::default(),
            ipv6_ip: None,
            ipv6_network: None,
            ipv4_ip: Some("172.16.0.2".into()),
            ipv4_network: Some("172.16.0.0/24".into()),
            host_ipv4: None,
        };
        assert!(Connectivity::from_networking(&net).is_none());
    }
}
