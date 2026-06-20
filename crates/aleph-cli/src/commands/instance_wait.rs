//! Poll until a freshly created or started instance is actually reachable.
//!
//! "Ready" means the VM appears in its allocated CRN's
//! `/v2/about/executions/list` with networking populated (an IPv6 or host
//! IPv4). The same data we poll for is what we print. The scheduler
//! auto-dispatches instances, so `instance create --wait` never notifies a
//! CRN; it only polls.
//!
//! The poll loop mirrors `instance_backup::poll_until_complete`: it takes an
//! injectable `sleep` closure so unit tests run instantly, and a `fetch`
//! closure so the network access is mockable.

use std::time::{Duration, Instant};

use aleph_sdk::crn::ActiveVmNetworking;
use aleph_sdk::scheduler::SchedulerClient;
use aleph_types::item_hash::ItemHash;
use url::Url;

/// Interval between successive polls.
pub(crate) const WAIT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Connectivity selected from a VM's CRN networking, applying the same rules
/// as `instance show --verbose`: IPv6 prefers the concrete address over the
/// subnet CIDR; the reachable IPv4 is the CRN host's public address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Connectivity {
    pub ipv6: Option<String>,
    pub ipv4: Option<String>,
}

impl Connectivity {
    /// Apply the networking-selection rules. Returns `None` when neither an
    /// IPv6 nor a host IPv4 is present, i.e. the VM is not reachable yet.
    fn from_networking(net: &ActiveVmNetworking) -> Option<Self> {
        let ipv6 = net.ipv6_ip.clone().or_else(|| net.ipv6_network.clone());
        let ipv4 = net.host_ipv4.clone();
        if ipv6.is_none() && ipv4.is_none() {
            return None;
        }
        Some(Self { ipv6, ipv4 })
    }
}

/// Outcome of [`poll_until_ready`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WaitOutcome {
    Ready(Connectivity),
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

/// Resolve the VM's allocated CRN and read its current connectivity.
///
/// Returns [`ReadyState::Pending`] when the VM is not allocated yet, when the
/// node has no reachable address, or when the CRN does not (yet) list the VM
/// with usable networking. Errors only on hard failures (malformed scheduler
/// data, CRN HTTP errors).
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
    match Connectivity::from_networking(net) {
        Some(conn) => Ok(ReadyState::Ready(conn)),
        None => Ok(ReadyState::Pending),
    }
}

/// Drive the readiness poll against the live scheduler/CRN, sleeping for real
/// between attempts. Used by `instance create --wait` and `instance start
/// --wait`.
pub(crate) async fn wait_until_ready(
    scheduler_url: &Url,
    vm_id: &ItemHash,
    timeout: Duration,
) -> anyhow::Result<WaitOutcome> {
    let scheduler = SchedulerClient::new(scheduler_url.clone());
    let http = reqwest::Client::new();
    poll_until_ready(
        || fetch_ready_state(&scheduler, &http, vm_id),
        tokio::time::sleep,
        timeout,
        WAIT_POLL_INTERVAL,
    )
    .await
}

/// Report a successful wait to the user. Human output goes to stderr (so it
/// does not pollute `--json` consumers); the SSH hint references the item
/// hash. When `json` is set, the connectivity is merged into the caller's
/// JSON object instead.
pub(crate) fn report_ready(conn: &Connectivity, vm_id: &ItemHash, json: bool) {
    if json {
        let payload = serde_json::json!({
            "ready": true,
            "ipv6": conn.ipv6,
            "ipv4": conn.ipv4,
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
        eprintln!("  SSH:  aleph instance ssh {vm_id}");
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
        }
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
