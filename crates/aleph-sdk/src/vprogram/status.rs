//! V-Program endpoint discovery.
//!
//! Once a V-Program instance has been allocated on a CRN, the CLI needs to
//! find the attested endpoint (the CRN host's public IPv4, on the port the
//! CRN mapped for the guest's attestation-agent listener) before it can
//! drive `call`/`show` against it. This module polls
//! `/v2/about/executions/list` until that mapping shows up, then resolves it
//! into a URL.

use std::time::Duration;

use aleph_types::item_hash::ItemHash;
use tokio::time::sleep;
use url::Url;

use crate::crn::{ActiveVmNetworking, CrnError, fetch_active_vms};

/// Poll interval while waiting for a V-Program to reach RUNNING with a
/// resolvable attested endpoint.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Errors from [`wait_for_running`].
#[derive(Debug, thiserror::Error)]
pub enum StatusError {
    /// A `fetch_active_vms` call failed.
    #[error(transparent)]
    Fetch(#[from] CrnError),

    /// Timed out waiting for the instance to appear with a resolvable
    /// attested endpoint. Carries the last observed state so callers can
    /// give the user something actionable.
    #[error("timed out after {elapsed:?} waiting for {item_hash} to be reachable: {last_state}")]
    Timeout {
        item_hash: ItemHash,
        elapsed: Duration,
        last_state: LastObservedState,
    },
}

/// What the last poll saw for the target item hash, used to make
/// [`StatusError::Timeout`] actionable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LastObservedState {
    /// The item hash never appeared in the CRN's active VM list.
    NotPresent,
    /// The VM is listed but the CRN hasn't reported a `host_ipv4` yet.
    PresentWithoutHostIp,
    /// The VM has a `host_ipv4` but the attestation port isn't mapped yet.
    PresentWithoutMappedPort { attest_port: u16 },
}

impl std::fmt::Display for LastObservedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LastObservedState::NotPresent => write!(f, "instance not present in executions list"),
            LastObservedState::PresentWithoutHostIp => {
                write!(f, "instance present but no host_ipv4 reported yet")
            }
            LastObservedState::PresentWithoutMappedPort { attest_port } => {
                write!(
                    f,
                    "instance present with host_ipv4 but port {attest_port} not mapped yet"
                )
            }
        }
    }
}

/// Resolve the attested endpoint for a running V-Program from its CRN
/// networking info: `https://{host_ipv4}:{mapped_ports[attest_port].host}`.
///
/// Returns `None` if the CRN hasn't reported a `host_ipv4` yet, or if
/// `attest_port` isn't (yet) present in `mapped_ports`.
pub fn resolve_attested_endpoint(net: &ActiveVmNetworking, attest_port: u16) -> Option<Url> {
    let host_ipv4 = net.host_ipv4.as_deref()?;
    let mapped = net.mapped_ports.get(&attest_port)?;
    Url::parse(&format!("https://{host_ipv4}:{}", mapped.host)).ok()
}

/// Poll a CRN's `/v2/about/executions/list` every ~5s until `item_hash` is
/// present, has a `host_ipv4`, and has `attest_port` mapped - i.e. until
/// [`resolve_attested_endpoint`] would succeed for it. Returns the
/// `ActiveVmNetworking` snapshot that satisfied the wait.
///
/// On timeout, returns [`StatusError::Timeout`] describing the last
/// observed state (not present / present without host IP / present without
/// the mapped port), so a caller can tell the user what's actually stuck.
pub async fn wait_for_running(
    http: &reqwest::Client,
    crn_url: &Url,
    item_hash: &ItemHash,
    attest_port: u16,
    timeout: Duration,
) -> Result<ActiveVmNetworking, StatusError> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let active = fetch_active_vms(http, crn_url).await?;

        let last_state = match active.0.get(item_hash) {
            Some(vm) => match &vm.networking {
                Some(net) if net.host_ipv4.is_some() => {
                    if net.mapped_ports.contains_key(&attest_port) {
                        return Ok(net.clone());
                    }
                    LastObservedState::PresentWithoutMappedPort { attest_port }
                }
                _ => LastObservedState::PresentWithoutHostIp,
            },
            None => LastObservedState::NotPresent,
        };

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(StatusError::Timeout {
                item_hash: item_hash.clone(),
                elapsed: timeout,
                last_state,
            });
        }

        let remaining = deadline - now;
        sleep(POLL_INTERVAL.min(remaining)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crn::MappedPort;
    use std::collections::BTreeMap;

    fn networking(
        host_ipv4: Option<&str>,
        mapped_ports: BTreeMap<u16, MappedPort>,
    ) -> ActiveVmNetworking {
        ActiveVmNetworking {
            mapped_ports,
            ipv6_ip: None,
            ipv6_network: None,
            ipv4_ip: None,
            ipv4_network: None,
            host_ipv4: host_ipv4.map(str::to_string),
        }
    }

    fn mapped_port(host: u16) -> MappedPort {
        MappedPort {
            host,
            extra: Default::default(),
        }
    }

    #[test]
    fn resolves_when_host_ipv4_and_mapped_port_present() {
        let mut mapped_ports = BTreeMap::new();
        mapped_ports.insert(8443, mapped_port(24101));
        let net = networking(Some("203.0.113.5"), mapped_ports);

        let url = resolve_attested_endpoint(&net, 8443).expect("should resolve");
        assert_eq!(url.as_str(), "https://203.0.113.5:24101/");
    }

    #[test]
    fn none_when_host_ipv4_missing() {
        let mut mapped_ports = BTreeMap::new();
        mapped_ports.insert(8443, mapped_port(24101));
        let net = networking(None, mapped_ports);

        assert!(resolve_attested_endpoint(&net, 8443).is_none());
    }

    #[test]
    fn none_when_attest_port_not_mapped() {
        let net = networking(Some("203.0.113.5"), BTreeMap::new());

        assert!(resolve_attested_endpoint(&net, 8443).is_none());
    }
}
