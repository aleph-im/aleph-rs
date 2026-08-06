//! V-Program endpoint discovery.
//!
//! Once a V-Program instance has been allocated on a CRN, the CLI needs to
//! find the attested endpoint (the CRN host's public IPv4, on the port the
//! CRN mapped for the guest's attestation-agent listener) before it can
//! drive `call`/`show` against it. This module resolves that mapping from a
//! CRN's `/v2/about/executions/list` networking snapshot into a URL.

use url::Url;

use crate::crn::ActiveVmNetworking;

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
