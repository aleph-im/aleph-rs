use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::process::Command;

use aleph_sdk::crn::{ActiveVmList, ExecutionInfo, fetch_active_vms, fetch_executions};
use aleph_types::item_hash::ItemHash;
use anyhow::{Context, Result, anyhow, bail};
use url::Url;

use crate::cli::InstanceSshArgs;
use crate::commands::instance_target::resolve_target;

pub async fn handle_ssh(scheduler_url: Url, args: InstanceSshArgs) -> Result<()> {
    let http = reqwest::Client::new();

    let (vm_id, crn_url) = resolve_target(&scheduler_url, &args.vm_id, args.crn.as_deref()).await?;

    // -4 (or --host-port) selects IPv4; -6 or the default selects IPv6.
    let (host, port) = if args.ipv4 || args.host_port.is_some() {
        let active_vms = fetch_active_vms(&http, &crn_url)
            .await
            .with_context(|| format!("fetching executions from CRN {crn_url}"))?;
        resolve_target_ipv4(&active_vms, &vm_id, &crn_url, args.port, args.host_port)?
    } else {
        let executions = fetch_executions(&http, &crn_url)
            .await
            .with_context(|| format!("fetching executions from CRN {crn_url}"))?;
        let target = resolve_target_ipv6(&executions, &vm_id, &crn_url)?;
        (target.to_string(), args.port)
    };

    eprintln!("Connecting to {host} port {port} (CRN: {crn_url})");

    let mut cmd = Command::new("ssh");
    cmd.arg("-p").arg(port.to_string());
    if let Some(path) = &args.identity {
        cmd.arg("-i").arg(path);
    }
    cmd.arg(format!("{}@{host}", args.user));
    cmd.args(&args.ssh_args);

    let status = cmd.status().context("failed to spawn ssh")?;
    std::process::exit(status.code().unwrap_or(1));
}

/// Pick the SSH target IPv6 out of the CRN's executions map.
///
/// The CRN reports a /124 network prefix per VM, e.g.
/// `fc00:1:2:3:1:d2b7:4aa2:9890/124`. We mask the host bits to zero and
/// then set the first-host bit, so the result is always the first usable
/// address in the range regardless of which member of the range the CRN
/// chose to report. Only /124 is supported; aleph-vm doesn't currently
/// hand out any other prefix length, so anything else is treated as a
/// schema change worth surfacing.
fn resolve_target_ipv6(
    executions: &HashMap<String, ExecutionInfo>,
    vm_id: &ItemHash,
    crn_url: &Url,
) -> Result<Ipv6Addr> {
    let vm_hash = vm_id.to_string();
    let info = executions.get(&vm_hash).ok_or_else(|| {
        anyhow!(
            "VM {vm_id} is not currently running on CRN {crn_url}. \
             Use `aleph instance start --crn {crn_url} {vm_id}` to allocate it first."
        )
    })?;
    let networking = info
        .networking
        .as_ref()
        .ok_or_else(|| anyhow!("CRN {crn_url} reports no networking info for VM {vm_id}"))?;
    let cidr = networking
        .ipv6
        .as_deref()
        .ok_or_else(|| anyhow!("CRN {crn_url} has not assigned an IPv6 address to VM {vm_id}"))?;

    let (net_str, prefix_str) = cidr
        .split_once('/')
        .ok_or_else(|| anyhow!("malformed IPv6 CIDR `{cidr}` from CRN {crn_url}"))?;
    let prefix: u8 = prefix_str
        .parse()
        .with_context(|| format!("parsing prefix length from `{cidr}`"))?;
    if prefix != 124 {
        bail!(
            "CRN {crn_url} reported an unexpected IPv6 prefix `/{prefix}` for VM {vm_id} \
             (expected /124). Refusing to guess at the first-host offset."
        );
    }
    let net: Ipv6Addr = net_str
        .parse()
        .with_context(|| format!("parsing IPv6 from `{cidr}`"))?;
    let mut octets = net.octets();
    // /124 = 4 host bits in the low nibble of octets[15]. Clear them first
    // so we end up at the first host regardless of whether the CRN reported
    // the network base or any other member of the /124.
    octets[15] &= 0xf0;
    octets[15] |= 0x01;
    Ok(Ipv6Addr::from(octets))
}

/// Resolve the IPv4 SSH target for a VM out of the CRN's v2 executions map.
///
/// The VM has no public IPv4 - only a private NAT'd address. The way in over
/// IPv4 is the CRN host's public IPv4 (`host_ipv4`) plus a forwarded port
/// (`mapped_ports`, keyed by the in-VM "guest" port, value `.host` = the
/// host-side port). We resolve the host port for `guest_port` (the in-VM SSH
/// port) unless the caller supplies an explicit `host_port_override`.
///
/// Returns the `(host, port)` pair to hand to `ssh`.
fn resolve_target_ipv4(
    active_vms: &ActiveVmList,
    vm_id: &ItemHash,
    crn_url: &Url,
    guest_port: u16,
    host_port_override: Option<u16>,
) -> Result<(String, u16)> {
    let info = active_vms.0.get(vm_id).ok_or_else(|| {
        anyhow!(
            "VM {vm_id} is not currently running on CRN {crn_url}. \
             Use `aleph instance start --crn {crn_url} {vm_id}` to allocate it first."
        )
    })?;
    let networking = info
        .networking
        .as_ref()
        .ok_or_else(|| anyhow!("CRN {crn_url} reports no networking info for VM {vm_id}"))?;
    let host_ipv4 = networking
        .host_ipv4
        .as_deref()
        .ok_or_else(|| anyhow!("CRN {crn_url} reports no host IPv4 address for VM {vm_id}"))?;

    let host_port = match host_port_override {
        Some(port) => port,
        None => networking
            .mapped_ports
            .get(&guest_port)
            .map(|mapped| mapped.host)
            .ok_or_else(|| {
                anyhow!(
                    "No port forward for guest port {guest_port} on VM {vm_id}. \
                     Create one with `aleph instance port-forward`, or pass the host \
                     port directly with `--host-port`."
                )
            })?,
    };

    Ok((host_ipv4.to_string(), host_port))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000042";

    fn cidr_to_ssh_target_result(cidr: &str) -> Result<Ipv6Addr> {
        let executions = HashMap::from([(
            TEST_HASH.to_string(),
            ExecutionInfo {
                networking: Some(aleph_sdk::crn::ExecutionNetworking {
                    ipv4: None,
                    ipv6: Some(cidr.to_string()),
                }),
            },
        )]);
        let vm_id: ItemHash = TEST_HASH.parse().unwrap();
        let crn = Url::parse("https://crn.example.io").unwrap();
        resolve_target_ipv6(&executions, &vm_id, &crn)
    }

    fn cidr_to_ssh_target(cidr: &str) -> Ipv6Addr {
        cidr_to_ssh_target_result(cidr).unwrap()
    }

    #[test]
    fn first_host_in_aligned_124() {
        let ip = cidr_to_ssh_target("fc00:1:2:3:1:d2b7:4aa2:9890/124");
        assert_eq!(ip.to_string(), "fc00:1:2:3:1:d2b7:4aa2:9891");
    }

    #[test]
    fn unaligned_input_is_masked_to_first_host() {
        // If the CRN ever reports the VM's own interface address instead of
        // the network base, we should still end up at the first host of the
        // /124 rather than connecting to whatever address the CRN happened
        // to mention.
        let ip = cidr_to_ssh_target("fc00:1:2:3:1:d2b7:4aa2:9895/124");
        assert_eq!(ip.to_string(), "fc00:1:2:3:1:d2b7:4aa2:9891");
    }

    #[test]
    fn already_at_first_host_is_idempotent() {
        let ip = cidr_to_ssh_target("fc00::1/124");
        assert_eq!(ip.to_string(), "fc00::1");
    }

    #[test]
    fn unsupported_prefix_length_errors() {
        let err = cidr_to_ssh_target_result("fc00::/120").unwrap_err();
        assert!(err.to_string().contains("unexpected IPv6 prefix `/120`"));
    }

    #[test]
    fn malformed_cidr_errors() {
        assert!(cidr_to_ssh_target_result("not-an-ip").is_err());
        assert!(cidr_to_ssh_target_result("fc00::/notnum").is_err());
    }

    /// Resolve the IPv4 SSH target from a v2 networking JSON blob for VM
    /// `TEST_HASH`, mirroring the real `/v2/about/executions/list` shape.
    fn ipv4_target(
        networking: serde_json::Value,
        guest_port: u16,
        host_port_override: Option<u16>,
    ) -> Result<(String, u16)> {
        let json = serde_json::json!({ TEST_HASH: { "networking": networking } });
        let vms: aleph_sdk::crn::ActiveVmList = serde_json::from_value(json).unwrap();
        let vm_id: ItemHash = TEST_HASH.parse().unwrap();
        let crn = Url::parse("https://crn.example.io").unwrap();
        resolve_target_ipv4(&vms, &vm_id, &crn, guest_port, host_port_override)
    }

    #[test]
    fn ipv4_resolves_host_port_from_mapping() {
        let (host, port) = ipv4_target(
            serde_json::json!({
                "host_ipv4": "37.27.143.174",
                "mapped_ports": { "22": { "host": 24221 } }
            }),
            22,
            None,
        )
        .unwrap();
        assert_eq!(host, "37.27.143.174");
        assert_eq!(port, 24221);
    }

    #[test]
    fn ipv4_host_port_override_bypasses_mapping() {
        // No mapping at all, but an explicit host port still connects.
        let (host, port) = ipv4_target(
            serde_json::json!({ "host_ipv4": "37.27.143.174", "mapped_ports": {} }),
            22,
            Some(50022),
        )
        .unwrap();
        assert_eq!(host, "37.27.143.174");
        assert_eq!(port, 50022);
    }

    #[test]
    fn ipv4_missing_mapping_errors() {
        let err = ipv4_target(
            serde_json::json!({ "host_ipv4": "37.27.143.174", "mapped_ports": {} }),
            22,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("port-forward"));
    }

    #[test]
    fn ipv4_missing_host_ipv4_errors() {
        let err = ipv4_target(
            serde_json::json!({ "mapped_ports": { "22": { "host": 24221 } } }),
            22,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("IPv4"));
    }

    #[test]
    fn ipv4_resolves_non_default_guest_port() {
        let (_host, port) = ipv4_target(
            serde_json::json!({
                "host_ipv4": "37.27.143.174",
                "mapped_ports": { "22": { "host": 24221 }, "8080": { "host": 24222 } }
            }),
            8080,
            None,
        )
        .unwrap();
        assert_eq!(port, 24222);
    }
}
