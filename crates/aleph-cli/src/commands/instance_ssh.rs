use std::process::Command;

use aleph_sdk::crn::{ActiveVmList, ActiveVmNetworking, fetch_active_vms};
use aleph_types::item_hash::ItemHash;
use anyhow::{Context, Result, anyhow};
use url::Url;

use crate::cli::InstanceSshArgs;
use crate::commands::instance_target::resolve_target;

pub async fn handle_ssh(scheduler_url: Url, args: InstanceSshArgs) -> Result<()> {
    let http = reqwest::Client::new();

    let (vm_id, crn_url) = resolve_target(&scheduler_url, &args.vm_id, args.crn.as_deref()).await?;

    let active_vms = fetch_active_vms(&http, &crn_url)
        .await
        .with_context(|| format!("fetching executions from CRN {crn_url}"))?;

    // -4 (or --host-port) selects IPv4; -6 or the default selects IPv6.
    let (host, port) = if args.ipv4 || args.host_port.is_some() {
        resolve_target_ipv4(&active_vms, &vm_id, &crn_url, args.port, args.host_port)?
    } else {
        (
            resolve_target_ipv6(&active_vms, &vm_id, &crn_url)?,
            args.port,
        )
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

/// Look up a VM's networking block in the CRN's v2 executions map, with the
/// shared "not running" / "no networking" errors used by both families.
fn vm_networking<'a>(
    active_vms: &'a ActiveVmList,
    vm_id: &ItemHash,
    crn_url: &Url,
) -> Result<&'a ActiveVmNetworking> {
    let info = active_vms.0.get(vm_id).ok_or_else(|| {
        anyhow!(
            "VM {vm_id} is not currently running on CRN {crn_url}. \
             Use `aleph instance start --crn {crn_url} {vm_id}` to allocate it first."
        )
    })?;
    info.networking
        .as_ref()
        .ok_or_else(|| anyhow!("CRN {crn_url} reports no networking info for VM {vm_id}"))
}

/// Resolve the IPv6 SSH target: the VM's directly-routable IPv6 address, as
/// reported by the CRN's v2 executions endpoint. Returns the `(host, port)`
/// pair to hand to `ssh` (the port is the in-VM SSH port, unchanged).
fn resolve_target_ipv6(
    active_vms: &ActiveVmList,
    vm_id: &ItemHash,
    crn_url: &Url,
) -> Result<String> {
    let networking = vm_networking(active_vms, vm_id, crn_url)?;
    networking
        .ipv6_ip
        .clone()
        .ok_or_else(|| anyhow!("CRN {crn_url} has not assigned an IPv6 address to VM {vm_id}"))
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
    let networking = vm_networking(active_vms, vm_id, crn_url)?;
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

    /// Build a v2 `ActiveVmList` with a single VM `TEST_HASH` carrying the
    /// given networking blob, mirroring the real `/v2/about/executions/list`
    /// shape.
    fn active_vms(networking: serde_json::Value) -> ActiveVmList {
        let json = serde_json::json!({ TEST_HASH: { "networking": networking } });
        serde_json::from_value(json).unwrap()
    }

    fn crn() -> Url {
        Url::parse("https://crn.example.io").unwrap()
    }

    fn ipv6_target(networking: serde_json::Value) -> Result<String> {
        let vm_id: ItemHash = TEST_HASH.parse().unwrap();
        resolve_target_ipv6(&active_vms(networking), &vm_id, &crn())
    }

    #[test]
    fn ipv6_resolves_assigned_address() {
        let host = ipv6_target(serde_json::json!({ "ipv6_ip": "2a01:4f9:1a:a061:1::1" })).unwrap();
        assert_eq!(host, "2a01:4f9:1a:a061:1::1");
    }

    #[test]
    fn ipv6_missing_address_errors() {
        let err = ipv6_target(serde_json::json!({ "mapped_ports": {} })).unwrap_err();
        assert!(err.to_string().contains("IPv6"));
    }

    #[test]
    fn vm_not_running_errors() {
        // A VM hash absent from the executions map yields the shared
        // "not currently running" error.
        let vms = active_vms(serde_json::json!({ "ipv6_ip": "2a01::1" }));
        let absent: ItemHash = "0000000000000000000000000000000000000000000000000000000000000001"
            .parse()
            .unwrap();
        let err = resolve_target_ipv6(&vms, &absent, &crn()).unwrap_err();
        assert!(err.to_string().contains("not currently running"));
    }

    /// Resolve the IPv4 SSH target from a v2 networking JSON blob for VM
    /// `TEST_HASH`, mirroring the real `/v2/about/executions/list` shape.
    fn ipv4_target(
        networking: serde_json::Value,
        guest_port: u16,
        host_port_override: Option<u16>,
    ) -> Result<(String, u16)> {
        let vm_id: ItemHash = TEST_HASH.parse().unwrap();
        resolve_target_ipv4(
            &active_vms(networking),
            &vm_id,
            &crn(),
            guest_port,
            host_port_override,
        )
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
