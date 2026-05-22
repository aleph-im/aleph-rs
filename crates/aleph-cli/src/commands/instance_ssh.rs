use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::process::Command;

use aleph_sdk::crn::{ExecutionInfo, fetch_executions};
use aleph_types::item_hash::ItemHash;
use anyhow::{Context, Result, anyhow, bail};
use url::Url;

use crate::cli::InstanceSshArgs;
use crate::commands::instance_target::resolve_target;

pub async fn handle_ssh(scheduler_url: Url, args: InstanceSshArgs) -> Result<()> {
    let http = reqwest::Client::new();

    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id, args.crn_url.as_deref()).await?;

    let executions = fetch_executions(&http, &crn_url)
        .await
        .with_context(|| format!("fetching executions from CRN {crn_url}"))?;
    let target = resolve_target_ipv6(&executions, &vm_id, &crn_url)?;

    eprintln!("Connecting to {target} (CRN: {crn_url})");

    let mut cmd = Command::new("ssh");
    cmd.arg("-p").arg(args.port.to_string());
    if let Some(path) = &args.identity {
        cmd.arg("-i").arg(path);
    }
    cmd.arg(format!("{}@{target}", args.user));
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
             Use `aleph instance start --crn-url {crn_url} {vm_id}` to allocate it first."
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
}
