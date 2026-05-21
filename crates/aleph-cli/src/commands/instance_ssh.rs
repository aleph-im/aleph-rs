use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::process::Command;

use aleph_sdk::crn::{ExecutionInfo, fetch_executions};
use aleph_sdk::crns_list::{DEFAULT_CRN_LIST_URL, fetch_crns_list};
use aleph_sdk::scheduler::SchedulerClient;
use aleph_types::item_hash::ItemHash;
use anyhow::{Context, Result, anyhow, bail};
use url::Url;

use crate::cli::InstanceSshArgs;

const SCHEDULER_BASE_URL: &str = "https://scheduler.api.aleph.cloud";

pub async fn handle_ssh(args: InstanceSshArgs) -> Result<()> {
    let crn_url = match &args.crn_url {
        Some(url) => Url::parse(url).context("invalid --crn-url")?,
        None => discover_crn_url(&args.vm_id).await?,
    };

    let http = reqwest::Client::new();
    let executions = fetch_executions(&http, &crn_url)
        .await
        .with_context(|| format!("fetching executions from CRN {crn_url}"))?;
    let target = resolve_target_ipv6(&executions, &args.vm_id, &crn_url)?;

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

/// Look up the CRN that owns a dispatched VM. Returns the CRN's HTTP URL.
async fn discover_crn_url(vm_id: &ItemHash) -> Result<Url> {
    let scheduler = SchedulerClient::new(
        Url::parse(SCHEDULER_BASE_URL).expect("SCHEDULER_BASE_URL is a valid URL"),
    );
    let entry = scheduler
        .get_vm(vm_id)
        .await
        .context("querying scheduler")?
        .ok_or_else(|| anyhow!("instance {vm_id} not found in the scheduler"))?;

    // `duplicated` means the VM is allocated on multiple CRNs because of a
    // re-scheduling race; the `allocated_node` still points to the
    // canonical placement, so we follow it.
    let status = entry.status.as_str();
    let allocated_node = match status {
        "dispatched" | "duplicated" => entry.allocated_node.as_deref().ok_or_else(|| {
            anyhow!("instance {vm_id} has status `{status}` but no allocated_node")
        })?,
        _ => bail!(
            "instance {vm_id} cannot be reached via SSH: scheduler reports status `{status}` \
             (expected `dispatched`). Use `aleph instance start` to allocate it, or wait for \
             the scheduler to dispatch it."
        ),
    };

    let list_url = Url::parse(DEFAULT_CRN_LIST_URL).expect("DEFAULT_CRN_LIST_URL is a valid URL");
    let http = reqwest::Client::new();
    let crns = fetch_crns_list(&http, &list_url, true)
        .await
        .context("fetching the public CRN list")?;
    let crn = crns
        .crns
        .iter()
        .find(|c| c.hash == allocated_node)
        .ok_or_else(|| {
            anyhow!(
                "instance {vm_id} is allocated to node {allocated_node}, but that CRN is not in \
                 the public CRN list (it may be inactive). Pass `--crn-url` to override."
            )
        })?;

    Url::parse(&crn.address).with_context(|| format!("invalid CRN address `{}`", crn.address))
}

/// Pick the SSH target IPv6 out of the CRN's executions map.
///
/// The CRN reports a /124 network prefix per VM, e.g.
/// `fc00:1:2:3:1:d2b7:4aa2:9890/124`. The VM is reachable on the first host
/// address of that range (network base OR'd with 1) — this matches
/// aleph-vm's firecracker networking convention.
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

    let (net_str, _prefix) = cidr
        .split_once('/')
        .ok_or_else(|| anyhow!("malformed IPv6 CIDR `{cidr}` from CRN {crn_url}"))?;
    let net: Ipv6Addr = net_str
        .parse()
        .with_context(|| format!("parsing IPv6 from `{cidr}`"))?;
    let mut octets = net.octets();
    octets[15] |= 0x01;
    Ok(Ipv6Addr::from(octets))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000042";

    fn cidr_to_ssh_target(cidr: &str) -> Ipv6Addr {
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
        resolve_target_ipv6(&executions, &vm_id, &crn).unwrap()
    }

    #[test]
    fn first_host_in_aligned_124() {
        let ip = cidr_to_ssh_target("fc00:1:2:3:1:d2b7:4aa2:9890/124");
        assert_eq!(ip.to_string(), "fc00:1:2:3:1:d2b7:4aa2:9891");
    }

    #[test]
    fn unaligned_low_bits_are_or_not_added() {
        // If the CRN ever reports a CIDR whose network address already has the
        // low bit set, OR-ing is a no-op rather than incorrectly bumping it.
        let ip = cidr_to_ssh_target("fc00::1/124");
        assert_eq!(ip.to_string(), "fc00::1");
    }
}
