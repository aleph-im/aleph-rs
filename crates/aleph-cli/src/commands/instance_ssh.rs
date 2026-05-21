use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::process::Command;

use aleph_sdk::crn::{ExecutionInfo, fetch_executions};
use aleph_sdk::crns_list::{DEFAULT_CRN_LIST_URL, fetch_crns_list};
use aleph_sdk::scheduler::{SchedulerClient, VmEntry};
use aleph_types::account::Account;
use aleph_types::chain::Address;
use aleph_types::item_hash::ItemHash;
use anyhow::{Context, Result, anyhow, bail};
use url::Url;

use crate::cli::InstanceSshArgs;
use crate::common::{resolve_account, resolve_address};

const SCHEDULER_BASE_URL: &str = "https://scheduler.api.aleph.cloud";

pub async fn handle_ssh(args: InstanceSshArgs) -> Result<()> {
    let (vm_id, crn_url) = match (
        args.crn_url.as_deref(),
        ItemHash::try_from(args.vm_id.as_str()),
    ) {
        (Some(url), Ok(hash)) => {
            // Full hash + explicit CRN: skip the scheduler entirely.
            (hash, Url::parse(url).context("invalid --crn-url")?)
        }
        (Some(url), Err(_)) => {
            // Prefix requires the scheduler to expand, but --crn-url still wins
            // over the scheduler's allocation.
            let (hash, _) = resolve_vm(&args.vm_id, args.address.as_deref()).await?;
            (hash, Url::parse(url).context("invalid --crn-url")?)
        }
        (None, _) => {
            let (hash, entry) = resolve_vm(&args.vm_id, args.address.as_deref()).await?;
            let url = crn_url_from_entry(&hash, &entry).await?;
            (hash, url)
        }
    };

    let http = reqwest::Client::new();
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

/// Resolve `input` to a VM, either by exact hash or by unique prefix match
/// against the address's scheduler-known instances. The returned `VmEntry`
/// lets the caller skip a second scheduler round-trip for CRN discovery.
async fn resolve_vm(input: &str, address_arg: Option<&str>) -> Result<(ItemHash, VmEntry)> {
    let scheduler = SchedulerClient::new(
        Url::parse(SCHEDULER_BASE_URL).expect("SCHEDULER_BASE_URL is a valid URL"),
    );

    if let Ok(hash) = ItemHash::try_from(input) {
        let entry = scheduler
            .get_vm(&hash)
            .await
            .context("querying scheduler")?
            .ok_or_else(|| anyhow!("instance {hash} not found in the scheduler"))?;
        return Ok((hash, entry));
    }

    let address = resolve_lookup_address(address_arg)?;
    let vms = scheduler
        .list_vms_by_owner(&address)
        .await
        .with_context(|| format!("listing instances of {address} for prefix resolution"))?;
    let entry = match_prefix(input, &address, &vms)?;
    let hash = entry.vm_hash.clone();
    Ok((hash, entry))
}

fn resolve_lookup_address(address_arg: Option<&str>) -> Result<Address> {
    match address_arg {
        Some(value) => resolve_address(value),
        None => {
            let identity = crate::cli::IdentityArgs {
                account: None,
                private_key: None,
                chain: None,
            };
            let account = resolve_account(&identity)?;
            Ok(account.address().clone())
        }
    }
}

/// Find the unique `VmEntry` whose `vm_hash` starts with `input`. Errors on
/// zero or multiple matches; the multi-match error lists candidates so the
/// user can pick a longer prefix.
fn match_prefix(input: &str, address: &Address, vms: &[VmEntry]) -> Result<VmEntry> {
    let matches: Vec<&VmEntry> = vms
        .iter()
        .filter(|v| v.vm_hash.to_string().starts_with(input))
        .collect();
    match matches.as_slice() {
        [] => bail!(
            "no instance matching `{input}` for address {address}. \
             Run `aleph instance list --address {address}` to see available hashes, \
             or pass a full hash."
        ),
        [entry] => Ok((*entry).clone()),
        many => {
            let mut hashes: Vec<String> = many.iter().map(|v| v.vm_hash.to_string()).collect();
            hashes.sort();
            bail!(
                "prefix `{input}` is ambiguous, matches {} instances:\n  {}",
                many.len(),
                hashes.join("\n  ")
            )
        }
    }
}

/// Translate a `VmEntry` to the URL of the CRN it's allocated to. Refuses any
/// status other than `dispatched` / `duplicated`.
async fn crn_url_from_entry(vm_id: &ItemHash, entry: &VmEntry) -> Result<Url> {
    // `duplicated` means the VM is allocated on multiple CRNs because of a
    // re-scheduling race; the `allocated_node` still points to the canonical
    // placement, so we follow it.
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

    fn vm_entry(hash_hex: &str) -> VmEntry {
        VmEntry {
            vm_hash: hash_hex.parse().unwrap(),
            vm_type: "instance".to_string(),
            allocated_node: None,
            status: "dispatched".to_string(),
            scheduling_status: "scheduled".to_string(),
            migration_target: None,
            owner: None,
            extra: serde_json::Map::new(),
        }
    }

    fn fake_address() -> Address {
        Address::from("0x0000000000000000000000000000000000000001".to_string())
    }

    #[test]
    fn prefix_matches_single() {
        let vms = vec![
            vm_entry("4e7df823423f0000000000000000000000000000000000000000000000000001"),
            vm_entry("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        ];
        let entry = match_prefix("4e7df823423f", &fake_address(), &vms).unwrap();
        assert!(entry.vm_hash.to_string().starts_with("4e7df823423f"));
    }

    #[test]
    fn prefix_no_match_errors() {
        let vms = vec![vm_entry(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )];
        let err = match_prefix("dead", &fake_address(), &vms).unwrap_err();
        assert!(err.to_string().contains("no instance matching `dead`"));
    }

    #[test]
    fn prefix_ambiguous_lists_candidates() {
        let vms = vec![
            vm_entry("4e7df823423f0000000000000000000000000000000000000000000000000001"),
            vm_entry("4e7df823423f0000000000000000000000000000000000000000000000000002"),
        ];
        let err = match_prefix("4e7df", &fake_address(), &vms).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ambiguous"));
        assert!(msg.contains("matches 2 instances"));
        assert!(msg.contains("0000000000000001"));
        assert!(msg.contains("0000000000000002"));
    }
}
