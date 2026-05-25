//! `aleph instance port-forwarder` subcommands.
//!
//! Backed by the per-sender `port-forwarding` aggregate plus two CRN
//! endpoints: `GET /v2/about/executions/list` (for the host-side mapped port)
//! and `POST /control/{vm_id}/update` (for `refresh`).

use crate::cli::{
    PortForwarderCommand, PortForwarderCreateArgs, PortForwarderDeleteArgs, PortForwarderListArgs,
    PortForwarderRefreshArgs, PortForwarderUpdateArgs,
};
use crate::common::{
    resolve_account, resolve_address, resolve_address_or_active, submit_or_preview,
};
use aleph_sdk::aggregate_models::port_forwarding::{
    PORT_FORWARDING_AGGREGATE_KEY, PortFlags, PortForwardingAggregate, Ports,
};
use aleph_sdk::client::{AlephAggregateClient, AlephClient};
use aleph_sdk::crn::CrnClient;
use aleph_sdk::messages::AggregateBuilder;
use aleph_sdk::scheduler::SchedulerClient;
use aleph_types::account::Account;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use anyhow::{Result, bail};
use reqwest;
use std::collections::HashMap;
use std::fmt::Write as _;
use url::Url;

pub async fn handle_port_forwarder_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    scheduler_url: &Url,
    json: bool,
    command: PortForwarderCommand,
) -> Result<()> {
    match command {
        PortForwarderCommand::List(args) => {
            handle_list(aleph_client, scheduler_url, json, args).await
        }
        PortForwarderCommand::Create(args) => {
            handle_create(aleph_client, ccn_url, scheduler_url, json, args).await
        }
        PortForwarderCommand::Update(args) => {
            handle_update(aleph_client, ccn_url, scheduler_url, json, args).await
        }
        PortForwarderCommand::Delete(args) => {
            handle_delete(aleph_client, ccn_url, scheduler_url, json, args).await
        }
        PortForwarderCommand::Refresh(args) => handle_refresh(scheduler_url, json, args).await,
    }
}

async fn handle_list(
    aleph_client: &AlephClient,
    scheduler_url: &Url,
    json: bool,
    args: PortForwarderListArgs,
) -> Result<()> {
    let address = resolve_address_or_active(args.address.as_deref())?;
    let aggregate = aleph_client.get_port_forwarding_aggregate(&address).await?;

    let resolved_filter: Option<ItemHash> = match args.vm_id.as_deref() {
        Some(input) => Some(
            super::instance_target::resolve_vm(scheduler_url, input)
                .await?
                .0,
        ),
        None => None,
    };
    let vm_filter = resolved_filter.as_ref();

    // Count non-null entries that pass the vm_id filter.
    let matching_count = aggregate
        .iter()
        .filter(|(k, v)| v.is_some() && vm_filter.is_none_or(|f| *k == f))
        .count();

    if matching_count == 0 {
        if json {
            println!("[]");
        } else if let Some(vm_id) = vm_filter {
            eprintln!("No port forwards found for VM {vm_id}");
        } else {
            eprintln!("No port forwards found for {address}");
        }
        return Ok(());
    }

    let externals = resolve_external_ports(scheduler_url, &aggregate).await;

    if json {
        println!("{}", render_list_json(&aggregate, &externals, vm_filter));
    } else {
        print!("{}", render_list_text(&aggregate, &externals, vm_filter));
    }

    Ok(())
}

/// Resolve the host-side mapped ports for each VM in the aggregate.
///
/// For each non-null entry, contacts the scheduler to get the CRN URL, then
/// fetches `/v2/about/executions/list` from the CRN to find the mapped ports.
/// Any failure (scheduler unreachable, CRN unreachable, allocated_node is a
/// hash rather than a URL) degrades silently: that VM simply won't have
/// external port data.
async fn resolve_external_ports(
    scheduler_url: &Url,
    aggregate: &PortForwardingAggregate,
) -> HashMap<ItemHash, HashMap<u16, u16>> {
    let scheduler = SchedulerClient::new(scheduler_url.clone());

    let mut result: HashMap<ItemHash, HashMap<u16, u16>> = HashMap::new();

    for (vm_id, maybe_ports) in aggregate {
        if maybe_ports.is_none() {
            continue;
        }

        // Get the VM entry from the scheduler.
        let vm_entry = match scheduler.get_vm(vm_id).await {
            Ok(Some(entry)) => entry,
            _ => continue,
        };

        // Only use allocated_node if it looks like a URL.
        let crn_url = match vm_entry.allocated_node {
            Some(ref s) if s.starts_with("http://") || s.starts_with("https://") => s.clone(),
            _ => continue,
        };

        // Fetch the executions list from the CRN.
        let executions_url = format!("{}/v2/about/executions/list", crn_url.trim_end_matches('/'));
        let response = match reqwest::get(&executions_url).await {
            Ok(r) if r.status().is_success() => r,
            _ => continue,
        };

        let body: serde_json::Value = match response.json().await {
            Ok(v) => v,
            Err(_) => continue,
        };

        // The response is an object keyed by vm_id string. Find the entry for
        // this VM and walk its networking.mapped_ports.
        let vm_key = vm_id.to_string();
        let entry = match body.get(&vm_key) {
            Some(e) => e,
            None => continue,
        };

        let mapped_ports = match entry
            .get("networking")
            .and_then(|n| n.get("mapped_ports"))
            .and_then(|m| m.as_object())
        {
            Some(obj) => obj,
            None => continue,
        };

        let mut port_map: HashMap<u16, u16> = HashMap::new();
        for (port_str, port_info) in mapped_ports {
            let requested: u16 = match port_str.parse() {
                Ok(p) => p,
                Err(_) => continue,
            };
            let host: u16 = match port_info
                .get("host")
                .and_then(|h| h.as_u64())
                .and_then(|h| u16::try_from(h).ok())
            {
                Some(p) => p,
                None => continue,
            };
            port_map.insert(requested, host);
        }

        if !port_map.is_empty() {
            result.insert(vm_id.clone(), port_map);
        }
    }

    result
}

/// Render the port-forwarding list as a JSON array.
///
/// Each element has keys: `item_hash`, `port`, `external_port`, `tcp`, `udp`.
/// `external_port` is JSON null when not resolvable. Entries with `None` value
/// (soft-deleted) are omitted. Results are sorted by item hash string for
/// deterministic output.
pub(crate) fn render_list_json(
    aggregate: &PortForwardingAggregate,
    externals: &HashMap<ItemHash, HashMap<u16, u16>>,
    vm_filter: Option<&ItemHash>,
) -> String {
    let mut entries: Vec<(&ItemHash, &Ports)> = aggregate
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|p| (k, p)))
        .filter(|(k, _)| vm_filter.is_none_or(|f| *k == f))
        .collect();
    entries.sort_by_cached_key(|(k, _)| k.to_string());

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for (vm_id, ports_entry) in entries {
        let port_externals = externals.get(vm_id);
        for (port, flags) in &ports_entry.ports {
            let external_port: serde_json::Value = port_externals
                .and_then(|m| m.get(port))
                .map(|&hp| serde_json::Value::Number(hp.into()))
                .unwrap_or(serde_json::Value::Null);
            rows.push(serde_json::json!({
                "item_hash": vm_id.to_string(),
                "port": port,
                "external_port": external_port,
                "tcp": flags.tcp,
                "udp": flags.udp,
            }));
        }
    }

    serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".to_string())
}

/// Render the port-forwarding list as a text table.
///
/// Returns an empty string when there are no matching entries. Entries with
/// `None` value (soft-deleted) are omitted. Results are sorted by item hash
/// string for deterministic output.
pub(crate) fn render_list_text(
    aggregate: &PortForwardingAggregate,
    externals: &HashMap<ItemHash, HashMap<u16, u16>>,
    vm_filter: Option<&ItemHash>,
) -> String {
    let mut entries: Vec<(&ItemHash, &Ports)> = aggregate
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|p| (k, p)))
        .filter(|(k, _)| vm_filter.is_none_or(|f| *k == f))
        .collect();
    entries.sort_by_cached_key(|(k, _)| k.to_string());

    if entries.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    writeln!(
        out,
        "{:<64}  {:>5}  {:>13}  {:>5}  {:>5}",
        "ITEM_HASH", "PORT", "EXTERNAL_PORT", "TCP", "UDP"
    )
    .unwrap();

    for (vm_id, ports_entry) in entries {
        let port_externals = externals.get(vm_id);
        for (port, flags) in &ports_entry.ports {
            let external_str = port_externals
                .and_then(|m| m.get(port))
                .map(|hp| hp.to_string())
                .unwrap_or_else(|| "N/A".to_string());
            writeln!(
                out,
                "{:<64}  {:>5}  {:>13}  {:>5}  {:>5}",
                vm_id.to_string(),
                port,
                external_str,
                flags.tcp,
                flags.udp,
            )
            .unwrap();
        }
    }

    out
}

/// Build the JSON content map for a `create` or `update` AGGREGATE message.
/// Reads the merged value of `<vm_id>` from `existing`, applies the new
/// `(port, flags)`, returns `{vm_id_string -> {"ports": {...}}}` ready for
/// `AggregateBuilder::new(...)`.
pub(crate) fn build_create_or_update_content(
    existing: &PortForwardingAggregate,
    vm_id: &ItemHash,
    port: u16,
    flags: PortFlags,
) -> serde_json::Map<String, serde_json::Value> {
    let mut merged_ports = existing
        .get(vm_id)
        .and_then(|v| v.as_ref())
        .map(|p| p.ports.clone())
        .unwrap_or_default();
    merged_ports.insert(port, flags);
    let value = serde_json::to_value(Ports {
        ports: merged_ports,
    })
    .expect("Ports serialization is infallible");
    let mut content = serde_json::Map::new();
    content.insert(vm_id.to_string(), value);
    content
}

/// Return `Err` when the entry for `vm_id` does not have `port` configured.
/// Used by `update` and the port-scoped `delete`.
pub(crate) fn ensure_port_exists(
    existing: &PortForwardingAggregate,
    vm_id: &ItemHash,
    port: u16,
) -> Result<()> {
    let entry = existing
        .get(vm_id)
        .and_then(|v| v.as_ref())
        .ok_or_else(|| anyhow::anyhow!("VM {} has no port-forwarding entry", vm_id))?;
    if !entry.ports.contains_key(&port) {
        bail!(
            "port {} is not configured for {}; use `aleph instance port-forwarder create` instead",
            port,
            vm_id
        );
    }
    Ok(())
}

/// Reject calls that disable both TCP and UDP at the protocol level. We can't
/// express this as a clap-time constraint because `bool` fields aren't
/// mutually-aware, so it lives here.
pub(crate) fn require_at_least_one_protocol(tcp: bool, udp: bool) -> Result<()> {
    if !tcp && !udp {
        bail!("at least one of --tcp or --udp must be enabled");
    }
    Ok(())
}

async fn handle_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    scheduler_url: &Url,
    json: bool,
    args: PortForwarderCreateArgs,
) -> Result<()> {
    require_at_least_one_protocol(args.tcp, args.udp)?;

    let (vm_id, _) = super::instance_target::resolve_vm(scheduler_url, &args.vm_id).await?;

    let account = resolve_account(&args.signing.identity)?;
    let owner_address = match args.on_behalf_of.as_deref() {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    let existing = aleph_client
        .get_port_forwarding_aggregate(&owner_address)
        .await?;

    let flags = PortFlags {
        tcp: args.tcp,
        udp: args.udp,
    };
    let content = build_create_or_update_content(&existing, &vm_id, args.port, flags);

    let mut builder = AggregateBuilder::new(&account, PORT_FORWARDING_AGGREGATE_KEY, content);
    if let Some(channel) = args.channel {
        builder = builder.channel(Channel::from(channel));
    }
    if args.on_behalf_of.is_some() {
        builder = builder.on_behalf_of(owner_address);
    }
    let pending = builder.build()?;

    submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await?;

    if !json && !args.signing.dry_run {
        eprintln!(
            "Port forward created for {} on port {} (tcp={}, udp={})",
            vm_id, args.port, args.tcp, args.udp
        );
    }
    Ok(())
}

async fn handle_update(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    scheduler_url: &Url,
    json: bool,
    args: PortForwarderUpdateArgs,
) -> Result<()> {
    require_at_least_one_protocol(args.tcp, args.udp)?;

    let (vm_id, _) = super::instance_target::resolve_vm(scheduler_url, &args.vm_id).await?;

    let account = resolve_account(&args.signing.identity)?;
    let owner_address = match args.on_behalf_of.as_deref() {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    let existing = aleph_client
        .get_port_forwarding_aggregate(&owner_address)
        .await?;
    ensure_port_exists(&existing, &vm_id, args.port)?;

    let flags = PortFlags {
        tcp: args.tcp,
        udp: args.udp,
    };
    let content = build_create_or_update_content(&existing, &vm_id, args.port, flags);

    let mut builder = AggregateBuilder::new(&account, PORT_FORWARDING_AGGREGATE_KEY, content);
    if let Some(channel) = args.channel {
        builder = builder.channel(Channel::from(channel));
    }
    if args.on_behalf_of.is_some() {
        builder = builder.on_behalf_of(owner_address);
    }
    let pending = builder.build()?;

    submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await?;

    if !json && !args.signing.dry_run {
        eprintln!(
            "Port forward updated for {} on port {} (tcp={}, udp={})",
            vm_id, args.port, args.tcp, args.udp
        );
    }
    Ok(())
}

/// Build the AGGREGATE content map for `delete --port P`. Returns `{vm_id: null}`
/// when `P` was the last port, otherwise `{vm_id: {ports: <remaining>}}`.
pub(crate) fn build_delete_port_content(
    existing: &PortForwardingAggregate,
    vm_id: &ItemHash,
    port: u16,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let entry = existing
        .get(vm_id)
        .and_then(|v| v.as_ref())
        .ok_or_else(|| anyhow::anyhow!("VM {} has no port-forwarding entry", vm_id))?;
    if !entry.ports.contains_key(&port) {
        bail!("port {} is not configured for {}", port, vm_id);
    }
    let mut remaining = entry.ports.clone();
    remaining.remove(&port);

    let mut content = serde_json::Map::new();
    let value = if remaining.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::to_value(Ports { ports: remaining }).expect("Ports serialization is infallible")
    };
    content.insert(vm_id.to_string(), value);
    Ok(content)
}

/// Build the AGGREGATE content map for `delete` without `--port`. Returns
/// `{vm_id: null}` when an entry exists; errors when there's nothing to delete.
pub(crate) fn build_delete_all_content(
    existing: &PortForwardingAggregate,
    vm_id: &ItemHash,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    if existing.get(vm_id).and_then(|v| v.as_ref()).is_none() {
        bail!("VM {} has no port-forwarding entry", vm_id);
    }
    let mut content = serde_json::Map::new();
    content.insert(vm_id.to_string(), serde_json::Value::Null);
    Ok(content)
}

async fn handle_delete(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    scheduler_url: &Url,
    json: bool,
    args: PortForwarderDeleteArgs,
) -> Result<()> {
    let (vm_id, _) = super::instance_target::resolve_vm(scheduler_url, &args.vm_id).await?;

    let account = resolve_account(&args.signing.identity)?;
    let owner_address = match args.on_behalf_of.as_deref() {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    let existing = aleph_client
        .get_port_forwarding_aggregate(&owner_address)
        .await?;

    let content = match args.port {
        Some(port) => build_delete_port_content(&existing, &vm_id, port)?,
        None => {
            if !args.yes
                && !crate::common::confirm_tty(&format!("Delete all port forwards for {vm_id}?"))?
            {
                bail!("aborted");
            }
            build_delete_all_content(&existing, &vm_id)?
        }
    };

    let mut builder = AggregateBuilder::new(&account, PORT_FORWARDING_AGGREGATE_KEY, content);
    if let Some(channel) = args.channel {
        builder = builder.channel(Channel::from(channel));
    }
    if args.on_behalf_of.is_some() {
        builder = builder.on_behalf_of(owner_address);
    }
    let pending = builder.build()?;

    submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await?;

    if !json && !args.signing.dry_run {
        match args.port {
            Some(p) => eprintln!("Port forward {p} deleted for {vm_id}"),
            None => eprintln!("All port forwards deleted for {vm_id}"),
        }
    }
    Ok(())
}

async fn handle_refresh(
    scheduler_url: &Url,
    json: bool,
    args: PortForwarderRefreshArgs,
) -> Result<()> {
    let account = resolve_account(&args.identity)?;

    let (vm_id, vm) = super::instance_target::resolve_vm(scheduler_url, &args.vm_id).await?;
    let crn_url_raw = vm
        .allocated_node
        .ok_or_else(|| anyhow::anyhow!("no allocation found for {vm_id}"))?;
    if !(crn_url_raw.starts_with("http://") || crn_url_raw.starts_with("https://")) {
        bail!(
            "scheduler returned a non-URL allocation ({crn_url_raw}); resolving CRN hash to URL is a follow-up"
        );
    }
    let crn_url = Url::parse(&crn_url_raw)?;

    let client = CrnClient::new(&account, crn_url.clone())?;
    client.update_instance_config(&vm_id).await?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "vm_id": vm_id.to_string(),
                "crn_url": crn_url.to_string(),
                "status": "refreshed"
            }))?
        );
    } else {
        eprintln!("CRN {crn_url} refreshed for VM {vm_id}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use aleph_sdk::aggregate_models::port_forwarding::{PortFlags, PortForwardingAggregate, Ports};
    use aleph_types::item_hash::ItemHash;
    use std::collections::{BTreeMap, HashMap};
    use std::str::FromStr;

    fn sample_item_hash() -> ItemHash {
        ItemHash::from_str("1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap()
    }

    fn sample_aggregate_one_entry() -> PortForwardingAggregate {
        let mut ports = BTreeMap::new();
        ports.insert(
            80,
            PortFlags {
                tcp: true,
                udp: false,
            },
        );
        ports.insert(
            443,
            PortFlags {
                tcp: true,
                udp: false,
            },
        );
        let mut agg = PortForwardingAggregate::new();
        agg.insert(sample_item_hash(), Some(Ports { ports }));
        agg
    }

    #[test]
    fn render_list_json_omits_null_entries() {
        let mut agg = sample_aggregate_one_entry();
        let deleted =
            ItemHash::from_str("2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap();
        agg.insert(deleted, None);

        let rendered = super::render_list_json(&agg, &HashMap::new(), None);
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let arr = parsed.as_array().unwrap();
        // 2 rows for the one VM with two ports; the null entry is omitted.
        assert_eq!(arr.len(), 2);
        for row in arr {
            assert_eq!(
                row["item_hash"],
                "1111111111111111111111111111111111111111111111111111111111111111"
            );
        }
    }

    #[test]
    fn render_list_json_respects_vm_id_filter() {
        let agg = sample_aggregate_one_entry();
        let other =
            ItemHash::from_str("3333333333333333333333333333333333333333333333333333333333333333")
                .unwrap();
        let rendered = super::render_list_json(&agg, &HashMap::new(), Some(&other));
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 0);
    }

    #[test]
    fn render_list_json_deterministic_order_by_item_hash() {
        // Two VMs with hashes that would land in arbitrary HashMap order.
        // Sorting by item_hash.to_string() lexicographically should put
        // "1111..." before "2222...".
        let vm_a =
            ItemHash::from_str("2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap();
        let vm_b =
            ItemHash::from_str("1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        let mut agg = PortForwardingAggregate::new();
        agg.insert(
            vm_a,
            Some(Ports {
                ports: BTreeMap::from([(
                    22,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );
        agg.insert(
            vm_b,
            Some(Ports {
                ports: BTreeMap::from([(
                    22,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );
        let rendered = super::render_list_json(&agg, &HashMap::new(), None);
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr[0]["item_hash"].as_str().unwrap().starts_with("1111"));
        assert!(arr[1]["item_hash"].as_str().unwrap().starts_with("2222"));
    }

    #[test]
    fn render_list_json_emits_external_port_when_resolved() {
        let vm = sample_item_hash();
        let mut agg = PortForwardingAggregate::new();
        agg.insert(
            vm.clone(),
            Some(Ports {
                ports: BTreeMap::from([(
                    80,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );
        let mut externals = HashMap::new();
        externals.insert(vm, HashMap::from([(80u16, 24001u16)]));

        let rendered = super::render_list_json(&agg, &externals, None);
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed[0]["external_port"], 24001);
    }

    #[test]
    fn render_list_text_emits_na_for_unresolved_external_port() {
        let agg = sample_aggregate_one_entry();
        let rendered = super::render_list_text(&agg, &HashMap::new(), None);
        assert!(rendered.contains("N/A"), "expected N/A; got:\n{}", rendered);
        assert!(rendered.contains("ITEM_HASH"));
    }

    #[test]
    fn build_create_content_merges_into_existing() {
        let vm = sample_item_hash();
        let mut existing = PortForwardingAggregate::new();
        existing.insert(
            vm.clone(),
            Some(Ports {
                ports: BTreeMap::from([(
                    80,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );

        let content = super::build_create_or_update_content(
            &existing,
            &vm,
            443,
            PortFlags {
                tcp: true,
                udp: false,
            },
        );

        let entry = content.get(&vm.to_string()).unwrap();
        let ports = entry.get("ports").unwrap().as_object().unwrap();
        assert!(ports.contains_key("80"));
        assert!(ports.contains_key("443"));
    }

    #[test]
    fn build_create_content_creates_first_entry() {
        let vm = sample_item_hash();
        let existing = PortForwardingAggregate::new();
        let content = super::build_create_or_update_content(
            &existing,
            &vm,
            22,
            PortFlags {
                tcp: true,
                udp: false,
            },
        );

        let entry = content.get(&vm.to_string()).unwrap();
        let ports = entry.get("ports").unwrap().as_object().unwrap();
        assert_eq!(ports.len(), 1);
        assert!(ports.contains_key("22"));
    }

    #[test]
    fn build_create_content_overwrites_same_port() {
        let vm = sample_item_hash();
        let mut existing = PortForwardingAggregate::new();
        existing.insert(
            vm.clone(),
            Some(Ports {
                ports: BTreeMap::from([(
                    80,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );

        let content = super::build_create_or_update_content(
            &existing,
            &vm,
            80,
            PortFlags {
                tcp: false,
                udp: true,
            },
        );

        let entry = content.get(&vm.to_string()).unwrap();
        let port80 = entry.pointer("/ports/80").unwrap();
        assert_eq!(port80["tcp"], false);
        assert_eq!(port80["udp"], true);
    }

    #[test]
    fn update_errors_when_port_not_present() {
        let vm = sample_item_hash();
        let mut existing = PortForwardingAggregate::new();
        existing.insert(
            vm.clone(),
            Some(Ports {
                ports: BTreeMap::from([(
                    80,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );
        let result = super::ensure_port_exists(&existing, &vm, 443);
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("port 443") && msg.contains(&vm.to_string()),
            "got: {msg}"
        );
    }

    #[test]
    fn update_ok_when_port_present() {
        let vm = sample_item_hash();
        let mut existing = PortForwardingAggregate::new();
        existing.insert(
            vm.clone(),
            Some(Ports {
                ports: BTreeMap::from([(
                    80,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );
        super::ensure_port_exists(&existing, &vm, 80).expect("present");
    }

    #[test]
    fn delete_one_port_keeps_others() {
        let vm = sample_item_hash();
        let mut existing = PortForwardingAggregate::new();
        existing.insert(
            vm.clone(),
            Some(Ports {
                ports: BTreeMap::from([
                    (
                        80,
                        PortFlags {
                            tcp: true,
                            udp: false,
                        },
                    ),
                    (
                        443,
                        PortFlags {
                            tcp: true,
                            udp: false,
                        },
                    ),
                ]),
            }),
        );
        let content = super::build_delete_port_content(&existing, &vm, 80).expect("ok");
        let entry = content.get(&vm.to_string()).unwrap();
        assert!(entry.is_object());
        let ports = entry.get("ports").unwrap().as_object().unwrap();
        assert_eq!(ports.len(), 1);
        assert!(ports.contains_key("443"));
    }

    #[test]
    fn delete_last_port_nulls_entry() {
        let vm = sample_item_hash();
        let mut existing = PortForwardingAggregate::new();
        existing.insert(
            vm.clone(),
            Some(Ports {
                ports: BTreeMap::from([(
                    80,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );
        let content = super::build_delete_port_content(&existing, &vm, 80).expect("ok");
        let entry = content.get(&vm.to_string()).unwrap();
        assert!(entry.is_null());
    }

    #[test]
    fn delete_unknown_port_errors() {
        let vm = sample_item_hash();
        let mut existing = PortForwardingAggregate::new();
        existing.insert(
            vm.clone(),
            Some(Ports {
                ports: BTreeMap::from([(
                    80,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );
        let err = super::build_delete_port_content(&existing, &vm, 443).unwrap_err();
        assert!(format!("{err}").contains("port 443"));
    }

    #[test]
    fn delete_entire_entry_returns_null() {
        let vm = sample_item_hash();
        let mut existing = PortForwardingAggregate::new();
        existing.insert(
            vm.clone(),
            Some(Ports {
                ports: BTreeMap::from([(
                    80,
                    PortFlags {
                        tcp: true,
                        udp: false,
                    },
                )]),
            }),
        );
        let content = super::build_delete_all_content(&existing, &vm).expect("ok");
        let entry = content.get(&vm.to_string()).unwrap();
        assert!(entry.is_null());
    }

    #[test]
    fn delete_entire_entry_errors_when_missing() {
        let vm = sample_item_hash();
        let existing = PortForwardingAggregate::new();
        let err = super::build_delete_all_content(&existing, &vm).unwrap_err();
        assert!(format!("{err}").contains("no port-forwarding entry"));
    }

    #[test]
    fn require_at_least_one_protocol_rejects_both_off() {
        let err = super::require_at_least_one_protocol(false, false).unwrap_err();
        assert!(format!("{err}").contains("--tcp or --udp"));
    }

    #[test]
    fn require_at_least_one_protocol_accepts_tcp_only() {
        super::require_at_least_one_protocol(true, false).expect("tcp-only ok");
    }

    #[test]
    fn require_at_least_one_protocol_accepts_udp_only() {
        super::require_at_least_one_protocol(false, true).expect("udp-only ok");
    }

    #[test]
    fn require_at_least_one_protocol_accepts_both() {
        super::require_at_least_one_protocol(true, true).expect("both ok");
    }
}
