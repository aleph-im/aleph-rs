use crate::cli::{
    ImageRef, InstanceCommand, InstanceCreateArgs, InstanceDeleteArgs, InstanceListArgs,
    InstancePriceArgs, parse_size_to_mib,
};
use crate::common::{
    confirm_action, resolve_account, resolve_address, resolve_address_or_active, submit_or_preview,
};
use aleph_sdk::aggregate_models::vm_images::{VmImagesData, VmImagesError};
use aleph_sdk::client::{
    AlephAggregateClient, AlephClient, AlephMessageClient, MessageFilter, MessageWithStatus,
};
use aleph_sdk::messages::{ForgetBuilder, InstanceBuilder};
use aleph_sdk::scheduler::{SchedulerClient, VmEntry};
use aleph_sdk::ssh::{AlephSshClient, SshKey};
use aleph_types::account::Account;
use aleph_types::chain::Address;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::base::{Payment, PaymentType};
use aleph_types::message::execution::environment::{
    GpuDeviceClass, GpuProperties, HostRequirements, Hypervisor, NodeRequirements,
    TrustedExecutionEnvironment,
};
use aleph_types::message::execution::volume::{
    BaseVolume, EphemeralVolume, ImmutableVolume, MachineVolume, PersistentVolume,
    PersistentVolumeSize, VolumePersistence,
};
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::{Message, MessageContentEnum, MessageType};
use aleph_types::timestamp::Timestamp;
use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use memsizes::MiB;
use url::Url;

/// Source filter that surfaced this row from the CCN.
///
/// CCN queries are run separately for `addresses=` (sender) and `owners=`
/// (resource owner). A row may be in one set, the other, or both. Used to
/// decide whether the second bulk scheduler call (by sender) is needed: only
/// rows that came from the sender filter and were not already enriched by the
/// owner-filtered call require it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SourceFlags {
    pub owner: bool,
    pub sender: bool,
}

impl SourceFlags {
    pub fn merge(&mut self, other: SourceFlags) {
        self.owner |= other.owner;
        self.sender |= other.sender;
    }
}

/// One row of `aleph instance list` output, extracted from an INSTANCE message.
///
/// Fields populated post-merge with data from the scheduler stay `None` when
/// the scheduler is unreachable or has no record of the VM.
#[derive(Debug, Clone)]
pub(crate) struct InstanceRow {
    pub item_hash: ItemHash,
    pub name: Option<String>,
    pub owner: Address,
    pub node_hash: Option<String>,
    pub created_at: Timestamp,
    /// Effective scheduler status, e.g. `dispatched`, `unschedulable`.
    pub status: Option<String>,
    /// Node hash where the VM is currently allocated, per the scheduler.
    pub allocated_node: Option<String>,
    /// VM's directly-routable IPv6 address, fetched best-effort from the CRN.
    pub ipv6: Option<String>,
    /// CRN host's public (shared, NAT) IPv4, fetched best-effort from the CRN.
    pub ipv4: Option<String>,
    /// Full scheduler entry, used for `--json` passthrough.
    pub scheduler_raw: Option<VmEntry>,
    pub source_flags: SourceFlags,
}

fn name_from_metadata(
    metadata: Option<&std::collections::HashMap<String, serde_json::Value>>,
) -> Option<String> {
    metadata
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn node_from_requirements(requirements: Option<&HostRequirements>) -> Option<String> {
    requirements
        .and_then(|r| r.node.as_ref())
        .and_then(|n| n.node_hash.clone())
}

/// First 12 chars of the item hash, lower-cased. Used in the text table.
pub(crate) fn format_item_hash_short(hash: &ItemHash) -> String {
    let s = hash.to_string();
    s.chars().take(12).collect()
}

/// Last 10 chars of a node hash. Returns `s` unchanged if shorter than 10.
fn format_node_short(node: &str) -> String {
    if node.len() <= 10 {
        return node.to_string();
    }
    node[node.len() - 10..].to_string()
}

/// Pure merge: copy scheduler fields onto rows whose `item_hash` is present
/// in the map. Rows without a match are left unchanged. Map entries with no
/// matching row are dropped (CCN remains authoritative).
pub(crate) fn merge_scheduler_into_rows(
    rows: &mut [InstanceRow],
    scheduler_by_hash: &std::collections::HashMap<ItemHash, VmEntry>,
) {
    for row in rows.iter_mut() {
        if let Some(entry) = scheduler_by_hash.get(&row.item_hash) {
            row.status = Some(entry.status.clone());
            row.allocated_node = entry.allocated_node.clone();
            row.scheduler_raw = Some(entry.clone());
        }
    }
}

/// Convert an INSTANCE message into a row. Returns `None` for non-instance
/// messages (defensive — callers already filter by `MessageType::Instance`,
/// but the CCN can occasionally return a mis-typed payload).
pub(crate) fn extract_instance_row(message: &Message) -> Option<InstanceRow> {
    let MessageContentEnum::Instance(instance) = message.content() else {
        return None;
    };
    Some(InstanceRow {
        item_hash: message.item_hash.clone(),
        name: name_from_metadata(instance.base.metadata.as_ref()),
        owner: message.owner().clone(),
        node_hash: node_from_requirements(instance.base.requirements.as_ref()),
        created_at: message.content.time.clone(),
        status: None,
        allocated_node: None,
        ipv6: None,
        ipv4: None,
        scheduler_raw: None,
        source_flags: SourceFlags::default(),
    })
}

/// Fetch all INSTANCE rows for `address`, deduped by item_hash.
/// Runs the sender filter and the owner filter in sequence and merges them
/// (the CCN ANDs `addresses` and `owners`; we want OR).
async fn fetch_instance_rows(
    aleph_client: &AlephClient,
    address: &Address,
) -> Result<Vec<InstanceRow>> {
    use std::collections::HashMap;

    let mut by_hash: HashMap<ItemHash, InstanceRow> = HashMap::new();

    let filters = [
        (
            MessageFilter {
                message_type: Some(MessageType::Instance),
                addresses: Some(vec![address.clone()]),
                ..Default::default()
            },
            SourceFlags {
                sender: true,
                owner: false,
            },
        ),
        (
            MessageFilter {
                message_type: Some(MessageType::Instance),
                owners: Some(vec![address.clone()]),
                ..Default::default()
            },
            SourceFlags {
                sender: false,
                owner: true,
            },
        ),
    ];

    for (filter, flags) in filters {
        let mut stream = Box::pin(aleph_client.get_messages_iterator(filter, None));
        while let Some(message) = stream.next().await {
            let message = message?;
            if let Some(mut row) = extract_instance_row(&message) {
                row.source_flags = flags;
                by_hash
                    .entry(row.item_hash.clone())
                    .and_modify(|existing| existing.source_flags.merge(flags))
                    .or_insert(row);
            } else {
                eprintln!(
                    "warning: skipping message {} with non-instance content",
                    message.item_hash
                );
            }
        }
    }

    let mut rows: Vec<InstanceRow> = by_hash.into_values().collect();
    // Newest first: sort by content.time descending.
    rows.sort_by(|a, b| {
        b.created_at
            .as_f64()
            .partial_cmp(&a.created_at.as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(rows)
}

/// Bulk-fetch every VM the scheduler knows about for `address`, indexed by
/// `item_hash`. On HTTP / network error, prints a warning to stderr and
/// returns an empty map so the caller can degrade gracefully.
async fn fetch_scheduler_map(
    scheduler: &SchedulerClient,
    address: &Address,
) -> std::collections::HashMap<ItemHash, VmEntry> {
    use std::collections::HashMap;

    match scheduler.list_vms_by_owner(address).await {
        Ok(entries) => entries
            .into_iter()
            .map(|entry| (entry.vm_hash.clone(), entry))
            .collect(),
        Err(err) => {
            eprintln!("warning: scheduler unreachable, status/allocation unavailable: {err}");
            HashMap::new()
        }
    }
}

/// Does any row still need the by-sender scheduler call? True when a row came
/// only from the CCN sender filter (`sender && !owner`) and the owner-filtered
/// bulk call did not already enrich it. Pure, so the decision is unit-testable.
fn needs_sender_enrichment(
    rows: &[InstanceRow],
    scheduler_map: &std::collections::HashMap<ItemHash, VmEntry>,
) -> bool {
    rows.iter().any(|row| {
        row.source_flags.sender
            && !row.source_flags.owner
            && !scheduler_map.contains_key(&row.item_hash)
    })
}

/// Enrich rows the owner-filtered bulk call missed (the "sender but not owner"
/// case, i.e. VMs created via permission delegation where sender != owner)
/// with a single second bulk call filtered by sender. The scheduler's `sender`
/// filter shipped in v0.1.1, replacing the previous per-VM fallback loop.
///
/// Only fires when at least one row actually needs it. Entries already in the
/// map are left as-is (same data). Degrades gracefully on error: a stderr
/// warning is printed and rows simply stay unenriched.
async fn enrich_by_sender(
    scheduler: &SchedulerClient,
    address: &Address,
    rows: &[InstanceRow],
    scheduler_map: &mut std::collections::HashMap<ItemHash, VmEntry>,
) {
    if !needs_sender_enrichment(rows, scheduler_map) {
        return;
    }
    match scheduler.list_vms_by_sender(address).await {
        Ok(entries) => {
            for entry in entries {
                scheduler_map.entry(entry.vm_hash.clone()).or_insert(entry);
            }
        }
        Err(err) => {
            eprintln!("warning: scheduler unreachable, status/allocation unavailable: {err}");
        }
    }
}

/// Group the indices of rows that have an `allocated_node` by node hash.
///
/// Pure (no I/O) so it can be unit-tested: the IP-enrichment pass makes one
/// CRN call per unique node, not one per VM, and applies the result to every
/// row sharing that node. Rows without an `allocated_node` are skipped.
fn group_row_indices_by_node(
    rows: &[InstanceRow],
) -> std::collections::HashMap<String, Vec<usize>> {
    let mut by_node: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for (idx, row) in rows.iter().enumerate() {
        if let Some(node) = row.allocated_node.as_deref() {
            by_node.entry(node.to_string()).or_default().push(idx);
        }
    }
    by_node
}

/// Apply the networking selection rules used across the CLI: the VM's
/// directly-routable IPv6 (falling back to its `/124` network) and the CRN
/// host's shared public IPv4. Self-contained so this stays independent of
/// `instance_show::populate_verbose`.
fn select_ips(net: &aleph_sdk::crn::ActiveVmNetworking) -> (Option<String>, Option<String>) {
    let ipv6 = net.ipv6_ip.clone().or_else(|| net.ipv6_network.clone());
    let ipv4 = net.host_ipv4.clone();
    (ipv6, ipv4)
}

/// Best-effort IP enrichment: one CRN call per unique allocated node, run in
/// parallel. A failed node lookup or CRN fetch logs a warning and leaves the
/// affected rows' IPs as `None`; it never fails the list command.
async fn enrich_rows_with_ips(scheduler: &SchedulerClient, rows: &mut [InstanceRow]) {
    use aleph_sdk::crn::fetch_active_vms;

    let by_node = group_row_indices_by_node(rows);
    if by_node.is_empty() {
        return;
    }

    let http = reqwest::Client::new();

    // For each unique node, resolve its CRN URL and fetch the active VM list
    // once. Returns the indices on that node alongside the fetched map so the
    // results can be applied without holding a mutable borrow across awaits.
    let fetches = by_node.into_iter().map(|(node, indices)| {
        let http = &http;
        async move {
            let crn_url = match scheduler.get_node(&node).await {
                Ok(Some(entry)) => match entry.address.as_deref() {
                    Some(addr) => match Url::parse(addr) {
                        Ok(url) => url,
                        Err(e) => {
                            eprintln!("warning: invalid CRN address `{addr}` for node {node}: {e}");
                            return (indices, None);
                        }
                    },
                    None => {
                        eprintln!(
                            "warning: scheduler knows node {node} but has no reachable \
                             address; IP unavailable"
                        );
                        return (indices, None);
                    }
                },
                Ok(None) => return (indices, None),
                Err(e) => {
                    eprintln!("warning: scheduler unreachable for node {node}: {e}");
                    return (indices, None);
                }
            };
            match fetch_active_vms(http, &crn_url).await {
                Ok(list) => (indices, Some(list)),
                Err(e) => {
                    eprintln!("warning: CRN {crn_url} unreachable, IP unavailable: {e}");
                    (indices, None)
                }
            }
        }
    });

    let results = futures_util::future::join_all(fetches).await;

    for (indices, list) in results {
        let Some(list) = list else { continue };
        for idx in indices {
            let item_hash = rows[idx].item_hash.clone();
            if let Some(entry) = list.0.get(&item_hash)
                && let Some(net) = entry.networking.as_ref()
            {
                let (ipv6, ipv4) = select_ips(net);
                rows[idx].ipv6 = ipv6;
                rows[idx].ipv4 = ipv4;
            }
        }
    }
}

async fn handle_instance_list(
    aleph_client: &AlephClient,
    scheduler_url: Url,
    json: bool,
    args: InstanceListArgs,
) -> Result<()> {
    // Read-only: resolve the address from the manifest without loading the
    // account (loading an encrypted account would prompt for its password).
    let address = resolve_address_or_active(args.address.as_deref())?;

    let mut rows = fetch_instance_rows(aleph_client, &address).await?;

    let scheduler = SchedulerClient::new(scheduler_url);
    let mut scheduler_map = fetch_scheduler_map(&scheduler, &address).await;
    enrich_by_sender(&scheduler, &address, &rows, &mut scheduler_map).await;
    merge_scheduler_into_rows(&mut rows, &scheduler_map);
    enrich_rows_with_ips(&scheduler, &mut rows).await;

    render_rows(&rows, json)
}

const MISSING_VALUE: &str = "-";

fn format_rows_json(rows: &[InstanceRow]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "item_hash": r.item_hash.to_string(),
                "name": r.name,
                "owner": r.owner.to_string(),
                "node_hash": r.node_hash,
                "ipv6": r.ipv6,
                "ipv4": r.ipv4,
                "created_at": r.created_at
                    .to_datetime()
                    .ok()
                    .map(|dt| dt.to_rfc3339()),
                "scheduler": r.scheduler_raw,
            })
        })
        .collect();
    serde_json::Value::Array(items)
}

fn format_rows_text(rows: &[InstanceRow]) -> String {
    use std::fmt::Write;

    const HASH_HEADER: &str = "ITEM_HASH";
    const NAME_HEADER: &str = "NAME";
    const OWNER_HEADER: &str = "OWNER";
    const STATUS_HEADER: &str = "STATUS";
    const IPV6_HEADER: &str = "IPV6";
    const ALLOC_HEADER: &str = "ALLOCATED";

    // Hash column: 12-char prefix.
    let hash_w = HASH_HEADER.len().max(12);
    let name_w = rows
        .iter()
        .map(|r| r.name.as_deref().unwrap_or(MISSING_VALUE).len())
        .chain(std::iter::once(NAME_HEADER.len()))
        .max()
        .unwrap_or(NAME_HEADER.len());
    let owner_w = rows
        .iter()
        .map(|r| r.owner.to_string().len())
        .chain(std::iter::once(OWNER_HEADER.len()))
        .max()
        .unwrap_or(OWNER_HEADER.len());
    let status_w = rows
        .iter()
        .map(|r| r.status.as_deref().unwrap_or(MISSING_VALUE).len())
        .chain(std::iter::once(STATUS_HEADER.len()))
        .max()
        .unwrap_or(STATUS_HEADER.len());
    let ipv6_w = rows
        .iter()
        .map(|r| r.ipv6.as_deref().unwrap_or(MISSING_VALUE).len())
        .chain(std::iter::once(IPV6_HEADER.len()))
        .max()
        .unwrap_or(IPV6_HEADER.len());

    let mut out = String::new();
    writeln!(
        out,
        "{:<hash_w$}  {:<name_w$}  {:<owner_w$}  {:<status_w$}  {:<ipv6_w$}  {}",
        HASH_HEADER,
        NAME_HEADER,
        OWNER_HEADER,
        STATUS_HEADER,
        IPV6_HEADER,
        ALLOC_HEADER,
        hash_w = hash_w,
        name_w = name_w,
        owner_w = owner_w,
        status_w = status_w,
        ipv6_w = ipv6_w,
    )
    .expect("writing to String cannot fail");

    for row in rows {
        let name = row.name.as_deref().unwrap_or(MISSING_VALUE);
        let status = row.status.as_deref().unwrap_or(MISSING_VALUE);
        let ipv6 = row.ipv6.as_deref().unwrap_or(MISSING_VALUE);
        let allocated = row
            .allocated_node
            .as_deref()
            .map(format_node_short)
            .unwrap_or_else(|| MISSING_VALUE.to_string());
        writeln!(
            out,
            "{:<hash_w$}  {:<name_w$}  {:<owner_w$}  {:<status_w$}  {:<ipv6_w$}  {}",
            format_item_hash_short(&row.item_hash),
            name,
            row.owner,
            status,
            ipv6,
            allocated,
            hash_w = hash_w,
            name_w = name_w,
            owner_w = owner_w,
            status_w = status_w,
            ipv6_w = ipv6_w,
        )
        .expect("writing to String cannot fail");
    }
    out
}

fn render_rows(rows: &[InstanceRow], json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&format_rows_json(rows))?);
    } else {
        print!("{}", format_rows_text(rows));
    }
    Ok(())
}

pub async fn handle_instance_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    network_override: Option<&str>,
    json: bool,
    command: InstanceCommand,
) -> Result<()> {
    use super::crn;
    match command {
        InstanceCommand::Create(args) => {
            handle_instance_create(aleph_client, ccn_url, json, args).await?;
        }
        InstanceCommand::Delete(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            handle_instance_delete(aleph_client, ccn_url, &scheduler_url, json, args).await?;
        }
        InstanceCommand::Price(args) => {
            handle_instance_price(aleph_client, json, args).await?;
        }
        InstanceCommand::List(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            handle_instance_list(aleph_client, scheduler_url, json, args).await?;
        }
        InstanceCommand::Start(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            crn::handle_start(scheduler_url, json, args).await?
        }
        InstanceCommand::Stop(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            crn::handle_operation(scheduler_url, json, args, "stop").await?
        }
        InstanceCommand::Reboot(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            crn::handle_operation(scheduler_url, json, args, "reboot").await?
        }
        InstanceCommand::Reinstall(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            crn::handle_reinstall(scheduler_url, json, args).await?
        }
        InstanceCommand::Show(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            super::instance_show::handle_instance_show(aleph_client, scheduler_url, json, args)
                .await?;
        }
        InstanceCommand::Erase(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            crn::handle_operation(scheduler_url, json, args, "erase").await?
        }
        InstanceCommand::Logs(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            crn::handle_logs(scheduler_url, json, args).await?
        }
        InstanceCommand::Ssh(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            super::instance_ssh::handle_ssh(scheduler_url, args).await?;
        }
        InstanceCommand::PortForward { command } => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            crate::commands::port_forward::handle_port_forward_command(
                aleph_client,
                ccn_url,
                &scheduler_url,
                json,
                command,
            )
            .await?;
        }
        InstanceCommand::Backup(sub) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            super::instance_backup::dispatch(scheduler_url, json, sub).await?;
        }
        InstanceCommand::Confidential(sub) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            super::confidential::dispatch(scheduler_url, json, sub).await?;
        }
    }
    Ok(())
}

pub(crate) fn validate_ssh_pubkey(key: &str, path: &std::path::Path) -> Result<()> {
    aleph_sdk::ssh::validate_pubkey(key).map_err(|msg| anyhow!("'{}' {}", path.display(), msg))
}

/// Merge the owner's and signer's registries for `--ssh-key` label lookup.
///
/// The signer's keys take precedence on label collisions (we act from the
/// signer's point of view): a label present in both resolves to the signer's
/// key. Owner labels not shadowed by the signer remain available, as do the
/// signer's own labels. `sender` is empty when not acting on behalf of another
/// address, so the result is just the owner's keys.
pub(crate) fn merge_ssh_registries(owner: &[SshKey], sender: &[SshKey]) -> Vec<SshKey> {
    let sender_labels: std::collections::HashSet<&str> =
        sender.iter().filter_map(|k| k.label.as_deref()).collect();
    sender
        .iter()
        .cloned()
        .chain(
            owner
                .iter()
                .filter(|k| match k.label.as_deref() {
                    Some(label) => !sender_labels.contains(label),
                    None => true,
                })
                .cloned(),
        )
        .collect()
}

/// Resolve `--ssh-key` labels against the registered keys, returning their key
/// strings. Errors (listing available labels) if any label is unknown.
pub(crate) fn select_keys_by_label(
    labels: &[String],
    registered: &[SshKey],
) -> Result<Vec<String>> {
    labels
        .iter()
        .map(|label| {
            registered
                .iter()
                .find(|k| k.label.as_deref() == Some(label.as_str()))
                .map(|k| k.key.clone())
                .ok_or_else(|| {
                    let avail: Vec<String> =
                        registered.iter().filter_map(|k| k.label.clone()).collect();
                    anyhow!(
                        "no registered SSH key named '{label}'. Available: {}",
                        if avail.is_empty() {
                            "(none)".to_string()
                        } else {
                            avail.join(", ")
                        }
                    )
                })
        })
        .collect()
}

/// Combine explicit (file + label-selected) keys, or fall back to every
/// registered key when neither flag was given. Dedupes by key value, preserving
/// order. Errors when the final set is empty.
///
/// `explicit_given` is `true` iff at least one of `--ssh-pubkey-file` or
/// `--ssh-key` was provided; when `true`, `all_registered` is ignored.
pub(crate) fn resolve_instance_ssh_keys(
    file_keys: Vec<String>,
    selected: Vec<String>,
    explicit_given: bool,
    all_registered: Vec<String>,
) -> Result<Vec<String>> {
    let mut keys = if explicit_given {
        let mut v = file_keys;
        v.extend(selected);
        v
    } else {
        all_registered
    };

    let mut seen = std::collections::HashSet::new();
    keys.retain(|k| seen.insert(k.clone()));

    if keys.is_empty() {
        bail!(
            "no SSH keys to attach. Register one with `aleph account ssh-key add`, \
             or pass --ssh-pubkey-file"
        );
    }
    Ok(keys)
}

/// Parse a "key=value,key=value" string into a list of (key, value) pairs.
fn parse_kv_pairs(s: &str) -> Result<Vec<(&str, &str)>, String> {
    s.split(',')
        .map(|pair| {
            let (k, v) = pair
                .split_once('=')
                .ok_or_else(|| format!("invalid key=value pair: '{pair}'"))?;
            Ok((k.trim(), v.trim()))
        })
        .collect()
}

pub(crate) fn parse_persistent_volumes(specs: &[String]) -> Result<Vec<MachineVolume>> {
    specs
        .iter()
        .map(|spec| {
            let pairs = parse_kv_pairs(spec).map_err(anyhow::Error::msg)?;
            let mut name: Option<String> = None;
            let mut mount: Option<String> = None;
            let mut size_mib: Option<u64> = None;
            let mut persistence: Option<VolumePersistence> = None;
            let mut comment: Option<String> = None;
            for (k, v) in pairs {
                match k {
                    "name" => name = Some(v.to_string()),
                    "mount" => mount = Some(v.to_string()),
                    "size" => size_mib = Some(parse_size_to_mib(v).map_err(anyhow::Error::msg)?),
                    "persistence" => {
                        persistence = Some(match v {
                            "host" => VolumePersistence::Host,
                            "store" => VolumePersistence::Store,
                            _ => bail!("invalid persistence: '{v}'"),
                        })
                    }
                    "comment" => comment = Some(v.to_string()),
                    _ => bail!("unknown persistent volume key: '{k}'"),
                }
            }
            let size_mib = size_mib.context("persistent volume requires size")?;
            let mount = mount.context("persistent volume requires mount")?;
            Ok(MachineVolume::Persistent(PersistentVolume {
                base: BaseVolume {
                    comment,
                    mount: Some(mount.into()),
                },
                parent: None,
                persistence,
                name,
                size_mib: PersistentVolumeSize::try_from(size_mib)?,
            }))
        })
        .collect()
}

pub(crate) fn parse_ephemeral_volumes(specs: &[String]) -> Result<Vec<MachineVolume>> {
    specs
        .iter()
        .map(|spec| {
            let pairs = parse_kv_pairs(spec).map_err(anyhow::Error::msg)?;
            let mut mount: Option<String> = None;
            let mut size_mib: Option<u64> = None;
            for (k, v) in pairs {
                match k {
                    "mount" => mount = Some(v.to_string()),
                    "size" => size_mib = Some(parse_size_to_mib(v).map_err(anyhow::Error::msg)?),
                    _ => bail!("unknown ephemeral volume key: '{k}'"),
                }
            }
            let size_mib = size_mib.context("ephemeral volume requires size")?;
            let mount = mount.context("ephemeral volume requires mount")?;
            Ok(MachineVolume::Ephemeral(EphemeralVolume::new(
                size_mib, mount,
            )?))
        })
        .collect()
}

pub(crate) fn parse_immutable_volumes(specs: &[String]) -> Result<Vec<MachineVolume>> {
    specs
        .iter()
        .map(|spec| {
            let pairs = parse_kv_pairs(spec).map_err(anyhow::Error::msg)?;
            let mut reference: Option<String> = None;
            let mut mount: Option<String> = None;
            let mut use_latest = true;
            for (k, v) in pairs {
                match k {
                    "ref" => reference = Some(v.to_string()),
                    "mount" => mount = Some(v.to_string()),
                    "use_latest" => {
                        use_latest = v
                            .parse()
                            .map_err(|_| anyhow!("invalid use_latest: '{v}'"))?
                    }
                    _ => bail!("unknown immutable volume key: '{k}'"),
                }
            }
            let reference = reference.context("immutable volume requires ref")?;
            let mount = mount.context("immutable volume requires mount")?;
            let item_hash = reference.parse().map_err(|e| anyhow!("invalid ref: {e}"))?;
            Ok(MachineVolume::Immutable(ImmutableVolume {
                base: BaseVolume {
                    comment: None,
                    mount: Some(mount.into()),
                },
                reference: item_hash,
                use_latest,
            }))
        })
        .collect()
}

/// Resolve (vcpus, memory_mib, disk_mib) from flags when no `--size` slug is used.
/// Defaults: 1 vCPU, 2048 MiB memory; disk must be provided.
pub(crate) fn resolve_instance_specs_from_flags(
    vcpus: Option<u32>,
    memory_mib: Option<u64>,
    disk_mib: Option<u64>,
) -> Result<(u32, u64, u64)> {
    let disk_mib = disk_mib.context(
        "--disk-size is required when --size is not used \
         (or use --size to specify a tier slug like 1vcpu-2gb)",
    )?;
    Ok((vcpus.unwrap_or(1), memory_mib.unwrap_or(2048), disk_mib))
}

/// Resolve the number of compute units for a GPU instance.
///
/// The GPU model's compute-unit count (`min_cu`) is a lower bound. A GPU can be
/// sized at any whole multiple of its compute unit at or above that minimum, so
/// `--size` (e.g. `3vcpu-18gb`, `4vcpu-24gb`, `5vcpu-30gb`) or `--vcpus`/`--memory`
/// can raise it but never lower it. With none of those flags, the minimum is used.
///
/// `gpu_display_slug` is only used in error messages. `instance_pricing` must
/// be the GPU pricing entity (from `for_instance(false, Some(model))`).
pub(crate) fn resolve_gpu_compute_units(
    instance_pricing: &aleph_sdk::aggregate_models::pricing::PricingPerEntity,
    min_cu: u32,
    gpu_display_slug: &str,
    size: Option<&str>,
    vcpus: Option<u32>,
    memory: Option<u64>,
) -> Result<u32> {
    let cu_spec = &instance_pricing.compute_unit;
    let min_slug = instance_pricing.slug_for_compute_units(min_cu);

    if let Some(slug) = size {
        // A GPU is sized at any whole multiple of its compute unit at or above
        // the model minimum, so the slug is parsed arithmetically rather than
        // matched against an enumerated tier list.
        let cu = instance_pricing
            .compute_units_for_slug(slug)
            .ok_or_else(|| {
                anyhow!(
                    "invalid size '{slug}' for GPU '{gpu_display_slug}'. GPU sizes scale in steps \
                     of {}vcpu + {} GiB RAM (1 compute unit); use the minimum {min_slug} or a \
                     larger multiple such as {} or {}.",
                    cu_spec.vcpus,
                    cu_spec.memory_mib / 1024,
                    instance_pricing.slug_for_compute_units(min_cu + 1),
                    instance_pricing.slug_for_compute_units(min_cu + 2),
                )
            })?;
        if cu < min_cu {
            bail!(
                "size '{slug}' ({cu} CU) is below the minimum for GPU '{gpu_display_slug}' (min: {min_slug}, {min_cu} CU)",
            );
        }
        Ok(cu)
    } else if vcpus.is_some() || memory.is_some() {
        // Compute CU count from raw resources, validate against GPU minimum
        let cu_from_vcpus = vcpus.map(|v| v.div_ceil(cu_spec.vcpus)).unwrap_or(0);
        let cu_from_mem = memory
            .map(|m| m.div_ceil(cu_spec.memory_mib) as u32)
            .unwrap_or(0);
        let requested_cu = cu_from_vcpus.max(cu_from_mem);
        if requested_cu < min_cu {
            bail!(
                "requested resources are below the minimum for GPU '{gpu_display_slug}' (min: {min_slug}, {min_cu} CU)",
            );
        }
        Ok(requested_cu)
    } else {
        Ok(min_cu)
    }
}

/// Output of `resolve_image_refs`. `confidential_firmware` is `Some` iff the
/// caller requested a confidential VM.
#[derive(Debug)]
pub(crate) struct ResolvedImages {
    pub rootfs: ItemHash,
    pub confidential_firmware: Option<ItemHash>,
}

/// Resolve `--image` and `--confidential-firmware` against an in-memory
/// `VmImagesData`. Pure: does no network I/O. The handler decides separately
/// whether to fetch the aggregate.
pub(crate) fn resolve_image_refs(
    image: ImageRef,
    confidential: bool,
    confidential_firmware: Option<ImageRef>,
    data: &VmImagesData,
) -> anyhow::Result<ResolvedImages> {
    let rootfs = match image {
        ImageRef::Hash(h) => h,
        ImageRef::Preset(name) => data.rootfs(&name)?.hash.clone(),
    };

    let firmware = if confidential {
        let resolved = match confidential_firmware {
            Some(ImageRef::Hash(h)) => h,
            Some(ImageRef::Preset(name)) => data.firmware(&name)?.hash.clone(),
            None => {
                let default_name = data
                    .defaults
                    .firmware
                    .as_deref()
                    .ok_or(VmImagesError::NoDefault { kind: "firmware" })?;
                data.firmware(default_name)?.hash.clone()
            }
        };
        Some(resolved)
    } else {
        None
    };

    Ok(ResolvedImages {
        rootfs,
        confidential_firmware: firmware,
    })
}

/// Resolve `--runtime` (program create) against an in-memory `VmImagesData`.
/// Pure: does no network I/O. `None` triggers the `defaults.runtime` fallback.
pub(crate) fn resolve_runtime_ref(
    runtime: Option<ImageRef>,
    data: &VmImagesData,
) -> anyhow::Result<ItemHash> {
    let resolved = match runtime {
        Some(ImageRef::Hash(h)) => h,
        Some(ImageRef::Preset(name)) => data.runtime(&name)?.hash.clone(),
        None => {
            let default_name = data
                .defaults
                .runtime
                .as_deref()
                .ok_or(VmImagesError::NoDefault { kind: "runtime" })?;
            data.runtime(default_name)?.hash.clone()
        }
    };
    Ok(resolved)
}

async fn handle_instance_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    mut args: InstanceCreateArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    // SSH keys are looked up for the instance OWNER. When signing on behalf of
    // another address, that address owns the instance, so its registered keys
    // (not the signer's) are the ones to attach.
    let owner_address = match &args.on_behalf_of {
        Some(owner) => resolve_address(owner)?,
        None => account.address().clone(),
    };

    if args.interactive {
        crate::commands::instance_interactive::resolve_interactive(
            &mut args,
            aleph_client,
            &owner_address,
        )
        .await?;
    }

    // Resolve SSH keys: ad-hoc files + label-selected registered keys, falling
    // back to all registered keys when neither flag is given.
    let mut file_keys = Vec::new();
    for path in &args.ssh_pubkey_file {
        let content = std::fs::read_to_string(path).map_err(|e| {
            anyhow!(
                "failed to read SSH public key file '{}': {e}",
                path.display()
            )
        })?;
        let key = content.trim().to_string();
        validate_ssh_pubkey(&key, path)?;
        file_keys.push(key);
    }
    let explicit_given = !args.ssh_pubkey_file.is_empty() || !args.ssh_key.is_empty();
    let signer_address = account.address().clone();
    // Fetch the owner's registry to resolve --ssh-key labels and for the no-flag
    // fallback (attach all of the owner's keys).
    let owner_keys = if !args.ssh_key.is_empty() || !explicit_given {
        aleph_client.list_ssh_keys(&owner_address).await?
    } else {
        Vec::new()
    };
    // When creating on behalf of another owner, --ssh-key may also reference the
    // signer's own registered keys; the signer's keys win on name collisions
    // (we act from the signer's point of view). The fallback stays owner-only.
    let sender_keys = if !args.ssh_key.is_empty() && signer_address != owner_address {
        aleph_client.list_ssh_keys(&signer_address).await?
    } else {
        Vec::new()
    };
    let label_registry = merge_ssh_registries(&owner_keys, &sender_keys);
    let selected = select_keys_by_label(&args.ssh_key, &label_registry)?;
    let all_registered: Vec<String> = owner_keys.iter().map(|k| k.key.clone()).collect();
    let ssh_keys = resolve_instance_ssh_keys(file_keys, selected, explicit_given, all_registered)?;

    // Resolve instance specs. GPU instances size from the GPU pricing namespace
    // (the tier minimum is a lower bound); otherwise from --size or raw flags.
    let gpu_requested = args.gpu.as_ref().is_some_and(|g| !g.is_empty());
    let (vcpus, memory_mib, disk_size_mib) = if gpu_requested {
        // When several GPUs are requested we size from the first model's minimum
        // (we do not sum across models); the others share the same VM resources.
        let gpu_slug = &args.gpu.as_ref().unwrap()[0];
        let model_name = GPU_PRESETS
            .iter()
            .find(|(slug, ..)| slug.eq_ignore_ascii_case(gpu_slug))
            .map(|(_, model, ..)| *model)
            .ok_or_else(|| {
                let available: Vec<&str> = GPU_PRESETS.iter().map(|(n, ..)| *n).collect();
                anyhow!(
                    "unknown GPU model '{gpu_slug}'. Available models: {}",
                    available.join(", ")
                )
            })?;

        let pricing = aleph_client
            .get_pricing_aggregate()
            .await
            .map_err(|e| anyhow!("failed to fetch pricing tiers: {e}"))?;
        let instance_pricing = pricing.pricing.for_instance(false, Some(model_name));

        let tier = instance_pricing
            .tiers
            .iter()
            .find(|t| t.model.as_deref() == Some(model_name))
            .ok_or_else(|| anyhow!("GPU tier not found for '{model_name}'"))?;
        let min_cu = tier.compute_units;
        let cu_spec = &instance_pricing.compute_unit;

        let cu = resolve_gpu_compute_units(
            instance_pricing,
            min_cu,
            gpu_slug,
            args.size.as_deref(),
            args.vcpus,
            args.memory,
        )?;

        let vcpus = cu * cu_spec.vcpus;
        let memory_mib = cu as u64 * cu_spec.memory_mib;
        let disk_size_mib = args.disk_size.unwrap_or(cu as u64 * cu_spec.disk_mib);

        eprintln!(
            "GPU '{gpu_slug}' ({cu} CU): {vcpus} vCPUs, {memory_mib} MiB memory, {disk_size_mib} MiB disk",
        );

        (vcpus, memory_mib, disk_size_mib)
    } else if let Some(slug) = &args.size {
        let pricing = aleph_client
            .get_pricing_aggregate()
            .await
            .map_err(|e| anyhow!("failed to fetch pricing tiers: {e}"))?;
        let instance_pricing = &pricing.pricing.instance;

        let tier = instance_pricing
            .find_tier_by_slug(slug)
            .ok_or_else(|| anyhow!(pricing.pricing.invalid_instance_size_message(slug)))?;

        let vcpus = args.vcpus.unwrap_or(tier.vcpus);
        let memory_mib = args.memory.unwrap_or(tier.memory_mib);
        let disk_size_mib = args.disk_size.unwrap_or(tier.disk_mib);

        eprintln!(
            "Size '{slug}': {vcpus} vCPUs, {} MiB memory, {} MiB disk",
            memory_mib, disk_size_mib,
        );

        (vcpus, memory_mib, disk_size_mib)
    } else {
        resolve_instance_specs_from_flags(args.vcpus, args.memory, args.disk_size)?
    };

    let disk_size = PersistentVolumeSize::try_from(disk_size_mib)
        .map_err(|e| anyhow!("invalid disk size: {e}"))?;

    let image_ref = args
        .image
        .clone()
        .context("--image is required (or use -i)")?;

    let needs_aggregate = matches!(image_ref, ImageRef::Preset(_))
        || (args.confidential && !matches!(args.confidential_firmware, Some(ImageRef::Hash(_))));

    let vm_images = if needs_aggregate {
        aleph_client
            .get_vm_images_aggregate()
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to fetch vm-images aggregate: {e}. \
                     As a fallback, pass --image with a raw item hash or IPFS CID."
                )
            })?
            .vm_images
    } else {
        VmImagesData::default()
    };

    let resolved = resolve_image_refs(
        image_ref,
        args.confidential,
        args.confidential_firmware.clone(),
        &vm_images,
    )?;

    let image = resolved.rootfs;

    let mut builder = InstanceBuilder::new(&account, image, disk_size)
        .vcpus(vcpus)
        .memory(MiB::from(memory_mib))
        .hypervisor(Hypervisor::Qemu)
        .payment(Payment {
            chain: None,
            receiver: None,
            payment_type: PaymentType::Credit,
        })
        .ssh_keys(ssh_keys);

    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("name".to_string(), serde_json::json!(args.name));
    builder = builder.metadata(metadata);

    // Confidential VM
    if args.confidential {
        let firmware = resolved
            .confidential_firmware
            .clone()
            .expect("resolver guarantees Some when confidential is true");
        builder = builder.trusted_execution(TrustedExecutionEnvironment {
            firmware: Some(firmware),
            policy: 0x1, // NoDebug
        });
    }

    // GPU requirements
    let gpu_props = if let Some(gpu_names) = &args.gpu {
        let mut gpus = Vec::new();
        for name in gpu_names {
            gpus.push(resolve_gpu(name)?);
        }
        Some(gpus)
    } else {
        None
    };

    // Build host requirements if CRN hash or GPU is specified
    if args.crn_hash.is_some() || gpu_props.is_some() {
        let requirements = HostRequirements {
            cpu: None,
            node: args.crn_hash.map(|hash| NodeRequirements {
                owner: None,
                address_regex: None,
                node_hash: Some(hash.to_string()),
                terms_and_conditions: None,
            }),
            gpu: gpu_props,
        };
        builder = builder.requirements(requirements);
    }

    // Parse volumes
    let mut volumes = Vec::new();
    if let Some(specs) = &args.persistent_volume {
        volumes.extend(parse_persistent_volumes(specs)?);
    }
    if let Some(specs) = &args.ephemeral_volume {
        volumes.extend(parse_ephemeral_volumes(specs)?);
    }
    if let Some(specs) = &args.immutable_volume {
        volumes.extend(parse_immutable_volumes(specs)?);
    }
    if !volumes.is_empty() {
        builder = builder.volumes(volumes);
    }

    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }

    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

/// Known GPU presets: (slug, pricing_model, vendor, device_name, device_class, device_id).
/// `pricing_model` matches the `model` field in pricing aggregate tiers.
const GPU_PRESETS: &[(&str, &str, &str, &str, &str, &str)] = &[
    (
        "rtx3090",
        "RTX 3090",
        "NVIDIA",
        "GA102 [GeForce RTX 3090]",
        "0300",
        "10de:2204",
    ),
    (
        "rtx4000ada",
        "RTX 4000 ADA",
        "NVIDIA",
        "AD104GL [RTX 4000 SFF Ada Generation]",
        "0300",
        "10de:27b0",
    ),
    (
        "rtx4090",
        "RTX 4090",
        "NVIDIA",
        "AD102 [GeForce RTX 4090]",
        "0300",
        "10de:2684",
    ),
    (
        "rtx5090",
        "RTX 5090",
        "NVIDIA",
        "GB202 [GeForce RTX 5090]",
        "0300",
        "10de:2684",
    ),
    (
        "l40s",
        "L40S",
        "NVIDIA",
        "AD102GL [L40S]",
        "0302",
        "10de:26b9",
    ),
    (
        "a100",
        "A100",
        "NVIDIA",
        "GA100 [A100 PCIe 80GB]",
        "0302",
        "10de:20b5",
    ),
    (
        "h100",
        "H100",
        "NVIDIA",
        "GH100 [H100 PCIe]",
        "0302",
        "10de:2331",
    ),
];

fn resolve_gpu(name: &str) -> Result<GpuProperties> {
    let lower = name.to_ascii_lowercase();
    for &(slug, _, vendor, device_name, class, device_id) in GPU_PRESETS {
        if lower == slug {
            let device_class = match class {
                "0300" => GpuDeviceClass::VgaCompatibleController,
                "0302" => GpuDeviceClass::_3DController,
                _ => unreachable!(),
            };
            return Ok(GpuProperties {
                vendor: vendor.to_string(),
                device_name: device_name.to_string(),
                device_class,
                device_id: device_id.to_string(),
            });
        }
    }
    let available: Vec<&str> = GPU_PRESETS.iter().map(|(n, ..)| *n).collect();
    Err(anyhow!(
        "unknown GPU model '{name}'. Available models: {}",
        available.join(", ")
    ))
}

/// Guidance shown under the GPU table so users know how to actually create a
/// GPU instance, and so they do not copy the "Min size" slug into `--size`.
///
/// `example_model` is a real model slug from the table (or a placeholder when
/// none is available) so the example line is copy-pasteable.
/// One line describing a tier's compute unit, e.g.
/// "1 compute unit = 1 vCPU + 6 GiB RAM + 60 GiB disk".
fn compute_unit_summary(cu: &aleph_sdk::aggregate_models::pricing::ComputeUnitSpec) -> String {
    format!(
        "1 compute unit = {} vCPU + {} GiB RAM + {} GiB disk",
        cu.vcpus,
        cu.memory_mib / 1024,
        cu.disk_mib / 1024,
    )
}

fn print_available_gpus(pricing: &aleph_sdk::aggregate_models::pricing::PricingData) {
    let models = pricing.available_gpu_models();
    if models.is_empty() {
        eprintln!("No GPU models available.");
        return;
    }
    // Group models by their pricing tier. Each tier has its own compute-unit
    // definition; a model's "Min size" is its minimum number of those units.
    for (tier_name, entity) in [
        ("standard", &pricing.instance_gpu_standard),
        ("premium", &pricing.instance_gpu_premium),
    ] {
        let mut tier_models: Vec<_> = models.iter().filter(|m| m.tier == tier_name).collect();
        if tier_models.is_empty() {
            continue;
        }
        // Sort alphabetically by display slug rather than aggregate order.
        tier_models.sort_by_key(|m| m.slug());
        eprintln!(
            "\n{tier_name} tier  ({})",
            compute_unit_summary(&entity.compute_unit)
        );
        eprintln!("  {:<30} {:<10} Min size", "Model", "VRAM");
        for gpu in tier_models {
            let vram = gpu
                .vram_mib
                .map(|v| format!("{} GiB", v / 1024))
                .unwrap_or_default();
            let min_size = entity.slug_for_compute_units(gpu.compute_units);
            eprintln!("  {:<30} {:<10} {}", gpu.slug(), vram, min_size);
        }
    }
}

async fn handle_instance_price(
    aleph_client: &AlephClient,
    json: bool,
    args: InstancePriceArgs,
) -> Result<()> {
    let pricing = aleph_client
        .get_pricing_aggregate()
        .await
        .map_err(|e| anyhow!("failed to fetch pricing tiers: {e}"))?;

    if args.confidential && args.gpu.is_some() {
        bail!("--confidential and --gpu cannot be combined");
    }

    if args.list_gpus || args.gpu.as_deref() == Some("") {
        print_available_gpus(&pricing.pricing);
        return Ok(());
    }

    // Match the user-provided GPU name against pricing tier model names
    let gpu_model = if let Some(slug) = args.gpu.as_deref() {
        let models = pricing.pricing.available_gpu_models();
        let matched = models.iter().find(|m| m.slug() == slug);
        match matched {
            Some(m) => Some(m.clone()),
            None => {
                let names: Vec<String> = models.iter().map(|m| m.slug()).collect();
                bail!(
                    "unknown GPU model '{slug}'. Available models: {}",
                    names.join(", ")
                );
            }
        }
    } else {
        None
    };
    let instance_pricing = pricing.pricing.for_instance(
        args.confidential,
        gpu_model.as_ref().map(|m| m.name.as_str()),
    );

    let cu_price = instance_pricing
        .price
        .get("compute_unit")
        .context("missing compute_unit price in pricing aggregate")?;

    let credit_per_cu: f64 = cu_price
        .credit
        .parse()
        .map_err(|_| anyhow!("invalid credit price: '{}'", cu_price.credit))?;

    // Resolve specs: GPU tier, --size tier, or fully manual
    let (size_slug, compute_units, vcpus, memory_mib, disk_mib) = if let Some(gpu) = &gpu_model {
        // GPU: tier CU count is a lower bound; --size or --vcpus/--memory can raise it.
        let tier = instance_pricing
            .tiers
            .iter()
            .find(|t| t.model.as_deref() == Some(&gpu.name))
            .ok_or_else(|| anyhow!("GPU tier not found for '{}'", gpu.name))?;
        let min_cu = tier.compute_units;
        let cu_spec = &instance_pricing.compute_unit;

        let cu = resolve_gpu_compute_units(
            instance_pricing,
            min_cu,
            &gpu.slug(),
            args.size.as_deref(),
            args.vcpus,
            args.memory,
        )?;

        let disk = args.disk_size.unwrap_or(cu as u64 * cu_spec.disk_mib);
        (
            None,
            cu,
            cu * cu_spec.vcpus,
            cu as u64 * cu_spec.memory_mib,
            disk,
        )
    } else if let Some(slug) = &args.size {
        let tier = instance_pricing
            .find_tier_by_slug(slug)
            .ok_or_else(|| anyhow!(pricing.pricing.invalid_instance_size_message(slug)))?;
        (
            Some(slug.clone()),
            tier.compute_units,
            args.vcpus.unwrap_or(tier.vcpus),
            args.memory.unwrap_or(tier.memory_mib),
            args.disk_size.unwrap_or(tier.disk_mib),
        )
    } else {
        match (args.vcpus, args.memory, args.disk_size) {
            (Some(vcpus), Some(memory), Some(disk)) => {
                let cu = &instance_pricing.compute_unit;
                let cu_from_vcpus = vcpus.div_ceil(cu.vcpus);
                let cu_from_mem = memory.div_ceil(cu.memory_mib) as u32;
                let compute_units = cu_from_vcpus.max(cu_from_mem);
                let actual_vcpus = compute_units * cu.vcpus;
                let actual_memory = compute_units as u64 * cu.memory_mib;
                (None, compute_units, actual_vcpus, actual_memory, disk)
            }
            _ => {
                bail!(
                    "--size is required unless --vcpus, --memory, and --disk-size are all specified"
                );
            }
        }
    };

    // Compute cost (credits/hour)
    let compute_credits = credit_per_cu * compute_units as f64;

    // Storage cost (credits/hour): all disk is charged, then a discount is applied
    // for the storage included in each compute unit.
    let storage_credit_per_mib: f64 = instance_pricing
        .price
        .get("storage")
        .map(|p| p.credit.parse::<f64>().unwrap_or(0.0))
        .unwrap_or(0.0);

    let storage_credits = storage_credit_per_mib * disk_mib as f64;
    let included_storage_mib = instance_pricing.compute_unit.disk_mib as f64 * compute_units as f64;
    let max_storage_discount = storage_credit_per_mib * included_storage_mib;
    let storage_discount = storage_credits.min(max_storage_discount);
    let extra_storage_credits = storage_credits - storage_discount;

    let total_credits = compute_credits + extra_storage_credits;
    let total_dollars = total_credits * 1e-6;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "size": size_slug,
                "compute_units": compute_units,
                "vcpus": vcpus,
                "memory_mib": memory_mib,
                "disk_mib": disk_mib,
                "gpu": gpu_model.as_ref().map(|m| m.slug()),
                "confidential": args.confidential,
                "compute_credits_per_hour": compute_credits,
                "storage_credits_per_hour": extra_storage_credits,
                "total_credits_per_hour": total_credits,
                "dollars_per_hour": total_dollars,
            }))?
        );
    } else {
        if let Some(slug) = &size_slug {
            eprintln!("Size:    {slug}");
        }
        if let Some(gpu) = &gpu_model {
            eprintln!("GPU:     {}", gpu.slug());
        }
        if args.confidential {
            eprintln!("Type:    confidential");
        }
        eprintln!("vCPUs:   {}", vcpus);
        eprintln!("Memory:  {} MiB", memory_mib);
        eprintln!("Disk:    {} MiB", disk_mib);
        if extra_storage_credits > 0.0 {
            eprintln!(
                "Cost:    {:.0} credits/hour (${:.4}/hour) — compute: {:.0}, extra storage: {:.0}",
                total_credits, total_dollars, compute_credits, extra_storage_credits
            );
        } else {
            eprintln!(
                "Cost:    {:.0} credits/hour (${:.4}/hour)",
                total_credits, total_dollars
            );
        }
    }

    Ok(())
}

/// Fetch an INSTANCE message by item hash and assert it is currently usable
/// (processed or in the process of being removed). Returns a clean error for
/// pending / forgotten / rejected statuses or for non-INSTANCE hashes.
async fn fetch_instance_message(
    aleph_client: &AlephClient,
    item_hash: &ItemHash,
) -> Result<Message> {
    let with_status = aleph_client
        .get_message(item_hash)
        .await
        .with_context(|| format!("failed to fetch instance {item_hash}"))?;
    let message = match with_status {
        MessageWithStatus::Processed { message } => message,
        MessageWithStatus::Removing { message, .. } => message,
        MessageWithStatus::Removed { .. } => {
            bail!("instance {item_hash} has been removed")
        }
        MessageWithStatus::Pending { .. } => {
            bail!(
                "instance {item_hash} is still pending; wait for it to be processed before deleting"
            )
        }
        MessageWithStatus::Forgotten { .. } => {
            bail!("instance {item_hash} has already been forgotten")
        }
        MessageWithStatus::Rejected { .. } => {
            bail!("instance {item_hash} was rejected by the network")
        }
    };
    if message.message_type != MessageType::Instance {
        bail!(
            "item {item_hash} is not an INSTANCE message (got {:?})",
            message.message_type
        );
    }
    Ok(message)
}

/// Build the FORGET targeting an INSTANCE message.
fn build_forget_for_instance<A: Account>(
    account: &A,
    instance: &Message,
    reason: &str,
) -> Result<PendingMessage> {
    if instance.message_type != MessageType::Instance {
        bail!("expected INSTANCE message, got {:?}", instance.message_type);
    }
    Ok(
        ForgetBuilder::new(account, vec![instance.item_hash.clone()])
            .reason(reason)
            .build()?,
    )
}

async fn handle_instance_delete(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    scheduler_url: &Url,
    json: bool,
    args: InstanceDeleteArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    let (vm_id, _) = super::instance_target::resolve_vm(scheduler_url, &args.vm_id).await?;
    let instance = fetch_instance_message(aleph_client, &vm_id).await?;
    if &instance.sender != account.address() {
        bail!(
            "you are not the owner of instance {} (sender: {})",
            vm_id,
            instance.sender
        );
    }

    let prompt = format!("Forget instance {vm_id}? This is irreversible.");
    if !dry_run && !confirm_action(&prompt, args.yes)? {
        bail!("aborted");
    }

    let pending = build_forget_for_instance(&account, &instance, &args.reason)?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::parse_size_to_mib;

    #[test]
    fn compute_unit_summary_describes_the_unit() {
        use aleph_sdk::aggregate_models::pricing::ComputeUnitSpec;
        let cu = ComputeUnitSpec {
            vcpus: 1,
            memory_mib: 6144,
            disk_mib: 61440,
        };
        assert_eq!(
            compute_unit_summary(&cu),
            "1 compute unit = 1 vCPU + 6 GiB RAM + 60 GiB disk"
        );
    }

    #[test]
    fn parse_kv_pairs_basic() {
        let pairs = parse_kv_pairs("name=data,mount=/opt/data,size=1GiB").unwrap();
        assert_eq!(
            pairs,
            vec![("name", "data"), ("mount", "/opt/data"), ("size", "1GiB")]
        );
    }

    #[test]
    fn parse_kv_pairs_missing_equals() {
        assert!(parse_kv_pairs("invalid").is_err());
    }

    #[test]
    fn parse_size_binary_units() {
        assert_eq!(parse_size_to_mib("100MiB").unwrap(), 100);
        assert_eq!(parse_size_to_mib("1GiB").unwrap(), 1024);
        assert_eq!(parse_size_to_mib("2GiB").unwrap(), 2048);
        assert_eq!(parse_size_to_mib("1TiB").unwrap(), 1024 * 1024);
    }

    #[test]
    fn parse_size_decimal_units() {
        // 1 GB = 1_000_000_000 bytes = ~953.674 MiB
        assert_eq!(parse_size_to_mib("1GB").unwrap(), 954);
        // 20 GB = ~19073.486 MiB
        assert_eq!(parse_size_to_mib("20GB").unwrap(), 19073);
        // 100 MB = ~95.367 MiB
        assert_eq!(parse_size_to_mib("100MB").unwrap(), 95);
    }

    #[test]
    fn parse_size_case_insensitive() {
        assert_eq!(parse_size_to_mib("1gib").unwrap(), 1024);
        assert_eq!(parse_size_to_mib("1GIB").unwrap(), 1024);
        assert_eq!(
            parse_size_to_mib("1gb").unwrap(),
            parse_size_to_mib("1GB").unwrap()
        );
    }

    #[test]
    fn parse_size_rejects_bare_numbers() {
        assert!(parse_size_to_mib("1024").is_err());
    }

    #[test]
    fn parse_size_rejects_unknown_units() {
        assert!(parse_size_to_mib("100KiB").is_err());
    }

    #[test]
    fn parse_persistent_volume_basic() {
        let specs = vec!["name=data,mount=/opt/data,size=1GiB".to_string()];
        let volumes = parse_persistent_volumes(&specs).unwrap();
        assert_eq!(volumes.len(), 1);
        assert!(matches!(volumes[0], MachineVolume::Persistent(_)));
    }

    #[test]
    fn parse_persistent_volume_with_persistence() {
        let specs = vec!["name=db,mount=/var/db,size=500MiB,persistence=store".to_string()];
        let volumes = parse_persistent_volumes(&specs).unwrap();
        if let MachineVolume::Persistent(v) = &volumes[0] {
            assert_eq!(v.persistence, Some(VolumePersistence::Store));
            assert_eq!(v.name, Some("db".to_string()));
        } else {
            panic!("expected persistent volume");
        }
    }

    #[test]
    fn parse_persistent_volume_with_comment() {
        let specs = vec!["name=db,mount=/var/db,size=500MiB,comment=My database".to_string()];
        let volumes = parse_persistent_volumes(&specs).unwrap();
        if let MachineVolume::Persistent(v) = &volumes[0] {
            assert_eq!(v.base.comment, Some("My database".to_string()));
        } else {
            panic!("expected persistent volume");
        }
    }

    #[test]
    fn parse_persistent_volume_missing_size() {
        let specs = vec!["name=data,mount=/opt/data".to_string()];
        assert!(parse_persistent_volumes(&specs).is_err());
    }

    #[test]
    fn parse_persistent_volume_missing_mount() {
        let specs = vec!["name=data,size=1GiB".to_string()];
        assert!(parse_persistent_volumes(&specs).is_err());
    }

    #[test]
    fn parse_ephemeral_volume_basic() {
        let specs = vec!["mount=/tmp/scratch,size=100MiB".to_string()];
        let volumes = parse_ephemeral_volumes(&specs).unwrap();
        assert_eq!(volumes.len(), 1);
        assert!(matches!(volumes[0], MachineVolume::Ephemeral(_)));
    }

    #[test]
    fn parse_ephemeral_volume_missing_mount() {
        let specs = vec!["size=100MiB".to_string()];
        assert!(parse_ephemeral_volumes(&specs).is_err());
    }

    #[test]
    fn parse_immutable_volume_basic() {
        let specs = vec![
            "ref=d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c,mount=/opt/pkg"
                .to_string(),
        ];
        let volumes = parse_immutable_volumes(&specs).unwrap();
        assert_eq!(volumes.len(), 1);
        if let MachineVolume::Immutable(v) = &volumes[0] {
            assert!(v.use_latest); // default
        } else {
            panic!("expected immutable volume");
        }
    }

    #[test]
    fn parse_immutable_volume_use_latest_false() {
        let specs = vec![
            "ref=d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c,mount=/opt/pkg,use_latest=false"
                .to_string(),
        ];
        let volumes = parse_immutable_volumes(&specs).unwrap();
        if let MachineVolume::Immutable(v) = &volumes[0] {
            assert!(!v.use_latest);
        } else {
            panic!("expected immutable volume");
        }
    }

    #[test]
    fn parse_immutable_volume_missing_ref() {
        let specs = vec!["mount=/opt/pkg".to_string()];
        assert!(parse_immutable_volumes(&specs).is_err());
    }

    #[test]
    fn parse_multiple_volumes() {
        let persistent = vec![
            "name=a,mount=/a,size=100MiB".to_string(),
            "name=b,mount=/b,size=200MiB".to_string(),
        ];
        let volumes = parse_persistent_volumes(&persistent).unwrap();
        assert_eq!(volumes.len(), 2);
    }

    #[test]
    fn validate_ssh_pubkey_accepts_valid_keys() {
        let path = std::path::Path::new("test.pub");
        validate_ssh_pubkey("ssh-rsa AAAAB3... user@host", path).unwrap();
        validate_ssh_pubkey("ssh-ed25519 AAAAC3... user@host", path).unwrap();
        validate_ssh_pubkey("ecdsa-sha2-nistp256 AAAAE2... user@host", path).unwrap();
        validate_ssh_pubkey("sk-ssh-ed25519@openssh.com AAAAG... user@host", path).unwrap();
    }

    #[test]
    fn validate_ssh_pubkey_rejects_private_key() {
        let path = std::path::Path::new("id_rsa");
        assert!(validate_ssh_pubkey("-----BEGIN OPENSSH PRIVATE KEY-----", path).is_err());
    }

    #[test]
    fn validate_ssh_pubkey_rejects_garbage() {
        let path = std::path::Path::new("garbage.txt");
        assert!(validate_ssh_pubkey("not a key at all", path).is_err());
    }

    #[test]
    fn parse_image_ref_handles_preset_strings() {
        use crate::cli::{ImageRef, parse_image_ref};
        // Preset names resolve to Preset variant; the aggregate is the source
        // of truth for which preset names are valid at runtime.
        for name in [
            "ubuntu22",
            "ubuntu24",
            "Ubuntu22",
            "debian12",
            "anything-else",
        ] {
            match parse_image_ref(name).unwrap() {
                ImageRef::Preset(p) => assert_eq!(p, name),
                ImageRef::Hash(_) => panic!("expected Preset for {name}"),
            }
        }
    }

    #[test]
    fn parse_image_ref_hash() {
        use crate::cli::{ImageRef, parse_image_ref};
        let parsed =
            parse_image_ref("5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e")
                .unwrap();
        match parsed {
            ImageRef::Hash(h) => assert_eq!(
                h.to_string(),
                "5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e"
            ),
            ImageRef::Preset(p) => panic!("expected Hash, got Preset({p})"),
        }
    }

    #[test]
    fn parse_image_ref_cid() {
        use crate::cli::{ImageRef, parse_image_ref};
        let parsed = parse_image_ref("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").unwrap();
        assert!(matches!(parsed, ImageRef::Hash(_)));
    }

    #[test]
    fn parse_image_ref_preset_name() {
        use crate::cli::{ImageRef, parse_image_ref};
        let parsed = parse_image_ref("ubuntu24").unwrap();
        match parsed {
            ImageRef::Preset(name) => assert_eq!(name, "ubuntu24"),
            ImageRef::Hash(_) => panic!("expected Preset, got Hash"),
        }
    }

    #[test]
    fn parse_image_ref_rejects_empty() {
        use crate::cli::parse_image_ref;
        assert!(parse_image_ref("").is_err());
        assert!(parse_image_ref("   ").is_err());
    }

    use std::collections::HashMap;

    #[test]
    fn name_from_metadata_returns_string_value() {
        let mut meta = HashMap::new();
        meta.insert("name".to_string(), serde_json::json!("my-vm"));
        assert_eq!(name_from_metadata(Some(&meta)), Some("my-vm".to_string()));
    }

    #[test]
    fn name_from_metadata_returns_none_when_missing() {
        assert_eq!(name_from_metadata(None), None);
        let empty: HashMap<String, serde_json::Value> = HashMap::new();
        assert_eq!(name_from_metadata(Some(&empty)), None);
    }

    #[test]
    fn name_from_metadata_returns_none_for_non_string() {
        let mut meta = HashMap::new();
        meta.insert("name".to_string(), serde_json::json!(42));
        assert_eq!(name_from_metadata(Some(&meta)), None);
    }

    #[test]
    fn node_from_requirements_returns_node_hash() {
        let req = HostRequirements {
            cpu: None,
            node: Some(NodeRequirements {
                owner: None,
                address_regex: None,
                node_hash: Some("aa00".to_string()),
                terms_and_conditions: None,
            }),
            gpu: None,
        };
        assert_eq!(node_from_requirements(Some(&req)), Some("aa00".to_string()));
    }

    #[test]
    fn node_from_requirements_returns_none_when_no_requirements() {
        assert_eq!(node_from_requirements(None), None);
    }

    #[test]
    fn node_from_requirements_returns_none_when_no_node() {
        let req = HostRequirements {
            cpu: None,
            node: None,
            gpu: None,
        };
        assert_eq!(node_from_requirements(Some(&req)), None);
    }

    #[test]
    fn node_from_requirements_returns_none_when_node_hash_missing() {
        let req = HostRequirements {
            cpu: None,
            node: Some(NodeRequirements {
                owner: None,
                address_regex: None,
                node_hash: None,
                terms_and_conditions: None,
            }),
            gpu: None,
        };
        assert_eq!(node_from_requirements(Some(&req)), None);
    }

    #[test]
    fn extract_instance_row_from_fixture() {
        const FIXTURE: &str = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/instance/instance-gpu-payg.json"
        ));
        let message: Message = serde_json::from_str(FIXTURE).expect("fixture parses");
        let row = extract_instance_row(&message).expect("instance row extracted");
        assert_eq!(
            row.item_hash.to_string(),
            "a41fb91c3e68370759b72338dd1947f18e2ed883837aec5dc731d5f427f90564"
        );
        assert_eq!(row.name.as_deref(), Some("gpu-l40s-2"));
        assert_eq!(
            row.owner.to_string(),
            "0x238224C744F4b90b4494516e074D2676ECfC6803"
        );
        assert_eq!(
            row.node_hash.as_deref(),
            Some("dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04")
        );
    }

    const INSTANCE_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/instance/instance-gpu-payg.json"
    ));

    fn fixture_message() -> Message {
        serde_json::from_str(INSTANCE_FIXTURE).expect("parse fixture")
    }

    #[test]
    fn fixture_loads_as_instance_message() {
        let msg = fixture_message();
        assert_eq!(msg.message_type, MessageType::Instance);
    }

    use aleph_types::account::{Account, SignError};
    use aleph_types::chain::{Chain, Signature};

    /// Minimal test account that produces a dummy signature. Mirrors the
    /// `TestAccount` in `commands/program.rs` tests.
    struct TestAccount {
        address: Address,
    }

    impl TestAccount {
        fn new() -> Self {
            Self {
                address: Address::from("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".to_string()),
            }
        }
    }

    impl Account for TestAccount {
        fn chain(&self) -> Chain {
            Chain::Ethereum
        }
        fn address(&self) -> &Address {
            &self.address
        }
        fn sign_raw(&self, _buffer: &[u8]) -> Result<Signature, SignError> {
            Ok(Signature::from("0xDUMMY".to_string()))
        }
    }

    #[test]
    fn build_forget_for_instance_targets_only_the_instance_hash() {
        let instance = fixture_message();
        let account = TestAccount::new();
        let pending = build_forget_for_instance(&account, &instance, "User deletion").unwrap();
        let value: serde_json::Value = serde_json::from_str(&pending.item_content).unwrap();
        let hashes = value["hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].as_str().unwrap(), instance.item_hash.to_string());
        assert_eq!(value["reason"], "User deletion");
        assert!(value["aggregates"].as_array().is_none_or(|a| a.is_empty()));
    }

    fn sample_row(
        hash: &str,
        name: Option<&str>,
        node: Option<&str>,
        epoch_seconds: f64,
    ) -> InstanceRow {
        InstanceRow {
            item_hash: hash.parse().expect("valid item hash"),
            name: name.map(|s| s.to_string()),
            owner: Address::from("0xAbCd1234567890aBcDEf1234567890AbCdEF1234".to_string()),
            node_hash: node.map(|s| s.to_string()),
            created_at: Timestamp::from(epoch_seconds),
            status: None,
            allocated_node: None,
            ipv6: None,
            ipv4: None,
            scheduler_raw: None,
            source_flags: Default::default(),
        }
    }

    #[test]
    fn format_rows_json_shape() {
        // Unix epoch 1_700_000_000 = 2023-11-14T22:13:20Z — used as a known-good
        // anchor to verify `created_at` is emitted as RFC 3339.
        let rows = vec![
            sample_row(
                "0000000000000000000000000000000000000000000000000000000000000001",
                Some("vm-a"),
                Some("aa00"),
                1_700_000_000.0,
            ),
            sample_row(
                "0000000000000000000000000000000000000000000000000000000000000002",
                None,
                None,
                1_700_000_001.0,
            ),
        ];
        let value = format_rows_json(&rows);
        let arr = value.as_array().expect("top-level is array");
        assert_eq!(arr.len(), 2);

        // First row: all fields populated.
        assert_eq!(
            arr[0]["item_hash"],
            "0000000000000000000000000000000000000000000000000000000000000001"
        );
        assert_eq!(arr[0]["name"], "vm-a");
        assert_eq!(
            arr[0]["owner"],
            "0xAbCd1234567890aBcDEf1234567890AbCdEF1234"
        );
        assert_eq!(arr[0]["node_hash"], "aa00");
        assert_eq!(arr[0]["created_at"], "2023-11-14T22:13:20+00:00");

        // Second row: missing name and node_hash serialize as JSON null.
        assert!(arr[1]["name"].is_null());
        assert!(arr[1]["node_hash"].is_null());

        // No scheduler data in these fixture rows: field must be null.
        assert!(arr[0]["scheduler"].is_null());
        assert!(arr[1]["scheduler"].is_null());
    }

    #[test]
    fn format_rows_json_includes_scheduler_object_when_enriched() {
        // Build a row and populate scheduler fields manually (mimics what
        // merge_scheduler_into_rows would do).
        let mut row = sample_row(
            "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99",
            Some("foo"),
            Some("requested-node"),
            1_700_000_000.0,
        );
        row.status = Some("dispatched".to_string());
        row.allocated_node =
            Some("d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77".to_string());
        row.scheduler_raw = Some(make_vm_entry(
            "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99",
            "dispatched",
            Some("d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77"),
        ));

        let v = format_rows_json(&[row]);
        let arr = v.as_array().expect("json array");
        assert_eq!(arr[0]["scheduler"]["status"], "dispatched");
        assert_eq!(
            arr[0]["scheduler"]["allocated_node"],
            "d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77"
        );
        // Top-level node_hash (CCN-requested) preserved alongside scheduler.allocated_node.
        assert_eq!(arr[0]["node_hash"], "requested-node");
    }

    #[test]
    fn format_rows_json_scheduler_is_null_when_unenriched() {
        let row = sample_row(
            "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99",
            Some("foo"),
            None,
            1_700_000_000.0,
        );
        let v = format_rows_json(&[row]);
        let arr = v.as_array().expect("json array");
        assert!(arr[0]["scheduler"].is_null());
    }

    #[test]
    fn format_rows_text_header_and_placeholders() {
        let rows = vec![
            sample_row(
                "0000000000000000000000000000000000000000000000000000000000000001",
                Some("vm-a"),
                Some("aa00"),
                1_700_000_000.0,
            ),
            sample_row(
                "0000000000000000000000000000000000000000000000000000000000000002",
                None,
                None,
                1_700_000_001.0,
            ),
        ];
        let text = format_rows_text(&rows);
        let lines: Vec<&str> = text.lines().collect();

        // Header + two data rows.
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("ITEM_HASH"));
        assert!(lines[0].contains("NAME"));
        assert!(lines[0].contains("OWNER"));
        assert!(lines[0].contains("STATUS"));
        assert!(lines[0].contains("IPV6"));
        assert!(lines[0].contains("ALLOCATED"));

        // Populated row renders the 12-char hash prefix (not full 64-char hash).
        assert!(lines[1].contains("000000000000"));
        assert!(
            !lines[1].contains("0000000000000000000000000000000000000000000000000000000000000001")
        );
        assert!(lines[1].contains("vm-a"));

        // Missing fields use the ASCII `-` placeholder (not `—`).
        assert!(!lines[2].contains('—'));
        // Exactly four `-` placeholders on the missing-fields row: NAME, STATUS,
        // IPV6, and ALLOCATED. Check with word boundaries (space on each side).
        assert_eq!(
            lines[2].matches(" - ").count() + lines[2].ends_with(" -") as usize,
            4
        );
    }

    #[test]
    fn format_rows_text_empty_has_header_only() {
        let text = format_rows_text(&[]);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("ITEM_HASH"));
    }

    #[test]
    fn format_rows_text_shows_ipv6_or_placeholder() {
        let mut with_ip = sample_row(
            "0000000000000000000000000000000000000000000000000000000000000001",
            Some("vm-a"),
            None,
            1_700_000_000.0,
        );
        with_ip.ipv6 = Some("2a01:240:ad00:1::7db1".to_string());
        let without_ip = sample_row(
            "0000000000000000000000000000000000000000000000000000000000000002",
            Some("vm-b"),
            None,
            1_700_000_001.0,
        );

        let text = format_rows_text(&[with_ip, without_ip]);
        let lines: Vec<&str> = text.lines().collect();

        // Header carries the IPV6 column.
        assert!(lines[0].contains("IPV6"));
        // Row with an address shows it under that column.
        assert!(lines[1].contains("2a01:240:ad00:1::7db1"));
        // Row without an address shows the `-` placeholder for IPv6.
        assert!(!lines[2].contains("2a01:240:ad00:1::7db1"));
        assert!(lines[2].contains(" - "));
    }

    #[test]
    fn group_row_indices_by_node_buckets_and_skips_unallocated() {
        let mut a = sample_row(
            "0000000000000000000000000000000000000000000000000000000000000001",
            None,
            None,
            0.0,
        );
        a.allocated_node = Some("node-x".to_string());
        let mut b = sample_row(
            "0000000000000000000000000000000000000000000000000000000000000002",
            None,
            None,
            0.0,
        );
        b.allocated_node = Some("node-x".to_string());
        let mut c = sample_row(
            "0000000000000000000000000000000000000000000000000000000000000003",
            None,
            None,
            0.0,
        );
        c.allocated_node = Some("node-y".to_string());
        // Row with no allocation must be excluded entirely.
        let d = sample_row(
            "0000000000000000000000000000000000000000000000000000000000000004",
            None,
            None,
            0.0,
        );

        let rows = vec![a, b, c, d];
        let groups = group_row_indices_by_node(&rows);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups.get("node-x"), Some(&vec![0, 1]));
        assert_eq!(groups.get("node-y"), Some(&vec![2]));
        assert!(!groups.contains_key(&String::new()));
    }

    #[test]
    fn resolve_instance_specs_without_size_uses_defaults() {
        let specs = resolve_instance_specs_from_flags(None, None, Some(20 * 1024));
        assert_eq!(specs.unwrap(), (1, 2048, 20 * 1024));
    }

    #[test]
    fn resolve_instance_specs_without_size_requires_disk() {
        assert!(resolve_instance_specs_from_flags(None, None, None).is_err());
    }

    #[test]
    fn resolve_instance_specs_applies_overrides() {
        let specs = resolve_instance_specs_from_flags(Some(4), Some(8192), Some(40 * 1024));
        assert_eq!(specs.unwrap(), (4, 8192, 40 * 1024));
    }

    use aleph_sdk::scheduler::VmEntry;

    fn make_vm_entry(hash: &str, status: &str, node: Option<&str>) -> VmEntry {
        let json = serde_json::json!({
            "vm_hash": hash,
            "vm_type": "instance",
            "allocated_node": node,
            "status": status,
            "scheduling_status": "scheduled",
            "migration_target": null,
            "owner": "0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072"
        });
        serde_json::from_value(json).expect("valid VmEntry json")
    }

    #[test]
    fn merge_populates_status_and_allocated_when_scheduler_has_entry() {
        const MERGE_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000042";
        let mut rows = vec![sample_row(MERGE_HASH, None, None, 0.0)];
        let hash: ItemHash = rows[0].item_hash.clone();
        let entry = make_vm_entry(
            &hash.to_string(),
            "dispatched",
            Some("d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77"),
        );
        let mut map = std::collections::HashMap::new();
        map.insert(hash, entry);

        merge_scheduler_into_rows(&mut rows, &map);

        assert_eq!(rows[0].status.as_deref(), Some("dispatched"));
        assert_eq!(
            rows[0].allocated_node.as_deref(),
            Some("d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77")
        );
        assert!(rows[0].scheduler_raw.is_some());
    }

    #[test]
    fn merge_leaves_row_blank_when_hash_not_in_map() {
        const MERGE_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000042";
        let mut rows = vec![sample_row(MERGE_HASH, None, None, 0.0)];
        let map = std::collections::HashMap::<ItemHash, VmEntry>::new();
        merge_scheduler_into_rows(&mut rows, &map);
        assert!(rows[0].status.is_none());
        assert!(rows[0].allocated_node.is_none());
        assert!(rows[0].scheduler_raw.is_none());
    }

    #[test]
    fn merge_does_not_add_scheduler_only_rows() {
        let mut rows: Vec<InstanceRow> = vec![];
        let mut map = std::collections::HashMap::new();
        let scheduler_only_hash: ItemHash =
            "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99"
                .parse()
                .expect("valid item hash");
        map.insert(
            scheduler_only_hash.clone(),
            make_vm_entry(
                &scheduler_only_hash.to_string(),
                "dispatched",
                Some("anything"),
            ),
        );
        merge_scheduler_into_rows(&mut rows, &map);
        assert!(rows.is_empty());
    }

    fn sample_row_with_flags(hash: &str, flags: SourceFlags) -> InstanceRow {
        let mut row = sample_row(hash, None, None, 0.0);
        row.source_flags = flags;
        row
    }

    #[test]
    fn needs_sender_enrichment_true_for_sender_only_row_not_in_map() {
        const HASH: &str = "0000000000000000000000000000000000000000000000000000000000000001";
        let rows = vec![sample_row_with_flags(
            HASH,
            SourceFlags {
                sender: true,
                owner: false,
            },
        )];
        let map = std::collections::HashMap::<ItemHash, VmEntry>::new();
        assert!(needs_sender_enrichment(&rows, &map));
    }

    #[test]
    fn needs_sender_enrichment_false_when_row_also_came_from_owner() {
        const HASH: &str = "0000000000000000000000000000000000000000000000000000000000000001";
        let rows = vec![sample_row_with_flags(
            HASH,
            SourceFlags {
                sender: true,
                owner: true,
            },
        )];
        let map = std::collections::HashMap::<ItemHash, VmEntry>::new();
        assert!(!needs_sender_enrichment(&rows, &map));
    }

    #[test]
    fn needs_sender_enrichment_false_when_already_in_map() {
        const HASH: &str = "0000000000000000000000000000000000000000000000000000000000000001";
        let rows = vec![sample_row_with_flags(
            HASH,
            SourceFlags {
                sender: true,
                owner: false,
            },
        )];
        let mut map = std::collections::HashMap::new();
        let hash: ItemHash = HASH.parse().unwrap();
        map.insert(
            hash.clone(),
            make_vm_entry(&hash.to_string(), "dispatched", None),
        );
        assert!(!needs_sender_enrichment(&rows, &map));
    }

    #[test]
    fn needs_sender_enrichment_false_for_empty_rows() {
        let rows: Vec<InstanceRow> = vec![];
        let map = std::collections::HashMap::<ItemHash, VmEntry>::new();
        assert!(!needs_sender_enrichment(&rows, &map));
    }

    #[tokio::test]
    async fn enrich_by_sender_skips_call_when_no_row_needs_it() {
        use wiremock::matchers::any;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // Server that would error on any request: if the bulk-sender call fired,
        // the 500 warning path would run. We assert it is never hit.
        let server = MockServer::start().await;
        Mock::given(any())
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        let scheduler = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let address = Address::from("0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072".to_string());
        // Owner-sourced row: no sender enrichment needed.
        let rows = vec![sample_row_with_flags(
            "0000000000000000000000000000000000000000000000000000000000000001",
            SourceFlags {
                sender: false,
                owner: true,
            },
        )];
        let mut map = std::collections::HashMap::new();
        enrich_by_sender(&scheduler, &address, &rows, &mut map).await;
        assert!(map.is_empty());
        // MockServer verifies expect(0) on drop.
    }

    #[tokio::test]
    async fn enrich_by_sender_fills_map_from_bulk_call() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const SENDER_HASH: &str =
            "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99";
        let address = "0xaAf798d5F80dAEE72AEe8557B890809E9f5B6072";

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .and(query_param("sender", address))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "items": [{
                    "vm_hash": SENDER_HASH,
                    "vm_type": "instance",
                    "allocated_node": "alloc-node",
                    "status": "dispatched",
                    "scheduling_status": "scheduled",
                    "migration_target": null,
                    "owner": "0xsomeotherowner"
                }],
                "pagination": { "page": 1, "page_size": 200, "total_items": 1, "total_pages": 1 }
            })))
            .mount(&server)
            .await;

        let scheduler = SchedulerClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from(address.to_string());
        let rows = vec![sample_row_with_flags(
            SENDER_HASH,
            SourceFlags {
                sender: true,
                owner: false,
            },
        )];
        let mut map = std::collections::HashMap::new();
        enrich_by_sender(&scheduler, &addr, &rows, &mut map).await;

        let hash: ItemHash = SENDER_HASH.parse().unwrap();
        let entry = map.get(&hash).expect("sender entry merged into map");
        assert_eq!(entry.status, "dispatched");
        assert_eq!(entry.allocated_node.as_deref(), Some("alloc-node"));
    }

    #[test]
    fn format_item_hash_short_takes_first_12() {
        let hash: ItemHash = "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99"
            .parse()
            .expect("valid item hash");
        assert_eq!(format_item_hash_short(&hash), "5a586d6f59f6");
    }

    #[test]
    fn format_node_short_takes_last_10() {
        let s = "d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77";
        assert_eq!(format_node_short(s), "78e0709d77");
    }

    #[test]
    fn format_node_short_passthrough_when_short() {
        assert_eq!(format_node_short("abc"), "abc");
        assert_eq!(format_node_short("0123456789"), "0123456789");
    }

    mod resolve_image_refs_tests {
        use super::super::resolve_image_refs;
        use crate::cli::ImageRef;
        use aleph_sdk::aggregate_models::vm_images::{
            ImageEntry, RootfsEntry, VmImageDefaults, VmImagesData,
        };
        use aleph_types::item_hash::ItemHash;
        use std::collections::BTreeMap;

        fn h(hex: &str) -> ItemHash {
            ItemHash::try_from(hex).unwrap()
        }

        fn fake_data() -> VmImagesData {
            let mut rootfs = BTreeMap::new();
            rootfs.insert(
                "ubuntu24".to_string(),
                RootfsEntry {
                    hash: h("5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e"),
                    display_name: None,
                    description: None,
                    min_disk_mib: None,
                    deprecated: false,
                },
            );
            let mut firmwares = BTreeMap::new();
            firmwares.insert(
                "ovmf-default".to_string(),
                ImageEntry {
                    hash: h("ba5bb13f3abca960b101a759be162b229e2b7e93ecad9d1307e54de887f177ff"),
                    display_name: None,
                    description: None,
                    deprecated: false,
                },
            );
            VmImagesData {
                rootfs,
                runtimes: BTreeMap::new(),
                firmwares,
                defaults: VmImageDefaults {
                    rootfs: None,
                    firmware: Some("ovmf-default".to_string()),
                    runtime: None,
                },
            }
        }

        #[test]
        fn resolves_preset_rootfs() {
            let data = fake_data();
            let r =
                resolve_image_refs(ImageRef::Preset("ubuntu24".to_string()), false, None, &data)
                    .unwrap();
            assert_eq!(
                r.rootfs.to_string(),
                "5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e"
            );
            assert!(r.confidential_firmware.is_none());
        }

        #[test]
        fn passes_through_hash_rootfs() {
            let data = VmImagesData::default();
            let raw = h("1111111111111111111111111111111111111111111111111111111111111111");
            let r = resolve_image_refs(ImageRef::Hash(raw.clone()), false, None, &data).unwrap();
            assert_eq!(r.rootfs.to_string(), raw.to_string());
        }

        #[test]
        fn confidential_uses_default_firmware_when_omitted() {
            let data = fake_data();
            let r = resolve_image_refs(ImageRef::Preset("ubuntu24".to_string()), true, None, &data)
                .unwrap();
            let fw = r.confidential_firmware.expect("firmware should resolve");
            assert_eq!(
                fw.to_string(),
                "ba5bb13f3abca960b101a759be162b229e2b7e93ecad9d1307e54de887f177ff"
            );
        }

        #[test]
        fn confidential_with_no_default_errors() {
            let mut data = fake_data();
            data.defaults.firmware = None;
            let err =
                resolve_image_refs(ImageRef::Preset("ubuntu24".to_string()), true, None, &data)
                    .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("no default firmware"), "msg={msg}");
        }

        #[test]
        fn confidential_with_explicit_hash() {
            let data = VmImagesData::default();
            let rootfs = h("1111111111111111111111111111111111111111111111111111111111111111");
            let firmware = h("2222222222222222222222222222222222222222222222222222222222222222");
            let r = resolve_image_refs(
                ImageRef::Hash(rootfs.clone()),
                true,
                Some(ImageRef::Hash(firmware.clone())),
                &data,
            )
            .unwrap();
            assert_eq!(
                r.confidential_firmware.unwrap().to_string(),
                firmware.to_string()
            );
        }

        #[test]
        fn unknown_preset_lists_available() {
            let data = fake_data();
            let err = resolve_image_refs(ImageRef::Preset("nope".to_string()), false, None, &data)
                .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("ubuntu24"), "msg={msg}");
        }

        #[test]
        fn non_confidential_ignores_firmware_arg() {
            let data = VmImagesData::default();
            let rootfs = h("1111111111111111111111111111111111111111111111111111111111111111");
            let firmware = h("2222222222222222222222222222222222222222222222222222222222222222");
            let r = resolve_image_refs(
                ImageRef::Hash(rootfs),
                false,
                Some(ImageRef::Hash(firmware)),
                &data,
            )
            .unwrap();
            assert!(r.confidential_firmware.is_none());
        }
    }

    mod resolve_runtime_ref_tests {
        use super::super::resolve_runtime_ref;
        use crate::cli::ImageRef;
        use aleph_sdk::aggregate_models::vm_images::{ImageEntry, VmImageDefaults, VmImagesData};
        use aleph_types::item_hash::ItemHash;
        use std::collections::BTreeMap;

        const PY312_HASH: &str = "63f07193e6ee9d207b7d1fcf8286f9aee34e6f12f101d2ec77c1229f92964696";

        fn h(hex: &str) -> ItemHash {
            ItemHash::try_from(hex).unwrap()
        }

        fn data_with_python312() -> VmImagesData {
            let mut runtimes = BTreeMap::new();
            runtimes.insert(
                "python312".to_string(),
                ImageEntry {
                    hash: h(PY312_HASH),
                    display_name: None,
                    description: None,
                    deprecated: false,
                },
            );
            VmImagesData {
                rootfs: BTreeMap::new(),
                runtimes,
                firmwares: BTreeMap::new(),
                defaults: VmImageDefaults {
                    rootfs: None,
                    firmware: None,
                    runtime: Some("python312".to_string()),
                },
            }
        }

        #[test]
        fn none_uses_default_runtime() {
            let r = resolve_runtime_ref(None, &data_with_python312()).unwrap();
            assert_eq!(r.to_string(), PY312_HASH);
        }

        #[test]
        fn none_without_default_errors() {
            let mut data = data_with_python312();
            data.defaults.runtime = None;
            let err = resolve_runtime_ref(None, &data).unwrap_err();
            assert!(err.to_string().contains("no default runtime"));
        }

        #[test]
        fn preset_resolves_active_entry() {
            let r = resolve_runtime_ref(
                Some(ImageRef::Preset("python312".to_string())),
                &data_with_python312(),
            )
            .unwrap();
            assert_eq!(r.to_string(), PY312_HASH);
        }

        #[test]
        fn unknown_preset_lists_available() {
            let err = resolve_runtime_ref(
                Some(ImageRef::Preset("nope".to_string())),
                &data_with_python312(),
            )
            .unwrap_err();
            assert!(err.to_string().contains("python312"));
        }

        #[test]
        fn hash_passes_through_without_aggregate() {
            let raw = h("1111111111111111111111111111111111111111111111111111111111111111");
            let r =
                resolve_runtime_ref(Some(ImageRef::Hash(raw.clone())), &VmImagesData::default())
                    .unwrap();
            assert_eq!(r.to_string(), raw.to_string());
        }
    }

    mod gpu_sizing {
        use super::super::resolve_gpu_compute_units;
        use aleph_sdk::aggregate_models::pricing::{
            ComputeUnitSpec, Price, PricingPerEntity, Tier,
        };
        use std::collections::HashMap;

        /// GPU pricing entity: 1 CU = 1 vcpu / 6144 MiB / 61440 MiB disk, with a
        /// GPU tier at 3 CU (min) plus larger plain tiers at 4 and 8 CU.
        fn gpu_pricing() -> PricingPerEntity {
            PricingPerEntity {
                compute_unit: ComputeUnitSpec {
                    vcpus: 1,
                    memory_mib: 6144,
                    disk_mib: 61440,
                },
                tiers: vec![
                    Tier {
                        id: "gpu".into(),
                        compute_units: 3,
                        model: Some("RTX 4000 ADA".into()),
                        vram: Some(20480),
                    },
                    Tier {
                        id: "t4".into(),
                        compute_units: 4,
                        model: None,
                        vram: None,
                    },
                    Tier {
                        id: "t8".into(),
                        compute_units: 8,
                        model: None,
                        vram: None,
                    },
                ],
                price: HashMap::from([(
                    "compute_unit".to_string(),
                    Price {
                        payg: None,
                        holding: None,
                        credit: "0.28".to_string(),
                    },
                )]),
            }
        }

        #[test]
        fn defaults_to_minimum() {
            let p = gpu_pricing();
            let cu = resolve_gpu_compute_units(&p, 3, "rtx-4000-ada", None, None, None).unwrap();
            assert_eq!(cu, 3);
        }

        #[test]
        fn raise_via_size() {
            let p = gpu_pricing();
            // 8vcpu-48gb -> 8 CU at this cu spec
            let cu =
                resolve_gpu_compute_units(&p, 3, "rtx-4000-ada", Some("8vcpu-48gb"), None, None)
                    .unwrap();
            assert_eq!(cu, 8);
        }

        #[test]
        fn raise_via_size_accepts_multiple_without_matching_tier() {
            let p = gpu_pricing();
            // 5 CU has no enumerated tier (tiers are 3, 4, 8) yet is a valid GPU
            // size: 5 vcpu + 30 GiB RAM at this 1vcpu/6gb-per-CU definition.
            let cu =
                resolve_gpu_compute_units(&p, 3, "rtx-4000-ada", Some("5vcpu-30gb"), None, None)
                    .unwrap();
            assert_eq!(cu, 5);
        }

        #[test]
        fn size_below_minimum_errors() {
            let p = gpu_pricing();
            // 1vcpu-6gb is a valid 1-CU size but below the 3-CU GPU minimum.
            let err =
                resolve_gpu_compute_units(&p, 3, "rtx-4000-ada", Some("1vcpu-6gb"), None, None)
                    .unwrap_err()
                    .to_string();
            assert!(err.contains("below the minimum"), "{err}");
        }

        #[test]
        fn malformed_or_mismatched_size_errors() {
            let p = gpu_pricing();
            // Memory does not match the CU definition (4 CU is 24gb, not 8gb).
            let err =
                resolve_gpu_compute_units(&p, 3, "rtx-4000-ada", Some("4vcpu-8gb"), None, None)
                    .unwrap_err()
                    .to_string();
            assert!(
                err.contains("invalid size '4vcpu-8gb' for GPU 'rtx-4000-ada'"),
                "{err}"
            );
        }

        #[test]
        fn raise_via_vcpus() {
            let p = gpu_pricing();
            // 6 vcpus -> 6 CU (div_ceil against 1 vcpu/CU)
            let cu = resolve_gpu_compute_units(&p, 3, "rtx-4000-ada", None, Some(6), None).unwrap();
            assert_eq!(cu, 6);
        }

        #[test]
        fn raise_via_memory() {
            let p = gpu_pricing();
            // 30720 MiB / 6144 MiB = 5 CU
            let cu =
                resolve_gpu_compute_units(&p, 3, "rtx-4000-ada", None, None, Some(30720)).unwrap();
            assert_eq!(cu, 5);
        }

        #[test]
        fn vcpus_below_minimum_errors() {
            let p = gpu_pricing();
            // 2 vcpus -> 2 CU, below the 3 CU GPU minimum.
            let err = resolve_gpu_compute_units(&p, 3, "rtx-4000-ada", None, Some(2), None)
                .unwrap_err()
                .to_string();
            assert!(err.contains("below the minimum"), "{err}");
            assert!(err.contains("rtx-4000-ada"), "{err}");
        }
    }

    #[test]
    fn resolve_keys_fallback_to_all_registered() {
        let out = resolve_instance_ssh_keys(
            vec![],
            vec![],
            false,
            vec!["ssh-ed25519 A".into(), "ssh-ed25519 B".into()],
        )
        .unwrap();
        assert_eq!(out, vec!["ssh-ed25519 A", "ssh-ed25519 B"]);
    }

    #[test]
    fn resolve_keys_union_and_dedupe() {
        let out = resolve_instance_ssh_keys(
            vec!["ssh-ed25519 A".into(), "ssh-ed25519 B".into()],
            vec!["ssh-ed25519 B".into(), "ssh-ed25519 C".into()],
            true,
            vec!["ssh-ed25519 Z".into()],
        )
        .unwrap();
        assert_eq!(out, vec!["ssh-ed25519 A", "ssh-ed25519 B", "ssh-ed25519 C"]);
    }

    #[test]
    fn resolve_keys_empty_is_error() {
        assert!(resolve_instance_ssh_keys(vec![], vec![], false, vec![]).is_err());
    }

    #[test]
    fn select_by_label_resolves_and_errors() {
        use aleph_sdk::ssh::SshKey;
        use chrono::Utc;
        let reg = vec![SshKey {
            item_hash: "1111111111111111111111111111111111111111111111111111111111111111"
                .parse()
                .unwrap(),
            key: "ssh-ed25519 AAAA".into(),
            label: Some("laptop".into()),
            created: Utc::now(),
        }];
        assert_eq!(
            select_keys_by_label(&["laptop".to_string()], &reg).unwrap(),
            vec!["ssh-ed25519 AAAA"]
        );
        assert!(select_keys_by_label(&["nope".to_string()], &reg).is_err());
    }

    #[cfg(test)]
    fn mk_key(label: &str, key: &str) -> aleph_sdk::ssh::SshKey {
        use chrono::Utc;
        aleph_sdk::ssh::SshKey {
            item_hash: "1111111111111111111111111111111111111111111111111111111111111111"
                .parse()
                .unwrap(),
            key: key.into(),
            label: Some(label.into()),
            created: Utc::now(),
        }
    }

    #[test]
    fn merge_registries_signer_overrides_owner_on_collision() {
        let owner = vec![
            mk_key("shared", "owner-shared"),
            mk_key("owneronly", "owner-x"),
        ];
        let sender = vec![
            mk_key("shared", "sender-shared"),
            mk_key("senderonly", "sender-y"),
        ];
        let merged = merge_ssh_registries(&owner, &sender);
        // Shared label resolves to the signer's key; both unique labels resolve.
        assert_eq!(
            select_keys_by_label(&["shared".into()], &merged).unwrap(),
            vec!["sender-shared"]
        );
        assert_eq!(
            select_keys_by_label(&["owneronly".into()], &merged).unwrap(),
            vec!["owner-x"]
        );
        assert_eq!(
            select_keys_by_label(&["senderonly".into()], &merged).unwrap(),
            vec!["sender-y"]
        );
    }

    #[test]
    fn merge_registries_empty_sender_is_owner_only() {
        let owner = vec![mk_key("laptop", "owner-laptop")];
        let merged = merge_ssh_registries(&owner, &[]);
        assert_eq!(
            select_keys_by_label(&["laptop".into()], &merged).unwrap(),
            vec!["owner-laptop"]
        );
    }
}
