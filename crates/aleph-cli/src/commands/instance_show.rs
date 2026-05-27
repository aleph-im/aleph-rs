//! `aleph instance show` - detail view for a single VM.
//!
//! Default view aggregates the CCN INSTANCE message and scheduler placement.
//! Passing `--verbose` additionally fetches live CRN networking and the
//! owner's port-forwarding aggregate.

use crate::cli::InstanceShowArgs;
use aleph_sdk::client::{AlephAggregateClient, AlephClient, AlephMessageClient, MessageWithStatus};
use aleph_sdk::crn::fetch_active_vms;
use aleph_sdk::scheduler::SchedulerClient;
use aleph_types::chain::{Address, Chain};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::base::{Payment, PaymentType};
use aleph_types::message::execution::environment::{
    GpuProperties, Hypervisor, TrustedExecutionEnvironment,
};
use aleph_types::message::execution::volume::{MachineVolume, VolumePersistence};
use aleph_types::message::{Message, MessageContentEnum, MessageType};
use aleph_types::timestamp::Timestamp;
use anyhow::{Context, Result, anyhow, bail};
use std::collections::BTreeMap;
use url::Url;

use crate::commands::instance_target::pick_unique_match;

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct InstanceShow {
    pub identity: Identity,
    pub payment: Option<PaymentInfo>,
    pub resources: Resources,
    pub image: Image,
    pub volumes: Vec<Volume>,
    pub ssh_keys: Vec<SshKey>,
    pub pinning: Option<String>,
    pub placement: Placement,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub networking: Option<Networking>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapped_ports: Option<BTreeMap<u16, u16>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port_forwards: Option<Vec<PortForward>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Identity {
    pub item_hash: ItemHash,
    pub name: Option<String>,
    #[serde(serialize_with = "serialize_ts_as_rfc3339")]
    pub created_at: Timestamp,
    pub owner: Address,
    pub sender: Address,
    pub channel: Option<Channel>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct PaymentInfo {
    #[serde(rename = "type")]
    pub kind: String, // "hold" | "superfluid" | "credit"
    pub chain: Option<Chain>,
    pub receiver: Option<Address>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Resources {
    pub vcpus: u32,
    pub memory_mib: u64,
    pub hypervisor: Option<String>,
    pub gpus: Vec<GpuSummary>,
    pub trusted_execution: Option<TrustedExecutionSummary>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct GpuSummary {
    pub vendor: String,
    pub device_name: String,
    pub device_id: String,
    pub device_class: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct TrustedExecutionSummary {
    pub firmware: Option<String>,
    pub policy: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Image {
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    pub use_latest: bool,
    pub persistence: String,
    pub size_mib: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Volume {
    Persistent {
        name: Option<String>,
        mount: Option<String>,
        size_mib: u64,
        persistence: String,
    },
    Ephemeral {
        mount: Option<String>,
        size_mib: u64,
    },
    Immutable {
        mount: Option<String>,
        reference: ItemHash,
        use_latest: bool,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct SshKey {
    pub fingerprint: String, // "SHA256:..." or "<unparseable>"
    pub algo: String,
    pub comment: Option<String>,
    pub raw: String,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
pub(crate) struct Placement {
    pub status: Option<String>,
    pub allocated_node: Option<String>,
    pub scheduling_status: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Networking {
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct PortForward {
    pub vm_port: u16,
    /// Host-side mapped port, cross-referenced from the CRN's `mapped_ports`.
    /// `None` when the aggregate exposes this VM port but the CRN reports no
    /// host mapping for it (rendered as `-`, serialized as `null`).
    pub host: Option<u16>,
    pub proto: Option<String>,
}

fn serialize_ts_as_rfc3339<S>(ts: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let dt = ts.to_datetime().map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&dt.to_rfc3339())
}

/// Build the core `InstanceShow` (identity, payment, resources, image,
/// volumes, ssh keys, pinning) from a CCN INSTANCE message. Placement and
/// the three verbose-only sections are left empty for callers to fill in
/// from the scheduler / CRN / aggregate.
pub(crate) fn build_from_message(message: &Message) -> anyhow::Result<InstanceShow> {
    let MessageContentEnum::Instance(content) = message.content() else {
        anyhow::bail!(
            "item {} is not an INSTANCE message (got {:?})",
            message.item_hash,
            message.message_type
        );
    };

    let name = content
        .base
        .metadata
        .as_ref()
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let identity = Identity {
        item_hash: message.item_hash.clone(),
        name,
        created_at: message.content.time.clone(),
        owner: message.owner().clone(),
        sender: message.sender.clone(),
        channel: message.channel.clone(),
    };

    let payment = content.base.payment.as_ref().map(payment_info);

    let resources = Resources {
        vcpus: content.base.resources.vcpus,
        memory_mib: u64::from(content.base.resources.memory),
        hypervisor: content.environment.hypervisor.as_ref().map(hypervisor_str),
        gpus: content
            .base
            .requirements
            .as_ref()
            .and_then(|r| r.gpu.as_ref())
            .map(|gpus| gpus.iter().map(gpu_summary).collect())
            .unwrap_or_default(),
        trusted_execution: content
            .environment
            .trusted_execution
            .as_ref()
            .map(tee_summary),
    };

    let image = Image {
        reference: content.rootfs.parent.reference.clone(),
        use_latest: content.rootfs.parent.use_latest,
        persistence: persistence_str(&content.rootfs.persistence).to_string(),
        size_mib: u64::from(content.rootfs.size_mib),
    };

    let volumes = content.base.volumes.iter().map(volume_summary).collect();

    let ssh_keys = content
        .base
        .authorized_keys
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|raw| parse_ssh_key(raw))
        .collect();

    let pinning = content
        .base
        .requirements
        .as_ref()
        .and_then(|r| r.node.as_ref())
        .and_then(|n| n.node_hash.clone());

    Ok(InstanceShow {
        identity,
        payment,
        resources,
        image,
        volumes,
        ssh_keys,
        pinning,
        placement: Placement::default(),
        networking: None,
        mapped_ports: None,
        port_forwards: None,
    })
}

fn payment_info(p: &Payment) -> PaymentInfo {
    PaymentInfo {
        kind: match p.payment_type {
            PaymentType::Hold => "hold".into(),
            PaymentType::Superfluid => "superfluid".into(),
            PaymentType::Credit => "credit".into(),
        },
        chain: p.chain.clone(),
        receiver: p.receiver.clone(),
    }
}

fn hypervisor_str(h: &Hypervisor) -> String {
    match h {
        Hypervisor::Firecracker => "firecracker".into(),
        Hypervisor::Qemu => "qemu".into(),
    }
}

fn persistence_str(p: &VolumePersistence) -> &'static str {
    match p {
        VolumePersistence::Host => "host",
        VolumePersistence::Store => "store",
    }
}

fn gpu_summary(g: &GpuProperties) -> GpuSummary {
    GpuSummary {
        vendor: g.vendor.clone(),
        device_name: g.device_name.clone(),
        device_id: g.device_id.clone(),
        device_class: format!("{:?}", g.device_class),
    }
}

fn tee_summary(t: &TrustedExecutionEnvironment) -> TrustedExecutionSummary {
    TrustedExecutionSummary {
        firmware: t.firmware.as_ref().map(|h| h.to_string()),
        policy: t.policy,
    }
}

fn volume_summary(v: &MachineVolume) -> Volume {
    match v {
        MachineVolume::Persistent(p) => Volume::Persistent {
            name: p.name.clone(),
            mount: p
                .base
                .mount
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
            size_mib: u64::from(p.size_mib),
            persistence: p
                .persistence
                .as_ref()
                .map(persistence_str)
                .unwrap_or("host")
                .to_string(),
        },
        MachineVolume::Ephemeral(e) => Volume::Ephemeral {
            mount: e
                .base
                .mount
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
            size_mib: u64::from(e.size_mib),
        },
        MachineVolume::Immutable(i) => Volume::Immutable {
            mount: i
                .base
                .mount
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
            reference: i.reference.clone(),
            use_latest: i.use_latest,
        },
    }
}

/// Parse an OpenSSH public key string into an `SshKey`.
///
/// Fingerprint format: `SHA256:<base64-no-padding(sha256(key_blob))>` where
/// `key_blob` is the base64-decoded middle field of the public-key line.
/// Returns `"<unparseable>"` for the fingerprint when the line is malformed or
/// the blob field is not valid standard base64.
fn parse_ssh_key(raw: &str) -> SshKey {
    use base64::Engine;
    use sha2::{Digest, Sha256};

    let mut parts = raw.split_whitespace();
    let algo = parts.next().unwrap_or("").to_string();
    let blob_b64 = parts.next();
    let comment = parts.collect::<Vec<_>>().join(" ");
    let comment = (!comment.is_empty()).then_some(comment);

    let fingerprint = match blob_b64 {
        Some(b64) if !algo.is_empty() => {
            match base64::engine::general_purpose::STANDARD.decode(b64) {
                Ok(blob) => {
                    let digest = Sha256::digest(&blob);
                    let b64 = base64::engine::general_purpose::STANDARD_NO_PAD.encode(digest);
                    format!("SHA256:{b64}")
                }
                Err(_) => "<unparseable>".to_string(),
            }
        }
        _ => "<unparseable>".to_string(),
    };

    SshKey {
        fingerprint,
        algo,
        comment,
        raw: raw.to_string(),
    }
}

const MISSING: &str = "-";

pub(crate) fn render_text(s: &InstanceShow) -> String {
    use std::fmt::Write;

    let mut out = String::new();

    // Header + identity block
    writeln!(out, "INSTANCE {}", s.identity.item_hash).unwrap();
    if let Some(name) = s.identity.name.as_deref() {
        writeln!(out, "  Name           {name}").unwrap();
    }
    writeln!(
        out,
        "  Created        {}",
        format_ts(&s.identity.created_at)
    )
    .unwrap();
    writeln!(out, "  Owner          {}", s.identity.owner).unwrap();
    if s.identity.sender != s.identity.owner {
        writeln!(out, "  Sender         {}", s.identity.sender).unwrap();
    }
    if let Some(c) = &s.identity.channel {
        // Channel serializes to a bare string; pull it out via JSON.
        let channel_str = serde_json::to_value(c)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();
        writeln!(out, "  Channel        {channel_str}").unwrap();
    }
    if let Some(p) = &s.payment {
        writeln!(out, "  Payment        {}", format_payment(p)).unwrap();
    }

    // Resources
    writeln!(out).unwrap();
    writeln!(out, "RESOURCES").unwrap();
    writeln!(
        out,
        "  Compute        {} vCPU, {} MiB RAM, {} MiB disk",
        s.resources.vcpus, s.resources.memory_mib, s.image.size_mib
    )
    .unwrap();
    writeln!(
        out,
        "  Hypervisor     {}",
        s.resources.hypervisor.as_deref().unwrap_or(MISSING)
    )
    .unwrap();
    writeln!(out, "  GPU            {}", format_gpus(&s.resources.gpus)).unwrap();
    writeln!(
        out,
        "  Trusted        {}",
        format_tee(s.resources.trusted_execution.as_ref())
    )
    .unwrap();

    // Image
    writeln!(out).unwrap();
    writeln!(out, "IMAGE").unwrap();
    writeln!(out, "  Hash           {}", s.image.reference).unwrap();
    writeln!(out, "  Persistence    {}", s.image.persistence).unwrap();
    writeln!(out, "  Size           {} MiB", s.image.size_mib).unwrap();

    // Volumes
    writeln!(out).unwrap();
    writeln!(out, "VOLUMES ({})", s.volumes.len()).unwrap();
    for v in &s.volumes {
        writeln!(out, "  {}", format_volume(v)).unwrap();
    }

    // SSH keys
    writeln!(out).unwrap();
    writeln!(out, "SSH KEYS ({})", s.ssh_keys.len()).unwrap();
    for k in &s.ssh_keys {
        let comment = k.comment.as_deref().unwrap_or("");
        writeln!(out, "  {} ({}) {}", k.fingerprint, k.algo, comment).unwrap();
    }

    // Placement
    writeln!(out).unwrap();
    writeln!(out, "PLACEMENT").unwrap();
    writeln!(
        out,
        "  Pinned         {}",
        s.pinning.as_deref().unwrap_or(MISSING)
    )
    .unwrap();
    writeln!(
        out,
        "  Status         {}",
        s.placement.status.as_deref().unwrap_or(MISSING)
    )
    .unwrap();
    writeln!(
        out,
        "  Allocated      {}",
        s.placement.allocated_node.as_deref().unwrap_or(MISSING)
    )
    .unwrap();

    if let Some(net) = &s.networking {
        writeln!(out).unwrap();
        writeln!(out, "NETWORKING").unwrap();
        writeln!(
            out,
            "  IPv6           {}",
            net.ipv6.as_deref().unwrap_or(MISSING)
        )
        .unwrap();
        writeln!(
            out,
            "  IPv4           {}",
            net.ipv4.as_deref().unwrap_or(MISSING)
        )
        .unwrap();
    }

    if let Some(mapped) = &s.mapped_ports {
        writeln!(out).unwrap();
        writeln!(out, "MAPPED PORTS").unwrap();
        if mapped.is_empty() {
            writeln!(out, "  {MISSING}").unwrap();
        } else {
            for (vm_port, host_port) in mapped {
                writeln!(out, "  {vm_port:<3} -> {host_port}").unwrap();
            }
        }
    }

    if let Some(forwards) = &s.port_forwards {
        writeln!(out).unwrap();
        writeln!(out, "PORT FORWARDS (aggregate)").unwrap();
        if forwards.is_empty() {
            writeln!(out, "  {MISSING}").unwrap();
        } else {
            for pf in forwards {
                let proto = pf.proto.as_deref().unwrap_or("tcp");
                let host = pf
                    .host
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| MISSING.into());
                writeln!(out, "  {}/{proto}  -> [host={host}]", pf.vm_port).unwrap();
            }
        }
    }

    out
}

fn format_ts(ts: &Timestamp) -> String {
    ts.to_datetime()
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|_| format!("{}", ts.as_f64()))
}

fn format_payment(p: &PaymentInfo) -> String {
    let chain = p
        .chain
        .as_ref()
        .map(|c| c.to_string())
        .unwrap_or_else(|| MISSING.into());
    match (p.kind.as_str(), p.receiver.as_ref()) {
        ("superfluid", Some(r)) => format!("superfluid ({chain} -> {r})"),
        _ => format!("{} ({chain})", p.kind),
    }
}

fn format_gpus(gpus: &[GpuSummary]) -> String {
    if gpus.is_empty() {
        return MISSING.into();
    }
    gpus.iter()
        .map(|g| {
            format!(
                "{} {} ({}, {})",
                g.vendor, g.device_name, g.device_id, g.device_class
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn format_tee(tee: Option<&TrustedExecutionSummary>) -> String {
    match tee {
        None => MISSING.into(),
        Some(t) => format!(
            "{} policy=0x{:x}",
            t.firmware.as_deref().unwrap_or("default-firmware"),
            t.policy
        ),
    }
}

fn format_volume(v: &Volume) -> String {
    match v {
        Volume::Persistent {
            name,
            mount,
            size_mib,
            persistence,
        } => {
            let name = name.as_deref().unwrap_or(MISSING);
            let mount = mount.as_deref().unwrap_or(MISSING);
            format!("{name:<12} persistent {size_mib} MiB mount={mount} persistence={persistence}")
        }
        Volume::Ephemeral { mount, size_mib } => {
            let mount = mount.as_deref().unwrap_or(MISSING);
            format!("{:<12} ephemeral  {size_mib} MiB mount={mount}", MISSING)
        }
        Volume::Immutable {
            mount,
            reference,
            use_latest,
        } => {
            let mount = mount.as_deref().unwrap_or(MISSING);
            format!(
                "{:<12} immutable  ref={reference} use_latest={use_latest} mount={mount}",
                MISSING
            )
        }
    }
}

pub(crate) fn render_json(s: &InstanceShow) -> serde_json::Value {
    serde_json::to_value(s).expect("InstanceShow always serializes")
}

async fn populate_verbose(
    show: &mut InstanceShow,
    scheduler: &SchedulerClient,
    aleph_client: &AlephClient,
) {
    // --- CRN networking + mapped ports ---
    if let Some(node_hash) = show.placement.allocated_node.as_deref() {
        match scheduler.get_node(node_hash).await {
            Ok(Some(node)) => {
                if let Some(addr) = node.address.as_deref() {
                    match Url::parse(addr) {
                        Ok(crn_url) => {
                            let http = reqwest::Client::new();
                            match fetch_active_vms(&http, &crn_url).await {
                                Ok(list) => {
                                    if let Some(entry) = list.0.get(&show.identity.item_hash)
                                        && let Some(net) = entry.networking.as_ref()
                                    {
                                        show.networking = Some(Networking {
                                            ipv4: net.ipv4.clone(),
                                            ipv6: net.ipv6.clone(),
                                        });
                                        let mapped: BTreeMap<u16, u16> = net
                                            .mapped_ports
                                            .iter()
                                            .map(|(k, v)| (*k, v.host))
                                            .collect();
                                        show.mapped_ports = Some(mapped);
                                    }
                                }
                                Err(e) => eprintln!(
                                    "warning: CRN {crn_url} unreachable, \
                                     networking/mapped ports unavailable: {e}"
                                ),
                            }
                        }
                        Err(e) => {
                            eprintln!("warning: invalid CRN address `{addr}` from scheduler: {e}")
                        }
                    }
                } else {
                    eprintln!(
                        "warning: scheduler knows node {node_hash} but has no reachable \
                         address; networking unavailable"
                    );
                }
            }
            Ok(None) => eprintln!(
                "warning: scheduler has no record of node {node_hash}; networking unavailable"
            ),
            Err(e) => eprintln!("warning: scheduler unreachable for node {node_hash}: {e}"),
        }
    }

    // --- Port-forwarding aggregate ---
    match aleph_client
        .get_port_forwarding_aggregate(&show.identity.owner)
        .await
    {
        Ok(agg) => {
            let mut forwards: Vec<PortForward> = Vec::new();
            for (vm_id, ports_opt) in agg.iter() {
                if vm_id != &show.identity.item_hash {
                    continue;
                }
                let Some(ports) = ports_opt else { continue };
                for (vm_port, flags) in ports.ports.iter() {
                    let host = show
                        .mapped_ports
                        .as_ref()
                        .and_then(|m| m.get(vm_port))
                        .copied();
                    forwards.push(PortForward {
                        vm_port: *vm_port,
                        host,
                        proto: format_proto(flags),
                    });
                }
            }
            forwards.sort_by_key(|p| p.vm_port);
            show.port_forwards = Some(forwards);
        }
        Err(e) => eprintln!(
            "warning: port-forwarding aggregate unavailable for {}: {e}",
            show.identity.owner
        ),
    }
}

/// Render a port's protocol flags as a compact string. Returns `None` when no
/// flags are set (default behaviour, typically tcp+udp depending on CRN config).
fn format_proto(flags: &aleph_sdk::aggregate_models::port_forwarding::PortFlags) -> Option<String> {
    let mut parts = Vec::new();
    if flags.tcp {
        parts.push("tcp");
    }
    if flags.udp {
        parts.push("udp");
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("+"))
    }
}

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
            bail!("instance {item_hash} is still pending; try again in a few seconds")
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

/// Build and populate an `InstanceShow` from the CCN, scheduler, and
/// optionally the CRN and port-forwarding aggregate. This is the orchestration
/// core; rendering is left to the caller.
pub(crate) async fn build_instance_show(
    aleph_client: &AlephClient,
    scheduler_url: Url,
    args: &InstanceShowArgs,
) -> Result<InstanceShow> {
    let scheduler = SchedulerClient::new(scheduler_url.clone());

    // 1. Resolve the VM. Accept full hash directly; otherwise let the
    //    scheduler expand a prefix.
    let (item_hash, entry) = if let Ok(hash) = ItemHash::try_from(args.vm_id.as_str()) {
        let entry = scheduler
            .get_vm(&hash)
            .await
            .context("querying scheduler")?
            .ok_or_else(|| anyhow!("instance {hash} not found in the scheduler"))?;
        (hash, entry)
    } else {
        let matches = scheduler
            .find_vms_by_hash_prefix(&args.vm_id)
            .await
            .with_context(|| {
                format!(
                    "looking up VMs matching prefix `{}` in the scheduler",
                    args.vm_id
                )
            })?;
        pick_unique_match(&args.vm_id, matches)?
    };

    // 2. Fetch the CCN INSTANCE message.
    let message = fetch_instance_message(aleph_client, &item_hash).await?;

    // 3. Build the core view.
    let mut show = build_from_message(&message)?;

    // 4. Populate placement from the scheduler entry we already fetched.
    show.placement = Placement {
        status: Some(entry.status.clone()),
        allocated_node: entry.allocated_node.clone(),
        scheduling_status: Some(entry.scheduling_status.clone()),
    };

    // 5. Verbose extras: CRN networking + port-forwarding aggregate.
    if args.verbose {
        populate_verbose(&mut show, &scheduler, aleph_client).await;
    }

    Ok(show)
}

pub async fn handle_instance_show(
    aleph_client: &AlephClient,
    scheduler_url: Url,
    json: bool,
    args: InstanceShowArgs,
) -> Result<()> {
    let show = build_instance_show(aleph_client, scheduler_url, &args).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&render_json(&show))?);
    } else {
        print!("{}", render_text(&show));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::message::Message;

    const INSTANCE_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/instance/instance-gpu-payg.json"
    ));

    fn fixture_message() -> Message {
        serde_json::from_str(INSTANCE_FIXTURE).expect("fixture parses")
    }

    #[test]
    fn build_from_message_populates_identity() {
        let msg = fixture_message();
        let show = build_from_message(&msg).expect("build succeeds");
        assert_eq!(
            show.identity.item_hash.to_string(),
            "a41fb91c3e68370759b72338dd1947f18e2ed883837aec5dc731d5f427f90564"
        );
        assert_eq!(show.identity.name.as_deref(), Some("gpu-l40s-2"));
        assert_eq!(
            show.identity.owner.to_string(),
            "0x238224C744F4b90b4494516e074D2676ECfC6803"
        );
        // Sender vs owner: equal in this fixture.
        assert_eq!(show.identity.sender, show.identity.owner);
    }

    #[test]
    fn build_from_message_populates_resources_and_image() {
        let msg = fixture_message();
        let show = build_from_message(&msg).expect("build succeeds");
        assert!(show.resources.vcpus >= 1);
        assert!(show.resources.memory_mib >= 512);
        assert!(show.image.size_mib >= 1024);
    }

    #[test]
    fn build_from_message_populates_pinning_when_set() {
        let msg = fixture_message();
        let show = build_from_message(&msg).expect("build succeeds");
        // The fixture pins to a specific node hash via requirements.node.
        assert_eq!(
            show.pinning.as_deref(),
            Some("dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04")
        );
    }

    #[test]
    fn build_from_message_collects_ssh_keys() {
        let msg = fixture_message();
        let show = build_from_message(&msg).expect("build succeeds");
        // The fixture includes at least one SSH key in authorized_keys.
        assert!(!show.ssh_keys.is_empty());
        // Each entry should at minimum carry the raw string.
        for k in &show.ssh_keys {
            assert!(!k.raw.is_empty());
        }
    }

    #[test]
    fn build_from_message_default_placement_is_unknown() {
        let msg = fixture_message();
        let show = build_from_message(&msg).expect("build succeeds");
        // No scheduler enrichment yet -> placement fields are None.
        assert!(show.placement.status.is_none());
        assert!(show.placement.allocated_node.is_none());
    }

    #[test]
    fn build_from_message_default_verbose_fields_are_none() {
        let msg = fixture_message();
        let show = build_from_message(&msg).expect("build succeeds");
        assert!(show.networking.is_none());
        assert!(show.mapped_ports.is_none());
        assert!(show.port_forwards.is_none());
    }

    // Well-known fingerprints. The blob is the base64-decoded second field
    // of the key line; SHA-256(blob), then base64 without padding, becomes
    // the fingerprint.

    #[test]
    fn parse_ssh_key_ed25519_with_comment() {
        // Sample ed25519 key.
        let raw = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGGqxlNwZh0RTk4UpAQ4XBQjPpswxqDjW7Lu8fThIzNd alice@example";
        let key = parse_ssh_key(raw);
        assert_eq!(key.algo, "ssh-ed25519");
        assert_eq!(key.comment.as_deref(), Some("alice@example"));
        assert!(
            key.fingerprint.starts_with("SHA256:"),
            "fingerprint must be SHA256:<base64>, got `{}`",
            key.fingerprint
        );
        // Length sanity: SHA-256 base64-no-pad is 43 chars; prefix adds 7.
        assert_eq!(key.fingerprint.len(), 7 + 43);
    }

    #[test]
    fn parse_ssh_key_without_comment() {
        let raw =
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGGqxlNwZh0RTk4UpAQ4XBQjPpswxqDjW7Lu8fThIzNd";
        let key = parse_ssh_key(raw);
        assert_eq!(key.algo, "ssh-ed25519");
        assert_eq!(key.comment, None);
        assert!(key.fingerprint.starts_with("SHA256:"));
    }

    #[test]
    fn parse_ssh_key_unparseable_returns_placeholder() {
        let raw = "not-a-valid-ssh-key";
        let key = parse_ssh_key(raw);
        assert_eq!(key.fingerprint, "<unparseable>");
        assert_eq!(key.raw, raw);
    }

    #[test]
    fn parse_ssh_key_unparseable_bad_base64() {
        let raw = "ssh-ed25519 not%valid%base64 alice";
        let key = parse_ssh_key(raw);
        assert_eq!(key.fingerprint, "<unparseable>");
    }

    #[test]
    fn parse_ssh_key_deterministic_fingerprint() {
        // The fingerprint of a fixed key blob is deterministic. We don't bake
        // the exact value (it's algorithm-dependent and not load-bearing here),
        // but two parses of the same key must agree.
        let raw =
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGGqxlNwZh0RTk4UpAQ4XBQjPpswxqDjW7Lu8fThIzNd";
        let a = parse_ssh_key(raw);
        let b = parse_ssh_key(raw);
        assert_eq!(a.fingerprint, b.fingerprint);
    }

    fn show_for_render() -> InstanceShow {
        let mut show = build_from_message(&fixture_message()).expect("build");
        // Make placement non-trivial to exercise rendering.
        show.placement = Placement {
            status: Some("dispatched".into()),
            allocated_node: Some(
                "d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77".into(),
            ),
            scheduling_status: Some("dispatched".into()),
        };
        show
    }

    #[test]
    fn render_text_default_contains_section_headers() {
        let show = show_for_render();
        let out = render_text(&show);
        assert!(out.contains("INSTANCE "));
        assert!(out.contains("RESOURCES"));
        assert!(out.contains("IMAGE"));
        assert!(out.contains("VOLUMES"));
        assert!(out.contains("SSH KEYS"));
        assert!(out.contains("PLACEMENT"));
    }

    #[test]
    fn render_text_includes_identity_lines() {
        let show = show_for_render();
        let out = render_text(&show);
        assert!(out.contains("Name           gpu-l40s-2"));
        assert!(out.contains("Owner          0x238224C744F4b90b4494516e074D2676ECfC6803"));
    }

    #[test]
    fn render_text_suppresses_sender_when_equal_to_owner() {
        let show = show_for_render();
        let out = render_text(&show);
        assert!(
            !out.contains("Sender         0x238224C744F4b90b4494516e074D2676ECfC6803"),
            "sender row should be suppressed when equal to owner"
        );
    }

    #[test]
    fn render_text_includes_sender_when_different_from_owner() {
        let mut show = show_for_render();
        show.identity.sender = Address::from("0xDIFFERENT".to_string());
        let out = render_text(&show);
        assert!(out.contains("Sender         0xDIFFERENT"));
    }

    #[test]
    fn render_text_omits_verbose_sections_by_default() {
        let show = show_for_render();
        let out = render_text(&show);
        assert!(!out.contains("NETWORKING"));
        assert!(!out.contains("MAPPED PORTS"));
        assert!(!out.contains("PORT FORWARDS"));
    }

    #[test]
    fn render_text_placement_shows_dispatched() {
        let show = show_for_render();
        let out = render_text(&show);
        assert!(out.contains("Status         dispatched"));
        assert!(out.contains("Allocated      d704be0b15e2fb"));
    }

    #[test]
    fn render_text_unallocated_renders_dashes() {
        let mut show = show_for_render();
        show.placement = Placement::default();
        let out = render_text(&show);
        assert!(out.contains("Status         -"));
        assert!(out.contains("Allocated      -"));
    }

    #[test]
    fn render_json_default_omits_verbose_keys() {
        let show = show_for_render();
        let v = render_json(&show);
        let obj = v.as_object().expect("top level is object");
        assert!(obj.contains_key("identity"));
        assert!(obj.contains_key("resources"));
        assert!(obj.contains_key("image"));
        assert!(obj.contains_key("placement"));
        // These three are absent (not null) when not fetched.
        assert!(!obj.contains_key("networking"));
        assert!(!obj.contains_key("mapped_ports"));
        assert!(!obj.contains_key("port_forwards"));
    }

    #[test]
    fn render_json_verbose_keys_present_when_set() {
        let mut show = show_for_render();
        show.networking = Some(Networking {
            ipv4: None,
            ipv6: Some("fc00::1/64".into()),
        });
        show.mapped_ports = Some(BTreeMap::from([(22u16, 24221u16)]));
        show.port_forwards = Some(vec![PortForward {
            vm_port: 22,
            host: Some(24221),
            proto: None,
        }]);
        let v = render_json(&show);
        let obj = v.as_object().expect("top level is object");
        assert!(obj.contains_key("networking"));
        assert!(obj.contains_key("mapped_ports"));
        assert!(obj.contains_key("port_forwards"));
        assert_eq!(obj["networking"]["ipv6"].as_str(), Some("fc00::1/64"));
    }

    #[test]
    fn render_json_identity_serializes_timestamp_as_rfc3339() {
        let show = show_for_render();
        let v = render_json(&show);
        let ts = v["identity"]["created_at"]
            .as_str()
            .expect("created_at is string");
        // RFC3339 always carries a `T` separator and a `Z` or `+HH:MM` suffix.
        assert!(ts.contains('T'));
        assert!(ts.ends_with('Z') || ts.contains('+') || ts.contains('-'));
    }

    // Handler-level integration tests using wiremock.

    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const VM_HASH: &str = "a41fb91c3e68370759b72338dd1947f18e2ed883837aec5dc731d5f427f90564";
    const NODE_HASH: &str = "dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04";

    fn make_show_args(vm_id: &str, verbose: bool) -> InstanceShowArgs {
        InstanceShowArgs {
            vm_id: vm_id.to_string(),
            verbose,
        }
    }

    fn vm_entry_dispatched(hash: &str, node: &str) -> serde_json::Value {
        serde_json::json!({
            "vm_hash": hash,
            "vm_type": "instance",
            "allocated_node": node,
            "status": "dispatched",
            "scheduling_status": "dispatched",
            "migration_target": null,
            "owner": null,
        })
    }

    /// Build the response envelope for `GET /api/v0/messages/{hash}`.
    /// `GetMessageResponse` uses `#[serde(flatten)]` on `MessageWithStatus`,
    /// which is `#[serde(tag = "status", rename_all = "lowercase")]`.
    /// So the wire format is `{"status": "processed", "message": <Message>}`.
    fn message_envelope(message: &Message) -> serde_json::Value {
        serde_json::json!({ "status": "processed", "message": message })
    }

    #[tokio::test]
    async fn handle_instance_show_default_renders_text() {
        let server = MockServer::start().await;
        let msg = fixture_message();

        // 1. scheduler.get_vm(hash) -> dispatched entry
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/vms/{VM_HASH}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(vm_entry_dispatched(
                    VM_HASH,
                    "dc3d1d194a990b5c54380c3c0439562fefa42f5a46807cba1c500ec3affecf04",
                )),
            )
            .mount(&server)
            .await;
        // 2. CCN get_message
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{VM_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(message_envelope(&msg)))
            .mount(&server)
            .await;

        let aleph_client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let scheduler_url = Url::parse(&server.uri()).unwrap();
        let args = InstanceShowArgs {
            vm_id: VM_HASH.to_string(),
            verbose: false,
        };
        // Capture stdout via a sub-test that asserts the call succeeds; the
        // actual textual content is exercised by render_text_* tests.
        handle_instance_show(&aleph_client, scheduler_url, false, args)
            .await
            .expect("handler succeeds");
    }

    #[tokio::test]
    async fn handle_instance_show_json_renders_successfully() {
        let server = MockServer::start().await;
        let msg = fixture_message();

        Mock::given(method("GET"))
            .and(path(format!("/api/v1/vms/{VM_HASH}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(vm_entry_dispatched(VM_HASH, NODE_HASH)),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{VM_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(message_envelope(&msg)))
            .mount(&server)
            .await;

        let aleph_client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let scheduler_url = Url::parse(&server.uri()).unwrap();
        let args = make_show_args(VM_HASH, false);
        // Exercises the json branch end to end (build -> render_json -> print);
        // the json content itself is asserted by render_json_* tests.
        handle_instance_show(&aleph_client, scheduler_url, true, args)
            .await
            .expect("json handler succeeds");
    }

    #[tokio::test]
    async fn handle_instance_show_prefix_zero_match_errors() {
        let server = MockServer::start().await;
        // Prefix path: /api/v1/vms?vm_hash=<prefix>
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .and(query_param("vm_hash", "deadbe"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "items": [],
                "pagination": {"total_items": 0},
            })))
            .mount(&server)
            .await;

        let aleph_client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let scheduler_url = Url::parse(&server.uri()).unwrap();
        let args = InstanceShowArgs {
            vm_id: "deadbe".to_string(),
            verbose: false,
        };
        let err = handle_instance_show(&aleph_client, scheduler_url, false, args)
            .await
            .expect_err("expected zero-match failure");
        let msg = err.to_string();
        assert!(msg.contains("no instance matches `deadbe`"), "got: {msg}");
    }

    #[tokio::test]
    async fn handle_instance_show_verbose_populates_networking_and_aggregate() {
        let server = MockServer::start().await;
        let msg = fixture_message();

        // 1. scheduler.get_vm
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/vms/{VM_HASH}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(vm_entry_dispatched(VM_HASH, NODE_HASH)),
            )
            .mount(&server)
            .await;
        // 2. CCN get_message
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{VM_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(message_envelope(&msg)))
            .mount(&server)
            .await;
        // 3. scheduler.get_node
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/nodes/{NODE_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "node_hash": NODE_HASH,
                "address": format!("{}/", server.uri()),
                "status": "ok",
            })))
            .mount(&server)
            .await;
        // 4. CRN /v2/about/executions/list
        Mock::given(method("GET"))
            .and(path("/v2/about/executions/list"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                VM_HASH: {
                    "networking": {
                        "ipv6": "fc00:1:2:3:1:abcd:1234:5670/124",
                        "mapped_ports": { "22": { "host": 24221 } }
                    }
                }
            })))
            .mount(&server)
            .await;
        // 5. CCN aggregate fetch (port-forwarding). The owner address comes
        //    from the fixture: 0x238224C744F4b90b4494516e074D2676ECfC6803
        //
        //    Wire format (verified against PortForwardingAggregate):
        //      {"data": {"port-forwarding": {"<hash>": {"ports": {"<port>": {"tcp":..,"udp":..}}}}}}
        let owner_addr = "0x238224C744F4b90b4494516e074D2676ECfC6803";
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/aggregates/{owner_addr}.json")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "address": owner_addr,
                "data": {
                    "port-forwarding": {
                        VM_HASH: {
                            "ports": { "22": { "tcp": true, "udp": false } }
                        }
                    }
                }
            })))
            .mount(&server)
            .await;

        let aleph_client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let scheduler_url = Url::parse(&server.uri()).unwrap();
        let args = make_show_args(VM_HASH, true);
        let show = build_instance_show(&aleph_client, scheduler_url, &args)
            .await
            .expect("verbose handler succeeds");

        // Networking populated from CRN.
        assert_eq!(
            show.networking.as_ref().and_then(|n| n.ipv6.as_deref()),
            Some("fc00:1:2:3:1:abcd:1234:5670/124")
        );

        // Mapped ports populated.
        let mapped = show.mapped_ports.as_ref().expect("mapped_ports is Some");
        assert_eq!(mapped.get(&22), Some(&24221u16));

        // Port-forwards populated from aggregate with correct vm_port.
        let forwards = show.port_forwards.as_ref().expect("port_forwards is Some");
        assert!(!forwards.is_empty(), "port_forwards must be non-empty");
        let pf = forwards
            .iter()
            .find(|p| p.vm_port == 22)
            .expect("port 22 present");
        assert_eq!(pf.host, Some(24221));
        assert_eq!(pf.proto.as_deref(), Some("tcp"));
    }

    #[tokio::test]
    async fn handle_instance_show_verbose_degrades_when_crn_unreachable() {
        let server = MockServer::start().await;
        let msg = fixture_message();

        Mock::given(method("GET"))
            .and(path(format!("/api/v1/vms/{VM_HASH}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(vm_entry_dispatched(VM_HASH, NODE_HASH)),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/messages/{VM_HASH}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(message_envelope(&msg)))
            .mount(&server)
            .await;
        // get_node fails (500) -> handler must warn and continue
        Mock::given(method("GET"))
            .and(path(format!("/api/v1/nodes/{NODE_HASH}")))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        // aggregate still returns ok with empty agg (separate code path)
        let owner_addr = "0x238224C744F4b90b4494516e074D2676ECfC6803";
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/aggregates/{owner_addr}.json")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "address": owner_addr,
                "data": { "port-forwarding": {} }
            })))
            .mount(&server)
            .await;

        let aleph_client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let scheduler_url = Url::parse(&server.uri()).unwrap();
        let args = make_show_args(VM_HASH, true);
        // Must NOT error - degradation only.
        let show = build_instance_show(&aleph_client, scheduler_url, &args)
            .await
            .expect("handler succeeds despite CRN unreachable");

        // CRN was unreachable, so networking is None.
        assert!(
            show.networking.is_none(),
            "networking must be None when CRN unreachable"
        );
        assert!(
            show.mapped_ports.is_none(),
            "mapped_ports must stay None when CRN is unreachable"
        );

        // Aggregate was fetched successfully but was empty.
        let forwards = show.port_forwards.as_ref().expect("port_forwards is Some");
        assert!(
            forwards.is_empty(),
            "port_forwards must be empty when aggregate is empty"
        );
    }

    fn show_with_verbose_data() -> InstanceShow {
        let mut show = show_for_render();
        show.networking = Some(Networking {
            ipv4: None,
            ipv6: Some("fc00:1:2:3:1:abcd:1234:5670/124".into()),
        });
        show.mapped_ports = Some(BTreeMap::from([(22u16, 24221u16), (80u16, 24222u16)]));
        show.port_forwards = Some(vec![
            PortForward {
                vm_port: 22,
                host: Some(24221),
                proto: None,
            },
            PortForward {
                vm_port: 80,
                host: Some(24222),
                proto: Some("tcp+udp".into()),
            },
        ]);
        show
    }

    #[test]
    fn render_text_verbose_includes_three_extra_sections() {
        let show = show_with_verbose_data();
        let out = render_text(&show);
        assert!(out.contains("NETWORKING"));
        assert!(out.contains("MAPPED PORTS"));
        assert!(out.contains("PORT FORWARDS"));
    }

    #[test]
    fn render_text_verbose_networking_shows_ipv6() {
        let show = show_with_verbose_data();
        let out = render_text(&show);
        assert!(out.contains("IPv6           fc00:1:2:3:1:abcd:1234:5670/124"));
        assert!(out.contains("IPv4           -"));
    }

    #[test]
    fn render_text_verbose_mapped_ports_sorted_ascending() {
        let show = show_with_verbose_data();
        let out = render_text(&show);
        let p22 = out.find("22  -> 24221").unwrap();
        let p80 = out.find("80  -> 24222").unwrap();
        assert!(
            p22 < p80,
            "mapped ports should be sorted ascending by VM port"
        );
    }

    #[test]
    fn render_text_verbose_port_forwards_proto_column() {
        let show = show_with_verbose_data();
        let out = render_text(&show);
        assert!(
            out.contains("22/tcp"),
            "default proto column for entries without flags"
        );
        assert!(out.contains("80/tcp+udp"));
    }

    #[test]
    fn render_text_port_forward_unmapped_host_renders_dash() {
        let mut show = show_for_render();
        show.port_forwards = Some(vec![PortForward {
            vm_port: 22,
            host: None,
            proto: Some("tcp".into()),
        }]);
        let out = render_text(&show);
        assert!(
            out.contains("22/tcp  -> [host=-]"),
            "unmapped host must render as '-', not 0: {out}"
        );
    }

    #[test]
    fn render_json_port_forward_unmapped_host_is_null() {
        let mut show = show_for_render();
        show.port_forwards = Some(vec![PortForward {
            vm_port: 22,
            host: None,
            proto: Some("tcp".into()),
        }]);
        let v = render_json(&show);
        assert!(v["port_forwards"][0]["host"].is_null());
    }

    #[tokio::test]
    async fn handle_instance_show_prefix_ambiguous_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/vms"))
            .and(query_param("vm_hash", "a41"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "items": [
                    vm_entry_dispatched(
                        "a41fb91c3e68370759b72338dd1947f18e2ed883837aec5dc731d5f427f90564",
                        "node1"
                    ),
                    vm_entry_dispatched(
                        "a41ffff00000000000000000000000000000000000000000000000000000ffff",
                        "node2"
                    ),
                ],
                "pagination": {"total_items": 2},
            })))
            .mount(&server)
            .await;

        let aleph_client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let scheduler_url = Url::parse(&server.uri()).unwrap();
        let args = InstanceShowArgs {
            vm_id: "a41".to_string(),
            verbose: false,
        };
        let err = handle_instance_show(&aleph_client, scheduler_url, false, args)
            .await
            .expect_err("expected ambiguous failure");
        let msg = err.to_string();
        assert!(msg.contains("ambiguous"));
        assert!(msg.contains("matches 2 instances"));
    }
}
