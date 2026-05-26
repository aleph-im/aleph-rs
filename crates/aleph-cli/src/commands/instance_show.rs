//! `aleph instance show` - detail view for a single VM.
//!
//! Default view aggregates the CCN INSTANCE message and scheduler placement.
//! Passing `--verbose` additionally fetches live CRN networking and the
//! owner's port-forwarding aggregate.

use crate::cli::InstanceShowArgs;
use aleph_sdk::client::AlephClient;
use aleph_types::chain::{Address, Chain};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::base::{Payment, PaymentType};
use aleph_types::message::execution::environment::{
    GpuProperties, Hypervisor, TrustedExecutionEnvironment,
};
use aleph_types::message::execution::volume::{MachineVolume, VolumePersistence};
use aleph_types::message::{Message, MessageContentEnum};
use aleph_types::timestamp::Timestamp;
use std::collections::BTreeMap;
use url::Url;

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
    pub host: u16,
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
    writeln!(out, "  Created        {}", format_ts(&s.identity.created_at)).unwrap();
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
        .map(|g| format!("{} {} ({}, {})", g.vendor, g.device_name, g.device_id, g.device_class))
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

pub async fn handle_instance_show(
    _aleph_client: &AlephClient,
    _scheduler_url: Url,
    _json: bool,
    _args: InstanceShowArgs,
) -> anyhow::Result<()> {
    anyhow::bail!("not yet implemented")
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
        let raw = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGGqxlNwZh0RTk4UpAQ4XBQjPpswxqDjW7Lu8fThIzNd";
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
        let raw = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGGqxlNwZh0RTk4UpAQ4XBQjPpswxqDjW7Lu8fThIzNd";
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
            host: 24221,
            proto: None,
        }]);
        let v = render_json(&show);
        let obj = v.as_object().expect("top level is object");
        assert!(obj.contains_key("networking"));
        assert!(obj.contains_key("mapped_ports"));
        assert!(obj.contains_key("port_forwards"));
        assert_eq!(
            obj["networking"]["ipv6"].as_str(),
            Some("fc00::1/64")
        );
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
}
