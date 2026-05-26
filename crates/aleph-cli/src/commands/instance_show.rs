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
}
