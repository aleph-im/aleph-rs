//! Verifiable programs (V-Programs): auto-booting confidential VMs whose full
//! software stack is attestable via SEV-SNP runtime attestation.
//!
//! Design: aleph-vm docs/plans/2026-07-08-confidential-vm-protocol-design.md

use crate::item_hash::ItemHash;
use crate::message::execution::base::{ExecutableContent, PaymentType};
use crate::message::execution::environment::{
    DEFAULT_SNP_POLICY, LaunchMeasurement, MAX_MEASUREMENTS, TeeError, TeePlatform,
    validate_snp_policy,
};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};

/// Bounded by the kernel cmdline budget: each roothash costs ~65 bytes in the
/// measured verified_volumes= slot.
pub const MAX_VERIFIED_VOLUMES: usize = 8;

/// Upper bound on runtime/volume comment length in characters, matching
/// aleph-message's MAX_RUNTIME_COMMENT_LENGTH.
pub const MAX_RUNTIME_COMMENT_LENGTH: usize = 1024;

fn deserialize_comment<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let comment = String::deserialize(deserializer)?;
    let len = comment.chars().count();
    if len > MAX_RUNTIME_COMMENT_LENGTH {
        return Err(serde::de::Error::custom(VProgramError::CommentTooLong(len)));
    }
    Ok(comment)
}

/// dm-verity root hash as printed by veritysetup format: 64 lowercase hex chars (sha256).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct VerityRoothash(String);

impl VerityRoothash {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for VerityRoothash {
    type Error = TeeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 64 {
            return Err(TeeError::BadDigestLength {
                platform: "dm-verity",
                expected: 64,
                got: value.len(),
            });
        }
        if !value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(TeeError::DigestNotLowercaseHex);
        }
        Ok(Self(value))
    }
}

/// The measured platform: a store message holding the runtime manifest, which
/// pins the OVMF, kernel, initrd and dm-verity platform rootfs (plus its hash
/// tree) by content hash, and declares the cmdline template, boot format and
/// attestation protocols the runtime implements.
///
/// There is deliberately no use_latest: the measurements in the message pin
/// exact artifacts, so the reference must be immutable (resolved as an exact
/// item hash, never through file tags).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifiableProgramRuntime {
    /// Store message of the runtime manifest.
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    #[serde(default, deserialize_with = "deserialize_comment")]
    pub comment: String,
}

/// The user's code: a read-only ext4 volume bound into the measured TCB via
/// its dm-verity root hash on the kernel cmdline (workload_roothash=).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerifiedWorkload {
    /// Store message of the workload data image.
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    /// Store message of the dm-verity hash tree for the data image.
    pub hash_tree: ItemHash,
    /// dm-verity root hash; measured via the kernel cmdline.
    pub roothash: VerityRoothash,
}

/// An extra read-only data volume bound into the attested TCB, e.g. LLM
/// weights or datasets shared across deployments.
///
/// Volumes are positional: the measured cmdline carries the roothashes in
/// list order (verified_volumes=h1,h2,...), the guest init verity-verifies
/// device i against roothash i and exposes it at a well-known indexed path,
/// and the verity-bound workload contract maps it onward. There is
/// deliberately no mount field: an unmeasured mount mapping would let a
/// malicious host permute volumes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedVolume {
    /// Store message of the volume data image.
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    /// Store message of the dm-verity hash tree for the data image.
    pub hash_tree: ItemHash,
    /// dm-verity root hash; measured via the kernel cmdline.
    pub roothash: VerityRoothash,
    #[serde(default, deserialize_with = "deserialize_comment")]
    pub comment: String,
}

fn default_snp_policy() -> u64 {
    DEFAULT_SNP_POLICY
}

/// TEE launch configuration plus supervisor-opaque measurement annotations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawTeeVerification")]
pub struct TeeVerification {
    /// TEE attestation platform the VM launches with.
    pub backend: TeePlatform,
    /// SEV-SNP 64-bit guest policy (not SEV bit semantics).
    #[serde(default = "default_snp_policy")]
    pub policy: u64,
    /// Expected launch digests; never sent to the supervisor.
    pub measurements: Vec<LaunchMeasurement>,
}

#[derive(Deserialize)]
struct RawTeeVerification {
    backend: TeePlatform,
    #[serde(default = "default_snp_policy")]
    policy: u64,
    measurements: Vec<LaunchMeasurement>,
}

impl TryFrom<RawTeeVerification> for TeeVerification {
    type Error = TeeError;

    fn try_from(raw: RawTeeVerification) -> Result<Self, Self::Error> {
        // TeePlatform only has SevSnp today; dispatch policy validation per
        // backend when other platforms (e.g. TDX) are added.
        validate_snp_policy(raw.policy)?;
        if raw.measurements.is_empty() {
            return Err(TeeError::SnpModeRequires("measurements"));
        }
        if raw.measurements.len() > MAX_MEASUREMENTS {
            return Err(TeeError::TooManyMeasurements(raw.measurements.len()));
        }
        // Unreachable while TeePlatform has a single variant, but load-bearing
        // the day a second platform (e.g. TDX) lands: a sev_snp backend must
        // not carry tdx measurements, or vice versa.
        for measurement in &raw.measurements {
            if measurement.platform != raw.backend {
                return Err(TeeError::MeasurementPlatformMismatch {
                    expected: raw.backend.as_str(),
                    got: measurement.platform.as_str(),
                });
            }
        }
        Ok(Self {
            backend: raw.backend,
            policy: raw.policy,
            measurements: raw.measurements,
        })
    }
}

/// Execution environment flags. The hypervisor is always QEMU.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiableProgramEnvironment {
    #[serde(default)]
    pub internet: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum VProgramError {
    #[error("V-Programs are credit-only: holder-tier and PAYG stream payments are not supported")]
    CreditOnly,
    #[error("at most {MAX_VERIFIED_VOLUMES} verified volumes are supported, got {0}")]
    TooManyVerifiedVolumes(usize),
    #[error(
        "variables are not supported for V-Programs: environment variables would reach the \
         guest unmeasured; ship them in the verity-bound workload instead"
    )]
    UnmeasuredVariables,
    #[error(
        "authorized_keys are not supported for V-Programs: host key injection has no place in \
         an attested VM"
    )]
    UnmeasuredAuthorizedKeys,
    #[error(
        "V-Programs are immutable: amendment would let the measured stack change under a fixed \
         deployment identity; publish a new message instead"
    )]
    NotAmendable,
    #[error("comment must be at most {MAX_RUNTIME_COMMENT_LENGTH} characters, got {0}")]
    CommentTooLong(usize),
}

/// Message content for scheduling a verifiable program (V-Program): an
/// auto-booting SEV-SNP VM whose full software stack is attestable.
///
/// Unlike classic programs there is no code/entrypoint/triggers model (the
/// workload contract belongs to the runtime bundle) and no hypervisor choice
/// (always QEMU). Unlike instances, the rootfs is Aleph-provided and measured;
/// the user contribution is the verity-bound workload volume.
///
/// Every input reaching the guest is measured or verity-bound: extra volumes
/// must be verified (`VerifiedVolume`), and the inherited unmeasured input
/// channels (`variables`, `authorized_keys`) are rejected. Workload
/// environment variables belong in the verity-bound workload contract.
///
/// V-Programs are also immutable: the inherited amendment channel
/// (`allow_amend`, `replaces`) is rejected, because an amend would let the
/// measured stack change under a fixed deployment identity. Upgrading is an
/// explicit redeployment: publish a new message, clients re-target it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(try_from = "RawVerifiableProgramContent")]
pub struct VerifiableProgramContent {
    pub base: ExecutableContent,
    /// Properties of the execution environment.
    pub environment: VerifiableProgramEnvironment,
    /// The measured platform (runtime manifest).
    pub runtime: VerifiableProgramRuntime,
    /// The user's verity-bound workload volume.
    pub workload: VerifiedWorkload,
    /// TEE launch config and expected launch measurements.
    pub verification: TeeVerification,
    /// Extra read-only volumes, verity-bound via the measured cmdline. Only
    /// verity-bound volumes are allowed: unverified extra volumes would be
    /// attacker-controllable input inside an attested VM.
    pub volumes: Vec<VerifiedVolume>,
}

impl VerifiableProgramContent {
    /// V-Programs always run in a confidential VM.
    pub fn is_confidential(&self) -> bool {
        true
    }
}

// `base` is serialized manually (rather than via `#[serde(flatten)]`) because
// `ExecutableContent` carries its own `volumes: Vec<MachineVolume>` field,
// which would otherwise collide on the wire with the verified-volume list
// below. V-Programs never populate `base.volumes` (see
// `RawVerifiableProgramContent`), so it is always dropped here.
impl Serialize for VerifiableProgramContent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut base_value = serde_json::to_value(&self.base).map_err(serde::ser::Error::custom)?;
        let base_obj = base_value.as_object_mut().ok_or_else(|| {
            serde::ser::Error::custom("ExecutableContent did not serialize to an object")
        })?;
        base_obj.remove("volumes");

        let mut map = serializer.serialize_map(None)?;
        for (key, value) in base_obj.iter() {
            map.serialize_entry(key, value)?;
        }
        map.serialize_entry("environment", &self.environment)?;
        map.serialize_entry("runtime", &self.runtime)?;
        map.serialize_entry("workload", &self.workload)?;
        map.serialize_entry("verification", &self.verification)?;
        map.serialize_entry("volumes", &self.volumes)?;
        map.end()
    }
}

#[derive(Deserialize)]
struct RawVerifiableProgramContent {
    #[serde(flatten)]
    base: ExecutableContent,
    environment: VerifiableProgramEnvironment,
    runtime: VerifiableProgramRuntime,
    workload: VerifiedWorkload,
    verification: TeeVerification,
    #[serde(default)]
    volumes: Vec<VerifiedVolume>,
}

impl TryFrom<RawVerifiableProgramContent> for VerifiableProgramContent {
    type Error = VProgramError;

    fn try_from(raw: RawVerifiableProgramContent) -> Result<Self, Self::Error> {
        match &raw.base.payment {
            Some(payment) if payment.payment_type == PaymentType::Credit => {}
            _ => return Err(VProgramError::CreditOnly),
        }
        if raw.base.variables.as_ref().is_some_and(|v| !v.is_empty()) {
            return Err(VProgramError::UnmeasuredVariables);
        }
        if raw
            .base
            .authorized_keys
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        {
            return Err(VProgramError::UnmeasuredAuthorizedKeys);
        }
        if raw.base.allow_amend || raw.base.replaces.is_some() {
            return Err(VProgramError::NotAmendable);
        }
        if raw.volumes.len() > MAX_VERIFIED_VOLUMES {
            return Err(VProgramError::TooManyVerifiedVolumes(raw.volumes.len()));
        }
        Ok(Self {
            base: raw.base,
            environment: raw.environment,
            runtime: raw.runtime,
            workload: raw.workload,
            verification: raw.verification,
            volumes: raw.volumes,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const SNP_DIGEST: &str = "abababababababababababababababababababababababababababababababababababababababababababababababab";
    const ITEM_HASH_HEX: &str = "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe";

    #[test]
    fn test_verity_roothash_validation() {
        let ok: VerityRoothash = serde_json::from_str(&format!("\"{}\"", "cd".repeat(32))).unwrap();
        assert_eq!(ok.as_str(), "cd".repeat(32));
        for bad in ["cd".repeat(31), "ZZ".repeat(32), "zz".repeat(32)] {
            assert!(
                serde_json::from_str::<VerityRoothash>(&format!("\"{bad}\"")).is_err(),
                "{bad} should be rejected"
            );
        }
    }

    #[test]
    fn test_verifiable_program_runtime_wire_names() {
        let r: VerifiableProgramRuntime = serde_json::from_str(&format!(
            r#"{{"ref": "{ITEM_HASH_HEX}", "comment": "compose-runner snp bundle"}}"#
        ))
        .unwrap();
        assert_eq!(r.comment, "compose-runner snp bundle");
        let value = serde_json::to_value(&r).unwrap();
        assert!(value.get("ref").is_some());
        assert!(value.get("reference").is_none());
    }

    #[test]
    fn test_comment_length_capped() {
        let long = "x".repeat(MAX_RUNTIME_COMMENT_LENGTH + 1);
        let json = format!(r#"{{"ref": "{ITEM_HASH_HEX}", "comment": "{long}"}}"#);
        assert!(serde_json::from_str::<VerifiableProgramRuntime>(&json).is_err());

        let max = "x".repeat(MAX_RUNTIME_COMMENT_LENGTH);
        let json = format!(r#"{{"ref": "{ITEM_HASH_HEX}", "comment": "{max}"}}"#);
        assert!(serde_json::from_str::<VerifiableProgramRuntime>(&json).is_ok());

        // VerifiedVolume comments share the bound
        let json = format!(
            r#"{{"ref": "{ITEM_HASH_HEX}", "hash_tree": "{ITEM_HASH_HEX}",
                 "roothash": "{}", "comment": "{long}"}}"#,
            "ab".repeat(32),
        );
        assert!(serde_json::from_str::<VerifiedVolume>(&json).is_err());
    }

    #[test]
    fn test_tee_verification_policy_and_measurements() {
        let json = format!(
            r#"{{"backend": "sev_snp",
                 "measurements": [{{"platform": "sev_snp", "digest": "{SNP_DIGEST}"}}]}}"#
        );
        let v: TeeVerification = serde_json::from_str(&json).unwrap();
        assert_eq!(v.policy, 0x30000); // default when omitted

        let json = json.replace("\"backend\"", "\"policy\": 1, \"backend\"");
        assert!(serde_json::from_str::<TeeVerification>(&json).is_err()); // bit 17 unset

        let json = r#"{"backend": "sev_snp", "measurements": []}"#;
        assert!(serde_json::from_str::<TeeVerification>(json).is_err()); // min 1
    }

    #[test]
    fn test_tee_verification_backend_wire_roundtrip() {
        let json = format!(
            r#"{{"backend": "sev_snp",
                 "measurements": [{{"platform": "sev_snp", "digest": "{SNP_DIGEST}"}}]}}"#
        );
        let v: TeeVerification = serde_json::from_str(&json).unwrap();
        assert_eq!(v.backend, TeePlatform::SevSnp);

        let value = serde_json::to_value(&v).unwrap();
        assert_eq!(value["backend"], "sev_snp");

        let back: TeeVerification = serde_json::from_value(value).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn test_vprogram_environment_defaults() {
        let env: VerifiableProgramEnvironment = serde_json::from_str("{}").unwrap();
        assert!(!env.internet);
    }

    #[test]
    fn test_vprogram_environment_rejects_unknown_fields() {
        assert!(
            serde_json::from_str::<VerifiableProgramEnvironment>(r#"{"hypervisor": "qemu"}"#)
                .is_err()
        );
        // dropped legacy program flag
        assert!(
            serde_json::from_str::<VerifiableProgramEnvironment>(r#"{"aleph_api": false}"#)
                .is_err()
        );
    }

    #[test]
    fn test_verified_volume() {
        let v: VerifiedVolume = serde_json::from_str(&format!(
            r#"{{"ref": "{ITEM_HASH_HEX}", "hash_tree": "{ITEM_HASH_HEX}",
                 "roothash": "{}", "comment": "llm weights"}}"#,
            "ab".repeat(32),
        ))
        .unwrap();
        assert_eq!(v.roothash.as_str(), "ab".repeat(32));

        // bad roothash length
        assert!(
            serde_json::from_str::<VerifiedVolume>(&format!(
                r#"{{"ref": "{ITEM_HASH_HEX}", "hash_tree": "{ITEM_HASH_HEX}", "roothash": "{}"}}"#,
                "ab".repeat(31),
            ))
            .is_err()
        );
        // no mount field: binding is positional via the measured cmdline, and an
        // unmeasured mount mapping would let a malicious host permute volumes
        assert!(
            serde_json::from_str::<VerifiedVolume>(&format!(
                r#"{{"ref": "{ITEM_HASH_HEX}", "hash_tree": "{ITEM_HASH_HEX}",
                     "roothash": "{}", "mount": "/data"}}"#,
                "ab".repeat(32),
            ))
            .is_err()
        );
    }

    fn vprogram_content_json(payment: &str) -> String {
        vprogram_content_json_with(payment, "[]")
    }

    fn vprogram_content_json_with(payment: &str, volumes: &str) -> String {
        format!(
            r#"{{
                "address": "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
                "time": 1719502000.0,
                "allow_amend": false,
                "payment": {payment},
                "environment": {{"internet": true}},
                "resources": {{"vcpus": 2, "memory": 2048, "seconds": 30}},
                "runtime": {{"ref": "{ITEM_HASH_HEX}", "comment": "compose-runner snp bundle"}},
                "workload": {{
                    "ref": "beefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef",
                    "hash_tree": "feedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeedfeed",
                    "roothash": "{roothash}"
                }},
                "verification": {{
                    "backend": "sev_snp",
                    "policy": 196608,
                    "measurements": [
                        {{"platform": "sev_snp", "digest": "{SNP_DIGEST}", "vcpu_type": "EPYC-v4"}}
                    ]
                }},
                "volumes": {volumes}
            }}"#,
            roothash = "cd".repeat(32),
        )
    }

    #[test]
    fn test_vprogram_content_valid_and_credit_only() {
        let content: VerifiableProgramContent =
            serde_json::from_str(&vprogram_content_json(r#"{"type": "credit"}"#)).unwrap();
        assert!(content.is_confidential());
        assert!(content.volumes.is_empty());
        assert_eq!(
            content.verification.measurements[0].vcpu_type.as_deref(),
            Some("EPYC-v4")
        );

        for payment in [
            r#"{"type": "hold"}"#,
            r#"{"type": "superfluid", "chain": "AVAX"}"#,
        ] {
            assert!(
                serde_json::from_str::<VerifiableProgramContent>(&vprogram_content_json(payment))
                    .is_err(),
                "{payment} must be rejected"
            );
        }
    }

    #[test]
    fn test_vprogram_content_payment_required() {
        let json = vprogram_content_json(r#"{"type": "credit"}"#)
            .replace(r#""payment": {"type": "credit"},"#, "");
        assert!(serde_json::from_str::<VerifiableProgramContent>(&json).is_err());
        let json = vprogram_content_json("null");
        assert!(serde_json::from_str::<VerifiableProgramContent>(&json).is_err());
    }

    #[test]
    fn test_vprogram_content_accepts_verified_volumes() {
        let volume = format!(
            r#"{{"ref": "{ITEM_HASH_HEX}", "hash_tree": "{ITEM_HASH_HEX}", "roothash": "{}"}}"#,
            "ab".repeat(32),
        );
        let json = vprogram_content_json_with(r#"{"type": "credit"}"#, &format!("[{volume}]"));
        let content: VerifiableProgramContent = serde_json::from_str(&json).unwrap();
        assert_eq!(content.volumes[0].roothash.as_str(), "ab".repeat(32));
    }

    #[test]
    fn test_vprogram_content_rejects_unverified_volumes() {
        // classic machine volumes are unmeasured input inside an attested VM
        for bad_volume in [
            r#"{"ephemeral": true, "mount": "/var/cache", "size_mib": 5}"#.to_string(),
            format!(r#"{{"ref": "{ITEM_HASH_HEX}", "mount": "/opt/venv", "use_latest": false}}"#),
            r#"{"persistence": "host", "name": "scratch", "mount": "/var/raw", "size_mib": 1}"#
                .to_string(),
        ] {
            let json =
                vprogram_content_json_with(r#"{"type": "credit"}"#, &format!("[{bad_volume}]"));
            assert!(
                serde_json::from_str::<VerifiableProgramContent>(&json).is_err(),
                "{bad_volume} should be rejected"
            );
        }
    }

    #[test]
    fn test_vprogram_content_caps_verified_volumes() {
        let volume = format!(
            r#"{{"ref": "{ITEM_HASH_HEX}", "hash_tree": "{ITEM_HASH_HEX}", "roothash": "{}"}}"#,
            "ab".repeat(32),
        );
        let volumes = format!("[{}]", vec![volume; 9].join(","));
        let json = vprogram_content_json_with(r#"{"type": "credit"}"#, &volumes);
        assert!(serde_json::from_str::<VerifiableProgramContent>(&json).is_err());
    }

    #[test]
    fn test_vprogram_content_rejects_unmeasured_inputs() {
        let json = vprogram_content_json(r#"{"type": "credit"}"#).replace(
            "\"volumes\": []",
            "\"volumes\": [], \"variables\": {\"VM_CUSTOM_VARIABLE\": \"SOMETHING\"}",
        );
        let err = serde_json::from_str::<VerifiableProgramContent>(&json).unwrap_err();
        assert!(err.to_string().contains("variables"));

        let json = vprogram_content_json(r#"{"type": "credit"}"#).replace(
            "\"volumes\": []",
            "\"volumes\": [], \"authorized_keys\": [\"ssh-ed25519 AAAA... user@example\"]",
        );
        let err = serde_json::from_str::<VerifiableProgramContent>(&json).unwrap_err();
        assert!(err.to_string().contains("authorized_keys"));
    }

    #[test]
    fn test_vprogram_content_rejects_amendment() {
        // an amend would let the measured stack change under a fixed
        // deployment identity; upgrades are explicit redeployments
        let json = vprogram_content_json(r#"{"type": "credit"}"#)
            .replace("\"allow_amend\": false", "\"allow_amend\": true");
        let err = serde_json::from_str::<VerifiableProgramContent>(&json).unwrap_err();
        assert!(err.to_string().contains("immutable"));

        let json = vprogram_content_json(r#"{"type": "credit"}"#).replace(
            "\"volumes\": []",
            &format!("\"volumes\": [], \"replaces\": \"{ITEM_HASH_HEX}\""),
        );
        let err = serde_json::from_str::<VerifiableProgramContent>(&json).unwrap_err();
        assert!(err.to_string().contains("immutable"));
    }

    use crate::message::MessageType;
    use crate::message::base_message::{Message, MessageContent, MessageContentEnum};
    use assert_matches::assert_matches;

    const VPROGRAM_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/vprogram/vprogram-credit.json"
    ));

    #[test]
    fn test_deserialize_vprogram_message() {
        let message: Message = serde_json::from_str(VPROGRAM_FIXTURE).unwrap();

        assert_matches!(message.message_type, MessageType::VProgram);
        let content = match message.content() {
            MessageContentEnum::VProgram(content) => content,
            other => panic!("Expected MessageContentEnum::VProgram, got {:?}", other),
        };

        assert!(!content.base.allow_amend);
        assert_eq!(content.base.resources.vcpus, 2);
        assert!(content.is_confidential());
        assert_eq!(content.runtime.comment, "compose-runner snp bundle");
        assert_eq!(content.workload.roothash.as_str(), "cd".repeat(32));
        assert_eq!(content.verification.policy, 0x30000);
        assert!(content.base.volumes.is_empty());
        assert_eq!(content.volumes.len(), 1);
        assert_eq!(content.volumes[0].comment, "model weights");
        assert!(!message.confirmed());

        message.verify_item_hash().unwrap();
    }

    #[test]
    fn test_typed_dispatch_vprogram() {
        // the verified message path dispatches by MessageType, not untagged matching
        let fixture: serde_json::Value = serde_json::from_str(VPROGRAM_FIXTURE).unwrap();
        let raw = fixture["item_content"].as_str().unwrap().as_bytes();
        let content = MessageContent::deserialize_with_type(MessageType::VProgram, raw).unwrap();
        assert_matches!(content.content, MessageContentEnum::VProgram(_));
    }

    #[test]
    fn test_typed_dispatch_rejects_non_credit_vprogram() {
        let fixture: serde_json::Value = serde_json::from_str(VPROGRAM_FIXTURE).unwrap();
        let raw = fixture["item_content"]
            .as_str()
            .unwrap()
            .replace("\"type\":\"credit\"", "\"type\":\"hold\"");
        assert!(
            MessageContent::deserialize_with_type(MessageType::VProgram, raw.as_bytes()).is_err()
        );
    }

    #[test]
    fn test_vprogram_serialize_roundtrip_no_duplicate_volumes_key() {
        let message: Message = serde_json::from_str(VPROGRAM_FIXTURE).unwrap();
        let content = match message.content() {
            MessageContentEnum::VProgram(content) => content,
            other => panic!("Expected MessageContentEnum::VProgram, got {:?}", other),
        };

        let serialized = serde_json::to_string(content).unwrap();
        // exactly one "volumes" key must appear on the wire: base.volumes (always
        // empty for V-Programs) must not leak alongside the verified-volume list
        assert_eq!(serialized.matches("\"volumes\"").count(), 1);

        let roundtripped: VerifiableProgramContent = serde_json::from_str(&serialized).unwrap();
        assert_eq!(roundtripped, *content);
    }
}
