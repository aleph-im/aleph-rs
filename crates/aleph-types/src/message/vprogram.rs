//! Verifiable programs (V-Programs): auto-booting confidential VMs whose full
//! software stack is attestable via SEV-SNP runtime attestation.
//!
//! Design: aleph-vm docs/plans/2026-07-08-confidential-vm-protocol-design.md

use crate::item_hash::ItemHash;
use crate::message::execution::base::{ExecutableContent, PaymentType};
use crate::message::execution::environment::{
    DEFAULT_SNP_POLICY, LaunchMeasurement, MAX_MEASUREMENTS, TeeError, validate_snp_policy,
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU16;

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

/// The measured platform: a store object bundling the manifest, OVMF, kernel,
/// initrd, and the dm-verity platform rootfs with its hash tree.
///
/// There is deliberately no use_latest: the measurements in the message pin
/// exact artifacts, so the reference must be immutable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfidentialRuntime {
    /// Store message of the measured runtime bundle.
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    #[serde(default)]
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

/// TEE attestation backend the VM launches with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TeeBackend {
    #[serde(rename = "sev_snp")]
    SevSnp,
}

fn default_snp_policy() -> u64 {
    DEFAULT_SNP_POLICY
}

/// TEE launch configuration plus supervisor-opaque measurement annotations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawTeeVerification")]
pub struct TeeVerification {
    pub backend: TeeBackend,
    /// SEV-SNP 64-bit guest policy (not SEV bit semantics).
    #[serde(default = "default_snp_policy")]
    pub policy: u64,
    /// Expected launch digests; never sent to the supervisor.
    pub measurements: Vec<LaunchMeasurement>,
}

#[derive(Deserialize)]
struct RawTeeVerification {
    backend: TeeBackend,
    #[serde(default = "default_snp_policy")]
    policy: u64,
    measurements: Vec<LaunchMeasurement>,
}

impl TryFrom<RawTeeVerification> for TeeVerification {
    type Error = TeeError;

    fn try_from(raw: RawTeeVerification) -> Result<Self, Self::Error> {
        validate_snp_policy(raw.policy)?;
        if raw.measurements.is_empty() {
            return Err(TeeError::SnpModeRequires("measurements"));
        }
        if raw.measurements.len() > MAX_MEASUREMENTS {
            return Err(TeeError::TooManyMeasurements(raw.measurements.len()));
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
pub struct VerifiableProgramEnvironment {
    #[serde(default)]
    pub internet: bool,
    #[serde(default)]
    pub aleph_api: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum VProgramError {
    #[error("V-Programs are credit-only: holder-tier and PAYG stream payments are not supported")]
    CreditOnly,
}

/// Message content for scheduling a verifiable program (V-Program): an
/// auto-booting SEV-SNP VM whose full software stack is attestable.
///
/// Unlike classic programs there is no code/entrypoint/triggers model (the
/// workload contract belongs to the runtime bundle) and no hypervisor choice
/// (always QEMU). Unlike instances, the rootfs is Aleph-provided and measured;
/// the user contribution is the verity-bound workload volume.
///
/// Extra `volumes` are allowed but are OUTSIDE the attested TCB: they are
/// neither measured nor verity-verified.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawVerifiableProgramContent")]
pub struct VerifiableProgramContent {
    #[serde(flatten)]
    pub base: ExecutableContent,
    /// Properties of the execution environment.
    pub environment: VerifiableProgramEnvironment,
    /// The measured platform (runtime bundle).
    pub runtime: ConfidentialRuntime,
    /// The user's verity-bound workload volume.
    pub workload: VerifiedWorkload,
    /// TEE launch config and expected launch measurements.
    pub verification: TeeVerification,
    /// In-guest attestation port; None means the runtime bundle's declared
    /// default (8443). Plumbed through the measured cmdline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_port: Option<NonZeroU16>,
}

impl VerifiableProgramContent {
    /// V-Programs always run in a confidential VM.
    pub fn is_confidential(&self) -> bool {
        true
    }
}

#[derive(Deserialize)]
struct RawVerifiableProgramContent {
    #[serde(flatten)]
    base: ExecutableContent,
    environment: VerifiableProgramEnvironment,
    runtime: ConfidentialRuntime,
    workload: VerifiedWorkload,
    verification: TeeVerification,
    #[serde(default)]
    attestation_port: Option<NonZeroU16>,
}

impl TryFrom<RawVerifiableProgramContent> for VerifiableProgramContent {
    type Error = VProgramError;

    fn try_from(raw: RawVerifiableProgramContent) -> Result<Self, Self::Error> {
        match &raw.base.payment {
            Some(payment) if payment.payment_type == PaymentType::Credit => {}
            _ => return Err(VProgramError::CreditOnly),
        }
        Ok(Self {
            base: raw.base,
            environment: raw.environment,
            runtime: raw.runtime,
            workload: raw.workload,
            verification: raw.verification,
            attestation_port: raw.attestation_port,
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
    fn test_confidential_runtime_wire_names() {
        let r: ConfidentialRuntime = serde_json::from_str(&format!(
            r#"{{"ref": "{ITEM_HASH_HEX}", "comment": "compose-runner snp bundle"}}"#
        ))
        .unwrap();
        assert_eq!(r.comment, "compose-runner snp bundle");
        let value = serde_json::to_value(&r).unwrap();
        assert!(value.get("ref").is_some());
        assert!(value.get("reference").is_none());
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
    fn test_vprogram_environment_defaults() {
        let env: VerifiableProgramEnvironment = serde_json::from_str("{}").unwrap();
        assert!(!env.internet);
        assert!(!env.aleph_api);
    }

    fn vprogram_content_json(payment: &str) -> String {
        format!(
            r#"{{
                "address": "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
                "time": 1719502000.0,
                "allow_amend": false,
                "payment": {payment},
                "environment": {{"internet": true, "aleph_api": false}},
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
                "volumes": []
            }}"#,
            roothash = "cd".repeat(32),
        )
    }

    #[test]
    fn test_vprogram_content_valid_and_credit_only() {
        let content: VerifiableProgramContent =
            serde_json::from_str(&vprogram_content_json(r#"{"type": "credit"}"#)).unwrap();
        assert!(content.is_confidential());
        assert_eq!(content.attestation_port, None);
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
        assert_eq!(content.attestation_port, None);
        assert!(content.base.volumes.is_empty());
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
}
