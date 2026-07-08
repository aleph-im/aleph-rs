//! Verifiable programs (V-Programs): auto-booting confidential VMs whose full
//! software stack is attestable via SEV-SNP runtime attestation.
//!
//! Design: aleph-vm docs/plans/2026-07-08-confidential-vm-protocol-design.md

use crate::item_hash::ItemHash;
use crate::message::execution::environment::{
    validate_snp_policy, LaunchMeasurement, TeeError, DEFAULT_SNP_POLICY, MAX_MEASUREMENTS,
};
use serde::{Deserialize, Serialize};

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
        if !value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
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

#[cfg(test)]
mod test {
    use super::*;

    const SNP_DIGEST: &str =
        "abababababababababababababababababababababababababababababababababababababababababababababababab";
    const ITEM_HASH_HEX: &str =
        "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe";

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

        let json = format!(r#"{{"backend": "sev_snp", "measurements": []}}"#);
        assert!(serde_json::from_str::<TeeVerification>(&json).is_err()); // min 1
    }

    #[test]
    fn test_vprogram_environment_defaults() {
        let env: VerifiableProgramEnvironment = serde_json::from_str("{}").unwrap();
        assert!(!env.internet);
        assert!(!env.aleph_api);
    }
}
