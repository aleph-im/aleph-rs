//! Attestation report DTO and X.509 certificate extension handling for
//! RA-TLS (Remote-Attestation TLS) V-Program clients.
//!
//! # Wire compatibility
//!
//! [`AttestationReport`]'s JSON serialization is a cross-repo wire format:
//! the guest `aleph-attest-agent` (built from aleph-vm
//! `rust/crates/aleph-tee`, originally aleph-cvm) embeds this exact JSON
//! encoding in a TLS certificate's custom X.509 extension using that
//! crate's `x509::encode_attestation_extension` and
//! `types::AttestationReport`. This DTO must serialize and deserialize
//! byte-identically to that one: same field names, the same
//! `#[serde(rename_all = "kebab-case")]` on [`TeeType`], and hex-encoding
//! on `data`. Do not change field names or the encoding without also
//! updating the deployed guest agent.
//!
pub mod certs;
pub mod owner_auth;
pub mod platform;
pub mod ratls;
pub mod tcb;
pub mod verify;
pub mod x509;

pub use platform::{PlatformPolicy, PlatformPosture};
pub use ratls::{
    AttestedResponse, FreshAttestation, MeasurementPin, PolicyPin, attested_request,
    fresh_attestation,
};
pub use tcb::{
    Component, Deficiency, TcbFloor, TcbFloorOverride, TcbFloorPolicy, builtin_baseline,
    builtin_baseline_policy,
};
pub use verify::{AmdProduct, VerificationResult, verify_sev_snp_report};
pub use x509::{
    ATTESTATION_OID, ATTESTATION_OID_STR, AttestError, decode_attestation_extension,
    encode_attestation_extension, extract_attestation_from_cert,
};

use serde::{Deserialize, Serialize};

/// TEE backend that produced an [`AttestationReport`].
///
/// Mirrors aleph-cvm `aleph-tee::types::TeeType` exactly (same variants,
/// same kebab-case wire encoding) since reports cross the aleph-cvm /
/// aleph-rs boundary as JSON embedded in a TLS certificate extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TeeType {
    SevSnp,
    Tdx,
    NvidiaCc,
    None,
}

/// A TEE attestation report, as embedded in an RA-TLS certificate's custom
/// X.509 extension by the guest `aleph-attest-agent`.
///
/// Mirrors the emitter's `aleph-tee::types::AttestationReport`
/// (aleph-vm `rust/crates/aleph-tee`) field-for-field: `tee_type` plus the
/// raw signed report. The emitter dropped the legacy `report_data` /
/// `measurement` sibling fields (both are derived by parsing `data`, which
/// this SDK's verifier already treats as the single source of truth), and
/// requiring them here rejected real guests (aleph-testnets#35 run
/// 31407307278). Legacy emitters that still send the siblings keep parsing:
/// serde ignores unknown fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttestationReport {
    pub tee_type: TeeType,
    /// Raw attestation report bytes, as returned by the TEE's attestation
    /// device (e.g. the SEV-SNP `SNP_GET_REPORT` ioctl output). Every
    /// verified quantity (`report_data` key binding, launch measurement) is
    /// parsed out of these signed bytes.
    #[serde(with = "hex_serde")]
    pub data: Vec<u8>,
}

/// Serde helper for hex-encoding `Vec<u8>` fields.
mod hex_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        hex::decode(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tee_type_uses_kebab_case_wire_encoding() {
        assert_eq!(
            serde_json::to_string(&TeeType::SevSnp).unwrap(),
            "\"sev-snp\""
        );
        assert_eq!(serde_json::to_string(&TeeType::Tdx).unwrap(), "\"tdx\"");
        assert_eq!(
            serde_json::to_string(&TeeType::NvidiaCc).unwrap(),
            "\"nvidia-cc\""
        );
        assert_eq!(serde_json::to_string(&TeeType::None).unwrap(), "\"none\"");
    }

    #[test]
    fn attestation_report_hex_encodes_binary_fields() {
        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: vec![0xde, 0xad, 0xbe, 0xef],
        };

        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("deadbeef"));

        let deserialized: AttestationReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.tee_type, report.tee_type);
        assert_eq!(deserialized.data, report.data);
    }

    /// Regression for aleph-testnets#35 run 31407307278: the guest agent
    /// emits only `{tee_type, data}`; requiring the legacy sibling fields
    /// rejected every real V-Program.
    #[test]
    fn attestation_report_parses_the_emitter_schema_without_siblings() {
        let report: AttestationReport =
            serde_json::from_str(r#"{"tee_type":"sev-snp","data":"deadbeef"}"#).unwrap();
        assert_eq!(report.tee_type, TeeType::SevSnp);
        assert_eq!(report.data, vec![0xde, 0xad, 0xbe, 0xef]);
    }

    /// Legacy emitters still carrying `report_data` / `measurement` siblings
    /// keep parsing: serde ignores unknown fields.
    #[test]
    fn attestation_report_ignores_legacy_sibling_fields() {
        let json = format!(
            r#"{{"tee_type":"sev-snp","data":"deadbeef","report_data":"{}","measurement":"{}"}}"#,
            "42".repeat(64),
            "ab".repeat(48),
        );
        let report: AttestationReport = serde_json::from_str(&json).unwrap();
        assert_eq!(report.data, vec![0xde, 0xad, 0xbe, 0xef]);
    }
}
