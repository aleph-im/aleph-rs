//! Attestation report DTO and X.509 certificate extension handling for
//! RA-TLS (Remote-Attestation TLS) V-Program clients.
//!
//! # Wire compatibility
//!
//! [`AttestationReport`]'s JSON serialization is a cross-repo wire format:
//! the guest `aleph-attest-agent` (aleph-cvm `crates/aleph-attest-agent`)
//! embeds this exact JSON encoding in a TLS certificate's custom X.509
//! extension using aleph-cvm's `aleph-tee::x509::encode_attestation_extension`
//! and `aleph-tee::types::AttestationReport`. This DTO must serialize and
//! deserialize byte-identically to that one: same field names, the same
//! `#[serde(rename_all = "kebab-case")]` on [`TeeType`], and hex-encoding on
//! `data`/`report_data`/`measurement`. Do not change field names or the
//! encoding without also updating the deployed guest agent.
//!
pub mod certs;
pub mod ratls;
pub mod verify;
pub mod x509;

pub use ratls::{AttestedResponse, attested_request};
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
/// Mirrors aleph-cvm `aleph-tee::types::AttestationReport` field-for-field;
/// see the module-level docs for why this must stay byte-identical.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttestationReport {
    pub tee_type: TeeType,
    /// Raw attestation report bytes, as returned by the TEE's attestation
    /// device (e.g. the SEV-SNP `SNP_GET_REPORT` ioctl output).
    #[serde(with = "hex_serde")]
    pub data: Vec<u8>,
    /// The 64-byte `REPORT_DATA` field the guest bound into the report
    /// (typically a hash of the RA-TLS certificate's public key).
    #[serde(with = "hex_serde_array")]
    pub report_data: [u8; 64],
    /// The TEE launch measurement extracted from the report.
    #[serde(with = "hex_serde")]
    pub measurement: Vec<u8>,
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

/// Serde helper for hex-encoding `[u8; 64]` fields.
mod hex_serde_array {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 64], D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
        let array: [u8; 64] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected exactly 64 bytes"))?;
        Ok(array)
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
            report_data: [0x42; 64],
            measurement: vec![0x01, 0x02, 0x03],
        };

        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("deadbeef"));
        assert!(json.contains(&"42".repeat(64)));
        assert!(json.contains("010203"));

        let deserialized: AttestationReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.tee_type, report.tee_type);
        assert_eq!(deserialized.data, report.data);
        assert_eq!(deserialized.report_data, report.report_data);
        assert_eq!(deserialized.measurement, report.measurement);
    }
}
