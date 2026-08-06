//! SEV-SNP attestation report verification: AMD certificate chain (ARK/ASK/
//! VCEK) plus the report's own ECDSA P-384/SHA-384 signature.
//!
//! This module owns exactly: parsing the raw report, the AMD chain +
//! signature check (delegated to the `sev` crate's pure-Rust `crypto_nossl`
//! backend, no OpenSSL), the VMPL policy check, and the ARK subject-identity
//! check (both of which `sev` does *not* perform). It does **not** check the
//! launch measurement or bind `report_data` to a TLS key - those live in the
//! RA-TLS handshake (a later task) which has the expected measurement and
//! public key to compare against.
//!
//! Fails closed: any parse, chain, signature, VMPL, or ARK-identity failure
//! returns `Err`; an `Ok(VerificationResult)` always means a fully verified
//! report.

use sev::certs::snp::{Certificate, Chain, Verifiable, builtin, ca};
use sev::firmware::guest::AttestationReport as SnpReport;
use sev::parser::ByteParser;

use super::AttestError;
use super::AttestationReport;
use super::certs;

/// AMD ARK certificates use CN = "ARK-{product}" (e.g. "ARK-Milan").
const AMD_ARK_CN_PREFIX: &str = "ARK-";
/// AMD ARK certificates' Organization is always this exact string.
const AMD_ORG_NAME: &str = "Advanced Micro Devices";

/// AMD EPYC product line a SEV-SNP report/VCEK was issued for.
///
/// This selects which builtin ARK/ASK pair (bundled with the `sev` crate, no
/// network needed) to verify against, and which AMD KDS URL segment to use
/// when fetching the per-chip VCEK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AmdProduct {
    Milan,
    #[default]
    Genoa,
    Turin,
}

impl AmdProduct {
    /// The product name AMD's KDS expects in VCEK URLs, e.g.
    /// `https://kdsintf.amd.com/vcek/v1/Genoa/{chip_id}?...`.
    pub fn kds_name(self) -> &'static str {
        match self {
            AmdProduct::Milan => "Milan",
            AmdProduct::Genoa => "Genoa",
            AmdProduct::Turin => "Turin",
        }
    }

    /// The builtin (bundled, no-network) ARK + ASK certificates for this
    /// product line.
    fn builtin_ca(self) -> Result<(Certificate, Certificate), AttestError> {
        Ok(match self {
            AmdProduct::Milan => (builtin::milan::ark()?, builtin::milan::ask()?),
            AmdProduct::Genoa => (builtin::genoa::ark()?, builtin::genoa::ask()?),
            AmdProduct::Turin => (builtin::turin::ark()?, builtin::turin::ask()?),
        })
    }
}

impl std::fmt::Display for AmdProduct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.kds_name())
    }
}

impl std::str::FromStr for AmdProduct {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "milan" => Ok(AmdProduct::Milan),
            "genoa" => Ok(AmdProduct::Genoa),
            "turin" => Ok(AmdProduct::Turin),
            other => Err(format!(
                "unknown AMD product '{other}' (expected milan, genoa, or turin)"
            )),
        }
    }
}

/// Outcome of verifying a SEV-SNP attestation report against AMD's identity
/// chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationResult {
    /// Hex-encoded 48-byte launch measurement from the report.
    pub measurement: String,
    /// Human-readable summary of what was checked.
    pub summary: String,
}

/// Verify a SEV-SNP attestation report's AMD certificate chain and report
/// signature.
///
/// Steps:
/// 1. Parse `dto.data` (the raw `SNP_GET_REPORT` bytes) as a SEV-SNP report.
/// 2. Fetch the VCEK certificate from AMD's KDS for this report's chip/TCB.
/// 3. Delegate to [`verify_report_with_vcek`] for the actual chain +
///    signature + policy checks.
pub async fn verify_sev_snp_report(
    dto: &AttestationReport,
    product: AmdProduct,
) -> Result<VerificationResult, AttestError> {
    let report = SnpReport::from_bytes(&dto.data).map_err(AttestError::Parse)?;
    let vcek_der = certs::fetch_vcek(product, &report.chip_id, &report.reported_tcb).await?;
    verify_report_with_vcek(&report, product, &vcek_der)
}

/// Synchronous core of [`verify_sev_snp_report`]: everything after the VCEK
/// bytes are in hand. Split out so tests can inject a fixture VCEK and never
/// touch the network.
///
/// Verifies, in order:
/// 1. The AMD certificate chain (ARK self-signed, ARK signs ASK, ASK signs
///    VCEK) and the report's ECDSA P-384/SHA-384 signature under the VCEK -
///    all in one `sev` crate call.
/// 2. VMPL: only 0-1 (firmware/kernel, the trusted measured stack) are
///    accepted; `sev` does not enforce this.
/// 3. ARK subject identity (CN prefix + Organization + self-issued): guards
///    against a poisoned/substituted "builtin" ARK; `sev` does not check
///    this either, it only checks the signature math.
fn verify_report_with_vcek(
    report: &SnpReport,
    product: AmdProduct,
    vcek_der: &[u8],
) -> Result<VerificationResult, AttestError> {
    let (ark, ask) = product.builtin_ca()?;
    let vek = Certificate::from_der(vcek_der)?;
    let chain = Chain {
        ca: ca::Chain { ark, ask },
        vek,
    };

    // ONE call: ARK self-signed, ARK -> ASK, ASK -> VCEK, AND the report's
    // ECDSA P-384/SHA-384 signature under the VCEK public key.
    (&chain, report).verify().map_err(AttestError::Chain)?;

    // Policy checks the `sev` crate does not perform:
    if report.vmpl > 1 {
        return Err(AttestError::Vmpl(report.vmpl));
    }
    verify_ark_identity(&chain.ca.ark)?;

    Ok(VerificationResult {
        measurement: hex::encode(report.measurement),
        summary: format!(
            "SEV-SNP verified against AMD {product} chain (VMPL {})",
            report.vmpl
        ),
    })
}

/// Verify that an ARK certificate has AMD's expected subject identity.
///
/// Checks:
/// - Subject CN starts with "ARK-" (e.g. "ARK-Milan", "ARK-Genoa").
/// - Subject O is "Advanced Micro Devices".
/// - Issuer matches subject (self-issued).
///
/// This guards against a cache-poisoning or supply-chain attack where an
/// attacker substitutes a self-signed certificate from a different issuer
/// for the (supposedly AMD-bundled) ARK - the chain math alone can't catch
/// that, since a self-signed cert always "verifies" against itself.
fn verify_ark_identity(ark: &Certificate) -> Result<(), AttestError> {
    let der = ark.to_der().map_err(AttestError::CertDecode)?;
    let (_, cert) = x509_parser::parse_x509_certificate(&der)
        .map_err(|e| AttestError::ArkIdentity(format!("failed to parse ARK certificate: {e}")))?;

    let subject = &cert.tbs_certificate.subject;
    let issuer = &cert.tbs_certificate.issuer;

    let cn = subject
        .iter_common_name()
        .next()
        .and_then(|attr| attr.as_str().ok())
        .ok_or_else(|| {
            AttestError::ArkIdentity("ARK certificate has no Common Name in subject".to_string())
        })?;
    if !cn.starts_with(AMD_ARK_CN_PREFIX) {
        return Err(AttestError::ArkIdentity(format!(
            "ARK certificate CN '{cn}' does not start with expected prefix '{AMD_ARK_CN_PREFIX}'"
        )));
    }

    let org = subject
        .iter_organization()
        .next()
        .and_then(|attr| attr.as_str().ok())
        .ok_or_else(|| {
            AttestError::ArkIdentity("ARK certificate has no Organization in subject".to_string())
        })?;
    if org != AMD_ORG_NAME {
        return Err(AttestError::ArkIdentity(format!(
            "ARK certificate Organization '{org}' does not match expected '{AMD_ORG_NAME}'"
        )));
    }

    if subject.as_raw() != issuer.as_raw() {
        return Err(AttestError::ArkIdentity(
            "ARK certificate issuer does not match subject (not self-issued)".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MILAN_VCEK_DER: &[u8] = include_bytes!("testdata/vcek_milan.der");
    const TEST_MILAN_REPORT_HEX: &[u8] = include_bytes!("testdata/report_milan.hex");

    /// The real, captured Milan report from the `sev` crate's own test
    /// fixtures (`tests/certs_data/report_milan.hex`), hex-decoded.
    fn milan_report_bytes() -> Vec<u8> {
        let hex_str = std::str::from_utf8(TEST_MILAN_REPORT_HEX)
            .expect("fixture is ASCII hex")
            .trim();
        hex::decode(hex_str).expect("fixture is valid hex")
    }

    #[test]
    fn amd_product_from_str_is_case_insensitive_and_rejects_unknown() {
        assert_eq!("milan".parse(), Ok(AmdProduct::Milan));
        assert_eq!("MILAN".parse(), Ok(AmdProduct::Milan));
        assert_eq!("Genoa".parse(), Ok(AmdProduct::Genoa));
        assert_eq!("turin".parse(), Ok(AmdProduct::Turin));
        assert!("naples".parse::<AmdProduct>().is_err());
    }

    #[test]
    fn amd_product_defaults_to_genoa() {
        assert_eq!(AmdProduct::default(), AmdProduct::Genoa);
    }

    #[test]
    fn verifies_real_milan_report_against_builtin_ark_ask_and_fixture_vcek() {
        let report_bytes = milan_report_bytes();
        let report = SnpReport::from_bytes(&report_bytes).expect("fixture report should parse");

        let result = verify_report_with_vcek(&report, AmdProduct::Milan, TEST_MILAN_VCEK_DER)
            .expect("verification of a genuine, untampered Milan report must succeed");

        // Pinned to the fixture's real launch measurement (also cross-checked
        // against the `sev` crate's own parsed `report.measurement` below),
        // so a regression that silently accepts a *different* measurement
        // doesn't slip through unnoticed.
        assert_eq!(
            result.measurement,
            "7a1e5c266c0108dbc9bb94fa926951320940915d0aafb42464bd88b579ea158d3e1a0dc39b2c60bd95b9c480cd81841f"
        );
        assert_eq!(result.measurement, hex::encode(report.measurement));
    }

    #[test]
    fn rejects_report_with_flipped_signed_byte() {
        let mut report_bytes = milan_report_bytes();
        // Flip a byte inside the signed region (offset 21, within the
        // `policy` field), mirroring the `sev` crate's own
        // `milan_report_invalid` test - this must break the report's
        // signature over that region.
        report_bytes[21] ^= 0x80;
        let report = SnpReport::from_bytes(&report_bytes)
            .expect("bit-flipped bytes still decode structurally");

        let result = verify_report_with_vcek(&report, AmdProduct::Milan, TEST_MILAN_VCEK_DER);
        assert!(
            result.is_err(),
            "a tampered report must fail closed, never verify successfully"
        );
    }
}
