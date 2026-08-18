//! RA-TLS (Remote-Attestation TLS) attested HTTP client.
//!
//! Ties together the cert-extension extraction (`x509`) and SEV-SNP
//! verification (`verify`) modules into a single [`attested_request`]: an
//! HTTP call made over a TLS channel whose server certificate is checked,
//! *during the handshake*, to carry a SEV-SNP attestation report that is
//! cryptographically bound to that exact TLS connection.
//!
//! Ported from aleph-cvm `crates/aleph-attest-cli/src/verify.rs` and
//! `client.rs`, adapted to this crate's [`AttestError`] and to return
//! structured response data (status/headers/body) rather than only a body
//! string.
//!
//! # Fail-closed
//!
//! `SnpCertVerifier::verify_server_cert` rejects the handshake (`Err`) if:
//! - the certificate has no attestation extension,
//! - the extension declares a TEE type other than SEV-SNP,
//! - the raw report bytes (`data`) don't parse as a SEV-SNP report,
//! - the key-binding check fails
//!   (`report_data != SHA-384(DOMAIN_KEY || pubkey) || zeros`),
//! - an `expected_measurement` was given and doesn't match,
//! - an `expected_policy` was given and the signed guest policy doesn't match.
//!
//! Crucially, the key-binding and measurement checks are made against the
//! fields parsed out of the AMD-signed report bytes (`data`). The DTO carries
//! no other copies of them: the legacy `report_data`/`measurement` sibling
//! fields were unsigned JSON a malicious node could lie in (the C1 finding),
//! and the wire schema has since dropped them on both sides.
//!
//! [`attested_request`] rejects the whole call (`Err`) if, after a
//! successful handshake, no report was stashed (should be unreachable, but
//! checked anyway), or if the post-handshake AMD certificate-chain check
//! ([`verify_sev_snp_report`]) fails. An `Ok(AttestedResponse)` therefore
//! always means the attestation verified: a bad attestation is an `Err`,
//! never a successful response.

use std::sync::{Arc, Mutex};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use sev::firmware::guest::AttestationReport as SnpReport;
use sev::parser::ByteParser;
use sha2::{Digest, Sha384};
use subtle::ConstantTimeEq;

use super::tcb::TcbFloorPolicy;
use super::verify::{AmdProduct, VerificationResult, verify_sev_snp_report};
use super::x509::{AttestError, extract_attestation_from_cert};
use super::{AttestationReport, TeeType};

/// Domain tag the guest agent mixes into the key-binding hash:
/// `report_data = SHA-384(KEY_BINDING_DOMAIN || public_key) || zeros`.
///
/// Mirrors `aleph_tee::report_data::DOMAIN_KEY` (aleph-vm
/// `rust/crates/aleph-tee`) byte-for-byte, trailing `0x00` separator
/// included. The emitter domain-separates its two report shapes (key binding
/// vs. nonce freshness) so an attacker cannot request a fresh report whose
/// `report_data` collides with a key-bound one; verifying without the domain
/// rejected every real guest (aleph-testnets#35 run 31433603211).
const KEY_BINDING_DOMAIN: &[u8] = b"aleph-attest-tls-key-v1\x00";

/// Domain tag the guest agent mixes into the fresh/nonce-binding hash:
/// `report_data = SHA-384(FRESH_DOMAIN || served_public_key || nonce) || zeros`.
///
/// Mirrors `aleph_tee::report_data::DOMAIN_FRESH` (aleph-vm
/// `rust/crates/aleph-tee`) byte-for-byte, trailing `0x00` separator
/// included. The distinct domain keeps fresh reports from ever colliding
/// with key-bound ones.
const FRESH_DOMAIN: &[u8] = b"aleph-attest-fresh-v1\x00";

/// The canonical 64-byte fresh report_data for `(served key, nonce)`:
/// `SHA-384(FRESH_DOMAIN || public_key_raw || nonce)` then zero padding.
fn fresh_bound_report_data(public_key_raw: &[u8], nonce: &[u8]) -> [u8; 64] {
    let mut hasher = Sha384::new();
    hasher.update(FRESH_DOMAIN);
    hasher.update(public_key_raw);
    hasher.update(nonce);
    let mut report_data = [0u8; 64];
    report_data[..48].copy_from_slice(&hasher.finalize());
    report_data
}

/// Verify that a fresh report's SIGNED report_data binds BOTH the served
/// TLS public key and the caller's nonce. Constant-time compare, matching
/// the key-binding check.
fn verify_fresh_binding(
    report_bytes: &[u8],
    served_public_key: &[u8],
    nonce: &[u8],
) -> Result<(), AttestError> {
    let signed = SnpReport::from_bytes(report_bytes).map_err(AttestError::Parse)?;
    let expected = fresh_bound_report_data(served_public_key, nonce);
    if signed.report_data.ct_eq(&expected).unwrap_u8() == 0 {
        return Err(AttestError::FreshnessBinding);
    }
    Ok(())
}

/// The result of an attested HTTP request: the HTTP response plus the
/// verified launch measurement the connection was gated on.
///
/// Constructing this type implies the attestation verified:
/// [`attested_request`] returns `Err` on any attestation failure, so there
/// is no validity flag to check.
#[derive(Debug, Clone)]
pub struct AttestedResponse {
    /// Hex-encoded launch measurement from the verified report.
    pub measurement: String,
    /// SEV-SNP guest policy from the verified report.
    pub policy: u64,
    /// TCB the VM was launched under (Option A gates on this).
    pub launch_tcb: sev::firmware::host::TcbVersion,
    /// TCB the VCEK is keyed to (from the signed report).
    pub reported_tcb: sev::firmware::host::TcbVersion,
    /// CPUID family/model/stepping of the attesting chip, from the signed
    /// report (version 3+; `None` on older reports). Evidence of which
    /// silicon family the TCB floor was selected for.
    pub cpuid_family: Option<u8>,
    pub cpuid_model: Option<u8>,
    pub cpuid_stepping: Option<u8>,
    /// Raw subjectPublicKey bytes of the attested TLS certificate the
    /// handshake verified (the same bytes the agent binds reports to).
    pub served_public_key: Vec<u8>,
    /// The HTTP status code of the response.
    pub status: u16,
    /// The HTTP response headers, in wire order (a header repeated multiple
    /// times yields multiple entries with the same name).
    pub headers: Vec<(String, String)>,
    /// The raw HTTP response body.
    pub body: bytes::Bytes,
}

/// The result of a verified fresh-nonce challenge.
///
/// Constructing this type implies every check passed (same convention as
/// [`AttestedResponse`]: no validity flag).
#[derive(Debug, Clone)]
pub struct FreshAttestation {
    /// Hex-encoded launch measurement from the verified fresh report.
    pub measurement: String,
    /// SEV-SNP guest policy from the verified fresh report.
    pub policy: u64,
    /// TCB the VM was launched under (Option A gates on this).
    pub launch_tcb: sev::firmware::host::TcbVersion,
    /// TCB the VCEK is keyed to (from the signed fresh report).
    pub reported_tcb: sev::firmware::host::TcbVersion,
    /// CPUID family/model/stepping of the attesting chip, from the signed
    /// fresh report (version 3+; `None` on older reports).
    pub cpuid_family: Option<u8>,
    pub cpuid_model: Option<u8>,
    pub cpuid_stepping: Option<u8>,
    /// Key of the RA-TLS exchange that answered the challenge, bound into
    /// the fresh report's report_data alongside the nonce.
    pub served_public_key: Vec<u8>,
}

/// A `rustls` [`ServerCertVerifier`] that extracts a SEV-SNP attestation
/// report from the server's certificate and verifies it is bound to that
/// certificate's TLS key during the handshake.
///
/// Full AMD certificate-chain verification ([`verify_sev_snp_report`]) is
/// deliberately *not* done here: `ServerCertVerifier::verify_server_cert` is
/// a synchronous callback, while the chain check needs an async VCEK fetch.
/// It is done by the caller ([`attested_request`]) after the handshake
/// completes, using the report this verifier stashes.
#[derive(Debug)]
struct SnpCertVerifier {
    extracted_report: Mutex<Option<AttestationReport>>,
    /// Why this verifier last rejected a handshake, if it did. reqwest
    /// surfaces a mid-handshake rejection only as an opaque "error sending
    /// request", so `attested_request` reads this back to name the actual
    /// attestation failure (a measurement mismatch looks identical to a
    /// connection refusal otherwise).
    last_rejection: Mutex<Option<String>>,
    /// Raw subjectPublicKey bytes of the last cert this verifier accepted.
    /// The fresh-attestation flow needs them to reconstruct the channel-bound
    /// fresh report_data.
    served_public_key: Mutex<Option<Vec<u8>>>,
    expected_measurement: Option<Vec<u8>>,
    expected_policy: Option<u64>,
    provider: Arc<CryptoProvider>,
}

impl SnpCertVerifier {
    /// Create a new verifier wrapped in an `Arc` for use with `rustls`.
    ///
    /// If `expected_measurement` is `Some`, the handshake is rejected unless
    /// the report's measurement matches exactly (a "measurement pin").
    /// If `expected_policy` is `Some`, the handshake is rejected unless the
    /// SIGNED report's guest policy matches exactly (a "policy pin"): the
    /// policy is not part of the launch measurement, so without this check a
    /// malicious host could launch the measured stack with a weaker policy
    /// (e.g. debug allowed, exposing guest memory) and still pass the
    /// measurement pin.
    fn new(expected_measurement: Option<Vec<u8>>, expected_policy: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            extracted_report: Mutex::new(None),
            last_rejection: Mutex::new(None),
            served_public_key: Mutex::new(None),
            expected_measurement,
            expected_policy,
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        })
    }

    /// The report stashed by a completed, successful handshake, if any.
    fn get_report(&self) -> Option<AttestationReport> {
        self.extracted_report.lock().unwrap().clone()
    }

    /// Why the last handshake was rejected, if this verifier rejected one.
    fn get_rejection(&self) -> Option<String> {
        self.last_rejection.lock().unwrap().clone()
    }

    /// The served public key stashed by a completed, successful handshake.
    fn get_served_public_key(&self) -> Option<Vec<u8>> {
        self.served_public_key.lock().unwrap().clone()
    }

    /// The attestation checks behind [`ServerCertVerifier::verify_server_cert`],
    /// split out so the trait impl can record a rejection before returning it.
    fn verify_snp_cert(
        &self,
        end_entity: &CertificateDer<'_>,
    ) -> Result<ServerCertVerified, RustlsError> {
        // 1. Extract the attestation report from the certificate extension.
        let report = extract_attestation_from_cert(end_entity.as_ref())
            .map_err(|e| {
                RustlsError::General(format!(
                    "failed to extract attestation from certificate: {e}"
                ))
            })?
            .ok_or_else(|| {
                RustlsError::General(
                    "certificate does not contain an attestation extension".to_string(),
                )
            })?;

        // Only SEV-SNP is implemented. Reject other TEE types before trying
        // to parse `data` as an SNP report, so the error names the actual
        // problem instead of surfacing as a confusing parse failure.
        if report.tee_type != TeeType::SevSnp {
            return Err(RustlsError::General(format!(
                "unsupported TEE type {:?}: only SEV-SNP attestation is supported",
                report.tee_type
            )));
        }

        // 2. Parse the SEV-SNP report out of the DTO's raw `data`. Every
        //    security check below uses these SIGNED fields (the AMD
        //    signature covers report bytes 0x000..0x2A0, which include the
        //    report's own report_data and measurement). The signature over
        //    `data` itself is checked post-handshake in
        //    `verify_sev_snp_report`; a forged `data` fails closed there, so
        //    checking the parsed fields here (pre-signature) is still sound.
        //    Fail closed if the bytes don't parse.
        let signed = SnpReport::from_bytes(&report.data).map_err(|e| {
            RustlsError::General(format!(
                "failed to parse SEV-SNP attestation report from certificate data: {e}"
            ))
        })?;

        // 3. Key binding: the SIGNED report_data must equal
        //    SHA-384(KEY_BINDING_DOMAIN || public_key) || zeros. This proves
        //    the report was generated for *this* TLS key, not replayed from a
        //    different (possibly still-valid) one. The hashed key bytes are
        //    the certificate's raw subjectPublicKey bit-string (the
        //    uncompressed EC point), the same bytes the agent feeds to
        //    `key_bound_report_data`.
        let (_, cert) = x509_parser::parse_x509_certificate(end_entity.as_ref()).map_err(|e| {
            RustlsError::General(format!("failed to parse certificate for key binding: {e}"))
        })?;
        let public_key_bytes = cert.tbs_certificate.subject_pki.subject_public_key.data;

        let mut hasher = Sha384::new();
        hasher.update(KEY_BINDING_DOMAIN);
        hasher.update(&public_key_bytes[..]);
        let hash = hasher.finalize();
        let mut expected_report_data = [0u8; 64];
        expected_report_data[..48].copy_from_slice(&hash);

        if signed.report_data.ct_eq(&expected_report_data).unwrap_u8() == 0 {
            return Err(RustlsError::General(format!(
                "key binding verification failed: report_data does not match \
                 SHA-384(domain || public_key). expected {}, got {}",
                hex::encode(expected_report_data),
                hex::encode(signed.report_data),
            )));
        }

        // 4. Optional measurement pin, against the SIGNED measurement.
        //    Constant-time comparison avoids leaking the first-differing-
        //    byte offset over the TLS handshake timing side channel.
        if let Some(ref expected) = self.expected_measurement
            && signed
                .measurement
                .as_slice()
                .ct_eq(expected.as_slice())
                .unwrap_u8()
                == 0
        {
            return Err(RustlsError::General(format!(
                "measurement mismatch: expected {}, got {}",
                hex::encode(expected),
                hex::encode(signed.measurement),
            )));
        }

        // 5. Optional policy pin, against the SIGNED guest policy. The
        //    policy is not covered by the launch measurement, so this check
        //    is all that stands between the client and a host that launched
        //    the same measured stack with a weaker policy (e.g. debug
        //    allowed, letting the host decrypt guest memory).
        if let Some(expected) = self.expected_policy {
            let got = u64::from(signed.policy);
            if got != expected {
                return Err(RustlsError::General(format!(
                    "guest policy mismatch: expected {expected:#x}, got {got:#x}"
                )));
            }
        }

        *self.extracted_report.lock().unwrap() = Some(report);
        *self.served_public_key.lock().unwrap() = Some(public_key_bytes.to_vec());
        Ok(ServerCertVerified::assertion())
    }
}

impl ServerCertVerifier for SnpCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        // Record a rejection, and clear any prior one on acceptance: one
        // request can drive several handshakes through the same verifier
        // (address fallback, redirects), and a stale rejection from an
        // earlier attempt must not relabel a later transport failure as
        // an attestation verdict.
        let result = self.verify_snp_cert(end_entity);
        *self.last_rejection.lock().unwrap() = result.as_ref().err().map(ToString::to_string);
        result
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Build a `reqwest::Client` whose TLS transport uses `verifier` in place of
/// normal WebPKI certificate validation.
///
/// Uses `ClientConfig::builder_with_provider` (rather than the unqualified
/// `ClientConfig::builder()`) to pin the `ring` `CryptoProvider` explicitly.
/// `reqwest`'s own default TLS feature set pulls in `aws-lc-rs` for the
/// *same* `rustls` crate instance, so the process may have two provider
/// crate-features active at once; the unqualified `builder()` resolves the
/// default provider via `CryptoProvider::get_default_or_install_from_crate_features()`,
/// which panics when that's ambiguous. Selecting the provider explicitly
/// here sidesteps that global, order-dependent state entirely.
fn build_attested_client(verifier: Arc<SnpCertVerifier>) -> Result<reqwest::Client, AttestError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| AttestError::Tls(format!("failed to select TLS protocol versions: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();

    reqwest::Client::builder()
        .use_preconfigured_tls(tls_config)
        .build()
        .map_err(AttestError::Http)
}

/// Make an HTTP request over a TLS channel whose server certificate is
/// verified, during the handshake, to carry a SEV-SNP attestation report
/// bound to that certificate's key (and, optionally, pinned to
/// `expected_measurement`). After the handshake, the report is further
/// checked against AMD's certificate chain via [`verify_sev_snp_report`].
///
/// The URL requested is `base_url` joined with `path` (so `path` may be
/// absolute, e.g. `"/status"`, replacing `base_url`'s path component per
/// `Url::join`'s usual rules).
///
/// Fails closed: if the handshake never stashes a report, or if
/// `verify_sev_snp_report` errors, this returns `Err`; an `Ok` response
/// always carries a fully verified attestation.
#[allow(clippy::too_many_arguments)]
pub async fn attested_request(
    base_url: &url::Url,
    method: reqwest::Method,
    path: &str,
    headers: &[(String, String)],
    body: Option<bytes::Bytes>,
    expected_measurement: Option<&[u8]>,
    expected_policy: Option<u64>,
    product: AmdProduct,
    min_tcb: &TcbFloorPolicy,
) -> Result<AttestedResponse, AttestError> {
    let url = base_url.join(path)?;

    let verifier = SnpCertVerifier::new(expected_measurement.map(<[u8]>::to_vec), expected_policy);
    let client = build_attested_client(verifier.clone())?;

    let mut request = client.request(method, url);
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    if let Some(body) = body {
        request = request.body(body);
    }

    // A request that died mid-handshake because OUR verifier rejected the
    // peer's attestation surfaces from reqwest as an opaque "error sending
    // request"; name the recorded rejection instead, so a measurement
    // mismatch is distinguishable from a plain connection failure.
    let response = request
        .send()
        .await
        .map_err(|error| match verifier.get_rejection() {
            Some(reason) => AttestError::HandshakeRejected(reason),
            None => AttestError::Http(error),
        })?;

    // Verify the attestation BEFORE reading the response body: the handshake
    // already enforced key binding and the measurement pin, but the AMD
    // chain check below is post-handshake, and body bytes from a peer whose
    // chain does not verify should never enter this process's memory.
    //
    // Fail closed: a successful handshake against `SnpCertVerifier` always
    // stashes a report (it errors the handshake otherwise), but check again
    // rather than trust that invariant silently.
    let report = verifier.get_report().ok_or(AttestError::MissingReport)?;
    let served_public_key = verifier
        .get_served_public_key()
        .ok_or(AttestError::MissingReport)?;

    // Propagate on failure rather than degrading to a partial response: an
    // unverifiable report must never look like a successful response.
    let result = verify_sev_snp_report(&report, product, min_tcb).await?;

    let status = response.status().as_u16();
    let response_headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.to_string(),
                value.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    let body = response.bytes().await.map_err(AttestError::Http)?;

    Ok(AttestedResponse {
        measurement: result.measurement,
        policy: result.policy,
        launch_tcb: result.launch_tcb,
        reported_tcb: result.reported_tcb,
        cpuid_family: result.cpuid_family,
        cpuid_model: result.cpuid_model,
        cpuid_stepping: result.cpuid_stepping,
        served_public_key,
        status,
        headers: response_headers,
        body,
    })
}

/// Re-run the measurement and policy pins against a VERIFIED fresh report.
/// These normally run during the TLS handshake against the cert report;
/// the fresh report arrives in a response body, so they must be re-applied
/// to its signed fields explicitly.
fn check_fresh_pins(
    result: &VerificationResult,
    expected_measurement: Option<&[u8]>,
    expected_policy: Option<u64>,
) -> Result<(), AttestError> {
    if let Some(expected) = expected_measurement {
        let expected_hex = hex::encode(expected);
        if result.measurement != expected_hex {
            return Err(AttestError::FreshMeasurementMismatch {
                expected: expected_hex,
                got: result.measurement.clone(),
            });
        }
    }
    if let Some(expected) = expected_policy
        && result.policy != expected
    {
        return Err(AttestError::FreshPolicyMismatch {
            expected,
            got: result.policy,
        });
    }
    Ok(())
}

/// Challenge the guest agent's fresh-attestation endpoint and verify the
/// answer end to end.
///
/// Proves liveness: the returned report must be AMD-signed (chain + TCB
/// floor via `verify_sev_snp_report`), satisfy the same measurement and
/// policy pins as the certificate report, and bind BOTH the served TLS key
/// of this exchange AND a nonce generated here, so it cannot have existed
/// before this call. See the G4a design doc for the security argument.
pub async fn fresh_attestation(
    base_url: &url::Url,
    expected_measurement: Option<&[u8]>,
    expected_policy: Option<u64>,
    product: AmdProduct,
    min_tcb: &TcbFloorPolicy,
) -> Result<FreshAttestation, AttestError> {
    use rand::RngCore;
    let mut nonce = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    fresh_attestation_with_nonce(
        base_url,
        &nonce,
        expected_measurement,
        expected_policy,
        product,
        min_tcb,
    )
    .await
}

async fn fresh_attestation_with_nonce(
    base_url: &url::Url,
    nonce: &[u8],
    expected_measurement: Option<&[u8]>,
    expected_policy: Option<u64>,
    product: AmdProduct,
    min_tcb: &TcbFloorPolicy,
) -> Result<FreshAttestation, AttestError> {
    // The challenge request itself runs over an attested channel, with the
    // same pins enforced on the CERT report during its handshake.
    let path = format!("/.well-known/attestation?nonce={}", hex::encode(nonce));
    let response = attested_request(
        base_url,
        reqwest::Method::GET,
        &path,
        &[],
        None,
        expected_measurement,
        expected_policy,
        product,
        min_tcb,
    )
    .await?;
    if response.status != 200 {
        return Err(AttestError::FreshEndpoint(response.status));
    }

    let dto: AttestationReport = serde_json::from_slice(&response.body)?;
    if dto.tee_type != TeeType::SevSnp {
        return Err(AttestError::UnsupportedTeeType(dto.tee_type));
    }

    // Full verification of the FRESH report: AMD chain, signature, VMPL,
    // TCB floor. Then the pins, then the freshness binding.
    let result = verify_sev_snp_report(&dto, product, min_tcb).await?;
    check_fresh_pins(&result, expected_measurement, expected_policy)?;
    verify_fresh_binding(&dto.data, &response.served_public_key, nonce)?;

    Ok(FreshAttestation {
        measurement: result.measurement,
        policy: result.policy,
        launch_tcb: result.launch_tcb,
        reported_tcb: result.reported_tcb,
        cpuid_family: result.cpuid_family,
        cpuid_model: result.cpuid_model,
        cpuid_stepping: result.cpuid_stepping,
        served_public_key: response.served_public_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attest::TeeType;
    use crate::attest::x509::{ATTESTATION_OID, encode_attestation_extension};

    /// A genuine Milan report fixture (the same one `verify.rs` uses), whose
    /// framing/version/chip_id are all well-formed so that `to_bytes()` /
    /// `from_bytes()` round-trip cleanly. We start from it and only overwrite
    /// `report_data` / `measurement` to synthesize the SIGNED report bytes a
    /// test needs. The AMD signature is left untouched (garbage relative to
    /// the mutated fields), which is fine: `verify_server_cert` never checks
    /// the signature - that happens post-handshake in `verify_sev_snp_report`.
    const MILAN_REPORT_HEX: &[u8] = include_bytes!("testdata/report_milan.hex");

    /// Build valid raw SEV-SNP report bytes (`AttestationReport::data`)
    /// carrying the given SIGNED `report_data` and `measurement`.
    fn signed_report_bytes(report_data: [u8; 64], measurement: [u8; 48]) -> Vec<u8> {
        let hex_str = std::str::from_utf8(MILAN_REPORT_HEX)
            .expect("fixture is ASCII hex")
            .trim();
        let bytes = hex::decode(hex_str).expect("fixture is valid hex");
        let mut report = SnpReport::from_bytes(&bytes).expect("fixture report should parse");
        report.report_data = report_data;
        report.measurement = measurement;
        report
            .to_bytes()
            .expect("re-encoding the report should succeed")
            .as_ref()
            .to_vec()
    }

    /// Like [`signed_report_bytes`], but also sets the SIGNED guest policy.
    fn signed_report_bytes_with_policy(
        report_data: [u8; 64],
        measurement: [u8; 48],
        policy: u64,
    ) -> Vec<u8> {
        use sev::firmware::guest::GuestPolicy;

        let bytes = signed_report_bytes(report_data, measurement);
        let mut report = SnpReport::from_bytes(&bytes).expect("fixture report should parse");
        report.policy = GuestPolicy::from(policy);
        report
            .to_bytes()
            .expect("re-encoding the report should succeed")
            .as_ref()
            .to_vec()
    }

    /// The 64-byte `REPORT_DATA` value that binds a report to a TLS key whose
    /// domained SPKI hash is `hash`:
    /// `SHA-384(KEY_BINDING_DOMAIN || pubkey) || zeros[16]`.
    fn key_bound_report_data(hash: [u8; 48]) -> [u8; 64] {
        let mut report_data = [0u8; 64];
        report_data[..48].copy_from_slice(&hash);
        report_data
    }

    /// Build a self-signed cert for `key_pair` and return its DER bytes.
    fn self_signed_der(key_pair: &rcgen::KeyPair, ext: Option<(&[u64], Vec<u8>)>) -> Vec<u8> {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .expect("CertificateParams should be valid");
        if let Some((oid, value)) = ext {
            params
                .custom_extensions
                .push(rcgen::CustomExtension::from_oid_content(oid, value));
        }
        let cert = params
            .self_signed(key_pair)
            .expect("self-signing should succeed");
        cert.der().to_vec()
    }

    /// `SHA-384(KEY_BINDING_DOMAIN || pubkey)` over the SubjectPublicKeyInfo
    /// `BIT STRING` content of `der`, as `verify_server_cert`'s key-binding
    /// check (and the guest agent's `key_bound_report_data`) computes it.
    fn subject_pubkey_sha384(der: &[u8]) -> [u8; 48] {
        let (_, cert) =
            x509_parser::parse_x509_certificate(der).expect("cert should parse for the probe");
        let mut hasher = Sha384::new();
        hasher.update(KEY_BINDING_DOMAIN);
        hasher.update(cert.tbs_certificate.subject_pki.subject_public_key.data);
        hasher.finalize().into()
    }

    fn dummy_server_name() -> ServerName<'static> {
        ServerName::try_from("localhost").unwrap()
    }

    #[test]
    fn rejects_a_cert_declaring_a_non_sev_snp_tee_type() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);
        let report_data = key_bound_report_data(hash);
        let measurement = [0xAB; 48];

        // Everything else is valid (key-bound report_data, matching pin);
        // only the declared TEE type is wrong, so a failure can only come
        // from the tee_type check.
        let report = AttestationReport {
            tee_type: TeeType::Tdx,
            data: signed_report_bytes(report_data, measurement),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        let verifier = SnpCertVerifier::new(Some(measurement.to_vec()), None);
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        let err = result.expect_err("a non-SEV-SNP tee_type must be rejected");
        assert!(
            err.to_string().contains("unsupported TEE type"),
            "error should name the unsupported TEE type, got: {err}"
        );
        let rejection = verifier
            .get_rejection()
            .expect("rejection must be recorded");
        assert!(rejection.contains("unsupported TEE type"), "{rejection}");
    }

    #[test]
    fn accepts_a_cert_whose_report_is_key_bound_and_measurement_matches() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        // Probe cert (same key_pair) purely to learn the SPKI bytes rcgen
        // will embed, so we can compute the expected report_data hash before
        // building the real, extension-carrying cert.
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);

        let report_data = key_bound_report_data(hash);
        let measurement = [0xAB; 48];

        // The SIGNED report bytes carry the key-bound report_data and the
        // pinned measurement.
        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: signed_report_bytes(report_data, measurement),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        let verifier = SnpCertVerifier::new(Some(measurement.to_vec()), None);
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        assert!(
            result.is_ok(),
            "a key-bound, correctly-pinned report must be accepted: {result:?}"
        );
        let stashed = verifier
            .get_report()
            .expect("a successful verification must stash the report");
        assert_eq!(stashed.data, signed_report_bytes(report_data, measurement));
        assert!(
            verifier.get_rejection().is_none(),
            "an accepted handshake must not record a rejection"
        );
    }

    #[test]
    fn an_accepted_handshake_clears_a_prior_rejection() {
        // One request can drive several handshakes through the same verifier
        // (address fallback, redirects). A rejection recorded by an earlier
        // attempt must not survive a later accepted handshake, or a
        // subsequent transport failure would be misattributed to attestation.
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);
        let report_data = key_bound_report_data(hash);
        let measurement = [0xAB; 48];

        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: signed_report_bytes(report_data, measurement),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let good_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));
        // No extension at all: guaranteed rejection.
        let bad_der = self_signed_der(&key_pair, None);

        let verifier = SnpCertVerifier::new(Some(measurement.to_vec()), None);
        verifier
            .verify_server_cert(
                &CertificateDer::from(bad_der),
                &[],
                &dummy_server_name(),
                &[],
                UnixTime::now(),
            )
            .expect_err("the extension-less cert must be rejected");
        assert!(verifier.get_rejection().is_some());

        verifier
            .verify_server_cert(
                &CertificateDer::from(good_der),
                &[],
                &dummy_server_name(),
                &[],
                UnixTime::now(),
            )
            .expect("the valid cert must be accepted");
        assert!(
            verifier.get_rejection().is_none(),
            "acceptance must clear the recorded rejection"
        );
    }

    #[test]
    fn rejects_a_measurement_pin_mismatch() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);

        let report_data = key_bound_report_data(hash);
        let measurement = [0xAB; 48];

        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: signed_report_bytes(report_data, measurement),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        // Pin to a *different* measurement than the SIGNED report carries.
        let verifier = SnpCertVerifier::new(Some(vec![0xFF; 48]), None);
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        assert!(
            result.is_err(),
            "a measurement pin mismatch must fail closed"
        );
        assert!(
            verifier.get_report().is_none(),
            "a rejected handshake must not stash a report"
        );
        let rejection = verifier
            .get_rejection()
            .expect("a rejected handshake must record its reason for attested_request to surface");
        assert!(
            rejection.contains("measurement mismatch"),
            "recorded reason should name the failed check: {rejection}"
        );
    }

    #[test]
    fn rejects_a_key_binding_mismatch() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");

        // SIGNED report_data does NOT hash this cert's actual public key
        // (tampered / replayed from a different TLS session).
        let measurement = [0xAB; 48];
        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: signed_report_bytes([0u8; 64], measurement),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        // No measurement pin configured - this must still fail purely on
        // the key-binding check.
        let verifier = SnpCertVerifier::new(None, None);
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        assert!(result.is_err(), "a key binding mismatch must fail closed");
        assert!(
            verifier.get_report().is_none(),
            "a rejected handshake must not stash a report"
        );
        let rejection = verifier
            .get_rejection()
            .expect("rejection must be recorded");
        assert!(rejection.contains("key binding"), "{rejection}");
    }

    /// Regression for aleph-testnets#35 run 31433603211: the guest agent
    /// domain-separates the key-binding hash
    /// (`SHA-384(KEY_BINDING_DOMAIN || pubkey)`); a report bound with the
    /// plain, undomained `SHA-384(pubkey)` (what this SDK expected before)
    /// must NOT verify. This pins the domain tag on the verifier side: if
    /// either side drops or changes it, this test or the accept test breaks.
    #[test]
    fn rejects_a_key_binding_without_the_domain_tag() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let (_, cert) = x509_parser::parse_x509_certificate(&probe_der)
            .expect("cert should parse for the probe");
        let undomained: [u8; 48] =
            Sha384::digest(cert.tbs_certificate.subject_pki.subject_public_key.data).into();

        let measurement = [0xAB; 48];
        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: signed_report_bytes(key_bound_report_data(undomained), measurement),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        let verifier = SnpCertVerifier::new(None, None);
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        assert!(
            result.is_err(),
            "an undomained key-binding hash must be rejected"
        );
        let rejection = verifier
            .get_rejection()
            .expect("rejection must be recorded");
        assert!(rejection.contains("key binding"), "{rejection}");
    }

    /// Policy pin: the SIGNED report's guest policy must match the expected
    /// policy exactly. The policy is NOT part of the launch measurement, so
    /// a malicious host can launch the same measured stack with a weaker
    /// policy (e.g. debug allowed, which lets the host decrypt guest
    /// memory); only this check catches that.
    #[test]
    fn rejects_a_policy_mismatch_against_the_signed_report() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);

        let report_data = key_bound_report_data(hash);
        let measurement = [0xAB; 48];
        // Launched with debug allowed (bit 19) on top of the expected policy.
        let launched_policy = 0x30000_u64 | (1 << 19);

        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: signed_report_bytes_with_policy(report_data, measurement, launched_policy),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        // Key binding and measurement pin both match; only the policy
        // differs, so a rejection can only come from the policy check.
        let verifier = SnpCertVerifier::new(Some(measurement.to_vec()), Some(0x30000));
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        assert!(
            result.is_err(),
            "a guest policy mismatch must fail closed: the same measured stack \
             launched with a weaker policy is not the attested deployment"
        );
        assert!(
            verifier.get_report().is_none(),
            "a rejected handshake must not stash a report"
        );
    }

    #[test]
    fn accepts_a_matching_policy_pin() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);

        let report_data = key_bound_report_data(hash);
        let measurement = [0xAB; 48];

        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: signed_report_bytes_with_policy(report_data, measurement, 0x30000),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        let verifier = SnpCertVerifier::new(Some(measurement.to_vec()), Some(0x30000));
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        assert!(
            result.is_ok(),
            "a key-bound report with matching measurement and policy must be accepted: {result:?}"
        );
    }

    #[test]
    fn rejects_a_certificate_without_an_attestation_extension() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let cert_der = self_signed_der(&key_pair, None);

        let verifier = SnpCertVerifier::new(None, None);
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        assert!(
            result.is_err(),
            "a certificate with no attestation extension must fail closed"
        );
        let rejection = verifier
            .get_rejection()
            .expect("rejection must be recorded");
        assert!(rejection.contains("attestation extension"), "{rejection}");
    }

    #[test]
    fn a_successful_handshake_stashes_the_served_public_key() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);
        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: signed_report_bytes(key_bound_report_data(hash), [0xAB; 48]),
        };
        let ext = encode_attestation_extension(&report).unwrap();
        let der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext)));

        let verifier = SnpCertVerifier::new(None, None);
        verifier
            .verify_server_cert(
                &CertificateDer::from(der.clone()),
                &[],
                &dummy_server_name(),
                &[],
                UnixTime::now(),
            )
            .expect("the valid cert must be accepted");

        let (_, cert) = x509_parser::parse_x509_certificate(&der).unwrap();
        assert_eq!(
            verifier
                .get_served_public_key()
                .expect("key must be stashed"),
            cert.tbs_certificate
                .subject_pki
                .subject_public_key
                .data
                .to_vec(),
        );
    }

    /// The canonical fresh report_data for `(key, nonce)`, computed the same
    /// way the emitter does, used to synthesize valid fresh reports.
    #[test]
    fn accepts_a_correctly_bound_fresh_report() {
        let key = b"served-public-key".to_vec();
        let nonce = [0x42u8; 32];
        let rd = fresh_bound_report_data(&key, &nonce);
        let bytes = signed_report_bytes(rd, [0xAB; 48]);
        verify_fresh_binding(&bytes, &key, &nonce).expect("correct binding must verify");
    }

    #[test]
    fn rejects_a_fresh_report_with_the_wrong_nonce() {
        let key = b"served-public-key".to_vec();
        let rd = fresh_bound_report_data(&key, &[0x42u8; 32]);
        let bytes = signed_report_bytes(rd, [0xAB; 48]);
        let err = verify_fresh_binding(&bytes, &key, &[0x43u8; 32]).unwrap_err();
        assert!(matches!(err, AttestError::FreshnessBinding));
    }

    #[test]
    fn rejects_a_fresh_report_bound_to_a_different_key() {
        let nonce = [0x42u8; 32];
        let rd = fresh_bound_report_data(b"key-A", &nonce);
        let bytes = signed_report_bytes(rd, [0xAB; 48]);
        let err = verify_fresh_binding(&bytes, b"key-B", &nonce).unwrap_err();
        assert!(matches!(err, AttestError::FreshnessBinding));
    }

    /// Domain separation regression: a KEY-BOUND report must never satisfy a
    /// fresh challenge, even when the attacker picks the nonce so the hashed
    /// payloads would otherwise collide (the aleph-cvm confusion attack).
    #[test]
    fn rejects_a_key_bound_report_presented_as_fresh() {
        let key = b"served-public-key".to_vec();
        // A genuine key-bound report for this key (KEY_BINDING_DOMAIN scheme).
        let mut hasher = Sha384::new();
        hasher.update(KEY_BINDING_DOMAIN);
        hasher.update(&key);
        let bytes =
            signed_report_bytes(key_bound_report_data(hasher.finalize().into()), [0xAB; 48]);
        // No nonce can make it pass the fresh check, including an empty one.
        let err = verify_fresh_binding(&bytes, &key, &[]).unwrap_err();
        assert!(matches!(err, AttestError::FreshnessBinding));
    }

    fn dummy_verification(measurement_hex: &str, policy: u64) -> VerificationResult {
        VerificationResult {
            measurement: measurement_hex.to_string(),
            policy,
            launch_tcb: Default::default(),
            reported_tcb: Default::default(),
            cpuid_family: None,
            cpuid_model: None,
            cpuid_stepping: None,
            summary: String::new(),
        }
    }

    #[test]
    fn fresh_pins_accept_matching_measurement_and_policy() {
        let v = dummy_verification(&"ab".repeat(48), 0x30000);
        check_fresh_pins(&v, Some(&[0xAB; 48]), Some(0x30000)).expect("matching pins must pass");
        check_fresh_pins(&v, None, None).expect("absent pins must pass");
    }

    #[test]
    fn fresh_pins_reject_a_measurement_mismatch() {
        let v = dummy_verification(&"ab".repeat(48), 0x30000);
        let err = check_fresh_pins(&v, Some(&[0xCD; 48]), None).unwrap_err();
        assert!(matches!(err, AttestError::FreshMeasurementMismatch { .. }));
    }

    #[test]
    fn fresh_pins_reject_a_policy_mismatch() {
        let v = dummy_verification(&"ab".repeat(48), 0x30000);
        let err = check_fresh_pins(&v, None, Some(0xa0000)).unwrap_err();
        assert!(matches!(err, AttestError::FreshPolicyMismatch { .. }));
    }
}
