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
//! - the key-binding check fails (`report_data != SHA-384(pubkey) || zeros`),
//! - an `expected_measurement` was given and doesn't match.
//!
//! Crucially, the key-binding and measurement checks are made against the
//! fields parsed out of the AMD-signed report bytes (`data`), *not* the DTO's
//! sibling `report_data`/`measurement` fields. Those siblings are decoded
//! straight from attacker-controlled JSON in the certificate extension and are
//! not covered by the AMD signature, so trusting them would let a malicious
//! node serve a genuine signed report while lying about which TLS key it is
//! bound to (or which measurement it carries). See the C1 regression tests.
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

use super::verify::{AmdProduct, verify_sev_snp_report};
use super::x509::{AttestError, extract_attestation_from_cert};
use super::{AttestationReport, TeeType};

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
    /// The HTTP status code of the response.
    pub status: u16,
    /// The HTTP response headers, in wire order (a header repeated multiple
    /// times yields multiple entries with the same name).
    pub headers: Vec<(String, String)>,
    /// The raw HTTP response body.
    pub body: bytes::Bytes,
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
    expected_measurement: Option<Vec<u8>>,
    provider: Arc<CryptoProvider>,
}

impl SnpCertVerifier {
    /// Create a new verifier wrapped in an `Arc` for use with `rustls`.
    ///
    /// If `expected_measurement` is `Some`, the handshake is rejected unless
    /// the report's measurement matches exactly (a "measurement pin").
    fn new(expected_measurement: Option<Vec<u8>>) -> Arc<Self> {
        Arc::new(Self {
            extracted_report: Mutex::new(None),
            last_rejection: Mutex::new(None),
            expected_measurement,
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
        //    security check below uses these SIGNED fields, never the DTO's
        //    sibling `report_data`/`measurement`: the AMD signature covers
        //    report bytes 0x000..0x2A0 (which include the report's own
        //    report_data and measurement), but the DTO siblings are just
        //    JSON in the cert extension and are trivially forgeable. The
        //    signature over `data` itself is checked post-handshake in
        //    `verify_sev_snp_report`; a forged `data` fails closed there, so
        //    checking the parsed fields here (pre-signature) is still sound.
        //    Fail closed if the bytes don't parse.
        let signed = SnpReport::from_bytes(&report.data).map_err(|e| {
            RustlsError::General(format!(
                "failed to parse SEV-SNP attestation report from certificate data: {e}"
            ))
        })?;

        // 3. Key binding: the SIGNED report_data must equal
        //    SHA-384(public_key) || zeros. This proves the report was
        //    generated for *this* TLS key, not replayed from a different
        //    (possibly still-valid) one.
        let (_, cert) = x509_parser::parse_x509_certificate(end_entity.as_ref()).map_err(|e| {
            RustlsError::General(format!("failed to parse certificate for key binding: {e}"))
        })?;
        let public_key_bytes = cert.tbs_certificate.subject_pki.subject_public_key.data;

        let hash = Sha384::digest(public_key_bytes);
        let mut expected_report_data = [0u8; 64];
        expected_report_data[..48].copy_from_slice(&hash);

        if signed.report_data != expected_report_data {
            return Err(RustlsError::General(format!(
                "key binding verification failed: report_data does not match SHA-384(public_key). \
                 expected {}, got {}",
                hex::encode(expected_report_data),
                hex::encode(signed.report_data),
            )));
        }

        // 4. Optional measurement pin, against the SIGNED measurement.
        if let Some(ref expected) = self.expected_measurement
            && signed.measurement.as_slice() != expected.as_slice()
        {
            return Err(RustlsError::General(format!(
                "measurement mismatch: expected {}, got {}",
                hex::encode(expected),
                hex::encode(signed.measurement),
            )));
        }

        *self.extracted_report.lock().unwrap() = Some(report);
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
pub async fn attested_request(
    base_url: &url::Url,
    method: reqwest::Method,
    path: &str,
    headers: &[(String, String)],
    body: Option<bytes::Bytes>,
    expected_measurement: Option<&[u8]>,
    product: AmdProduct,
) -> Result<AttestedResponse, AttestError> {
    let url = base_url.join(path)?;

    let verifier = SnpCertVerifier::new(expected_measurement.map(<[u8]>::to_vec));
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

    // Propagate on failure rather than degrading to a partial response: an
    // unverifiable report must never look like a successful response.
    let result = verify_sev_snp_report(&report, product).await?;

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
        status,
        headers: response_headers,
        body,
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

    /// The 64-byte `REPORT_DATA` value that binds a report to a TLS key whose
    /// SPKI hashes to `hash`: `SHA-384(pubkey) || zeros[16]`.
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

    /// SHA-384 of the SubjectPublicKeyInfo `BIT STRING` content of `der`, as
    /// `verify_server_cert`'s key-binding check computes it.
    fn subject_pubkey_sha384(der: &[u8]) -> [u8; 48] {
        let (_, cert) =
            x509_parser::parse_x509_certificate(der).expect("cert should parse for the probe");
        Sha384::digest(cert.tbs_certificate.subject_pki.subject_public_key.data).into()
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
            report_data,
            measurement: measurement.to_vec(),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        let verifier = SnpCertVerifier::new(Some(measurement.to_vec()));
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

        // SIGNED report bytes carry the key-bound report_data and the pinned
        // measurement; the DTO siblings mirror them (honest node).
        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: signed_report_bytes(report_data, measurement),
            report_data,
            measurement: measurement.to_vec(),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        let verifier = SnpCertVerifier::new(Some(measurement.to_vec()));
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
        assert_eq!(stashed.measurement, measurement.to_vec());
        assert_eq!(stashed.report_data, report_data);
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
            report_data,
            measurement: measurement.to_vec(),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let good_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));
        // No extension at all: guaranteed rejection.
        let bad_der = self_signed_der(&key_pair, None);

        let verifier = SnpCertVerifier::new(Some(measurement.to_vec()));
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
            report_data,
            measurement: measurement.to_vec(),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        // Pin to a *different* measurement than the SIGNED report carries.
        let verifier = SnpCertVerifier::new(Some(vec![0xFF; 48]));
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
            report_data: [0u8; 64],
            measurement: measurement.to_vec(),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        // No measurement pin configured - this must still fail purely on
        // the key-binding check.
        let verifier = SnpCertVerifier::new(None);
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

    /// C1 regression (measurement): the SIGNED report carries measurement
    /// `M_signed`, but the DTO sibling lies with `M_expected` that matches the
    /// pin. The old, vulnerable code pinned against the DTO sibling and would
    /// have accepted this; the fix pins against the SIGNED measurement, so it
    /// must reject. Revert step 4 to use `report.measurement` and this fails.
    #[test]
    fn rejects_when_dto_measurement_lies_to_match_the_pin() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);

        let report_data = key_bound_report_data(hash);
        let m_signed = [0x11; 48];
        let m_expected = [0x22; 48]; // what the node claims / what is pinned
        assert_ne!(m_signed, m_expected);

        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            // Genuine signed bytes: key-bound, measurement = m_signed.
            data: signed_report_bytes(report_data, m_signed),
            // DTO siblings: report_data honest, measurement is the LIE.
            report_data,
            measurement: m_expected.to_vec(),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        // Pin to m_expected - which the DTO sibling matches but the SIGNED
        // report does not.
        let verifier = SnpCertVerifier::new(Some(m_expected.to_vec()));
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        assert!(
            result.is_err(),
            "measurement pin must be checked against the SIGNED report, not the DTO sibling"
        );
        assert!(verifier.get_report().is_none());
        let rejection = verifier
            .get_rejection()
            .expect("rejection must be recorded");
        assert!(rejection.contains("measurement mismatch"), "{rejection}");
    }

    /// C1 regression (key binding): the SIGNED report_data is NOT bound to
    /// this TLS key, but the DTO sibling lies with the correct
    /// `SHA-384(pubkey)`. The old code key-bound against the DTO sibling and
    /// would have accepted; the fix key-binds against the SIGNED report_data,
    /// so it must reject. Revert step 3 to use `report.report_data` and this
    /// fails.
    #[test]
    fn rejects_when_dto_report_data_lies_to_match_the_key() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);

        let honest_binding = key_bound_report_data(hash);
        let measurement = [0xAB; 48];

        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            // SIGNED report_data is some other value (NOT bound to this key).
            data: signed_report_bytes([0x33; 64], measurement),
            // DTO sibling LIES: it carries the correct key binding.
            report_data: honest_binding,
            measurement: measurement.to_vec(),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        // Pin the (matching) measurement so only the key binding can fail.
        let verifier = SnpCertVerifier::new(Some(measurement.to_vec()));
        let result = verifier.verify_server_cert(
            &CertificateDer::from(cert_der),
            &[],
            &dummy_server_name(),
            &[],
            UnixTime::now(),
        );

        assert!(
            result.is_err(),
            "key binding must be checked against the SIGNED report, not the DTO sibling"
        );
        assert!(verifier.get_report().is_none());
        let rejection = verifier
            .get_rejection()
            .expect("rejection must be recorded");
        assert!(rejection.contains("key binding"), "{rejection}");
    }

    #[test]
    fn rejects_a_certificate_without_an_attestation_extension() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let cert_der = self_signed_der(&key_pair, None);

        let verifier = SnpCertVerifier::new(None);
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
}
