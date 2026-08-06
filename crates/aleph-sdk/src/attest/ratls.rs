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
//! [`SnpCertVerifier::verify_server_cert`] rejects the handshake (`Err`) if:
//! - the certificate has no attestation extension,
//! - the key-binding check fails (`report_data != SHA-384(pubkey) || zeros`),
//! - an `expected_measurement` was given and doesn't match.
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
use sha2::{Digest, Sha384};

use super::AttestationReport;
use super::verify::{AmdProduct, verify_sev_snp_report};
use super::x509::{AttestError, extract_attestation_from_cert};

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
            expected_measurement,
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        })
    }

    /// The report stashed by a completed, successful handshake, if any.
    fn get_report(&self) -> Option<AttestationReport> {
        self.extracted_report.lock().unwrap().clone()
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

        // 2. Key binding: report_data must equal SHA-384(public_key) ||
        //    zeros. This proves the report was generated for *this* TLS key,
        //    not replayed from a different (possibly still-valid) one.
        let (_, cert) = x509_parser::parse_x509_certificate(end_entity.as_ref()).map_err(|e| {
            RustlsError::General(format!("failed to parse certificate for key binding: {e}"))
        })?;
        let public_key_bytes = cert.tbs_certificate.subject_pki.subject_public_key.data;

        let hash = Sha384::digest(public_key_bytes);
        let mut expected_report_data = [0u8; 64];
        expected_report_data[..48].copy_from_slice(&hash);

        if report.report_data != expected_report_data {
            return Err(RustlsError::General(format!(
                "key binding verification failed: report_data does not match SHA-384(public_key). \
                 expected {}, got {}",
                hex::encode(expected_report_data),
                hex::encode(report.report_data),
            )));
        }

        // 3. Optional measurement pin.
        if let Some(ref expected) = self.expected_measurement
            && report.measurement != *expected
        {
            return Err(RustlsError::General(format!(
                "measurement mismatch: expected {}, got {}",
                hex::encode(expected),
                hex::encode(&report.measurement),
            )));
        }

        *self.extracted_report.lock().unwrap() = Some(report);
        Ok(ServerCertVerified::assertion())
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

    let response = request.send().await.map_err(AttestError::Http)?;

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

    // Fail closed: a successful handshake against `SnpCertVerifier` always
    // stashes a report (it errors the handshake otherwise), but check again
    // rather than trust that invariant silently.
    let report = verifier.get_report().ok_or(AttestError::MissingReport)?;

    // Propagate on failure rather than degrading to a partial response: an
    // unverifiable report must never look like a successful response.
    let result = verify_sev_snp_report(&report, product).await?;

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
    fn accepts_a_cert_whose_report_is_key_bound_and_measurement_matches() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        // Probe cert (same key_pair) purely to learn the SPKI bytes rcgen
        // will embed, so we can compute the expected report_data hash before
        // building the real, extension-carrying cert.
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);

        let mut report_data = [0u8; 64];
        report_data[..48].copy_from_slice(&hash);
        let measurement = vec![0xAB; 48];

        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: vec![0x01, 0x02, 0x03],
            report_data,
            measurement: measurement.clone(),
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        let verifier = SnpCertVerifier::new(Some(measurement.clone()));
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
        assert_eq!(stashed.measurement, measurement);
        assert_eq!(stashed.report_data, report_data);
    }

    #[test]
    fn rejects_a_measurement_pin_mismatch() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");
        let probe_der = self_signed_der(&key_pair, None);
        let hash = subject_pubkey_sha384(&probe_der);

        let mut report_data = [0u8; 64];
        report_data[..48].copy_from_slice(&hash);

        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: vec![],
            report_data,
            measurement: vec![0xAB; 48],
        };
        let ext_value = encode_attestation_extension(&report).expect("encoding should succeed");
        let cert_der = self_signed_der(&key_pair, Some((ATTESTATION_OID, ext_value)));

        // Pin to a *different* measurement than the report actually carries.
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
    }

    #[test]
    fn rejects_a_key_binding_mismatch() {
        let key_pair = rcgen::KeyPair::generate().expect("key generation should succeed");

        // report_data does NOT hash this cert's actual public key (tampered
        // / replayed from a different TLS session).
        let report = AttestationReport {
            tee_type: TeeType::SevSnp,
            data: vec![],
            report_data: [0u8; 64],
            measurement: vec![0xAB; 48],
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
    }
}
