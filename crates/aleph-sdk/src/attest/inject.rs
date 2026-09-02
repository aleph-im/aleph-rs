//! Owner-authenticated secret injection over RA-TLS (the POST half of the
//! wire contract whose payload builder lives in [`super::owner_auth`]).
//!
//! The caller signs a payload built by `owner_auth::inject_secret_payload`
//! and posts it, alongside the plaintext secrets, to the guest agent's
//! `/confidential/inject-secret` endpoint over an attested channel. The
//! agent verifies the signature recovers the expected owner address before
//! decrypting and injecting the secrets, so the request body carries both
//! the secrets and the signature together.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::platform::PlatformPolicy;
use super::ratls::{AttestedResponse, MeasurementPin, PolicyPin, attested_request};
use super::tcb::TcbFloorPolicy;
use super::verify::AmdProduct;
use super::x509::AttestError;

/// Request body for `POST /confidential/inject-secret`: the plaintext
/// secrets keyed by name, plus the EIP-191 signature over the payload
/// `owner_auth::inject_secret_payload` builds from them and the guest's
/// served TLS key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InjectSecretEnvelope {
    pub secrets: BTreeMap<String, String>,
    pub signature: String,
}

/// Response body for a successful injection: the names of the secrets the
/// agent accepted and injected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InjectSecretResponse {
    pub injected: Vec<String>,
}

/// POST a signed [`InjectSecretEnvelope`] to the guest agent's
/// `/confidential/inject-secret` endpoint over an attested TLS channel, and
/// parse its response.
///
/// # Two-step contract
///
/// This call is the second half of a two-step flow the caller drives:
///
/// 1. Call `fresh_attestation` (this module's sibling in [`super::ratls`])
///    to obtain and verify the guest's current `served_public_key`.
/// 2. Build the payload with `owner_auth::inject_secret_payload` over that
///    key and the canonical secrets JSON, sign it, wrap it in an
///    [`InjectSecretEnvelope`], and call `post_secrets`.
///
/// The two steps are not atomic: if the guest reboots in between, its TLS
/// key changes, and the agent rejects the envelope because the signature no
/// longer covers the key `post_secrets`'s own handshake now sees. That
/// rejection surfaces as [`AttestError::InjectRejected`] with the agent's
/// error body (which names the stale-key/signature mismatch); the caller's
/// only correct response is to retry from step 1, not to retry the same
/// envelope. The payload's key binding is what makes this window safe: a
/// captured, replayed envelope can never inject into a different boot.
///
/// On success, returns both the parsed [`InjectSecretResponse`] and the
/// [`AttestedResponse`] the request was answered over, so the caller can
/// print the same attestation verification summary other attested calls
/// print (measurement, policy, TCB, platform posture) from one exchange.
pub async fn post_secrets(
    base_url: &url::Url,
    envelope: &InjectSecretEnvelope,
    measurement: MeasurementPin<'_>,
    policy: PolicyPin,
    product: AmdProduct,
    min_tcb: &TcbFloorPolicy,
    platform: &PlatformPolicy,
) -> Result<(InjectSecretResponse, AttestedResponse), AttestError> {
    let body = serde_json::to_vec(envelope)?;

    let response = attested_request(
        base_url,
        reqwest::Method::POST,
        "/confidential/inject-secret",
        &[("content-type".to_string(), "application/json".to_string())],
        Some(body.into()),
        measurement,
        policy,
        product,
        min_tcb,
        platform,
    )
    .await?;

    // The agent's 4xx bodies name the specific rejection (most notably a
    // stale-key signature after a guest reboot between fresh_attestation and
    // this call), so that text must reach the caller rather than being
    // discarded in favor of a bare status code.
    if !(200..300).contains(&response.status) {
        return Err(AttestError::InjectRejected {
            status: response.status,
            body: String::from_utf8_lossy(&response.body).into_owned(),
        });
    }

    let parsed: InjectSecretResponse = serde_json::from_slice(&response.body)?;
    Ok((parsed, response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn envelope_serializes_to_the_agent_contract() {
        let envelope = InjectSecretEnvelope {
            secrets: BTreeMap::from([("luks_passphrase".into(), "hunter2".into())]),
            signature: "0xabcd".into(),
        };
        assert_eq!(
            serde_json::to_string(&envelope).unwrap(),
            r#"{"secrets":{"luks_passphrase":"hunter2"},"signature":"0xabcd"}"#
        );
    }

    #[test]
    fn response_parses_injected_keys() {
        let r: InjectSecretResponse =
            serde_json::from_str(r#"{"injected":["luks_passphrase"]}"#).unwrap();
        assert_eq!(r.injected, vec!["luks_passphrase".to_string()]);
    }
}
