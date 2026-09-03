//! Owner authentication payload for confidential-instance secret injection.
//!
//! The client-side half of the wire contract implemented by aleph-vm's
//! in-guest attest-agent: an EIP-191 personal-sign signature over a payload
//! bound to the guest's per-boot TLS key,
//! `aleph-snp-inject-secret-v1|sha384(server public key)|sha256(canonical
//! secrets JSON)`, all hex lowercase. Binding to the per-boot key gives
//! channel binding and replay protection in one: a captured signed request
//! is only valid for that key, i.e. that boot of that VM. Signing itself
//! stays with the caller (the CLI account layer), which already applies
//! EIP-191 to raw buffers.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256, Sha384};

/// Domain separator of the inject-secret payload (wire contract).
pub const INJECT_SECRET_DOMAIN: &str = "aleph-snp-inject-secret-v1";

/// The exact bytes hashed into the payload body: compact JSON, sorted keys.
pub fn canonical_secrets_json(secrets: &BTreeMap<String, String>) -> String {
    serde_json::to_string(secrets).expect("a BTreeMap<String, String> always serializes")
}

/// Build the string the owner signs (EIP-191 personal-sign) to authorize
/// injecting `canonical_secrets_json` into the guest serving
/// `server_public_key_raw` as its attested TLS key.
pub fn inject_secret_payload(server_public_key_raw: &[u8], canonical_secrets_json: &str) -> String {
    let key_hash = hex::encode(Sha384::digest(server_public_key_raw));
    let body_hash = hex::encode(Sha256::digest(canonical_secrets_json.as_bytes()));
    format!("{INJECT_SECRET_DOMAIN}|{key_hash}|{body_hash}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
    use sha3::{Digest as Sha3Digest, Keccak256};
    use std::collections::BTreeMap;

    // Test-local twins of aleph-vm's aleph-tee owner_auth signing side. The
    // production client signs through the account layer; these exist so the
    // payload this module builds verifies against the exact agent algorithm.
    fn eip191_digest(payload: &str) -> [u8; 32] {
        let mut hasher = Keccak256::new();
        hasher.update(format!("\x19Ethereum Signed Message:\n{}", payload.len()));
        hasher.update(payload.as_bytes());
        hasher.finalize().into()
    }

    fn address_from_verifying_key(key: &VerifyingKey) -> String {
        let uncompressed = key.to_encoded_point(false);
        let digest = Keccak256::digest(&uncompressed.as_bytes()[1..]);
        format!("0x{}", hex::encode(&digest[12..]))
    }

    fn sign_payload(signing_key: &SigningKey, payload: &str) -> String {
        let digest = eip191_digest(payload);
        let (signature, recovery_id) = signing_key
            .sign_prehash_recoverable(&digest)
            .expect("signing a 32-byte prehash cannot fail");
        let mut bytes = signature.to_bytes().to_vec();
        bytes.push(27 + recovery_id.to_byte());
        format!("0x{}", hex::encode(bytes))
    }

    fn recover_address(payload: &str, signature_hex: &str) -> String {
        let bytes = hex::decode(signature_hex.strip_prefix("0x").unwrap()).unwrap();
        let recovery = match bytes[64] {
            27 | 28 => bytes[64] - 27,
            v => v,
        };
        let signature = Signature::from_slice(&bytes[..64]).unwrap();
        let key = VerifyingKey::recover_from_prehash(
            &eip191_digest(payload),
            &signature,
            RecoveryId::try_from(recovery).unwrap(),
        )
        .unwrap();
        address_from_verifying_key(&key)
    }

    fn test_key() -> SigningKey {
        SigningKey::from_slice(&[0x42u8; 32]).expect("valid scalar")
    }

    fn secrets() -> BTreeMap<String, String> {
        BTreeMap::from([("luks_passphrase".to_string(), "hunter2".to_string())])
    }

    #[test]
    fn canonical_json_is_sorted_and_compact() {
        let mut map = BTreeMap::new();
        map.insert("b".to_string(), "2".to_string());
        map.insert("a".to_string(), "1".to_string());
        assert_eq!(canonical_secrets_json(&map), r#"{"a":"1","b":"2"}"#);
    }

    #[test]
    fn payload_shape_is_stable() {
        // Wire contract shared with aleph-vm's attest-agent; lock its shape.
        let payload = inject_secret_payload(b"k", "{}");
        let parts: Vec<&str> = payload.split('|').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], INJECT_SECRET_DOMAIN);
        assert_eq!(parts[1].len(), 96); // sha384 hex
        assert_eq!(parts[2].len(), 64); // sha256 hex
    }

    #[test]
    fn signed_payload_recovers_the_signer() {
        let key = test_key();
        let owner = address_from_verifying_key(key.verifying_key());
        let payload =
            inject_secret_payload(b"server-pubkey-raw", &canonical_secrets_json(&secrets()));
        let sig = sign_payload(&key, &payload);
        assert_eq!(recover_address(&payload, &sig), owner);
    }

    #[test]
    fn different_server_key_changes_the_payload() {
        let json = canonical_secrets_json(&secrets());
        assert_ne!(
            inject_secret_payload(b"server-pubkey-raw", &json),
            inject_secret_payload(b"OTHER-server-key", &json),
        );
    }
}
