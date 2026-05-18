//! EVM signature verification. Mirrors `aleph/chains/evm.py`.
//!
//! Inlines the same EIP-191 + secp256k1 recovery the canonical
//! `aleph_types::verify_signature::ethereum` module does (its
//! `recover_address` is not publicly exported, so we replicate it here
//! and keep the algorithm identical).

use async_trait::async_trait;
use k256::ecdsa::{RecoveryId, Signature as K256Signature, VerifyingKey};
use sha3::{Digest, Keccak256};

use super::abc::{PendingMessageView, Verifier};
use super::common::verification_buffer;
use crate::AlephResult;

/// Verifier for EVM chains. Recovers the signer address via EIP-191 and
/// compares it to `message.sender` case-insensitively (Python uses
/// `.lower()`).
#[derive(Default, Debug, Clone, Copy)]
pub struct EvmVerifier;

#[async_trait]
impl Verifier for EvmVerifier {
    async fn verify_signature(&self, message: &dyn PendingMessageView) -> AlephResult<bool> {
        let Some(signature) = message.signature() else {
            tracing::warn!(item_hash = %message.item_hash(), "EVM: missing signature");
            return Ok(false);
        };

        let buffer = verification_buffer(message);

        let recovered = match recover_eth_address(&buffer, signature) {
            Ok(addr) => addr,
            Err(e) => {
                tracing::warn!(
                    sender = message.sender(),
                    error = %e,
                    "EVM: error processing signature",
                );
                return Ok(false);
            }
        };

        if recovered.eq_ignore_ascii_case(message.sender()) {
            Ok(true)
        } else {
            tracing::warn!(
                recovered = %recovered,
                sender = message.sender(),
                "EVM: bad signature",
            );
            Ok(false)
        }
    }
}

/// Recovers the Ethereum address (EIP-55 checksummed) that signed the
/// EIP-191 personal-sign hash of `message`.
///
/// This mirrors `aleph_types::verify_signature::ethereum::recover_address`,
/// which is not publicly exported from the types crate.
pub(crate) fn recover_eth_address(message: &[u8], signature_hex: &str) -> Result<String, String> {
    let sig_bytes = decode_signature(signature_hex)?;
    let (r_s, v) = sig_bytes.split_at(64);

    let recovery_id = normalize_v(v[0])?;
    let signature = K256Signature::from_slice(r_s).map_err(|e| e.to_string())?;

    let digest = eip191_hash(message);

    let verifying_key = VerifyingKey::recover_from_prehash(&digest, &signature, recovery_id)
        .map_err(|e| e.to_string())?;

    Ok(public_key_to_address(&verifying_key))
}

fn decode_signature(hex_str: &str) -> Result<[u8; 65], String> {
    let hex_str = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = hex::decode(hex_str).map_err(|e| e.to_string())?;
    bytes
        .try_into()
        .map_err(|v: Vec<u8>| format!("expected 65 signature bytes, got {}", v.len()))
}

fn normalize_v(v: u8) -> Result<RecoveryId, String> {
    let id = match v {
        0 | 1 => v,
        27 | 28 => v - 27,
        _ => return Err(format!("unexpected recovery id byte: {v}")),
    };
    RecoveryId::try_from(id).map_err(|e| e.to_string())
}

fn eip191_hash(message: &[u8]) -> [u8; 32] {
    let prefix = format!("\x19Ethereum Signed Message:\n{}", message.len());
    let mut hasher = Keccak256::new();
    hasher.update(prefix.as_bytes());
    hasher.update(message);
    hasher.finalize().into()
}

fn public_key_to_address(key: &VerifyingKey) -> String {
    let uncompressed = key.to_encoded_point(false);
    let public_key_bytes = &uncompressed.as_bytes()[1..];
    let hash = Keccak256::digest(public_key_bytes);
    eip55_checksum(&hash[12..])
}

fn eip55_checksum(address_bytes: &[u8]) -> String {
    let hex_addr = hex::encode(address_bytes);
    let hash = Keccak256::digest(hex_addr.as_bytes());

    let mut out = String::with_capacity(42);
    out.push_str("0x");
    for (i, c) in hex_addr.chars().enumerate() {
        if c.is_ascii_alphabetic() {
            let hash_byte = hash[i / 2];
            let nibble = if i % 2 == 0 {
                hash_byte >> 4
            } else {
                hash_byte & 0x0f
            };
            if nibble >= 8 {
                out.push(c.to_ascii_uppercase());
            } else {
                out.push(c);
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::abc::SimplePendingMessage;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;

    #[tokio::test]
    async fn verify_post_fixture() {
        let msg = SimplePendingMessage {
            chain: Chain::Ethereum,
            sender: "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".into(),
            message_type: MessageType::Post,
            item_hash: "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c".into(),
            signature: Some("0x636728dbea33fa6064f099045421b980dff3c71932cd89c2bf42387ebb6f53890a24e13f16678463876224772b90838c2b9557cd8fc2cdae45509ce8cb89e3fd1b".into()),
            time_seconds: 1762515431.653,
        };

        let v = EvmVerifier;
        assert!(v.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn verify_rejects_wrong_sender() {
        let msg = SimplePendingMessage {
            chain: Chain::Ethereum,
            sender: "0x0000000000000000000000000000000000000000".into(),
            message_type: MessageType::Post,
            item_hash: "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c".into(),
            signature: Some("0x636728dbea33fa6064f099045421b980dff3c71932cd89c2bf42387ebb6f53890a24e13f16678463876224772b90838c2b9557cd8fc2cdae45509ce8cb89e3fd1b".into()),
            time_seconds: 1762515431.653,
        };
        let v = EvmVerifier;
        assert!(!v.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn verify_missing_signature_returns_false() {
        let msg = SimplePendingMessage {
            chain: Chain::Ethereum,
            sender: "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".into(),
            message_type: MessageType::Post,
            item_hash: "deadbeef".into(),
            signature: None,
            time_seconds: 0.0,
        };
        let v = EvmVerifier;
        assert!(!v.verify_signature(&msg).await.unwrap());
    }
}
