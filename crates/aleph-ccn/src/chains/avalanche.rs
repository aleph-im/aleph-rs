//! Avalanche signature verification. Mirrors `aleph/chains/avalanche.py`.
//!
//! Avalanche X-chain signatures: base58-encoded ECDSA recoverable signature
//! over a wrapper of the verification buffer:
//!   `\x1aAvalanche Signed Message:\n<len-be-u32><buffer>`
//! After recovery, the public key is hashed (SHA256 -> RIPEMD160) and
//! encoded as bech32 with the appropriate HRP, then compared to the sender
//! address (`<chain_id>-<bech32>`).

use async_trait::async_trait;
use bech32::Hrp;
use k256::ecdsa::{RecoveryId, Signature as K256Signature, VerifyingKey};
use ripemd::Ripemd160;
use sha2::{Digest, Sha256};

use super::abc::{PendingMessageView, Verifier};
use super::common::verification_buffer;
use crate::AlephResult;

const MESSAGE_PREFIX: &[u8] = b"\x1aAvalanche Signed Message:\n";

/// Verifier for Avalanche X-chain addresses.
#[derive(Default, Debug, Clone, Copy)]
pub struct AvalancheConnector;

#[async_trait]
impl Verifier for AvalancheConnector {
    async fn verify_signature(&self, message: &dyn PendingMessageView) -> AlephResult<bool> {
        let Some(signature_b58) = message.signature() else {
            tracing::warn!(item_hash = %message.item_hash(), "Avalanche: missing signature");
            return Ok(false);
        };

        let (chain_id, hrp) = match parse_sender(message.sender()) {
            Ok(parts) => parts,
            Err(e) => {
                tracing::warn!(error = %e, "Avalanche sender address deserialization error");
                return Ok(false);
            }
        };

        let sig_bytes = match bs58::decode(signature_b58).into_vec() {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!("Avalanche signature base58 decode error");
                return Ok(false);
            }
        };

        // Trim 4-byte trailing checksum (last 4 bytes are SHA256(rest)[-4:]).
        if sig_bytes.len() < 5 {
            return Ok(false);
        }
        let (payload, checksum) = sig_bytes.split_at(sig_bytes.len() - 4);
        let expected_checksum = &Sha256::digest(payload)[28..32];
        if checksum != expected_checksum {
            tracing::warn!("Avalanche signature checksum error");
            return Ok(false);
        }

        if payload.len() != 65 {
            tracing::warn!("Avalanche signature: expected 65 bytes after checksum trim");
            return Ok(false);
        }

        // Recover the public key from the signature.
        let buffer = verification_buffer(message);
        let mut wrapped = Vec::with_capacity(MESSAGE_PREFIX.len() + 4 + buffer.len());
        wrapped.extend_from_slice(MESSAGE_PREFIX);
        wrapped.extend_from_slice(&(buffer.len() as u32).to_be_bytes());
        wrapped.extend_from_slice(&buffer);

        // Avalanche uses raw double-SHA-style: coincurve.PublicKey
        // .from_signature_and_message expects the message itself, not a
        // prehash, and applies SHA-256 internally.
        let digest = Sha256::digest(&wrapped);

        let recovery_id = match RecoveryId::try_from(payload[64]) {
            Ok(r) => r,
            Err(_) => return Ok(false),
        };
        let k_sig = match K256Signature::from_slice(&payload[..64]) {
            Ok(s) => s,
            Err(_) => return Ok(false),
        };
        let key = match VerifyingKey::recover_from_prehash(&digest, &k_sig, recovery_id) {
            Ok(k) => k,
            Err(_) => return Ok(false),
        };

        // Compressed public key (33 bytes).
        let compressed = key.to_encoded_point(true);
        let pubkey_bytes = compressed.as_bytes();
        let derived = address_to_string(&chain_id, &hrp, &address_from_public_key(pubkey_bytes));

        Ok(derived == message.sender())
    }
}

fn parse_sender(sender: &str) -> Result<(String, String), &'static str> {
    let (chain_id, rest) = sender.split_once('-').ok_or("missing chain id separator")?;
    let (hrp, _) = rest.split_once('1').ok_or("missing bech32 1 separator")?;
    Ok((chain_id.to_string(), hrp.to_string()))
}

fn address_from_public_key(pubk: &[u8]) -> [u8; 20] {
    // SHA-256 then RIPEMD-160 — same as Bitcoin's hash160.
    let sha = Sha256::digest(pubk);
    let ripe = Ripemd160::digest(sha);
    let mut out = [0u8; 20];
    out.copy_from_slice(&ripe);
    out
}

fn address_to_string(chain_id: &str, _hrp: &str, address: &[u8; 20]) -> String {
    // Python uses bech32_encode("avax", bits) regardless of HRP — match that.
    let hrp = Hrp::parse("avax").expect("static hrp");
    let encoded = bech32::encode::<bech32::Bech32>(hrp, address).expect("bech32 encode");
    format!("{}-{}", chain_id, encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::abc::SimplePendingMessage;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;

    #[test]
    fn sender_parser_extracts_chain_and_hrp() {
        let (chain, hrp) = parse_sender("X-avax1h4kqfeyhfh4l9zpdg8mlxqe2x3v4uwjcfpv8nq").unwrap();
        assert_eq!(chain, "X");
        assert_eq!(hrp, "avax");
    }

    #[tokio::test]
    async fn missing_signature_returns_false() {
        let msg = SimplePendingMessage {
            chain: Chain::Avax,
            sender: "X-avax1h4kqfeyhfh4l9zpdg8mlxqe2x3v4uwjcfpv8nq".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: None,
            time_seconds: 0.0,
        };
        assert!(!AvalancheConnector.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn malformed_signature_returns_false() {
        let msg = SimplePendingMessage {
            chain: Chain::Avax,
            sender: "X-avax1h4kqfeyhfh4l9zpdg8mlxqe2x3v4uwjcfpv8nq".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: Some("not-base58!".into()),
            time_seconds: 0.0,
        };
        assert!(!AvalancheConnector.verify_signature(&msg).await.unwrap());
    }
}
