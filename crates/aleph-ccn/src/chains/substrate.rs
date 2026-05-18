//! Substrate (DOT/Polkadot) signature verification.
//! Mirrors `aleph/chains/substrate.py`.
//!
//! Signature payload is a JSON object:
//!   { "data": "0x<hex sig>", "curve": "sr25519" }
//!
//! The sender is a SS58-encoded address. We decode it, derive the
//! sr25519 (Schnorrkel) public key, and verify the signature against
//! the verification buffer using the canonical wallet context
//! `b"substrate"`.

use async_trait::async_trait;
use schnorrkel::{PublicKey, Signature as SrSignature, signing_context};
use serde::Deserialize;

use super::abc::{PendingMessageView, Verifier};
use super::common::verification_buffer;
use crate::AlephResult;

/// Verifier for Substrate-based chains. Defaults to sr25519.
#[derive(Default, Debug, Clone, Copy)]
pub struct SubstrateConnector;

#[derive(Deserialize)]
struct SubstrateSig {
    data: String,
    #[serde(default)]
    curve: Option<String>,
}

#[async_trait]
impl Verifier for SubstrateConnector {
    async fn verify_signature(&self, message: &dyn PendingMessageView) -> AlephResult<bool> {
        let Some(payload) = message.signature() else {
            tracing::warn!(item_hash = %message.item_hash(), "Substrate: missing signature");
            return Ok(false);
        };

        let parsed: SubstrateSig = match serde_json::from_str(payload) {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!("Substrate signature deserialization error");
                return Ok(false);
            }
        };

        if parsed.curve.as_deref().unwrap_or("sr25519") != "sr25519" {
            tracing::warn!(
                curve = parsed.curve.unwrap_or_default(),
                "Substrate: unsupported curve",
            );
            return Ok(false);
        }

        let sig_hex = parsed.data.strip_prefix("0x").unwrap_or(&parsed.data);
        let sig_bytes = match hex::decode(sig_hex) {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!("Substrate signature hex decode error");
                return Ok(false);
            }
        };
        let signature = match SrSignature::from_bytes(&sig_bytes) {
            Ok(s) => s,
            Err(_) => {
                tracing::warn!("Substrate signature: bad length");
                return Ok(false);
            }
        };

        let pub_bytes = match ss58_to_public(message.sender()) {
            Some(b) => b,
            None => {
                tracing::warn!("Substrate sender SS58 decode error");
                return Ok(false);
            }
        };
        let public = match PublicKey::from_bytes(&pub_bytes) {
            Ok(p) => p,
            Err(_) => return Ok(false),
        };

        let buffer = verification_buffer(message);
        let ctx = signing_context(b"substrate");
        Ok(public.verify(ctx.bytes(&buffer), &signature).is_ok())
    }
}

/// Decodes an SS58 address into its 32-byte public key.
///
/// Supports both the 1-byte (network prefix < 64) and 2-byte (network prefix
/// 64..=16383) prefix forms. Matches the discriminator used by
/// `substrate-interface`'s SS58 codec: if the first decoded byte's bit 6 is
/// set, the prefix occupies two bytes; otherwise it's a single byte.
///
/// Layout: `[prefix (1-2 bytes)] [public key 32 bytes] [blake2 checksum 2 bytes]`.
fn ss58_to_public(addr: &str) -> Option<[u8; 32]> {
    use blake2::{Blake2b512, Digest};
    let raw = bs58::decode(addr).into_vec().ok()?;
    // Minimum is 35 (1-byte prefix); 2-byte form is 36.
    if raw.len() < 35 {
        return None;
    }
    let (prefix_len, pubkey_offset) = if raw[0] & 0b0100_0000 == 0 {
        (1usize, 1usize)
    } else {
        (2usize, 2usize)
    };
    if raw.len() != prefix_len + 32 + 2 {
        return None;
    }
    let payload = &raw[..prefix_len + 32];
    let checksum = &raw[prefix_len + 32..];

    let mut hasher = Blake2b512::new();
    hasher.update(b"SS58PRE");
    hasher.update(payload);
    let hash = hasher.finalize();
    if &hash[..2] != checksum {
        return None;
    }

    let mut out = [0u8; 32];
    out.copy_from_slice(&raw[pubkey_offset..pubkey_offset + 32]);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::abc::SimplePendingMessage;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;

    #[tokio::test]
    async fn missing_signature_returns_false() {
        let msg = SimplePendingMessage {
            chain: Chain::Polkadot,
            sender: "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: None,
            time_seconds: 0.0,
        };
        assert!(!SubstrateConnector.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn unsupported_curve_returns_false() {
        let payload = r#"{"data":"0x00","curve":"ed25519"}"#;
        let msg = SimplePendingMessage {
            chain: Chain::Polkadot,
            sender: "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: Some(payload.into()),
            time_seconds: 0.0,
        };
        assert!(!SubstrateConnector.verify_signature(&msg).await.unwrap());
    }

    #[test]
    fn ss58_alice_decodes() {
        // Alice's well-known dev key public hex.
        let pub_bytes = ss58_to_public("5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY").unwrap();
        assert_eq!(
            hex::encode(pub_bytes),
            "d43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d"
        );
    }
}
