//! NULS (legacy) signature verification. Mirrors `aleph/chains/nuls.py`.
//!
//! Signature is a hex-encoded `[length+pubkey][ecc_type][length+sig]` blob;
//! the sender address embeds the chain ID, which is used to re-derive the
//! address from the recovered pubkey for comparison.

use async_trait::async_trait;

use super::abc::{PendingMessageView, Verifier};
use super::common::verification_buffer;
use super::nuls_aleph_sdk::{
    NulsSignature, address_from_hash, hash_from_address, public_key_to_hash,
};
use crate::AlephResult;

/// Verifier for the legacy NULS chain.
#[derive(Default, Debug, Clone, Copy)]
pub struct NulsConnector;

#[async_trait]
impl Verifier for NulsConnector {
    async fn verify_signature(&self, message: &dyn PendingMessageView) -> AlephResult<bool> {
        let Some(signature_hex) = message.signature() else {
            tracing::warn!(item_hash = %message.item_hash(), "NULS: missing signature");
            return Ok(false);
        };

        let sig_raw = match hex::decode(signature_hex) {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!("NULS signature hex decode error");
                return Ok(false);
            }
        };

        let sig = match NulsSignature::parse(&sig_raw) {
            Some(s) => s,
            None => {
                tracing::warn!("NULS signature parse error");
                return Ok(false);
            }
        };

        let sender_hash = match hash_from_address(message.sender()) {
            Some(h) => h,
            None => return Ok(false),
        };
        if sender_hash.len() < 2 {
            return Ok(false);
        }
        let chain_id = i16::from_le_bytes([sender_hash[0], sender_hash[1]]);

        let hash = public_key_to_hash(&sig.pub_key, chain_id, 1);
        let derived = address_from_hash(&hash);
        if derived != message.sender() {
            tracing::warn!(
                derived = %derived,
                sender = message.sender(),
                "NULS: bad signature",
            );
            return Ok(false);
        }

        let buffer = verification_buffer(message);
        Ok(sig.verify(&buffer))
    }
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
            chain: Chain::Nuls,
            sender: "NULSd6Hga3NuLs2ChainTestAddressXyzAbcDef".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: None,
            time_seconds: 0.0,
        };
        assert!(!NulsConnector.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn bad_hex_returns_false() {
        let msg = SimplePendingMessage {
            chain: Chain::Nuls,
            sender: "NULSd6Hga3NuLs2ChainTestAddressXyzAbcDef".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: Some("not-hex!".into()),
            time_seconds: 0.0,
        };
        assert!(!NulsConnector.verify_signature(&msg).await.unwrap());
    }
}
