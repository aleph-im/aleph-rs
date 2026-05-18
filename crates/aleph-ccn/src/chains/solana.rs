//! Solana signature verification. Mirrors `aleph/chains/solana.py`.
//!
//! Solana signatures are serialized as a JSON object with the shape
//! `{"signature": "<base58>", "publicKey": "<base58>", "version": 1}`.
//! Verification uses Ed25519 on the verification buffer.

use async_trait::async_trait;
use ed25519_dalek::{Signature as Ed25519Signature, Verifier as Ed25519Verifier, VerifyingKey};
use serde::Deserialize;

use super::abc::{PendingMessageView, Verifier};
use super::common::verification_buffer;
use crate::AlephResult;

/// Verifier for Solana (and Eclipse, which uses the same SVM signature format).
#[derive(Default, Debug, Clone, Copy)]
pub struct SolanaConnector;

#[derive(Deserialize)]
struct SolanaSig {
    signature: String,
    #[serde(rename = "publicKey")]
    public_key: String,
    #[serde(default = "default_version")]
    version: u32,
}

fn default_version() -> u32 {
    1
}

#[async_trait]
impl Verifier for SolanaConnector {
    async fn verify_signature(&self, message: &dyn PendingMessageView) -> AlephResult<bool> {
        let Some(signature) = message.signature() else {
            tracing::warn!(item_hash = %message.item_hash(), "Solana: missing signature");
            return Ok(false);
        };

        let parsed: SolanaSig = match serde_json::from_str(signature) {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!("Solana signature deserialization error");
                return Ok(false);
            }
        };

        if parsed.version != 1 {
            tracing::warn!(
                version = parsed.version,
                "Solana: unsupported signature version"
            );
            return Ok(false);
        }

        if message.sender() != parsed.public_key {
            tracing::warn!("Solana signature source error");
            return Ok(false);
        }

        let sig_bytes = match bs58::decode(&parsed.signature).into_vec() {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!("Solana signature base58 decode error");
                return Ok(false);
            }
        };
        let pk_bytes = match bs58::decode(&parsed.public_key).into_vec() {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!("Solana public key base58 decode error");
                return Ok(false);
            }
        };

        let sig_array: [u8; 64] = match sig_bytes.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => return Ok(false),
        };
        let pk_array: [u8; 32] = match pk_bytes.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => return Ok(false),
        };

        let verifying_key = match VerifyingKey::from_bytes(&pk_array) {
            Ok(k) => k,
            Err(_) => return Ok(false),
        };
        let ed_sig = Ed25519Signature::from_bytes(&sig_array);

        let buffer = verification_buffer(message);
        Ok(verifying_key.verify(&buffer, &ed_sig).is_ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::abc::SimplePendingMessage;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;

    #[tokio::test]
    async fn verify_post_sol_fixture() {
        // From fixtures/messages/post/post-sol.json
        let signature = r#"{"signature":"5HH5Z9jpWMicA8iGYnucQmDhGszvxmz76L14nDyRYTH7QrjQXFh7C1p7BG62UfpDbRfhhzGSESGX5dw6ef25V3GT","publicKey":"5SwCeHbZ9oY3556YFBEhPTHyy9t4yse26v7MUyGm2bHS"}"#;
        let msg = SimplePendingMessage {
            chain: Chain::Sol,
            sender: "5SwCeHbZ9oY3556YFBEhPTHyy9t4yse26v7MUyGm2bHS".into(),
            message_type: MessageType::Post,
            item_hash: "a5498c4c81d0bec9ab7c7fd6a78228eb6ac530ea387ae2878c64d376769dbb79".into(),
            signature: Some(signature.into()),
            time_seconds: 1773291768.546,
        };

        let v = SolanaConnector;
        assert!(v.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn verify_solana_rejects_wrong_sender() {
        let signature = r#"{"signature":"5HH5Z9jpWMicA8iGYnucQmDhGszvxmz76L14nDyRYTH7QrjQXFh7C1p7BG62UfpDbRfhhzGSESGX5dw6ef25V3GT","publicKey":"5SwCeHbZ9oY3556YFBEhPTHyy9t4yse26v7MUyGm2bHS"}"#;
        let msg = SimplePendingMessage {
            chain: Chain::Sol,
            sender: "DifferentSenderAddr12345678901234567890".into(),
            message_type: MessageType::Post,
            item_hash: "a5498c4c81d0bec9ab7c7fd6a78228eb6ac530ea387ae2878c64d376769dbb79".into(),
            signature: Some(signature.into()),
            time_seconds: 1773291768.546,
        };
        let v = SolanaConnector;
        assert!(!v.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn verify_solana_invalid_json() {
        let msg = SimplePendingMessage {
            chain: Chain::Sol,
            sender: "5SwCeHbZ9oY3556YFBEhPTHyy9t4yse26v7MUyGm2bHS".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: Some("not-json".into()),
            time_seconds: 0.0,
        };
        let v = SolanaConnector;
        assert!(!v.verify_signature(&msg).await.unwrap());
    }
}
