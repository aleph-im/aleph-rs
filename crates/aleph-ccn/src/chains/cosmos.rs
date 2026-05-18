//! Cosmos signature verification. Mirrors `aleph/chains/cosmos.py`.
//!
//! Signature payload is a JSON object:
//!   { "pub_key": {"type": "tendermint/PubKeySecp256k1", "value": "<base64>"},
//!     "signature": "<base64>" }
//!
//! The signer signs a JSON envelope:
//!   {"chain_id":"signed-message-v1","account_number":"0", ...,
//!     "msgs":[{"type":"signutil/MsgSignText",
//!              "value":{"message":"<verification buffer>", "signer":"<sender>"}}]}
//! serialised with sort_keys=True / separators (",", ":").
//!
//! The pub key (33-byte compressed secp256k1) is hashed (SHA256 ->
//! RIPEMD160) and bech32-encoded with the sender's HRP; we then verify
//! that against the sender address.

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use bech32::Hrp;
use k256::ecdsa::{Signature as K256Signature, VerifyingKey, signature::Verifier as _};
use ripemd::Ripemd160;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::abc::{PendingMessageView, Verifier};
use super::common::verification_buffer;
use crate::AlephResult;

/// Verifier for Cosmos SDK signed messages.
#[derive(Default, Debug, Clone, Copy)]
pub struct CosmosConnector;

#[derive(Deserialize)]
struct CosmosSig {
    pub_key: CosmosPubKey,
    signature: String,
}

#[derive(Deserialize)]
struct CosmosPubKey {
    #[serde(rename = "type")]
    type_: String,
    value: String,
}

#[async_trait]
impl Verifier for CosmosConnector {
    async fn verify_signature(&self, message: &dyn PendingMessageView) -> AlephResult<bool> {
        let Some(payload) = message.signature() else {
            tracing::warn!(item_hash = %message.item_hash(), "Cosmos: missing signature");
            return Ok(false);
        };

        let parsed: CosmosSig = match serde_json::from_str(payload) {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!("Cosmos signature deserialization error");
                return Ok(false);
            }
        };

        if parsed.pub_key.type_ != "tendermint/PubKeySecp256k1" {
            tracing::warn!(kind = parsed.pub_key.type_, "Cosmos: unsupported curve",);
            return Ok(false);
        }

        let pub_key = match BASE64.decode(&parsed.pub_key.value) {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!("Cosmos: pubkey base64 decode error");
                return Ok(false);
            }
        };
        let sig_compact = match BASE64.decode(&parsed.signature) {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!("Cosmos: signature base64 decode error");
                return Ok(false);
            }
        };

        let hrp = match message.sender().split_once('1') {
            Some((hrp, _)) => hrp,
            None => {
                tracing::warn!("Cosmos: malformed bech32 sender");
                return Ok(false);
            }
        };

        // Re-derive the bech32 address from the pubkey and compare.
        let derived = match pubkey_to_address(&pub_key, hrp) {
            Some(addr) => addr,
            None => return Ok(false),
        };
        if derived != message.sender() {
            tracing::warn!(
                derived = %derived,
                sender = message.sender(),
                "Cosmos: signature for bad address",
            );
            return Ok(false);
        }

        // Verify the signature over the signed JSON envelope.
        let verification_string = match build_verification_string(message) {
            Some(s) => s,
            None => return Ok(false),
        };
        let key = match VerifyingKey::from_sec1_bytes(&pub_key) {
            Ok(k) => k,
            Err(_) => return Ok(false),
        };
        let sig = match K256Signature::from_slice(&sig_compact) {
            Ok(s) => s,
            Err(_) => return Ok(false),
        };
        let digest = Sha256::digest(verification_string.as_bytes());

        Ok(key.verify(&digest, &sig).is_ok())
    }
}

fn pubkey_to_address(pub_key: &[u8], hrp: &str) -> Option<String> {
    let sha = Sha256::digest(pub_key);
    let ripe = Ripemd160::digest(sha);
    let hrp = Hrp::parse(hrp).ok()?;
    bech32::encode::<bech32::Bech32>(hrp, &ripe).ok()
}

/// Builds the canonical JSON envelope the signer signs over.
///
/// Mirrors `get_verification_string` in `aleph/chains/cosmos.py`:
/// `json.dumps(value, separators=(",", ":"), sort_keys=True)`.
fn build_verification_string(message: &dyn PendingMessageView) -> Option<String> {
    let buffer = verification_buffer(message);
    let signable = std::str::from_utf8(&buffer).ok()?.to_string();
    let signer = message.sender().to_string();
    // serde_json::to_string sorts BTreeMap keys; using Map preserves insertion
    // order, so we build the JSON manually to mirror Python's sort_keys=True.
    // The resulting fields are:
    //   {"account_number":"0","chain_id":"signed-message-v1",
    //    "fee":{"amount":[],"gas":"0"},"memo":"","msgs":[
    //      {"type":"signutil/MsgSignText",
    //       "value":{"message":"<buf>","signer":"<sender>"}}],
    //    "sequence":"0"}
    Some(format!(
        "{{\"account_number\":\"0\",\
            \"chain_id\":\"signed-message-v1\",\
            \"fee\":{{\"amount\":[],\"gas\":\"0\"}},\
            \"memo\":\"\",\
            \"msgs\":[{{\"type\":\"signutil/MsgSignText\",\
                       \"value\":{{\"message\":{},\"signer\":{}}}}}],\
            \"sequence\":\"0\"}}",
        json_string(&signable),
        json_string(&signer)
    ))
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).expect("string serialization can't fail")
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
            chain: Chain::Csdk,
            sender: "cosmos1abc".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: None,
            time_seconds: 0.0,
        };
        assert!(!CosmosConnector.verify_signature(&msg).await.unwrap());
    }

    #[tokio::test]
    async fn unsupported_curve_returns_false() {
        let payload =
            r#"{"pub_key":{"type":"tendermint/PubKeyEd25519","value":"AAAA"},"signature":"AAAA"}"#;
        let msg = SimplePendingMessage {
            chain: Chain::Csdk,
            sender: "cosmos1abc".into(),
            message_type: MessageType::Post,
            item_hash: "h".into(),
            signature: Some(payload.into()),
            time_seconds: 0.0,
        };
        assert!(!CosmosConnector.verify_signature(&msg).await.unwrap());
    }

    #[test]
    fn verification_string_matches_python_format() {
        let msg = SimplePendingMessage {
            chain: Chain::Csdk,
            sender: "cosmos1abc".into(),
            message_type: MessageType::Post,
            item_hash: "deadbeef".into(),
            signature: None,
            time_seconds: 0.0,
        };
        let s = build_verification_string(&msg).unwrap();
        // Python form: '{"account_number":"0","chain_id":"signed-message-v1",...}'
        assert!(s.starts_with("{\"account_number\":\"0\",\"chain_id\":\"signed-message-v1\""));
        assert!(s.contains("\"signer\":\"cosmos1abc\""));
        assert!(s.contains("\"message\":\"CSDK\\ncosmos1abc\\nPOST\\ndeadbeef\""));
    }
}
