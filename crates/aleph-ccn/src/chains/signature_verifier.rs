//! Signature dispatcher. Mirrors `aleph/chains/signature_verifier.py`.
//!
//! Owns one `Verifier` per `Chain` and forwards messages based on
//! `message.chain()`. Returns `Err(InvalidSignature)` on chain mismatch,
//! `Err(InvalidMessage)` on bad signature payloads, and `Ok(())` on success.
//!
//! Note: `aleph_types::chain::Chain` does not implement `Hash`/`Ord`, so
//! we use a `Vec<(Chain, Box<dyn Verifier>)>` for lookup — the constant set
//! of chains (~30 entries) makes this trivial.

use super::abc::{PendingMessageView, Verifier};
use super::avalanche::AvalancheConnector;
use super::cosmos::CosmosConnector;
use super::ethereum::EthereumVerifier;
use super::evm::EvmVerifier;
use super::nuls::NulsConnector;
use super::nuls2::Nuls2Verifier;
use super::solana::SolanaConnector;
use super::substrate::SubstrateConnector;
use super::tezos::TezosVerifier;
use crate::{AlephError, AlephResult};

/// Drop-in signature verifier that delegates to the per-chain verifier.
///
/// Mirrors `SignatureVerifier` in `aleph/chains/signature_verifier.py`,
/// including the same chain set.
pub struct SignatureVerifier {
    verifiers: Vec<(aleph_types::chain::Chain, Box<dyn Verifier>)>,
}

impl SignatureVerifier {
    /// Returns a verifier wired up with the full chain set.
    pub fn new() -> Self {
        use aleph_types::chain::Chain;
        let mut verifiers: Vec<(Chain, Box<dyn Verifier>)> = Vec::new();

        // EVM-compatible chains.
        for c in [
            Chain::Arbitrum,
            Chain::Base,
            Chain::Blast,
            Chain::Bob,
            Chain::Bsc,
            Chain::Cyber,
            Chain::Etherlink,
            Chain::Fraxtal,
            Chain::Hype,
            Chain::Ink,
            Chain::Lens,
            Chain::Linea,
            Chain::Lisk,
            Chain::Metis,
            Chain::Mode,
            Chain::Neo,
            Chain::Optimism,
            Chain::Pol,
            Chain::Sonic,
            Chain::Unichain,
            Chain::Worldchain,
            Chain::Zora,
        ] {
            verifiers.push((c, Box::new(EvmVerifier)));
        }

        verifiers.push((Chain::Avax, Box::new(AvalancheConnector)));
        verifiers.push((Chain::Csdk, Box::new(CosmosConnector)));
        verifiers.push((Chain::Polkadot, Box::new(SubstrateConnector)));
        verifiers.push((Chain::Eclipse, Box::new(SolanaConnector)));
        verifiers.push((Chain::Ethereum, Box::new(EthereumVerifier::default())));
        verifiers.push((Chain::Nuls, Box::new(NulsConnector)));
        verifiers.push((Chain::Nuls2, Box::new(Nuls2Verifier)));
        verifiers.push((Chain::Sol, Box::new(SolanaConnector)));
        verifiers.push((Chain::Tezos, Box::new(TezosVerifier)));

        Self { verifiers }
    }

    /// Validates the signature on `message`. Mirrors Python's
    /// `verify_signature`: returns `Ok(())` on success and `Err(...)` for
    /// any failure mode (including bad chain).
    pub async fn verify_signature(&self, message: &dyn PendingMessageView) -> AlephResult<()> {
        let chain = message.chain();
        let verifier = self
            .verifiers
            .iter()
            .find_map(|(c, v)| (*c == chain).then_some(v))
            .ok_or_else(|| {
                AlephError::InvalidMessage(format!("Unknown chain for validation: {chain}"))
            })?;

        match verifier.verify_signature(message).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(AlephError::InvalidSignature),
            Err(e) => Err(AlephError::InvalidMessage(format!(
                "Signature validation error: {e}"
            ))),
        }
    }
}

impl Default for SignatureVerifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chains::abc::SimplePendingMessage;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;

    #[tokio::test]
    async fn dispatches_to_evm_for_ethereum() {
        let msg = SimplePendingMessage {
            chain: Chain::Ethereum,
            sender: "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".into(),
            message_type: MessageType::Post,
            item_hash: "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c".into(),
            signature: Some("0x636728dbea33fa6064f099045421b980dff3c71932cd89c2bf42387ebb6f53890a24e13f16678463876224772b90838c2b9557cd8fc2cdae45509ce8cb89e3fd1b".into()),
            time_seconds: 1762515431.653,
        };
        SignatureVerifier::new()
            .verify_signature(&msg)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dispatches_to_solana_for_sol() {
        let signature = r#"{"signature":"5HH5Z9jpWMicA8iGYnucQmDhGszvxmz76L14nDyRYTH7QrjQXFh7C1p7BG62UfpDbRfhhzGSESGX5dw6ef25V3GT","publicKey":"5SwCeHbZ9oY3556YFBEhPTHyy9t4yse26v7MUyGm2bHS"}"#;
        let msg = SimplePendingMessage {
            chain: Chain::Sol,
            sender: "5SwCeHbZ9oY3556YFBEhPTHyy9t4yse26v7MUyGm2bHS".into(),
            message_type: MessageType::Post,
            item_hash: "a5498c4c81d0bec9ab7c7fd6a78228eb6ac530ea387ae2878c64d376769dbb79".into(),
            signature: Some(signature.into()),
            time_seconds: 1773291768.546,
        };
        SignatureVerifier::new()
            .verify_signature(&msg)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn invalid_signature_returns_err() {
        let msg = SimplePendingMessage {
            chain: Chain::Ethereum,
            sender: "0x0000000000000000000000000000000000000000".into(),
            message_type: MessageType::Post,
            item_hash: "deadbeef".into(),
            signature: Some("0x00".into()),
            time_seconds: 0.0,
        };
        let err = SignatureVerifier::new()
            .verify_signature(&msg)
            .await
            .unwrap_err();
        assert!(matches!(err, AlephError::InvalidSignature));
    }

    #[tokio::test]
    async fn unknown_chain_returns_invalid_message() {
        let msg = SimplePendingMessage {
            chain: Chain::Aurora, // Not in pyaleph's verifier map.
            sender: "0xabc".into(),
            message_type: MessageType::Post,
            item_hash: "deadbeef".into(),
            signature: Some("0x00".into()),
            time_seconds: 0.0,
        };
        let err = SignatureVerifier::new()
            .verify_signature(&msg)
            .await
            .unwrap_err();
        assert!(matches!(err, AlephError::InvalidMessage(_)));
    }
}
