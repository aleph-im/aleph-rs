mod ethereum;
mod solana;

use crate::chain::{Address, Chain, Signature};
use crate::item_hash::ItemHash;
use crate::message::MessageType;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum SignatureVerificationError {
    /// The recovered signer address doesn't match the message sender.
    #[error(
        "Signature mismatch: message sender is {expected}, but signature was produced by {recovered}"
    )]
    SignatureMismatch {
        expected: Address,
        recovered: Address,
    },
    /// The signature bytes could not be parsed or recovery failed.
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),
    /// Signature verification is not implemented for this chain.
    #[error("Unsupported chain for signature verification: {0}")]
    UnsupportedChain(Chain),
}

/// Constructs the verification buffer that was signed by the sender.
/// Format: "{chain}\n{sender}\n{message_type}\n{item_hash}"
fn verification_buffer(
    chain: &Chain,
    sender: &Address,
    message_type: MessageType,
    item_hash: &ItemHash,
) -> String {
    format!("{chain}\n{sender}\n{message_type}\n{item_hash}")
}

/// Verifies the cryptographic signature of a message.
pub(crate) fn verify(
    chain: &Chain,
    sender: &Address,
    signature: &Signature,
    message_type: MessageType,
    item_hash: &ItemHash,
) -> Result<(), SignatureVerificationError> {
    let buffer = verification_buffer(chain, sender, message_type, item_hash);

    if chain.is_evm() {
        let recovered = ethereum::recover_address(buffer.as_bytes(), signature.as_str())?;
        let recovered_addr = Address::from(recovered);

        if !sender
            .as_str()
            .eq_ignore_ascii_case(recovered_addr.as_str())
        {
            return Err(SignatureVerificationError::SignatureMismatch {
                expected: sender.clone(),
                recovered: recovered_addr,
            });
        }

        return Ok(());
    }

    if matches!(chain, Chain::Sol) {
        // For Solana, the sender address is the base58-encoded public key.
        return solana::verify(buffer.as_bytes(), signature.as_str(), sender.as_str());
    }

    Err(SignatureVerificationError::UnsupportedChain(chain.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{address, item_hash};

    #[test]
    fn test_verification_buffer_format() {
        let chain = Chain::Ethereum;
        let sender = address!("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef");
        let message_type = MessageType::Post;
        let item_hash =
            item_hash!("d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c");

        let buffer = verification_buffer(&chain, &sender, message_type, &item_hash);

        assert_eq!(
            buffer,
            "ETH\n\
             0xB68B9D4f3771c246233823ed1D3Add451055F9Ef\n\
             POST\n\
             d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
    }

    #[test]
    fn test_verification_buffer_different_chain_and_type() {
        let chain = Chain::Arbitrum;
        let sender = address!("0xABCD");
        let message_type = MessageType::Aggregate;
        let item_hash =
            item_hash!("0000000000000000000000000000000000000000000000000000000000000001");

        let buffer = verification_buffer(&chain, &sender, message_type, &item_hash);

        assert_eq!(
            buffer,
            "ARB\n0xABCD\nAGGREGATE\n0000000000000000000000000000000000000000000000000000000000000001"
        );
    }

    #[test]
    fn test_verify_with_v_zero_format() {
        // The fixture signature ends with 1b (v=27). Replacing the last byte
        // with 00 (v=0) should be equivalent for recovery.
        let json = include_str!("../../../../fixtures/messages/post/post.json");
        let mut message: crate::message::Message = serde_json::from_str(json).unwrap();

        // Original signature ends with "1b" (v=27); replace with "00" (v=0)
        let sig = message.signature.as_str().to_string();
        let normalized_sig = format!("{}00", &sig[..sig.len() - 2]);
        message.signature = Signature::from(normalized_sig);

        message.verify_signature().unwrap();
    }
}
