mod ethereum;

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
    if !chain.is_evm() {
        return Err(SignatureVerificationError::UnsupportedChain(chain.clone()));
    }

    let buffer = verification_buffer(chain, sender, message_type, item_hash);
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

    Ok(())
}
