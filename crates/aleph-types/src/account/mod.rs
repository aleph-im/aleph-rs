#[cfg(feature = "account-evm")]
mod evm;
#[cfg(feature = "account-sol")]
mod solana;

#[cfg(feature = "account-evm")]
pub use evm::EvmAccount;
#[cfg(feature = "account-sol")]
pub use solana::SolanaAccount;

use crate::chain::{Address, Chain, Signature};
use crate::item_hash::ItemHash;
use crate::message::pending::PendingMessage;
use crate::message::unsigned::UnsignedMessage;
use crate::message::MessageType;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SignError {
    #[error("signing failed: {0}")]
    SigningFailed(String),
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AccountError {
    #[error("chain {0} is not supported by this account type")]
    UnsupportedChain(Chain),
    #[error("invalid private key: {0}")]
    InvalidKey(String),
}

pub trait Account: Send + Sync {
    fn chain(&self) -> Chain;
    fn address(&self) -> &Address;
    fn sign_raw(&self, buffer: &[u8]) -> Result<Signature, SignError>;
}

pub fn verification_buffer(
    chain: &Chain,
    sender: &Address,
    message_type: MessageType,
    item_hash: &ItemHash,
) -> String {
    format!("{chain}\n{sender}\n{message_type}\n{item_hash}")
}

pub fn sign_message<A: Account>(
    account: &A,
    unsigned: UnsignedMessage,
) -> Result<PendingMessage, SignError> {
    let buffer = verification_buffer(
        &account.chain(),
        account.address(),
        unsigned.message_type,
        &unsigned.item_hash,
    );
    let signature = account.sign_raw(buffer.as_bytes())?;

    Ok(PendingMessage {
        chain: account.chain(),
        sender: account.address().clone(),
        signature,
        message_type: unsigned.message_type,
        item_type: unsigned.item_type,
        item_content: unsigned.item_content,
        item_hash: unsigned.item_hash,
        time: unsigned.time,
        channel: unsigned.channel,
    })
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
        let hash =
            item_hash!("d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c");

        let buffer = verification_buffer(&chain, &sender, message_type, &hash);

        assert_eq!(
            buffer,
            "ETH\n\
             0xB68B9D4f3771c246233823ed1D3Add451055F9Ef\n\
             POST\n\
             d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
    }
}
