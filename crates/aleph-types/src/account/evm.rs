use crate::account::{Account, AccountError, SignError};
use crate::chain::{Address, Chain, Signature};
use crate::verify_signature::ethereum::{eip191_hash, public_key_to_address};
use k256::ecdsa::signature::hazmat::PrehashSigner;
use k256::ecdsa::{RecoveryId, SigningKey, VerifyingKey};
use secrecy::{ExposeSecret, SecretBox};

/// An Aleph account backed by a secp256k1 private key for EVM-compatible chains.
pub struct EvmAccount {
    chain: Chain,
    address: Address,
    signing_key: SecretBox<[u8; 32]>,
}

impl EvmAccount {
    /// Creates a new EVM account from a raw private key.
    ///
    /// The `chain` must be an EVM-compatible chain (see [`Chain::is_evm`]).
    /// The `private_key` must be exactly 32 bytes and a valid secp256k1 scalar.
    pub fn new(chain: Chain, private_key: &[u8]) -> Result<Self, AccountError> {
        if !chain.is_evm() {
            return Err(AccountError::UnsupportedChain(chain));
        }

        let key_bytes: [u8; 32] = private_key
            .try_into()
            .map_err(|_| AccountError::InvalidKey(format!(
                "expected 32 bytes, got {}",
                private_key.len()
            )))?;

        let signing_key = SigningKey::from_bytes((&key_bytes).into())
            .map_err(|e| AccountError::InvalidKey(e.to_string()))?;

        let verifying_key = VerifyingKey::from(&signing_key);
        let address = Address::from(public_key_to_address(&verifying_key));

        Ok(Self {
            chain,
            address,
            signing_key: SecretBox::new(Box::new(key_bytes)),
        })
    }
}

impl Account for EvmAccount {
    fn chain(&self) -> Chain {
        self.chain.clone()
    }

    fn address(&self) -> &Address {
        &self.address
    }

    fn sign_raw(&self, buffer: &[u8]) -> Result<Signature, SignError> {
        let digest = eip191_hash(buffer);

        let signing_key = SigningKey::from_bytes(self.signing_key.expose_secret().into())
            .map_err(|e| SignError::SigningFailed(e.to_string()))?;

        let (sig, recovery_id): (k256::ecdsa::Signature, RecoveryId) = signing_key
            .sign_prehash(&digest)
            .map_err(|e| SignError::SigningFailed(e.to_string()))?;

        let v = 27 + recovery_id.to_byte();
        let mut sig_bytes = [0u8; 65];
        sig_bytes[..64].copy_from_slice(&sig.to_bytes());
        sig_bytes[64] = v;

        Ok(Signature::from(format!("0x{}", hex::encode(sig_bytes))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::{verification_buffer, Account};
    use crate::chain::Chain;
    use crate::message::MessageType;

    const TEST_KEY: [u8; 32] = [
        0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3,
        0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff, 0x94,
        0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfb,
        0xba, 0x0f, 0x2d, 0x1d, 0xb7, 0x44, 0xce, 0x06,
    ];

    #[test]
    fn test_evm_account_creation() {
        let account = EvmAccount::new(Chain::Ethereum, &TEST_KEY).unwrap();
        assert_eq!(account.chain(), Chain::Ethereum);
        assert!(account.address().as_str().starts_with("0x"));
        assert_eq!(account.address().as_str().len(), 42);
    }

    #[test]
    fn test_evm_account_wrong_chain() {
        let result = EvmAccount::new(Chain::Sol, &TEST_KEY);
        assert!(result.is_err());
    }

    #[test]
    fn test_evm_account_invalid_key_length() {
        let result = EvmAccount::new(Chain::Ethereum, &[0u8; 16]);
        assert!(result.is_err());
    }

    #[test]
    fn test_evm_sign_and_verify_roundtrip() {
        let account = EvmAccount::new(Chain::Ethereum, &TEST_KEY).unwrap();
        let item_hash = crate::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let message_type = MessageType::Post;

        let buffer =
            verification_buffer(&account.chain(), account.address(), message_type, &item_hash);
        let signature = account.sign_raw(buffer.as_bytes()).unwrap();

        // Verify using existing verification infrastructure
        crate::verify_signature::verify(
            &account.chain(),
            account.address(),
            &signature,
            message_type,
            &item_hash,
        )
        .expect("round-trip verification should pass");
    }
}
