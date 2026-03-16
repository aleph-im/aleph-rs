use crate::account::{Account, AccountError, SignError};
use crate::chain::{Address, Chain, Signature};
use ed25519_dalek::Signer;
use secrecy::{ExposeSecret, SecretBox};

/// An Aleph account backed by an Ed25519 private key for Solana-compatible chains.
pub struct SolanaAccount {
    chain: Chain,
    address: Address,
    private_key: SecretBox<[u8; 32]>,
}

impl SolanaAccount {
    /// Creates a new Solana account from a raw private key.
    ///
    /// The `chain` must be an SVM-compatible chain (see [`Chain::is_svm`]).
    /// The `private_key` must be 32 bytes (raw Ed25519 seed) or 64 bytes
    /// (Phantom format: first 32 bytes are the private key, last 32 are the public key).
    pub fn new(chain: Chain, private_key: &[u8]) -> Result<Self, AccountError> {
        if !chain.is_svm() {
            return Err(AccountError::UnsupportedChain(chain));
        }

        let key_bytes: [u8; 32] = match private_key.len() {
            32 => private_key.try_into().unwrap(),
            64 => private_key[..32].try_into().unwrap(),
            n => {
                return Err(AccountError::InvalidKey(format!(
                    "expected 32 or 64 bytes, got {n}"
                )))
            }
        };

        let signing_key = ed25519_dalek::SigningKey::from_bytes(&key_bytes);
        let public_key = signing_key.verifying_key();
        let address = Address::from(bs58::encode(public_key.as_bytes()).into_string());

        Ok(Self {
            chain,
            address,
            private_key: SecretBox::new(Box::new(key_bytes)),
        })
    }
}

impl Account for SolanaAccount {
    fn chain(&self) -> Chain {
        self.chain.clone()
    }

    fn address(&self) -> &Address {
        &self.address
    }

    fn sign_raw(&self, buffer: &[u8]) -> Result<Signature, SignError> {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(self.private_key.expose_secret());
        let sig = signing_key.sign(buffer);
        let sig_b58 = bs58::encode(sig.to_bytes()).into_string();

        Ok(Signature::with_public_key(
            sig_b58,
            self.address.as_str().to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::{verification_buffer, Account};
    use crate::chain::Chain;
    use crate::message::MessageType;

    const TEST_KEY: [u8; 32] = [
        0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec,
        0x2c, 0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03,
        0x1c, 0xae, 0x7f, 0x60,
    ];

    #[test]
    fn test_solana_account_creation() {
        let account = SolanaAccount::new(Chain::Sol, &TEST_KEY).unwrap();
        assert_eq!(account.chain(), Chain::Sol);
        assert!(!account.address().as_str().is_empty());
        assert!(!account.address().as_str().starts_with("0x"));
    }

    #[test]
    fn test_solana_account_wrong_chain() {
        let result = SolanaAccount::new(Chain::Ethereum, &TEST_KEY);
        assert!(result.is_err());
    }

    #[test]
    fn test_solana_account_64_byte_phantom_key() {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&TEST_KEY);
        let public_key = signing_key.verifying_key();
        let mut phantom_key = [0u8; 64];
        phantom_key[..32].copy_from_slice(&TEST_KEY);
        phantom_key[32..].copy_from_slice(public_key.as_bytes());

        let account = SolanaAccount::new(Chain::Sol, &phantom_key).unwrap();
        let account_32 = SolanaAccount::new(Chain::Sol, &TEST_KEY).unwrap();
        assert_eq!(account.address(), account_32.address());
    }

    #[test]
    fn test_solana_account_invalid_key_length() {
        let result = SolanaAccount::new(Chain::Sol, &[0u8; 16]);
        assert!(result.is_err());
    }

    #[test]
    fn test_solana_sign_produces_public_key() {
        let account = SolanaAccount::new(Chain::Sol, &TEST_KEY).unwrap();
        let signature = account.sign_raw(b"test message").unwrap();
        assert!(signature.public_key().is_some());
        assert_eq!(signature.public_key().unwrap(), account.address().as_str());
    }

    #[test]
    fn test_solana_sign_and_verify_roundtrip() {
        let account = SolanaAccount::new(Chain::Sol, &TEST_KEY).unwrap();
        let item_hash = crate::item_hash!(
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
        let message_type = MessageType::Post;

        let buffer =
            verification_buffer(&account.chain(), account.address(), message_type, &item_hash);
        let signature = account.sign_raw(buffer.as_bytes()).unwrap();

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
