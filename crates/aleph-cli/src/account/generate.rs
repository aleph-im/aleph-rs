use aleph_types::account::Account;
use aleph_types::chain::Chain;
use anyhow::Result;
use zeroize::Zeroizing;

/// Generate a random secp256k1 private key and return (hex_key, address).
pub fn generate_evm_key(chain: Chain) -> Result<(Zeroizing<String>, String)> {
    use k256::ecdsa::SigningKey;
    use rand::rngs::OsRng;

    let signing_key = SigningKey::random(&mut OsRng);
    let key_hex = Zeroizing::new(hex::encode(signing_key.to_bytes()));

    let account = aleph_types::account::EvmAccount::new(chain, &signing_key.to_bytes())
        .map_err(|e| anyhow::anyhow!(e))?;
    let address = account.address().to_string();

    Ok((key_hex, address))
}

/// Generate a random Ed25519 private key and return (hex_key, address).
pub fn generate_sol_key(chain: Chain) -> Result<(Zeroizing<String>, String)> {
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    let signing_key = SigningKey::generate(&mut OsRng);
    let key_hex = Zeroizing::new(hex::encode(signing_key.to_bytes()));

    let account = aleph_types::account::SolanaAccount::new(chain, &signing_key.to_bytes())
        .map_err(|e| anyhow::anyhow!(e))?;
    let address = account.address().to_string();

    Ok((key_hex, address))
}

/// Generate a random key pair for the given chain.
/// Returns (hex_encoded_private_key, address).
pub fn generate_key(chain: Chain) -> Result<(Zeroizing<String>, String)> {
    if chain.is_evm() {
        generate_evm_key(chain)
    } else if chain.is_svm() {
        generate_sol_key(chain)
    } else {
        anyhow::bail!("key generation is not supported for chain {chain}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_evm_key_produces_valid_address() {
        let (key_hex, address) = generate_evm_key(Chain::Ethereum).unwrap();
        assert_eq!(hex::decode(&key_hex).unwrap().len(), 32);
        assert!(address.starts_with("0x"));
        assert_eq!(address.len(), 42);
    }

    #[test]
    fn generate_evm_key_is_random() {
        let (k1, _) = generate_evm_key(Chain::Ethereum).unwrap();
        let (k2, _) = generate_evm_key(Chain::Ethereum).unwrap();
        assert_ne!(k1, k2);
    }

    #[test]
    fn generate_sol_key_produces_valid_address() {
        let (key_hex, address) = generate_sol_key(Chain::Sol).unwrap();
        assert_eq!(hex::decode(&key_hex).unwrap().len(), 32);
        assert!(!address.starts_with("0x"));
        assert!(!address.is_empty());
    }

    #[test]
    fn generate_key_dispatches_by_chain() {
        let (_, addr) = generate_key(Chain::Ethereum).unwrap();
        assert!(addr.starts_with("0x"));

        let (_, addr) = generate_key(Chain::Sol).unwrap();
        assert!(!addr.starts_with("0x"));
    }

    #[test]
    fn generate_key_rejects_unsupported_chain() {
        assert!(generate_key(Chain::Tezos).is_err());
    }
}
