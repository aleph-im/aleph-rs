pub mod generate;
pub mod keystore;
pub mod ledger;
pub mod migrate;
pub mod password;
pub mod store;

use aleph_types::account::{Account, EvmAccount, SignError, SolanaAccount};
use aleph_types::chain::{Address, Chain, Signature};
use anyhow::{Context, Result, bail};
use std::sync::OnceLock;
use zeroize::Zeroizing;

/// Account wrapper that dispatches to the correct signing implementation
/// based on the chain type. This exists because the SDK builders are generic
/// over `A: Account` and need a concrete type, not a trait object.
pub enum CliAccount {
    Evm(EvmAccount),
    Sol(SolanaAccount),
    LedgerEvm(ledger::LedgerEvmAccount),
    /// An encrypted (keystore) account decrypted lazily — see
    /// [`LazyKeystoreAccount`].
    LazyKeystore(LazyKeystoreAccount),
}

/// An encrypted (keystore) EVM account whose private key is decrypted lazily.
///
/// The address is recorded in the account store, so read-only operations can
/// be served without ever touching the key. The keystore is parsed and
/// decrypted — prompting for the password — only on the first signing
/// operation, after which the decrypted account is cached for the rest of the
/// process.
pub struct LazyKeystoreAccount {
    chain: Chain,
    address: Address,
    label: String,
    keystore_json: String,
    evm: OnceLock<EvmAccount>,
}

impl LazyKeystoreAccount {
    /// Parse the keystore and decrypt the key, sourcing the password from
    /// `ALEPH_PASSWORD` or an interactive prompt.
    fn decrypt(&self) -> Result<EvmAccount> {
        let ks = keystore::parse_keystore(&self.keystore_json)
            .map_err(|e| anyhow::anyhow!("invalid keystore for '{}': {e}", self.label))?;
        let key = password::unlock_keystore(&ks, &self.label)?;
        EvmAccount::new(self.chain.clone(), &key[..]).map_err(|e| anyhow::anyhow!(e))
    }

    /// Decrypt the key on first use and return the underlying EVM account,
    /// caching the result for subsequent calls.
    fn evm(&self) -> Result<&EvmAccount> {
        if let Some(account) = self.evm.get() {
            return Ok(account);
        }
        let account = self.decrypt()?;
        // If another caller raced us, theirs is kept and ours is dropped (its
        // key wiped on drop); either way `get()` now returns a cached account.
        let _ = self.evm.set(account);
        Ok(self.evm.get().expect("just set above"))
    }

    /// Consume the lazy account, returning the decrypted EVM account.
    ///
    /// Used by operations (e.g. on-chain credit/token transactions) that need
    /// the raw [`EvmAccount`] rather than the [`Account`] trait interface.
    pub fn into_evm(self) -> Result<EvmAccount> {
        if self.evm.get().is_some() {
            return Ok(self.evm.into_inner().expect("just checked it is set"));
        }
        self.decrypt()
    }
}

impl std::fmt::Debug for CliAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliAccount::Evm(a) => write!(f, "CliAccount::Evm({})", a.address()),
            CliAccount::Sol(a) => write!(f, "CliAccount::Sol({})", a.address()),
            CliAccount::LedgerEvm(a) => write!(f, "CliAccount::LedgerEvm({})", a.address()),
            CliAccount::LazyKeystore(a) => write!(f, "CliAccount::LazyKeystore({})", a.address),
        }
    }
}

impl Account for CliAccount {
    fn chain(&self) -> Chain {
        match self {
            CliAccount::Evm(a) => a.chain(),
            CliAccount::Sol(a) => a.chain(),
            CliAccount::LedgerEvm(a) => a.chain(),
            CliAccount::LazyKeystore(a) => a.chain.clone(),
        }
    }

    fn address(&self) -> &Address {
        match self {
            CliAccount::Evm(a) => a.address(),
            CliAccount::Sol(a) => a.address(),
            CliAccount::LedgerEvm(a) => a.address(),
            CliAccount::LazyKeystore(a) => &a.address,
        }
    }

    fn sign_raw(&self, buffer: &[u8]) -> Result<Signature, SignError> {
        match self {
            CliAccount::Evm(a) => a.sign_raw(buffer),
            CliAccount::Sol(a) => a.sign_raw(buffer),
            CliAccount::LedgerEvm(a) => a.sign_raw(buffer),
            CliAccount::LazyKeystore(a) => a
                .evm()
                .map_err(|e| SignError::SigningFailed(e.to_string()))?
                .sign_raw(buffer),
        }
    }
}

/// Load an account from a hex-encoded private key and chain.
///
/// The private key is read from `private_key` if provided, otherwise
/// from the `ALEPH_PRIVATE_KEY` environment variable.
///
/// The hex string may optionally have a `0x` prefix.
pub fn load_account(private_key: Option<&str>, chain: Chain) -> Result<CliAccount> {
    let key_hex = Zeroizing::new(match private_key {
        Some(k) => k.to_string(),
        None => std::env::var("ALEPH_PRIVATE_KEY")
            .context("no private key provided; use --private-key or set ALEPH_PRIVATE_KEY")?,
    });

    let key_hex = key_hex.strip_prefix("0x").unwrap_or(&key_hex);
    let key_bytes = Zeroizing::new(hex::decode(key_hex).context("invalid hex in private key")?);

    if chain.is_evm() {
        let account = EvmAccount::new(chain, &key_bytes).map_err(|e| anyhow::anyhow!(e))?;
        Ok(CliAccount::Evm(account))
    } else if chain.is_svm() {
        let account = SolanaAccount::new(chain, &key_bytes).map_err(|e| anyhow::anyhow!(e))?;
        Ok(CliAccount::Sol(account))
    } else {
        bail!("chain {chain} is not supported for signing (only EVM and SVM chains)")
    }
}

/// Load a named account from the account store.
///
/// Retrieves the private key from the OS keychain and constructs the
/// appropriate account type based on the stored chain.
pub fn load_account_by_name(store: &store::AccountStore, name: &str) -> Result<CliAccount> {
    let entry = store
        .get_account(name)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    match entry.kind {
        store::AccountKind::Local => {
            let key_hex = Zeroizing::new(
                store
                    .get_private_key(name)
                    .map_err(|e| anyhow::anyhow!("{e}"))?,
            );
            load_account(Some(&key_hex), entry.chain)
        }
        store::AccountKind::Ledger => {
            let path_str = entry
                .derivation_path
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("ledger account '{name}' has no derivation path"))?;
            let path = ledger::DerivationPath::parse(path_str)
                .map_err(|e| anyhow::anyhow!("invalid derivation path for '{name}': {e}"))?;
            let address = Address::from(entry.address);

            if entry.chain.is_evm() {
                Ok(CliAccount::LedgerEvm(ledger::LedgerEvmAccount::new(
                    address,
                    entry.chain,
                    path,
                )))
            } else if entry.chain.is_svm() {
                bail!("Solana Ledger signing is not supported. Use a local Solana key instead.")
            } else {
                bail!("chain {} is not supported for Ledger signing", entry.chain)
            }
        }
        store::AccountKind::Keystore => {
            if !entry.chain.is_evm() {
                bail!("encrypted accounts are only supported for EVM chains");
            }
            let json = store
                .read_keystore_json(name)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            // The address is recorded in the store, so we defer decryption (and
            // the password prompt) until the first signing operation. Read-only
            // commands that only need the address never trigger a prompt.
            Ok(CliAccount::LazyKeystore(LazyKeystoreAccount {
                chain: entry.chain,
                address: Address::from(entry.address),
                label: name.to_string(),
                keystore_json: json,
                evm: OnceLock::new(),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 32-byte test key (not a real funded key)
    const TEST_KEY_HEX: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efbba0f2d1db744ce06";

    #[test]
    fn load_evm_account() {
        let account = load_account(Some(TEST_KEY_HEX), Chain::Ethereum).unwrap();
        assert!(matches!(account, CliAccount::Evm(_)));
        assert_eq!(account.chain(), Chain::Ethereum);
        assert!(account.address().as_str().starts_with("0x"));
    }

    #[test]
    fn load_evm_account_with_0x_prefix() {
        let prefixed = format!("0x{TEST_KEY_HEX}");
        let account = load_account(Some(&prefixed), Chain::Ethereum).unwrap();
        // Same key with or without prefix should produce the same address
        let account_no_prefix = load_account(Some(TEST_KEY_HEX), Chain::Ethereum).unwrap();
        assert_eq!(account.address(), account_no_prefix.address());
    }

    #[test]
    fn load_evm_account_other_chain() {
        let account = load_account(Some(TEST_KEY_HEX), Chain::Base).unwrap();
        assert!(matches!(account, CliAccount::Evm(_)));
        assert_eq!(account.chain(), Chain::Base);
    }

    #[test]
    fn load_sol_account() {
        let account = load_account(Some(TEST_KEY_HEX), Chain::Sol).unwrap();
        assert!(matches!(account, CliAccount::Sol(_)));
        assert_eq!(account.chain(), Chain::Sol);
        // Solana addresses are base58, not 0x-prefixed
        assert!(!account.address().as_str().starts_with("0x"));
    }

    #[test]
    fn load_account_invalid_hex() {
        let err = load_account(Some("not-valid-hex!"), Chain::Ethereum).unwrap_err();
        assert!(err.to_string().contains("invalid hex"));
    }

    #[test]
    fn load_account_wrong_key_length() {
        let err = load_account(Some("abcd"), Chain::Ethereum).unwrap_err();
        assert!(err.to_string().contains("expected 32 bytes"));
    }

    #[test]
    fn load_account_unsupported_chain() {
        let err = load_account(Some(TEST_KEY_HEX), Chain::Tezos).unwrap_err();
        assert!(err.to_string().contains("not supported for signing"));
    }

    #[test]
    fn load_account_no_key_no_env() {
        // Only test when env var is not set (avoid unsafe set_var/remove_var)
        if std::env::var("ALEPH_PRIVATE_KEY").is_err() {
            let err = load_account(None, Chain::Ethereum).unwrap_err();
            assert!(err.to_string().contains("no private key provided"));
        }
    }

    #[test]
    fn cli_account_can_sign() {
        let account = load_account(Some(TEST_KEY_HEX), Chain::Ethereum).unwrap();
        let sig = account.sign_raw(b"test message").unwrap();
        assert!(sig.as_str().starts_with("0x"));
    }

    #[test]
    fn load_account_by_name_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = store::AccountStore::with_manifest_path(dir.path().join("accounts.toml"));
        let err = load_account_by_name(&store, "nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn load_account_by_name_keystore_rejects_non_evm_chain() {
        let dir = tempfile::tempdir().unwrap();
        let store = store::AccountStore::with_manifest_path(dir.path().join("accounts.toml"));
        store
            .add_keystore_account("enc", Chain::Sol, "abc".to_string(), r#"{"x": 1}"#)
            .unwrap();

        let err = load_account_by_name(&store, "enc").unwrap_err();
        assert!(err.to_string().contains("only supported for EVM"));
    }

    #[test]
    fn load_account_by_name_keystore_is_lazy() {
        // A keystore account must load WITHOUT prompting for a password: the
        // address is recorded in the store, so read-only operations need no
        // decryption. (There is no terminal in tests, so an eager password
        // prompt would fail here — making this a faithful regression test.)
        let dir = tempfile::tempdir().unwrap();
        let store = store::AccountStore::with_manifest_path(dir.path().join("accounts.toml"));
        let address = "0x0000000000000000000000000000000000000001";
        store
            .add_keystore_account(
                "enc",
                Chain::Ethereum,
                address.to_string(),
                r#"{"placeholder": true}"#,
            )
            .unwrap();

        let account = load_account_by_name(&store, "enc").unwrap();
        assert!(matches!(account, CliAccount::LazyKeystore(_)));
        assert_eq!(account.chain(), Chain::Ethereum);
        assert_eq!(account.address().as_str(), address);
    }

    #[test]
    fn load_account_by_name_keystore_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = store::AccountStore::with_manifest_path(dir.path().join("accounts.toml"));
        store
            .add_keystore_account(
                "enc",
                Chain::Ethereum,
                "0x1234".to_string(),
                r#"{"placeholder": true}"#,
            )
            .unwrap();
        // Simulate a manually deleted keystore file
        std::fs::remove_file(store.keystore_path("enc")).unwrap();

        let err = load_account_by_name(&store, "enc").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }
}
