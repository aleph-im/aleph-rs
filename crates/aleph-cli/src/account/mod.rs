pub mod generate;
pub mod ledger;
pub mod migrate;
pub mod store;

use aleph_types::account::{Account, EvmAccount, SignError, SolanaAccount};
use aleph_types::chain::{Address, Chain, Signature};
use anyhow::{Context, Result, bail};
use zeroize::Zeroizing;

/// Account wrapper that dispatches to the correct signing implementation
/// based on the chain type. This exists because the SDK builders are generic
/// over `A: Account` and need a concrete type, not a trait object.
pub enum CliAccount {
    Evm(EvmAccount),
    Sol(SolanaAccount),
    LedgerEvm(ledger::LedgerEvmAccount),
}

impl std::fmt::Debug for CliAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliAccount::Evm(a) => write!(f, "CliAccount::Evm({})", a.address()),
            CliAccount::Sol(a) => write!(f, "CliAccount::Sol({})", a.address()),
            CliAccount::LedgerEvm(a) => write!(f, "CliAccount::LedgerEvm({})", a.address()),
        }
    }
}

impl Account for CliAccount {
    fn chain(&self) -> Chain {
        match self {
            CliAccount::Evm(a) => a.chain(),
            CliAccount::Sol(a) => a.chain(),
            CliAccount::LedgerEvm(a) => a.chain(),
        }
    }

    fn address(&self) -> &Address {
        match self {
            CliAccount::Evm(a) => a.address(),
            CliAccount::Sol(a) => a.address(),
            CliAccount::LedgerEvm(a) => a.address(),
        }
    }

    fn sign_raw(&self, buffer: &[u8]) -> Result<Signature, SignError> {
        match self {
            CliAccount::Evm(a) => a.sign_raw(buffer),
            CliAccount::Sol(a) => a.sign_raw(buffer),
            CliAccount::LedgerEvm(a) => a.sign_raw(buffer),
        }
    }
}

/// Resolve the hex-encoded private key from explicit arg, env var, or account store.
///
/// Resolution order:
/// 1. `private_key` argument (if Some)
/// 2. `ALEPH_PRIVATE_KEY` environment variable
/// 3. Named account from store (if `account_name` is Some)
/// 4. Default account from store
///
/// Returns the hex-encoded key (without 0x prefix) wrapped in Zeroizing.
/// Errors if the resolved account is a Ledger account (no raw key available).
pub fn resolve_key_hex(
    private_key: Option<&str>,
    account_name: Option<&str>,
) -> Result<Zeroizing<String>> {
    // 1. Explicit private key takes precedence
    if let Some(k) = private_key {
        let hex = k.strip_prefix("0x").unwrap_or(k).to_string();
        return Ok(Zeroizing::new(hex));
    }

    // 2. Environment variable
    if let Ok(k) = std::env::var("ALEPH_PRIVATE_KEY") {
        let hex = k.strip_prefix("0x").unwrap_or(&k).to_string();
        return Ok(Zeroizing::new(hex));
    }

    // 3-4. Account store
    let store = store::AccountStore::open()
        .map_err(|e| anyhow::anyhow!("failed to open account store: {e}"))?;

    let name = match account_name {
        Some(name) => name.to_string(),
        None => store
            .default_account_name()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .ok_or_else(|| anyhow::anyhow!(
                "no account specified and no default account set.\n\
                 Use --private-key, --account, or create an account with: aleph account create --name <NAME>"
            ))?
            .to_string(),
    };

    let entry = store
        .get_account(&name)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    match entry.kind {
        store::AccountKind::Local => {
            let key_hex = store
                .get_private_key(&name)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let hex = key_hex.strip_prefix("0x").unwrap_or(&key_hex).to_string();
            Ok(Zeroizing::new(hex))
        }
        store::AccountKind::Ledger => {
            bail!("Ledger accounts are not supported for credit purchases. Use a local account.")
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
}
