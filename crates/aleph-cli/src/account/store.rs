use aleph_types::chain::Chain;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const KEYRING_SERVICE: &str = "cloud.aleph.cli";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AccountKind {
    Local,
    Ledger, // Phase 2
}

/// One entry in the accounts manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountEntry {
    pub name: String,
    pub chain: Chain,
    pub address: String,
    pub kind: AccountKind,
    /// BIP44 derivation path — only for Ledger accounts (Phase 2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub derivation_path: Option<String>,
}

impl AccountEntry {
    pub fn kind_display(&self) -> &'static str {
        match self.kind {
            AccountKind::Local => "local",
            AccountKind::Ledger => "ledger",
        }
    }
}

/// The on-disk accounts manifest (`accounts.toml`).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccountsManifest {
    /// Name of the default account (used when no --account flag is provided).
    pub default: Option<String>,
    #[serde(default)]
    pub accounts: Vec<AccountEntry>,
}

/// Convert a `keyring` crate error into a `StoreError::Keyring` with
/// platform-specific guidance when the keyring backend is unavailable.
fn keyring_error(err: keyring::Error) -> StoreError {
    let msg = err.to_string();

    // Detect "no backend" / unavailable secret service — common on headless Linux
    let is_backend_unavailable = matches!(
        err,
        keyring::Error::NoStorageAccess(_) | keyring::Error::PlatformFailure(_)
    ) || msg.contains("secret service")
        || msg.contains("dbus")
        || msg.contains("DBus")
        || msg.contains("No storage");

    if is_backend_unavailable {
        StoreError::Keyring(format!(
            "OS keyring is not available: {msg}\n\
             \n\
             On Linux, the keyring requires a running Secret Service provider\n\
             (GNOME Keyring or KWallet) with an unlocked session.\n\
             \n\
             On headless servers, you can use --private-key or the ALEPH_PRIVATE_KEY\n\
             environment variable instead."
        ))
    } else {
        StoreError::Keyring(format!("failed to access keyring: {msg}"))
    }
}

/// Manages reading/writing the accounts manifest and keyring credentials.
pub struct AccountStore {
    manifest_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("account '{0}' already exists")]
    AlreadyExists(String),
    #[error("account '{0}' not found")]
    NotFound(String),
    #[error(
        "invalid account name '{0}': names must be non-empty and contain only alphanumeric characters, hyphens, and underscores"
    )]
    InvalidName(String),
    #[error("{0}")]
    Keyring(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("failed to parse manifest: {0}")]
    Parse(String),
}

impl AccountStore {
    /// Create a store that reads/writes the manifest at the given path.
    /// Used for testing -- production code should use `AccountStore::open()`.
    #[cfg(test)]
    pub fn with_manifest_path(manifest_path: PathBuf) -> Self {
        Self { manifest_path }
    }

    /// Open the default store at `~/.config/aleph/accounts.toml`.
    pub fn open() -> Result<Self, StoreError> {
        let proj = directories::ProjectDirs::from("", "", "aleph").ok_or_else(|| {
            StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "could not determine home directory",
            ))
        })?;
        let config_dir = proj.config_dir();
        std::fs::create_dir_all(config_dir)?;
        Ok(Self {
            manifest_path: config_dir.join("accounts.toml"),
        })
    }

    /// Load the manifest from disk (returns default empty manifest if file doesn't exist).
    pub fn load_manifest(&self) -> Result<AccountsManifest, StoreError> {
        match std::fs::read_to_string(&self.manifest_path) {
            Ok(contents) => toml::from_str(&contents).map_err(|e| StoreError::Parse(e.to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(AccountsManifest::default()),
            Err(e) => Err(StoreError::Io(e)),
        }
    }

    /// Save the manifest to disk with owner-only permissions (0600 on Unix).
    fn save_manifest(&self, manifest: &AccountsManifest) -> Result<(), StoreError> {
        let content =
            toml::to_string_pretty(manifest).map_err(|e| StoreError::Parse(e.to_string()))?;
        if let Some(parent) = self.manifest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        self.write_restricted(&self.manifest_path, &content)?;
        Ok(())
    }

    /// Write a file with restricted permissions (0600 on Unix, default on other platforms).
    fn write_restricted(&self, path: &Path, content: &str) -> Result<(), std::io::Error> {
        use std::io::Write;

        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }

        let mut file = opts.open(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    /// Validate an account name.
    fn validate_name(name: &str) -> Result<(), StoreError> {
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(StoreError::InvalidName(name.to_string()));
        }
        Ok(())
    }

    /// Add a local account (private key stored in keyring).
    ///
    /// The manifest is written first, then the key is stored in the keyring.
    /// If the keyring write fails, the manifest entry is rolled back to prevent
    /// orphaned entries. This ordering ensures that a failed manifest write
    /// (disk full, permissions) never leaves a key orphaned in the keyring
    /// with no way to find it — which would mean permanent key loss for
    /// newly generated keys the user has never seen.
    pub fn add_local_account(
        &self,
        name: &str,
        chain: Chain,
        address: String,
        private_key_hex: &str,
    ) -> Result<(), StoreError> {
        Self::validate_name(name)?;

        let mut manifest = self.load_manifest()?;
        if manifest.accounts.iter().any(|a| a.name == name) {
            return Err(StoreError::AlreadyExists(name.to_string()));
        }

        // 1. Write manifest first — a failure here is safe (no key stored yet)
        manifest.accounts.push(AccountEntry {
            name: name.to_string(),
            chain,
            address,
            kind: AccountKind::Local,
            derivation_path: None,
        });
        if manifest.default.is_none() {
            manifest.default = Some(name.to_string());
        }
        self.save_manifest(&manifest)?;

        // 2. Store key in OS keyring — if this fails, roll back the manifest
        let entry = keyring::Entry::new(KEYRING_SERVICE, name).map_err(keyring_error);
        let keyring_result =
            entry.and_then(|e| e.set_password(private_key_hex).map_err(keyring_error));

        if let Err(keyring_err) = keyring_result {
            // Roll back: remove the entry we just added
            manifest.accounts.retain(|a| a.name != name);
            if manifest.default.as_deref() == Some(name) {
                manifest.default = manifest.accounts.first().map(|a| a.name.clone());
            }
            // Best-effort rollback — if this also fails, we log but return the keyring error
            if let Err(rollback_err) = self.save_manifest(&manifest) {
                eprintln!(
                    "warning: failed to roll back manifest after keyring error: {rollback_err}"
                );
            }
            return Err(keyring_err);
        }

        Ok(())
    }

    /// Add a Ledger account (no private key stored — only address and derivation path).
    pub fn add_ledger_account(
        &self,
        name: &str,
        chain: Chain,
        address: String,
        derivation_path: String,
    ) -> Result<(), StoreError> {
        Self::validate_name(name)?;

        let mut manifest = self.load_manifest()?;
        if manifest.accounts.iter().any(|a| a.name == name) {
            return Err(StoreError::AlreadyExists(name.to_string()));
        }

        manifest.accounts.push(AccountEntry {
            name: name.to_string(),
            chain,
            address,
            kind: AccountKind::Ledger,
            derivation_path: Some(derivation_path),
        });

        if manifest.default.is_none() {
            manifest.default = Some(name.to_string());
        }

        self.save_manifest(&manifest)
    }

    /// Get the private key for a local account from the keyring.
    pub fn get_private_key(&self, name: &str) -> Result<String, StoreError> {
        let manifest = self.load_manifest()?;
        let entry = manifest
            .accounts
            .iter()
            .find(|a| a.name == name)
            .ok_or_else(|| StoreError::NotFound(name.to_string()))?;

        if entry.kind != AccountKind::Local {
            return Err(StoreError::Keyring(format!(
                "account '{name}' is not a local account"
            )));
        }

        let keyring_entry = keyring::Entry::new(KEYRING_SERVICE, name).map_err(keyring_error)?;
        keyring_entry.get_password().map_err(keyring_error)
    }

    /// Look up an account entry by name.
    pub fn get_account(&self, name: &str) -> Result<AccountEntry, StoreError> {
        let manifest = self.load_manifest()?;
        manifest
            .accounts
            .iter()
            .find(|a| a.name == name)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(name.to_string()))
    }

    /// Get the default account name.
    pub fn default_account_name(&self) -> Result<Option<String>, StoreError> {
        Ok(self.load_manifest()?.default)
    }

    /// Set the default account.
    pub fn set_default(&self, name: &str) -> Result<(), StoreError> {
        let mut manifest = self.load_manifest()?;
        if !manifest.accounts.iter().any(|a| a.name == name) {
            return Err(StoreError::NotFound(name.to_string()));
        }
        manifest.default = Some(name.to_string());
        self.save_manifest(&manifest)
    }

    /// Delete an account (removes from manifest and keyring).
    ///
    /// The manifest is updated first, then the keyring entry is removed.
    /// If keyring deletion fails, the key is orphaned but harmless (just
    /// wasted storage). The reverse ordering would risk permanent key loss
    /// if the manifest save failed after the keyring entry was deleted.
    pub fn delete_account(&self, name: &str) -> Result<(), StoreError> {
        let mut manifest = self.load_manifest()?;
        let idx = manifest
            .accounts
            .iter()
            .position(|a| a.name == name)
            .ok_or_else(|| StoreError::NotFound(name.to_string()))?;

        let is_local = manifest.accounts[idx].kind == AccountKind::Local;

        // 1. Update manifest first — remove the entry
        manifest.accounts.remove(idx);
        if manifest.default.as_deref() == Some(name) {
            manifest.default = manifest.accounts.first().map(|a| a.name.clone());
        }
        self.save_manifest(&manifest)?;

        // 2. Remove from keyring if local — failure here is non-fatal
        //    (orphaned keyring entry is harmless)
        if is_local {
            let keyring_entry =
                keyring::Entry::new(KEYRING_SERVICE, name).map_err(keyring_error)?;
            match keyring_entry.delete_credential() {
                Ok(()) => {}
                Err(keyring::Error::NoEntry) => {}
                Err(e) => return Err(keyring_error(e)),
            }
        }

        Ok(())
    }

    /// Return path to the manifest file (for display in CLI output).
    #[allow(dead_code)]
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, AccountStore) {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("accounts.toml");
        let store = AccountStore::with_manifest_path(manifest_path);
        (dir, store)
    }

    #[test]
    fn load_empty_manifest_returns_default() {
        let (_dir, store) = temp_store();
        let manifest = store.load_manifest().unwrap();
        assert!(manifest.default.is_none());
        assert!(manifest.accounts.is_empty());
    }

    #[test]
    fn roundtrip_manifest_serde() {
        let manifest = AccountsManifest {
            default: Some("main".to_string()),
            accounts: vec![
                AccountEntry {
                    name: "main".to_string(),
                    chain: Chain::Ethereum,
                    address: "0xABCD".to_string(),
                    kind: AccountKind::Local,
                    derivation_path: None,
                },
                AccountEntry {
                    name: "hw".to_string(),
                    chain: Chain::Sol,
                    address: "7Hg3".to_string(),
                    kind: AccountKind::Ledger,
                    derivation_path: Some("m/44'/501'/0'/0'".to_string()),
                },
            ],
        };

        let serialized = toml::to_string_pretty(&manifest).unwrap();
        let deserialized: AccountsManifest = toml::from_str(&serialized).unwrap();

        assert_eq!(deserialized.default.as_deref(), Some("main"));
        assert_eq!(deserialized.accounts.len(), 2);
        assert_eq!(deserialized.accounts[0].chain, Chain::Ethereum);
        assert_eq!(deserialized.accounts[1].kind, AccountKind::Ledger);
        assert_eq!(
            deserialized.accounts[1].derivation_path.as_deref(),
            Some("m/44'/501'/0'/0'")
        );
    }

    #[test]
    fn validate_name_rejects_empty() {
        assert!(AccountStore::validate_name("").is_err());
    }

    #[test]
    fn validate_name_rejects_spaces() {
        assert!(AccountStore::validate_name("my wallet").is_err());
    }

    #[test]
    fn validate_name_rejects_special_chars() {
        assert!(AccountStore::validate_name("my@wallet").is_err());
    }

    #[test]
    fn validate_name_accepts_valid() {
        assert!(AccountStore::validate_name("my-wallet_01").is_ok());
    }

    #[test]
    fn save_and_load_manifest() {
        let (_dir, store) = temp_store();
        let manifest = AccountsManifest {
            default: Some("test".to_string()),
            accounts: vec![AccountEntry {
                name: "test".to_string(),
                chain: Chain::Ethereum,
                address: "0x1234".to_string(),
                kind: AccountKind::Local,
                derivation_path: None,
            }],
        };
        store.save_manifest(&manifest).unwrap();
        let loaded = store.load_manifest().unwrap();
        assert_eq!(loaded.default.as_deref(), Some("test"));
        assert_eq!(loaded.accounts.len(), 1);
    }

    #[test]
    fn add_and_load_ledger_account() {
        let (_dir, store) = temp_store();
        store
            .add_ledger_account(
                "hw",
                Chain::Ethereum,
                "0xABCD".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap();

        let manifest = store.load_manifest().unwrap();
        assert_eq!(manifest.accounts.len(), 1);
        assert_eq!(manifest.accounts[0].kind, AccountKind::Ledger);
        assert_eq!(
            manifest.accounts[0].derivation_path.as_deref(),
            Some("m/44'/60'/0'/0/0")
        );
        assert_eq!(manifest.default.as_deref(), Some("hw"));
    }

    #[test]
    fn add_ledger_account_no_keyring_touched() {
        let (_dir, store) = temp_store();
        store
            .add_ledger_account(
                "hw",
                Chain::Sol,
                "7Hg3test".to_string(),
                "m/44'/501'/0'".to_string(),
            )
            .unwrap();

        let err = store.get_private_key("hw").unwrap_err();
        assert!(err.to_string().contains("not a local account"));
    }

    #[test]
    fn set_default_errors_on_unknown_account() {
        let (_dir, store) = temp_store();
        let err = store.set_default("nonexistent").unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    // Integration tests that actually touch the OS keyring are marked #[ignore].
    // Run them manually with: cargo test -p aleph-cli -- --ignored
    #[test]
    #[ignore]
    fn keyring_roundtrip() {
        let (_dir, store) = temp_store();
        store
            .add_local_account(
                "test-keyring-roundtrip",
                Chain::Ethereum,
                "0xtest".to_string(),
                "deadbeef",
            )
            .unwrap();

        let key = store.get_private_key("test-keyring-roundtrip").unwrap();
        assert_eq!(key, "deadbeef");

        // Cleanup
        store.delete_account("test-keyring-roundtrip").unwrap();
    }
}
