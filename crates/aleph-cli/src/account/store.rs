use aleph_types::chain::Chain;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const KEYRING_SERVICE: &str = "cloud.aleph.cli";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AccountKind {
    Local,
    Ledger, // Phase 2
    /// Password-protected Ethereum keystore V3 file (no keyring).
    /// Serialized as "keystore" in the manifest; displayed as "encrypted".
    Keystore,
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

/// An address alias — a named bookmark for an address without a private key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasEntry {
    pub name: String,
    pub address: String,
}

impl AccountEntry {
    pub fn kind_display(&self) -> &'static str {
        match self.kind {
            AccountKind::Local => "local",
            AccountKind::Ledger => "ledger",
            AccountKind::Keystore => "encrypted",
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
    #[serde(default)]
    pub aliases: Vec<AliasEntry>,
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
    #[error("keystore file for account '{name}' not found at {}", path.display())]
    KeystoreMissing { name: String, path: PathBuf },
    #[error(
        "cannot change account '{name}' from {from} to {to}: the address is derived from the key and differs between chain families (EVM vs SVM), so the label cannot simply be switched. Import a separate account for {to} instead."
    )]
    ChainFamilyMismatch {
        name: String,
        from: Chain,
        to: Chain,
    },
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

    /// Verify a name is valid and not already taken by an account or alias.
    ///
    /// Useful as a fast pre-flight before slow operations (e.g. talking to a
    /// Ledger device) so the user isn't asked to plug in their device just to
    /// be told the name was invalid afterwards.
    pub fn check_name_available(&self, name: &str) -> Result<(), StoreError> {
        Self::validate_name(name)?;
        let manifest = self.load_manifest()?;
        if manifest.accounts.iter().any(|a| a.name == name)
            || manifest.aliases.iter().any(|a| a.name == name)
        {
            return Err(StoreError::AlreadyExists(name.to_string()));
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
        if manifest.accounts.iter().any(|a| a.name == name)
            || manifest.aliases.iter().any(|a| a.name == name)
        {
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
        if manifest.accounts.iter().any(|a| a.name == name)
            || manifest.aliases.iter().any(|a| a.name == name)
        {
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

    fn keystores_dir(&self) -> PathBuf {
        self.manifest_path
            .parent()
            .map(|p| p.join("keystores"))
            .unwrap_or_else(|| PathBuf::from("keystores"))
    }

    /// Path of the keystore file for an account (derived from its name).
    pub fn keystore_path(&self, name: &str) -> PathBuf {
        self.keystores_dir().join(format!("{name}.json"))
    }

    /// Add a keystore (password-encrypted file) account.
    ///
    /// File first, manifest second — the reverse of `add_local_account`.
    ///
    /// The keyring flow writes the manifest first because an orphaned
    /// keyring secret would be undiscoverable. A keystore file lives at a
    /// deterministic path derived from the name, so an orphaned file is
    /// discoverable and harmless; a manifest entry pointing at a missing
    /// file would instead be a broken account. Writing the file first means
    /// any single failure leaves at worst an orphaned file.
    pub fn add_keystore_account(
        &self,
        name: &str,
        chain: Chain,
        address: String,
        keystore_json: &str,
    ) -> Result<(), StoreError> {
        Self::validate_name(name)?;

        let mut manifest = self.load_manifest()?;
        if manifest.accounts.iter().any(|a| a.name == name)
            || manifest.aliases.iter().any(|a| a.name == name)
        {
            return Err(StoreError::AlreadyExists(name.to_string()));
        }

        let dir = self.keystores_dir();
        std::fs::create_dir_all(&dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        }
        self.write_restricted(&self.keystore_path(name), keystore_json)?;

        manifest.accounts.push(AccountEntry {
            name: name.to_string(),
            chain,
            address,
            kind: AccountKind::Keystore,
            derivation_path: None,
        });
        if manifest.default.is_none() {
            manifest.default = Some(name.to_string());
        }
        if let Err(save_err) = self.save_manifest(&manifest) {
            // Best-effort cleanup; an orphaned file would be harmless anyway.
            let _ = std::fs::remove_file(self.keystore_path(name));
            return Err(save_err);
        }

        Ok(())
    }

    /// Read the raw keystore JSON for an account.
    pub fn read_keystore_json(&self, name: &str) -> Result<String, StoreError> {
        let path = self.keystore_path(name);
        match std::fs::read_to_string(&path) {
            Ok(contents) => Ok(contents),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StoreError::KeystoreMissing {
                    name: name.to_string(),
                    path,
                })
            }
            Err(e) => Err(StoreError::Io(e)),
        }
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

    /// Change the chain of an existing account.
    ///
    /// Only changes within the same signature family are allowed (EVM↔EVM,
    /// SVM↔SVM). The account address is derived from the key and is invariant
    /// within a family, so the stored address stays valid. Crossing families
    /// (e.g. EVM→SOL) would silently point the account at a different address,
    /// so it is rejected with `ChainFamilyMismatch`.
    pub fn set_account_chain(&self, name: &str, chain: Chain) -> Result<(), StoreError> {
        let mut manifest = self.load_manifest()?;
        let entry = manifest
            .accounts
            .iter_mut()
            .find(|a| a.name == name)
            .ok_or_else(|| StoreError::NotFound(name.to_string()))?;

        let same_family =
            (entry.chain.is_evm() && chain.is_evm()) || (entry.chain.is_svm() && chain.is_svm());
        if entry.chain != chain && !same_family {
            return Err(StoreError::ChainFamilyMismatch {
                name: name.to_string(),
                from: entry.chain.clone(),
                to: chain,
            });
        }

        entry.chain = chain;
        self.save_manifest(&manifest)
    }

    /// Rename an account, moving any associated secret material.
    ///
    /// The secret (keyring entry for local accounts, keystore file for
    /// encrypted accounts) is copied to the new name *before* the manifest is
    /// updated, and the old copy is removed only after the manifest save
    /// succeeds. A failure at any step therefore leaves the original account
    /// intact and at worst an orphaned (harmless) copy under the new name.
    pub fn rename_account(&self, old: &str, new: &str) -> Result<(), StoreError> {
        Self::validate_name(new)?;
        if old == new {
            return Ok(());
        }

        let mut manifest = self.load_manifest()?;
        if manifest.accounts.iter().any(|a| a.name == new)
            || manifest.aliases.iter().any(|a| a.name == new)
        {
            return Err(StoreError::AlreadyExists(new.to_string()));
        }
        let idx = manifest
            .accounts
            .iter()
            .position(|a| a.name == old)
            .ok_or_else(|| StoreError::NotFound(old.to_string()))?;
        let kind = manifest.accounts[idx].kind;

        // 1. Stage the secret under the new name (old copy untouched for now).
        match kind {
            AccountKind::Local => {
                let old_entry = keyring::Entry::new(KEYRING_SERVICE, old).map_err(keyring_error)?;
                let secret = old_entry.get_password().map_err(keyring_error)?;
                let new_entry = keyring::Entry::new(KEYRING_SERVICE, new).map_err(keyring_error)?;
                new_entry.set_password(&secret).map_err(keyring_error)?;
            }
            AccountKind::Keystore => {
                let json = self.read_keystore_json(old)?;
                let dir = self.keystores_dir();
                std::fs::create_dir_all(&dir)?;
                self.write_restricted(&self.keystore_path(new), &json)?;
            }
            AccountKind::Ledger => {}
        }

        // 2. Update the manifest. On failure, clean up the staged copy.
        manifest.accounts[idx].name = new.to_string();
        if manifest.default.as_deref() == Some(old) {
            manifest.default = Some(new.to_string());
        }
        if let Err(save_err) = self.save_manifest(&manifest) {
            match kind {
                AccountKind::Local => {
                    if let Ok(e) = keyring::Entry::new(KEYRING_SERVICE, new) {
                        let _ = e.delete_credential();
                    }
                }
                AccountKind::Keystore => {
                    let _ = std::fs::remove_file(self.keystore_path(new));
                }
                AccountKind::Ledger => {}
            }
            return Err(save_err);
        }

        // 3. Manifest committed — remove the old secret (best effort).
        match kind {
            AccountKind::Local => {
                if let Ok(e) = keyring::Entry::new(KEYRING_SERVICE, old) {
                    match e.delete_credential() {
                        Ok(()) | Err(keyring::Error::NoEntry) => {}
                        Err(err) => {
                            eprintln!("warning: failed to remove old keyring entry: {err}");
                        }
                    }
                }
            }
            AccountKind::Keystore => {
                if let Err(e) = std::fs::remove_file(self.keystore_path(old))
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    eprintln!("warning: failed to remove old keystore file: {e}");
                }
            }
            AccountKind::Ledger => {}
        }

        Ok(())
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

        let kind = manifest.accounts[idx].kind;

        // 1. Update manifest first — remove the entry
        manifest.accounts.remove(idx);
        if manifest.default.as_deref() == Some(name) {
            manifest.default = manifest.accounts.first().map(|a| a.name.clone());
        }
        self.save_manifest(&manifest)?;

        // 2. Clean up the secret store based on account kind
        match kind {
            AccountKind::Local => {
                let keyring_entry =
                    keyring::Entry::new(KEYRING_SERVICE, name).map_err(keyring_error)?;
                match keyring_entry.delete_credential() {
                    Ok(()) => {}
                    Err(keyring::Error::NoEntry) => {}
                    Err(e) => return Err(keyring_error(e)),
                }
            }
            AccountKind::Keystore => {
                // Non-fatal if already gone — the manifest entry is removed,
                // and an orphaned file is only wasted disk.
                if let Err(e) = std::fs::remove_file(self.keystore_path(name))
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    eprintln!("warning: failed to remove keystore file: {e}");
                }
            }
            AccountKind::Ledger => {}
        }

        Ok(())
    }

    /// Add an address alias (a named bookmark with no private key).
    pub fn add_alias(&self, name: &str, address: String) -> Result<(), StoreError> {
        Self::validate_name(name)?;

        let mut manifest = self.load_manifest()?;
        if manifest.accounts.iter().any(|a| a.name == name)
            || manifest.aliases.iter().any(|a| a.name == name)
        {
            return Err(StoreError::AlreadyExists(name.to_string()));
        }

        manifest.aliases.push(AliasEntry {
            name: name.to_string(),
            address,
        });
        self.save_manifest(&manifest)
    }

    /// Look up an alias by name.
    pub fn get_alias(&self, name: &str) -> Result<AliasEntry, StoreError> {
        let manifest = self.load_manifest()?;
        manifest
            .aliases
            .iter()
            .find(|a| a.name == name)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(name.to_string()))
    }

    /// Remove an alias by name.
    pub fn remove_alias(&self, name: &str) -> Result<(), StoreError> {
        let mut manifest = self.load_manifest()?;
        let idx = manifest
            .aliases
            .iter()
            .position(|a| a.name == name)
            .ok_or_else(|| StoreError::NotFound(name.to_string()))?;
        manifest.aliases.remove(idx);
        self.save_manifest(&manifest)
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
            aliases: vec![],
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
            aliases: vec![],
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

    #[test]
    fn set_account_chain_within_evm_family_keeps_address() {
        let (_dir, store) = temp_store();
        store
            .add_ledger_account(
                "hw",
                Chain::Base,
                "0xABCD".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap();

        store.set_account_chain("hw", Chain::Ethereum).unwrap();

        let entry = store.get_account("hw").unwrap();
        assert_eq!(entry.chain, Chain::Ethereum);
        assert_eq!(entry.address, "0xABCD");
    }

    #[test]
    fn set_account_chain_rejects_cross_family() {
        let (_dir, store) = temp_store();
        store
            .add_ledger_account(
                "hw",
                Chain::Ethereum,
                "0xABCD".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap();

        let err = store.set_account_chain("hw", Chain::Sol).unwrap_err();
        assert!(matches!(err, StoreError::ChainFamilyMismatch { .. }));
        // Unchanged on rejection.
        assert_eq!(store.get_account("hw").unwrap().chain, Chain::Ethereum);
    }

    #[test]
    fn set_account_chain_unknown_account_errors() {
        let (_dir, store) = temp_store();
        let err = store
            .set_account_chain("ghost", Chain::Ethereum)
            .unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[test]
    fn rename_ledger_account_updates_name_and_default() {
        let (_dir, store) = temp_store();
        store
            .add_ledger_account(
                "old",
                Chain::Ethereum,
                "0xABCD".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap();
        assert_eq!(
            store.default_account_name().unwrap().as_deref(),
            Some("old")
        );

        store.rename_account("old", "new").unwrap();

        assert!(store.get_account("old").is_err());
        assert_eq!(store.get_account("new").unwrap().address, "0xABCD");
        assert_eq!(
            store.default_account_name().unwrap().as_deref(),
            Some("new")
        );
    }

    #[test]
    fn rename_keystore_account_moves_file() {
        let (_dir, store) = temp_store();
        store
            .add_keystore_account("old", Chain::Ethereum, "0x1234".to_string(), KEYSTORE_JSON)
            .unwrap();
        assert!(store.keystore_path("old").exists());

        store.rename_account("old", "new").unwrap();

        assert!(!store.keystore_path("old").exists());
        assert!(store.keystore_path("new").exists());
        assert_eq!(store.read_keystore_json("new").unwrap(), KEYSTORE_JSON);
    }

    #[test]
    fn rename_to_existing_name_rejected() {
        let (_dir, store) = temp_store();
        store
            .add_ledger_account(
                "a",
                Chain::Ethereum,
                "0x1".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap();
        store
            .add_ledger_account(
                "b",
                Chain::Ethereum,
                "0x2".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap();

        let err = store.rename_account("a", "b").unwrap_err();
        assert!(matches!(err, StoreError::AlreadyExists(_)));
        assert!(store.get_account("a").is_ok());
    }

    #[test]
    fn rename_unknown_account_errors() {
        let (_dir, store) = temp_store();
        let err = store.rename_account("ghost", "new").unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[test]
    fn rename_rejects_invalid_new_name() {
        let (_dir, store) = temp_store();
        store
            .add_ledger_account(
                "ok",
                Chain::Ethereum,
                "0x1".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap();
        let err = store.rename_account("ok", "bad name!").unwrap_err();
        assert!(matches!(err, StoreError::InvalidName(_)));
    }

    #[test]
    fn add_and_get_alias() {
        let (_dir, store) = temp_store();
        store
            .add_alias("treasury", "0xABCD1234".to_string())
            .unwrap();

        let alias = store.get_alias("treasury").unwrap();
        assert_eq!(alias.name, "treasury");
        assert_eq!(alias.address, "0xABCD1234");
    }

    #[test]
    fn add_and_remove_alias() {
        let (_dir, store) = temp_store();
        store
            .add_alias("treasury", "0xABCD1234".to_string())
            .unwrap();
        store.remove_alias("treasury").unwrap();

        let err = store.get_alias("treasury").unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[test]
    fn remove_nonexistent_alias_errors() {
        let (_dir, store) = temp_store();
        let err = store.remove_alias("nope").unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[test]
    fn alias_name_collides_with_account() {
        let (_dir, store) = temp_store();
        store
            .add_ledger_account(
                "shared",
                Chain::Ethereum,
                "0x1111".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap();

        let err = store.add_alias("shared", "0x2222".to_string()).unwrap_err();
        assert!(matches!(err, StoreError::AlreadyExists(_)));
    }

    #[test]
    fn account_name_collides_with_alias() {
        let (_dir, store) = temp_store();
        store.add_alias("shared", "0x1111".to_string()).unwrap();

        let err = store
            .add_ledger_account(
                "shared",
                Chain::Ethereum,
                "0x2222".to_string(),
                "m/44'/60'/0'/0/0".to_string(),
            )
            .unwrap_err();
        assert!(matches!(err, StoreError::AlreadyExists(_)));
    }

    #[test]
    fn alias_roundtrip_manifest_serde() {
        let (_dir, store) = temp_store();
        store.add_alias("treasury", "0xABCD".to_string()).unwrap();
        store.add_alias("vault", "0xDEAD".to_string()).unwrap();

        let manifest = store.load_manifest().unwrap();
        assert_eq!(manifest.aliases.len(), 2);
        assert_eq!(manifest.aliases[0].name, "treasury");
        assert_eq!(manifest.aliases[1].address, "0xDEAD");
    }

    #[test]
    fn alias_invalid_name_rejected() {
        let (_dir, store) = temp_store();
        let err = store
            .add_alias("bad name!", "0x1234".to_string())
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidName(_)));
    }

    const KEYSTORE_JSON: &str = r#"{"version":3,"id":"test","crypto":{}}"#;

    #[test]
    fn add_keystore_account_writes_file_and_manifest() {
        let (_dir, store) = temp_store();
        store
            .add_keystore_account("enc", Chain::Ethereum, "0x1234".to_string(), KEYSTORE_JSON)
            .unwrap();

        let manifest = store.load_manifest().unwrap();
        assert_eq!(manifest.accounts.len(), 1);
        assert_eq!(manifest.accounts[0].kind, AccountKind::Keystore);
        assert_eq!(manifest.default.as_deref(), Some("enc"));

        let contents = store.read_keystore_json("enc").unwrap();
        assert_eq!(contents, KEYSTORE_JSON);
    }

    #[test]
    fn keystore_kind_displays_as_encrypted() {
        let entry = AccountEntry {
            name: "enc".to_string(),
            chain: Chain::Ethereum,
            address: "0x1234".to_string(),
            kind: AccountKind::Keystore,
            derivation_path: None,
        };
        assert_eq!(entry.kind_display(), "encrypted");
    }

    #[test]
    fn keystore_kind_roundtrips_in_manifest() {
        let (_dir, store) = temp_store();
        store
            .add_keystore_account("enc", Chain::Ethereum, "0x1234".to_string(), KEYSTORE_JSON)
            .unwrap();
        let manifest = store.load_manifest().unwrap();
        let serialized = toml::to_string_pretty(&manifest).unwrap();
        assert!(serialized.contains("keystore"));
        let parsed: AccountsManifest = toml::from_str(&serialized).unwrap();
        assert_eq!(parsed.accounts[0].kind, AccountKind::Keystore);
    }

    #[test]
    fn read_keystore_json_missing_file_errors_with_path() {
        let (_dir, store) = temp_store();
        // Manifest entry without a file (e.g. file deleted manually)
        let err = store.read_keystore_json("ghost").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ghost"));
        assert!(msg.contains("keystores"));
    }

    #[test]
    fn delete_keystore_account_removes_file() {
        let (_dir, store) = temp_store();
        store
            .add_keystore_account("enc", Chain::Ethereum, "0x1234".to_string(), KEYSTORE_JSON)
            .unwrap();
        let path = store.keystore_path("enc");
        assert!(path.exists());

        store.delete_account("enc").unwrap();
        assert!(!path.exists());
        assert!(store.load_manifest().unwrap().accounts.is_empty());
    }

    #[test]
    fn failed_keystore_write_leaves_no_manifest_entry() {
        let (dir, store) = temp_store();
        // A *file* named "keystores" makes create_dir_all fail deterministically.
        std::fs::write(dir.path().join("keystores"), b"not a dir").unwrap();

        let err = store
            .add_keystore_account("enc", Chain::Ethereum, "0x1234".to_string(), KEYSTORE_JSON)
            .unwrap_err();
        assert!(matches!(err, StoreError::Io(_)));

        let manifest = store.load_manifest().unwrap();
        assert!(manifest.accounts.is_empty());
        assert!(manifest.default.is_none());
    }

    #[test]
    fn keystore_account_name_collision_rejected() {
        let (_dir, store) = temp_store();
        store
            .add_keystore_account("enc", Chain::Ethereum, "0x1234".to_string(), KEYSTORE_JSON)
            .unwrap();
        let err = store
            .add_keystore_account("enc", Chain::Ethereum, "0x5678".to_string(), KEYSTORE_JSON)
            .unwrap_err();
        assert!(matches!(err, StoreError::AlreadyExists(_)));
    }

    #[cfg(unix)]
    #[test]
    fn keystore_file_has_restricted_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let (_dir, store) = temp_store();
        store
            .add_keystore_account("enc", Chain::Ethereum, "0x1234".to_string(), KEYSTORE_JSON)
            .unwrap();
        let mode = std::fs::metadata(store.keystore_path("enc"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn get_private_key_rejects_keystore_account() {
        let (_dir, store) = temp_store();
        store
            .add_keystore_account("enc", Chain::Ethereum, "0x1234".to_string(), KEYSTORE_JSON)
            .unwrap();
        let err = store.get_private_key("enc").unwrap_err();
        assert!(err.to_string().contains("not a local account"));
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
