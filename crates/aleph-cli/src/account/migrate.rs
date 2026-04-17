use aleph_types::account::Account;
use anyhow::{Context, Result};
use std::path::Path;
use zeroize::Zeroizing;

use crate::account::store::AccountStore;

/// Read a private key from a file.
///
/// If the file is exactly 32 bytes, treat it as raw binary (Python CLI `.key` format)
/// and hex-encode it. Otherwise, read as UTF-8 text, trim whitespace, strip an
/// optional `0x` prefix, and validate as hex.
pub fn read_key_file(path: &Path) -> Result<Zeroizing<String>> {
    let raw = std::fs::read(path)
        .with_context(|| format!("failed to read key file: {}", path.display()))?;

    if raw.is_empty() {
        anyhow::bail!("key file is empty: {}", path.display());
    }

    if raw.len() == 32 {
        return Ok(Zeroizing::new(hex::encode(&raw)));
    }

    let text = std::str::from_utf8(&raw).with_context(|| {
        format!(
            "key file is not 32-byte binary and not valid UTF-8: {}",
            path.display()
        )
    })?;
    let trimmed = text.trim();
    let hex_str = trimmed.strip_prefix("0x").unwrap_or(trimmed);

    // Validate hex
    hex::decode(hex_str).with_context(|| {
        format!(
            "key file is not 32-byte binary and not valid hex text: {}",
            path.display()
        )
    })?;

    Ok(Zeroizing::new(hex_str.to_string()))
}

/// Parsed representation of the Python CLI's `config.json`.
#[derive(Debug, serde::Deserialize)]
pub struct PythonConfig {
    /// Path to the active key file.
    pub path: Option<String>,
    /// Account type: "imported" (or legacy "internal") for file-based keys,
    /// "hardware" (or legacy "external") for Ledger.
    #[serde(rename = "type")]
    pub account_type: Option<String>,
    /// Active chain code (e.g. "ETH", "SOL").
    pub chain: Option<String>,
    /// Ledger address.
    pub address: Option<String>,
    /// Ledger derivation path (may lack "m/" prefix).
    pub derivation_path: Option<String>,
}

impl PythonConfig {
    /// Read and parse the Python CLI's config.json.
    /// Returns `Ok(None)` if the file does not exist.
    pub fn load(python_home: &Path) -> Result<Option<Self>> {
        let path = python_home.join("config.json");
        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                let config: Self = serde_json::from_str(&contents)
                    .with_context(|| format!("failed to parse {}", path.display()))?;
                Ok(Some(config))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
        }
    }

    /// Whether this config describes a Ledger hardware wallet.
    pub fn is_hardware(&self) -> bool {
        matches!(
            self.account_type.as_deref(),
            Some("hardware") | Some("external")
        )
    }

    /// Whether this config describes a file-based (imported) key.
    #[cfg(test)]
    pub fn is_imported(&self) -> bool {
        matches!(
            self.account_type.as_deref(),
            Some("imported") | Some("internal")
        ) || (self.account_type.is_none() && self.path.is_some())
    }

    /// Parse the chain code into a `Chain`, defaulting to Ethereum.
    pub fn chain_or_default(&self) -> aleph_types::chain::Chain {
        self.chain
            .as_deref()
            .and_then(|s| serde_json::from_value(serde_json::Value::String(s.to_string())).ok())
            .unwrap_or(aleph_types::chain::Chain::Ethereum)
    }
}

/// Normalize a derivation path: prepend "m/" if missing.
///
/// Python CLI stores paths like `"44'/60'/0'/0/0"`, Rust CLI expects `"m/44'/60'/0'/0/0"`.
pub fn normalize_derivation_path(path: &str) -> String {
    if path.starts_with("m/") {
        path.to_string()
    } else {
        format!("m/{path}")
    }
}

/// Result of a single account migration attempt.
#[derive(Debug)]
pub struct MigratedAccount {
    pub name: String,
    pub chain: aleph_types::chain::Chain,
    pub address: String,
    pub kind: &'static str,
    pub derivation_path: Option<String>,
    pub is_default: bool,
}

/// Result of a single skipped file.
#[derive(Debug)]
pub struct SkippedFile {
    pub filename: String,
    pub reason: String,
}

/// Full result of a migration run.
#[derive(Debug)]
pub struct MigrateResult {
    pub migrated: Vec<MigratedAccount>,
    pub skipped: Vec<SkippedFile>,
}

/// Resolve the Python CLI config home directory.
pub fn resolve_python_home(override_path: Option<&Path>) -> Result<std::path::PathBuf> {
    if let Some(p) = override_path {
        return Ok(p.to_path_buf());
    }
    if let Ok(p) = std::env::var("ALEPH_CONFIG_HOME") {
        return Ok(std::path::PathBuf::from(p));
    }
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        return Ok(std::path::PathBuf::from(xdg).join(".aleph-im"));
    }
    let base = directories::BaseDirs::new()
        .ok_or_else(|| anyhow::anyhow!("could not determine home directory"))?;
    Ok(base.home_dir().join(".aleph-im"))
}

/// Derive an account name from a key filename.
///
/// Strips the `.key` extension. Returns `None` if the resulting name
/// is invalid (not alphanumeric + hyphens + underscores).
fn name_from_filename(filename: &str) -> Option<String> {
    let name = filename.strip_suffix(".key")?;
    if name.is_empty() {
        return None;
    }
    let valid = name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_');
    if valid { Some(name.to_string()) } else { None }
}

/// Find an available name by appending -1, -2, ... if the base name is taken.
fn available_name(base: &str, existing: &[String]) -> String {
    if !existing.contains(&base.to_string()) {
        return base.to_string();
    }
    for i in 1.. {
        let candidate = format!("{base}-{i}");
        if !existing.contains(&candidate) {
            return candidate;
        }
    }
    unreachable!()
}

/// Discover and import Python CLI accounts into the Rust CLI account store.
///
/// If `dry_run` is true, no accounts are actually imported — only the discovery
/// and validation logic runs.
pub fn migrate_accounts(
    store: &AccountStore,
    python_home: &Path,
    dry_run: bool,
) -> Result<MigrateResult> {
    if !python_home.exists() {
        anyhow::bail!("Python CLI directory not found: {}", python_home.display());
    }

    // Check keyring availability early to fail fast before importing anything
    if !dry_run {
        let probe = keyring::Entry::new("cloud.aleph.cli", "__migrate_probe__");
        match probe {
            Ok(entry) => {
                // Try a no-op read — if the backend is unavailable, this will fail
                match entry.get_password() {
                    Err(keyring::Error::NoEntry) => {} // Backend works, key just doesn't exist
                    Err(keyring::Error::NoStorageAccess(_))
                    | Err(keyring::Error::PlatformFailure(_)) => {
                        anyhow::bail!(
                            "OS keyring is not available. Migration requires a running \
                             Secret Service provider (GNOME Keyring or KWallet) with an \
                             unlocked session.\n\n\
                             Use --dry-run to preview what would be imported."
                        );
                    }
                    _ => {} // Other results (including Ok) are fine
                }
            }
            Err(_) => {
                anyhow::bail!(
                    "OS keyring is not available. Migration requires a running \
                     Secret Service provider (GNOME Keyring or KWallet) with an \
                     unlocked session.\n\n\
                     Use --dry-run to preview what would be imported."
                );
            }
        }
    }

    let config = PythonConfig::load(python_home)?;
    let keys_dir = python_home.join("private-keys");

    let mut result = MigrateResult {
        migrated: Vec::new(),
        skipped: Vec::new(),
    };

    // Collect names already in the store + names we've imported this run
    let manifest = store.load_manifest().map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut used_names: Vec<String> = manifest.accounts.iter().map(|a| a.name.clone()).collect();
    let mut known_addresses: Vec<String> = manifest
        .accounts
        .iter()
        .map(|a| a.address.clone())
        .collect();
    let has_existing_default = manifest.default.is_some();

    // Determine the active key path from config.json
    let active_key_path = config
        .as_ref()
        .and_then(|c| c.path.as_deref())
        .map(std::path::PathBuf::from);
    let active_chain = config
        .as_ref()
        .map(|c| c.chain_or_default())
        .unwrap_or(aleph_types::chain::Chain::Ethereum);

    // --- Import Ledger from config.json ---
    if let Some(cfg) = config.as_ref().filter(|c| c.is_hardware())
        && let Some(address) = &cfg.address
    {
        if known_addresses.contains(address) {
            result.skipped.push(SkippedFile {
                filename: "config.json (ledger)".to_string(),
                reason: format!("address {address} already exists"),
            });
        } else {
            let name = available_name("ledger", &used_names);
            let chain = cfg.chain_or_default();
            let derivation_path = cfg
                .derivation_path
                .as_deref()
                .map(normalize_derivation_path)
                .unwrap_or_else(|| "m/44'/60'/0'/0/0".to_string());

            if !dry_run {
                store
                    .add_ledger_account(
                        &name,
                        chain.clone(),
                        address.clone(),
                        derivation_path.clone(),
                    )
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
            }

            used_names.push(name.clone());
            known_addresses.push(address.clone());
            result.migrated.push(MigratedAccount {
                name,
                chain,
                address: address.clone(),
                kind: "ledger",
                derivation_path: Some(derivation_path),
                is_default: false,
            });
        }
    }

    // --- Scan key files ---
    if keys_dir.exists() {
        let mut entries: Vec<_> = std::fs::read_dir(&keys_dir)
            .with_context(|| format!("failed to read {}", keys_dir.display()))?
            .filter_map(|e| e.ok())
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let path = entry.path();
            let filename = entry.file_name().to_string_lossy().to_string();

            // Skip symlinks (e.g. default.key)
            if path.is_symlink() {
                continue;
            }

            // Warn about mnemonic files
            if filename.ends_with(".mnemonic") {
                result.skipped.push(SkippedFile {
                    filename: filename.clone(),
                    reason: "Substrate accounts are not supported".to_string(),
                });
                continue;
            }

            // Only process .key files
            if !filename.ends_with(".key") {
                continue;
            }

            // Derive name from filename
            let name = match name_from_filename(&filename) {
                Some(n) => n,
                None => {
                    result.skipped.push(SkippedFile {
                        filename: filename.clone(),
                        reason: "invalid account name (must be alphanumeric, hyphens, underscores)"
                            .to_string(),
                    });
                    continue;
                }
            };

            // Check name conflicts
            let name = available_name(&name, &used_names);

            // Read key file
            let key_hex = match read_key_file(&path) {
                Ok(k) => k,
                Err(e) => {
                    result.skipped.push(SkippedFile {
                        filename: filename.clone(),
                        reason: format!("{e}"),
                    });
                    continue;
                }
            };

            // Determine chain: use config chain if this is the active key, else ETH
            let chain = if active_key_path.as_deref() == Some(&path) {
                active_chain.clone()
            } else {
                aleph_types::chain::Chain::Ethereum
            };

            // Derive address
            let account = match crate::account::load_account(Some(&key_hex), chain.clone()) {
                Ok(a) => a,
                Err(e) => {
                    result.skipped.push(SkippedFile {
                        filename: filename.clone(),
                        reason: format!("failed to load key: {e}"),
                    });
                    continue;
                }
            };
            let address = account.address().to_string();

            // Check address conflicts
            if known_addresses.contains(&address) {
                result.skipped.push(SkippedFile {
                    filename: filename.clone(),
                    reason: format!("address {address} already exists"),
                });
                continue;
            }

            if !dry_run {
                store
                    .add_local_account(&name, chain.clone(), address.clone(), &key_hex)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
            }

            let is_default = !has_existing_default && active_key_path.as_deref() == Some(&path);

            used_names.push(name.clone());
            known_addresses.push(address.clone());
            result.migrated.push(MigratedAccount {
                name,
                chain,
                address,
                kind: "local",
                derivation_path: None,
                is_default,
            });
        }
    }

    // Set default account
    if !dry_run && !has_existing_default {
        // Prefer the active key from config.json, otherwise first imported
        let default_name = result
            .migrated
            .iter()
            .find(|m| m.is_default)
            .or_else(|| result.migrated.first())
            .map(|m| m.name.clone());

        if let Some(ref name) = default_name {
            store
                .set_default(name)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }

        // Update the is_default flags for display
        if let Some(ref default_name) = default_name {
            for m in &mut result.migrated {
                m.is_default = m.name == *default_name;
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_key_file_raw_32_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.key");
        let key_bytes: [u8; 32] = [0xab; 32];
        std::fs::write(&path, key_bytes).unwrap();

        let result = read_key_file(&path).unwrap();
        assert_eq!(*result, "ab".repeat(32));
    }

    #[test]
    fn read_key_file_hex_text_no_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.key");
        let hex_str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efbba0f2d1db744ce06";
        std::fs::write(&path, hex_str).unwrap();

        let result = read_key_file(&path).unwrap();
        assert_eq!(*result, hex_str);
    }

    #[test]
    fn read_key_file_hex_text_with_0x_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.key");
        let hex_str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efbba0f2d1db744ce06";
        std::fs::write(&path, format!("0x{hex_str}")).unwrap();

        let result = read_key_file(&path).unwrap();
        assert_eq!(*result, hex_str);
    }

    #[test]
    fn read_key_file_hex_text_with_whitespace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.key");
        let hex_str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efbba0f2d1db744ce06";
        std::fs::write(&path, format!("  0x{hex_str}\n")).unwrap();

        let result = read_key_file(&path).unwrap();
        assert_eq!(*result, hex_str);
    }

    #[test]
    fn read_key_file_invalid_not_binary_not_hex() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.key");
        std::fs::write(&path, "not a valid key at all!").unwrap();

        let err = read_key_file(&path).unwrap_err();
        assert!(err.to_string().contains("not valid hex"));
    }

    #[test]
    fn read_key_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.key");
        std::fs::write(&path, "").unwrap();

        let err = read_key_file(&path).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn read_key_file_nonexistent() {
        let path = Path::new("/tmp/nonexistent-key-file-test.key");
        let err = read_key_file(path).unwrap_err();
        assert!(err.to_string().contains("failed to read"));
    }

    #[test]
    fn parse_python_config_imported() {
        let dir = tempfile::tempdir().unwrap();
        let config = r#"{
            "path": "/home/user/.aleph-im/private-keys/ethereum.key",
            "type": "imported",
            "chain": "ETH"
        }"#;
        std::fs::write(dir.path().join("config.json"), config).unwrap();

        let parsed = PythonConfig::load(dir.path()).unwrap().unwrap();
        assert!(parsed.is_imported());
        assert!(!parsed.is_hardware());
        assert_eq!(
            parsed.chain_or_default(),
            aleph_types::chain::Chain::Ethereum
        );
        assert_eq!(
            parsed.path.as_deref(),
            Some("/home/user/.aleph-im/private-keys/ethereum.key")
        );
    }

    #[test]
    fn parse_python_config_hardware() {
        let dir = tempfile::tempdir().unwrap();
        let config = r#"{
            "type": "hardware",
            "chain": "ETH",
            "address": "0x1234abcd",
            "derivation_path": "44'/60'/0'/0/0"
        }"#;
        std::fs::write(dir.path().join("config.json"), config).unwrap();

        let parsed = PythonConfig::load(dir.path()).unwrap().unwrap();
        assert!(parsed.is_hardware());
        assert!(!parsed.is_imported());
        assert_eq!(parsed.address.as_deref(), Some("0x1234abcd"));
        assert_eq!(parsed.derivation_path.as_deref(), Some("44'/60'/0'/0/0"));
    }

    #[test]
    fn parse_python_config_legacy_types() {
        let dir = tempfile::tempdir().unwrap();

        // Legacy "internal" maps to imported
        let config = r#"{"type": "internal", "path": "/some/path.key"}"#;
        std::fs::write(dir.path().join("config.json"), config).unwrap();
        let parsed = PythonConfig::load(dir.path()).unwrap().unwrap();
        assert!(parsed.is_imported());

        // Legacy "external" maps to hardware
        let config = r#"{"type": "external", "address": "0xabc"}"#;
        std::fs::write(dir.path().join("config.json"), config).unwrap();
        let parsed = PythonConfig::load(dir.path()).unwrap().unwrap();
        assert!(parsed.is_hardware());
    }

    #[test]
    fn parse_python_config_infer_type_from_path() {
        let dir = tempfile::tempdir().unwrap();
        // No explicit type, but has a path -> assume imported
        let config = r#"{"path": "/some/path.key", "chain": "ETH"}"#;
        std::fs::write(dir.path().join("config.json"), config).unwrap();

        let parsed = PythonConfig::load(dir.path()).unwrap().unwrap();
        assert!(parsed.is_imported());
    }

    #[test]
    fn parse_python_config_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let result = PythonConfig::load(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn parse_python_config_default_chain() {
        let dir = tempfile::tempdir().unwrap();
        let config = r#"{"path": "/some/path.key"}"#;
        std::fs::write(dir.path().join("config.json"), config).unwrap();

        let parsed = PythonConfig::load(dir.path()).unwrap().unwrap();
        assert_eq!(
            parsed.chain_or_default(),
            aleph_types::chain::Chain::Ethereum
        );
    }

    #[test]
    fn normalize_derivation_path_adds_prefix() {
        assert_eq!(
            normalize_derivation_path("44'/60'/0'/0/0"),
            "m/44'/60'/0'/0/0"
        );
    }

    #[test]
    fn normalize_derivation_path_keeps_existing_prefix() {
        assert_eq!(
            normalize_derivation_path("m/44'/60'/0'/0/0"),
            "m/44'/60'/0'/0/0"
        );
    }

    #[test]
    fn name_from_filename_valid() {
        assert_eq!(
            name_from_filename("ethereum.key"),
            Some("ethereum".to_string())
        );
        assert_eq!(
            name_from_filename("my-wallet_01.key"),
            Some("my-wallet_01".to_string())
        );
    }

    #[test]
    fn name_from_filename_invalid() {
        assert_eq!(name_from_filename("my.wallet.key"), None); // dot is invalid
        assert_eq!(name_from_filename(".key"), None); // empty name
        assert_eq!(name_from_filename("noext"), None); // no .key suffix
    }

    #[test]
    fn available_name_no_conflict() {
        let existing = vec!["other".to_string()];
        assert_eq!(available_name("ethereum", &existing), "ethereum");
    }

    #[test]
    fn available_name_with_conflict() {
        let existing = vec!["ledger".to_string()];
        assert_eq!(available_name("ledger", &existing), "ledger-1");
    }

    #[test]
    fn available_name_multiple_conflicts() {
        let existing = vec!["ledger".to_string(), "ledger-1".to_string()];
        assert_eq!(available_name("ledger", &existing), "ledger-2");
    }

    #[test]
    fn resolve_python_home_override() {
        let path = Path::new("/custom/path");
        let result = resolve_python_home(Some(path)).unwrap();
        assert_eq!(result, Path::new("/custom/path"));
    }

    use crate::account::store::AccountStore;

    /// Create a fake Python home directory with the given config.json and key files.
    fn fake_python_home(
        dir: &tempfile::TempDir,
        config_json: Option<&str>,
        key_files: &[(&str, &[u8])],
    ) {
        let keys_dir = dir.path().join("private-keys");
        std::fs::create_dir_all(&keys_dir).unwrap();

        if let Some(config) = config_json {
            std::fs::write(dir.path().join("config.json"), config).unwrap();
        }

        for (name, content) in key_files {
            std::fs::write(keys_dir.join(name), content).unwrap();
        }
    }

    fn temp_store() -> (tempfile::TempDir, AccountStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = AccountStore::with_manifest_path(dir.path().join("accounts.toml"));
        (dir, store)
    }

    // 32-byte test key (not a real funded key)
    const TEST_KEY_BYTES: [u8; 32] = [
        0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3, 0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff,
        0x94, 0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfb, 0xba, 0x0f, 0x2d, 0x1d, 0xb7, 0x44,
        0xce, 0x06,
    ];

    #[test]
    fn migrate_dry_run_discovers_keys() {
        let python_dir = tempfile::tempdir().unwrap();
        let (_store_dir, store) = temp_store();

        let config = r#"{
            "path": "/will/not/match",
            "type": "imported",
            "chain": "ETH"
        }"#;
        fake_python_home(
            &python_dir,
            Some(config),
            &[("ethereum.key", &TEST_KEY_BYTES)],
        );

        let result = migrate_accounts(&store, python_dir.path(), true).unwrap();
        assert_eq!(result.migrated.len(), 1);
        assert_eq!(result.migrated[0].name, "ethereum");
        assert_eq!(
            result.migrated[0].chain,
            aleph_types::chain::Chain::Ethereum
        );
        assert!(result.migrated[0].address.starts_with("0x"));
    }

    #[test]
    fn migrate_dry_run_skips_mnemonic_files() {
        let python_dir = tempfile::tempdir().unwrap();
        let (_store_dir, store) = temp_store();

        fake_python_home(&python_dir, None, &[("ethereum.key", &TEST_KEY_BYTES)]);
        // Add a mnemonic file
        std::fs::write(
            python_dir.path().join("private-keys/substrate.mnemonic"),
            "word1 word2 word3",
        )
        .unwrap();

        let result = migrate_accounts(&store, python_dir.path(), true).unwrap();
        assert_eq!(result.migrated.len(), 1);
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("Substrate"));
    }

    #[test]
    fn migrate_dry_run_skips_symlinks() {
        let python_dir = tempfile::tempdir().unwrap();
        let (_store_dir, store) = temp_store();

        fake_python_home(&python_dir, None, &[("ethereum.key", &TEST_KEY_BYTES)]);
        // Create a symlink default.key -> ethereum.key
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            python_dir.path().join("private-keys/ethereum.key"),
            python_dir.path().join("private-keys/default.key"),
        )
        .unwrap();

        let result = migrate_accounts(&store, python_dir.path(), true).unwrap();
        // Only one account (ethereum), not two
        assert_eq!(result.migrated.len(), 1);
        assert_eq!(result.migrated[0].name, "ethereum");
    }

    #[test]
    fn migrate_dry_run_detects_ledger() {
        let python_dir = tempfile::tempdir().unwrap();
        let (_store_dir, store) = temp_store();

        let config = r#"{
            "type": "hardware",
            "chain": "ETH",
            "address": "0xAbCdEf1234567890abcdef1234567890AbCdEf12",
            "derivation_path": "44'/60'/0'/0/0"
        }"#;
        fake_python_home(&python_dir, Some(config), &[]);

        let result = migrate_accounts(&store, python_dir.path(), true).unwrap();
        assert_eq!(result.migrated.len(), 1);
        assert_eq!(result.migrated[0].kind, "ledger");
        assert_eq!(
            result.migrated[0].derivation_path.as_deref(),
            Some("m/44'/60'/0'/0/0")
        );
    }

    #[test]
    fn migrate_no_python_dir_errors() {
        let (_store_dir, store) = temp_store();
        let err = migrate_accounts(&store, Path::new("/nonexistent"), false).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn migrate_empty_python_dir() {
        let python_dir = tempfile::tempdir().unwrap();
        let (_store_dir, store) = temp_store();

        let result = migrate_accounts(&store, python_dir.path(), true).unwrap();
        assert!(result.migrated.is_empty());
        assert!(result.skipped.is_empty());
    }

    #[test]
    fn migrate_dry_run_skips_invalid_filenames() {
        let python_dir = tempfile::tempdir().unwrap();
        let (_store_dir, store) = temp_store();

        fake_python_home(&python_dir, None, &[("my.wallet.key", &TEST_KEY_BYTES)]);

        let result = migrate_accounts(&store, python_dir.path(), true).unwrap();
        assert!(result.migrated.is_empty());
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("invalid account name"));
    }

    #[test]
    fn migrate_sets_active_key_as_default() {
        let python_dir = tempfile::tempdir().unwrap();
        let (_store_dir, store) = temp_store();

        let key_path = python_dir.path().join("private-keys/ethereum.key");
        let path_json = serde_json::to_string(&key_path).unwrap();
        let config = format!(r#"{{"path": {path_json}, "type": "imported", "chain": "ETH"}}"#,);
        fake_python_home(
            &python_dir,
            Some(&config),
            &[("ethereum.key", &TEST_KEY_BYTES)],
        );

        let result = migrate_accounts(&store, python_dir.path(), true).unwrap();
        assert_eq!(result.migrated.len(), 1);
        assert!(result.migrated[0].is_default);
    }
}
