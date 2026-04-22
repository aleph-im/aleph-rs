use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const BUILTIN_CCN_NAME: &str = "official";
pub const BUILTIN_CCN_URL: &str = "https://api.aleph.im";

/// One named CCN endpoint inside a network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CcnEntry {
    pub name: String,
    pub url: String,
}

/// A named network: CCN endpoints + (future) Ethereum settlement config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkEntry {
    pub name: String,
    pub default_ccn: Option<String>,
    #[serde(default)]
    pub ccns: Vec<CcnEntry>,
}

/// The on-disk config manifest (`config.toml`).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigManifest {
    pub default_network: Option<String>,
    #[serde(default)]
    pub networks: Vec<NetworkEntry>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("network '{0}' already exists")]
    NetworkAlreadyExists(String),
    #[error("network '{0}' not found")]
    NetworkNotFound(String),
    #[error("cannot remove network '{0}': it is the default network; use 'aleph config network use <name>' to switch first")]
    CannotRemoveDefaultNetwork(String),
    #[error("ccn '{ccn}' already exists in network '{network}'")]
    CcnAlreadyExists { network: String, ccn: String },
    #[error("ccn '{ccn}' not found in network '{network}'")]
    CcnNotFound { network: String, ccn: String },
    #[error(
        "invalid name '{0}': names must be non-empty and contain only alphanumeric characters, hyphens, and underscores"
    )]
    InvalidName(String),
    #[error("invalid URL '{0}': {1}")]
    InvalidUrl(String, String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(String),
}

pub struct ConfigStore {
    manifest_path: PathBuf,
}

impl ConfigStore {
    #[cfg(test)]
    pub fn with_manifest_path(manifest_path: PathBuf) -> Self {
        Self { manifest_path }
    }

    pub fn open() -> Result<Self, ConfigError> {
        let proj = directories::ProjectDirs::from("", "", "aleph").ok_or_else(|| {
            ConfigError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "could not determine home directory",
            ))
        })?;
        let config_dir = proj.config_dir();
        std::fs::create_dir_all(config_dir)?;
        let store = Self {
            manifest_path: config_dir.join("config.toml"),
        };
        store.ensure_builtin()?;
        Ok(store)
    }

    pub fn load_manifest(&self) -> Result<ConfigManifest, ConfigError> {
        match std::fs::read_to_string(&self.manifest_path) {
            Ok(contents) => {
                toml::from_str(&contents).map_err(|e| ConfigError::Parse(e.to_string()))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ConfigManifest::default()),
            Err(e) => Err(ConfigError::Io(e)),
        }
    }

    fn save_manifest(&self, manifest: &ConfigManifest) -> Result<(), ConfigError> {
        let content =
            toml::to_string_pretty(manifest).map_err(|e| ConfigError::Parse(e.to_string()))?;
        if let Some(parent) = self.manifest_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.manifest_path, content)?;
        Ok(())
    }

    fn validate_name(name: &str) -> Result<(), ConfigError> {
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(ConfigError::InvalidName(name.to_string()));
        }
        Ok(())
    }

    fn validate_url(raw: &str) -> Result<(), ConfigError> {
        let parsed = url::Url::parse(raw)
            .map_err(|e| ConfigError::InvalidUrl(raw.to_string(), e.to_string()))?;
        match parsed.scheme() {
            "http" | "https" => Ok(()),
            other => Err(ConfigError::InvalidUrl(
                raw.to_string(),
                format!("scheme must be http or https, got '{other}'"),
            )),
        }
    }

    // Network CRUD — implemented in Task 2
    pub fn add_network(&self, name: &str) -> Result<(), ConfigError> {
        Self::validate_name(name)?;
        let mut manifest = self.load_manifest()?;
        if manifest.networks.iter().any(|n| n.name == name) {
            return Err(ConfigError::NetworkAlreadyExists(name.to_string()));
        }
        manifest.networks.push(NetworkEntry {
            name: name.to_string(),
            default_ccn: None,
            ccns: Vec::new(),
        });
        if manifest.default_network.is_none() {
            manifest.default_network = Some(name.to_string());
        }
        self.save_manifest(&manifest)
    }

    pub fn get_network(&self, name: &str) -> Result<NetworkEntry, ConfigError> {
        self.load_manifest()?
            .networks
            .into_iter()
            .find(|n| n.name == name)
            .ok_or_else(|| ConfigError::NetworkNotFound(name.to_string()))
    }

    pub fn remove_network(&self, name: &str) -> Result<(), ConfigError> {
        let mut manifest = self.load_manifest()?;
        if !manifest.networks.iter().any(|n| n.name == name) {
            return Err(ConfigError::NetworkNotFound(name.to_string()));
        }
        if manifest.default_network.as_deref() == Some(name) {
            return Err(ConfigError::CannotRemoveDefaultNetwork(name.to_string()));
        }
        manifest.networks.retain(|n| n.name != name);
        self.save_manifest(&manifest)
    }

    pub fn set_default_network(&self, name: &str) -> Result<(), ConfigError> {
        let mut manifest = self.load_manifest()?;
        if !manifest.networks.iter().any(|n| n.name == name) {
            return Err(ConfigError::NetworkNotFound(name.to_string()));
        }
        manifest.default_network = Some(name.to_string());
        self.save_manifest(&manifest)
    }

    pub fn default_network_name(&self) -> Result<Option<String>, ConfigError> {
        Ok(self.load_manifest()?.default_network)
    }

    pub fn list_networks(&self) -> Result<Vec<NetworkEntry>, ConfigError> {
        Ok(self.load_manifest()?.networks)
    }

    // CCN CRUD scoped to a network — implemented in Task 3
    pub fn add_ccn(&self, _network: &str, _name: &str, _url: &str) -> Result<(), ConfigError> {
        unimplemented!()
    }
    pub fn get_ccn(&self, _network: &str, _name: &str) -> Result<CcnEntry, ConfigError> {
        unimplemented!()
    }
    pub fn remove_ccn(&self, _network: &str, _name: &str) -> Result<(), ConfigError> {
        unimplemented!()
    }
    pub fn set_default_ccn(&self, _network: &str, _ccn: &str) -> Result<(), ConfigError> {
        unimplemented!()
    }
    pub fn list_ccns(&self, _network: &str) -> Result<Vec<CcnEntry>, ConfigError> {
        unimplemented!()
    }
    pub fn list_all_ccns(&self) -> Result<Vec<(String, CcnEntry)>, ConfigError> {
        unimplemented!()
    }

    // Built-in seeding — implemented in Task 4
    fn ensure_builtin(&self) -> Result<(), ConfigError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, ConfigStore) {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("config.toml");
        let store = ConfigStore::with_manifest_path(manifest_path);
        (dir, store)
    }

    #[test]
    fn roundtrip_manifest_serde() {
        let manifest = ConfigManifest {
            default_network: Some("mainnet".to_string()),
            networks: vec![NetworkEntry {
                name: "mainnet".to_string(),
                default_ccn: Some("official".to_string()),
                ccns: vec![CcnEntry {
                    name: "official".to_string(),
                    url: "https://api.aleph.im".to_string(),
                }],
            }],
        };
        let serialized = toml::to_string_pretty(&manifest).unwrap();
        let deserialized: ConfigManifest = toml::from_str(&serialized).unwrap();
        assert_eq!(deserialized.default_network.as_deref(), Some("mainnet"));
        assert_eq!(deserialized.networks.len(), 1);
        assert_eq!(deserialized.networks[0].name, "mainnet");
        assert_eq!(deserialized.networks[0].ccns[0].url, "https://api.aleph.im");
    }

    #[test]
    fn load_empty_manifest_returns_default() {
        let (_dir, store) = temp_store();
        let manifest = store.load_manifest().unwrap();
        assert!(manifest.default_network.is_none());
        assert!(manifest.networks.is_empty());
    }

    #[test]
    fn validate_name_rejects_empty() {
        assert!(ConfigStore::validate_name("").is_err());
    }

    #[test]
    fn validate_name_rejects_spaces() {
        assert!(ConfigStore::validate_name("my node").is_err());
    }

    #[test]
    fn validate_name_rejects_special_chars() {
        assert!(ConfigStore::validate_name("my@node").is_err());
    }

    #[test]
    fn validate_name_accepts_valid() {
        assert!(ConfigStore::validate_name("my-node_01").is_ok());
    }

    #[test]
    fn validate_url_rejects_ftp() {
        assert!(ConfigStore::validate_url("ftp://example.com").is_err());
    }

    #[test]
    fn validate_url_rejects_garbage() {
        assert!(ConfigStore::validate_url("not a url").is_err());
    }

    #[test]
    fn validate_url_accepts_https() {
        assert!(ConfigStore::validate_url("https://api3.aleph.im").is_ok());
    }

    #[test]
    fn validate_url_accepts_http() {
        assert!(ConfigStore::validate_url("http://localhost:4024").is_ok());
    }

    #[test]
    fn add_network_basic() {
        let (_dir, store) = temp_store();
        store.add_network("testnet").unwrap();
        let nets = store.list_networks().unwrap();
        assert_eq!(nets.len(), 1);
        assert_eq!(nets[0].name, "testnet");
        assert!(nets[0].ccns.is_empty());
        assert!(nets[0].default_ccn.is_none());
    }

    #[test]
    fn first_network_becomes_default() {
        let (_dir, store) = temp_store();
        store.add_network("testnet").unwrap();
        assert_eq!(store.default_network_name().unwrap().as_deref(), Some("testnet"));
    }

    #[test]
    fn second_network_does_not_override_default() {
        let (_dir, store) = temp_store();
        store.add_network("mainnet").unwrap();
        store.add_network("testnet").unwrap();
        assert_eq!(store.default_network_name().unwrap().as_deref(), Some("mainnet"));
    }

    #[test]
    fn add_duplicate_network_errors() {
        let (_dir, store) = temp_store();
        store.add_network("mainnet").unwrap();
        let err = store.add_network("mainnet").unwrap_err();
        assert!(matches!(err, ConfigError::NetworkAlreadyExists(_)));
    }

    #[test]
    fn add_network_invalid_name_errors() {
        let (_dir, store) = temp_store();
        let err = store.add_network("bad name!").unwrap_err();
        assert!(matches!(err, ConfigError::InvalidName(_)));
    }

    #[test]
    fn get_nonexistent_network_errors() {
        let (_dir, store) = temp_store();
        let err = store.get_network("nope").unwrap_err();
        assert!(matches!(err, ConfigError::NetworkNotFound(_)));
    }

    #[test]
    fn set_default_network() {
        let (_dir, store) = temp_store();
        store.add_network("mainnet").unwrap();
        store.add_network("testnet").unwrap();
        store.set_default_network("testnet").unwrap();
        assert_eq!(store.default_network_name().unwrap().as_deref(), Some("testnet"));
    }

    #[test]
    fn set_default_network_unknown_errors() {
        let (_dir, store) = temp_store();
        let err = store.set_default_network("nope").unwrap_err();
        assert!(matches!(err, ConfigError::NetworkNotFound(_)));
    }

    #[test]
    fn remove_default_network_refused() {
        let (_dir, store) = temp_store();
        store.add_network("mainnet").unwrap();
        let err = store.remove_network("mainnet").unwrap_err();
        assert!(matches!(err, ConfigError::CannotRemoveDefaultNetwork(_)));
    }

    #[test]
    fn remove_non_default_network() {
        let (_dir, store) = temp_store();
        store.add_network("mainnet").unwrap();
        store.add_network("testnet").unwrap();
        store.remove_network("testnet").unwrap();
        let nets = store.list_networks().unwrap();
        assert_eq!(nets.len(), 1);
        assert_eq!(nets[0].name, "mainnet");
    }

    #[test]
    fn remove_unknown_network_errors() {
        let (_dir, store) = temp_store();
        let err = store.remove_network("nope").unwrap_err();
        assert!(matches!(err, ConfigError::NetworkNotFound(_)));
    }
}
