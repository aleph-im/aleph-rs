use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const BUILTIN_CCN_NAME: &str = "official";
pub const BUILTIN_CCN_URL: &str = "https://api.aleph.im";

/// One named CCN endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CcnEntry {
    pub name: String,
    pub url: String,
}

/// The on-disk config manifest (`config.toml`).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigManifest {
    /// Name of the default CCN (used when no --ccn or --ccn-url flag is provided).
    pub default_ccn: Option<String>,
    #[serde(default)]
    pub ccns: Vec<CcnEntry>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("ccn '{0}' already exists")]
    AlreadyExists(String),
    #[error("ccn '{0}' not found")]
    NotFound(String),
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

    /// Ensure the built-in "official" CCN entry exists in the manifest.
    fn ensure_builtin(&self) -> Result<(), ConfigError> {
        let mut manifest = self.load_manifest()?;
        if manifest.ccns.iter().any(|c| c.name == BUILTIN_CCN_NAME) {
            return Ok(());
        }
        manifest.ccns.push(CcnEntry {
            name: BUILTIN_CCN_NAME.to_string(),
            url: BUILTIN_CCN_URL.to_string(),
        });
        if manifest.default_ccn.is_none() {
            manifest.default_ccn = Some(BUILTIN_CCN_NAME.to_string());
        }
        self.save_manifest(&manifest)
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

    pub fn add_ccn(&self, name: &str, url: &str) -> Result<(), ConfigError> {
        Self::validate_name(name)?;
        Self::validate_url(url)?;

        let mut manifest = self.load_manifest()?;
        if manifest.ccns.iter().any(|c| c.name == name) {
            return Err(ConfigError::AlreadyExists(name.to_string()));
        }

        manifest.ccns.push(CcnEntry {
            name: name.to_string(),
            url: url.to_string(),
        });
        if manifest.default_ccn.is_none() {
            manifest.default_ccn = Some(name.to_string());
        }
        self.save_manifest(&manifest)
    }

    pub fn get_ccn(&self, name: &str) -> Result<CcnEntry, ConfigError> {
        let manifest = self.load_manifest()?;
        manifest
            .ccns
            .iter()
            .find(|c| c.name == name)
            .cloned()
            .ok_or_else(|| ConfigError::NotFound(name.to_string()))
    }

    pub fn default_ccn_name(&self) -> Result<Option<String>, ConfigError> {
        Ok(self.load_manifest()?.default_ccn)
    }

    pub fn set_default_ccn(&self, name: &str) -> Result<(), ConfigError> {
        let mut manifest = self.load_manifest()?;
        if !manifest.ccns.iter().any(|c| c.name == name) {
            return Err(ConfigError::NotFound(name.to_string()));
        }
        manifest.default_ccn = Some(name.to_string());
        self.save_manifest(&manifest)
    }

    pub fn remove_ccn(&self, name: &str) -> Result<(), ConfigError> {
        let mut manifest = self.load_manifest()?;
        let len_before = manifest.ccns.len();
        manifest.ccns.retain(|c| c.name != name);
        if manifest.ccns.len() == len_before {
            return Err(ConfigError::NotFound(name.to_string()));
        }
        if manifest.default_ccn.as_deref() == Some(name) {
            manifest.default_ccn = None;
        }
        self.save_manifest(&manifest)
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
    fn load_empty_manifest_returns_default() {
        let (_dir, store) = temp_store();
        let manifest = store.load_manifest().unwrap();
        assert!(manifest.default_ccn.is_none());
        assert!(manifest.ccns.is_empty());
    }

    #[test]
    fn roundtrip_manifest_serde() {
        let manifest = ConfigManifest {
            default_ccn: Some("api3".to_string()),
            ccns: vec![
                CcnEntry {
                    name: "api3".to_string(),
                    url: "https://api3.aleph.im".to_string(),
                },
                CcnEntry {
                    name: "local".to_string(),
                    url: "http://localhost:4024".to_string(),
                },
            ],
        };

        let serialized = toml::to_string_pretty(&manifest).unwrap();
        let deserialized: ConfigManifest = toml::from_str(&serialized).unwrap();

        assert_eq!(deserialized.default_ccn.as_deref(), Some("api3"));
        assert_eq!(deserialized.ccns.len(), 2);
        assert_eq!(deserialized.ccns[0].url, "https://api3.aleph.im");
        assert_eq!(deserialized.ccns[1].name, "local");
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
    fn add_and_get_ccn() {
        let (_dir, store) = temp_store();
        store.add_ccn("api3", "https://api3.aleph.im").unwrap();
        let entry = store.get_ccn("api3").unwrap();
        assert_eq!(entry.name, "api3");
        assert_eq!(entry.url, "https://api3.aleph.im");
    }

    #[test]
    fn add_ccn_sets_first_as_default() {
        let (_dir, store) = temp_store();
        store.add_ccn("api3", "https://api3.aleph.im").unwrap();
        assert_eq!(store.default_ccn_name().unwrap().as_deref(), Some("api3"));
    }

    #[test]
    fn add_ccn_does_not_override_existing_default() {
        let (_dir, store) = temp_store();
        store.add_ccn("api3", "https://api3.aleph.im").unwrap();
        store.add_ccn("local", "http://localhost:4024").unwrap();
        assert_eq!(store.default_ccn_name().unwrap().as_deref(), Some("api3"));
    }

    #[test]
    fn add_duplicate_errors() {
        let (_dir, store) = temp_store();
        store.add_ccn("api3", "https://api3.aleph.im").unwrap();
        let err = store.add_ccn("api3", "https://other.aleph.im").unwrap_err();
        assert!(matches!(err, ConfigError::AlreadyExists(_)));
    }

    #[test]
    fn add_ccn_invalid_name_errors() {
        let (_dir, store) = temp_store();
        let err = store
            .add_ccn("bad name!", "https://api3.aleph.im")
            .unwrap_err();
        assert!(matches!(err, ConfigError::InvalidName(_)));
    }

    #[test]
    fn add_ccn_invalid_url_errors() {
        let (_dir, store) = temp_store();
        let err = store.add_ccn("api3", "not a url").unwrap_err();
        assert!(matches!(err, ConfigError::InvalidUrl(_, _)));
    }

    #[test]
    fn get_nonexistent_ccn_errors() {
        let (_dir, store) = temp_store();
        let err = store.get_ccn("nope").unwrap_err();
        assert!(matches!(err, ConfigError::NotFound(_)));
    }

    #[test]
    fn set_default_ccn() {
        let (_dir, store) = temp_store();
        store.add_ccn("api3", "https://api3.aleph.im").unwrap();
        store.add_ccn("local", "http://localhost:4024").unwrap();
        store.set_default_ccn("local").unwrap();
        assert_eq!(store.default_ccn_name().unwrap().as_deref(), Some("local"));
    }

    #[test]
    fn set_default_ccn_nonexistent_errors() {
        let (_dir, store) = temp_store();
        let err = store.set_default_ccn("nope").unwrap_err();
        assert!(matches!(err, ConfigError::NotFound(_)));
    }

    #[test]
    fn remove_ccn() {
        let (_dir, store) = temp_store();
        store.add_ccn("api3", "https://api3.aleph.im").unwrap();
        store.add_ccn("local", "http://localhost:4024").unwrap();
        store.remove_ccn("local").unwrap();
        assert!(store.get_ccn("local").is_err());
        // api3 still there
        assert!(store.get_ccn("api3").is_ok());
    }

    #[test]
    fn remove_default_ccn_clears_default() {
        let (_dir, store) = temp_store();
        store.add_ccn("api3", "https://api3.aleph.im").unwrap();
        store.add_ccn("local", "http://localhost:4024").unwrap();
        // api3 is default (first added)
        store.remove_ccn("api3").unwrap();
        // default should be cleared to None, not reassigned
        assert_eq!(store.default_ccn_name().unwrap(), None);
    }

    #[test]
    fn remove_nonexistent_ccn_errors() {
        let (_dir, store) = temp_store();
        let err = store.remove_ccn("nope").unwrap_err();
        assert!(matches!(err, ConfigError::NotFound(_)));
    }

    #[test]
    fn save_and_load_manifest() {
        let (_dir, store) = temp_store();
        let manifest = ConfigManifest {
            default_ccn: Some("api3".to_string()),
            ccns: vec![CcnEntry {
                name: "api3".to_string(),
                url: "https://api3.aleph.im".to_string(),
            }],
        };
        store.save_manifest(&manifest).unwrap();
        let loaded = store.load_manifest().unwrap();
        assert_eq!(loaded.default_ccn.as_deref(), Some("api3"));
        assert_eq!(loaded.ccns.len(), 1);
    }
}
