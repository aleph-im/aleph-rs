//! SSH public keys stored on the Aleph network.
//!
//! Keys are persisted as `POST` messages of type [`SSH_POST_TYPE`] on channel
//! [`SSH_CHANNEL`], byte-compatible with the web console. Each post's content is
//! a [`SshKeyContent`]. This module is the single, standardized place that knows
//! that representation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use aleph_types::item_hash::ItemHash;

/// Post type used for SSH key records (shared with the web console).
pub const SSH_POST_TYPE: &str = "ALEPH-SSH";
/// Channel used for SSH key records (shared with the web console).
pub const SSH_CHANNEL: &str = "ALEPH-CLOUDSOLUTIONS";

/// The on-network content of an SSH key post.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshKeyContent {
    /// The SSH public key string.
    pub key: String,
    /// Optional user-facing label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// A registered SSH key parsed from a network post.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SshKey {
    /// Hash of the post that registered this key (used for removal).
    pub item_hash: ItemHash,
    /// The SSH public key string.
    pub key: String,
    /// Optional user-facing label.
    pub label: Option<String>,
    /// When the key was registered.
    pub created: DateTime<Utc>,
}

/// Accepted SSH public key prefixes.
const SSH_PUBKEY_PREFIXES: &[&str] = &[
    "ssh-rsa",
    "ssh-ed25519",
    "ssh-dss",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
    "sk-ssh-ed25519@openssh.com",
    "sk-ecdsa-sha2-nistp256@openssh.com",
];

/// Validate that `key` looks like an SSH public key (not a private key/garbage).
pub fn validate_pubkey(key: &str) -> Result<(), String> {
    if SSH_PUBKEY_PREFIXES.iter().any(|p| key.starts_with(p)) {
        return Ok(());
    }
    Err(
        "does not look like an SSH public key (expected a line starting with \
         ssh-rsa, ssh-ed25519, etc.)"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_pubkey_accepts_valid_keys() {
        assert!(validate_pubkey("ssh-ed25519 AAAAC3NzaC1 user@host").is_ok());
        assert!(validate_pubkey("ssh-rsa AAAAB3Nza").is_ok());
        assert!(validate_pubkey("ecdsa-sha2-nistp256 AAAA").is_ok());
        assert!(validate_pubkey("sk-ssh-ed25519@openssh.com AAAA").is_ok());
    }

    #[test]
    fn validate_pubkey_rejects_private_key() {
        assert!(validate_pubkey("-----BEGIN OPENSSH PRIVATE KEY-----").is_err());
    }

    #[test]
    fn validate_pubkey_rejects_garbage() {
        assert!(validate_pubkey("not a key").is_err());
    }

    #[test]
    fn content_serializes_with_key_and_label() {
        let c = SshKeyContent { key: "ssh-ed25519 AAAA".into(), label: Some("laptop".into()) };
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["key"], "ssh-ed25519 AAAA");
        assert_eq!(v["label"], "laptop");
    }

    #[test]
    fn content_omits_absent_label() {
        let c = SshKeyContent { key: "ssh-ed25519 AAAA".into(), label: None };
        let v = serde_json::to_value(&c).unwrap();
        assert!(v.get("label").is_none());
    }

    #[test]
    fn content_deserializes_without_label() {
        let c: SshKeyContent = serde_json::from_value(serde_json::json!({"key": "ssh-rsa X"})).unwrap();
        assert_eq!(c.key, "ssh-rsa X");
        assert_eq!(c.label, None);
    }
}
