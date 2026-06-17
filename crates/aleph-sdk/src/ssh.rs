//! SSH public keys stored on the Aleph network.
//!
//! Keys are persisted as `POST` messages of type [`SSH_POST_TYPE`] on channel
//! [`SSH_CHANNEL`], byte-compatible with the web console. Each post's content is
//! a [`SshKeyContent`]. This module is the single, standardized place that knows
//! that representation.

use std::future::Future;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use aleph_types::account::{Account, SignError};
use aleph_types::chain::Address;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::MessageType;
use aleph_types::message::pending::PendingMessage;

use crate::builder::MessageBuilder;
use crate::client::{AlephClient, AlephPostClient, MessageError, PaginationParams, PostFilter};

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

/// Read SSH keys registered on the Aleph network.
pub trait AlephSshClient {
    /// List all SSH keys registered by `address`, newest first.
    ///
    /// Posts whose content does not parse as [`SshKeyContent`] are skipped.
    fn list_ssh_keys(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<Vec<SshKey>, MessageError>> + Send;
}

impl AlephSshClient for AlephClient {
    async fn list_ssh_keys(&self, address: &Address) -> Result<Vec<SshKey>, MessageError> {
        let filter = PostFilter {
            addresses: Some(vec![address.clone()]),
            post_types: Some(vec![SSH_POST_TYPE.to_string()]),
            channels: Some(vec![SSH_CHANNEL.to_string()]),
            ..Default::default()
        };
        let pagination = PaginationParams {
            pagination: Some(200),
            page: Some(1),
        };
        let response = self.get_posts_v1(&filter, pagination).await?;

        let mut keys: Vec<SshKey> = response
            .posts
            .into_iter()
            .filter_map(|post| {
                let content: SshKeyContent = serde_json::from_value(post.content).ok()?;
                Some(SshKey {
                    item_hash: post.item_hash,
                    key: content.key,
                    label: content.label,
                    created: post.created,
                })
            })
            .collect();
        keys.sort_by(|a, b| b.created.cmp(&a.created));
        Ok(keys)
    }
}

/// Build the POST envelope that registers an SSH key.
fn add_ssh_key_envelope(key: &str, label: &str) -> serde_json::Value {
    serde_json::json!({
        "type": SSH_POST_TYPE,
        "content": { "key": key, "label": label },
    })
}

/// Build the FORGET envelope that removes an SSH key post by hash.
fn forget_ssh_key_envelope(item_hash: &ItemHash) -> serde_json::Value {
    serde_json::json!({ "hashes": [item_hash.to_string()] })
}

/// Build a signed message registering `key` under `label` on the SSH channel.
pub fn build_add_ssh_key<A: Account>(
    account: &A,
    key: &str,
    label: &str,
) -> Result<PendingMessage, SignError> {
    MessageBuilder::new(account, MessageType::Post, add_ssh_key_envelope(key, label))
        .channel(Channel::from(SSH_CHANNEL.to_string()))
        .build()
}

/// Build a signed FORGET message removing the SSH key post `item_hash`.
pub fn build_forget_ssh_key<A: Account>(
    account: &A,
    item_hash: &ItemHash,
) -> Result<PendingMessage, SignError> {
    MessageBuilder::new(
        account,
        MessageType::Forget,
        forget_ssh_key_envelope(item_hash),
    )
    .channel(Channel::from(SSH_CHANNEL.to_string()))
    .build()
}

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

    #[tokio::test]
    async fn list_ssh_keys_parses_and_sorts() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let body = serde_json::json!({
            "posts": [
                {
                    "item_hash": "1111111111111111111111111111111111111111111111111111111111111111",
                    "content": {"key": "ssh-ed25519 AAAA", "label": "older"},
                    "original_item_hash": "1111111111111111111111111111111111111111111111111111111111111111",
                    "address": "0x0000000000000000000000000000000000000001",
                    "created": "2024-01-01T00:00:00Z",
                    "last_updated": "2024-01-01T00:00:00Z"
                },
                {
                    "item_hash": "2222222222222222222222222222222222222222222222222222222222222222",
                    "content": {"key": "ssh-rsa BBBB", "label": "newer"},
                    "original_item_hash": "2222222222222222222222222222222222222222222222222222222222222222",
                    "address": "0x0000000000000000000000000000000000000001",
                    "created": "2024-06-01T00:00:00Z",
                    "last_updated": "2024-06-01T00:00:00Z"
                },
                {
                    "item_hash": "3333333333333333333333333333333333333333333333333333333333333333",
                    "content": {"not": "an ssh key"},
                    "original_item_hash": "3333333333333333333333333333333333333333333333333333333333333333",
                    "address": "0x0000000000000000000000000000000000000001",
                    "created": "2024-03-01T00:00:00Z",
                    "last_updated": "2024-03-01T00:00:00Z"
                }
            ],
            "pagination_per_page": 200,
            "pagination_page": 1,
            "pagination_total": 3
        });
        Mock::given(method("GET"))
            .and(path("/api/v1/posts.json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let client = AlephClient::new(server.uri().parse().unwrap());
        let addr = Address::from("0x0000000000000000000000000000000000000001".to_string());
        let keys = client.list_ssh_keys(&addr).await.unwrap();

        assert_eq!(keys.len(), 2); // the non-SSH post is skipped
        assert_eq!(keys[0].label.as_deref(), Some("newer")); // newest first
        assert_eq!(keys[1].label.as_deref(), Some("older"));
    }

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
        let c = SshKeyContent {
            key: "ssh-ed25519 AAAA".into(),
            label: Some("laptop".into()),
        };
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(v["key"], "ssh-ed25519 AAAA");
        assert_eq!(v["label"], "laptop");
    }

    #[test]
    fn content_omits_absent_label() {
        let c = SshKeyContent {
            key: "ssh-ed25519 AAAA".into(),
            label: None,
        };
        let v = serde_json::to_value(&c).unwrap();
        assert!(v.get("label").is_none());
    }

    #[test]
    fn content_deserializes_without_label() {
        let c: SshKeyContent =
            serde_json::from_value(serde_json::json!({"key": "ssh-rsa X"})).unwrap();
        assert_eq!(c.key, "ssh-rsa X");
        assert_eq!(c.label, None);
    }

    #[test]
    fn add_envelope_has_type_and_content() {
        let v = add_ssh_key_envelope("ssh-ed25519 AAAA", "laptop");
        assert_eq!(v["type"], "ALEPH-SSH");
        assert_eq!(v["content"]["key"], "ssh-ed25519 AAAA");
        assert_eq!(v["content"]["label"], "laptop");
    }

    #[test]
    fn forget_envelope_lists_hash() {
        let hash: ItemHash = "1111111111111111111111111111111111111111111111111111111111111111"
            .parse()
            .unwrap();
        let v = forget_ssh_key_envelope(&hash);
        assert_eq!(
            v["hashes"][0],
            "1111111111111111111111111111111111111111111111111111111111111111"
        );
    }
}
