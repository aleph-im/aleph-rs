//! Storage orchestration. Mirrors `aleph/storage.py`.
//!
//! `StorageService` is the top-level facade that ties the local storage
//! engine, the IPFS gateway, and the process-local cache. It exposes the
//! same five operations as the Python class:
//!
//! - [`StorageService::get_message_content`] — return the JSON content of a
//!   pending or persisted message, regardless of where it lives (inline,
//!   storage, IPFS).
//! - [`StorageService::get_hash_content`] / [`get_hash_content_iterator`] —
//!   fetch raw bytes addressed by a content hash, trying the DB first, then
//!   the P2P API servers, then IPFS.
//! - [`StorageService::get_json`] — `get_hash_content` + JSON decode.
//! - [`StorageService::pin_hash`] — pin a CID on the IPFS daemon.
//! - [`StorageService::add_json`] / [`add_file`] — write a payload to local
//!   storage + IPFS, returning the hash.
//!
//! Two implementation notes:
//! - The Python module exposes a `check_for_u0000` helper that raises
//!   `InvalidContent` when the payload contains the NUL escape sequence.
//!   We replicate it here as [`check_for_u0000`].
//! - SHA-256 hashing uses the `sha2` crate; CIDv0/v1 detection delegates to
//!   [`crate::services::ipfs::common::get_cid_version`].

use std::sync::Arc;
use std::time::Duration;

use aleph_types::message::item_type::ItemType;
use bytes::Bytes;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;

use crate::AlephError;
use crate::AlephResult;
use crate::db::accessors::files::upsert_file;
use crate::db::models::pending_messages::PendingMessageDb;
use crate::services::ipfs::IpfsService;
use crate::services::ipfs::common::get_cid_version;
use crate::services::p2p::http::request_hash as p2p_request_hash;
use crate::services::p2p::jobs::ApiServerLookup;
use crate::services::storage::engine::StorageEngine;
use crate::types::files::FileType;

/// The escape sequence `\u0000` rejected by Postgres `jsonb` columns. Mirrors
/// the Python `U0000_STR` constant.
pub const U0000_STR: &str = "\\u0000";

/// Source from which a [`MessageContent`] / [`RawContent`] was obtained.
/// Mirrors Python `aleph.schemas.message_content.ContentSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentSource {
    Db,
    P2p,
    Ipfs,
    Inline,
}

impl ContentSource {
    pub fn as_str(self) -> &'static str {
        match self {
            ContentSource::Db => "DB",
            ContentSource::P2p => "P2P",
            ContentSource::Ipfs => "IPFS",
            ContentSource::Inline => "inline",
        }
    }
}

/// Raw bytes addressed by `hash`, plus where they came from.
#[derive(Debug, Clone)]
pub struct RawContent {
    pub hash: String,
    pub value: Bytes,
    pub source: ContentSource,
}

/// Decoded JSON content (`value`) carried alongside the original bytes
/// (`raw_value`).
#[derive(Debug, Clone)]
pub struct MessageContent {
    pub hash: String,
    pub source: ContentSource,
    pub value: Value,
    pub raw_value: Bytes,
}

/// Reject payloads carrying a literal `\u0000` escape — Postgres `jsonb`
/// refuses to store them. Mirrors `check_for_u0000`.
pub fn check_for_u0000(item_content: &[u8]) -> AlephResult<()> {
    if windowed_contains(item_content, U0000_STR.as_bytes()) {
        Err(AlephError::InvalidMessage(
            "Unsupported character in message: \\u0000".into(),
        ))
    } else {
        Ok(())
    }
}

fn windowed_contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Minimal slice of `aleph.schemas.base_messages.AlephBaseMessage` /
/// `PendingMessageDb` that [`StorageService::get_message_content`] needs.
/// Decouples the function from the concrete message struct so we can call it
/// with either a wire-message dict or a `PendingMessageDb` row.
pub trait MessageContentView {
    fn item_hash(&self) -> &str;
    fn item_type(&self) -> ItemType;
    fn item_content(&self) -> Option<&str>;
}

impl MessageContentView for PendingMessageDb {
    fn item_hash(&self) -> &str {
        &self.item_hash
    }
    fn item_type(&self) -> ItemType {
        self.item_type
    }
    fn item_content(&self) -> Option<&str> {
        self.item_content.as_deref()
    }
}

/// Storage facade. Mirrors `aleph.storage.StorageService`.
#[derive(Clone)]
pub struct StorageService {
    pub storage_engine: Arc<dyn StorageEngine>,
    pub ipfs_service: Arc<IpfsService>,
    /// Read-only view over the shared API-server set. Mirrors
    /// `NodeCache.get_api_servers()` (Redis-backed in production, in-memory
    /// during tests).
    pub node_cache: Arc<dyn ApiServerLookup>,
    /// Whether IPFS-backed operations are enabled. Mirrors
    /// `config.ipfs.enabled.value`. Defaults to `true`.
    pub ipfs_enabled: bool,
    /// Whether the HTTP P2P client is enabled (`"http" in config.p2p.clients`).
    /// Defaults to `true` to match pyaleph's seeded default.
    pub http_p2p_enabled: bool,
}

impl StorageService {
    pub fn new(
        storage_engine: Arc<dyn StorageEngine>,
        ipfs_service: Arc<IpfsService>,
        node_cache: Arc<dyn ApiServerLookup>,
    ) -> Self {
        Self {
            storage_engine,
            ipfs_service,
            node_cache,
            ipfs_enabled: true,
            http_p2p_enabled: true,
        }
    }

    /// Toggle the IPFS-enabled flag (used in tests + when wiring up against
    /// the global config). Returns `self` for chaining.
    pub fn with_ipfs_enabled(mut self, enabled: bool) -> Self {
        self.ipfs_enabled = enabled;
        self
    }

    /// Toggle the HTTP-P2P-enabled flag (mirrors `"http" in
    /// config.p2p.clients.value`). Returns `self` for chaining.
    pub fn with_http_p2p_enabled(mut self, enabled: bool) -> Self {
        self.http_p2p_enabled = enabled;
        self
    }

    /// Fetch + JSON-decode the content referenced by `message`. Mirrors
    /// `StorageService.get_message_content`.
    pub async fn get_message_content<M: MessageContentView + ?Sized>(
        &self,
        message: &M,
    ) -> AlephResult<MessageContent> {
        let item_hash = message.item_hash().to_string();
        let item_type = message.item_type();

        let (raw_bytes, source) = match item_type {
            ItemType::Inline => {
                let body = message.item_content().ok_or_else(|| {
                    AlephError::InvalidMessage("Inline message missing item_content".into())
                })?;
                (
                    Bytes::copy_from_slice(body.as_bytes()),
                    ContentSource::Inline,
                )
            }
            ItemType::Storage | ItemType::Ipfs => {
                let raw = self.get_hash_content_default(&item_hash, item_type).await?;
                (raw.value, raw.source)
            }
        };

        check_for_u0000(&raw_bytes)?;

        let value: Value = serde_json::from_slice(&raw_bytes)
            .map_err(|e| AlephError::InvalidMessage(format!("Can't decode JSON: {e}")))?;

        Ok(MessageContent {
            hash: item_hash,
            source,
            value,
            raw_value: raw_bytes,
        })
    }

    async fn get_hash_content_default(
        &self,
        content_hash: &str,
        engine: ItemType,
    ) -> AlephResult<RawContent> {
        self.get_hash_content(
            content_hash,
            engine,
            Duration::from_secs(2),
            1,
            true,
            true,
            true,
        )
        .await
    }

    /// Fetch raw bytes for `content_hash`, trying DB → P2P API servers → IPFS.
    /// Mirrors `StorageService.get_hash_content`.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_hash_content(
        &self,
        content_hash: &str,
        engine: ItemType,
        timeout: Duration,
        tries: u32,
        use_network: bool,
        use_ipfs: bool,
        store_value: bool,
    ) -> AlephResult<RawContent> {
        let mut source: Option<ContentSource> = None;
        let mut content: Option<Bytes> = self.storage_engine.read(content_hash).await?;
        if content.is_some() {
            source = Some(ContentSource::Db);
        }

        if content.is_none() && use_network {
            if let Some(fetched) = self
                .fetch_content_from_network(content_hash, engine, timeout)
                .await?
            {
                content = Some(fetched);
                source = Some(ContentSource::P2p);
            }
        }

        if content.is_none() && use_ipfs && engine == ItemType::Ipfs && self.ipfs_enabled {
            if let Some(fetched) = self
                .ipfs_service
                .get_ipfs_content(content_hash, timeout, tries)
                .await?
            {
                content = Some(fetched);
                source = Some(ContentSource::Ipfs);
            }
        }

        let (content, source) = match (content, source) {
            (Some(c), Some(s)) => (c, s),
            _ => {
                return Err(AlephError::NotFound(format!(
                    "Could not fetch content for '{content_hash}'."
                )));
            }
        };

        tracing::info!("Got content from {} for '{content_hash}'.", source.as_str());

        if store_value && source != ContentSource::Db {
            tracing::debug!("Storing content for '{content_hash}'.");
            self.storage_engine.write(content_hash, &content).await?;
        }

        Ok(RawContent {
            hash: content_hash.to_string(),
            value: content,
            source,
        })
    }

    /// Streaming variant that returns whatever the storage engine can yield
    /// without buffering. Falls back to [`get_hash_content`] when streaming
    /// isn't available, matching Python's `get_hash_content_iterator`.
    pub async fn get_hash_content_iterator(
        &self,
        content_hash: &str,
        engine: ItemType,
        timeout: Duration,
        tries: u32,
    ) -> AlephResult<RawContent> {
        // Best effort: keep the same fallback shape but materialize the bytes
        // since the consumer only really cares about reproducing the
        // database/IPFS-cached value. The iterator-specific path on Python is
        // only exercised by the `GET /storage/raw/` controller which is
        // separately ported in `web::controllers::storage`.
        self.get_hash_content(content_hash, engine, timeout, tries, true, true, true)
            .await
    }

    async fn fetch_content_from_network(
        &self,
        content_hash: &str,
        engine: ItemType,
        timeout: Duration,
    ) -> AlephResult<Option<Bytes>> {
        // Mirrors pyaleph: only attempt the HTTP P2P branch when
        // `"http" in config.p2p.clients.value`.
        if !self.http_p2p_enabled {
            return Ok(None);
        }
        let api_servers = self.api_servers_from_cache().await?;
        if api_servers.is_empty() {
            return Ok(None);
        }
        let Some(content) = p2p_request_hash(&api_servers, content_hash, timeout).await else {
            return Ok(None);
        };
        self.verify_content_hash(&content, engine, content_hash)
            .await?;
        Ok(Some(content))
    }

    /// Returns the list of currently-known peer API servers from the shared
    /// node cache. Mirrors `NodeCache.get_api_servers()` (Redis `SMEMBERS
    /// api_servers`).
    async fn api_servers_from_cache(&self) -> AlephResult<Vec<String>> {
        self.node_cache.get_api_servers().await
    }

    /// Verify that the content matches the expected hash. Mirrors
    /// `_verify_content_hash`.
    async fn verify_content_hash(
        &self,
        content: &[u8],
        engine: ItemType,
        expected_hash: &str,
    ) -> AlephResult<()> {
        let computed = match engine {
            ItemType::Ipfs if self.ipfs_enabled => {
                let cid_version = get_cid_version(expected_hash)?;
                self.compute_content_hash_ipfs(content, cid_version).await?
            }
            ItemType::Storage => Some(verify_content_hash_sha256(content)),
            _ => {
                return Err(AlephError::InvalidMessage(format!(
                    "Invalid storage engine: '{engine:?}'."
                )));
            }
        };

        let Some(computed) = computed else {
            return Err(AlephError::NotFound(format!(
                "Could not compute hash for '{expected_hash}'."
            )));
        };
        if computed != expected_hash {
            return Err(AlephError::InvalidMessage(format!(
                "Got a bad hash! Expected '{expected_hash}' but computed '{computed}'."
            )));
        }
        Ok(())
    }

    /// Compute the IPFS hash of `content` by uploading it through the daemon.
    /// Mirrors `_compute_content_hash_ipfs`.
    pub async fn compute_content_hash_ipfs(
        &self,
        content: &[u8],
        cid_version: u8,
    ) -> AlephResult<Option<String>> {
        match self
            .ipfs_service
            .add_bytes(Bytes::copy_from_slice(content), cid_version)
            .await
        {
            Ok(h) => Ok(Some(h)),
            Err(AlephError::Ipfs(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// `get_hash_content` + JSON-decode.
    pub async fn get_json(
        &self,
        content_hash: &str,
        engine: ItemType,
        timeout: Duration,
        tries: u32,
    ) -> AlephResult<MessageContent> {
        let raw = self
            .get_hash_content(content_hash, engine, timeout, tries, true, true, true)
            .await?;
        let value: Value = serde_json::from_slice(&raw.value)
            .map_err(|e| AlephError::InvalidMessage(format!("Cannot decode JSON: {e}")))?;
        Ok(MessageContent {
            hash: raw.hash,
            source: raw.source,
            value,
            raw_value: raw.value,
        })
    }

    /// Pin a CID on the IPFS daemon. Mirrors `pin_hash`.
    pub async fn pin_hash(&self, cid: &str, timeout: Duration, tries: u32) -> AlephResult<()> {
        self.ipfs_service.pin_add(cid, timeout, tries).await
    }

    /// Write a JSON payload to local storage + IPFS (or storage-only). Mirrors
    /// `add_json`.
    pub async fn add_json(
        &self,
        client: &impl GenericClient,
        value: &Value,
        engine: ItemType,
    ) -> AlephResult<String> {
        let bytes = serde_json::to_vec(value)?;
        let hash = match engine {
            ItemType::Ipfs => {
                self.ipfs_service
                    .add_bytes(Bytes::copy_from_slice(&bytes), 0)
                    .await?
            }
            ItemType::Storage => verify_content_hash_sha256(&bytes),
            ItemType::Inline => {
                return Err(AlephError::InvalidMessage(
                    "storage engine inline not supported".into(),
                ));
            }
        };
        self.storage_engine.write(&hash, &bytes).await?;
        upsert_file(client, &hash, bytes.len() as i64, FileType::File).await?;
        Ok(hash)
    }

    /// Write file content to local storage + IPFS (or storage-only). Mirrors
    /// `add_file`.
    pub async fn add_file(
        &self,
        client: &impl GenericClient,
        file_content: &[u8],
        engine: ItemType,
    ) -> AlephResult<String> {
        let hash = match engine {
            ItemType::Ipfs => {
                self.ipfs_service
                    .add_bytes(Bytes::copy_from_slice(file_content), 0)
                    .await?
            }
            ItemType::Storage => verify_content_hash_sha256(file_content),
            ItemType::Inline => {
                return Err(AlephError::InvalidMessage(format!(
                    "Unsupported item type: {engine:?}"
                )));
            }
        };
        self.storage_engine.write(&hash, file_content).await?;
        upsert_file(client, &hash, file_content.len() as i64, FileType::File).await?;
        Ok(hash)
    }
}

/// SHA-256 hex digest of `content`. Mirrors `aleph.utils.get_sha256`.
pub fn verify_content_hash_sha256(content: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::storage::in_memory::InMemoryStorageEngine;
    use serde_json::json;

    /// Build a `StorageService` wired to in-memory storage + an IPFS service
    /// pointed at a dummy URL. None of these tests touch IPFS — they only
    /// exercise the inline / storage paths.
    fn storage_service(engine: Arc<dyn StorageEngine>) -> StorageService {
        let ipfs = Arc::new(crate::services::ipfs::IpfsService::from_parts(
            reqwest::Client::new(),
            None,
            crate::services::ipfs::common::IpfsEndpoint {
                scheme: "http".into(),
                host: "127.0.0.1".into(),
                port: 1,
                timeout: Duration::from_millis(1),
            },
            crate::services::ipfs::common::IpfsEndpoint {
                scheme: "http".into(),
                host: "127.0.0.1".into(),
                port: 1,
                timeout: Duration::from_millis(1),
            },
        ));
        let cache: Arc<dyn ApiServerLookup> = Arc::new(EmptyApiServerLookup);
        StorageService::new(engine, ipfs, cache).with_ipfs_enabled(false)
    }

    struct EmptyApiServerLookup;

    #[async_trait::async_trait]
    impl ApiServerLookup for EmptyApiServerLookup {
        async fn get_api_servers(&self) -> AlephResult<Vec<String>> {
            Ok(Vec::new())
        }
    }

    struct TestMsg {
        item_hash: String,
        item_type: ItemType,
        item_content: Option<String>,
    }

    impl MessageContentView for TestMsg {
        fn item_hash(&self) -> &str {
            &self.item_hash
        }
        fn item_type(&self) -> ItemType {
            self.item_type
        }
        fn item_content(&self) -> Option<&str> {
            self.item_content.as_deref()
        }
    }

    #[test]
    fn check_for_u0000_rejects() {
        assert!(check_for_u0000(b"normal text").is_ok());
        assert!(check_for_u0000(b"oops \\u0000 sneaky").is_err());
    }

    #[test]
    fn sha256_hex_matches_python() {
        // sha256("hello").hexdigest() in Python.
        assert_eq!(
            verify_content_hash_sha256(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[tokio::test]
    async fn get_message_content_inline() {
        let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::new());
        let svc = storage_service(engine);
        let msg = TestMsg {
            item_hash: "hash-inline".into(),
            item_type: ItemType::Inline,
            item_content: Some(r#"{"hello": "world"}"#.into()),
        };
        let content = svc.get_message_content(&msg).await.unwrap();
        assert_eq!(content.source, ContentSource::Inline);
        assert_eq!(content.value, json!({"hello": "world"}));
    }

    #[tokio::test]
    async fn get_message_content_storage_reads_from_engine() {
        let engine = Arc::new(InMemoryStorageEngine::new());
        let body = br#"{"k":42}"#;
        engine.write("hash-store", body).await.unwrap();
        let svc = storage_service(engine.clone() as Arc<dyn StorageEngine>);
        let msg = TestMsg {
            item_hash: "hash-store".into(),
            item_type: ItemType::Storage,
            item_content: None,
        };
        let content = svc.get_message_content(&msg).await.unwrap();
        assert_eq!(content.source, ContentSource::Db);
        assert_eq!(content.value, json!({"k": 42}));
    }

    #[tokio::test]
    async fn get_message_content_storage_missing_returns_not_found() {
        let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::new());
        let svc = storage_service(engine);
        let msg = TestMsg {
            item_hash: "missing".into(),
            item_type: ItemType::Storage,
            item_content: None,
        };
        let err = svc.get_message_content(&msg).await.unwrap_err();
        assert!(matches!(err, AlephError::NotFound(_)));
    }

    #[tokio::test]
    async fn get_message_content_rejects_u0000() {
        let engine = Arc::new(InMemoryStorageEngine::new());
        // The escape sequence \u0000 (six characters) must appear verbatim
        // in the payload; check_for_u0000 should reject it.
        let payload = "{\"a\":\"\\u0000\"}";
        engine.write("h", payload.as_bytes()).await.unwrap();
        let svc = storage_service(engine.clone() as Arc<dyn StorageEngine>);
        let msg = TestMsg {
            item_hash: "h".into(),
            item_type: ItemType::Storage,
            item_content: None,
        };
        let err = svc.get_message_content(&msg).await.unwrap_err();
        assert!(matches!(err, AlephError::InvalidMessage(_)));
    }

    #[tokio::test]
    async fn get_message_content_ipfs_uses_engine_when_present() {
        let engine = Arc::new(InMemoryStorageEngine::new());
        engine
            .write("ipfs-hash", br#"{"src":"ipfs"}"#)
            .await
            .unwrap();
        let svc = storage_service(engine.clone() as Arc<dyn StorageEngine>);
        let msg = TestMsg {
            item_hash: "ipfs-hash".into(),
            item_type: ItemType::Ipfs,
            item_content: None,
        };
        let content = svc.get_message_content(&msg).await.unwrap();
        assert_eq!(content.source, ContentSource::Db);
        assert_eq!(content.value, json!({"src":"ipfs"}));
    }
}
