//! STORE message handler. Mirrors `aleph/handlers/content/store.py`.

use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aleph_types::message::item_type::ItemType;

use crate::db::accessors::files::{
    delete_file_pin, get_file, get_file_tag, get_message_file_pin, insert_grace_period_file_pin,
    insert_message_file_pin, is_pinned_file, refresh_file_tag, upsert_file, upsert_file_tag,
};
use crate::db::models::account_costs::{AccountCostsDb, PaymentType};
use crate::db::models::messages::MessageDb;
use crate::handlers::content::content_handler::ContentHandler;
use crate::schemas::base_messages::item_type_from_hash;
use crate::services::cost::{
    CostContent, CostContentKind, calculate_storage_size, get_payment_type,
    get_total_and_detailed_costs,
};
use crate::services::cost_validation::validate_balance_for_payment;
use crate::services::ipfs::IpfsService;
use crate::services::storage::engine::StorageEngine;
use crate::storage::StorageService;
use crate::toolkit::constants::MIB;
use crate::toolkit::costs::{
    StoreAndProgramFreeInput, are_store_and_program_free, is_credit_only_required,
};
use crate::toolkit::metrics_keys::store_fetch_keys;
use crate::toolkit::timestamp::{timestamp_to_datetime, utc_now};
use crate::types::files::{FileTag, FileType};
use crate::types::message_status::MessageProcessingException;

fn content_address(message: &MessageDb) -> Result<String, MessageProcessingException> {
    message
        .content
        .get("address")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| MessageProcessingException::InvalidMessageFormat {
            errors: vec![format!(
                "Store message {} missing 'address'",
                message.item_hash
            )],
        })
}

fn store_item_hash(message: &MessageDb) -> Result<String, MessageProcessingException> {
    message
        .content
        .get("item_hash")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| MessageProcessingException::InvalidMessageFormat {
            errors: vec![format!(
                "Store message {} missing 'item_hash'",
                message.item_hash
            )],
        })
}

fn store_item_type(message: &MessageDb) -> Result<ItemType, MessageProcessingException> {
    let s = message
        .content
        .get("item_type")
        .and_then(|v| v.as_str())
        .unwrap_or("storage");
    serde_json::from_value::<ItemType>(serde_json::Value::String(s.to_string())).map_err(|e| {
        MessageProcessingException::InvalidMessageFormat {
            errors: vec![format!("Invalid item_type {s}: {e}")],
        }
    })
}

fn content_ref(message: &MessageDb) -> Option<String> {
    message
        .content
        .get("ref")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn content_time(message: &MessageDb) -> chrono::DateTime<chrono::Utc> {
    let ts = message
        .content
        .get("time")
        .and_then(|v| v.as_f64())
        .unwrap_or_else(|| {
            message.time.timestamp() as f64
                + (message.time.timestamp_subsec_nanos() as f64) / 1_000_000_000.0
        });
    timestamp_to_datetime(ts)
}

/// Mirror Python's `ItemHash(ref)` recognition rules, which `make_file_tag`
/// uses to decide whether a ref is an actual hash or a user tag.
///
/// This follows pyaleph's strict hash recognition so a plain user tag like
/// `"myref"` is not silently treated as a hash.
fn ref_is_item_hash(r: &str) -> bool {
    (r.starts_with("Qm") && (44..=46).contains(&r.len()))
        || (r.starts_with("bafy") && r.len() == 59)
        || r.len() == 64
}

/// Compute a file tag from `(owner, ref, item_hash)`. Mirrors Python's
/// `aleph.utils.make_file_tag`.
///
/// Rules (must match `pyaleph/aleph/utils.py::make_file_tag`):
/// * `ref` is None / empty   → tag = `item_hash`
/// * `ref` is a valid hash  → tag = `ref` (as-is)
/// * `ref` is a user value  → tag = `<owner>/<ref>`
fn make_file_tag(owner: &str, r#ref: Option<&str>, item_hash: &str) -> FileTag {
    match r#ref {
        Some(r) if !r.is_empty() => {
            if ref_is_item_hash(r) {
                FileTag::from(r)
            } else {
                FileTag::from(format!("{owner}/{r}"))
            }
        }
        _ => FileTag::from(item_hash),
    }
}

fn build_free_input(message: &MessageDb) -> StoreAndProgramFreeInput {
    StoreAndProgramFreeInput {
        confirmation_height: message.first_confirmed_height,
        time: message.time,
    }
}

/// Stats for an IPFS file. Mirrors Python's `IpfsFileStats` dataclass.
#[derive(Debug, Clone)]
pub struct IpfsFileStats {
    pub size: i64,
    pub file_type: FileType,
    /// True when the underlying IPFS object is a directory (UnixFS folder).
    pub is_directory: bool,
}

/// Wait a randomized delay before starting an IPFS fetch. Mirrors Python's
/// `_apply_fetch_jitter`.
///
/// Spreads the simultaneous fetch attempts from many CCNs receiving the same
/// STORE message into a rolling wave so early fetchers can become reseeders for
/// later ones before the origin's uplink is saturated. A no-op when the window
/// is zero, so the behaviour is opt-in via `ipfs.fetch_jitter_seconds`.
async fn apply_fetch_jitter(window_seconds: f64, file_hash: &str) {
    if window_seconds <= 0.0 {
        return;
    }
    let delay = rand::random::<f64>() * window_seconds;
    tracing::info!("ipfs_fetch_jitter hash={file_hash} delay={delay:.2}");
    tokio::time::sleep(Duration::from_secs_f64(delay)).await;
}

/// Decide whether an IPFS object should be pinned locally. Mirrors Python's
/// `_should_pin_on_ipfs`: always pin directories; pin files only when they
/// exceed `min_file_size_for_pinning`.
pub fn should_pin_on_ipfs(stats: &IpfsFileStats, min_file_size_for_pinning: i64) -> bool {
    if stats.is_directory {
        return true;
    }
    stats.size > min_file_size_for_pinning
}

/// STORE message handler.
pub struct StoreMessageHandler {
    pub storage_engine: Arc<dyn StorageEngine>,
    pub ipfs_service: Option<Arc<IpfsService>>,
    pub grace_period_hours: i64,
    pub max_unauthenticated_upload_file_size: i64,
    /// When false, even IPFS-pinning operations are skipped.
    pub ipfs_enabled: bool,
    /// When false, network-fetched content is not persisted to storage.
    pub store_files: bool,
    /// IPFS `/files/stat` timeout, in seconds. Pulled from `config.ipfs.stat_timeout`.
    pub ipfs_stat_timeout_secs: u64,
    /// Randomized delay window (seconds) applied before an IPFS fetch starts.
    /// Pulled from `config.ipfs.fetch_jitter_seconds`.
    pub ipfs_fetch_jitter_seconds: f64,
    /// HTTP API servers used as a fallback when a `storage`-type file is
    /// missing locally. Mirrors Python's `api-servers` peers list.
    pub api_servers: Vec<String>,
    pub storage_service: Option<Arc<StorageService>>,
}

impl StoreMessageHandler {
    pub fn new(
        storage_engine: Arc<dyn StorageEngine>,
        ipfs_service: Option<Arc<IpfsService>>,
        grace_period_hours: i64,
        max_unauthenticated_upload_file_size: i64,
        ipfs_enabled: bool,
        store_files: bool,
        ipfs_stat_timeout_secs: u64,
        ipfs_fetch_jitter_seconds: f64,
        api_servers: Vec<String>,
    ) -> Self {
        Self {
            storage_engine,
            ipfs_service,
            grace_period_hours,
            max_unauthenticated_upload_file_size,
            ipfs_enabled,
            store_files,
            ipfs_stat_timeout_secs,
            ipfs_fetch_jitter_seconds,
            api_servers,
            storage_service: None,
        }
    }

    pub fn with_storage_service(mut self, storage_service: Arc<StorageService>) -> Self {
        self.storage_service = Some(storage_service);
        self
    }

    /// Increment a shared STORE file-fetch metric counter by 1. Routes through
    /// the storage service's node cache (Redis in production). A no-op when no
    /// storage service is wired or when the increment fails — metrics must
    /// never block message processing. Mirrors `node_cache.incr` in pyaleph.
    async fn incr_metric(&self, key: &str) {
        if let Some(storage_service) = &self.storage_service {
            if let Err(e) = storage_service.node_cache.incr_metric(key).await {
                tracing::debug!("failed to increment metric {key}: {e}");
            }
        }
    }

    /// Increment a shared STORE file-fetch metric counter by `amount`. See
    /// [`Self::incr_metric`]. Mirrors `node_cache.incrby` in pyaleph.
    async fn incrby_metric(&self, key: &str, amount: i64) {
        if let Some(storage_service) = &self.storage_service {
            if let Err(e) = storage_service.node_cache.incrby_metric(key, amount).await {
                tracing::debug!("failed to increment metric {key} by {amount}: {e}");
            }
        }
    }

    async fn pin_and_tag_file(
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let file_hash = store_item_hash(message)?;
        let owner = content_address(message)?;
        let r#ref = content_ref(message);
        let created = content_time(message);

        insert_message_file_pin(
            client,
            &file_hash,
            Some(owner.as_str()),
            &message.item_hash,
            r#ref.as_deref(),
            created,
        )
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("DB error inserting message file pin: {e}")],
        })?;

        let tag = make_file_tag(&owner, r#ref.as_deref(), &message.item_hash);
        upsert_file_tag(client, &tag, &owner, &file_hash, created)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error upserting file tag: {e}")],
            })?;
        Ok(())
    }

    async fn check_remaining_pins(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        storage_hash: &str,
        storage_type: ItemType,
    ) -> Result<(), MessageProcessingException> {
        tracing::debug!("Garbage collecting {storage_hash}");
        let pinned = is_pinned_file(client, storage_hash).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error checking is_pinned_file: {e}")],
            }
        })?;
        if pinned {
            tracing::debug!("File {storage_hash} has at least one reference left");
            return Ok(());
        }
        // Sanity-check ItemType consistency.
        let detected = item_type_from_hash(storage_hash).map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("Unknown ItemType for {storage_hash}: {e}")],
            }
        })?;
        if detected != storage_type {
            return Err(MessageProcessingException::InternalError {
                errors: vec![format!(
                    "Inconsistent ItemType {storage_type:?} != {detected:?} for hash '{storage_hash}'"
                )],
            });
        }
        let now = utc_now();
        let delete_by = now + chrono::Duration::hours(self.grace_period_hours);
        tracing::info!("Inserting grace period pin for {storage_hash}");
        insert_grace_period_file_pin(client, storage_hash, now, delete_by, None, None, None)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting grace period pin: {e}")],
            })?;
        Ok(())
    }

    /// Fetch the file referenced by the STORE message via the storage engine
    /// and (optionally) the IPFS gateway. Mirrors the `fetch_related_content`
    /// flow in Python: directories are always pinned on IPFS regardless of
    /// size; files >1MiB are pinned; smaller files / `storage`-type entries
    /// fall back to the storage engine and finally to the network.
    async fn fetch_related_content_impl(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let file_hash = store_item_hash(message)?;
        let item_type = store_item_type(message)?;

        // Basic sanity check: the hash must match the declared item_type.
        let detected = item_type_from_hash(&file_hash).map_err(|e| {
            MessageProcessingException::InvalidMessageFormat {
                errors: vec![format!(
                    "Item hash '{file_hash}' is not of the expected type ({item_type:?}): {e}"
                )],
            }
        })?;
        if detected != item_type {
            return Err(MessageProcessingException::InvalidMessageFormat {
                errors: vec![format!(
                    "Item hash '{file_hash}' is not of the expected type ('{item_type:?}')"
                )],
            });
        }

        let (total_key, failed_key, duration_key) = store_fetch_keys(item_type);

        // Mirror pyaleph: jitter is applied at the top of the `item_type == ipfs`
        // branch, BEFORE the `ipfs_enabled` gate, so every node that receives the
        // STORE message participates in the rolling fetch wave even if local IPFS
        // pinning is disabled (#1171/#1164).
        if item_type == ItemType::Ipfs {
            apply_fetch_jitter(self.ipfs_fetch_jitter_seconds, &file_hash).await;
        }

        if item_type == ItemType::Ipfs && self.ipfs_enabled {
            if let Some(ipfs) = &self.ipfs_service {
                let stat_timeout = Duration::from_secs(self.ipfs_stat_timeout_secs);
                let stat = ipfs.stat(&file_hash, stat_timeout).await.map_err(|_| {
                    MessageProcessingException::file_unavailable_with_details(
                        file_hash.clone(),
                        "could not retrieve IPFS file stats at this time",
                    )
                })?;
                let stats = IpfsFileStats {
                    size: stat.size as i64,
                    file_type: if stat.is_directory {
                        FileType::Directory
                    } else {
                        FileType::File
                    },
                    is_directory: stat.is_directory,
                };

                if should_pin_on_ipfs(&stats, MIB as i64) {
                    // Counted before the fetch so every attempt is represented. A
                    // crash between here and the success/failure path leaves the
                    // total without a matching duration or failure entry, marginally
                    // skewing the mean — an acceptable tradeoff for approximate
                    // monitoring counters. Mirrors pyaleph #1164.
                    self.incr_metric(total_key).await;
                    let timer = std::time::Instant::now();
                    // Directories are force-pinned regardless of size.
                    if ipfs
                        .pin_add(&file_hash, Duration::from_secs(30), 1)
                        .await
                        .is_err()
                    {
                        self.incr_metric(failed_key).await;
                        tracing::warn!(
                            "ipfs_fetch hash={file_hash} type=ipfs path=pin size={} duration={:.2} outcome=fail",
                            stats.size,
                            timer.elapsed().as_secs_f64(),
                        );
                        return Err(MessageProcessingException::file_unavailable_with_details(
                            file_hash.clone(),
                            "could not pin IPFS content at this time",
                        ));
                    }
                    let elapsed = timer.elapsed().as_secs_f64();
                    self.incrby_metric(duration_key, (elapsed * 1000.0).round() as i64)
                        .await;
                    tracing::info!(
                        "ipfs_fetch hash={file_hash} type=ipfs path=pin size={} duration={elapsed:.2} outcome=ok",
                        stats.size,
                    );
                    upsert_file(client, &file_hash, stats.size, stats.file_type)
                        .await
                        .map_err(|e| MessageProcessingException::InternalError {
                            errors: vec![format!("DB error upserting file: {e}")],
                        })?;
                    return Ok(());
                }
            }
        }

        // Smaller files (or storage-type files) come from local storage.
        //
        // pyaleph unconditionally `node_cache.incr(total_key)` on the HTTP path:
        // it always reaches `get_hash_content` for non-pinned items, even when the
        // file is already present locally. Increment here — before the local-exists
        // short-circuit and independent of `storage_service` presence — so the
        // total count matches pyaleph (#1164). A no-op without a storage service.
        self.incr_metric(total_key).await;

        let exists = self.storage_engine.exists(&file_hash).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("Storage engine exists() failed: {e}")],
            }
        })?;
        if !exists {
            if let Some(storage_service) = &self.storage_service {
                // Fetch content directly from the Aleph network storage API.
                let timer = std::time::Instant::now();
                match storage_service
                    .get_hash_content(
                        &file_hash,
                        item_type,
                        Duration::from_secs(15),
                        4,
                        true,
                        true,
                        self.store_files,
                    )
                    .await
                {
                    Ok(raw) => {
                        let elapsed = timer.elapsed().as_secs_f64();
                        self.incrby_metric(duration_key, (elapsed * 1000.0).round() as i64)
                            .await;
                        tracing::info!(
                            "ipfs_fetch hash={file_hash} type={item_type:?} path=http size={} duration={elapsed:.2} outcome=ok",
                            raw.value.len(),
                        );
                        upsert_file(client, &file_hash, raw.value.len() as i64, FileType::File)
                            .await
                            .map_err(|e| MessageProcessingException::InternalError {
                                errors: vec![format!("DB error upserting file: {e}")],
                            })?;
                        return Ok(());
                    }
                    Err(crate::AlephError::InvalidMessage(msg)) => {
                        return Err(MessageProcessingException::InvalidMessageFormat {
                            errors: vec![msg],
                        });
                    }
                    Err(crate::AlephError::NotFound(_))
                    | Err(crate::AlephError::Ipfs(_))
                    | Err(crate::AlephError::P2p(_)) => {
                        // AlephStorageException equivalents: the network fetch
                        // came up empty. Record the failure before falling
                        // through to the IPFS-gateway / api-servers fallbacks.
                        self.incr_metric(failed_key).await;
                        tracing::warn!(
                            "ipfs_fetch hash={file_hash} type={item_type:?} path=http duration={:.2} outcome=unavailable",
                            timer.elapsed().as_secs_f64(),
                        );
                    }
                    Err(e) => {
                        return Err(MessageProcessingException::InternalError {
                            errors: vec![format!("Storage service fetch failed: {e}")],
                        });
                    }
                }
            }

            // For IPFS we can fetch via the gateway.
            if item_type == ItemType::Ipfs && self.ipfs_enabled {
                if let Some(ipfs) = &self.ipfs_service {
                    let bytes = ipfs
                        .get_ipfs_content(&file_hash, Duration::from_secs(15), 4)
                        .await
                        .map_err(|_| {
                            MessageProcessingException::file_unavailable_with_details(
                                file_hash.clone(),
                                "could not retrieve IPFS content at this time",
                            )
                        })?;
                    let bytes = bytes.ok_or_else(|| {
                        MessageProcessingException::file_unavailable_with_details(
                            file_hash.clone(),
                            "could not retrieve IPFS content at this time",
                        )
                    })?;
                    if self.store_files {
                        self.storage_engine
                            .write(&file_hash, &bytes)
                            .await
                            .map_err(|e| MessageProcessingException::InternalError {
                                errors: vec![format!("Storage write failed: {e}")],
                            })?;
                    }
                    upsert_file(client, &file_hash, bytes.len() as i64, FileType::File)
                        .await
                        .map_err(|e| MessageProcessingException::InternalError {
                            errors: vec![format!("DB error upserting file: {e}")],
                        })?;
                    return Ok(());
                }
            }

            // Network fallback for `storage`-type items: Python's
            // `StorageService.get_hash_content(..., use_network=True)` tries
            // the configured `api-servers` peer list. Mirror that here.
            if item_type == ItemType::Storage && !self.api_servers.is_empty() {
                if let Some(bytes) = crate::services::p2p::http::request_hash(
                    &self.api_servers,
                    &file_hash,
                    Duration::from_secs(15),
                )
                .await
                {
                    // Verify the peer-supplied bytes actually hash to the
                    // requested `file_hash` before persisting. pyaleph only
                    // fetches storage content through `StorageService`, which
                    // calls `_verify_content_hash`; a raw `request_hash` peer
                    // fetch must not write unverified content under a hash it
                    // does not match (otherwise a malicious peer could make
                    // this node serve corrupted content for that hash).
                    if crate::storage::verify_content_hash_sha256(&bytes) != file_hash {
                        self.incr_metric(failed_key).await;
                        tracing::warn!(
                            "store_fetch hash={file_hash} type={item_type:?} path=api-servers outcome=hash-mismatch",
                        );
                        return Err(MessageProcessingException::file_unavailable(file_hash));
                    }
                    if self.store_files {
                        self.storage_engine
                            .write(&file_hash, &bytes)
                            .await
                            .map_err(|e| MessageProcessingException::InternalError {
                                errors: vec![format!("Storage write failed: {e}")],
                            })?;
                    }
                    upsert_file(client, &file_hash, bytes.len() as i64, FileType::File)
                        .await
                        .map_err(|e| MessageProcessingException::InternalError {
                            errors: vec![format!("DB error upserting file: {e}")],
                        })?;
                    return Ok(());
                }
            }
            // All fetch paths exhausted. Mirrors pyaleph's
            // `except AlephStorageException: raise FileUnavailable(file_hash,
            // "could not retrieve file from storage at this time")`.
            return Err(MessageProcessingException::file_unavailable_with_details(
                file_hash,
                "could not retrieve file from storage at this time",
            ));
        }

        // Resolve the on-disk size.
        let size = match self.storage_engine.read(&file_hash).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("Storage read failed: {e}")],
            }
        })? {
            Some(bytes) => bytes.len() as i64,
            None => 0,
        };
        upsert_file(client, &file_hash, size, FileType::File)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error upserting file: {e}")],
            })?;
        Ok(())
    }
}

#[async_trait]
impl ContentHandler for StoreMessageHandler {
    async fn is_related_content_fetched(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> crate::AlephResult<bool> {
        let file_hash = match message.content.get("item_hash").and_then(|v| v.as_str()) {
            Some(h) => h.to_string(),
            None => return Ok(false),
        };
        self.storage_engine.exists(&file_hash).await
    }

    async fn fetch_related_content(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        self.fetch_related_content_impl(client, message).await
    }

    async fn pre_check_balance(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let cost_content = CostContent::from_value(&message.content)
            .unwrap_or_else(|| CostContent::new(CostContentKind::Store, &message.content));
        let store_is_free = are_store_and_program_free(&build_free_input(message));

        let payment_type = get_payment_type(&cost_content);
        if store_is_free {
            return Ok(());
        }

        if is_credit_only_required(message.time) && payment_type != PaymentType::Credit {
            return Err(MessageProcessingException::InvalidPaymentMethod { errors: Vec::new() });
        }

        let item_type = store_item_type(message)?;
        if item_type == ItemType::Ipfs && self.ipfs_enabled {
            if let Some(ipfs) = &self.ipfs_service {
                let file_hash = store_item_hash(message)?;
                // If we already have the file locally (e.g. from a prior
                // add_file or add_car upload on this node), use the stored size
                // instead of asking kubo. Avoids a redundant dag.get round-trip
                // and the rejection risk when the daemon is busy right after
                // upload. Mirrors pyaleph #1170.
                let stored_file = get_file(client, &file_hash).await.map_err(|e| {
                    MessageProcessingException::InternalError {
                        errors: vec![format!("DB error fetching file: {e}")],
                    }
                })?;
                let ipfs_size = match stored_file {
                    Some(file) => Some(file.size as u64),
                    None => ipfs
                        .get_ipfs_size(
                            &file_hash,
                            Duration::from_secs(self.ipfs_stat_timeout_secs),
                            3,
                        )
                        .await
                        .map_err(|_| {
                            MessageProcessingException::file_unavailable_with_details(
                                file_hash.clone(),
                                "could not retrieve IPFS file stats at this time",
                            )
                        })?,
                };
                if let Some(byte_size) = ipfs_size {
                    let storage_mib = rust_decimal::Decimal::from(byte_size as i64)
                        / rust_decimal::Decimal::from(MIB);
                    if payment_type == PaymentType::Hold
                        && storage_mib
                            <= rust_decimal::Decimal::from(
                                self.max_unauthenticated_upload_file_size,
                            ) / rust_decimal::Decimal::from(MIB)
                    {
                        return Ok(());
                    }

                    // Build a CostEstimationStoreContent JSON value and price it.
                    let mut estimation = message.content.clone();
                    if let Some(obj) = estimation.as_object_mut() {
                        let estimated_size_mib = byte_size.div_ceil(MIB);
                        obj.insert(
                            "estimated_size_mib".into(),
                            serde_json::Value::Number(serde_json::Number::from(
                                estimated_size_mib,
                            )),
                        );
                    }
                    let est_content = CostContent::new(CostContentKind::Store, &estimation);
                    let (message_cost, _) =
                        get_total_and_detailed_costs(client, &est_content, &message.item_hash)
                            .await
                            .map_err(|e| MessageProcessingException::InternalError {
                                errors: vec![format!("Cost calc failed: {e}")],
                            })?;
                    let validation = validate_balance_for_payment(
                        client,
                        &content_address(message)?,
                        message_cost,
                        payment_type,
                    )
                    .await
                    .map_err(|e| {
                        MessageProcessingException::InternalError {
                            errors: vec![format!("Balance validation failed: {e}")],
                        }
                    })?;
                    return validation.into_result();
                }
            }
        }

        // Default: no IPFS file or feature disabled — skip the pre-check.
        // The full balance check during process() still runs once content has
        // been fetched and its size is known.
        Ok(())
    }

    async fn check_balance(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<Option<Vec<AccountCostsDb>>, MessageProcessingException> {
        let cost_content = CostContent::from_value(&message.content)
            .unwrap_or_else(|| CostContent::new(CostContentKind::Store, &message.content));
        let (mut message_cost, mut costs) =
            get_total_and_detailed_costs(client, &cost_content, &message.item_hash)
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("Cost calc failed: {e}")],
                })?;

        let payment_type = get_payment_type(&cost_content);
        let store_is_free = are_store_and_program_free(&build_free_input(message));

        if store_is_free {
            return Ok(Some(costs));
        }

        if is_credit_only_required(message.time) && payment_type != PaymentType::Credit {
            return Err(MessageProcessingException::InvalidPaymentMethod { errors: Vec::new() });
        }

        let mut storage_mib = calculate_storage_size(client, &cost_content)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("Storage size calc failed: {e}")],
            })?;
        if storage_mib.is_none() && message.size > 0 {
            let estimated_size_mib = (message.size as u64).div_ceil(MIB);
            storage_mib = Some(rust_decimal::Decimal::from(message.size as i64)
                / rust_decimal::Decimal::from(MIB));
            let mut estimation = message.content.clone();
            if let Some(obj) = estimation.as_object_mut() {
                obj.insert(
                    "estimated_size_mib".into(),
                    serde_json::Value::Number(serde_json::Number::from(estimated_size_mib)),
                );
            }
            let est_content = CostContent::new(CostContentKind::Store, &estimation);
            (message_cost, costs) = get_total_and_detailed_costs(
                client,
                &est_content,
                &message.item_hash,
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("Cost calc failed: {e}")],
            })?;
        }
        if payment_type == PaymentType::Hold {
            if let Some(s) = storage_mib {
                if s <= rust_decimal::Decimal::from(self.max_unauthenticated_upload_file_size)
                    / rust_decimal::Decimal::from(MIB)
                {
                    return Ok(Some(costs));
                }
            }
        }

        let validation = validate_balance_for_payment(
            client,
            &content_address(message)?,
            message_cost,
            payment_type,
        )
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("Balance validation failed: {e}")],
        })?;
        validation.into_result()?;
        Ok(Some(costs))
    }

    async fn check_dependencies(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let r#ref = match content_ref(message) {
            Some(r) => r,
            None => return Ok(()),
        };
        // If the ref isn't a real ItemHash, treat it as a user tag — no checks.
        if item_type_from_hash(&r#ref).is_err() {
            return Ok(());
        }
        let ref_pin = get_message_file_pin(client, &r#ref).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error fetching ref pin: {e}")],
            }
        })?;
        let ref_pin = match ref_pin {
            None => {
                return Err(MessageProcessingException::StoreRefNotFound {
                    errors: vec![r#ref],
                });
            }
            Some(p) => p,
        };
        if ref_pin.r#ref.is_some() {
            return Err(MessageProcessingException::StoreCannotUpdateStoreWithRef {
                errors: Vec::new(),
            });
        }
        Ok(())
    }

    async fn check_permissions(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
        lookup: &dyn crate::permissions::AuthorityLookup,
    ) -> Result<(), MessageProcessingException> {
        use crate::handlers::content::content_handler::{
            MessageAuthView, check_authorization_local,
        };
        let view = MessageAuthView::from_message(message);
        if !check_authorization_local(lookup, &view).await {
            return Err(MessageProcessingException::PermissionDenied {
                errors: vec![format!(
                    "Sender {} is not authorized to post on behalf of address {}",
                    message.sender, view.content_address
                )],
            });
        }
        let r#ref = match content_ref(message) {
            Some(r) => r,
            None => return Ok(()),
        };
        let owner = content_address(message)?;
        let tag = make_file_tag(&owner, Some(&r#ref), &message.item_hash);
        let existing = get_file_tag(client, &tag).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error fetching file tag: {e}")],
            }
        })?;
        if let Some(t) = existing {
            if t.owner != owner {
                return Err(MessageProcessingException::PermissionDenied {
                    errors: vec![format!(
                        "{} attempts to update a file tag belonging to another user",
                        message.item_hash
                    )],
                });
            }
        }
        Ok(())
    }

    async fn process(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        messages: &[MessageDb],
    ) -> Result<(), MessageProcessingException> {
        for message in messages {
            Self::pin_and_tag_file(client, message).await?;
        }
        Ok(())
    }

    async fn forget_message(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<HashSet<String>, MessageProcessingException> {
        let owner = content_address(message)?;
        let file_hash = store_item_hash(message)?;
        let item_type = store_item_type(message)?;
        let r#ref = content_ref(message);

        delete_file_pin(client, &message.item_hash)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error deleting file pin: {e}")],
            })?;
        let tag = make_file_tag(&owner, r#ref.as_deref(), &message.item_hash);
        refresh_file_tag(client, &tag).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error refreshing file tag: {e}")],
            }
        })?;
        self.check_remaining_pins(client, &file_hash, item_type)
            .await?;
        Ok(HashSet::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::storage::in_memory::InMemoryStorageEngine;
    use crate::types::message_status::MessageStatus;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;
    use chrono::Utc;
    use serde_json::json;

    fn store_msg(item_hash: &str, file_hash: &str, item_type: &str) -> MessageDb {
        let now = Utc::now();
        MessageDb {
            item_hash: item_hash.into(),
            r#type: MessageType::Store,
            chain: Chain::Ethereum,
            sender: "0xabc".into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content: json!({
                "address": "0xabc",
                "item_hash": file_hash,
                "item_type": item_type,
                "time": now.timestamp() as f64,
            }),
            time: now,
            channel: None,
            size: 0,
            status_value: MessageStatus::Processed,
            reception_time: now,
            owner: Some("0xabc".into()),
            content_type: None,
            content_ref: None,
            content_key: None,
            first_confirmed_at: None,
            first_confirmed_height: None,
            payment_type: None,
            content_item_hash: Some(file_hash.into()),
            tags: None,
        }
    }

    #[test]
    fn make_file_tag_uses_owner_ref() {
        let tag = make_file_tag("0xowner", Some("myref"), "0xhash");
        assert_eq!(tag.as_str(), "0xowner/myref");
        let tag = make_file_tag("0xowner", None, "0xhash");
        assert_eq!(tag.as_str(), "0xhash");
    }

    #[test]
    fn make_file_tag_returns_hash_when_ref_is_item_hash() {
        // When the ref is itself a real item hash, the tag must be the ref
        // verbatim (no `<owner>/` prefix), matching pyaleph's `ItemHash(ref)`
        // recognition branch.
        let hashref = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";
        let tag = make_file_tag("0xowner", Some(hashref), "0xhash");
        assert_eq!(tag.as_str(), hashref);
    }

    #[test]
    fn content_helpers_extract() {
        let m = store_msg(
            "h1",
            "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8",
            "ipfs",
        );
        assert_eq!(content_address(&m).unwrap(), "0xabc");
        assert_eq!(
            store_item_hash(&m).unwrap(),
            "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8"
        );
        assert_eq!(store_item_type(&m).unwrap(), ItemType::Ipfs);
    }

    #[test]
    fn directories_are_force_pinned_regardless_of_size() {
        // Directories must always be pinned even when their cumulative size
        // is below the file-pin threshold.
        let stats = IpfsFileStats {
            size: 1,
            file_type: FileType::Directory,
            is_directory: true,
        };
        assert!(should_pin_on_ipfs(&stats, MIB as i64));

        // Files smaller than the threshold are not pinned.
        let stats = IpfsFileStats {
            size: 1,
            file_type: FileType::File,
            is_directory: false,
        };
        assert!(!should_pin_on_ipfs(&stats, MIB as i64));

        // Files larger than the threshold are pinned.
        let stats = IpfsFileStats {
            size: (MIB as i64) + 1,
            file_type: FileType::File,
            is_directory: false,
        };
        assert!(should_pin_on_ipfs(&stats, MIB as i64));
    }

    #[tokio::test]
    async fn engine_exists_drives_fetched_decision() {
        // We exercise the underlying engine that `is_related_content_fetched`
        // delegates to. The Postgres `GenericClient` is sealed by
        // tokio-postgres so we cannot stub it for non-IO paths.
        let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::default());
        let _handler =
            StoreMessageHandler::new(engine.clone(), None, 24, 0, false, false, 30, 0.0, Vec::new());
        let file_hash = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";
        assert!(!engine.exists(file_hash).await.unwrap());
        engine.write(file_hash, b"data").await.unwrap();
        assert!(engine.exists(file_hash).await.unwrap());
    }
}
