//! Background garbage collector for orphan files + REMOVING -> REMOVED
//! message transitions. Mirrors `aleph/services/storage/garbage_collector.py`.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;

use crate::AlephError;
use crate::AlephResult;
use crate::db::accessors::cost::delete_costs_for_forgotten_and_deleted_messages;
use crate::db::accessors::files::{
    delete_file as delete_file_db, delete_grace_period_file_pins, file_pin_exists,
    get_unpinned_files,
};
use crate::db::accessors::messages::{
    MessageHashesFilters, get_matching_hashes, get_message_by_item_hash, upsert_message_status,
};
use crate::services::ipfs::service::IpfsService;
use crate::services::storage::engine::StorageEngine;
use crate::types::message_status::MessageStatus;

/// Orphan-file collector. Mirrors `aleph.services.storage.garbage_collector.GarbageCollector`.
///
/// `grace_period_hours` is not used by the algorithm itself — the deletion
/// `delete_by` is set at the call sites that create grace-period pins. We
/// retain the field for API parity with future schedulers.
#[derive(Clone)]
pub struct GarbageCollector {
    pub pool: Pool,
    pub storage_engine: Arc<dyn StorageEngine>,
    pub ipfs: Option<Arc<IpfsService>>,
    pub grace_period_hours: u64,
}

impl GarbageCollector {
    pub fn new(
        pool: Pool,
        storage_engine: Arc<dyn StorageEngine>,
        ipfs: Option<Arc<IpfsService>>,
        grace_period_hours: u64,
    ) -> Self {
        Self {
            pool,
            storage_engine,
            ipfs,
            grace_period_hours,
        }
    }

    /// Detach an unpinned IPFS file: unpin it on the daemon, then erase its
    /// local cache. Mirrors `_delete_from_ipfs`.
    async fn delete_from_ipfs(&self, file_hash: &str) {
        if let Some(ipfs) = &self.ipfs
            && let Err(err) = ipfs.unpin(file_hash).await
        {
            tracing::warn!("Failed to unpin file {file_hash}: {err}");
        }
        if let Err(err) = self.storage_engine.delete(file_hash).await {
            tracing::warn!("Failed to remove {file_hash} from local storage: {err}");
        }
    }

    async fn delete_from_local_storage(&self, file_hash: &str) -> AlephResult<()> {
        self.storage_engine.delete(file_hash).await
    }

    /// Move REMOVING messages whose resources are gone to REMOVED. Mirrors
    /// `_check_and_update_removing_messages`.
    pub async fn check_and_update_removing_messages(&self) -> AlephResult<()> {
        tracing::info!("Checking messages with REMOVING status");
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        let filters = MessageHashesFilters {
            pagination: 0,
            page: 1,
            status: Some(MessageStatus::Removing),
            hash_only: true,
            ..MessageHashesFilters::default()
        };
        let rows = get_matching_hashes(&**client, &filters).await?;
        tracing::info!("Found {} messages with REMOVING status", rows.len());

        let tx = client
            .transaction()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        for row in rows {
            let item_hash = row.item_hash;
            // Open a savepoint per row so a single bad row doesn't abort the
            // whole batch (matches Python's per-statement savepoint semantics).
            if let Err(err) = tx.execute("SAVEPOINT removing_row", &[]).await {
                tracing::error!(
                    "Failed to open savepoint for message {item_hash}: {err}; aborting batch"
                );
                break;
            }
            let result = async {
                let msg = get_message_by_item_hash(&*tx, &item_hash).await?;
                let mut resources_deleted = true;
                if let Some(m) = msg
                    && m.r#type == aleph_types::message::MessageType::Store
                    && file_pin_exists(&*tx, &item_hash).await?
                {
                    resources_deleted = false;
                }
                if resources_deleted {
                    let now = Utc::now();
                    upsert_message_status(
                        &*tx,
                        &item_hash,
                        MessageStatus::Removed,
                        now,
                        Some("message_status.status = 'removing'"),
                    )
                    .await?;
                    tx.execute(
                        "UPDATE messages SET status = $2 WHERE item_hash = $1",
                        &[&item_hash, &"removed"],
                    )
                    .await?;
                }
                Ok::<(), AlephError>(())
            }
            .await;
            match result {
                Ok(()) => {
                    if let Err(err) = tx.execute("RELEASE SAVEPOINT removing_row", &[]).await {
                        tracing::warn!(
                            "Failed to release savepoint for {item_hash}: {err}"
                        );
                    }
                }
                Err(err) => {
                    tracing::error!(
                        "Failed to check or update message status {item_hash}: {err}"
                    );
                    if let Err(rb_err) =
                        tx.execute("ROLLBACK TO SAVEPOINT removing_row", &[]).await
                    {
                        tracing::error!(
                            "Failed to roll back savepoint for {item_hash}: {rb_err}; aborting batch"
                        );
                        break;
                    }
                    // Also release the savepoint so subsequent rows reuse the name.
                    let _ = tx.execute("RELEASE SAVEPOINT removing_row", &[]).await;
                }
            }
        }
        delete_costs_for_forgotten_and_deleted_messages(&*tx).await?;
        tx.commit()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        Ok(())
    }

    /// One collection sweep: clear expired grace-period pins, drop orphan
    /// files (from local storage + IPFS + db), then bring REMOVING messages
    /// to REMOVED if their resources are gone. Mirrors `GarbageCollector.collect`.
    pub async fn collect(&self, datetime: DateTime<Utc>) -> AlephResult<()> {
        let files_to_delete = {
            let client = self
                .pool
                .get()
                .await
                .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
            delete_grace_period_file_pins(&**client, datetime).await?;
            let files = get_unpinned_files(&**client).await?;
            tracing::info!("Found {} files to delete", files.len());
            files
        };

        for file in files_to_delete {
            let file_hash = file.hash;
            tracing::info!("Deleting {file_hash}...");
            let is_ipfs = match crate::schemas::base_messages::item_type_from_hash(&file_hash) {
                Ok(t) => t == aleph_types::message::item_type::ItemType::Ipfs,
                Err(_) => false,
            };
            if is_ipfs {
                self.delete_from_ipfs(&file_hash).await;
            } else if let Err(err) = self.delete_from_local_storage(&file_hash).await {
                tracing::error!("Failed to delete file {file_hash}: {err}");
                continue;
            }
            let client = self
                .pool
                .get()
                .await
                .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
            if let Err(err) = delete_file_db(&**client, &file_hash).await {
                tracing::error!("Failed to delete file row {file_hash}: {err}");
                continue;
            }
            tracing::info!("Deleted {file_hash}");
        }

        self.check_and_update_removing_messages().await?;
        Ok(())
    }
}

/// Long-running collector loop. Mirrors `garbage_collector_task`.
///
/// Sleeps `garbage_collector_period_hours * 3600` between iterations. Loops
/// while `should_continue` returns true so tests can break out cleanly.
pub async fn garbage_collector_task<F>(
    gc: GarbageCollector,
    garbage_collector_period_hours: u64,
    mut should_continue: F,
) where
    F: FnMut() -> bool + Send + 'static,
{
    let interval = Duration::from_secs(garbage_collector_period_hours.saturating_mul(3600));
    while should_continue() {
        tracing::info!("Next garbage collector run in {:?}", interval);
        tokio::time::sleep(interval).await;
        if !should_continue() {
            break;
        }
        tracing::info!("Starting garbage collection...");
        match gc.collect(Utc::now()).await {
            Ok(()) => tracing::info!("Garbage collector ran successfully."),
            Err(err) => {
                tracing::error!("An unexpected error occurred during garbage collection: {err}")
            }
        }
    }
}

/// Cancellable runtime loop used by the Rust node entrypoint. Same cadence as
/// [`garbage_collector_task`], but wakes promptly when the shared shutdown
/// token fires.
pub async fn run(
    gc: GarbageCollector,
    garbage_collector_period_hours: u64,
    cancel: crate::jobs::job_utils::CancelToken,
) -> AlephResult<()> {
    let interval = Duration::from_secs(garbage_collector_period_hours.saturating_mul(3600));
    loop {
        tracing::info!("Next garbage collector run in {:?}", interval);
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = cancel.cancelled() => return Ok(()),
        }

        if cancel.is_cancelled() {
            return Ok(());
        }
        tracing::info!("Starting garbage collection...");
        match gc.collect(Utc::now()).await {
            Ok(()) => tracing::info!("Garbage collector ran successfully."),
            Err(err) => {
                tracing::error!("An unexpected error occurred during garbage collection: {err}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::storage::in_memory::InMemoryStorageEngine;

    #[tokio::test]
    async fn loop_exits_when_should_continue_returns_false() {
        // Build a GarbageCollector without exercising any DB by using a
        // never-used pool. We pre-empt the loop by returning false on the
        // first check.
        let cfg = crate::config::PostgresSettings {
            host: "127.0.0.1".into(),
            port: 1, // unreachable; not actually used because should_continue=false
            database: "x".into(),
            user: "x".into(),
            password: "x".into(),
            pool_size: 1,
            pool_pre_ping: false,
            pool_recycle: 1,
        };
        // Building a pool doesn't connect; we never call pool.get().
        let pool = crate::db::connect(&cfg).await.unwrap();
        let engine = Arc::new(InMemoryStorageEngine::new()) as Arc<dyn StorageEngine>;
        let gc = GarbageCollector::new(pool, engine, None, 1);
        garbage_collector_task(gc, 1, || false).await;
    }
}
