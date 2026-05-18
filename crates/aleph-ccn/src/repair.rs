//! Startup repair routines. Mirrors `aleph/repair.py`.
//!
//! Two passes run on each boot:
//! - [`fix_file_sizes`] — populate `files.size` for rows whose size is
//!   sentinel-negative by re-fetching content from local/IPFS storage.
//! - [`repair_credit_balances`] — rebuild the `credit_balances` lot cache
//!   chronologically from `credit_history`. Idempotent.

use std::time::Duration;

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use tokio_postgres::GenericClient;

use crate::AlephError;
use crate::AlephResult;
use crate::db::accessors::files::upsert_file;
use crate::db::models::files::StoredFileDb;
use crate::storage::StorageService;

/// Fetch all `files` rows whose `size < 0`.
async fn list_files_with_negative_size(
    client: &impl GenericClient,
) -> AlephResult<Vec<StoredFileDb>> {
    let sql = "SELECT hash, size, type FROM files WHERE size < 0";
    let rows = client.query(sql, &[]).await?;
    Ok(rows.iter().map(StoredFileDb::from_row).collect())
}

/// Patch up files with sentinel-negative size by re-reading their content.
/// Mirrors `_fix_file_sizes`.
///
/// Routes through [`StorageService::get_hash_content`] so the P2P fallback
/// runs and successful fetches are written back to local storage (matching
/// Python's `use_network=True, use_ipfs=True, store_value=store_files`).
pub async fn fix_file_sizes(
    pool: &Pool,
    storage_service: &StorageService,
    store_files: bool,
) -> AlephResult<()> {
    let mut client = pool
        .get()
        .await
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    let files = list_files_with_negative_size(&**client).await?;
    tracing::info!("Found {} files with negative size", files.len());

    let tx = client
        .transaction()
        .await
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    for file in files {
        let hash = file.hash.as_str();
        tracing::info!("Fixing file {hash}");

        let engine = match crate::schemas::base_messages::item_type_from_hash(hash) {
            Ok(t) => t,
            Err(err) => {
                tracing::error!("Cannot infer item type for {hash}: {err}");
                continue;
            }
        };

        match storage_service
            .get_hash_content(
                hash,
                engine,
                Duration::from_secs(30),
                3,
                true,
                true,
                store_files,
            )
            .await
        {
            Ok(raw) => {
                let size = raw.value.len() as i64;
                if let Err(err) = upsert_file(&*tx, hash, size, file.r#type).await {
                    tracing::error!("Failed to upsert file size for {hash}: {err}");
                }
            }
            Err(err) => {
                tracing::error!("Failed to fetch file {hash}: {err}");
            }
        }
    }
    tx.commit()
        .await
        .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
    Ok(())
}

/// One pass of credit-balance reconstruction for a single address. Mirrors
/// `_rebuild_credit_lots_for_address`.
async fn rebuild_credit_lots_for_address(
    client: &impl GenericClient,
    address: &str,
) -> AlephResult<()> {
    client
        .execute(
            "DELETE FROM credit_balances WHERE address = $1",
            &[&address],
        )
        .await?;

    let sql = "SELECT credit_ref, credit_index, amount, expiration_date, message_timestamp \
               FROM credit_history WHERE address = $1 \
               ORDER BY message_timestamp ASC, credit_ref ASC, credit_index ASC";
    let rows = client.query(sql, &[&address]).await?;

    struct Lot {
        credit_ref: String,
        credit_index: i32,
        amount_remaining: i64,
        expiration_date: Option<DateTime<Utc>>,
        message_timestamp: DateTime<Utc>,
    }

    let mut lots: Vec<Lot> = Vec::new();
    for row in rows {
        let amount: i64 = row.get("amount");
        let expiration_date: Option<DateTime<Utc>> = row.get("expiration_date");
        let message_timestamp: DateTime<Utc> = row.get("message_timestamp");
        let credit_ref: String = row.get("credit_ref");
        let credit_index: i32 = row.get("credit_index");

        if amount > 0 {
            lots.push(Lot {
                credit_ref,
                credit_index,
                amount_remaining: amount,
                expiration_date,
                message_timestamp,
            });
        } else {
            let mut remaining = -amount;
            for lot in lots.iter_mut() {
                if remaining <= 0 {
                    break;
                }
                if lot.amount_remaining <= 0 {
                    continue;
                }
                if let Some(exp) = lot.expiration_date
                    && exp <= message_timestamp
                {
                    continue;
                }
                let take = lot.amount_remaining.min(remaining);
                lot.amount_remaining -= take;
                remaining -= take;
            }
        }
    }

    for lot in lots.into_iter().filter(|l| l.amount_remaining > 0) {
        client
            .execute(
                "INSERT INTO credit_balances(address, credit_ref, credit_index, amount_remaining, \
                                              expiration_date, message_timestamp) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &address,
                    &lot.credit_ref,
                    &lot.credit_index,
                    &lot.amount_remaining,
                    &lot.expiration_date,
                    &lot.message_timestamp,
                ],
            )
            .await?;
    }
    Ok(())
}

/// Bootstrap or repair `credit_balances` from `credit_history` for every
/// address that has any history rows. Mirrors `_repair_credit_balances`.
pub async fn repair_credit_balances(pool: &Pool) -> AlephResult<()> {
    let addresses: Vec<String> = {
        let client = pool
            .get()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        let rows = client
            .query("SELECT DISTINCT address FROM credit_history", &[])
            .await?;
        rows.into_iter().map(|r| r.get::<_, String>(0)).collect()
    };
    tracing::info!(
        "Repairing credit_balances for {} address(es)",
        addresses.len()
    );

    for (i, address) in addresses.iter().enumerate() {
        let client = pool
            .get()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        rebuild_credit_lots_for_address(&**client, address).await?;
        if (i + 1) % 500 == 0 {
            tracing::info!("Repaired {} / {}", i + 1, addresses.len());
        }
    }
    tracing::info!(
        "Credit balances repair complete ({} address(es))",
        addresses.len()
    );
    Ok(())
}

/// Run all startup repairs in order. Mirrors `repair_node`.
pub async fn repair_node(
    pool: &Pool,
    storage_service: &StorageService,
) -> AlephResult<()> {
    tracing::info!("Fixing file sizes");
    fix_file_sizes(pool, storage_service, true).await?;
    tracing::info!("Repairing credit balances");
    repair_credit_balances(pool).await?;
    Ok(())
}
