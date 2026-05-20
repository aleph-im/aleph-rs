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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CreditLot {
    credit_ref: String,
    credit_index: i32,
    amount_remaining: i64,
    expiration_date: Option<DateTime<Utc>>,
    message_timestamp: DateTime<Utc>,
}

fn rebuild_credit_lots_from_history(rows: Vec<CreditLot>) -> Vec<CreditLot> {
    let mut lots: Vec<CreditLot> = Vec::new();
    for row in rows {
        if row.amount_remaining > 0 {
            lots.push(row);
        } else {
            let mut remaining = -row.amount_remaining;
            for lot in lots.iter_mut() {
                if remaining <= 0 {
                    break;
                }
                if lot.amount_remaining <= 0 {
                    continue;
                }
                if let Some(exp) = lot.expiration_date
                    && exp <= row.message_timestamp
                {
                    continue;
                }
                let take = lot.amount_remaining.min(remaining);
                lot.amount_remaining -= take;
                remaining -= take;
            }
        }
    }

    lots.into_iter()
        .filter(|lot| lot.amount_remaining > 0)
        .collect()
}

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

    let history = rows
        .into_iter()
        .map(|row| CreditLot {
            credit_ref: row.get("credit_ref"),
            credit_index: row.get("credit_index"),
            amount_remaining: row.get("amount"),
            expiration_date: row.get("expiration_date"),
            message_timestamp: row.get("message_timestamp"),
        })
        .collect();

    for lot in rebuild_credit_lots_from_history(history) {
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
        let mut client = pool
            .get()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        rebuild_credit_lots_for_address(&*tx, address).await?;
        tx.commit()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn lot(
        credit_ref: &str,
        credit_index: i32,
        amount_remaining: i64,
        expiration_date: Option<DateTime<Utc>>,
        message_timestamp: DateTime<Utc>,
    ) -> CreditLot {
        CreditLot {
            credit_ref: credit_ref.to_string(),
            credit_index,
            amount_remaining,
            expiration_date,
            message_timestamp,
        }
    }

    #[test]
    fn rebuild_credit_lots_consumes_oldest_lots_valid_at_expense_time() {
        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2025, 1, 4, 0, 0, 0).unwrap();

        let rebuilt = rebuild_credit_lots_from_history(vec![
            lot("expired", 0, 40, Some(t1), t0),
            lot("grant-a", 0, 100, None, t0),
            lot("grant-b", 0, 50, Some(t3), t1),
            lot("expense", 0, -120, None, t2),
        ]);

        assert_eq!(
            rebuilt,
            vec![
                lot("expired", 0, 40, Some(t1), t0),
                lot("grant-b", 0, 30, Some(t3), t1),
            ]
        );
    }
}
