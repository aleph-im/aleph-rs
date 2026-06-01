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
use serde_json::{Map, Value, json};
use tokio_postgres::GenericClient;

use crate::AlephError;
use crate::AlephResult;
use crate::db::accessors::files::upsert_file;
use crate::db::accessors::messages::{
    get_message_by_item_hash, upsert_message_status, upsert_rejected_message,
};
use crate::db::accessors::vms::{delete_vm, delete_vm_updates};
use crate::db::models::files::StoredFileDb;
use crate::db::models::messages::MessageDb;
use crate::storage::StorageService;
use crate::toolkit::timestamp::utc_now;
use aleph_types::message::MessageType;
use crate::types::message_status::{ErrorCode, MessageStatus};

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

/// Reason attached to rejected PROGRAM messages with non-dict metadata.
/// Mirrors `_INVALID_METADATA_REASON`.
const INVALID_METADATA_REASON: &str =
    "ExecutableContent.metadata must be a dict; legacy rows with a list value \
     no longer parse and surfaced as 500s at the API.";

/// Snapshot a [`MessageDb`] row as a JSON-serializable wire-format dict
/// suitable for the `rejected_messages.message` JSONB column.
///
/// Mirrors `_wire_message_dict`: emits only canonical wire columns (excludes
/// `MessageDb::DENORMALIZED_COLUMNS`), renders `time` as a POSIX timestamp,
/// and renders enum columns (`chain`, `type`, `item_type`) as their wire
/// `.value` strings.
fn wire_message_dict(message: &MessageDb) -> Value {
    let mut out = Map::new();
    out.insert("item_hash".into(), json!(message.item_hash));
    out.insert("type".into(), serde_json::to_value(message.r#type).unwrap());
    out.insert("chain".into(), serde_json::to_value(&message.chain).unwrap());
    out.insert("sender".into(), json!(message.sender));
    out.insert(
        "signature".into(),
        match &message.signature {
            Some(s) => json!(s),
            None => Value::Null,
        },
    );
    out.insert(
        "item_type".into(),
        serde_json::to_value(message.item_type).unwrap(),
    );
    out.insert(
        "item_content".into(),
        message
            .item_content
            .as_ref()
            .map(|c| json!(c))
            .unwrap_or(Value::Null),
    );
    out.insert("content".into(), message.content.clone());
    let time = message.time.timestamp() as f64
        + (message.time.timestamp_subsec_nanos() as f64) / 1_000_000_000.0;
    out.insert("time".into(), json!(time));
    out.insert(
        "channel".into(),
        serde_json::to_value(&message.channel).unwrap(),
    );
    out.insert("size".into(), json!(message.size));
    Value::Object(out)
}

/// Transition a processed message into the REJECTED state.
///
/// Mirrors `mark_processed_message_as_rejected`: snapshots the row into
/// `rejected_messages`, cleans up VM state for program/instance messages,
/// flips `message_status` to REJECTED (only if not already rejected), and
/// deletes the `messages` row. Runs inside the caller's transaction; does not
/// commit.
async fn mark_processed_message_as_rejected(
    client: &impl GenericClient,
    message: &MessageDb,
    error_code: ErrorCode,
    reason: &str,
) -> AlephResult<()> {
    let snapshot = wire_message_dict(message);

    if matches!(message.r#type, MessageType::Program | MessageType::Instance) {
        delete_vm(client, &message.item_hash).await?;
        let _ = delete_vm_updates(client, &message.item_hash).await?;
    }

    let details = json!({ "errors": [reason] });
    upsert_rejected_message(
        client,
        &message.item_hash,
        &snapshot,
        error_code as i32,
        Some(&details),
        Some(reason),
        None,
    )
    .await?;

    upsert_message_status(
        client,
        &message.item_hash,
        MessageStatus::Rejected,
        utc_now(),
        Some("message_status.status != 'rejected'"),
    )
    .await?;

    client
        .execute(
            "DELETE FROM messages WHERE item_hash = $1",
            &[&message.item_hash],
        )
        .await?;
    Ok(())
}

/// Reject already-processed PROGRAM messages whose `content.metadata` is a
/// JSON array. Mirrors `_reject_invalid_program_metadata`.
///
/// aleph-message historically accepted `ExecutableContent.metadata` as either
/// a dict or a list; the current validator requires a dict. Rows accepted
/// under the old rules trip `parsed_content` access and surface as 500s on
/// `GET /api/v0/messages/<hash>`. Moving them to the rejected state lets the
/// API render them the way nodes that rejected them up front do.
///
/// Per-message transaction so a single bad row does not roll back the rest.
pub async fn reject_invalid_program_metadata(pool: &Pool) -> AlephResult<()> {
    let item_hashes: Vec<String> = {
        let client = pool
            .get()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        let rows = client
            .query(
                "SELECT item_hash FROM messages \
                 WHERE type = 'PROGRAM' \
                   AND status = 'processed' \
                   AND jsonb_typeof(content -> 'metadata') = 'array'",
                &[],
            )
            .await?;
        rows.into_iter().map(|r| r.get::<_, String>(0)).collect()
    };

    if item_hashes.is_empty() {
        return Ok(());
    }

    tracing::info!(
        "Rejecting {} PROGRAM message(s) with non-dict metadata",
        item_hashes.len()
    );

    let mut rejected = 0usize;
    for item_hash in &item_hashes {
        let mut client = pool
            .get()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
        let tx = client
            .transaction()
            .await
            .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;

        let result: AlephResult<bool> = async {
            let message = match get_message_by_item_hash(&*tx, item_hash).await? {
                Some(m) => m,
                None => return Ok(false),
            };
            if message.status_value != MessageStatus::Processed {
                return Ok(false);
            }
            mark_processed_message_as_rejected(
                &*tx,
                &message,
                ErrorCode::InvalidFormat,
                INVALID_METADATA_REASON,
            )
            .await?;
            Ok(true)
        }
        .await;

        match result {
            Ok(true) => {
                tx.commit()
                    .await
                    .map_err(|e| AlephError::Internal(anyhow::anyhow!(e)))?;
                rejected += 1;
            }
            Ok(false) => {
                let _ = tx.rollback().await;
            }
            Err(err) => {
                tracing::error!("Failed to reject program {item_hash}: {err}");
                let _ = tx.rollback().await;
            }
        }
    }

    tracing::info!(
        "Done: rejected {} / {} PROGRAM message(s) with non-dict metadata",
        rejected,
        item_hashes.len()
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
    tracing::info!("Rejecting PROGRAM messages with invalid metadata");
    reject_invalid_program_metadata(pool).await?;
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
