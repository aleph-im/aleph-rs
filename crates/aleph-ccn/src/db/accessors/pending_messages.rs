//! `pending_messages` accessors. Mirrors `aleph/db/accessors/pending_messages.py`.

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio_postgres::GenericClient;

use aleph_types::chain::Chain;

use crate::AlephResult;
use crate::db::models::pending_messages::PendingMessageDb;

/// Common SELECT clause for `pending_messages`.
const PENDING_MESSAGE_COLS: &str = "id, item_hash, type, chain, sender, signature, item_type, \
     item_content, content, time, channel, reception_time, check_message, \
     next_attempt, retries, tx_hash, fetched, origin";

/// Insert a `PendingMessageDb` row. Returns the assigned `id`.
///
/// Mirrors the Python `session.add(pending_message)` flow.
pub async fn insert_pending_message(
    client: &impl GenericClient,
    pending: &PendingMessageDb,
) -> AlephResult<i64> {
    let chain = chain_to_str(&pending.chain);
    let r#type = serde_json::to_value(&pending.r#type)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default();
    let item_type = serde_json::to_value(&pending.item_type)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default();
    let channel: Option<String> = pending
        .channel
        .as_ref()
        .and_then(|c| serde_json::to_value(c).ok())
        .and_then(|v| v.as_str().map(|s| s.to_string()));
    let row = client
        .query_one(
            "INSERT INTO pending_messages (\
                item_hash, type, chain, sender, signature, item_type, item_content, \
                content, time, channel, reception_time, check_message, next_attempt, \
                retries, tx_hash, fetched, origin) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) \
             RETURNING id",
            &[
                &pending.item_hash,
                &r#type,
                &chain,
                &pending.sender,
                &pending.signature,
                &item_type,
                &pending.item_content,
                &pending.content,
                &pending.time,
                &channel,
                &pending.reception_time,
                &pending.check_message,
                &pending.next_attempt,
                &pending.retries,
                &pending.tx_hash,
                &pending.fetched,
                &pending.origin,
            ],
        )
        .await?;
    Ok(row.get(0))
}

fn chain_to_str(c: &Chain) -> String {
    serde_json::to_value(c)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default()
}

/// Return the next pending message ready to process (or None).
///
/// Mirrors `get_next_pending_message`.
pub async fn get_next_pending_message(
    client: &impl GenericClient,
    current_time: DateTime<Utc>,
    offset: i64,
    fetched: Option<bool>,
    exclude_item_hashes: Option<&[String]>,
) -> AlephResult<Option<PendingMessageDb>> {
    let mut sql = format!(
        "SELECT {cols} FROM pending_messages WHERE next_attempt <= $1",
        cols = PENDING_MESSAGE_COLS
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(current_time)];

    if let Some(f) = fetched {
        params.push(Box::new(f));
        sql.push_str(&format!(" AND fetched = ${}", params.len()));
    }
    if let Some(hashes) = exclude_item_hashes {
        if !hashes.is_empty() {
            params.push(Box::new(hashes.to_vec()));
            sql.push_str(&format!(" AND item_hash <> ALL(${})", params.len()));
        }
    }
    sql.push_str(" ORDER BY next_attempt ASC OFFSET ");
    params.push(Box::new(offset));
    sql.push_str(&format!("${}", params.len()));
    sql.push_str(" LIMIT 1 FOR UPDATE SKIP LOCKED");

    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let row = client.query_opt(&sql, &param_refs).await?;
    row.as_ref().map(PendingMessageDb::try_from_row).transpose()
}

/// Return up to `limit` pending messages ready to process.
pub async fn get_next_pending_messages(
    client: &impl GenericClient,
    current_time: DateTime<Utc>,
    limit: i64,
    offset: i64,
    fetched: Option<bool>,
    exclude_item_hashes: Option<&[String]>,
) -> AlephResult<Vec<PendingMessageDb>> {
    let mut sql = format!(
        "SELECT {cols} FROM pending_messages WHERE next_attempt <= $1",
        cols = PENDING_MESSAGE_COLS
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(current_time)];
    if let Some(f) = fetched {
        params.push(Box::new(f));
        sql.push_str(&format!(" AND fetched = ${}", params.len()));
    }
    if let Some(hashes) = exclude_item_hashes {
        if !hashes.is_empty() {
            params.push(Box::new(hashes.to_vec()));
            sql.push_str(&format!(" AND item_hash <> ALL(${})", params.len()));
        }
    }
    sql.push_str(" ORDER BY next_attempt ASC OFFSET ");
    params.push(Box::new(offset));
    sql.push_str(&format!("${}", params.len()));
    sql.push_str(" LIMIT ");
    params.push(Box::new(limit));
    sql.push_str(&format!("${}", params.len()));
    sql.push_str(" FOR UPDATE SKIP LOCKED");

    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    rows.iter().map(PendingMessageDb::try_from_row).collect()
}

/// Atomically claim up to `limit` pending messages by pushing their
/// `next_attempt` into the future. This prevents other workers from fetching
/// the same non-inline content concurrently while still allowing recovery if
/// the worker crashes before it marks the row as fetched.
pub async fn claim_next_pending_messages(
    client: &impl GenericClient,
    current_time: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: i64,
    fetched: Option<bool>,
    exclude_item_hashes: Option<&[String]>,
) -> AlephResult<Vec<PendingMessageDb>> {
    if limit <= 0 {
        return Ok(Vec::new());
    }

    let mut where_sql = String::from("next_attempt <= $1");
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(current_time)];

    if let Some(f) = fetched {
        params.push(Box::new(f));
        where_sql.push_str(&format!(" AND fetched = ${}", params.len()));
    }
    if let Some(hashes) = exclude_item_hashes {
        if !hashes.is_empty() {
            params.push(Box::new(hashes.to_vec()));
            where_sql.push_str(&format!(" AND item_hash <> ALL(${})", params.len()));
        }
    }

    params.push(Box::new(limit));
    let limit_param = params.len();
    params.push(Box::new(lease_until));
    let lease_param = params.len();

    let sql = format!(
        "WITH claimed AS (\
             SELECT id FROM pending_messages \
             WHERE {where_sql} \
             ORDER BY next_attempt ASC \
             LIMIT ${limit_param} \
             FOR UPDATE SKIP LOCKED\
         ) \
         UPDATE pending_messages pm \
         SET next_attempt = ${lease_param} \
         FROM claimed \
         WHERE pm.id = claimed.id \
         RETURNING {cols}",
        cols = PENDING_MESSAGE_COLS
    );

    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    rows.iter().map(PendingMessageDb::try_from_row).collect()
}

/// All pending messages with a given item hash, ordered by `time` ascending.
pub async fn get_pending_messages(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Vec<PendingMessageDb>> {
    let sql = format!(
        "SELECT {cols} FROM pending_messages \
         WHERE item_hash = $1 ORDER BY time ASC",
        cols = PENDING_MESSAGE_COLS
    );
    let rows = client.query(&sql, &[&item_hash]).await?;
    rows.iter().map(PendingMessageDb::try_from_row).collect()
}

/// Fetch a pending message by its primary-key id.
pub async fn get_pending_message(
    client: &impl GenericClient,
    pending_message_id: i64,
) -> AlephResult<Option<PendingMessageDb>> {
    let sql = format!(
        "SELECT {cols} FROM pending_messages WHERE id = $1",
        cols = PENDING_MESSAGE_COLS
    );
    let row = client.query_opt(&sql, &[&pending_message_id]).await?;
    row.as_ref().map(PendingMessageDb::try_from_row).transpose()
}

/// Count pending messages, optionally filtered by chain (via chain_txs).
pub async fn count_pending_messages(
    client: &impl GenericClient,
    chain: Option<Chain>,
) -> AlephResult<i64> {
    let row = match chain {
        Some(c) => {
            let chain_s = chain_to_str(&c);
            client
                .query_one(
                    "SELECT COUNT(pm.id) FROM pending_messages pm \
                     JOIN chain_txs ct ON pm.tx_hash = ct.hash \
                     WHERE ct.chain = $1",
                    &[&chain_s],
                )
                .await?
        }
        None => {
            client
                .query_one("SELECT COUNT(id) FROM pending_messages", &[])
                .await?
        }
    };
    Ok(row.get::<_, i64>(0))
}

/// Mark a pending message as fetched and replace its `content`/`retries`.
///
/// Mirrors `make_pending_message_fetched_statement` (executes inline rather
/// than returning the statement — Python kept the statement around to dispatch
/// it via `session.execute(...)` later; in Rust we directly run it).
pub async fn set_pending_message_fetched(
    client: &impl GenericClient,
    pending_message_id: i64,
    content: &Value,
) -> AlephResult<()> {
    client
        .execute(
            "UPDATE pending_messages \
             SET fetched = TRUE, content = $1, retries = 0, next_attempt = NOW() \
             WHERE id = $2",
            &[content, &pending_message_id],
        )
        .await?;
    Ok(())
}

/// Increment retries and schedule the next attempt.
pub async fn set_next_retry(
    client: &impl GenericClient,
    pending_message_id: i64,
    next_attempt: DateTime<Utc>,
) -> AlephResult<()> {
    client
        .execute(
            "UPDATE pending_messages \
             SET retries = retries + 1, next_attempt = $1 WHERE id = $2",
            &[&next_attempt, &pending_message_id],
        )
        .await?;
    Ok(())
}

/// Delete a pending message by its primary-key id.
pub async fn delete_pending_message(
    client: &impl GenericClient,
    pending_message_id: i64,
) -> AlephResult<()> {
    client
        .execute(
            "DELETE FROM pending_messages WHERE id = $1",
            &[&pending_message_id],
        )
        .await?;
    Ok(())
}
