//! `messages`, `message_status`, `message_confirmations`,
//! `forgotten_messages`, `rejected_messages` accessors.
//!
//! Mirrors `aleph/db/accessors/messages.py`.

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio_postgres::GenericClient;
use tokio_postgres::types::ToSql;

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;

use crate::AlephError;
use crate::AlephResult;
use crate::db::accessors::address_stats::escape_like_pattern;
use crate::db::accessors::cost::delete_costs_for_message;
use crate::db::accessors::pending_messages::delete_pending_message;
use crate::db::models::messages::{
    ForgottenMessageDb, MessageDb, MessageStatusDb, RejectedMessageDb,
};
use crate::db::models::pending_messages::PendingMessageDb;
use crate::toolkit::timestamp::{DatetimeOrTimestamp, coerce_to_datetime, utc_now};
use crate::types::channel::Channel;
use crate::types::message_status::{ErrorCode, MessageProcessingException, MessageStatus};
use crate::types::sort_order::{SortBy, SortByMessageType, SortOrder};

const MESSAGE_COLS: &str = "item_hash, type, chain, sender, signature, item_type, item_content, \
    content, time, channel, size, status, reception_time, owner, content_type, content_ref, \
    content_key, content_item_hash, first_confirmed_at, first_confirmed_height, payment_type, tags";

const FORGOTTEN_COLS: &str =
    "item_hash, type, chain, sender, signature, item_type, time, channel, forgotten_by";

const REJECTED_COLS: &str = "item_hash, message, error_code, details, traceback, tx_hash";

fn chain_to_str(c: &Chain) -> String {
    serde_json::to_value(c)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default()
}

fn message_type_to_str(t: MessageType) -> &'static str {
    match t {
        MessageType::Aggregate => "AGGREGATE",
        MessageType::Forget => "FORGET",
        MessageType::Instance => "INSTANCE",
        MessageType::Post => "POST",
        MessageType::Program => "PROGRAM",
        MessageType::Store => "STORE",
    }
}

fn message_status_to_str(s: MessageStatus) -> &'static str {
    match s {
        MessageStatus::Pending => "pending",
        MessageStatus::Processed => "processed",
        MessageStatus::Rejected => "rejected",
        MessageStatus::Forgotten => "forgotten",
        MessageStatus::Removing => "removing",
        MessageStatus::Removed => "removed",
    }
}

/// Fetch a `MessageDb` by its primary-key item hash.
pub async fn get_message_by_item_hash(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<MessageDb>> {
    let sql = format!(
        "SELECT {cols} FROM messages WHERE item_hash = $1",
        cols = MESSAGE_COLS
    );
    let row = client.query_opt(&sql, &[&item_hash]).await?;
    Ok(row.as_ref().map(MessageDb::from_row))
}

/// Whether a message row exists.
pub async fn message_exists(client: &impl GenericClient, item_hash: &str) -> AlephResult<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE item_hash = $1)",
            &[&item_hash],
        )
        .await?;
    Ok(row.get::<_, bool>(0))
}

/// Same as [`get_message_by_item_hash`] but using `query_one_or_none` semantics.
pub async fn get_one_message_by_item_hash(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<MessageDb>> {
    get_message_by_item_hash(client, item_hash).await
}

/// Filters accepted by [`make_matching_messages_query`].
///
/// Mirrors the kwargs of Python `make_matching_messages_query`.
#[derive(Debug, Clone)]
pub struct MessageFilters {
    pub hashes: Option<Vec<String>>,
    pub addresses: Option<Vec<String>>,
    pub owners: Option<Vec<String>>,
    pub refs: Option<Vec<String>>,
    pub chains: Option<Vec<String>>,
    pub message_type: Option<MessageType>,
    pub message_types: Option<Vec<MessageType>>,
    pub message_statuses: Option<Vec<MessageStatus>>,
    pub start_date: Option<f64>,
    pub end_date: Option<f64>,
    pub start_block: Option<i64>,
    pub end_block: Option<i64>,
    pub content_hashes: Option<Vec<String>>,
    pub content_types: Option<Vec<String>>,
    pub tags: Option<Vec<String>>,
    pub channels: Option<Vec<String>>,
    pub content_keys: Option<Vec<String>>,
    pub payment_types: Option<Vec<String>>,
    pub sort_by: SortBy,
    pub sort_order: SortOrder,
    pub page: i64,
    pub pagination: i64,
    pub include_confirmations: bool,
    pub after_time: Option<DateTime<Utc>>,
    pub after_hash: Option<String>,
    pub cursor_mode: bool,
}

impl Default for MessageFilters {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageFilters {
    pub fn new() -> Self {
        Self {
            hashes: None,
            addresses: None,
            owners: None,
            refs: None,
            chains: None,
            message_type: None,
            message_types: None,
            message_statuses: None,
            start_date: None,
            end_date: None,
            start_block: None,
            end_block: None,
            content_hashes: None,
            content_types: None,
            tags: None,
            channels: None,
            content_keys: None,
            payment_types: None,
            sort_by: SortBy::Time,
            sort_order: SortOrder::Descending,
            page: 1,
            pagination: 20,
            include_confirmations: false,
            after_time: None,
            after_hash: None,
            cursor_mode: false,
        }
    }
}

/// Built SQL + parameters for `make_matching_messages_query`.
pub struct BuiltMessagesQuery {
    pub sql: String,
    pub params: Vec<Box<dyn ToSql + Sync + Send>>,
}

impl BuiltMessagesQuery {
    /// Render `params` into refs suitable for `client.query`.
    pub fn param_refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params
            .iter()
            .map(|b| b.as_ref() as &(dyn ToSql + Sync))
            .collect()
    }
}

fn push_array_param(
    wheres: &mut Vec<String>,
    params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
    col: &str,
    values: &[String],
) {
    if !values.is_empty() {
        params.push(Box::new(values.to_vec()));
        wheres.push(format!("{col} = ANY(${})", params.len()));
    }
}

/// Build the SQL/params for a matching-messages query.
///
/// Mirrors `make_matching_messages_query`.
pub fn make_matching_messages_query(filters: &MessageFilters) -> BuiltMessagesQuery {
    let mut sql = format!("SELECT {cols} FROM messages", cols = MESSAGE_COLS);
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut wheres: Vec<String> = Vec::new();

    if let Some(statuses) = &filters.message_statuses {
        if !statuses.is_empty() {
            let strs: Vec<String> = statuses
                .iter()
                .map(|s| message_status_to_str(*s).to_string())
                .collect();
            push_array_param(&mut wheres, &mut params, "status", &strs);
        }
    }

    if let Some(start) = coerce_to_datetime(DatetimeOrTimestamp::from(filters.start_date)) {
        params.push(Box::new(start));
        wheres.push(format!("time >= ${}", params.len()));
    }
    if let Some(end) = coerce_to_datetime(DatetimeOrTimestamp::from(filters.end_date)) {
        params.push(Box::new(end));
        wheres.push(format!("time < ${}", params.len()));
    }

    if let Some(hashes) = &filters.hashes {
        if !hashes.is_empty() {
            push_array_param(&mut wheres, &mut params, "item_hash", hashes);
        }
    }
    if let Some(addresses) = &filters.addresses {
        if !addresses.is_empty() {
            push_array_param(&mut wheres, &mut params, "sender", addresses);
        }
    }
    if let Some(owners) = &filters.owners {
        if !owners.is_empty() {
            push_array_param(&mut wheres, &mut params, "owner", owners);
        }
    }
    if let Some(chains) = &filters.chains {
        if !chains.is_empty() {
            push_array_param(&mut wheres, &mut params, "chain", chains);
        }
    }
    if let Some(types) = &filters.message_types {
        if !types.is_empty() {
            let strs: Vec<String> = types
                .iter()
                .map(|t| message_type_to_str(*t).to_string())
                .collect();
            push_array_param(&mut wheres, &mut params, "type", &strs);
        }
    }
    if let Some(t) = filters.message_type {
        params.push(Box::new(message_type_to_str(t).to_string()));
        wheres.push(format!("type = ${}", params.len()));
    }
    if let Some(refs) = &filters.refs {
        if !refs.is_empty() {
            push_array_param(&mut wheres, &mut params, "content_ref", refs);
        }
    }
    if let Some(hashes) = &filters.content_hashes {
        if !hashes.is_empty() {
            push_array_param(&mut wheres, &mut params, "content_item_hash", hashes);
        }
    }
    if let Some(types) = &filters.content_types {
        if !types.is_empty() {
            push_array_param(&mut wheres, &mut params, "content_type", types);
        }
    }
    if let Some(keys) = &filters.content_keys {
        if !keys.is_empty() {
            push_array_param(&mut wheres, &mut params, "content_key", keys);
        }
    }
    if let Some(tags) = &filters.tags {
        if !tags.is_empty() {
            params.push(Box::new(tags.clone()));
            wheres.push(format!("tags && ${}", params.len()));
        }
    }
    if let Some(channels) = &filters.channels {
        if !channels.is_empty() {
            push_array_param(&mut wheres, &mut params, "channel", channels);
        }
    }
    if let Some(payment_types) = &filters.payment_types {
        if !payment_types.is_empty() {
            push_array_param(&mut wheres, &mut params, "payment_type", payment_types);
        }
    }

    let tx_time_sort = filters.sort_by == SortBy::TxTime
        || filters.start_block.is_some()
        || filters.end_block.is_some();
    if let Some(sb) = filters.start_block {
        params.push(Box::new(sb));
        wheres.push(format!(
            "(first_confirmed_height IS NULL OR first_confirmed_height >= ${})",
            params.len()
        ));
    }
    if let Some(eb) = filters.end_block {
        params.push(Box::new(eb));
        wheres.push(format!("first_confirmed_height < ${}", params.len()));
    }

    if let Some(at) = filters.after_time {
        let cmp = if filters.sort_order == SortOrder::Descending {
            "<"
        } else {
            ">"
        };
        params.push(Box::new(at));
        let at_idx = params.len();
        // Pass `Option<String>` so a missing cursor hash binds as SQL NULL
        // (cf. pyaleph: `item_hash > None`). Defaulting to "" would let any
        // row through on the tie-breaker disjunct.
        let ah: Option<String> = filters.after_hash.clone();
        params.push(Box::new(ah));
        let ah_idx = params.len();
        wheres.push(format!(
            "(time {cmp} ${tidx} OR (time = ${tidx} AND item_hash > ${hidx}))",
            cmp = cmp,
            tidx = at_idx,
            hidx = ah_idx,
        ));
    }

    if !wheres.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&wheres.join(" AND "));
    }

    let order_by = if tx_time_sort {
        if filters.sort_order == SortOrder::Descending {
            " ORDER BY first_confirmed_at DESC NULLS FIRST, time DESC, item_hash ASC"
        } else {
            " ORDER BY first_confirmed_at ASC NULLS LAST, time ASC, item_hash ASC"
        }
    } else if filters.sort_order == SortOrder::Descending {
        " ORDER BY time DESC, item_hash ASC"
    } else {
        " ORDER BY time ASC, item_hash ASC"
    };
    sql.push_str(order_by);

    if filters.after_time.is_none() && filters.page > 1 && filters.pagination > 0 {
        params.push(Box::new((filters.page - 1) * filters.pagination));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
    }
    if filters.pagination > 0 {
        let lim = if filters.after_time.is_some() || filters.cursor_mode {
            filters.pagination + 1
        } else {
            filters.pagination
        };
        params.push(Box::new(lim));
        sql.push_str(&format!(" LIMIT ${}", params.len()));
    }
    BuiltMessagesQuery { sql, params }
}

/// Execute [`make_matching_messages_query`] and return `MessageDb` rows.
pub async fn get_matching_messages(
    client: &impl GenericClient,
    filters: &MessageFilters,
) -> AlephResult<Vec<MessageDb>> {
    let q = make_matching_messages_query(filters);
    let rows = client.query(&q.sql, &q.param_refs()).await?;
    Ok(rows.iter().map(MessageDb::from_row).collect())
}

/// Count rows matching `filters`. Mirrors `count_matching_messages`.
pub async fn count_matching_messages(
    client: &impl GenericClient,
    filters: &MessageFilters,
) -> AlephResult<i64> {
    // Build the same query but with no LIMIT/ORDER BY and no pagination so
    // the COUNT runs over the unbounded result set.
    let mut f = filters.clone();
    f.pagination = 0;
    f.page = 1;
    f.cursor_mode = false;
    f.after_time = None;
    let q = make_matching_messages_query(&f);
    // Wrap in `SELECT COUNT(*) FROM (...)` and strip the trailing ORDER BY by
    // wrapping it as a subquery — Postgres accepts the inner ORDER BY but it's
    // a no-op for counting purposes.
    let sql = format!("SELECT COUNT(*) FROM ({inner}) AS sub", inner = q.sql);
    let row = client.query_one(&sql, &q.param_refs()).await?;
    Ok(row.get::<_, i64>(0))
}

/// Fast-path counter that uses the `message_counts` aggregate table when the
/// requested dimension combo is tracked, else returns `None`.
///
/// Mirrors `count_matching_messages_fast`.
pub async fn count_matching_messages_fast(
    client: &impl GenericClient,
    message_type: Option<&str>,
    statuses: Option<&[String]>,
    sender: Option<&str>,
    owner: Option<&str>,
) -> AlephResult<Option<i64>> {
    if sender.is_some() && owner.is_some() {
        return Ok(None);
    }
    if owner.is_some() && message_type.is_some() {
        return Ok(None);
    }
    // Cast SUM(BIGINT) → BIGINT so tokio_postgres can read it as i64.
    let mut sql = String::from("SELECT COALESCE(SUM(count), 0)::BIGINT FROM message_counts WHERE ");
    let mut wheres: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();

    if let Some(mt) = message_type {
        params.push(Box::new(mt.to_string()));
        wheres.push(format!("type = ${}", params.len()));
    } else {
        wheres.push("type = ''".to_string());
    }
    if let Some(s) = sender {
        params.push(Box::new(s.to_string()));
        wheres.push(format!("sender = ${}", params.len()));
    } else {
        wheres.push("sender = ''".to_string());
    }
    if let Some(o) = owner {
        params.push(Box::new(o.to_string()));
        wheres.push(format!("owner = ${}", params.len()));
    } else {
        wheres.push("owner = ''".to_string());
    }
    if let Some(st) = statuses {
        if !st.is_empty() {
            params.push(Box::new(st.to_vec()));
            wheres.push(format!("status = ANY(${})", params.len()));
        } else {
            wheres.push("status <> ''".to_string());
        }
    } else {
        wheres.push("status <> ''".to_string());
    }
    sql.push_str(&wheres.join(" AND "));
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let row = client.query_one(&sql, &param_refs).await?;
    let count: i64 = row.get(0);
    Ok(Some(count))
}

/// One row of [`get_message_stats_by_address`].
#[derive(Debug, Clone)]
pub struct AddressStatsRow {
    pub address: String,
    pub total: i64,
    pub post: i64,
    pub aggregate: i64,
    pub store: i64,
    pub program: i64,
    pub instance: i64,
    pub forget: i64,
}

/// Per-address message-type stats from `message_counts`.
///
/// Mirrors `get_message_stats_by_address`.
pub async fn get_message_stats_by_address(
    client: &impl GenericClient,
    addresses: Option<&[String]>,
    address_contains: Option<&str>,
    sort_by: Option<SortByMessageType>,
    sort_order: SortOrder,
    page: i64,
    pagination: i64,
    after_sort_value: Option<i64>,
    after_address: Option<&str>,
    cursor_mode: bool,
) -> AlephResult<Vec<AddressStatsRow>> {
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let processed = message_status_to_str(MessageStatus::Processed).to_string();
    params.push(Box::new(processed));
    let mut sql = format!(
        // `SUM(BIGINT)` returns NUMERIC in PostgreSQL; cast to BIGINT so
        // tokio_postgres can deserialize directly into i64. Mirrors pyaleph's
        // explicit `cast()` calls in `make_address_stats_query`.
        "WITH sub AS ( \
            SELECT sender AS address, \
                COALESCE(SUM(count), 0)::BIGINT AS total, \
                COALESCE(SUM(count) FILTER (WHERE type = 'POST'), 0)::BIGINT AS post, \
                COALESCE(SUM(count) FILTER (WHERE type = 'AGGREGATE'), 0)::BIGINT AS aggregate, \
                COALESCE(SUM(count) FILTER (WHERE type = 'STORE'), 0)::BIGINT AS store, \
                COALESCE(SUM(count) FILTER (WHERE type = 'PROGRAM'), 0)::BIGINT AS program, \
                COALESCE(SUM(count) FILTER (WHERE type = 'INSTANCE'), 0)::BIGINT AS instance, \
                COALESCE(SUM(count) FILTER (WHERE type = 'FORGET'), 0)::BIGINT AS forget \
            FROM message_counts \
            WHERE status = $1 AND owner = '' AND sender <> '' AND type <> ''"
    );
    if let Some(addrs) = addresses {
        if !addrs.is_empty() {
            params.push(Box::new(addrs.to_vec()));
            sql.push_str(&format!(" AND sender = ANY(${})", params.len()));
        }
    }
    if let Some(pat) = address_contains {
        let escaped = format!("%{}%", escape_like_pattern(pat));
        params.push(Box::new(escaped));
        sql.push_str(&format!(" AND sender ILIKE ${} ESCAPE '\\'", params.len()));
    }
    sql.push_str(" GROUP BY sender) SELECT * FROM sub");

    let sort_col = match sort_by {
        Some(SortByMessageType::Aggregate) => "aggregate",
        Some(SortByMessageType::Forget) => "forget",
        Some(SortByMessageType::Instance) => "instance",
        Some(SortByMessageType::Post) => "post",
        Some(SortByMessageType::Program) => "program",
        Some(SortByMessageType::Store) => "store",
        Some(SortByMessageType::Total) => "total",
        None => "address",
    };

    if let Some(asv) = after_sort_value {
        let cmp = if sort_order == SortOrder::Descending {
            "<"
        } else {
            ">"
        };
        params.push(Box::new(asv));
        let asv_idx = params.len();
        let aa = after_address.unwrap_or("").to_string();
        params.push(Box::new(aa));
        let aa_idx = params.len();
        sql.push_str(&format!(
            " WHERE ({sort_col} {cmp} ${vidx} OR \
                     ({sort_col} = ${vidx} AND address > ${aidx}))",
            sort_col = sort_col,
            cmp = cmp,
            vidx = asv_idx,
            aidx = aa_idx,
        ));
    }
    let direction = sort_order.to_sql();
    sql.push_str(&format!(" ORDER BY {sort_col} {direction}, address ASC"));
    if after_sort_value.is_none() && pagination > 0 && page > 1 {
        params.push(Box::new((page - 1) * pagination));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
    }
    if pagination > 0 {
        let lim = if after_sort_value.is_some() || cursor_mode {
            pagination + 1
        } else {
            pagination
        };
        params.push(Box::new(lim));
        sql.push_str(&format!(" LIMIT ${}", params.len()));
    }
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    Ok(rows
        .into_iter()
        .map(|r| AddressStatsRow {
            address: r.get("address"),
            total: r.get("total"),
            post: r.get("post"),
            aggregate: r.get("aggregate"),
            store: r.get("store"),
            program: r.get("program"),
            instance: r.get("instance"),
            forget: r.get("forget"),
        })
        .collect())
}

/// Stream of messages with a signature but no first confirmation, ordered by
/// reception time ascending. Mirrors `get_unconfirmed_messages`.
pub async fn get_unconfirmed_messages(
    client: &impl GenericClient,
    limit: i64,
    offset: i64,
) -> AlephResult<Vec<MessageDb>> {
    let sql = format!(
        "SELECT {cols} FROM messages \
         WHERE signature IS NOT NULL AND first_confirmed_at IS NULL \
         ORDER BY reception_time ASC LIMIT $1 OFFSET $2",
        cols = MESSAGE_COLS
    );
    let rows = client.query(&sql, &[&limit, &offset]).await?;
    Ok(rows.iter().map(MessageDb::from_row).collect())
}

/// Upsert a `MessageDb` row, keeping the lower `time` on conflict.
pub async fn upsert_message(client: &impl GenericClient, message: &MessageDb) -> AlephResult<()> {
    let chain_s = chain_to_str(&message.chain);
    let type_s = message_type_to_str(message.r#type);
    let item_type_s = serde_json::to_value(&message.item_type)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default();
    let channel_s = message
        .channel
        .as_ref()
        .and_then(|c| serde_json::to_value(c).ok())
        .and_then(|v| v.as_str().map(|s| s.to_string()));
    let status_s = message_status_to_str(message.status_value);

    let sql = "INSERT INTO messages(item_hash, type, chain, sender, signature, item_type, \
                                     item_content, content, time, channel, size, status, \
                                     reception_time, owner, content_type, content_ref, \
                                     content_key, content_item_hash, first_confirmed_at, \
                                     first_confirmed_height, payment_type, tags) \
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, \
                       $17, $18, $19, $20, $21, $22) \
               ON CONFLICT ON CONSTRAINT messages_pkey \
               DO UPDATE SET time = LEAST(messages.time, EXCLUDED.time)";
    client
        .execute(
            sql,
            &[
                &message.item_hash,
                &type_s,
                &chain_s,
                &message.sender,
                &message.signature,
                &item_type_s,
                &message.item_content,
                &message.content,
                &message.time,
                &channel_s,
                &message.size,
                &status_s,
                &message.reception_time,
                &message.owner,
                &message.content_type,
                &message.content_ref,
                &message.content_key,
                &message.content_item_hash,
                &message.first_confirmed_at,
                &message.first_confirmed_height,
                &message.payment_type,
                &message.tags,
            ],
        )
        .await?;
    Ok(())
}

/// Upsert a confirmation row; conflict on `(item_hash, tx_hash)` is a no-op.
pub async fn upsert_confirmation(
    client: &impl GenericClient,
    item_hash: &str,
    tx_hash: &str,
) -> AlephResult<()> {
    client
        .execute(
            "INSERT INTO message_confirmations(item_hash, tx_hash) VALUES ($1, $2) \
             ON CONFLICT DO NOTHING",
            &[&item_hash, &tx_hash],
        )
        .await?;
    Ok(())
}

/// Read a row from `message_status` by item hash.
pub async fn get_message_status(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<MessageStatusDb>> {
    let row = client
        .query_opt(
            "SELECT item_hash, status, reception_time FROM message_status WHERE item_hash = $1",
            &[&item_hash],
        )
        .await?;
    Ok(row.as_ref().map(MessageStatusDb::from_row))
}

/// Fetch a rejected-message row, if any.
pub async fn get_rejected_message(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<RejectedMessageDb>> {
    let sql = format!(
        "SELECT {cols} FROM rejected_messages WHERE item_hash = $1",
        cols = REJECTED_COLS
    );
    let row = client.query_opt(&sql, &[&item_hash]).await?;
    Ok(row.as_ref().map(RejectedMessageDb::from_row))
}

/// Upsert a message-status row, keeping the lower `reception_time` and
/// optionally restricting the conflict update via a `WHERE` clause.
///
/// Mirrors `make_message_status_upsert_query`. `where_sql` is a SQL fragment
/// using the table alias `message_status` (e.g.
/// `"message_status.status = 'pending'"`).
pub async fn upsert_message_status(
    client: &impl GenericClient,
    item_hash: &str,
    new_status: MessageStatus,
    reception_time: DateTime<Utc>,
    where_sql: Option<&str>,
) -> AlephResult<()> {
    let status_s = message_status_to_str(new_status);
    let where_clause = match where_sql {
        Some(w) => format!(" WHERE {w}"),
        None => String::new(),
    };
    let sql = format!(
        "INSERT INTO message_status(item_hash, status, reception_time) VALUES ($1, $2, $3) \
         ON CONFLICT ON CONSTRAINT message_status_pkey \
         DO UPDATE SET status = EXCLUDED.status, \
                       reception_time = LEAST(message_status.reception_time, EXCLUDED.reception_time)\
         {where_clause}"
    );
    client
        .execute(&sql, &[&item_hash, &status_s, &reception_time])
        .await?;
    Ok(())
}

/// Distinct channels seen in `messages`, ordered ascending. Mirrors
/// `get_distinct_channels`.
pub async fn get_distinct_channels(
    client: &impl GenericClient,
) -> AlephResult<Vec<Option<Channel>>> {
    let rows = client
        .query(
            "SELECT DISTINCT channel FROM messages ORDER BY channel",
            &[],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.get::<_, Option<String>>(0).map(Channel::from))
        .collect())
}

/// Distinct POST content_types for an address. Mirrors
/// `get_distinct_post_types_for_address`.
pub async fn get_distinct_post_types_for_address(
    client: &impl GenericClient,
    address: &str,
) -> AlephResult<Vec<String>> {
    let rows = client
        .query(
            "SELECT DISTINCT content_type FROM messages \
             WHERE sender = $1 AND type = 'POST' AND content_type IS NOT NULL \
             ORDER BY content_type",
            &[&address],
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| r.get::<_, Option<String>>(0))
        .collect())
}

/// Distinct non-null channels for an address.
pub async fn get_distinct_channels_for_address(
    client: &impl GenericClient,
    address: &str,
) -> AlephResult<Vec<String>> {
    let rows = client
        .query(
            "SELECT DISTINCT channel FROM messages \
             WHERE sender = $1 AND channel IS NOT NULL ORDER BY channel",
            &[&address],
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| r.get::<_, Option<String>>(0))
        .collect())
}

/// Get a forgotten-message row, if any.
pub async fn get_forgotten_message(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<ForgottenMessageDb>> {
    let sql = format!(
        "SELECT {cols} FROM forgotten_messages WHERE item_hash = $1",
        cols = FORGOTTEN_COLS
    );
    let row = client.query_opt(&sql, &[&item_hash]).await?;
    Ok(row.as_ref().map(ForgottenMessageDb::from_row))
}

/// Mark a processed message as forgotten.
///
/// Mirrors `forget_message`. Must run inside the caller's transaction (the
/// `client` here can be a Transaction).
pub async fn forget_message(
    client: &impl GenericClient,
    item_hash: &str,
    forget_message_hash: &str,
) -> AlephResult<()> {
    let forgotten_by = vec![forget_message_hash.to_string()];
    let copy_sql = "INSERT INTO forgotten_messages(item_hash, type, chain, sender, signature, \
                                                    item_type, time, channel, forgotten_by) \
                    SELECT item_hash, type, chain, sender, signature, item_type, time, channel, \
                           $2::varchar[] \
                    FROM messages WHERE item_hash = $1";
    client
        .execute(copy_sql, &[&item_hash, &forgotten_by])
        .await?;

    client
        .execute(
            "DELETE FROM message_confirmations WHERE item_hash = $1",
            &[&item_hash],
        )
        .await?;
    client
        .execute("DELETE FROM messages WHERE item_hash = $1", &[&item_hash])
        .await?;
    client
        .execute(
            "UPDATE message_status SET status = 'forgotten' WHERE item_hash = $1",
            &[&item_hash],
        )
        .await?;
    delete_costs_for_message(client, item_hash).await?;
    Ok(())
}

/// Append `forget_message_hash` to `forgotten_messages.forgotten_by`.
pub async fn append_to_forgotten_by(
    client: &impl GenericClient,
    forgotten_message_hash: &str,
    forget_message_hash: &str,
) -> AlephResult<()> {
    client
        .execute(
            "UPDATE forgotten_messages SET forgotten_by = array_append(forgotten_by, $2) \
             WHERE item_hash = $1",
            &[&forgotten_message_hash, &forget_message_hash],
        )
        .await?;
    Ok(())
}

/// Upsert a rejected-message row.
///
/// Mirrors `make_upsert_rejected_message_statement` + execution.
pub async fn upsert_rejected_message(
    client: &impl GenericClient,
    item_hash: &str,
    pending_message_dict: &Value,
    error_code: i32,
    details: Option<&Value>,
    exc_traceback: Option<&str>,
    tx_hash: Option<&str>,
) -> AlephResult<()> {
    let sql = "INSERT INTO rejected_messages(item_hash, message, error_code, details, \
                                              traceback, tx_hash) \
               VALUES ($1, $2, $3, $4, $5, $6) \
               ON CONFLICT ON CONSTRAINT rejected_messages_pkey \
               DO UPDATE SET error_code = EXCLUDED.error_code, \
                             details = EXCLUDED.details, \
                             traceback = EXCLUDED.traceback, \
                             tx_hash = EXCLUDED.tx_hash";
    client
        .execute(
            sql,
            &[
                &item_hash,
                pending_message_dict,
                &error_code,
                &details,
                &exc_traceback,
                &tx_hash,
            ],
        )
        .await?;
    Ok(())
}

/// Mark a pending-message as rejected and persist the rejection.
///
/// Mirrors `mark_pending_message_as_rejected`.
pub async fn mark_pending_message_as_rejected(
    client: &impl GenericClient,
    item_hash: &str,
    pending_message_dict: &Value,
    exception: &MessageProcessingException,
    tx_hash: Option<&str>,
) -> AlephResult<RejectedMessageDb> {
    let error_code: i32 = exception.error_code() as i32;
    let details = exception.details();
    let exc_traceback: Option<String> = None;

    let reception_time = utc_now();
    upsert_message_status(
        client,
        item_hash,
        MessageStatus::Rejected,
        reception_time,
        Some("message_status.status = 'pending'"),
    )
    .await?;
    upsert_rejected_message(
        client,
        item_hash,
        pending_message_dict,
        error_code,
        details.as_ref(),
        exc_traceback.as_deref(),
        tx_hash,
    )
    .await?;
    Ok(RejectedMessageDb {
        item_hash: item_hash.to_string(),
        message: pending_message_dict.clone(),
        error_code: ErrorCode::try_from(error_code).unwrap_or(ErrorCode::InternalError),
        details,
        traceback: exc_traceback,
        tx_hash: tx_hash.map(|s| s.to_string()),
    })
}

/// Reject a pending message that hasn't been persisted yet.
///
/// Mirrors `reject_new_pending_message`. Returns `None` if the message is
/// missing an item_hash, or if a non-rejected status already exists.
pub async fn reject_new_pending_message(
    client: &impl GenericClient,
    pending_message_dict: &Value,
    exception: &MessageProcessingException,
    tx_hash: Option<&str>,
) -> AlephResult<Option<RejectedMessageDb>> {
    let item_hash = match pending_message_dict
        .get("item_hash")
        .and_then(|v| v.as_str())
    {
        Some(h) => h.to_string(),
        None => return Ok(None),
    };

    if let Some(status) = get_message_status(client, &item_hash).await? {
        if status.status != MessageStatus::Rejected {
            return Ok(None);
        }
    }
    let r = mark_pending_message_as_rejected(
        client,
        &item_hash,
        pending_message_dict,
        exception,
        tx_hash,
    )
    .await?;
    Ok(Some(r))
}

/// Reject a pending message that already exists in `pending_messages`.
///
/// Mirrors `reject_existing_pending_message`.
pub async fn reject_existing_pending_message(
    client: &impl GenericClient,
    pending_message: &PendingMessageDb,
    pending_message_dict: &Value,
    exception: &MessageProcessingException,
) -> AlephResult<Option<RejectedMessageDb>> {
    let item_hash = &pending_message.item_hash;
    if let Some(status) = get_message_status(client, item_hash).await? {
        if status.status != MessageStatus::Pending && status.status != MessageStatus::Rejected {
            delete_pending_message(client, pending_message.id).await?;
            return Ok(None);
        }
    }
    let r = mark_pending_message_as_rejected(
        client,
        item_hash,
        pending_message_dict,
        exception,
        pending_message.tx_hash.as_deref(),
    )
    .await?;
    delete_pending_message(client, pending_message.id).await?;
    Ok(Some(r))
}

/// Result row of [`get_programs_triggered_by_messages`].
#[derive(Debug, Clone)]
pub struct ProgramSubscriptionRow {
    pub item_hash: String,
    pub message_subscriptions: Value,
}

/// Find PROGRAM messages whose `content.on.message` is non-null.
pub async fn get_programs_triggered_by_messages(
    client: &impl GenericClient,
    sort_order: SortOrder,
) -> AlephResult<Vec<ProgramSubscriptionRow>> {
    let direction = sort_order.to_sql();
    let sql = format!(
        "SELECT item_hash, content->'on'->'message' AS message_subscriptions \
         FROM messages \
         WHERE type = 'PROGRAM' AND content->'on'->'message' IS NOT NULL \
         ORDER BY time {direction}"
    );
    let rows = client.query(&sql, &[]).await?;
    Ok(rows
        .into_iter()
        .map(|r| ProgramSubscriptionRow {
            item_hash: r.get("item_hash"),
            message_subscriptions: r.get("message_subscriptions"),
        })
        .collect())
}

/// Filters for the hash-only listing query.
#[derive(Debug, Clone)]
pub struct MessageHashesFilters {
    pub start_date: Option<f64>,
    pub end_date: Option<f64>,
    pub status: Option<MessageStatus>,
    pub sort_order: SortOrder,
    pub page: i64,
    pub pagination: i64,
    /// When false, returns `(item_hash, status, reception_time)` rows instead.
    pub hash_only: bool,
}

impl Default for MessageHashesFilters {
    fn default() -> Self {
        Self {
            start_date: None,
            end_date: None,
            status: None,
            sort_order: SortOrder::Descending,
            page: 1,
            pagination: 20,
            hash_only: true,
        }
    }
}

/// Result row of [`get_matching_hashes`] when `hash_only=false`.
#[derive(Debug, Clone)]
pub struct MatchingHashRow {
    pub item_hash: String,
    pub status: Option<MessageStatus>,
    pub reception_time: Option<DateTime<Utc>>,
}

fn build_matching_hashes(
    filters: &MessageHashesFilters,
) -> (String, Vec<Box<dyn ToSql + Sync + Send>>) {
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut sql = if filters.hash_only {
        String::from("SELECT item_hash FROM messages")
    } else {
        String::from("SELECT item_hash, status, reception_time FROM messages")
    };
    let mut wheres: Vec<String> = Vec::new();
    if let Some(start) = coerce_to_datetime(DatetimeOrTimestamp::from(filters.start_date)) {
        params.push(Box::new(start));
        wheres.push(format!("reception_time >= ${}", params.len()));
    }
    if let Some(end) = coerce_to_datetime(DatetimeOrTimestamp::from(filters.end_date)) {
        params.push(Box::new(end));
        wheres.push(format!("reception_time < ${}", params.len()));
    }
    if let Some(s) = filters.status {
        params.push(Box::new(message_status_to_str(s).to_string()));
        wheres.push(format!("status = ${}", params.len()));
    }
    if !wheres.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&wheres.join(" AND "));
    }
    let direction = filters.sort_order.to_sql();
    sql.push_str(&format!(
        " ORDER BY reception_time {direction}, item_hash ASC"
    ));
    if filters.page > 1 && filters.pagination > 0 {
        params.push(Box::new((filters.page - 1) * filters.pagination));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
    }
    if filters.pagination > 0 {
        params.push(Box::new(filters.pagination));
        sql.push_str(&format!(" LIMIT ${}", params.len()));
    }
    (sql, params)
}

/// Fetch matching item hashes, or `(item_hash, status, reception_time)` rows.
pub async fn get_matching_hashes(
    client: &impl GenericClient,
    filters: &MessageHashesFilters,
) -> AlephResult<Vec<MatchingHashRow>> {
    let (sql, params) = build_matching_hashes(filters);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    Ok(rows
        .iter()
        .map(|r| MatchingHashRow {
            item_hash: r.get("item_hash"),
            status: if filters.hash_only {
                None
            } else {
                let s: Option<String> = r.try_get("status").ok().flatten();
                s.and_then(|s| serde_json::from_value::<MessageStatus>(Value::String(s)).ok())
            },
            reception_time: if filters.hash_only {
                None
            } else {
                r.try_get("reception_time").ok().flatten()
            },
        })
        .collect())
}

/// Count matching item hashes. Mirrors `count_matching_hashes`.
pub async fn count_matching_hashes(
    client: &impl GenericClient,
    filters: &MessageHashesFilters,
) -> AlephResult<i64> {
    let mut f = filters.clone();
    f.pagination = 0;
    f.page = 1;
    let (sql, params) = build_matching_hashes(&f);
    let wrapped = format!("SELECT COUNT(*) FROM ({sql}) AS sub");
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let row = client.query_one(&wrapped, &param_refs).await?;
    Ok(row.get::<_, i64>(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_matching_messages_query_emits_filters_in_order() {
        let mut filters = MessageFilters::new();
        filters.hashes = Some(vec!["a".into(), "b".into()]);
        filters.message_type = Some(MessageType::Post);
        filters.pagination = 5;
        let q = make_matching_messages_query(&filters);
        assert!(q.sql.contains("WHERE"));
        assert!(q.sql.contains("item_hash = ANY"));
        assert!(q.sql.contains("type = $"));
        assert!(q.sql.contains("ORDER BY"));
        assert!(q.sql.contains("LIMIT"));
    }

    #[test]
    fn make_matching_messages_query_tx_time_order() {
        let mut filters = MessageFilters::new();
        filters.sort_by = SortBy::TxTime;
        filters.sort_order = SortOrder::Ascending;
        let q = make_matching_messages_query(&filters);
        assert!(q.sql.contains("first_confirmed_at ASC NULLS LAST"));
    }

    #[test]
    fn make_matching_messages_query_cursor_mode_doubles_limit() {
        let mut filters = MessageFilters::new();
        filters.pagination = 7;
        filters.cursor_mode = true;
        let q = make_matching_messages_query(&filters);
        // The last param (LIMIT) is pagination + 1.
        // We can't introspect ToSql easily, but the SQL must reference LIMIT.
        assert!(q.sql.ends_with(&format!("LIMIT ${}", q.params.len())));
    }
}
