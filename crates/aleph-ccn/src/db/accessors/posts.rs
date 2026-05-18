//! `posts` accessors. Mirrors `aleph/db/accessors/posts.py`.

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio_postgres::GenericClient;

use aleph_types::chain::Chain;
use aleph_types::message::item_type::ItemType;

use crate::AlephResult;
use crate::db::models::posts::PostDb;
use crate::toolkit::timestamp::{DatetimeOrTimestamp, coerce_to_datetime};
use crate::types::channel::Channel;
use crate::types::sort_order::{SortBy, SortOrder};

const POST_COLS: &str =
    "item_hash, owner, type, ref, amends, channel, content, creation_datetime, latest_amend, tags";

/// Merged-post row returned by the v1 /posts/ endpoint. Mirrors Python
/// `MergedPost` protocol.
#[derive(Debug, Clone)]
pub struct MergedPost {
    pub item_hash: String,
    pub original_item_hash: String,
    pub content: Value,
    pub owner: String,
    pub r#ref: Option<String>,
    pub channel: Option<Channel>,
    pub last_updated: DateTime<Utc>,
    pub created: DateTime<Utc>,
    pub original_type: Option<String>,
    pub tags: Option<Vec<String>>,
}

/// Merged-post row returned by the v0 /posts/ endpoint. Mirrors Python
/// `MergedPostV0` protocol.
#[derive(Debug, Clone)]
pub struct MergedPostV0 {
    pub chain: Chain,
    pub item_hash: String,
    pub content: Value,
    pub r#type: Option<String>,
    pub item_type: ItemType,
    pub item_content: Option<String>,
    pub original_item_hash: String,
    pub original_type: Option<String>,
    pub owner: String,
    pub r#ref: Option<String>,
    pub channel: Option<Channel>,
    pub signature: Option<String>,
    pub original_signature: Option<String>,
    pub time: f64,
    pub size: i32,
    pub last_updated: DateTime<Utc>,
    pub tags: Option<Vec<String>>,
}

/// Sub-select that yields originals joined with their latest amend
/// (v1 shape).
const MERGED_POST_BASE: &str = "SELECT \
    o.item_hash AS original_item_hash, \
    COALESCE(a.item_hash, o.item_hash) AS item_hash, \
    COALESCE(a.content, o.content) AS content, \
    o.owner AS owner, \
    o.ref AS ref, \
    COALESCE(a.creation_datetime, o.creation_datetime) AS last_updated, \
    o.channel AS channel, \
    o.creation_datetime AS created, \
    o.type AS original_type, \
    COALESCE(a.tags, o.tags) AS tags \
    FROM posts o LEFT OUTER JOIN posts a ON o.latest_amend = a.item_hash \
    WHERE o.amends IS NULL";

/// V0 sub-select with extra columns (latest_amend, amend-aware type) used to
/// later join the `messages` table.
const MERGED_POST_V0_BASE: &str = "SELECT \
    o.item_hash AS original_item_hash, \
    COALESCE(a.item_hash, o.item_hash) AS item_hash, \
    o.latest_amend AS latest_amend, \
    COALESCE(a.content, o.content) AS content, \
    o.owner AS owner, \
    o.ref AS ref, \
    COALESCE(a.creation_datetime, o.creation_datetime) AS last_updated, \
    o.channel AS channel, \
    o.creation_datetime AS created, \
    COALESCE(a.type, o.type) AS type, \
    o.type AS original_type, \
    COALESCE(a.tags, o.tags) AS tags \
    FROM posts o LEFT OUTER JOIN posts a ON o.latest_amend = a.item_hash \
    WHERE o.amends IS NULL";

/// Filters applied to a posts query. Mirrors the kwargs accepted by Python's
/// `filter_post_select_stmt`.
#[derive(Debug, Clone, Default)]
pub struct PostFilters {
    pub hashes: Option<Vec<String>>,
    pub addresses: Option<Vec<String>>,
    pub refs: Option<Vec<String>>,
    pub post_types: Option<Vec<String>>,
    pub tags: Option<Vec<String>>,
    pub channels: Option<Vec<String>>,
    pub start_date: Option<f64>,
    pub end_date: Option<f64>,
    pub sort_by: Option<SortBy>,
    pub sort_order: Option<SortOrder>,
    pub page: i64,
    pub pagination: i64,
    pub after_time: Option<DateTime<Utc>>,
    pub after_hash: Option<String>,
    pub cursor_mode: bool,
}

// Both `SortBy` and `SortOrder` are wrapped in `Option<>` here so the auto
// `Default` derive only ever sees `Option::default() == None`.

fn map_merged(row: &tokio_postgres::Row) -> MergedPost {
    let channel: Option<String> = row.get("channel");
    MergedPost {
        original_item_hash: row.get("original_item_hash"),
        item_hash: row.get("item_hash"),
        content: row.get("content"),
        owner: row.get("owner"),
        r#ref: row.get("ref"),
        last_updated: row.get("last_updated"),
        channel: channel.map(Channel::from),
        created: row.get("created"),
        original_type: row.get("original_type"),
        tags: row.get("tags"),
    }
}

fn map_merged_v0(row: &tokio_postgres::Row) -> MergedPostV0 {
    let channel: Option<String> = row.get("channel");
    let chain_s: String = row.get("chain");
    let chain = serde_json::from_value::<Chain>(serde_json::Value::String(chain_s.clone()))
        .unwrap_or_else(|_| panic!("unknown Chain in DB: {chain_s}"));
    let item_type_s: String = row.get("item_type");
    let item_type = serde_json::from_value::<ItemType>(serde_json::Value::String(item_type_s))
        .expect("valid ItemType");
    MergedPostV0 {
        chain,
        item_hash: row.get("item_hash"),
        content: row.get("content"),
        r#type: row.get("type"),
        item_type,
        item_content: row.get("item_content"),
        original_item_hash: row.get("original_item_hash"),
        original_type: row.get("original_type"),
        owner: row.get("owner"),
        r#ref: row.get("ref"),
        channel: channel.map(Channel::from),
        signature: row.get("signature"),
        original_signature: row.get("original_signature"),
        time: row.get("time"),
        size: row.get("size"),
        last_updated: row.get("last_updated"),
        tags: row.get("tags"),
    }
}

/// Build the filter SQL fragment + params for a posts query. The fragment is
/// suitable to splice into a `SELECT ... FROM (<base>) sub WHERE <fragment>`
/// shape.
fn build_filters(
    f: &PostFilters,
) -> (
    String,
    Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
) {
    let mut wheres: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    if let Some(hashes) = &f.hashes {
        if !hashes.is_empty() {
            params.push(Box::new(hashes.clone()));
            wheres.push(format!("sub.original_item_hash = ANY(${})", params.len()));
        }
    }
    if let Some(addrs) = &f.addresses {
        if !addrs.is_empty() {
            params.push(Box::new(addrs.clone()));
            wheres.push(format!("sub.owner = ANY(${})", params.len()));
        }
    }
    if let Some(refs) = &f.refs {
        if !refs.is_empty() {
            params.push(Box::new(refs.clone()));
            wheres.push(format!("sub.ref = ANY(${})", params.len()));
        }
    }
    if let Some(types) = &f.post_types {
        if !types.is_empty() {
            params.push(Box::new(types.clone()));
            wheres.push(format!("sub.original_type = ANY(${})", params.len()));
        }
    }
    if let Some(tags) = &f.tags {
        if !tags.is_empty() {
            params.push(Box::new(tags.clone()));
            wheres.push(format!("sub.tags && ${}", params.len()));
        }
    }
    if let Some(chans) = &f.channels {
        if !chans.is_empty() {
            params.push(Box::new(chans.clone()));
            wheres.push(format!("sub.channel = ANY(${})", params.len()));
        }
    }
    if let Some(start) = coerce_to_datetime(DatetimeOrTimestamp::from(f.start_date)) {
        params.push(Box::new(start));
        wheres.push(format!("sub.last_updated >= ${}", params.len()));
    }
    if let Some(end) = coerce_to_datetime(DatetimeOrTimestamp::from(f.end_date)) {
        params.push(Box::new(end));
        wheres.push(format!("sub.last_updated < ${}", params.len()));
    }
    if let Some(at) = f.after_time {
        if f.sort_by == Some(SortBy::Time) {
            let cmp = if f.sort_order == Some(SortOrder::Descending) {
                "<"
            } else {
                ">"
            };
            params.push(Box::new(at));
            let at_idx = params.len();
            // Pass `Option<String>` (not `""`) so a missing cursor hash
            // produces SQL NULL. With `> NULL` the tie-breaker disjunct
            // evaluates to NULL/false, matching pyaleph's
            // `select_stmt.where(item_hash > None)` semantics. Coercing to
            // an empty string here would let *every* tied row through.
            let ah: Option<String> = f.after_hash.clone();
            params.push(Box::new(ah));
            let ah_idx = params.len();
            wheres.push(format!(
                "(sub.last_updated {cmp} ${tidx} OR \
                  (sub.last_updated = ${tidx} AND sub.original_item_hash > ${hidx}))",
                cmp = cmp,
                tidx = at_idx,
                hidx = ah_idx,
            ));
        }
    }
    let frag = if wheres.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", wheres.join(" AND "))
    };
    (frag, params)
}

fn build_order_by(f: &PostFilters, tx_time_alias: bool) -> String {
    let order = f.sort_order.unwrap_or(SortOrder::Descending).to_sql();
    if f.sort_by == Some(SortBy::TxTime) && tx_time_alias {
        if f.sort_order == Some(SortOrder::Ascending) {
            " ORDER BY ec.earliest_confirmation ASC NULLS LAST, \
             sub.created ASC, sub.item_hash ASC"
                .to_string()
        } else {
            " ORDER BY ec.earliest_confirmation DESC NULLS FIRST, \
             sub.created DESC, sub.item_hash ASC"
                .to_string()
        }
    } else {
        format!(" ORDER BY sub.last_updated {order}, sub.original_item_hash ASC")
    }
}

/// Get a single merged post by item hash. Mirrors `get_post`.
pub async fn get_post(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<MergedPost>> {
    let sql = format!(
        "SELECT * FROM ({base}) sub WHERE sub.original_item_hash = $1 LIMIT 1",
        base = MERGED_POST_BASE
    );
    let row = client.query_opt(&sql, &[&item_hash]).await?;
    Ok(row.as_ref().map(map_merged))
}

/// Get the original `PostDb` row, by item hash. Mirrors `get_original_post`.
pub async fn get_original_post(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<PostDb>> {
    let sql = format!(
        "SELECT {cols} FROM posts WHERE item_hash = $1",
        cols = POST_COLS
    );
    let row = client.query_opt(&sql, &[&item_hash]).await?;
    Ok(row.as_ref().map(PostDb::from_row))
}

/// Recompute `posts.latest_amend` for `item_hash`.
///
/// When multiple amends share the same `creation_datetime`, ties are broken
/// by `item_hash` ascending so that the result is deterministic across runs
/// and across nodes. Pyaleph's `SELECT ... LIMIT 1` is non-deterministic on
/// ties; we deliberately diverge here to keep behaviour stable.
pub async fn refresh_latest_amend(client: &impl GenericClient, item_hash: &str) -> AlephResult<()> {
    let sql = "WITH latest AS ( \
                   SELECT amends, MAX(creation_datetime) AS creation_datetime \
                   FROM posts WHERE amends = $1 GROUP BY amends \
               ) \
               UPDATE posts SET latest_amend = ( \
                   SELECT p.item_hash FROM posts p \
                   JOIN latest l ON p.amends = l.amends \
                                AND p.creation_datetime = l.creation_datetime \
                   ORDER BY p.item_hash ASC \
                   LIMIT 1 \
               ) WHERE item_hash = $1";
    client.execute(sql, &[&item_hash]).await?;
    Ok(())
}

/// V1 posts list query. Mirrors `get_matching_posts`.
pub async fn get_matching_posts(
    client: &impl GenericClient,
    filters: &PostFilters,
) -> AlephResult<Vec<MergedPost>> {
    let (filter_sql, mut params) = build_filters(filters);
    let mut sql = if filters.sort_by == Some(SortBy::TxTime) {
        format!(
            "SELECT sub.*, ec.earliest_confirmation FROM ({base}) sub \
             LEFT JOIN ( \
                 SELECT mc.item_hash, MIN(ct.datetime) AS earliest_confirmation \
                 FROM message_confirmations mc \
                 JOIN chain_txs ct ON mc.tx_hash = ct.hash \
                 GROUP BY mc.item_hash \
             ) ec ON sub.original_item_hash = ec.item_hash",
            base = MERGED_POST_BASE
        )
    } else {
        format!("SELECT sub.* FROM ({base}) sub", base = MERGED_POST_BASE)
    };
    sql.push_str(&filter_sql);
    if filters.sort_order.is_some() {
        sql.push_str(&build_order_by(filters, true));
    }
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
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    Ok(rows.iter().map(map_merged).collect())
}

/// V0 posts list query. Mirrors `get_matching_posts_legacy`.
pub async fn get_matching_posts_legacy(
    client: &impl GenericClient,
    filters: &PostFilters,
) -> AlephResult<Vec<MergedPostV0>> {
    let (filter_sql, mut params) = build_filters(filters);

    // Outer SELECT joins messages tables for the message-side columns.
    let mut sql = format!(
        "WITH sub AS ( \
            SELECT * FROM ({base}) sub_inner",
        base = MERGED_POST_V0_BASE
    );
    // Apply the same WHERE inside the CTE so the LIMIT is bounded.
    // Replace `sub.` with `sub_inner.` for the inner clause:
    let inner_filter = filter_sql.replace("sub.", "sub_inner.");
    sql.push_str(&inner_filter);

    // Order + limit on the bounded set inside the CTE.
    let order_by_inner = if filters.sort_order.is_some() {
        // For the inner CTE we can only order by last_updated/created/item_hash.
        let order = filters.sort_order.unwrap_or(SortOrder::Descending).to_sql();
        format!(" ORDER BY sub_inner.last_updated {order}, sub_inner.original_item_hash ASC")
    } else {
        String::new()
    };
    sql.push_str(&order_by_inner);

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
    sql.push_str(
        ") \
        SELECT sub.original_item_hash, sub.item_hash, om.chain AS chain, \
               sub.content, \
               CASE WHEN am.item_type IS NULL THEN om.item_content ELSE am.item_content END \
                   AS item_content, \
               COALESCE(am.item_type, om.item_type) AS item_type, \
               sub.owner, sub.ref, sub.last_updated, sub.channel, sub.created, \
               sub.type, sub.original_type, \
               COALESCE(am.signature, om.signature) AS signature, \
               om.signature AS original_signature, \
               COALESCE(am.size, om.size) AS size, \
               EXTRACT(EPOCH FROM COALESCE(am.time, om.time))::double precision AS time, \
               sub.tags \
        FROM sub \
        JOIN messages om ON om.item_hash = sub.original_item_hash \
        LEFT JOIN messages am ON am.item_hash = sub.latest_amend",
    );

    // Apply the equivalent ORDER BY at the outer scope so callers receive a
    // sorted page (the inner CTE order is discarded when wrapping).
    if filters.sort_order.is_some() {
        if filters.sort_by == Some(SortBy::TxTime) {
            // The outer needs a join to earliest_confirmation
            sql = format!(
                "WITH ec AS ( \
                    SELECT mc.item_hash, MIN(ct.datetime) AS earliest_confirmation \
                    FROM message_confirmations mc \
                    JOIN chain_txs ct ON mc.tx_hash = ct.hash \
                    GROUP BY mc.item_hash \
                ), inner_q AS ({inner}) \
                SELECT inner_q.*, ec.earliest_confirmation FROM inner_q \
                LEFT JOIN ec ON ec.item_hash = inner_q.original_item_hash",
                inner = sql
            );
            if filters.sort_order == Some(SortOrder::Ascending) {
                sql.push_str(
                    " ORDER BY earliest_confirmation ASC NULLS LAST, \
                     inner_q.created ASC, inner_q.item_hash ASC",
                );
            } else {
                sql.push_str(
                    " ORDER BY earliest_confirmation DESC NULLS FIRST, \
                     inner_q.created DESC, inner_q.item_hash ASC",
                );
            }
        } else {
            let order = filters.sort_order.unwrap_or(SortOrder::Descending).to_sql();
            sql.push_str(&format!(
                " ORDER BY last_updated {order}, original_item_hash ASC"
            ));
        }
    }

    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    Ok(rows.iter().map(map_merged_v0).collect())
}

/// Count posts matching the given filters. Mirrors `count_matching_posts`.
///
/// `filters` should usually have `pagination = 0`. When no filters are set the
/// faster `COUNT(*)` over `posts WHERE amends IS NULL` is used.
pub async fn count_matching_posts(
    client: &impl GenericClient,
    filters: Option<&PostFilters>,
) -> AlephResult<i64> {
    match filters {
        None => {
            let row = client
                .query_one("SELECT COUNT(*) FROM posts WHERE amends IS NULL", &[])
                .await?;
            Ok(row.get::<_, i64>(0))
        }
        Some(filters) => {
            let mut f = filters.clone();
            f.pagination = 0;
            f.page = 1;
            let (filter_sql, params) = build_filters(&f);
            let sql = format!(
                "SELECT COUNT(*) FROM ({base}) sub {where_clause}",
                base = MERGED_POST_BASE,
                where_clause = filter_sql
            );
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            let row = client.query_one(&sql, &param_refs).await?;
            Ok(row.get::<_, i64>(0))
        }
    }
}

/// Delete every post that amends `item_hash`. Mirrors `delete_amends`.
pub async fn delete_amends(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Vec<String>> {
    let rows = client
        .query(
            "DELETE FROM posts WHERE amends = $1 RETURNING item_hash",
            &[&item_hash],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Delete one post by item hash.
pub async fn delete_post(client: &impl GenericClient, item_hash: &str) -> AlephResult<()> {
    client
        .execute("DELETE FROM posts WHERE item_hash = $1", &[&item_hash])
        .await?;
    Ok(())
}
