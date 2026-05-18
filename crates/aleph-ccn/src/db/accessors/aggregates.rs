//! `aggregates` and `aggregate_elements` accessors.
//!
//! Mirrors `aleph/db/accessors/aggregates.py`.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use tokio_postgres::GenericClient;
use tokio_postgres::types::ToSql;

use crate::AlephResult;
use crate::db::models::aggregates::{AggregateDb, AggregateElementDb};
use crate::types::sort_order::{SortByAggregate, SortOrder};

/// Check whether an aggregate exists for `(key, owner)`.
pub async fn aggregate_exists(
    client: &impl GenericClient,
    key: &str,
    owner: &str,
) -> AlephResult<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM aggregates WHERE key = $1 AND owner = $2)",
            &[&key, &owner],
        )
        .await?;
    Ok(row.get::<_, bool>(0))
}

/// Output row of [`get_aggregates_by_owner`] when `with_info=true`.
#[derive(Debug, Clone)]
pub struct AggregateInfoRow {
    pub key: String,
    pub content: Value,
    pub created: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
    pub last_update_item_hash: String,
    pub original_item_hash: String,
}

/// Output row of [`get_aggregates_by_owner`] when `with_info=false`.
#[derive(Debug, Clone)]
pub struct AggregateKeyContentRow {
    pub key: String,
    pub content: Value,
}

/// Result of [`get_aggregates_by_owner`].
pub enum AggregatesByOwner {
    Plain(Vec<AggregateKeyContentRow>),
    WithInfo(Vec<AggregateInfoRow>),
}

/// Fetch aggregates owned by `owner`. Mirrors `get_aggregates_by_owner`.
///
/// The Python implementation has an in-process cache; in Rust this is the
/// raw DB call and callers can layer caching on top.
pub async fn get_aggregates_by_owner(
    client: &impl GenericClient,
    owner: &str,
    with_info: bool,
    keys: Option<&[String]>,
) -> AlephResult<AggregatesByOwner> {
    if with_info {
        let mut sql = String::from(
            "SELECT a.key AS key, a.content AS content, \
                    a.creation_datetime AS created, \
                    ae.creation_datetime AS last_updated, \
                    a.last_revision_hash AS last_update_item_hash, \
                    ae.item_hash AS original_item_hash \
             FROM aggregates a \
             JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
             WHERE a.owner = $1",
        );
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(owner.to_string())];
        if let Some(keys) = keys {
            if !keys.is_empty() {
                params.push(Box::new(keys.to_vec()));
                sql.push_str(&format!(" AND a.key = ANY(${})", params.len()));
            }
        }
        let param_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|b| b.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let rows = client.query(&sql, &param_refs).await?;
        let out: Vec<AggregateInfoRow> = rows
            .iter()
            .map(|r| AggregateInfoRow {
                key: r.get("key"),
                content: r.get("content"),
                created: r.get("created"),
                last_updated: r.get("last_updated"),
                last_update_item_hash: r.get("last_update_item_hash"),
                original_item_hash: r.get("original_item_hash"),
            })
            .collect();
        Ok(AggregatesByOwner::WithInfo(out))
    } else {
        let mut sql = String::from("SELECT key, content FROM aggregates WHERE owner = $1");
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(owner.to_string())];
        if let Some(keys) = keys {
            if !keys.is_empty() {
                params.push(Box::new(keys.to_vec()));
                sql.push_str(&format!(" AND key = ANY(${})", params.len()));
            }
        }
        sql.push_str(" ORDER BY key");
        let param_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|b| b.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let rows = client.query(&sql, &param_refs).await?;
        let out = rows
            .iter()
            .map(|r| AggregateKeyContentRow {
                key: r.get("key"),
                content: r.get("content"),
            })
            .collect();
        Ok(AggregatesByOwner::Plain(out))
    }
}

/// Fetch a single aggregate by its `(owner, key)` primary key.
///
/// `with_content=false` mirrors Python's deferred-content option: the
/// returned `content` is JSON null when callers don't need it.
pub async fn get_aggregate_by_key(
    client: &impl GenericClient,
    owner: &str,
    key: &str,
    with_content: bool,
) -> AlephResult<Option<AggregateDb>> {
    let sql = if with_content {
        "SELECT key, owner, content, creation_datetime, last_revision_hash, dirty \
         FROM aggregates WHERE owner = $1 AND key = $2"
    } else {
        "SELECT key, owner, 'null'::jsonb AS content, creation_datetime, \
                last_revision_hash, dirty \
         FROM aggregates WHERE owner = $1 AND key = $2"
    };
    let row = client.query_opt(sql, &[&owner, &key]).await?;
    Ok(row.as_ref().map(AggregateDb::from_row))
}

/// Return the list of top-level JSONB keys of an aggregate's `content`.
///
/// Mirrors `get_aggregate_content_keys` (which uses
/// `AggregateDb.jsonb_keys`).
pub async fn get_aggregate_content_keys(
    client: &impl GenericClient,
    owner: &str,
    key: &str,
) -> AlephResult<Vec<String>> {
    let sql = "SELECT jsonb_object_keys(content) AS k \
               FROM aggregates WHERE key = $1 AND owner = $2";
    let rows = client.query(sql, &[&key, &owner]).await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>("k")).collect())
}

/// All elements that compose an aggregate, ordered by `creation_datetime` ASC.
pub async fn get_aggregate_elements(
    client: &impl GenericClient,
    owner: &str,
    key: &str,
) -> AlephResult<Vec<AggregateElementDb>> {
    let sql = "SELECT item_hash, key, owner, content, creation_datetime \
               FROM aggregate_elements \
               WHERE key = $1 AND owner = $2 \
               ORDER BY creation_datetime ASC";
    let rows = client.query(sql, &[&key, &owner]).await?;
    Ok(rows.iter().map(AggregateElementDb::from_row).collect())
}

/// Insert a fully-built aggregate row.
pub async fn insert_aggregate(
    client: &impl GenericClient,
    key: &str,
    owner: &str,
    content: &Value,
    creation_datetime: DateTime<Utc>,
    last_revision_hash: &str,
) -> AlephResult<()> {
    let sql = "INSERT INTO aggregates(key, owner, content, creation_datetime, \
                                       last_revision_hash, dirty) \
               VALUES ($1, $2, $3, $4, $5, FALSE)";
    client
        .execute(
            sql,
            &[
                &key,
                &owner,
                content,
                &creation_datetime,
                &last_revision_hash,
            ],
        )
        .await?;
    Ok(())
}

/// Update an aggregate's content, merging via `jsonb` concat (append or
/// prepend), and refreshing the revision metadata.
pub async fn update_aggregate(
    client: &impl GenericClient,
    key: &str,
    owner: &str,
    content: &Value,
    creation_datetime: DateTime<Utc>,
    last_revision_hash: &str,
    prepend: bool,
) -> AlephResult<()> {
    let merge_expr = if prepend {
        "$3::jsonb || content"
    } else {
        "content || $3::jsonb"
    };
    let sql = format!(
        "UPDATE aggregates \
         SET content = {merge_expr}, \
             creation_datetime = $4, \
             last_revision_hash = $5 \
         WHERE key = $1 AND owner = $2"
    );
    client
        .execute(
            &sql,
            &[
                &key,
                &owner,
                content,
                &creation_datetime,
                &last_revision_hash,
            ],
        )
        .await?;
    Ok(())
}

/// Insert one revision into `aggregate_elements`.
pub async fn insert_aggregate_element(
    client: &impl GenericClient,
    item_hash: &str,
    key: &str,
    owner: &str,
    content: &Value,
    creation_datetime: DateTime<Utc>,
) -> AlephResult<()> {
    let sql = "INSERT INTO aggregate_elements(item_hash, key, owner, content, creation_datetime) \
               VALUES ($1, $2, $3, $4, $5)";
    client
        .execute(
            sql,
            &[&item_hash, &key, &owner, content, &creation_datetime],
        )
        .await?;
    Ok(())
}

/// Count the number of revisions of an aggregate.
pub async fn count_aggregate_elements(
    client: &impl GenericClient,
    owner: &str,
    key: &str,
) -> AlephResult<i64> {
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM aggregate_elements WHERE key = $1 AND owner = $2",
            &[&key, &owner],
        )
        .await?;
    Ok(row.get::<_, i64>(0))
}

/// Merge a stream of revisions into a single JSON object, mirroring
/// `merge_aggregate_elements`.
pub fn merge_aggregate_elements<'a, I>(elements: I) -> Map<String, Value>
where
    I: IntoIterator<Item = &'a AggregateElementDb>,
{
    let mut content: Map<String, Value> = Map::new();
    for elem in elements {
        if let Some(obj) = elem.content.as_object() {
            for (k, v) in obj {
                content.insert(k.clone(), v.clone());
            }
        }
    }
    content
}

/// Mark an aggregate row as dirty so it gets recomputed on the next refresh.
pub async fn mark_aggregate_as_dirty(
    client: &impl GenericClient,
    owner: &str,
    key: &str,
) -> AlephResult<()> {
    client
        .execute(
            "UPDATE aggregates SET dirty = TRUE WHERE key = $1 AND owner = $2",
            &[&key, &owner],
        )
        .await?;
    Ok(())
}

/// Recompute and upsert the merged content for `(owner, key)` from
/// `aggregate_elements`. Mirrors `refresh_aggregate`.
///
/// Uses the `jsonb_merge` aggregate function created by the v1 migration.
pub async fn refresh_aggregate(
    client: &impl GenericClient,
    owner: &str,
    key: &str,
) -> AlephResult<()> {
    // Equivalent SQL: merge revisions, look up the hash of the last revision,
    // and upsert the result.
    let sql = "WITH merged AS ( \
                   SELECT key, owner, \
                          MIN(creation_datetime) AS creation_datetime, \
                          MAX(creation_datetime) AS last_revision_datetime, \
                          jsonb_merge(content ORDER BY creation_datetime) AS content \
                   FROM aggregate_elements \
                   WHERE key = $1 AND owner = $2 \
                   GROUP BY key, owner \
               ) \
               INSERT INTO aggregates(key, owner, creation_datetime, content, \
                                      last_revision_hash, dirty) \
               SELECT m.key, m.owner, m.creation_datetime, m.content, \
                      ae.item_hash, FALSE \
               FROM merged m \
               JOIN aggregate_elements ae \
                   ON m.key = ae.key \
                  AND m.owner = ae.owner \
                  AND m.last_revision_datetime = ae.creation_datetime \
               ON CONFLICT ON CONSTRAINT aggregates_pkey DO UPDATE \
               SET content = EXCLUDED.content, \
                   creation_datetime = EXCLUDED.creation_datetime, \
                   last_revision_hash = EXCLUDED.last_revision_hash, \
                   dirty = EXCLUDED.dirty";
    client.execute(sql, &[&key, &owner]).await?;
    Ok(())
}

/// Delete an aggregate row.
pub async fn delete_aggregate(
    client: &impl GenericClient,
    owner: &str,
    key: &str,
) -> AlephResult<()> {
    client
        .execute(
            "DELETE FROM aggregates WHERE key = $1 AND owner = $2",
            &[&key, &owner],
        )
        .await?;
    Ok(())
}

/// Delete a single revision from `aggregate_elements`.
pub async fn delete_aggregate_element(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<()> {
    client
        .execute(
            "DELETE FROM aggregate_elements WHERE item_hash = $1",
            &[&item_hash],
        )
        .await?;
    Ok(())
}

/// Parameters for [`get_aggregates`].
#[derive(Debug, Clone)]
pub struct AggregatesQuery {
    pub keys: Option<Vec<String>>,
    pub addresses: Option<Vec<String>>,
    pub sort_by: SortByAggregate,
    pub sort_order: SortOrder,
    pub page: i64,
    pub pagination: i64,
    pub after_time: Option<DateTime<Utc>>,
    pub after_key: Option<String>,
    pub after_owner: Option<String>,
    pub cursor_mode: bool,
}

impl Default for AggregatesQuery {
    fn default() -> Self {
        Self {
            keys: None,
            addresses: None,
            sort_by: SortByAggregate::LastModified,
            sort_order: SortOrder::Descending,
            page: 1,
            pagination: 100,
            after_time: None,
            after_key: None,
            after_owner: None,
            cursor_mode: false,
        }
    }
}

/// Paginated list of aggregates with optional filters, mirroring
/// `get_aggregates`.
pub async fn get_aggregates(
    client: &impl GenericClient,
    q: &AggregatesQuery,
) -> AlephResult<Vec<AggregateDb>> {
    let order_col = match q.sort_by {
        SortByAggregate::CreationTime => "a.creation_datetime",
        SortByAggregate::LastModified => "ae.creation_datetime",
    };
    let direction = q.sort_order.to_sql();

    let mut sql = String::from(
        "SELECT a.key, a.owner, a.content, a.creation_datetime, \
                a.last_revision_hash, a.dirty \
         FROM aggregates a \
         JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash",
    );
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut where_clauses: Vec<String> = Vec::new();

    if let Some(keys) = &q.keys {
        if !keys.is_empty() {
            params.push(Box::new(keys.clone()));
            where_clauses.push(format!("a.key = ANY(${})", params.len()));
        }
    }
    if let Some(addrs) = &q.addresses {
        if !addrs.is_empty() {
            params.push(Box::new(addrs.clone()));
            where_clauses.push(format!("a.owner = ANY(${})", params.len()));
        }
    }

    if let Some(after_time) = q.after_time {
        let cmp = if q.sort_order == SortOrder::Descending {
            "<"
        } else {
            ">"
        };
        params.push(Box::new(after_time));
        let after_time_idx = params.len();
        // Bind as `Option<String>` so a missing cursor key/owner is SQL NULL,
        // matching pyaleph's `(key > None)` semantics (always false).
        let after_key: Option<String> = q.after_key.clone();
        params.push(Box::new(after_key));
        let after_key_idx = params.len();
        let after_owner: Option<String> = q.after_owner.clone();
        params.push(Box::new(after_owner));
        let after_owner_idx = params.len();
        where_clauses.push(format!(
            "({col} {cmp} ${tidx} OR ({col} = ${tidx} AND \
              (a.key > ${kidx} OR (a.key = ${kidx} AND a.owner > ${oidx}))))",
            col = order_col,
            tidx = after_time_idx,
            kidx = after_key_idx,
            oidx = after_owner_idx,
        ));
    }

    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }
    sql.push_str(&format!(" ORDER BY {order_col} {direction}"));

    if q.after_time.is_none() && q.page > 1 {
        let off = (q.page - 1) * q.pagination;
        params.push(Box::new(off));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
    }
    if q.pagination > 0 {
        let lim = if q.after_time.is_some() || q.cursor_mode {
            q.pagination + 1
        } else {
            q.pagination
        };
        params.push(Box::new(lim));
        sql.push_str(&format!(" LIMIT ${}", params.len()));
    }

    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    Ok(rows.iter().map(AggregateDb::from_row).collect())
}

/// Same as [`get_aggregates`] but returns the `(AggregateDb, last_revision_datetime)`
/// tuple so the API layer can populate `last_updated` from the most recent
/// revision's timestamp (matching pyaleph's `result[3]`).
pub async fn get_aggregates_with_last_revision(
    client: &impl GenericClient,
    q: &AggregatesQuery,
) -> AlephResult<Vec<(AggregateDb, DateTime<Utc>)>> {
    let order_col = match q.sort_by {
        SortByAggregate::CreationTime => "a.creation_datetime",
        SortByAggregate::LastModified => "ae.creation_datetime",
    };
    let direction = q.sort_order.to_sql();

    let mut sql = String::from(
        "SELECT a.key, a.owner, a.content, a.creation_datetime, \
                a.last_revision_hash, a.dirty, \
                ae.creation_datetime AS last_revision_datetime \
         FROM aggregates a \
         JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash",
    );
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut where_clauses: Vec<String> = Vec::new();

    if let Some(keys) = &q.keys {
        if !keys.is_empty() {
            params.push(Box::new(keys.clone()));
            where_clauses.push(format!("a.key = ANY(${})", params.len()));
        }
    }
    if let Some(addrs) = &q.addresses {
        if !addrs.is_empty() {
            params.push(Box::new(addrs.clone()));
            where_clauses.push(format!("a.owner = ANY(${})", params.len()));
        }
    }

    if let Some(after_time) = q.after_time {
        let cmp = if q.sort_order == SortOrder::Descending {
            "<"
        } else {
            ">"
        };
        params.push(Box::new(after_time));
        let after_time_idx = params.len();
        // Bind missing cursor key/owner as SQL NULL (cf. pyaleph semantics).
        let after_key: Option<String> = q.after_key.clone();
        params.push(Box::new(after_key));
        let after_key_idx = params.len();
        let after_owner: Option<String> = q.after_owner.clone();
        params.push(Box::new(after_owner));
        let after_owner_idx = params.len();
        where_clauses.push(format!(
            "({col} {cmp} ${tidx} OR ({col} = ${tidx} AND \
              (a.key > ${kidx} OR (a.key = ${kidx} AND a.owner > ${oidx}))))",
            col = order_col,
            tidx = after_time_idx,
            kidx = after_key_idx,
            oidx = after_owner_idx,
        ));
    }

    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }
    sql.push_str(&format!(" ORDER BY {order_col} {direction}"));

    if q.after_time.is_none() && q.page > 1 {
        let off = (q.page - 1) * q.pagination;
        params.push(Box::new(off));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
    }
    if q.pagination > 0 {
        let lim = if q.after_time.is_some() || q.cursor_mode {
            q.pagination + 1
        } else {
            q.pagination
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
        .iter()
        .map(|r| {
            let last_revision: DateTime<Utc> = r.get("last_revision_datetime");
            (AggregateDb::from_row(r), last_revision)
        })
        .collect())
}

/// Return the keys of all dirty aggregates owned by `owner`. Used by the
/// API layer to fire a refresh before serving aggregate data. Mirrors the
/// `select(AggregateDb.key).where(...dirty)` query in pyaleph.
pub async fn get_dirty_aggregate_keys_for_owner(
    client: &impl GenericClient,
    owner: &str,
) -> AlephResult<Vec<String>> {
    let rows = client
        .query(
            "SELECT key FROM aggregates WHERE owner = $1 AND dirty = TRUE",
            &[&owner],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.get::<_, String>("key"))
        .collect())
}

/// Count aggregates matching the same filters as [`get_aggregates`].
pub async fn count_aggregates(
    client: &impl GenericClient,
    keys: Option<&[String]>,
    addresses: Option<&[String]>,
) -> AlephResult<i64> {
    let mut sql = String::from("SELECT COUNT(*) FROM aggregates");
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut where_clauses: Vec<String> = Vec::new();
    if let Some(keys) = keys {
        if !keys.is_empty() {
            params.push(Box::new(keys.to_vec()));
            where_clauses.push(format!("key = ANY(${})", params.len()));
        }
    }
    if let Some(addrs) = addresses {
        if !addrs.is_empty() {
            params.push(Box::new(addrs.to_vec()));
            where_clauses.push(format!("owner = ANY(${})", params.len()));
        }
    }
    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }
    let param_refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let row = client.query_one(&sql, &param_refs).await?;
    Ok(row.get::<_, i64>(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_aggregate_elements_merges_in_order() {
        let elems = vec![
            AggregateElementDb {
                item_hash: "h1".into(),
                key: "k".into(),
                owner: "o".into(),
                content: json!({"a": 1}),
                creation_datetime: Utc::now(),
            },
            AggregateElementDb {
                item_hash: "h2".into(),
                key: "k".into(),
                owner: "o".into(),
                content: json!({"b": 2, "a": 3}),
                creation_datetime: Utc::now(),
            },
        ];
        let merged = merge_aggregate_elements(&elems);
        assert_eq!(merged.get("a"), Some(&json!(3)));
        assert_eq!(merged.get("b"), Some(&json!(2)));
    }
}
