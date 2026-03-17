use rusqlite::{Connection, OptionalExtension, Result as SqlResult, params};

/// A merged aggregate record as stored in the `aggregates` table.
#[derive(Debug, Clone)]
pub struct AggregateRecord {
    pub address: String,
    pub key: String,
    pub content: String, // JSON
    pub time: f64,
    pub last_revision_hash: Option<String>,
    pub dirty: bool,
    pub created_at: String,
    pub last_updated: String,
}

/// An individual aggregate element as stored in the `aggregate_elements` table.
#[derive(Debug, Clone)]
pub struct AggregateElementRecord {
    pub item_hash: String,
    pub content: String, // JSON
    pub time: f64,
}

/// Insert an aggregate element. Uses INSERT OR IGNORE for idempotency.
pub fn insert_aggregate_element(
    conn: &Connection,
    item_hash: &str,
    address: &str,
    key: &str,
    content_json: &str,
    time: f64,
) -> SqlResult<usize> {
    conn.execute(
        "INSERT OR IGNORE INTO aggregate_elements (item_hash, address, key, content, time)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![item_hash, address, key, content_json, time],
    )
}

/// Try to insert into the aggregates table (ON CONFLICT DO NOTHING).
/// Returns true if a new row was inserted, false if it already existed.
pub fn upsert_aggregate(
    conn: &Connection,
    address: &str,
    key: &str,
    content_json: &str,
    time: f64,
    last_revision_hash: Option<&str>,
) -> SqlResult<bool> {
    let rows = conn.execute(
        "INSERT OR IGNORE INTO aggregates (address, key, content, time, last_revision_hash)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![address, key, content_json, time, last_revision_hash],
    )?;
    Ok(rows > 0)
}

/// Fetch an aggregate record by (address, key). Returns None if not found.
pub fn get_aggregate(
    conn: &Connection,
    address: &str,
    key: &str,
) -> SqlResult<Option<AggregateRecord>> {
    conn.query_row(
        "SELECT address, key, content, time, last_revision_hash, dirty, created_at, last_updated
         FROM aggregates WHERE address = ?1 AND key = ?2",
        params![address, key],
        |row| {
            Ok(AggregateRecord {
                address: row.get(0)?,
                key: row.get(1)?,
                content: row.get(2)?,
                time: row.get(3)?,
                last_revision_hash: row.get(4)?,
                dirty: row.get::<_, i64>(5)? != 0,
                created_at: row.get(6)?,
                last_updated: row.get(7)?,
            })
        },
    )
    .optional()
}

/// Update the merged aggregate content and metadata.
pub fn update_aggregate(
    conn: &Connection,
    address: &str,
    key: &str,
    content_json: &str,
    time: f64,
    last_revision_hash: Option<&str>,
    dirty: bool,
) -> SqlResult<usize> {
    conn.execute(
        "UPDATE aggregates
         SET content = ?1, time = ?2, last_revision_hash = ?3, dirty = ?4,
             last_updated = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE address = ?5 AND key = ?6",
        params![
            content_json,
            time,
            last_revision_hash,
            dirty as i64,
            address,
            key
        ],
    )
}

/// Mark an aggregate as dirty (needs full rebuild on next read).
pub fn mark_aggregate_dirty(conn: &Connection, address: &str, key: &str) -> SqlResult<usize> {
    conn.execute(
        "UPDATE aggregates SET dirty = 1,
             last_updated = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE address = ?1 AND key = ?2",
        params![address, key],
    )
}

/// Fetch all elements for an aggregate, ordered by time ASC, then item_hash ASC.
pub fn get_aggregate_elements(
    conn: &Connection,
    address: &str,
    key: &str,
) -> SqlResult<Vec<AggregateElementRecord>> {
    let mut stmt = conn.prepare(
        "SELECT item_hash, content, time
         FROM aggregate_elements
         WHERE address = ?1 AND key = ?2
         ORDER BY time ASC, item_hash ASC",
    )?;
    let rows = stmt.query_map(params![address, key], |row| {
        Ok(AggregateElementRecord {
            item_hash: row.get(0)?,
            content: row.get(1)?,
            time: row.get(2)?,
        })
    })?;
    rows.collect()
}

/// Count the number of elements for an aggregate.
pub fn count_aggregate_elements(conn: &Connection, address: &str, key: &str) -> SqlResult<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM aggregate_elements WHERE address = ?1 AND key = ?2",
        params![address, key],
        |row| row.get(0),
    )
}

/// Delete a single aggregate element by item_hash.
pub fn delete_aggregate_element(conn: &Connection, item_hash: &str) -> SqlResult<usize> {
    conn.execute(
        "DELETE FROM aggregate_elements WHERE item_hash = ?1",
        params![item_hash],
    )
}

/// Delete the aggregate summary row for (address, key).
pub fn delete_aggregate(conn: &Connection, address: &str, key: &str) -> SqlResult<usize> {
    conn.execute(
        "DELETE FROM aggregates WHERE address = ?1 AND key = ?2",
        params![address, key],
    )
}

/// Rebuild the merged aggregate from remaining elements and update the aggregate row.
/// If there are no remaining elements, deletes the aggregate row.
pub fn rebuild_aggregate(conn: &Connection, address: &str, key: &str) -> SqlResult<()> {
    let elements = get_aggregate_elements(conn, address, key)?;
    if elements.is_empty() {
        delete_aggregate(conn, address, key)?;
        return Ok(());
    }

    // Shallow merge all elements in time ASC order.
    let mut merged = serde_json::Map::new();
    for elem in &elements {
        if let Ok(serde_json::Value::Object(patch)) =
            serde_json::from_str::<serde_json::Value>(&elem.content)
        {
            for (k, v) in patch {
                merged.insert(k, v);
            }
        }
    }
    let content_json = serde_json::to_string(&merged).unwrap_or_else(|_| "{}".to_string());

    let latest_time = elements
        .iter()
        .map(|e| e.time)
        .fold(f64::NEG_INFINITY, f64::max);
    let last_hash = elements.last().map(|e| e.item_hash.as_str());

    update_aggregate(
        conn,
        address,
        key,
        &content_json,
        latest_time,
        last_hash,
        false,
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Query functions for API
// ---------------------------------------------------------------------------

/// Filter parameters for querying aggregates.
#[derive(Debug, Default)]
pub struct AggregateFilter {
    pub addresses: Vec<String>,
    pub keys: Vec<String>,
    pub sort_by: String, // "last_modified"
    pub sort_order: i32, // -1 desc, 1 asc
    pub page: u32,
    pub per_page: u32,
}

/// Fetch all aggregate records for a given address, optionally filtered by keys.
pub fn get_aggregates_for_address(
    conn: &Connection,
    address: &str,
    keys: Option<&[&str]>,
    limit: usize,
) -> SqlResult<Vec<AggregateRecord>> {
    let (where_clause, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = match keys {
        Some(ks) if !ks.is_empty() => {
            let mut p: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(address.to_string())];
            let placeholders: Vec<String> = ks
                .iter()
                .map(|k| {
                    p.push(Box::new(k.to_string()));
                    format!("?{}", p.len())
                })
                .collect();
            (
                format!("WHERE address = ?1 AND key IN ({})", placeholders.join(",")),
                p,
            )
        }
        _ => (
            "WHERE address = ?1".to_string(),
            vec![Box::new(address.to_string())],
        ),
    };

    let limit_clause = if limit > 0 {
        format!(" LIMIT {limit}")
    } else {
        String::new()
    };

    let sql = format!(
        "SELECT address, key, content, time, last_revision_hash, dirty, created_at, last_updated
         FROM aggregates {where_clause} ORDER BY last_updated DESC{limit_clause}"
    );

    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params_vec.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        Ok(AggregateRecord {
            address: row.get(0)?,
            key: row.get(1)?,
            content: row.get(2)?,
            time: row.get(3)?,
            last_revision_hash: row.get(4)?,
            dirty: row.get::<_, i64>(5)? != 0,
            created_at: row.get(6)?,
            last_updated: row.get(7)?,
        })
    })?;
    rows.collect()
}

/// Query aggregates with optional address/key filtering and pagination.
/// Returns (records, total_count).
pub fn query_aggregates(
    conn: &Connection,
    filter: &AggregateFilter,
) -> SqlResult<(Vec<AggregateRecord>, i64)> {
    let mut clauses: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    fn add_in(
        clauses: &mut Vec<String>,
        params: &mut Vec<Box<dyn rusqlite::types::ToSql>>,
        col: &str,
        values: &[String],
    ) {
        if values.is_empty() {
            return;
        }
        let ph: Vec<String> = values
            .iter()
            .map(|v| {
                params.push(Box::new(v.clone()));
                format!("?{}", params.len())
            })
            .collect();
        clauses.push(format!("{col} IN ({})", ph.join(",")));
    }

    add_in(&mut clauses, &mut params, "address", &filter.addresses);
    add_in(&mut clauses, &mut params, "key", &filter.keys);

    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };

    let count_sql = format!("SELECT COUNT(*) FROM aggregates{where_sql}");
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let total: i64 = conn.query_row(&count_sql, param_refs.as_slice(), |row| row.get(0))?;

    let sort_col = match filter.sort_by.as_str() {
        "last_modified" | "last_updated" => "last_updated",
        "created" | "created_at" => "created_at",
        _ => "last_updated",
    };
    let sort_dir = if filter.sort_order >= 0 {
        "ASC"
    } else {
        "DESC"
    };

    let (limit_clause, offset) = if filter.per_page == 0 {
        (String::new(), 0i64)
    } else {
        let off = (filter.page.saturating_sub(1)) as i64 * filter.per_page as i64;
        (format!(" LIMIT {} OFFSET {}", filter.per_page, off), off)
    };
    let _ = offset;

    let query_sql = format!(
        "SELECT address, key, content, time, last_revision_hash, dirty, created_at, last_updated
         FROM aggregates{where_sql} ORDER BY {sort_col} {sort_dir}{limit_clause}"
    );
    let param_refs2: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&query_sql)?;
    let rows = stmt.query_map(param_refs2.as_slice(), |row| {
        Ok(AggregateRecord {
            address: row.get(0)?,
            key: row.get(1)?,
            content: row.get(2)?,
            time: row.get(3)?,
            last_revision_hash: row.get(4)?,
            dirty: row.get::<_, i64>(5)? != 0,
            created_at: row.get(6)?,
            last_updated: row.get(7)?,
        })
    })?;
    let records: Vec<AggregateRecord> = rows.collect::<SqlResult<_>>()?;
    Ok((records, total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    #[test]
    fn test_insert_and_get_aggregate_element() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            insert_aggregate_element(
                conn,
                "hash1",
                "0xABCD",
                "profile",
                r#"{"name":"Alice"}"#,
                1000.0,
            )
            .unwrap();
            let elements = get_aggregate_elements(conn, "0xABCD", "profile").unwrap();
            assert_eq!(elements.len(), 1);
            assert_eq!(elements[0].item_hash, "hash1");
            assert_eq!(elements[0].time, 1000.0);
        });
    }

    #[test]
    fn test_upsert_aggregate_insert_then_ignore() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            let inserted = upsert_aggregate(
                conn,
                "0xABCD",
                "profile",
                r#"{"name":"Alice"}"#,
                1000.0,
                Some("hash1"),
            )
            .unwrap();
            assert!(inserted, "first upsert should insert");

            let inserted2 = upsert_aggregate(
                conn,
                "0xABCD",
                "profile",
                r#"{"name":"Bob"}"#,
                1001.0,
                Some("hash2"),
            )
            .unwrap();
            assert!(!inserted2, "second upsert should not insert (conflict)");

            // Content should still be Alice's.
            let agg = get_aggregate(conn, "0xABCD", "profile").unwrap().unwrap();
            assert_eq!(agg.content, r#"{"name":"Alice"}"#);
        });
    }

    #[test]
    fn test_update_aggregate() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            upsert_aggregate(
                conn,
                "0xABCD",
                "profile",
                r#"{"name":"Alice"}"#,
                1000.0,
                Some("hash1"),
            )
            .unwrap();
            update_aggregate(
                conn,
                "0xABCD",
                "profile",
                r#"{"name":"Bob","age":30}"#,
                1001.0,
                Some("hash2"),
                false,
            )
            .unwrap();

            let agg = get_aggregate(conn, "0xABCD", "profile").unwrap().unwrap();
            assert_eq!(agg.content, r#"{"name":"Bob","age":30}"#);
            assert_eq!(agg.time, 1001.0);
            assert!(!agg.dirty);
        });
    }

    #[test]
    fn test_mark_aggregate_dirty() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            upsert_aggregate(
                conn,
                "0xABCD",
                "profile",
                r#"{"name":"Alice"}"#,
                1000.0,
                Some("hash1"),
            )
            .unwrap();
            mark_aggregate_dirty(conn, "0xABCD", "profile").unwrap();

            let agg = get_aggregate(conn, "0xABCD", "profile").unwrap().unwrap();
            assert!(agg.dirty);
        });
    }

    #[test]
    fn test_count_aggregate_elements() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            assert_eq!(
                count_aggregate_elements(conn, "0xABCD", "profile").unwrap(),
                0
            );
            insert_aggregate_element(conn, "h1", "0xABCD", "profile", r#"{"a":1}"#, 1000.0)
                .unwrap();
            insert_aggregate_element(conn, "h2", "0xABCD", "profile", r#"{"b":2}"#, 1001.0)
                .unwrap();
            assert_eq!(
                count_aggregate_elements(conn, "0xABCD", "profile").unwrap(),
                2
            );
        });
    }

    #[test]
    fn test_get_aggregate_elements_ordered() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            // Insert out of order.
            insert_aggregate_element(conn, "h2", "0xABCD", "k", r#"{"b":2}"#, 1002.0).unwrap();
            insert_aggregate_element(conn, "h1", "0xABCD", "k", r#"{"a":1}"#, 1001.0).unwrap();
            insert_aggregate_element(conn, "h3", "0xABCD", "k", r#"{"c":3}"#, 1003.0).unwrap();

            let elements = get_aggregate_elements(conn, "0xABCD", "k").unwrap();
            assert_eq!(elements.len(), 3);
            assert_eq!(elements[0].item_hash, "h1");
            assert_eq!(elements[1].item_hash, "h2");
            assert_eq!(elements[2].item_hash, "h3");
        });
    }
}
