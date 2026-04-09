use rusqlite::{Connection, OptionalExtension, Result as SqlResult, params};

/// A post record as stored in and read back from the `posts` table.
#[derive(Debug, Clone)]
pub struct PostRecord {
    pub item_hash: String,
    pub address: String,
    pub post_type: String,
    pub ref_: Option<String>,
    pub content: Option<String>, // JSON
    pub channel: Option<String>,
    pub time: f64,
    pub original_item_hash: Option<String>,
    pub latest_amend: Option<String>,
}

/// Insert a post record. Uses INSERT OR IGNORE for idempotency.
pub fn insert_post(conn: &Connection, post: &PostRecord) -> SqlResult<usize> {
    conn.execute(
        "INSERT OR IGNORE INTO posts (
            item_hash, address, post_type, ref_, content, channel, time,
            original_item_hash, latest_amend
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
        )",
        params![
            post.item_hash,
            post.address,
            post.post_type,
            post.ref_,
            post.content,
            post.channel,
            post.time,
            post.original_item_hash,
            post.latest_amend,
        ],
    )
}

/// Fetch a post record by item_hash. Returns None if not found.
pub fn get_post(conn: &Connection, item_hash: &str) -> SqlResult<Option<PostRecord>> {
    conn.query_row(
        "SELECT item_hash, address, post_type, ref_, content, channel, time,
                original_item_hash, latest_amend
         FROM posts WHERE item_hash = ?1",
        params![item_hash],
        |row| {
            Ok(PostRecord {
                item_hash: row.get(0)?,
                address: row.get(1)?,
                post_type: row.get(2)?,
                ref_: row.get(3)?,
                content: row.get(4)?,
                channel: row.get(5)?,
                time: row.get(6)?,
                original_item_hash: row.get(7)?,
                latest_amend: row.get(8)?,
            })
        },
    )
    .optional()
}

/// Update latest_amend on the original post, but only if this amend is newer
/// (i.e. amend_time >= existing time, or latest_amend is NULL).
pub fn update_latest_amend(
    conn: &Connection,
    original_hash: &str,
    amend_hash: &str,
    amend_time: f64,
) -> SqlResult<usize> {
    // Only update if latest_amend is NULL or the existing amend's time is older.
    // We join with the amends table to compare times.
    conn.execute(
        "UPDATE posts SET latest_amend = ?1
         WHERE item_hash = ?2
           AND (
               latest_amend IS NULL
               OR (
                   SELECT time FROM posts WHERE item_hash = (
                       SELECT latest_amend FROM posts WHERE item_hash = ?2
                   )
               ) <= ?3
           )",
        params![amend_hash, original_hash, amend_time],
    )
}

/// Delete a post record by item_hash. Returns the number of rows deleted.
pub fn delete_post(conn: &Connection, item_hash: &str) -> SqlResult<usize> {
    conn.execute("DELETE FROM posts WHERE item_hash = ?1", params![item_hash])
}

/// Return all amend hashes for a given original post hash.
pub fn get_amends_for_post(conn: &Connection, original_hash: &str) -> SqlResult<Vec<String>> {
    let mut stmt = conn.prepare("SELECT item_hash FROM posts WHERE original_item_hash = ?1")?;
    let rows = stmt.query_map(params![original_hash], |row| row.get(0))?;
    rows.collect()
}

/// Refresh latest_amend on an original post by scanning remaining amends.
/// Sets latest_amend to the hash of the most recent (by time DESC) amend, or NULL if none.
pub fn refresh_latest_amend(conn: &Connection, original_hash: &str) -> SqlResult<usize> {
    conn.execute(
        "UPDATE posts SET latest_amend = (
            SELECT item_hash FROM posts
            WHERE original_item_hash = ?1
            ORDER BY time DESC, item_hash DESC
            LIMIT 1
         )
         WHERE item_hash = ?1",
        params![original_hash],
    )
}

// ---------------------------------------------------------------------------
// Query functions for API
// ---------------------------------------------------------------------------

/// A post record joined with its message row, and optionally the amend's message row.
#[derive(Debug, Clone)]
pub struct PostWithMessage {
    pub post: PostRecord,
    /// The message row for the *effective* version (amend if latest_amend is set, else original).
    pub msg: StoredMessage,
    /// The message row for the original post (only Some when latest_amend is set).
    pub original_msg: Option<StoredMessage>,
}

/// A minimal stored message row for joins.
#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub item_hash: String,
    pub message_type: String,
    pub chain: String,
    pub sender: String,
    pub signature: String,
    pub item_type: String,
    pub item_content: Option<String>,
    pub channel: Option<String>,
    pub time: f64,
    pub size: i64,
    pub status: String,
}

/// Filter for querying posts.
#[derive(Debug, Default)]
pub struct PostFilter {
    pub addresses: Vec<String>,
    pub hashes: Vec<String>,
    pub refs: Vec<String>,
    pub types: Vec<String>,
    pub channels: Vec<String>,
    pub start_date: Option<f64>,
    pub end_date: Option<f64>,
    pub sort_by: String,
    pub sort_order: i32,
    pub page: u32,
    pub per_page: u32,
}

/// Query original posts (non-amend) with filtering and pagination.
/// Returns (posts_with_messages, total_count).
pub fn query_posts(
    conn: &Connection,
    filter: &PostFilter,
) -> SqlResult<(Vec<PostWithMessage>, i64)> {
    let mut clauses: Vec<String> = vec!["p.original_item_hash IS NULL".to_string()];
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

    add_in(&mut clauses, &mut params, "p.address", &filter.addresses);
    add_in(&mut clauses, &mut params, "p.item_hash", &filter.hashes);
    add_in(&mut clauses, &mut params, "p.ref_", &filter.refs);
    add_in(&mut clauses, &mut params, "p.post_type", &filter.types);
    add_in(&mut clauses, &mut params, "p.channel", &filter.channels);

    if let Some(start) = filter.start_date {
        params.push(Box::new(start));
        clauses.push(format!("p.time >= ?{}", params.len()));
    }
    if let Some(end) = filter.end_date {
        params.push(Box::new(end));
        clauses.push(format!("p.time <= ?{}", params.len()));
    }

    let where_sql = format!(" WHERE {}", clauses.join(" AND "));

    // Count total
    let count_sql = format!("SELECT COUNT(*) FROM posts p{where_sql}");
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let total: i64 = conn.query_row(&count_sql, param_refs.as_slice(), |row| row.get(0))?;

    let sort_col = match filter.sort_by.as_str() {
        "time" => "p.time",
        _ => "p.time",
    };
    let sort_dir = if filter.sort_order >= 0 {
        "ASC"
    } else {
        "DESC"
    };

    let limit_clause = if filter.per_page == 0 {
        String::new()
    } else {
        let offset = (filter.page.saturating_sub(1)) as i64 * filter.per_page as i64;
        format!(" LIMIT {} OFFSET {}", filter.per_page, offset)
    };

    // Columns: post fields (9) + orig_msg fields (11) + amend_msg fields (11)
    let query_sql = format!(
        "SELECT
            p.item_hash, p.address, p.post_type, p.ref_, p.content, p.channel, p.time,
            p.original_item_hash, p.latest_amend,
            om.item_hash, om.type, om.chain, om.sender, om.signature,
            om.item_type, om.item_content, om.channel, om.time, om.size, om.status,
            am.item_hash, am.type, am.chain, am.sender, am.signature,
            am.item_type, am.item_content, am.channel, am.time, am.size, am.status
         FROM posts p
         LEFT JOIN messages om ON om.item_hash = p.item_hash
         LEFT JOIN messages am ON am.item_hash = p.latest_amend
         {where_sql} ORDER BY {sort_col} {sort_dir}{limit_clause}"
    );

    let param_refs2: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&query_sql)?;
    let rows = stmt.query_map(param_refs2.as_slice(), |row| {
        let post = PostRecord {
            item_hash: row.get(0)?,
            address: row.get(1)?,
            post_type: row.get(2)?,
            ref_: row.get(3)?,
            content: row.get(4)?,
            channel: row.get(5)?,
            time: row.get(6)?,
            original_item_hash: row.get(7)?,
            latest_amend: row.get(8)?,
        };

        // orig_msg (offset 9)
        let orig_item_hash: Option<String> = row.get(9)?;
        let orig_msg = if let Some(item_hash) = orig_item_hash {
            Some(StoredMessage {
                item_hash,
                message_type: row.get(10)?,
                chain: row.get(11)?,
                sender: row.get(12)?,
                signature: row.get(13)?,
                item_type: row.get(14)?,
                item_content: row.get(15)?,
                channel: row.get(16)?,
                time: row.get(17)?,
                size: row.get(18)?,
                status: row.get(19)?,
            })
        } else {
            None
        };

        // amend_msg (offset 20)
        let amend_item_hash: Option<String> = row.get(20)?;
        let amend_msg = if let Some(item_hash) = amend_item_hash {
            Some(StoredMessage {
                item_hash,
                message_type: row.get(21)?,
                chain: row.get(22)?,
                sender: row.get(23)?,
                signature: row.get(24)?,
                item_type: row.get(25)?,
                item_content: row.get(26)?,
                channel: row.get(27)?,
                time: row.get(28)?,
                size: row.get(29)?,
                status: row.get(30)?,
            })
        } else {
            None
        };

        Ok((post, orig_msg, amend_msg))
    })?;

    let mut result = Vec::new();
    for row in rows {
        let (post, orig_msg_opt, amend_msg_opt) = row?;
        // Determine effective message: amend if present, else original
        let (msg, original_msg) = match (orig_msg_opt, amend_msg_opt) {
            (Some(orig), Some(amend)) => (amend, Some(orig)),
            (Some(orig), None) => (orig, None),
            _ => continue, // no message row found, skip
        };
        result.push(PostWithMessage {
            post,
            msg,
            original_msg,
        });
    }
    Ok((result, total))
}

/// Result of a cursor-based post query.
pub struct CursorPostsResult {
    pub posts: Vec<PostWithMessage>,
    pub next_cursor: Option<String>,
}

/// Query posts with cursor-based pagination.
///
/// Like [`query_posts`] but uses keyset pagination instead of OFFSET.
/// Fetches `per_page + 1` to detect whether more results exist.
pub fn query_posts_cursor(
    conn: &Connection,
    filter: &PostFilter,
    cursor: Option<&crate::cursor::MessageCursor>,
    per_page: u32,
) -> SqlResult<CursorPostsResult> {
    let mut clauses: Vec<String> = vec!["p.original_item_hash IS NULL".to_string()];
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

    add_in(&mut clauses, &mut params, "p.address", &filter.addresses);
    add_in(&mut clauses, &mut params, "p.item_hash", &filter.hashes);
    add_in(&mut clauses, &mut params, "p.ref_", &filter.refs);
    add_in(&mut clauses, &mut params, "p.post_type", &filter.types);
    add_in(&mut clauses, &mut params, "p.channel", &filter.channels);

    if let Some(start) = filter.start_date {
        params.push(Box::new(start));
        clauses.push(format!("p.time >= ?{}", params.len()));
    }
    if let Some(end) = filter.end_date {
        params.push(Box::new(end));
        clauses.push(format!("p.time <= ?{}", params.len()));
    }

    // Cursor keyset condition
    if let Some(c) = cursor {
        let clause = if filter.sort_order >= 0 {
            params.push(Box::new(c.time_f64));
            let t_idx = params.len();
            params.push(Box::new(c.item_hash.clone()));
            let h_idx = params.len();
            format!("(p.time > ?{t_idx} OR (p.time = ?{t_idx} AND p.item_hash > ?{h_idx}))")
        } else {
            params.push(Box::new(c.time_f64));
            let t_idx = params.len();
            params.push(Box::new(c.item_hash.clone()));
            let h_idx = params.len();
            format!("(p.time < ?{t_idx} OR (p.time = ?{t_idx} AND p.item_hash > ?{h_idx}))")
        };
        clauses.push(clause);
    }

    let where_sql = format!(" WHERE {}", clauses.join(" AND "));

    let sort_col = match filter.sort_by.as_str() {
        "time" => "p.time",
        _ => "p.time",
    };
    let sort_dir = if filter.sort_order >= 0 {
        "ASC"
    } else {
        "DESC"
    };

    let limit = per_page + 1;
    let query_sql = format!(
        "SELECT
            p.item_hash, p.address, p.post_type, p.ref_, p.content, p.channel, p.time,
            p.original_item_hash, p.latest_amend,
            om.item_hash, om.type, om.chain, om.sender, om.signature,
            om.item_type, om.item_content, om.channel, om.time, om.size, om.status,
            am.item_hash, am.type, am.chain, am.sender, am.signature,
            am.item_type, am.item_content, am.channel, am.time, am.size, am.status
         FROM posts p
         LEFT JOIN messages om ON om.item_hash = p.item_hash
         LEFT JOIN messages am ON am.item_hash = p.latest_amend
         {where_sql} ORDER BY {sort_col} {sort_dir}, p.item_hash ASC LIMIT {limit}"
    );

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&query_sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let post = PostRecord {
            item_hash: row.get(0)?,
            address: row.get(1)?,
            post_type: row.get(2)?,
            ref_: row.get(3)?,
            content: row.get(4)?,
            channel: row.get(5)?,
            time: row.get(6)?,
            original_item_hash: row.get(7)?,
            latest_amend: row.get(8)?,
        };
        let orig_item_hash: Option<String> = row.get(9)?;
        let orig_msg = if let Some(item_hash) = orig_item_hash {
            Some(StoredMessage {
                item_hash,
                message_type: row.get(10)?,
                chain: row.get(11)?,
                sender: row.get(12)?,
                signature: row.get(13)?,
                item_type: row.get(14)?,
                item_content: row.get(15)?,
                channel: row.get(16)?,
                time: row.get(17)?,
                size: row.get(18)?,
                status: row.get(19)?,
            })
        } else {
            None
        };
        let amend_item_hash: Option<String> = row.get(20)?;
        let amend_msg = if let Some(item_hash) = amend_item_hash {
            Some(StoredMessage {
                item_hash,
                message_type: row.get(21)?,
                chain: row.get(22)?,
                sender: row.get(23)?,
                signature: row.get(24)?,
                item_type: row.get(25)?,
                item_content: row.get(26)?,
                channel: row.get(27)?,
                time: row.get(28)?,
                size: row.get(29)?,
                status: row.get(30)?,
            })
        } else {
            None
        };
        Ok((post, orig_msg, amend_msg))
    })?;

    let mut result = Vec::new();
    for row in rows {
        let (post, orig_msg_opt, amend_msg_opt) = row?;
        let (msg, original_msg) = match (orig_msg_opt, amend_msg_opt) {
            (Some(orig), Some(amend)) => (amend, Some(orig)),
            (Some(orig), None) => (orig, None),
            _ => continue,
        };
        result.push(PostWithMessage {
            post,
            msg,
            original_msg,
        });
    }

    let has_more = result.len() > per_page as usize;
    if has_more {
        result.truncate(per_page as usize);
    }

    let next_cursor = if has_more {
        result
            .last()
            .map(|pwm| crate::cursor::encode_message_cursor(pwm.post.time, &pwm.post.item_hash))
    } else {
        None
    };

    Ok(CursorPostsResult {
        posts: result,
        next_cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    fn sample_post(hash: &str, addr: &str, time: f64) -> PostRecord {
        PostRecord {
            item_hash: hash.to_string(),
            address: addr.to_string(),
            post_type: "test".to_string(),
            ref_: None,
            content: Some(r#"{"body":"Hello"}"#.to_string()),
            channel: Some("TEST".to_string()),
            time,
            original_item_hash: None,
            latest_amend: None,
        }
    }

    #[test]
    fn test_insert_and_get_post() {
        let db = Db::open_in_memory().unwrap();
        let post = sample_post("hash1", "0xABCD", 1000.0);
        db.with_conn(|conn| {
            insert_post(conn, &post).unwrap();
            let fetched = get_post(conn, "hash1").unwrap().unwrap();
            assert_eq!(fetched.item_hash, "hash1");
            assert_eq!(fetched.address, "0xABCD");
            assert_eq!(fetched.post_type, "test");
            assert_eq!(fetched.time, 1000.0);
            assert!(fetched.latest_amend.is_none());
        });
    }

    #[test]
    fn test_get_post_missing() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            let result = get_post(conn, "nonexistent").unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_insert_idempotent() {
        let db = Db::open_in_memory().unwrap();
        let post = sample_post("hash2", "0xABCD", 1000.0);
        db.with_conn(|conn| {
            insert_post(conn, &post).unwrap();
            // Second insert should be silently ignored (INSERT OR IGNORE).
            let rows = insert_post(conn, &post).unwrap();
            assert_eq!(rows, 0, "duplicate insert should be ignored");
        });
    }

    #[test]
    fn test_update_latest_amend() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            let original = sample_post("orig1", "0xABCD", 1000.0);
            insert_post(conn, &original).unwrap();

            // Insert amend1 with time 1001.
            let amend1 = PostRecord {
                item_hash: "amend1".to_string(),
                address: "0xABCD".to_string(),
                post_type: "amend".to_string(),
                ref_: Some("orig1".to_string()),
                content: Some(r#"{"body":"Updated"}"#.to_string()),
                channel: None,
                time: 1001.0,
                original_item_hash: Some("orig1".to_string()),
                latest_amend: None,
            };
            insert_post(conn, &amend1).unwrap();
            update_latest_amend(conn, "orig1", "amend1", 1001.0).unwrap();

            let orig = get_post(conn, "orig1").unwrap().unwrap();
            assert_eq!(orig.latest_amend, Some("amend1".to_string()));

            // Insert amend2 with time 1002 — should replace.
            let amend2 = PostRecord {
                item_hash: "amend2".to_string(),
                address: "0xABCD".to_string(),
                post_type: "amend".to_string(),
                ref_: Some("orig1".to_string()),
                content: None,
                channel: None,
                time: 1002.0,
                original_item_hash: Some("orig1".to_string()),
                latest_amend: None,
            };
            insert_post(conn, &amend2).unwrap();
            update_latest_amend(conn, "orig1", "amend2", 1002.0).unwrap();

            let orig = get_post(conn, "orig1").unwrap().unwrap();
            assert_eq!(orig.latest_amend, Some("amend2".to_string()));

            // Older amend (time 900) should NOT replace amend2.
            let amend_old = PostRecord {
                item_hash: "amend_old".to_string(),
                address: "0xABCD".to_string(),
                post_type: "amend".to_string(),
                ref_: Some("orig1".to_string()),
                content: None,
                channel: None,
                time: 900.0,
                original_item_hash: Some("orig1".to_string()),
                latest_amend: None,
            };
            insert_post(conn, &amend_old).unwrap();
            update_latest_amend(conn, "orig1", "amend_old", 900.0).unwrap();

            let orig = get_post(conn, "orig1").unwrap().unwrap();
            assert_eq!(
                orig.latest_amend,
                Some("amend2".to_string()),
                "older amend should not replace newer"
            );
        });
    }
}
