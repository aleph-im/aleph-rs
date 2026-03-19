use aleph_types::message::{MessageContentEnum, MessageStatus, MessageType};
use rusqlite::{Connection, OptionalExtension, Result as SqlResult, params};
use serde_json;

/// A denormalized message row as stored in (and read back from) the `messages` table.
#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub item_hash: String,
    pub message_type: String,
    pub chain: String,
    pub sender: String,
    pub signature: String,
    pub item_type: String,
    pub item_content: Option<String>,
    /// Serialized JSON of the parsed content.
    pub content: String,
    pub channel: Option<String>,
    pub time: f64,
    pub size: i64,
    pub status: String,
    pub reception_time: f64,
    // Denormalized fields
    pub owner: Option<String>,
    pub content_type: Option<String>,
    pub content_ref: Option<String>,
    pub content_key: Option<String>,
    pub content_item_hash: Option<String>,
    pub payment_type: Option<String>,
}

/// Parameters for inserting a message into the database.
#[derive(Debug)]
pub struct InsertMessage<'a> {
    pub item_hash: &'a str,
    pub message_type: MessageType,
    pub chain: &'a str,
    pub sender: &'a str,
    pub signature: &'a str,
    pub item_type: &'a str,
    pub item_content: Option<&'a str>,
    /// Pre-serialized JSON of the parsed content.
    pub content_json: &'a str,
    pub channel: Option<&'a str>,
    pub time: f64,
    pub size: i64,
    pub status: MessageStatus,
    pub reception_time: f64,
    // Denormalized fields
    pub owner: Option<&'a str>,
    pub content_type: Option<&'a str>,
    pub content_ref: Option<&'a str>,
    pub content_key: Option<&'a str>,
    pub content_item_hash: Option<&'a str>,
    pub payment_type: Option<&'a str>,
}

/// Insert a message into the `messages` table.
///
/// Returns the number of rows inserted (should always be 1 on success).
pub fn insert_message(conn: &Connection, msg: &InsertMessage<'_>) -> SqlResult<usize> {
    conn.execute(
        "INSERT INTO messages (
            item_hash, type, chain, sender, signature,
            item_type, item_content, content, channel, time,
            size, status, reception_time,
            owner, content_type, content_ref, content_key, content_item_hash, payment_type
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5,
            ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13,
            ?14, ?15, ?16, ?17, ?18, ?19
        )",
        params![
            msg.item_hash,
            msg.message_type.to_string(),
            msg.chain,
            msg.sender,
            msg.signature,
            msg.item_type,
            msg.item_content,
            msg.content_json,
            msg.channel,
            msg.time,
            msg.size,
            msg.status.to_string(),
            msg.reception_time,
            msg.owner,
            msg.content_type,
            msg.content_ref,
            msg.content_key,
            msg.content_item_hash,
            msg.payment_type,
        ],
    )
}

/// Retrieve a full message row by its `item_hash`.
pub fn get_message_by_hash(conn: &Connection, item_hash: &str) -> SqlResult<Option<StoredMessage>> {
    conn.query_row(
        "SELECT
            item_hash, type, chain, sender, signature,
            item_type, item_content, content, channel, time,
            size, status, reception_time,
            owner, content_type, content_ref, content_key, content_item_hash, payment_type
         FROM messages WHERE item_hash = ?1",
        params![item_hash],
        |row| {
            Ok(StoredMessage {
                item_hash: row.get(0)?,
                message_type: row.get(1)?,
                chain: row.get(2)?,
                sender: row.get(3)?,
                signature: row.get(4)?,
                item_type: row.get(5)?,
                item_content: row.get(6)?,
                content: row.get(7)?,
                channel: row.get(8)?,
                time: row.get(9)?,
                size: row.get(10)?,
                status: row.get(11)?,
                reception_time: row.get(12)?,
                owner: row.get(13)?,
                content_type: row.get(14)?,
                content_ref: row.get(15)?,
                content_key: row.get(16)?,
                content_item_hash: row.get(17)?,
                payment_type: row.get(18)?,
            })
        },
    )
    .optional()
}

/// Retrieve only the `status` of a message, for a cheap duplicate check.
///
/// Returns `None` if no message with that `item_hash` exists.
pub fn get_message_status(conn: &Connection, item_hash: &str) -> SqlResult<Option<String>> {
    conn.query_row(
        "SELECT status FROM messages WHERE item_hash = ?1",
        params![item_hash],
        |row| row.get::<_, String>(0),
    )
    .optional()
}

/// Update the `status` column of an existing message.
///
/// Returns the number of rows affected (0 if the `item_hash` was not found).
pub fn update_message_status(
    conn: &Connection,
    item_hash: &str,
    new_status: MessageStatus,
) -> SqlResult<usize> {
    conn.execute(
        "UPDATE messages SET status = ?1 WHERE item_hash = ?2",
        params![new_status.to_string(), item_hash],
    )
}

/// Insert a record into the `forgotten_messages` table.
/// Uses INSERT OR IGNORE so repeated calls for the same (item_hash, forgotten_by) are safe.
pub fn insert_forgotten(
    conn: &Connection,
    item_hash: &str,
    forgotten_by: &str,
    reason: Option<&str>,
) -> SqlResult<usize> {
    conn.execute(
        "INSERT OR IGNORE INTO forgotten_messages (item_hash, forgotten_by, reason)
         VALUES (?1, ?2, ?3)",
        params![item_hash, forgotten_by, reason],
    )
}

/// Retrieve the list of `forgotten_by` values for a given item_hash.
pub fn get_forgotten_by(conn: &Connection, item_hash: &str) -> SqlResult<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT forgotten_by FROM forgotten_messages WHERE item_hash = ?1")?;
    let rows = stmt.query_map(params![item_hash], |row| row.get(0))?;
    rows.collect()
}

// ---------------------------------------------------------------------------
// Query support
// ---------------------------------------------------------------------------

/// Filter parameters for querying messages.
#[derive(Debug, Default)]
pub struct MessageFilter {
    pub statuses: Vec<String>,
    pub message_types: Vec<String>,
    pub addresses: Vec<String>,
    pub owners: Vec<String>,
    pub refs: Vec<String>,
    pub content_hashes: Vec<String>,
    pub content_keys: Vec<String>,
    pub content_types: Vec<String>,
    pub chains: Vec<String>,
    pub channels: Vec<String>,
    pub hashes: Vec<String>,
    pub tags: Vec<String>,
    pub start_date: Option<f64>,
    pub end_date: Option<f64>,
    pub sort_by: String,
    pub sort_order: i32, // -1 desc, 1 asc
    pub page: u32,
    pub per_page: u32, // 0 = unlimited
}

impl MessageFilter {
    pub fn default_list() -> Self {
        Self {
            statuses: vec!["processed".into(), "removing".into()],
            sort_by: "time".into(),
            sort_order: -1,
            page: 1,
            per_page: 20,
            ..Default::default()
        }
    }
}

/// Build WHERE clauses and params dynamically. Returns (where_sql, params).
fn build_where_clause(filter: &MessageFilter) -> (String, Vec<Box<dyn rusqlite::types::ToSql>>) {
    let mut clauses: Vec<String> = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    fn add_in_clause(
        clauses: &mut Vec<String>,
        params: &mut Vec<Box<dyn rusqlite::types::ToSql>>,
        column: &str,
        values: &[String],
    ) {
        if values.is_empty() {
            return;
        }
        let placeholders: Vec<String> = values
            .iter()
            .map(|v| {
                params.push(Box::new(v.clone()));
                format!("?{}", params.len())
            })
            .collect();
        clauses.push(format!("{column} IN ({})", placeholders.join(",")));
    }

    add_in_clause(&mut clauses, &mut params, "status", &filter.statuses);
    add_in_clause(&mut clauses, &mut params, "type", &filter.message_types);
    add_in_clause(&mut clauses, &mut params, "sender", &filter.addresses);
    add_in_clause(&mut clauses, &mut params, "owner", &filter.owners);
    add_in_clause(&mut clauses, &mut params, "content_ref", &filter.refs);
    add_in_clause(
        &mut clauses,
        &mut params,
        "content_item_hash",
        &filter.content_hashes,
    );
    add_in_clause(
        &mut clauses,
        &mut params,
        "content_key",
        &filter.content_keys,
    );
    add_in_clause(
        &mut clauses,
        &mut params,
        "content_type",
        &filter.content_types,
    );
    add_in_clause(&mut clauses, &mut params, "chain", &filter.chains);
    add_in_clause(&mut clauses, &mut params, "channel", &filter.channels);
    add_in_clause(&mut clauses, &mut params, "item_hash", &filter.hashes);

    if let Some(start) = filter.start_date {
        params.push(Box::new(start));
        clauses.push(format!("time >= ?{}", params.len()));
    }
    if let Some(end) = filter.end_date {
        params.push(Box::new(end));
        clauses.push(format!("time <= ?{}", params.len()));
    }

    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };

    (where_sql, params)
}

/// Query messages with filtering and pagination.
/// Returns (messages, total_count).
pub fn query_messages(
    conn: &Connection,
    filter: &MessageFilter,
) -> SqlResult<(Vec<StoredMessage>, i64)> {
    let (where_sql, params) = build_where_clause(filter);

    // Count total
    let count_sql = format!("SELECT COUNT(*) FROM messages{where_sql}");
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let total: i64 = conn.query_row(&count_sql, param_refs.as_slice(), |row| row.get(0))?;

    // Sort
    let sort_col = match filter.sort_by.as_str() {
        "time" => "time",
        "reception_time" => "reception_time",
        _ => "time",
    };
    let sort_dir = if filter.sort_order >= 0 {
        "ASC"
    } else {
        "DESC"
    };

    // Pagination
    let (limit_clause, offset_val) = if filter.per_page == 0 {
        (String::new(), None)
    } else {
        let offset = (filter.page.saturating_sub(1)) as i64 * filter.per_page as i64;
        (
            format!(" LIMIT {} OFFSET {}", filter.per_page, offset),
            Some(offset),
        )
    };
    let _ = offset_val; // used in limit_clause already

    let query_sql = format!(
        "SELECT
            item_hash, type, chain, sender, signature,
            item_type, item_content, content, channel, time,
            size, status, reception_time,
            owner, content_type, content_ref, content_key, content_item_hash, payment_type
         FROM messages{where_sql} ORDER BY {sort_col} {sort_dir}{limit_clause}"
    );

    let param_refs2: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&query_sql)?;
    let rows = stmt.query_map(param_refs2.as_slice(), |row| {
        Ok(StoredMessage {
            item_hash: row.get(0)?,
            message_type: row.get(1)?,
            chain: row.get(2)?,
            sender: row.get(3)?,
            signature: row.get(4)?,
            item_type: row.get(5)?,
            item_content: row.get(6)?,
            content: row.get(7)?,
            channel: row.get(8)?,
            time: row.get(9)?,
            size: row.get(10)?,
            status: row.get(11)?,
            reception_time: row.get(12)?,
            owner: row.get(13)?,
            content_type: row.get(14)?,
            content_ref: row.get(15)?,
            content_key: row.get(16)?,
            content_item_hash: row.get(17)?,
            payment_type: row.get(18)?,
        })
    })?;

    let messages: Vec<StoredMessage> = rows.collect::<SqlResult<_>>()?;
    Ok((messages, total))
}

/// Query just item_hash values with filtering and pagination.
pub fn query_message_hashes(
    conn: &Connection,
    filter: &MessageFilter,
) -> SqlResult<(Vec<String>, i64)> {
    let (where_sql, params) = build_where_clause(filter);

    let count_sql = format!("SELECT COUNT(*) FROM messages{where_sql}");
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let total: i64 = conn.query_row(&count_sql, param_refs.as_slice(), |row| row.get(0))?;

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

    let query_sql =
        format!("SELECT item_hash FROM messages{where_sql} ORDER BY time {sort_dir}{limit_clause}");
    let param_refs2: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&query_sql)?;
    let rows = stmt.query_map(param_refs2.as_slice(), |row| row.get::<_, String>(0))?;
    let hashes: Vec<String> = rows.collect::<SqlResult<_>>()?;
    Ok((hashes, total))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract denormalized fields from a `MessageContentEnum` for efficient querying.
pub struct DenormalizedFields {
    pub owner: Option<String>,
    pub content_type: Option<String>,
    pub content_ref: Option<String>,
    pub content_key: Option<String>,
    pub content_item_hash: Option<String>,
    pub payment_type: Option<String>,
}

impl DenormalizedFields {
    pub fn from_content(content_enum: &MessageContentEnum, owner_address: &str) -> Self {
        let mut fields = DenormalizedFields {
            owner: Some(owner_address.to_string()),
            content_type: None,
            content_ref: None,
            content_key: None,
            content_item_hash: None,
            payment_type: None,
        };

        match content_enum {
            MessageContentEnum::Post(post) => {
                fields.content_type = Some(post.post_type_str().to_string());
                if let Some(ref r) = post.reference {
                    fields.content_ref = Some(r.to_string());
                }
            }
            MessageContentEnum::Aggregate(agg) => {
                fields.content_key = Some(agg.key().to_string());
            }
            MessageContentEnum::Store(store) => {
                fields.content_item_hash = Some(store.file_hash().to_string());
                // Populate content_ref if the STORE message has a ref field.
                if let Some(ref raw_ref) = store.reference {
                    fields.content_ref = Some(raw_ref.to_string());
                }
            }
            MessageContentEnum::Program(prog) => {
                // Payment type is nested in on/payment objects — use a JSON value approach.
                if let Some(v) = serde_json::to_value(prog).ok()
                    && let Some(pt) = v
                        .get("on")
                        .and_then(|o| o.get("payment"))
                        .and_then(|p| p.get("type"))
                {
                    fields.payment_type = pt.as_str().map(|s| s.to_string());
                }
            }
            MessageContentEnum::Instance(inst) => {
                if let Some(v) = serde_json::to_value(inst).ok()
                    && let Some(pt) = v.get("payment").and_then(|p| p.get("type"))
                {
                    fields.payment_type = pt.as_str().map(|s| s.to_string());
                }
            }
            MessageContentEnum::Forget(_) => {}
        }

        fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use aleph_types::message::{MessageStatus, MessageType};

    fn sample_insert(item_hash: &str) -> InsertMessage<'_> {
        InsertMessage {
            item_hash,
            message_type: MessageType::Post,
            chain: "ETH",
            sender: "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            signature: "0xdeadbeef",
            item_type: "inline",
            item_content: Some(r#"{"type":"test","address":"0xB68","time":1000.0}"#),
            content_json: r#"{"address":"0xB68","time":1000.0,"type":"test"}"#,
            channel: Some("TEST"),
            time: 1000.0,
            size: 45,
            status: MessageStatus::Pending,
            reception_time: 1001.0,
            owner: Some("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef"),
            content_type: Some("test"),
            content_ref: None,
            content_key: None,
            content_item_hash: None,
            payment_type: None,
        }
    }

    #[test]
    fn test_insert_and_query_round_trip() {
        let db = Db::open_in_memory().unwrap();

        db.with_conn(|conn| {
            let rows = insert_message(conn, &sample_insert("abc123hash")).unwrap();
            assert_eq!(rows, 1);

            let stored = get_message_by_hash(conn, "abc123hash")
                .unwrap()
                .expect("message should exist");

            assert_eq!(stored.item_hash, "abc123hash");
            assert_eq!(stored.message_type, "POST");
            assert_eq!(stored.chain, "ETH");
            assert_eq!(stored.sender, "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef");
            assert_eq!(stored.status, "pending");
            assert_eq!(stored.channel, Some("TEST".into()));
            assert_eq!(
                stored.owner,
                Some("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".into())
            );
            assert_eq!(stored.content_type, Some("test".into()));
            assert_eq!(stored.time, 1000.0);
            assert_eq!(stored.size, 45);
        });
    }

    #[test]
    fn test_get_message_by_hash_missing() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            let result = get_message_by_hash(conn, "nonexistent").unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_get_message_status_exists() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            insert_message(conn, &sample_insert("hash001")).unwrap();
            let status = get_message_status(conn, "hash001").unwrap();
            assert_eq!(status, Some("pending".to_string()));
        });
    }

    #[test]
    fn test_get_message_status_missing() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            let status = get_message_status(conn, "nope").unwrap();
            assert!(status.is_none());
        });
    }

    #[test]
    fn test_duplicate_check_returns_some_for_existing() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            insert_message(conn, &sample_insert("dup_hash")).unwrap();
            // Duplicate check: get_message_status should return Some.
            let status = get_message_status(conn, "dup_hash").unwrap();
            assert!(
                status.is_some(),
                "duplicate check: expected Some for existing message"
            );
        });
    }

    #[test]
    fn test_update_message_status() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            insert_message(conn, &sample_insert("upd_hash")).unwrap();

            // Verify initial status.
            assert_eq!(
                get_message_status(conn, "upd_hash").unwrap(),
                Some("pending".to_string())
            );

            // Update to processed.
            let affected =
                update_message_status(conn, "upd_hash", MessageStatus::Processed).unwrap();
            assert_eq!(affected, 1);

            // Verify updated status.
            assert_eq!(
                get_message_status(conn, "upd_hash").unwrap(),
                Some("processed".to_string())
            );
        });
    }

    #[test]
    fn test_update_status_nonexistent_returns_zero() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            let affected = update_message_status(conn, "ghost", MessageStatus::Processed).unwrap();
            assert_eq!(affected, 0);
        });
    }

    #[test]
    fn test_insert_all_statuses() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            let statuses = [
                MessageStatus::Pending,
                MessageStatus::Processed,
                MessageStatus::Removing,
                MessageStatus::Removed,
                MessageStatus::Forgotten,
                MessageStatus::Rejected,
            ];

            for (i, status) in statuses.iter().enumerate() {
                let hash = format!("hash_{i}");
                let hash_ref: &str = &hash;
                let msg = InsertMessage {
                    item_hash: hash_ref,
                    status: status.clone(),
                    ..sample_insert(hash_ref)
                };
                insert_message(conn, &msg).unwrap();

                let stored_status = get_message_status(conn, hash_ref).unwrap().unwrap();
                assert_eq!(stored_status, status.to_string());
            }
        });
    }
}
