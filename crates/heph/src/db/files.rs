use rusqlite::{Connection, OptionalExtension, Result as SqlResult, params};

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct FileRecord {
    pub hash: String,
    pub size: i64,
    pub file_type: String,
}

#[derive(Debug, Clone)]
pub struct FilePinRecord {
    pub id: i64,
    pub file_hash: String,
    pub owner: String,
    pub pin_type: String,
    pub message_hash: Option<String>,
    pub size: Option<i64>,
    pub content_type: Option<String>,
    pub ref_: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FileTagRecord {
    pub tag: String,
    pub owner: String,
    pub file_hash: String,
    pub last_updated: f64,
}

// ---------------------------------------------------------------------------
// File operations
// ---------------------------------------------------------------------------

/// Insert a file record if it doesn't already exist (INSERT OR IGNORE).
pub fn upsert_file(conn: &Connection, hash: &str, size: i64, file_type: &str) -> SqlResult<usize> {
    conn.execute(
        "INSERT OR IGNORE INTO files (hash, size, file_type) VALUES (?1, ?2, ?3)",
        params![hash, size, file_type],
    )
}

/// Look up a file record by its hash.
pub fn get_file(conn: &Connection, hash: &str) -> SqlResult<Option<FileRecord>> {
    conn.query_row(
        "SELECT hash, size, file_type FROM files WHERE hash = ?1",
        params![hash],
        |row| {
            Ok(FileRecord {
                hash: row.get(0)?,
                size: row.get(1)?,
                file_type: row.get(2)?,
            })
        },
    )
    .optional()
}

// ---------------------------------------------------------------------------
// File pin operations
// ---------------------------------------------------------------------------

/// Parameters for inserting a file pin record.
pub struct InsertFilePin<'a> {
    pub file_hash: &'a str,
    pub owner: &'a str,
    pub pin_type: &'a str,
    pub message_hash: Option<&'a str>,
    pub size: Option<i64>,
    pub content_type: Option<&'a str>,
    pub ref_: Option<&'a str>,
}

/// Insert a new file pin record.
pub fn insert_file_pin(conn: &Connection, pin: &InsertFilePin<'_>) -> SqlResult<i64> {
    conn.execute(
        "INSERT INTO file_pins (file_hash, owner, pin_type, message_hash, size, content_type, ref_)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            pin.file_hash,
            pin.owner,
            pin.pin_type,
            pin.message_hash,
            pin.size,
            pin.content_type,
            pin.ref_
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Delete all pins whose `message_hash` matches the given value.
pub fn delete_file_pin_by_message(conn: &Connection, message_hash: &str) -> SqlResult<usize> {
    conn.execute(
        "DELETE FROM file_pins WHERE message_hash = ?1",
        params![message_hash],
    )
}

/// Count active (non-grace_period) pins for a given file hash.
pub fn count_active_pins(conn: &Connection, file_hash: &str) -> SqlResult<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM file_pins WHERE file_hash = ?1 AND pin_type != 'grace_period'",
        params![file_hash],
        |row| row.get(0),
    )
}

// ---------------------------------------------------------------------------
// File tag operations
// ---------------------------------------------------------------------------

/// Create or update a file tag, but only if `time` is newer than the current `last_updated`.
///
/// Uses INSERT OR REPLACE with a guard: if the tag already exists and its
/// `last_updated` is >= `time`, the row is left unchanged.
pub fn upsert_file_tag(
    conn: &Connection,
    tag: &str,
    owner: &str,
    file_hash: &str,
    time: f64,
) -> SqlResult<usize> {
    // First attempt: insert (succeeds only if tag doesn't exist).
    let inserted = conn.execute(
        "INSERT OR IGNORE INTO file_tags (tag, owner, file_hash, last_updated)
         VALUES (?1, ?2, ?3, ?4)",
        params![tag, owner, file_hash, time],
    )?;

    if inserted > 0 {
        return Ok(1);
    }

    // Already exists — update only if the new time is strictly newer.
    conn.execute(
        "UPDATE file_tags
            SET owner = ?2, file_hash = ?3, last_updated = ?4
          WHERE tag = ?1 AND last_updated < ?4",
        params![tag, owner, file_hash, time],
    )
}

/// Retrieve a file tag by its tag string.
pub fn get_file_tag(conn: &Connection, tag: &str) -> SqlResult<Option<FileTagRecord>> {
    conn.query_row(
        "SELECT tag, owner, file_hash, last_updated FROM file_tags WHERE tag = ?1",
        params![tag],
        |row| {
            Ok(FileTagRecord {
                tag: row.get(0)?,
                owner: row.get(1)?,
                file_hash: row.get(2)?,
                last_updated: row.get(3)?,
            })
        },
    )
    .optional()
}

// ---------------------------------------------------------------------------
// Grace-period pin
// ---------------------------------------------------------------------------

/// Insert a grace-period pin with an expiry timestamp.
pub fn insert_grace_period_pin(
    conn: &Connection,
    file_hash: &str,
    owner: &str,
    delete_by: f64,
) -> SqlResult<i64> {
    conn.execute(
        "INSERT INTO file_pins (file_hash, owner, pin_type, delete_by)
         VALUES (?1, ?2, 'grace_period', ?3)",
        params![file_hash, owner, delete_by],
    )?;
    Ok(conn.last_insert_rowid())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    #[test]
    fn test_upsert_file_and_get() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            let rows = upsert_file(conn, "aabbcc", 1024, "file").unwrap();
            assert_eq!(rows, 1);

            let rec = get_file(conn, "aabbcc").unwrap().expect("should exist");
            assert_eq!(rec.hash, "aabbcc");
            assert_eq!(rec.size, 1024);
            assert_eq!(rec.file_type, "file");
        });
    }

    #[test]
    fn test_upsert_file_ignore_duplicate() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            upsert_file(conn, "hash1", 100, "file").unwrap();
            let rows = upsert_file(conn, "hash1", 999, "dir").unwrap();
            // INSERT OR IGNORE: second call should not update.
            assert_eq!(rows, 0);
            let rec = get_file(conn, "hash1").unwrap().unwrap();
            assert_eq!(rec.size, 100, "size should not change on duplicate");
        });
    }

    #[test]
    fn test_insert_file_pin_and_count() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            upsert_file(conn, "filehash", 512, "file").unwrap();
            insert_file_pin(
                conn,
                &InsertFilePin {
                    file_hash: "filehash",
                    owner: "0xOwner",
                    pin_type: "message",
                    message_hash: Some("msghash"),
                    size: None,
                    content_type: None,
                    ref_: None,
                },
            )
            .unwrap();

            let count = count_active_pins(conn, "filehash").unwrap();
            assert_eq!(count, 1);
        });
    }

    #[test]
    fn test_grace_period_pin_not_counted() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            upsert_file(conn, "fh2", 64, "file").unwrap();
            insert_grace_period_pin(conn, "fh2", "0xOwner", 9999999.0).unwrap();

            let count = count_active_pins(conn, "fh2").unwrap();
            assert_eq!(count, 0, "grace_period pins should not be counted");
        });
    }

    #[test]
    fn test_delete_file_pin_by_message() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            upsert_file(conn, "fh3", 64, "file").unwrap();
            insert_file_pin(
                conn,
                &InsertFilePin {
                    file_hash: "fh3",
                    owner: "0xOwner",
                    pin_type: "message",
                    message_hash: Some("mh1"),
                    size: None,
                    content_type: None,
                    ref_: None,
                },
            )
            .unwrap();

            let deleted = delete_file_pin_by_message(conn, "mh1").unwrap();
            assert_eq!(deleted, 1);
            assert_eq!(count_active_pins(conn, "fh3").unwrap(), 0);
        });
    }

    #[test]
    fn test_upsert_file_tag_newer_wins() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            upsert_file_tag(conn, "owner:ref1", "owner", "hash_v1", 1000.0).unwrap();
            // Update with newer time — should succeed.
            upsert_file_tag(conn, "owner:ref1", "owner", "hash_v2", 1001.0).unwrap();

            let tag = get_file_tag(conn, "owner:ref1").unwrap().unwrap();
            assert_eq!(tag.file_hash, "hash_v2");
            assert_eq!(tag.last_updated, 1001.0);
        });
    }

    #[test]
    fn test_upsert_file_tag_older_ignored() {
        let db = Db::open_in_memory().unwrap();
        db.with_conn(|conn| {
            upsert_file_tag(conn, "owner:ref2", "owner", "hash_v1", 1000.0).unwrap();
            // Update with older time — should NOT replace.
            upsert_file_tag(conn, "owner:ref2", "owner", "hash_old", 500.0).unwrap();

            let tag = get_file_tag(conn, "owner:ref2").unwrap().unwrap();
            assert_eq!(tag.file_hash, "hash_v1", "older update should not replace");
        });
    }
}
