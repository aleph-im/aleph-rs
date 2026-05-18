//! `files`, `file_pins` and `file_tags` accessors.
//!
//! Mirrors `aleph/db/accessors/files.py`.

use chrono::{DateTime, Utc};
use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::db::models::files::{FilePinDb, FilePinType, FileTagDb, StoredFileDb};
use crate::types::files::{FileTag, FileType};
use crate::types::sort_order::SortOrder;

const FILE_PIN_COLS: &str =
    "id, file_hash, created, type, owner, item_hash, ref, tx_hash, delete_by";

fn file_type_value(ft: FileType) -> &'static str {
    match ft {
        FileType::File => "file",
        FileType::Directory => "dir",
    }
}

/// Mirror Python's strict `ItemHash(ref)` recognition. See
/// `make_file_tag` in `handlers/content/store.rs` for the rationale —
/// `crate::schemas::base_messages::item_type_from_hash` is too permissive
/// for this use case.
fn ref_is_item_hash(r: &str) -> bool {
    (r.starts_with("Qm") && (44..=46).contains(&r.len()))
        || (r.starts_with("bafy") && r.len() == 59)
        || r.len() == 64
}

fn message_file_tag_default(owner: Option<&str>, item_hash: &str, r#ref: Option<&str>) -> String {
    // Replicates Python `aleph.utils.make_file_tag(owner, ref, item_hash)`.
    // Rules:
    //   * `ref` is None         → tag = item_hash
    //   * `ref` is a real hash → tag = ref (verbatim, ignoring owner)
    //   * otherwise            → tag = `<owner>/<ref>`
    match r#ref {
        None => item_hash.to_string(),
        Some(r) if r.is_empty() => item_hash.to_string(),
        Some(r) => {
            if ref_is_item_hash(r) {
                r.to_string()
            } else {
                match owner {
                    Some(o) => format!("{o}/{r}"),
                    // Without an owner we cannot form `<owner>/<ref>`; fall back
                    // to item_hash so we never emit a `None/ref` placeholder.
                    None => item_hash.to_string(),
                }
            }
        }
    }
}

/// Whether any pin row exists for a given file hash.
pub async fn is_pinned_file(client: &impl GenericClient, file_hash: &str) -> AlephResult<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM file_pins WHERE file_hash = $1)",
            &[&file_hash],
        )
        .await?;
    Ok(row.get::<_, bool>(0))
}

/// Files in the `files` table that are not referenced by any pin row.
pub async fn get_unpinned_files(client: &impl GenericClient) -> AlephResult<Vec<StoredFileDb>> {
    let sql = "SELECT f.hash, f.size, f.type FROM files f \
               LEFT JOIN file_pins fp ON f.hash = fp.file_hash \
               WHERE fp.id IS NULL";
    let rows = client.query(sql, &[]).await?;
    Ok(rows.iter().map(StoredFileDb::from_row).collect())
}

/// Upsert a `tx`-type file pin.
pub async fn upsert_tx_file_pin(
    client: &impl GenericClient,
    file_hash: &str,
    tx_hash: &str,
    created: DateTime<Utc>,
) -> AlephResult<()> {
    let sql = "INSERT INTO file_pins(file_hash, tx_hash, type, created) \
               VALUES ($1, $2, 'tx', $3) ON CONFLICT DO NOTHING";
    client
        .execute(sql, &[&file_hash, &tx_hash, &created])
        .await?;
    Ok(())
}

/// Insert a `content`-type file pin.
pub async fn insert_content_file_pin(
    client: &impl GenericClient,
    file_hash: &str,
    owner: &str,
    item_hash: &str,
    created: DateTime<Utc>,
) -> AlephResult<()> {
    let sql = "INSERT INTO file_pins(file_hash, owner, item_hash, type, created) \
               VALUES ($1, $2, $3, 'content', $4)";
    client
        .execute(sql, &[&file_hash, &owner, &item_hash, &created])
        .await?;
    Ok(())
}

/// Insert a `message`-type file pin.
///
/// `owner` is `Option<&str>` to allow NULL owners when restoring a pin from a
/// `grace_period` row whose owner column is null. Pyaleph passes the value
/// through verbatim; coercing to `""` here would corrupt the column.
pub async fn insert_message_file_pin(
    client: &impl GenericClient,
    file_hash: &str,
    owner: Option<&str>,
    item_hash: &str,
    r#ref: Option<&str>,
    created: DateTime<Utc>,
) -> AlephResult<()> {
    let sql = "INSERT INTO file_pins(file_hash, owner, item_hash, type, ref, created) \
               VALUES ($1, $2, $3, 'message', $4, $5)";
    client
        .execute(sql, &[&file_hash, &owner, &item_hash, &r#ref, &created])
        .await?;
    Ok(())
}

/// Count pins for a given file hash.
pub async fn count_file_pins(client: &impl GenericClient, file_hash: &str) -> AlephResult<i64> {
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM file_pins WHERE file_hash = $1",
            &[&file_hash],
        )
        .await?;
    Ok(row.get::<_, i64>(0))
}

/// Return the message-pin item hashes from a list of candidates that actually exist.
pub async fn find_file_pins(
    client: &impl GenericClient,
    item_hashes: &[String],
) -> AlephResult<Vec<String>> {
    let sql = "SELECT item_hash FROM file_pins \
               WHERE type = 'message' AND item_hash = ANY($1)";
    let rows = client.query(sql, &[&item_hashes]).await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Delete a message-type file pin by item hash.
pub async fn delete_file_pin(client: &impl GenericClient, item_hash: &str) -> AlephResult<()> {
    client
        .execute(
            "DELETE FROM file_pins WHERE type = 'message' AND item_hash = $1",
            &[&item_hash],
        )
        .await?;
    Ok(())
}

/// Insert a `grace_period`-type file pin.
pub async fn insert_grace_period_file_pin(
    client: &impl GenericClient,
    file_hash: &str,
    created: DateTime<Utc>,
    delete_by: DateTime<Utc>,
    item_hash: Option<&str>,
    owner: Option<&str>,
    r#ref: Option<&str>,
) -> AlephResult<()> {
    let sql = "INSERT INTO file_pins(item_hash, file_hash, owner, ref, created, type, delete_by) \
               VALUES ($1, $2, $3, $4, $5, 'grace_period', $6)";
    client
        .execute(
            sql,
            &[&item_hash, &file_hash, &owner, &r#ref, &created, &delete_by],
        )
        .await?;
    Ok(())
}

/// Convert a message pin into a grace-period pin or back, refreshing the tag.
///
/// Mirrors `update_file_pin_grace_period`.
pub async fn update_file_pin_grace_period(
    client: &impl GenericClient,
    item_hash: &str,
    delete_by: Option<DateTime<Utc>>,
) -> AlephResult<()> {
    // Grab the existing row that we'll convert, then re-insert as the other kind.
    let (target_select, target_delete) = if delete_by.is_none() {
        (
            "SELECT file_hash, owner, ref, created FROM file_pins \
             WHERE type = 'grace_period' AND item_hash = $1",
            "DELETE FROM file_pins WHERE type = 'grace_period' AND item_hash = $1 \
             RETURNING file_hash, owner, ref, created",
        )
    } else {
        (
            "SELECT file_hash, owner, ref, created FROM file_pins \
             WHERE type = 'message' AND item_hash = $1",
            "DELETE FROM file_pins WHERE type = 'message' AND item_hash = $1 \
             RETURNING file_hash, owner, ref, created",
        )
    };
    let _ = target_select; // kept for clarity / parallel with Python comment
    let row = client.query_opt(target_delete, &[&item_hash]).await?;
    let row = match row {
        Some(r) => r,
        None => return Ok(()),
    };
    let file_hash: String = row.get("file_hash");
    let owner: Option<String> = row.get("owner");
    let r#ref: Option<String> = row.get("ref");
    let created: DateTime<Utc> = row.get("created");

    if let Some(db) = delete_by {
        insert_grace_period_file_pin(
            client,
            &file_hash,
            created,
            db,
            Some(item_hash),
            owner.as_deref(),
            r#ref.as_deref(),
        )
        .await?;
    } else {
        insert_message_file_pin(
            client,
            &file_hash,
            owner.as_deref(),
            item_hash,
            r#ref.as_deref(),
            created,
        )
        .await?;
    }

    let tag = message_file_tag_default(owner.as_deref(), item_hash, r#ref.as_deref());
    refresh_file_tag(client, &FileTag::from(tag)).await?;
    Ok(())
}

/// Delete grace-period file pins whose `delete_by` is before `datetime`.
pub async fn delete_grace_period_file_pins(
    client: &impl GenericClient,
    datetime: DateTime<Utc>,
) -> AlephResult<()> {
    client
        .execute(
            "DELETE FROM file_pins WHERE type = 'grace_period' AND delete_by < $1",
            &[&datetime],
        )
        .await?;
    Ok(())
}

/// Get a single message-type file pin row.
pub async fn get_message_file_pin(
    client: &impl GenericClient,
    item_hash: &str,
) -> AlephResult<Option<FilePinDb>> {
    let sql = format!(
        "SELECT {cols} FROM file_pins WHERE type = 'message' AND item_hash = $1",
        cols = FILE_PIN_COLS
    );
    let row = client.query_opt(&sql, &[&item_hash]).await?;
    Ok(row.as_ref().map(FilePinDb::from_row))
}

/// `(nb_files, total_size)` for an owner, mirroring `get_address_files_stats`.
pub async fn get_address_files_stats(
    client: &impl GenericClient,
    owner: &str,
) -> AlephResult<(i64, i64)> {
    let row = client
        .query_one(
            "SELECT COUNT(*) AS nb_files, COALESCE(SUM(f.size), 0) AS total_size \
             FROM file_pins fp JOIN files f ON fp.file_hash = f.hash \
             WHERE fp.type = 'message' AND fp.owner = $1",
            &[&owner],
        )
        .await?;
    let nb_files: i64 = row.get("nb_files");
    let total_size: i64 = row
        .try_get::<_, Option<i64>>("total_size")
        .ok()
        .flatten()
        .unwrap_or(0);
    Ok((nb_files, total_size))
}

/// API row used by [`get_address_files_for_api`].
#[derive(Debug, Clone)]
pub struct AddressFileRow {
    pub file_hash: String,
    pub created: DateTime<Utc>,
    pub item_hash: String,
    pub size: i64,
    pub r#type: FileType,
}

/// Paginated list of files owned by `owner`.
pub async fn get_address_files_for_api(
    client: &impl GenericClient,
    owner: &str,
    pagination: i64,
    page: i64,
    sort_order: SortOrder,
    after_time: Option<DateTime<Utc>>,
    after_hash: Option<&str>,
    cursor_mode: bool,
) -> AlephResult<Vec<AddressFileRow>> {
    let mut sql = String::from(
        "SELECT fp.file_hash, fp.created, fp.item_hash, f.size, f.type \
         FROM file_pins fp JOIN files f ON fp.file_hash = f.hash \
         WHERE fp.type = 'message' AND fp.owner = $1",
    );
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(owner.to_string())];

    if let Some(at) = after_time {
        let cmp = if sort_order == SortOrder::Descending {
            "<"
        } else {
            ">"
        };
        params.push(Box::new(at));
        let at_idx = params.len();
        // Bind as `Option<String>` so a missing cursor hash is SQL NULL.
        // Defaulting to `""` would erroneously match every tied row through
        // the tie-breaker disjunct (cf. pyaleph cursor semantics).
        let ah: Option<String> = after_hash.map(|s| s.to_string());
        params.push(Box::new(ah));
        let ah_idx = params.len();
        sql.push_str(&format!(
            " AND (fp.created {cmp} ${tidx} \
                  OR (fp.created = ${tidx} AND fp.item_hash > ${hidx}))",
            cmp = cmp,
            tidx = at_idx,
            hidx = ah_idx,
        ));
    }

    let direction = sort_order.to_sql();
    sql.push_str(&format!(
        " ORDER BY fp.created {direction}, fp.item_hash ASC"
    ));

    if after_time.is_none() && pagination > 0 && page > 1 {
        params.push(Box::new((page - 1) * pagination));
        sql.push_str(&format!(" OFFSET ${}", params.len()));
    }
    if pagination > 0 {
        let lim = if after_time.is_some() || cursor_mode {
            pagination + 1
        } else {
            pagination
        };
        params.push(Box::new(lim));
        sql.push_str(&format!(" LIMIT ${}", params.len()));
    }

    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = client.query(&sql, &param_refs).await?;
    let out: Vec<AddressFileRow> = rows
        .iter()
        .map(|r| {
            let type_s: String = r.get("type");
            let ft = serde_json::from_value::<FileType>(serde_json::Value::String(type_s))
                .expect("valid FileType");
            AddressFileRow {
                file_hash: r.get("file_hash"),
                created: r.get("created"),
                item_hash: r.get("item_hash"),
                size: r.get("size"),
                r#type: ft,
            }
        })
        .collect();
    Ok(out)
}

/// Upsert a row in `files`. Mirrors `upsert_file`.
pub async fn upsert_file(
    client: &impl GenericClient,
    file_hash: &str,
    size: i64,
    file_type: FileType,
) -> AlephResult<()> {
    let sql = "INSERT INTO files(hash, size, type) VALUES ($1, $2, $3) \
               ON CONFLICT ON CONSTRAINT files_pkey \
               DO UPDATE SET size = EXCLUDED.size, type = EXCLUDED.type";
    client
        .execute(sql, &[&file_hash, &size, &file_type_value(file_type)])
        .await?;
    Ok(())
}

/// Fetch a file row by hash.
pub async fn get_file(
    client: &impl GenericClient,
    file_hash: &str,
) -> AlephResult<Option<StoredFileDb>> {
    let row = client
        .query_opt(
            "SELECT hash, size, type FROM files WHERE hash = $1",
            &[&file_hash],
        )
        .await?;
    Ok(row.as_ref().map(StoredFileDb::from_row))
}

/// Delete a file row by hash.
pub async fn delete_file(client: &impl GenericClient, file_hash: &str) -> AlephResult<()> {
    client
        .execute("DELETE FROM files WHERE hash = $1", &[&file_hash])
        .await?;
    Ok(())
}

/// Fetch a file tag by exact tag value.
pub async fn get_file_tag(
    client: &impl GenericClient,
    tag: &FileTag,
) -> AlephResult<Option<FileTagDb>> {
    let row = client
        .query_opt(
            "SELECT tag, owner, file_hash, last_updated FROM file_tags WHERE tag = $1",
            &[&tag.as_str()],
        )
        .await?;
    Ok(row.as_ref().map(FileTagDb::from_row))
}

/// Whether a file-pin row with the given `item_hash` exists.
pub async fn file_pin_exists(client: &impl GenericClient, item_hash: &str) -> AlephResult<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM file_pins WHERE item_hash = $1)",
            &[&item_hash],
        )
        .await?;
    Ok(row.get::<_, bool>(0))
}

/// Whether a tag row exists.
pub async fn file_tag_exists(client: &impl GenericClient, tag: &FileTag) -> AlephResult<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM file_tags WHERE tag = $1)",
            &[&tag.as_str()],
        )
        .await?;
    Ok(row.get::<_, bool>(0))
}

/// Find which of the given tags actually exist.
pub async fn find_file_tags(
    client: &impl GenericClient,
    tags: &[FileTag],
) -> AlephResult<Vec<FileTag>> {
    let tag_strs: Vec<String> = tags.iter().map(|t| t.as_str().to_string()).collect();
    let rows = client
        .query(
            "SELECT tag FROM file_tags WHERE tag = ANY($1)",
            &[&tag_strs],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| FileTag::from(r.get::<_, String>("tag")))
        .collect())
}

/// Upsert a file tag, only updating if `last_updated` is more recent.
pub async fn upsert_file_tag(
    client: &impl GenericClient,
    tag: &FileTag,
    owner: &str,
    file_hash: &str,
    last_updated: DateTime<Utc>,
) -> AlephResult<()> {
    let sql = "INSERT INTO file_tags(tag, owner, file_hash, last_updated) \
               VALUES ($1, $2, $3, $4) \
               ON CONFLICT ON CONSTRAINT file_tags_pkey \
               DO UPDATE SET file_hash = EXCLUDED.file_hash, \
                             last_updated = EXCLUDED.last_updated \
               WHERE file_tags.last_updated < EXCLUDED.last_updated";
    client
        .execute(sql, &[&tag.as_str(), &owner, &file_hash, &last_updated])
        .await?;
    Ok(())
}

/// Recompute a single file_tag row from the latest message-pin matching it.
///
/// Mirrors `refresh_file_tag`.
pub async fn refresh_file_tag(client: &impl GenericClient, tag: &FileTag) -> AlephResult<()> {
    let tag_str = tag.as_str();
    client
        .execute("DELETE FROM file_tags WHERE tag = $1", &[&tag_str])
        .await?;

    let sql = "WITH latest AS ( \
                   SELECT COALESCE(ref, item_hash) AS computed_ref, \
                          MAX(created) AS created \
                   FROM file_pins \
                   WHERE type = 'message' \
                     AND COALESCE(ref, item_hash) = $1 \
                   GROUP BY COALESCE(ref, item_hash) \
               ) \
               INSERT INTO file_tags(tag, owner, file_hash, last_updated) \
               SELECT COALESCE(fp.ref, fp.item_hash) AS tag, fp.owner, fp.file_hash, fp.created \
               FROM file_pins fp \
               JOIN latest l \
                   ON COALESCE(fp.ref, fp.item_hash) = l.computed_ref \
                  AND fp.created = l.created \
               WHERE fp.type = 'message' \
               ON CONFLICT ON CONSTRAINT file_tags_pkey \
               DO UPDATE SET file_hash = EXCLUDED.file_hash, \
                             last_updated = EXCLUDED.last_updated";
    client.execute(sql, &[&tag_str]).await?;
    Ok(())
}

/// Build a `FilePinDb` accessor from a typed kind. Convenience helper that
/// callers can use to construct rows.
pub fn make_message_pin(
    file_hash: String,
    owner: Option<String>,
    item_hash: Option<String>,
    r#ref: Option<String>,
    created: DateTime<Utc>,
) -> FilePinDb {
    FilePinDb::message(0, file_hash, created, owner, item_hash, r#ref)
}

/// Constant strings useful to callers introspecting pin types.
pub const PIN_TYPE_MESSAGE: FilePinType = FilePinType::Message;
pub const PIN_TYPE_CONTENT: FilePinType = FilePinType::Content;
pub const PIN_TYPE_TX: FilePinType = FilePinType::Tx;
pub const PIN_TYPE_GRACE_PERIOD: FilePinType = FilePinType::GracePeriod;
