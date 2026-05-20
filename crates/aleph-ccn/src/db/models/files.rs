//! Stored files, pins and tags (`files`, `file_pins`, `file_tags`).
//!
//! Mirrors `src/aleph/db/models/files.py`. The Python module uses
//! single-table inheritance with a `type` polymorphic discriminator. In Rust
//! we keep a single `FilePinDb` carrying all columns plus a [`FilePinType`]
//! tag — the same shape the DB sees.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::files::{FileTag, FileType};
use crate::{AlephError, AlephResult};

fn file_type_from_text(s: &str) -> FileType {
    try_file_type_from_text(s).unwrap_or_else(|_| panic!("unknown FileType in DB: {s}"))
}

pub(crate) fn try_file_type_from_text(s: &str) -> AlephResult<FileType> {
    serde_json::from_value::<FileType>(serde_json::Value::String(s.to_string()))
        .map_err(|_| AlephError::InvalidMessage(format!("unknown FileType in DB: {s}")))
}

/// Polymorphic identity for a file pin. Mirrors Python `FilePinType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FilePinType {
    /// The file that contains a non-inline message's `content`.
    #[serde(rename = "content")]
    Content,
    /// A file pinned by a STORE-like message.
    #[serde(rename = "message")]
    Message,
    /// A file containing a batch of sync messages.
    #[serde(rename = "tx")]
    Tx,
    /// A file in grace period (kept around briefly even though nobody pays
    /// for it).
    #[serde(rename = "grace_period")]
    GracePeriod,
}

impl FilePinType {
    pub fn as_value_str(self) -> &'static str {
        match self {
            FilePinType::Content => "content",
            FilePinType::Message => "message",
            FilePinType::Tx => "tx",
            FilePinType::GracePeriod => "grace_period",
        }
    }
}

impl TryFrom<&str> for FilePinType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "content" => Ok(FilePinType::Content),
            "message" => Ok(FilePinType::Message),
            "tx" => Ok(FilePinType::Tx),
            "grace_period" => Ok(FilePinType::GracePeriod),
            other => Err(format!("unknown FilePinType: {other}")),
        }
    }
}

impl std::fmt::Display for FilePinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_value_str())
    }
}

/// Row of the `files` table. Mirrors `StoredFileDb`.
#[derive(Debug, Clone)]
pub struct StoredFileDb {
    pub hash: String,
    pub size: i64,
    pub r#type: FileType,
}

impl StoredFileDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self::try_from_row(row).expect("valid StoredFileDb row")
    }

    pub fn try_from_row(row: &tokio_postgres::Row) -> AlephResult<Self> {
        let type_s: String = row.get("type");
        Ok(Self {
            hash: row.get("hash"),
            size: row.get("size"),
            r#type: try_file_type_from_text(&type_s)?,
        })
    }
}

/// Row of the `file_tags` table.
#[derive(Debug, Clone)]
pub struct FileTagDb {
    pub tag: FileTag,
    pub owner: String,
    pub file_hash: String,
    pub last_updated: DateTime<Utc>,
}

impl FileTagDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let tag_s: String = row.get("tag");
        Self {
            tag: FileTag::from(tag_s),
            owner: row.get("owner"),
            file_hash: row.get("file_hash"),
            last_updated: row.get("last_updated"),
        }
    }
}

/// Row of the `file_pins` table.
///
/// Polymorphic columns from the Python subclass tree are kept here as
/// optional fields:
///
/// * `tx_hash`     — populated by `TxFilePinDb`.
/// * `delete_by`   — populated by `GracePeriodFilePinDb`.
/// * `owner`,`item_hash`,`ref` — populated by `MessageFilePinDb`/`ContentFilePinDb`.
#[derive(Debug, Clone)]
pub struct FilePinDb {
    pub id: i64,
    pub file_hash: String,
    pub created: DateTime<Utc>,
    pub r#type: FilePinType,
    pub owner: Option<String>,
    pub item_hash: Option<String>,
    pub r#ref: Option<String>,
    pub tx_hash: Option<String>,
    pub delete_by: Option<DateTime<Utc>>,
}

impl FilePinDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self::try_from_row(row).expect("valid FilePinDb row")
    }

    pub fn try_from_row(row: &tokio_postgres::Row) -> AlephResult<Self> {
        let type_s: String = row.get("type");
        let pin_type = FilePinType::try_from(type_s.as_str())
            .map_err(|e| AlephError::InvalidMessage(format!("{e} in DB")))?;
        Ok(Self {
            id: row.get("id"),
            file_hash: row.get("file_hash"),
            created: row.get("created"),
            r#type: pin_type,
            owner: row.get("owner"),
            item_hash: row.get("item_hash"),
            r#ref: row.get("ref"),
            tx_hash: row.try_get("tx_hash").ok().flatten(),
            delete_by: row.try_get("delete_by").ok().flatten(),
        })
    }

    /// Constructor matching Python's `TxFilePinDb(...)`.
    pub fn tx(id: i64, file_hash: String, created: DateTime<Utc>, tx_hash: Option<String>) -> Self {
        Self {
            id,
            file_hash,
            created,
            r#type: FilePinType::Tx,
            owner: None,
            item_hash: None,
            r#ref: None,
            tx_hash,
            delete_by: None,
        }
    }

    /// Constructor matching Python's `MessageFilePinDb(...)`.
    pub fn message(
        id: i64,
        file_hash: String,
        created: DateTime<Utc>,
        owner: Option<String>,
        item_hash: Option<String>,
        r#ref: Option<String>,
    ) -> Self {
        Self {
            id,
            file_hash,
            created,
            r#type: FilePinType::Message,
            owner,
            item_hash,
            r#ref,
            tx_hash: None,
            delete_by: None,
        }
    }

    /// Constructor matching Python's `ContentFilePinDb(...)`.
    pub fn content(
        id: i64,
        file_hash: String,
        created: DateTime<Utc>,
        owner: Option<String>,
        item_hash: Option<String>,
    ) -> Self {
        Self {
            id,
            file_hash,
            created,
            r#type: FilePinType::Content,
            owner,
            item_hash,
            r#ref: None,
            tx_hash: None,
            delete_by: None,
        }
    }

    /// Constructor matching Python's `GracePeriodFilePinDb(...)`.
    pub fn grace_period(
        id: i64,
        file_hash: String,
        created: DateTime<Utc>,
        owner: Option<String>,
        item_hash: Option<String>,
        r#ref: Option<String>,
        delete_by: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            file_hash,
            created,
            r#type: FilePinType::GracePeriod,
            owner,
            item_hash,
            r#ref,
            tx_hash: None,
            delete_by,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_pin_type_roundtrip() {
        for variant in [
            FilePinType::Content,
            FilePinType::Message,
            FilePinType::Tx,
            FilePinType::GracePeriod,
        ] {
            let s = variant.as_value_str();
            assert_eq!(FilePinType::try_from(s).unwrap(), variant);
            let json = serde_json::to_string(&variant).unwrap();
            let back: FilePinType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
        assert!(FilePinType::try_from("nope").is_err());
    }

    #[test]
    fn invalid_file_type_returns_error() {
        assert!(try_file_type_from_text("nope").is_err());
    }

    #[test]
    fn stored_file_construct() {
        let f = StoredFileDb {
            hash: "deadbeef".into(),
            size: 1024,
            r#type: FileType::File,
        };
        assert_eq!(f.size, 1024);
        assert_eq!(f.r#type, FileType::File);
    }

    #[test]
    fn file_tag_construct() {
        let t = FileTagDb {
            tag: FileTag::from("user/profile"),
            owner: "0xabc".into(),
            file_hash: "deadbeef".into(),
            last_updated: Utc::now(),
        };
        assert_eq!(t.tag.as_str(), "user/profile");
    }

    #[test]
    fn file_pin_builders() {
        let now = Utc::now();
        let p = FilePinDb::tx(1, "h".into(), now, Some("0xtx".into()));
        assert_eq!(p.r#type, FilePinType::Tx);
        assert_eq!(p.tx_hash.as_deref(), Some("0xtx"));
        assert!(p.delete_by.is_none());

        let p = FilePinDb::message(
            2,
            "h2".into(),
            now,
            Some("0xowner".into()),
            Some("0xitem".into()),
            Some("0xref".into()),
        );
        assert_eq!(p.r#type, FilePinType::Message);
        assert_eq!(p.owner.as_deref(), Some("0xowner"));

        let later = now + chrono::Duration::hours(1);
        let p = FilePinDb::grace_period(3, "h3".into(), now, None, None, None, Some(later));
        assert_eq!(p.r#type, FilePinType::GracePeriod);
        assert_eq!(p.delete_by, Some(later));
    }
}
