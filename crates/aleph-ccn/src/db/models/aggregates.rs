//! Aggregate tables (`aggregate_elements`, `aggregates`).
//!
//! Mirrors `src/aleph/db/models/aggregates.py`.

use chrono::{DateTime, Utc};
use serde_json::Value;

/// Individual revision of an aggregate. Mirrors `AggregateElementDb`.
#[derive(Debug, Clone)]
pub struct AggregateElementDb {
    pub item_hash: String,
    pub key: String,
    pub owner: String,
    pub content: Value,
    pub creation_datetime: DateTime<Utc>,
}

impl AggregateElementDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            item_hash: row.get("item_hash"),
            key: row.get("key"),
            owner: row.get("owner"),
            content: row.get("content"),
            creation_datetime: row.get("creation_datetime"),
        }
    }
}

/// Compacted aggregate row served to API consumers. Mirrors `AggregateDb`.
#[derive(Debug, Clone)]
pub struct AggregateDb {
    pub key: String,
    pub owner: String,
    pub content: Value,
    pub creation_datetime: DateTime<Utc>,
    pub last_revision_hash: String,
    pub dirty: bool,
}

impl AggregateDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            key: row.get("key"),
            owner: row.get("owner"),
            content: row.get("content"),
            creation_datetime: row.get("creation_datetime"),
            last_revision_hash: row.get("last_revision_hash"),
            dirty: row.get("dirty"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn aggregate_element_construct() {
        let elem = AggregateElementDb {
            item_hash: "deadbeef".into(),
            key: "profile".into(),
            owner: "0xabc".into(),
            content: json!({"name": "bob"}),
            creation_datetime: Utc::now(),
        };
        assert_eq!(elem.key, "profile");
        assert_eq!(elem.content["name"], "bob");
    }

    #[test]
    fn aggregate_construct() {
        let agg = AggregateDb {
            key: "profile".into(),
            owner: "0xabc".into(),
            content: json!({"name": "bob"}),
            creation_datetime: Utc::now(),
            last_revision_hash: "feedface".into(),
            dirty: false,
        };
        assert_eq!(agg.key, "profile");
        assert!(!agg.dirty);
    }
}
