//! Post records (`posts` table).
//!
//! Mirrors `src/aleph/db/models/posts.py`.

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::types::channel::Channel;

/// Row of the `posts` table.
#[derive(Debug, Clone)]
pub struct PostDb {
    pub item_hash: String,
    pub owner: String,
    pub r#type: Option<String>,
    pub r#ref: Option<String>,
    pub amends: Option<String>,
    pub channel: Option<Channel>,
    pub content: Value,
    pub creation_datetime: DateTime<Utc>,
    pub latest_amend: Option<String>,
    pub tags: Option<Vec<String>>,
}

impl PostDb {
    /// Build a [`PostDb`] from a database row.
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let channel: Option<String> = row.get("channel");
        Self {
            item_hash: row.get("item_hash"),
            owner: row.get("owner"),
            r#type: row.get("type"),
            r#ref: row.get("ref"),
            amends: row.get("amends"),
            channel: channel.map(Channel::from),
            content: row.get("content"),
            creation_datetime: row.get("creation_datetime"),
            latest_amend: row.get("latest_amend"),
            tags: row.get("tags"),
        }
    }

    /// Extract POST tags from a content JSON dict the same way the Python
    /// `PostDb.__init__` does (`content['tags']` → list of strings, or `None`).
    pub fn extract_post_tags(content: &Value) -> Option<Vec<String>> {
        let raw = content.get("tags")?.as_array()?;
        if raw.is_empty() {
            return None;
        }
        let cleaned: Vec<String> = raw
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        if cleaned.is_empty() {
            None
        } else {
            Some(cleaned)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_post_tags_basic() {
        let c = json!({"tags": ["a", "b"]});
        assert_eq!(
            PostDb::extract_post_tags(&c),
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn extract_post_tags_filters_non_strings() {
        let c = json!({"tags": ["a", 1, true, "b"]});
        assert_eq!(
            PostDb::extract_post_tags(&c),
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn extract_post_tags_none() {
        assert!(PostDb::extract_post_tags(&json!({})).is_none());
        assert!(PostDb::extract_post_tags(&json!({"tags": []})).is_none());
        assert!(PostDb::extract_post_tags(&json!({"tags": [1, 2]})).is_none());
    }

    #[test]
    fn post_db_construct() {
        let p = PostDb {
            item_hash: "deadbeef".into(),
            owner: "0xabc".into(),
            r#type: Some("custom".into()),
            r#ref: None,
            amends: None,
            channel: Some(Channel::from("TEST".to_string())),
            content: json!({"body": "hi"}),
            creation_datetime: Utc::now(),
            latest_amend: None,
            tags: Some(vec!["a".into()]),
        };
        assert_eq!(p.r#type.as_deref(), Some("custom"));
        assert_eq!(p.tags.as_ref().unwrap().len(), 1);
    }
}
