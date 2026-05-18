//! Denormalized message counts (`message_counts` table).
//!
//! Mirrors `src/aleph/db/models/message_counts.py`.
//!
//! Note: the Python column is named `count` but is mapped onto the
//! `row_count` attribute (Python reserved keyword avoidance). We follow the
//! same convention in Rust.

/// Row of the `message_counts` table.
#[derive(Debug, Clone)]
pub struct MessageCountsDb {
    pub r#type: String,
    pub status: String,
    pub sender: String,
    pub owner: String,
    /// Persisted as the `count` column.
    pub row_count: i64,
}

impl Default for MessageCountsDb {
    fn default() -> Self {
        Self {
            r#type: String::new(),
            status: String::new(),
            sender: String::new(),
            owner: String::new(),
            row_count: 0,
        }
    }
}

impl MessageCountsDb {
    /// Build a [`MessageCountsDb`] from a database row.
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            r#type: row.get("type"),
            status: row.get("status"),
            sender: row.get("sender"),
            owner: row.get("owner"),
            row_count: row.get("count"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_python() {
        let mc = MessageCountsDb::default();
        assert!(mc.r#type.is_empty());
        assert!(mc.status.is_empty());
        assert!(mc.sender.is_empty());
        assert!(mc.owner.is_empty());
        assert_eq!(mc.row_count, 0);
    }

    #[test]
    fn construct_with_values() {
        let mc = MessageCountsDb {
            r#type: "POST".into(),
            status: "processed".into(),
            sender: "0xabc".into(),
            owner: "0xdef".into(),
            row_count: 42,
        };
        assert_eq!(mc.row_count, 42);
        assert_eq!(mc.r#type, "POST");
    }
}
