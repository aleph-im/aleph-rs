//! Counters over `message_counts`. Mirrors `aleph/db/accessors/address_stats.py`.

use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::types::message_status::MessageStatus;

/// Escape SQL LIKE/ILIKE wildcard characters to prevent pattern injection.
///
/// Mirrors Python `escape_like_pattern`: escapes `\` first, then `%` and `_`.
pub fn escape_like_pattern(pattern: &str) -> String {
    pattern
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn message_status_value(status: MessageStatus) -> &'static str {
    match status {
        MessageStatus::Pending => "pending",
        MessageStatus::Processed => "processed",
        MessageStatus::Rejected => "rejected",
        MessageStatus::Forgotten => "forgotten",
        MessageStatus::Removing => "removing",
        MessageStatus::Removed => "removed",
    }
}

/// Count the total number of unique addresses using the message_counts table.
///
/// Mirrors `count_address_stats`.
pub async fn count_address_stats(
    client: &impl GenericClient,
    address_contains: Option<&str>,
) -> AlephResult<i64> {
    let processed = message_status_value(MessageStatus::Processed);
    let row = if let Some(pat) = address_contains {
        let escaped = format!("%{}%", escape_like_pattern(pat));
        let sql = "SELECT COUNT(*) FROM (\
                       SELECT sender FROM message_counts \
                       WHERE status = $1 AND owner = '' AND sender <> '' AND type <> '' \
                         AND sender ILIKE $2 ESCAPE '\\' \
                       GROUP BY sender\
                   ) AS sub";
        client.query_one(sql, &[&processed, &escaped]).await?
    } else {
        let sql = "SELECT COUNT(*) FROM (\
                       SELECT sender FROM message_counts \
                       WHERE status = $1 AND owner = '' AND sender <> '' AND type <> '' \
                       GROUP BY sender\
                   ) AS sub";
        client.query_one(sql, &[&processed]).await?
    };
    let total: i64 = row.get(0);
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_like_pattern_escapes_backslash_first() {
        assert_eq!(escape_like_pattern("foo"), "foo");
        assert_eq!(escape_like_pattern("a%b"), "a\\%b");
        assert_eq!(escape_like_pattern("a_b"), "a\\_b");
        // Backslash must be escaped first so wildcard escapes survive.
        assert_eq!(escape_like_pattern("\\%"), "\\\\\\%");
    }
}
