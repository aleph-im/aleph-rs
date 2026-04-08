use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json;

/// Maximum items per page in cursor mode (matches pyaleph).
pub const CURSOR_MAX_PAGINATION: u32 = 200;

/// Encode a cursor payload dict as a base64url string (no padding).
/// Keys are sorted alphabetically to match the pyaleph encoding.
pub fn encode_message_cursor(time: f64, item_hash: &str) -> String {
    // Format time as ISO 8601 to match pyaleph cursor format.
    use chrono::{TimeZone, Utc};
    let secs = time as i64;
    let nanos = ((time - secs as f64) * 1_000_000_000.0) as u32;
    let iso = match Utc.timestamp_opt(secs, nanos) {
        chrono::LocalResult::Single(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        _ => format!("{time}"),
    };
    // Keys sorted: "h" < "t"
    let payload = format!(r#"{{"h":"{item_hash}","t":"{iso}"}}"#);
    URL_SAFE_NO_PAD.encode(payload.as_bytes())
}

/// Decoded cursor fields for messages/posts.
pub struct MessageCursor {
    pub time_iso: String,
    pub time_f64: f64,
    pub item_hash: String,
}

/// Decode a cursor string back to (time, item_hash).
/// Returns Err with a human-readable message on malformed cursors.
pub fn decode_message_cursor(cursor: &str) -> Result<MessageCursor, String> {
    if cursor.is_empty() {
        return Err("Invalid cursor: empty string".into());
    }
    // Add padding back
    let padded = match cursor.len() % 4 {
        2 => format!("{cursor}=="),
        3 => format!("{cursor}="),
        _ => cursor.to_string(),
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(padded.trim_end_matches('='))
        .or_else(|_| URL_SAFE_NO_PAD.decode(cursor))
        .map_err(|e| format!("Invalid cursor: {e}"))?;
    let parsed: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("Invalid cursor: {e}"))?;
    let obj = parsed.as_object().ok_or("Invalid cursor: not an object")?;
    let time_iso = obj
        .get("t")
        .and_then(|v| v.as_str())
        .ok_or("Invalid cursor: missing 't'")?
        .to_string();
    let item_hash = obj
        .get("h")
        .and_then(|v| v.as_str())
        .ok_or("Invalid cursor: missing 'h'")?
        .to_string();

    // Parse ISO time to f64 unix timestamp
    let time_f64 = chrono::DateTime::parse_from_rfc3339(&time_iso)
        .map(|dt| dt.timestamp() as f64 + dt.timestamp_subsec_nanos() as f64 / 1_000_000_000.0)
        .map_err(|e| format!("Invalid cursor time: {e}"))?;

    Ok(MessageCursor {
        time_iso,
        time_f64,
        item_hash,
    })
}

/// Validate and cap pagination for cursor mode.
/// Returns Err if pagination=0 with cursor.
pub fn validate_cursor_pagination(pagination: u32) -> Result<u32, String> {
    if pagination == 0 {
        return Err("pagination=0 is not allowed with cursor-based pagination".into());
    }
    Ok(pagination.min(CURSOR_MAX_PAGINATION))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let cursor = encode_message_cursor(1_700_000_000.0, "abc123");
        let decoded = decode_message_cursor(&cursor).unwrap();
        assert_eq!(decoded.item_hash, "abc123");
        assert!((decoded.time_f64 - 1_700_000_000.0).abs() < 1.0);
    }

    #[test]
    fn test_decode_invalid_base64() {
        assert!(decode_message_cursor("!!!invalid!!!").is_err());
    }

    #[test]
    fn test_decode_empty() {
        assert!(decode_message_cursor("").is_err());
    }

    #[test]
    fn test_decode_missing_fields() {
        let payload = r#"{"h":"abc"}"#;
        let encoded = URL_SAFE_NO_PAD.encode(payload.as_bytes());
        assert!(decode_message_cursor(&encoded).is_err());
    }

    #[test]
    fn test_validate_cursor_pagination() {
        assert!(validate_cursor_pagination(0).is_err());
        assert_eq!(validate_cursor_pagination(50).unwrap(), 50);
        assert_eq!(validate_cursor_pagination(300).unwrap(), 200);
    }
}
