//! Opaque pagination cursors.
//!
//! Port of `src/aleph/toolkit/cursor.py`.
//!
//! Encoded as URL-safe base64 (no padding) of a sorted-key JSON object.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use super::json;

/// Error type returned by cursor decoding helpers. Mirrors Python's
/// `ValueError("Invalid cursor: ...")`.
#[derive(Debug, thiserror::Error)]
#[error("Invalid cursor: {0}")]
pub struct CursorError(pub String);

impl CursorError {
    fn missing_field() -> Self {
        Self("missing required fields".to_string())
    }
    fn new<S: Into<String>>(msg: S) -> Self {
        Self(msg.into())
    }
}

/// Encode an arbitrary value-map into an opaque base64url cursor.
///
/// Mirrors `encode_cursor(values: Dict[str, Any])`.
pub fn encode_cursor(values: &Map<String, Value>) -> String {
    let payload = json::dumps_str(&Value::Object(values.clone()), true);
    URL_SAFE_NO_PAD.encode(payload.as_bytes())
}

/// Decode a cursor produced by [`encode_cursor`] back into a JSON object map.
///
/// Mirrors `decode_cursor(cursor: str)`. Returns `Err(CursorError)` on any
/// malformed input.
pub fn decode_cursor(cursor: &str) -> Result<Map<String, Value>, CursorError> {
    if cursor.is_empty() {
        return Err(CursorError::new("empty string"));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor.as_bytes())
        .map_err(|e| CursorError::new(e.to_string()))?;
    let utf8 = std::str::from_utf8(&decoded).map_err(|e| CursorError::new(e.to_string()))?;
    let value: Value = json::loads(utf8).map_err(|e| CursorError::new(e.to_string()))?;
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(CursorError::new("payload is not a dict")),
    }
}

pub fn datetime_isoformat(dt: DateTime<Utc>) -> String {
    use chrono::Timelike;
    // Python's `datetime.isoformat()` for UTC-localized datetimes yields
    // `YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00` — fixed 6-digit microseconds (no
    // trailing-zero stripping past seconds), and `+00:00` instead of `Z`. When
    // the subsecond component is zero, the entire `.ffffff` is omitted.
    if dt.nanosecond() == 0 {
        dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
    } else {
        dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, false)
    }
}

fn parse_isoformat(s: &str) -> Result<DateTime<Utc>, CursorError> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| CursorError::new(e.to_string()))
}

fn get_field<'a>(map: &'a Map<String, Value>, key: &str) -> Result<&'a Value, CursorError> {
    map.get(key).ok_or_else(CursorError::missing_field)
}

fn get_str(map: &Map<String, Value>, key: &str) -> Result<String, CursorError> {
    let v = get_field(map, key)?;
    match v {
        Value::String(s) => Ok(s.clone()),
        // Mirror Python `str(d[key])` for any JSON scalar.
        other => Ok(other.to_string()),
    }
}

fn get_i64(map: &Map<String, Value>, key: &str) -> Result<i64, CursorError> {
    let v = get_field(map, key)?;
    v.as_i64()
        .ok_or_else(|| CursorError::new(format!("'{key}' is not an integer")))
}

// -- Message cursor (t, h) --------------------------------------------------

pub fn encode_message_cursor(time: DateTime<Utc>, item_hash: &str) -> String {
    let mut map = Map::new();
    map.insert("t".to_string(), Value::String(datetime_isoformat(time)));
    map.insert("h".to_string(), Value::String(item_hash.to_string()));
    encode_cursor(&map)
}

pub fn decode_message_cursor(cursor: &str) -> Result<(DateTime<Utc>, String), CursorError> {
    let map = decode_cursor(cursor)?;
    let t = parse_isoformat(&get_str(&map, "t")?)?;
    let h = get_str(&map, "h")?;
    Ok((t, h))
}

// -- Aggregate cursor (t, k, o) ---------------------------------------------

pub fn encode_aggregate_cursor(time: DateTime<Utc>, key: &str, owner: &str) -> String {
    let mut map = Map::new();
    map.insert("t".to_string(), Value::String(datetime_isoformat(time)));
    map.insert("k".to_string(), Value::String(key.to_string()));
    map.insert("o".to_string(), Value::String(owner.to_string()));
    encode_cursor(&map)
}

pub fn decode_aggregate_cursor(
    cursor: &str,
) -> Result<(DateTime<Utc>, String, String), CursorError> {
    let map = decode_cursor(cursor)?;
    let t = parse_isoformat(&get_str(&map, "t")?)?;
    let k = get_str(&map, "k")?;
    let o = get_str(&map, "o")?;
    Ok((t, k, o))
}

// -- Address cursor (a,) ----------------------------------------------------

pub fn encode_address_cursor(address: &str) -> String {
    let mut map = Map::new();
    map.insert("a".to_string(), Value::String(address.to_string()));
    encode_cursor(&map)
}

pub fn decode_address_cursor(cursor: &str) -> Result<String, CursorError> {
    let map = decode_cursor(cursor)?;
    get_str(&map, "a")
}

// -- Credit history cursor (t, r, i) ----------------------------------------

pub fn encode_credit_history_cursor(
    time: DateTime<Utc>,
    credit_ref: &str,
    credit_index: i64,
) -> String {
    let mut map = Map::new();
    map.insert("t".to_string(), Value::String(datetime_isoformat(time)));
    map.insert("r".to_string(), Value::String(credit_ref.to_string()));
    map.insert("i".to_string(), Value::Number(credit_index.into()));
    encode_cursor(&map)
}

pub fn decode_credit_history_cursor(
    cursor: &str,
) -> Result<(DateTime<Utc>, String, i64), CursorError> {
    let map = decode_cursor(cursor)?;
    let t = parse_isoformat(&get_str(&map, "t")?)?;
    let r = get_str(&map, "r")?;
    let i = get_i64(&map, "i")?;
    Ok((t, r, i))
}

// -- Credit history sort cursor ---------------------------------------------

pub fn encode_credit_history_sort_cursor(
    sort_by: &str,
    sort_value: Value,
    sort_order: i64,
    credit_ref: &str,
    credit_index: i64,
) -> String {
    let value = match sort_value {
        Value::String(s) => Value::String(s),
        // datetimes are represented as ISO strings; callers should pass an
        // already-formatted ISO string in that case (matching Python).
        other => other,
    };
    let mut map = Map::new();
    map.insert("s".to_string(), Value::String(sort_by.to_string()));
    map.insert("v".to_string(), value);
    map.insert("o".to_string(), Value::Number(sort_order.into()));
    map.insert("r".to_string(), Value::String(credit_ref.to_string()));
    map.insert("i".to_string(), Value::Number(credit_index.into()));
    encode_cursor(&map)
}

/// Decoded credit-history-sort cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct CreditHistorySortCursor {
    pub sort_by: String,
    pub sort_value: Value,
    pub sort_order: i64,
    pub credit_ref: String,
    pub credit_index: i64,
}

pub fn decode_credit_history_sort_cursor(
    cursor: &str,
) -> Result<CreditHistorySortCursor, CursorError> {
    let map = decode_cursor(cursor)?;
    if map.contains_key("s") {
        return Ok(CreditHistorySortCursor {
            sort_by: get_str(&map, "s")?,
            sort_value: get_field(&map, "v")?.clone(),
            sort_order: get_i64(&map, "o")?,
            credit_ref: get_str(&map, "r")?,
            credit_index: get_i64(&map, "i")?,
        });
    }
    // Backward compat: old cursor (t, r, i)
    let t = get_field(&map, "t")?.clone();
    let r = get_str(&map, "r")?;
    let i = get_i64(&map, "i")?;
    Ok(CreditHistorySortCursor {
        sort_by: "message_timestamp".to_string(),
        sort_value: t,
        sort_order: -1,
        credit_ref: r,
        credit_index: i,
    })
}

// -- Address stats cursor (v, a) --------------------------------------------

pub fn encode_address_stats_cursor(sort_value: Value, address: &str) -> String {
    let mut map = Map::new();
    map.insert("v".to_string(), sort_value);
    map.insert("a".to_string(), Value::String(address.to_string()));
    encode_cursor(&map)
}

pub fn decode_address_stats_cursor(cursor: &str) -> Result<(Value, String), CursorError> {
    let map = decode_cursor(cursor)?;
    let v = get_field(&map, "v")?.clone();
    let a = get_str(&map, "a")?;
    Ok((v, a))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_encode_decode_message_cursor() {
        let t = Utc.with_ymd_and_hms(2024, 1, 2, 3, 4, 5).unwrap();
        let enc = encode_message_cursor(t, "abc");
        let (t2, h) = decode_message_cursor(&enc).unwrap();
        assert_eq!(t2, t);
        assert_eq!(h, "abc");
    }

    #[test]
    fn test_aggregate_cursor_round_trip() {
        let t = Utc.with_ymd_and_hms(2024, 5, 5, 12, 0, 0).unwrap();
        let enc = encode_aggregate_cursor(t, "mykey", "0xowner");
        let (tt, k, o) = decode_aggregate_cursor(&enc).unwrap();
        assert_eq!(tt, t);
        assert_eq!(k, "mykey");
        assert_eq!(o, "0xowner");
    }

    #[test]
    fn test_address_cursor_round_trip() {
        let enc = encode_address_cursor("0xabc");
        assert_eq!(decode_address_cursor(&enc).unwrap(), "0xabc");
    }

    #[test]
    fn test_credit_history_cursor_round_trip() {
        let t = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let enc = encode_credit_history_cursor(t, "rrr", 42);
        let (tt, r, i) = decode_credit_history_cursor(&enc).unwrap();
        assert_eq!(tt, t);
        assert_eq!(r, "rrr");
        assert_eq!(i, 42);
    }

    #[test]
    fn test_credit_sort_cursor_backward_compat() {
        let t = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let mut map = Map::new();
        map.insert("t".to_string(), Value::String(datetime_isoformat(t)));
        map.insert("r".to_string(), Value::String("r1".to_string()));
        map.insert("i".to_string(), Value::Number(7.into()));
        let old_cursor = encode_cursor(&map);
        let decoded = decode_credit_history_sort_cursor(&old_cursor).unwrap();
        assert_eq!(decoded.sort_by, "message_timestamp");
        assert_eq!(decoded.sort_order, -1);
        assert_eq!(decoded.credit_ref, "r1");
        assert_eq!(decoded.credit_index, 7);
    }

    #[test]
    fn test_credit_sort_cursor_new_format() {
        let enc =
            encode_credit_history_sort_cursor("amount", Value::Number(100.into()), 1, "r1", 8);
        let dec = decode_credit_history_sort_cursor(&enc).unwrap();
        assert_eq!(dec.sort_by, "amount");
        assert_eq!(dec.sort_value, Value::Number(100.into()));
        assert_eq!(dec.sort_order, 1);
    }

    #[test]
    fn test_address_stats_cursor_round_trip() {
        let enc = encode_address_stats_cursor(Value::Number(5.into()), "0xabc");
        let (v, a) = decode_address_stats_cursor(&enc).unwrap();
        assert_eq!(v, Value::Number(5.into()));
        assert_eq!(a, "0xabc");
    }

    #[test]
    fn test_decode_empty_errors() {
        assert!(decode_cursor("").is_err());
    }

    #[test]
    fn test_decode_invalid_errors() {
        assert!(decode_cursor("!!!").is_err());
    }
}
