//! JSON serialization helpers.
//!
//! Port of `src/aleph/toolkit/json.py`.
//!
//! Python wraps `orjson`/`json` to provide:
//! - `loads(bytes|str)` -> Any
//! - `dumps(obj, sort_keys=True)` -> bytes (sorted keys; supports
//!   datetime/Decimal/pydantic models)
//!
//! The Rust equivalent uses `serde_json`. `dumps` sorts object keys lexically
//! when `sort_keys` is true (matching `OPT_SORT_KEYS`).

use serde_json::{Map, Value};

/// Re-export of `serde_json::Error`, equivalent to Python `DecodeError`.
pub type DecodeError = serde_json::Error;

/// Serialized JSON output type. Python's `SerializedJson = bytes`.
pub type SerializedJson = Vec<u8>;

/// All possible serialized JSON inputs (bytes or UTF-8 string).
/// Mirrors Python's `SerializedJsonInput = Union[bytes, str]`.
pub enum SerializedJsonInput<'a> {
    Bytes(&'a [u8]),
    Str(&'a str),
}

impl<'a> From<&'a [u8]> for SerializedJsonInput<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::Bytes(value)
    }
}

impl<'a> From<&'a str> for SerializedJsonInput<'a> {
    fn from(value: &'a str) -> Self {
        Self::Str(value)
    }
}

/// Deserialize a JSON document.
///
/// Mirrors `loads(s)`. Accepts either bytes or `&str`.
pub fn loads<'a, I>(input: I) -> Result<Value, DecodeError>
where
    I: Into<SerializedJsonInput<'a>>,
{
    match input.into() {
        SerializedJsonInput::Bytes(b) => serde_json::from_slice(b),
        SerializedJsonInput::Str(s) => serde_json::from_str(s),
    }
}

/// Serialize a value to JSON bytes.
///
/// Mirrors `dumps(obj, sort_keys=True)`. When `sort_keys` is true, all object
/// keys (recursively) are sorted lexically — matching `orjson.OPT_SORT_KEYS`.
pub fn dumps(value: &Value, sort_keys: bool) -> SerializedJson {
    let v = if sort_keys {
        sort_value_keys(value.clone())
    } else {
        value.clone()
    };
    serde_json::to_vec(&v).expect("serde_json::Value always serializable")
}

/// Convenience: serialize to UTF-8 `String` (most call-sites in pyaleph
/// decode the bytes immediately).
pub fn dumps_str(value: &Value, sort_keys: bool) -> String {
    let v = if sort_keys {
        sort_value_keys(value.clone())
    } else {
        value.clone()
    };
    serde_json::to_string(&v).expect("serde_json::Value always serializable")
}

/// Recursively sort all object keys in a value.
fn sort_value_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = Map::new();
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (k, v) in entries {
                sorted.insert(k, sort_value_keys(v));
            }
            Value::Object(sorted)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(sort_value_keys).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_loads_str_and_bytes() {
        let v: Value = loads(r#"{"a":1}"#).unwrap();
        assert_eq!(v["a"], 1);
        let v: Value = loads(&b"{\"a\":1}"[..]).unwrap();
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn test_dumps_sorts_keys() {
        let value = json!({"b": 1, "a": 2, "nested": {"z": 0, "y": 1}});
        let out = dumps_str(&value, true);
        assert_eq!(out, r#"{"a":2,"b":1,"nested":{"y":1,"z":0}}"#);
    }

    #[test]
    fn test_dumps_without_sort() {
        let value = json!({"a": 1});
        let bytes = dumps(&value, false);
        assert_eq!(bytes, br#"{"a":1}"#);
    }

    #[test]
    fn test_round_trip() {
        let value = json!({"nested": [1, 2, {"k": "v"}], "n": 1});
        let bytes = dumps(&value, true);
        let parsed = loads(&bytes[..]).unwrap();
        assert_eq!(parsed, value);
    }
}
