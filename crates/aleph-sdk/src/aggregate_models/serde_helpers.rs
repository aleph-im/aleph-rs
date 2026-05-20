//! Shared serde helpers for aggregate model deserialization.

/// Lenient deserializer for `updated_at` / `created_at` epoch-seconds fields
/// on dashboard-owned aggregates (`websites`, `domains`, ...).
///
/// The spec says these are floats (epoch seconds), and that's what this CLI
/// writes. The frontend dashboard, however, often persists them as ISO-8601
/// strings (`"2026-01-18T22:30:40.691Z"`). Accept either on the read path so
/// the CLI can list/show entries created by either tool; always serialize as
/// f64 epoch seconds.
pub mod epoch_secs_lenient {
    use chrono::DateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &f64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(*value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Float(f64),
            Int(i64),
            Str(String),
        }

        match Raw::deserialize(d)? {
            Raw::Float(v) => Ok(v),
            Raw::Int(v) => Ok(v as f64),
            Raw::Str(s) => DateTime::parse_from_rfc3339(&s)
                .map(|dt| dt.timestamp_micros() as f64 / 1_000_000.0)
                .map_err(|e| {
                    serde::de::Error::custom(format!(
                        "expected epoch seconds (f64) or RFC-3339 timestamp, got '{s}': {e}"
                    ))
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Wrap {
        #[serde(with = "super::epoch_secs_lenient")]
        t: f64,
    }

    #[test]
    fn accepts_float() {
        let w: Wrap = serde_json::from_value(serde_json::json!({"t": 1714000000.5})).unwrap();
        assert_eq!(w.t, 1714000000.5);
    }

    #[test]
    fn accepts_integer() {
        let w: Wrap = serde_json::from_value(serde_json::json!({"t": 1714000000})).unwrap();
        assert_eq!(w.t, 1714000000.0);
    }

    #[test]
    fn accepts_rfc3339_with_millis() {
        let w: Wrap =
            serde_json::from_value(serde_json::json!({"t": "2026-01-18T22:30:40.691Z"})).unwrap();
        // 2026-01-18T22:30:40.691Z = 1768775440.691 epoch seconds
        assert!((w.t - 1768775440.691).abs() < 1e-3, "got {}", w.t);
    }

    #[test]
    fn accepts_rfc3339_with_offset() {
        let w: Wrap =
            serde_json::from_value(serde_json::json!({"t": "2026-01-18T23:30:40+01:00"})).unwrap();
        // Same instant as 2026-01-18T22:30:40Z.
        assert!((w.t - 1768775440.0).abs() < 1e-3, "got {}", w.t);
    }

    #[test]
    fn rejects_garbage_string() {
        let err = serde_json::from_value::<Wrap>(serde_json::json!({"t": "not a date"}))
            .unwrap_err()
            .to_string();
        assert!(err.contains("RFC-3339"), "unexpected error: {err}");
    }

    #[test]
    fn round_trips_as_float() {
        let w = Wrap { t: 123.5 };
        let json = serde_json::to_value(&w).unwrap();
        assert_eq!(json, serde_json::json!({"t": 123.5}));
    }
}
