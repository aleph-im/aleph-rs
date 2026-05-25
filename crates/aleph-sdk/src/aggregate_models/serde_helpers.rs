//! Shared serde helpers for aggregate model deserialization.

use chrono::DateTime;
use serde::Deserialize;

/// Internal shape used by both [`epoch_secs_lenient`] and
/// [`option_epoch_secs_lenient`] to accept either a numeric epoch or an
/// RFC-3339 string on the wire.
#[derive(Deserialize)]
#[serde(untagged)]
enum LenientEpoch {
    Float(f64),
    Int(i64),
    Str(String),
}

fn lenient_epoch_to_f64<E: serde::de::Error>(raw: LenientEpoch) -> Result<f64, E> {
    match raw {
        LenientEpoch::Float(v) => Ok(v),
        LenientEpoch::Int(v) => Ok(v as f64),
        LenientEpoch::Str(s) => DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.timestamp_micros() as f64 / 1_000_000.0)
            .map_err(|e| {
                serde::de::Error::custom(format!(
                    "expected epoch seconds (f64) or RFC-3339 timestamp, got '{s}': {e}"
                ))
            }),
    }
}

/// Lenient deserializer for `updated_at` / `created_at` epoch-seconds fields
/// on dashboard-owned aggregates (`websites`, `domains`, ...).
///
/// The spec says these are floats (epoch seconds), and that's what this CLI
/// writes. The frontend dashboard, however, often persists them as ISO-8601
/// strings (`"2026-01-18T22:30:40.691Z"`). Accept either on the read path so
/// the CLI can list/show entries created by either tool; always serialize as
/// f64 epoch seconds.
pub mod epoch_secs_lenient {
    use super::{LenientEpoch, lenient_epoch_to_f64};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &f64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(*value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        lenient_epoch_to_f64(LenientEpoch::deserialize(d)?)
    }
}

/// `Option<f64>` variant of [`epoch_secs_lenient`] for entries where the
/// field may be missing or explicitly `null` on the wire. Combine with
/// `#[serde(default, skip_serializing_if = "Option::is_none")]` so absence and
/// null both deserialize to `None`, and `None` round-trips as omission.
pub mod option_epoch_secs_lenient {
    use super::{LenientEpoch, lenient_epoch_to_f64};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Option<f64>, s: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(v) => s.serialize_f64(*v),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
        match Option::<LenientEpoch>::deserialize(d)? {
            None => Ok(None),
            Some(raw) => lenient_epoch_to_f64(raw).map(Some),
        }
    }
}

/// `#[serde(deserialize_with)]` helper that treats an explicit `null` on the
/// wire as `T::default()` rather than a deserialization error. Pair with
/// `#[serde(default)]` so a missing field also defaults.
///
/// Use when the CCN/dashboard writes `null` for "no extras" on a field whose
/// Rust type is a struct with optional inner fields (e.g. `DomainOptions`).
pub fn default_on_null<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + serde::Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
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

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct OptWrap {
        #[serde(
            default,
            with = "super::option_epoch_secs_lenient",
            skip_serializing_if = "Option::is_none"
        )]
        t: Option<f64>,
    }

    #[test]
    fn option_accepts_missing_field() {
        let w: OptWrap = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(w.t, None);
    }

    #[test]
    fn option_accepts_explicit_null() {
        let w: OptWrap = serde_json::from_value(serde_json::json!({"t": null})).unwrap();
        assert_eq!(w.t, None);
    }

    #[test]
    fn option_accepts_float_and_iso() {
        let a: OptWrap = serde_json::from_value(serde_json::json!({"t": 1714000000.5})).unwrap();
        assert_eq!(a.t, Some(1714000000.5));
        let b: OptWrap =
            serde_json::from_value(serde_json::json!({"t": "2026-01-18T22:30:40.691Z"})).unwrap();
        assert!((b.t.unwrap() - 1768775440.691).abs() < 1e-3);
    }

    #[test]
    fn option_round_trip_some_serializes_as_number_none_omits() {
        let json = serde_json::to_value(OptWrap { t: Some(42.0) }).unwrap();
        assert_eq!(json, serde_json::json!({"t": 42.0}));
        let json = serde_json::to_value(OptWrap { t: None }).unwrap();
        assert_eq!(json, serde_json::json!({}));
    }

    #[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
    struct Inner {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        v: Option<String>,
    }

    #[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
    struct DonWrap {
        #[serde(default, deserialize_with = "super::default_on_null")]
        inner: Inner,
    }

    #[test]
    fn default_on_null_handles_null() {
        let w: DonWrap = serde_json::from_value(serde_json::json!({"inner": null})).unwrap();
        assert_eq!(w.inner, Inner::default());
    }

    #[test]
    fn default_on_null_handles_missing() {
        let w: DonWrap = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(w.inner, Inner::default());
    }

    #[test]
    fn default_on_null_handles_present_value() {
        let w: DonWrap =
            serde_json::from_value(serde_json::json!({"inner": {"v": "hello"}})).unwrap();
        assert_eq!(
            w.inner,
            Inner {
                v: Some("hello".into())
            }
        );
    }
}
