//! Timestamp helpers.
//!
//! Port of `src/aleph/toolkit/timestamp.py`.
//!
//! Python uses naive UNIX epoch floats and `datetime.datetime` localized to
//! UTC. We mirror that here with `chrono::DateTime<Utc>` and conversions
//! from/to `f64` POSIX timestamps (seconds with sub-second precision).

use chrono::{DateTime, TimeZone, Utc};

/// Error returned by [`try_timestamp_to_datetime`] when the value falls outside
/// chrono's representable range.
#[derive(Debug, Clone, thiserror::Error)]
#[error("timestamp {0} out of range")]
pub struct TimestampError(pub f64);

/// Transform a UNIX timestamp (seconds, possibly fractional) into a
/// UTC-localized [`DateTime`].
///
/// Mirrors `timestamp_to_datetime(timestamp: float)` from
/// `aleph.toolkit.timestamp`. Python raises `OverflowError` / `OSError` on
/// out-of-range input; we return [`TimestampError`] so callers can propagate or
/// log-and-skip without taking down the worker.
pub fn try_timestamp_to_datetime(timestamp: f64) -> Result<DateTime<Utc>, TimestampError> {
    let secs = timestamp.floor() as i64;
    // Clamp the nanosecond component to its valid range. `(timestamp -
    // floor(timestamp)) * 1e9` can yield values near 1e9 due to floating
    // point rounding, so we clamp to 999_999_999.
    let nsecs_raw = ((timestamp - timestamp.floor()) * 1_000_000_000.0).round() as i64;
    let nsecs = nsecs_raw.clamp(0, 999_999_999) as u32;
    match Utc.timestamp_opt(secs, nsecs) {
        chrono::LocalResult::Single(dt) => Ok(dt),
        chrono::LocalResult::Ambiguous(_, latest) => Ok(latest),
        chrono::LocalResult::None => Err(TimestampError(timestamp)),
    }
}

/// Infallible variant that returns the UNIX epoch on out-of-range values and
/// logs a warning. Callers that need to detect failure should use
/// [`try_timestamp_to_datetime`] directly.
pub fn timestamp_to_datetime(timestamp: f64) -> DateTime<Utc> {
    match try_timestamp_to_datetime(timestamp) {
        Ok(dt) => dt,
        Err(e) => {
            tracing::warn!("{e}; falling back to epoch");
            Utc.timestamp_opt(0, 0).single().unwrap_or_default()
        }
    }
}

/// One of: an existing datetime, a POSIX timestamp, or nothing.
///
/// Used to mirror Python's untyped `Optional[Union[float, datetime]]` argument
/// for [`coerce_to_datetime`].
#[derive(Debug, Clone, Copy)]
pub enum DatetimeOrTimestamp {
    /// An already-typed UTC datetime.
    Datetime(DateTime<Utc>),
    /// A POSIX timestamp (seconds since epoch).
    Timestamp(f64),
    /// Missing value.
    None,
}

impl From<DateTime<Utc>> for DatetimeOrTimestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self::Datetime(value)
    }
}

impl From<f64> for DatetimeOrTimestamp {
    fn from(value: f64) -> Self {
        Self::Timestamp(value)
    }
}

impl<T> From<Option<T>> for DatetimeOrTimestamp
where
    T: Into<DatetimeOrTimestamp>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Self::None,
        }
    }
}

/// Coerce a datetime-or-timestamp value into an optional datetime.
///
/// Mirrors `coerce_to_datetime` from `aleph.toolkit.timestamp`. Python returns
/// `None` for either `None` or a falsy `0` timestamp; we replicate by treating
/// `0.0` as None (matching Python's `not 0`).
pub fn coerce_to_datetime(value: DatetimeOrTimestamp) -> Option<DateTime<Utc>> {
    match value {
        DatetimeOrTimestamp::None => None,
        DatetimeOrTimestamp::Timestamp(t) if t == 0.0 => None,
        DatetimeOrTimestamp::Timestamp(t) => Some(timestamp_to_datetime(t)),
        DatetimeOrTimestamp::Datetime(dt) => Some(dt),
    }
}

/// Return the current time as a UTC-localized [`DateTime`].
///
/// Mirrors `utc_now()` from `aleph.toolkit.timestamp`.
pub fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_to_datetime_round_trip() {
        let ts: f64 = 1_700_000_000.5;
        let dt = timestamp_to_datetime(ts);
        assert_eq!(dt.timestamp(), 1_700_000_000);
        assert_eq!(dt.timestamp_subsec_millis(), 500);
    }

    #[test]
    fn test_coerce_to_datetime_none() {
        assert!(coerce_to_datetime(DatetimeOrTimestamp::None).is_none());
        assert!(coerce_to_datetime(DatetimeOrTimestamp::Timestamp(0.0)).is_none());
    }

    #[test]
    fn test_coerce_to_datetime_some_timestamp() {
        let dt = coerce_to_datetime(DatetimeOrTimestamp::Timestamp(1_700_000_000.0)).unwrap();
        assert_eq!(dt.timestamp(), 1_700_000_000);
    }

    #[test]
    fn test_coerce_to_datetime_some_datetime() {
        let dt = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let coerced = coerce_to_datetime(DatetimeOrTimestamp::Datetime(dt)).unwrap();
        assert_eq!(coerced, dt);
    }

    #[test]
    fn test_utc_now_monotonic() {
        let before = Utc::now();
        let now = utc_now();
        let after = Utc::now();
        assert!(now >= before);
        assert!(now <= after);
    }

    #[test]
    fn test_from_option_conversion() {
        let opt_dt: Option<DateTime<Utc>> = Some(Utc.timestamp_opt(1, 0).unwrap());
        let coerced: DatetimeOrTimestamp = opt_dt.into();
        assert!(matches!(coerced, DatetimeOrTimestamp::Datetime(_)));
        let none: Option<f64> = None;
        let coerced: DatetimeOrTimestamp = none.into();
        assert!(matches!(coerced, DatetimeOrTimestamp::None));
    }
}
