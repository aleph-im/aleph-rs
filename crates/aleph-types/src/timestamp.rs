use chrono::{DateTime, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, thiserror::Error)]
pub enum TimestampError {
    #[error("Timestamp out of bounds")]
    OutOfBounds,
    #[error("Failed to parse timestamp")]
    ParseError,
}

/// Timestamp type on the Aleph Cloud network.
///
/// Time in Aleph messages is usually represented as a floating-point epoch timestamp. This type
/// keeps the floating point representation for fast serialization/deserialization and to avoid
/// loss of precision, but provides helpers to convert to datetime for human readability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Timestamp(f64);

impl From<f64> for Timestamp {
    fn from(value: f64) -> Self {
        Self(value)
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(datetime: DateTime<Utc>) -> Self {
        Self(datetime.timestamp() as f64 + datetime.nanosecond() as f64 / 1_000_000_000.0)
    }
}

impl Timestamp {
    pub fn to_datetime(&self) -> Result<DateTime<Utc>, TimestampError> {
        let secs = self.0.floor() as i64;
        let nsecs = ((self.0.fract() * 1_000_000_000.0).round() as u32).min(999_999_999);
        match Utc.timestamp_opt(secs, nsecs) {
            chrono::LocalResult::Single(dt) => Ok(dt),
            chrono::LocalResult::Ambiguous(earliest, latest) => {
                panic!(
                    "Ambiguous timestamp (earliest: {} - latest: {}), which should be impossible when importing a timestamp to UTC datetime",
                    earliest.to_rfc3339(),
                    latest.to_rfc3339()
                )
            }
            chrono::LocalResult::None => Err(TimestampError::OutOfBounds),
        }
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let datetime_str = match self.to_datetime() {
            Ok(dt) => dt.to_rfc3339(),
            Err(_) => "invalid datetime".to_string(),
        };

        write!(f, "{} ({})", self.0, datetime_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_timestamp_serialization() {
        let dt = Utc.timestamp_opt(1635789600, 500_000_000).unwrap();
        let timestamp = Timestamp::from(dt);
        let serialized = serde_json::to_string(&timestamp).unwrap();
        assert_eq!(serialized, "1635789600.5");
    }

    #[test]
    fn test_timestamp_deserialization() {
        let json = "1635789600.5";
        let timestamp: Timestamp = serde_json::from_str(json).unwrap();
        assert_eq!(timestamp.0, 1635789600.5);
    }

    #[test]
    fn test_timestamp_display() {
        let dt = Utc.timestamp_opt(1635789600, 500_000_000).unwrap();
        let timestamp = Timestamp::from(dt);
        assert_eq!(
            format!("{}", timestamp),
            "1635789600.5 (2021-11-01T18:00:00.500+00:00)"
        );
    }
}
