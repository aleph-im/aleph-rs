use chrono::{DateTime, TimeZone, Timelike, Utc};
use serde::de::{self, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

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
#[derive(Debug, Clone, PartialEq)]
pub struct Timestamp(f64);

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        let s = self.0.to_string();
        let number = serde_json::Number::from_str(&s).map_err(S::Error::custom)?;
        number.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TimestampVisitor;

        impl<'de> Visitor<'de> for TimestampVisitor {
            type Value = Timestamp;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a float, integer or string timestamp")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Timestamp(value as f64))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Timestamp(value as f64))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Timestamp(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                value.parse::<f64>().map(Timestamp).map_err(E::custom)
            }

            fn visit_map<M>(self, _map: M) -> Result<Self::Value, M::Error>
            where
                M: de::MapAccess<'de>,
            {
                // If arbitrary_precision is enabled, serde_json might deserialize numbers as maps in some contexts
                // like when using Untagged enums or other complex structures.
                // The map should have a single key "$serde_json::private::Number"
                #[derive(Deserialize)]
                struct NumberWrapper {
                    #[serde(rename = "$serde_json::private::Number")]
                    number: String,
                }

                let wrapper =
                    NumberWrapper::deserialize(de::value::MapAccessDeserializer::new(_map))?;
                wrapper
                    .number
                    .parse::<f64>()
                    .map(Timestamp)
                    .map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(TimestampVisitor)
    }
}

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
        // Since we enabled arbitrary_precision and use Number, it should serialize as a float
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
