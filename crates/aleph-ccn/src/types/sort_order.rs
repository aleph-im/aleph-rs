//! Sort order enumerations used by the API/listing endpoints.
//!
//! Mirrors `src/aleph/types/sort_order.py`.

use serde::{Deserialize, Serialize};

/// Direction of a sort (mirrors Python `IntEnum` with values `1` / `-1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum SortOrder {
    Ascending = 1,
    Descending = -1,
}

impl Serialize for SortOrder {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> Deserialize<'de> for SortOrder {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let n = i32::deserialize(deserializer)?;
        SortOrder::try_from(n)
            .map_err(|v| serde::de::Error::custom(format!("unknown SortOrder value: {v}")))
    }
}

impl SortOrder {
    /// Return the SQL ORDER BY direction string.
    pub fn to_sql(&self) -> &'static str {
        match self {
            SortOrder::Ascending => "ASC",
            SortOrder::Descending => "DESC",
        }
    }

    /// Parse `"ASC"`/`"ASCENDING"`/`"DESC"`/`"DESCENDING"` (any case) into a SortOrder.
    ///
    /// This mirrors the Python `_parse_sort_order_for_metrics` `BeforeValidator`.
    pub fn parse_for_metrics(value: &str) -> Option<Self> {
        match value.to_ascii_uppercase().as_str() {
            "ASC" | "ASCENDING" => Some(SortOrder::Ascending),
            "DESC" | "DESCENDING" => Some(SortOrder::Descending),
            _ => None,
        }
    }
}

impl TryFrom<i32> for SortOrder {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(SortOrder::Ascending),
            -1 => Ok(SortOrder::Descending),
            other => Err(other),
        }
    }
}

impl From<SortOrder> for i32 {
    fn from(value: SortOrder) -> Self {
        value as i32
    }
}

/// Generic time-based sort field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SortBy {
    #[serde(rename = "time")]
    Time,
    #[serde(rename = "tx-time")]
    TxTime,
}

/// Field by which message-type DB requests should be sorted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortByMessageType {
    Aggregate,
    Forget,
    Instance,
    Post,
    Program,
    Store,
    Total,
}

/// Field by which aggregates should be sorted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortByAggregate {
    CreationTime,
    LastModified,
}

/// Field by which credit history entries should be sorted.
///
/// Values match `AlephCreditHistoryDb` column names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortByCreditHistory {
    MessageTimestamp,
    ExpirationDate,
    PaymentMethod,
    Amount,
    Origin,
    TxHash,
    Provider,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_order_roundtrip() {
        assert_eq!(serde_json::to_string(&SortOrder::Ascending).unwrap(), "1");
        assert_eq!(serde_json::to_string(&SortOrder::Descending).unwrap(), "-1");
        let asc: SortOrder = serde_json::from_str("1").unwrap();
        assert_eq!(asc, SortOrder::Ascending);
        let desc: SortOrder = serde_json::from_str("-1").unwrap();
        assert_eq!(desc, SortOrder::Descending);
    }

    #[test]
    fn sort_order_to_sql() {
        assert_eq!(SortOrder::Ascending.to_sql(), "ASC");
        assert_eq!(SortOrder::Descending.to_sql(), "DESC");
    }

    #[test]
    fn sort_order_parse_for_metrics() {
        assert_eq!(
            SortOrder::parse_for_metrics("asc"),
            Some(SortOrder::Ascending)
        );
        assert_eq!(
            SortOrder::parse_for_metrics("ASCENDING"),
            Some(SortOrder::Ascending)
        );
        assert_eq!(
            SortOrder::parse_for_metrics("desc"),
            Some(SortOrder::Descending)
        );
        assert_eq!(
            SortOrder::parse_for_metrics("DESCENDING"),
            Some(SortOrder::Descending)
        );
        assert_eq!(SortOrder::parse_for_metrics("foo"), None);
    }

    #[test]
    fn sort_order_int_conversions() {
        assert_eq!(i32::from(SortOrder::Ascending), 1);
        assert_eq!(i32::from(SortOrder::Descending), -1);
        assert_eq!(SortOrder::try_from(1).unwrap(), SortOrder::Ascending);
        assert_eq!(SortOrder::try_from(-1).unwrap(), SortOrder::Descending);
        assert_eq!(SortOrder::try_from(0), Err(0));
    }

    #[test]
    fn sort_by_roundtrip() {
        assert_eq!(serde_json::to_string(&SortBy::Time).unwrap(), "\"time\"");
        assert_eq!(
            serde_json::to_string(&SortBy::TxTime).unwrap(),
            "\"tx-time\""
        );
        let parsed: SortBy = serde_json::from_str("\"tx-time\"").unwrap();
        assert_eq!(parsed, SortBy::TxTime);
    }

    #[test]
    fn sort_by_message_type_roundtrip() {
        assert_eq!(
            serde_json::to_string(&SortByMessageType::Aggregate).unwrap(),
            "\"aggregate\""
        );
        let parsed: SortByMessageType = serde_json::from_str("\"total\"").unwrap();
        assert_eq!(parsed, SortByMessageType::Total);
    }

    #[test]
    fn sort_by_aggregate_roundtrip() {
        let s = serde_json::to_string(&SortByAggregate::CreationTime).unwrap();
        assert_eq!(s, "\"creation_time\"");
        let parsed: SortByAggregate = serde_json::from_str("\"last_modified\"").unwrap();
        assert_eq!(parsed, SortByAggregate::LastModified);
    }

    #[test]
    fn sort_by_credit_history_roundtrip() {
        assert_eq!(
            serde_json::to_string(&SortByCreditHistory::MessageTimestamp).unwrap(),
            "\"message_timestamp\""
        );
        let parsed: SortByCreditHistory = serde_json::from_str("\"tx_hash\"").unwrap();
        assert_eq!(parsed, SortByCreditHistory::TxHash);
    }
}
