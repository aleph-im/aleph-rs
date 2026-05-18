//! Mirrors `src/aleph/schemas/api/accounts.py`.
//!
//! Request / response shapes for the `/api/v0/addresses/...` endpoints.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use aleph_types::chain::Chain;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::de::{Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use crate::schemas::messages_query_params::{DEFAULT_PAGE, LIST_FIELD_SEPARATOR};
use crate::types::files::FileType;
use crate::types::sort_order::{SortByCreditHistory, SortOrder};

fn default_pagination_100() -> i64 {
    100
}

fn default_page() -> i64 {
    DEFAULT_PAGE
}

fn default_pagination_0() -> i64 {
    0
}

fn default_sort_order_descending() -> SortOrder {
    SortOrder::Descending
}

fn default_sort_by_message_timestamp() -> SortByCreditHistory {
    SortByCreditHistory::MessageTimestamp
}

fn deserialize_chains_csv<'de, D>(d: D) -> Result<Option<Vec<Chain>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<Vec<Chain>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a sequence or comma-separated string of chains")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            d.deserialize_any(self)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            let mut out = Vec::new();
            for part in v.split(LIST_FIELD_SEPARATOR) {
                let quoted = format!("\"{part}\"");
                let chain: Chain = serde_json::from_str(&quoted).map_err(E::custom)?;
                out.push(chain);
            }
            Ok(Some(out))
        }

        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(c) = seq.next_element::<Chain>()? {
                out.push(c);
            }
            Ok(Some(out))
        }
    }

    d.deserialize_option(V)
}

fn deserialize_str_csv<'de, D>(d: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<Vec<String>>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a sequence or comma-separated string")
        }
        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            d.deserialize_any(self)
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(Some(
                v.split(LIST_FIELD_SEPARATOR)
                    .map(|s| s.to_string())
                    .collect(),
            ))
        }
        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(s) = seq.next_element::<String>()? {
                out.push(s);
            }
            Ok(Some(out))
        }
    }
    d.deserialize_option(V)
}

/// A decimal value serialized as a float (mirrors Python `FloatDecimal`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloatDecimal(pub Decimal);

impl FloatDecimal {
    pub fn into_inner(self) -> Decimal {
        self.0
    }
}

impl From<Decimal> for FloatDecimal {
    fn from(value: Decimal) -> Self {
        Self(value)
    }
}

impl Serialize for FloatDecimal {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use std::str::FromStr;
        let s = self.0.to_string();
        let f = f64::from_str(&s).unwrap_or(0.0);
        serializer.serialize_f64(f)
    }
}

impl<'de> Deserialize<'de> for FloatDecimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(deserializer)?;
        let s = match v {
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => s,
            other => {
                return Err(serde::de::Error::custom(format!(
                    "expected number or string, got {other:?}"
                )));
            }
        };
        Decimal::from_str(&s)
            .or_else(|_| Decimal::from_str_exact(&s))
            .map(FloatDecimal)
            .map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// Account / balance schemas
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAccountQueryParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<Chain>,
    #[serde(default)]
    pub include_credit_details: bool,
}

impl Default for GetAccountQueryParams {
    fn default() -> Self {
        Self {
            chain: None,
            include_credit_details: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditBalanceDetailItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration_date: Option<DateTime<Utc>>,
    pub amount: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetAccountBalanceResponse {
    pub address: String,
    pub balance: FloatDecimal,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<HashMap<String, FloatDecimal>>,
    pub locked_amount: FloatDecimal,
    #[serde(default)]
    pub credit_balance: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credit_balance_details: Option<Vec<CreditBalanceDetailItem>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAccountFilesQueryParams {
    #[serde(default = "default_pagination_100")]
    pub pagination: i64,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_sort_order_descending")]
    pub sort_order: SortOrder,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl Default for GetAccountFilesQueryParams {
    fn default() -> Self {
        Self {
            pagination: 100,
            page: DEFAULT_PAGE,
            sort_order: SortOrder::Descending,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBalancesChainsQueryParams {
    #[serde(
        default,
        deserialize_with = "deserialize_chains_csv",
        skip_serializing_if = "Option::is_none"
    )]
    pub chains: Option<Vec<Chain>>,
    #[serde(default = "default_pagination_100")]
    pub pagination: i64,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default)]
    pub min_balance: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl Default for GetBalancesChainsQueryParams {
    fn default() -> Self {
        Self {
            chains: None,
            pagination: 100,
            page: DEFAULT_PAGE,
            min_balance: 0,
            cursor: None,
        }
    }
}

impl GetBalancesChainsQueryParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.pagination < 0 {
            return Err("pagination must be >= 0".to_string());
        }
        if self.page < 1 {
            return Err("page must be >= 1".to_string());
        }
        if self.min_balance < 1 {
            return Err("min_balance must be >= 1".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddressBalanceResponse {
    pub address: String,
    pub balance: FloatDecimal,
    pub chain: Chain,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetCreditBalancesQueryParams {
    #[serde(default = "default_pagination_100")]
    pub pagination: i64,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default)]
    pub min_balance: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl Default for GetCreditBalancesQueryParams {
    fn default() -> Self {
        Self {
            pagination: 100,
            page: DEFAULT_PAGE,
            min_balance: 0,
            cursor: None,
        }
    }
}

impl GetCreditBalancesQueryParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.pagination < 0 {
            return Err("pagination must be >= 0".to_string());
        }
        if self.page < 1 {
            return Err("page must be >= 1".to_string());
        }
        if self.min_balance < 1 {
            return Err("min_balance must be >= 1".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddressCreditBalanceResponse {
    pub address: String,
    pub credits: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetAccountFilesResponseItem {
    pub file_hash: String,
    pub size: i64,
    #[serde(rename = "type")]
    pub file_type: FileType,
    pub created: DateTime<Utc>,
    pub item_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetAccountFilesResponse {
    pub address: String,
    pub total_size: i64,
    pub files: Vec<GetAccountFilesResponseItem>,
    pub pagination_page: i64,
    pub pagination_total: i64,
    pub pagination_per_page: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetAccountCreditHistoryQueryParams {
    #[serde(default = "default_pagination_0")]
    pub pagination: i64,
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payment_method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_expiration: Option<bool>,
    #[serde(
        default,
        deserialize_with = "deserialize_str_csv",
        skip_serializing_if = "Option::is_none"
    )]
    pub exclude_payment_method: Option<Vec<String>>,
    #[serde(default = "default_sort_by_message_timestamp")]
    pub sort_by: SortByCreditHistory,
    #[serde(default = "default_sort_order_descending")]
    pub sort_order: SortOrder,
}

impl Default for GetAccountCreditHistoryQueryParams {
    fn default() -> Self {
        Self {
            pagination: 0,
            page: DEFAULT_PAGE,
            cursor: None,
            tx_hash: None,
            token: None,
            chain: None,
            provider: None,
            origin: None,
            origin_ref: None,
            payment_method: None,
            has_expiration: None,
            exclude_payment_method: None,
            sort_by: SortByCreditHistory::MessageTimestamp,
            sort_order: SortOrder::Descending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditHistoryResponseItem {
    pub amount: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bonus_amount: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payment_method: Option<String>,
    pub credit_ref: String,
    pub credit_index: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration_date: Option<DateTime<Utc>>,
    pub message_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetAccountCreditHistoryResponse {
    pub address: String,
    pub credit_history: Vec<CreditHistoryResponseItem>,
    pub pagination_page: i64,
    pub pagination_total: i64,
    pub pagination_per_page: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetResourceConsumedCreditsResponse {
    pub item_hash: String,
    pub consumed_credits: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetAccountPostTypesResponse {
    pub address: String,
    pub post_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetAccountChannelsResponse {
    pub address: String,
    pub channels: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn dt(ts: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(ts, 0).single().unwrap()
    }

    #[test]
    fn test_float_decimal_serialize() {
        let v = FloatDecimal(Decimal::from_str("1.5").unwrap());
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, "1.5");
    }

    #[test]
    fn test_float_decimal_deserialize() {
        let v: FloatDecimal = serde_json::from_str("1.5").unwrap();
        assert_eq!(v.0, Decimal::from_str("1.5").unwrap());
    }

    #[test]
    fn test_get_account_query_params_default() {
        let p = GetAccountQueryParams::default();
        assert!(!p.include_credit_details);
    }

    #[test]
    fn test_get_account_balance_response_roundtrip() {
        let resp = GetAccountBalanceResponse {
            address: "0xa".into(),
            balance: FloatDecimal(Decimal::from(100)),
            details: None,
            locked_amount: FloatDecimal(Decimal::ZERO),
            credit_balance: 50,
            credit_balance_details: Some(vec![CreditBalanceDetailItem {
                expiration_date: Some(dt(1700000000)),
                amount: 50,
            }]),
        };
        let json = serde_json::to_value(&resp).unwrap();
        let back: GetAccountBalanceResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn test_get_balances_chains_query_params_csv() {
        let json = serde_json::json!({"chains": "ETH,SOL", "min_balance": 10});
        let parsed: GetBalancesChainsQueryParams = serde_json::from_value(json).unwrap();
        parsed.validate().unwrap();
        assert_eq!(parsed.chains, Some(vec![Chain::Ethereum, Chain::Sol]));
        assert_eq!(parsed.min_balance, 10);
    }

    #[test]
    fn test_get_account_credit_history_query_params_csv() {
        let json = serde_json::json!({"exclude_payment_method": "a,b,c"});
        let parsed: GetAccountCreditHistoryQueryParams = serde_json::from_value(json).unwrap();
        assert_eq!(
            parsed.exclude_payment_method,
            Some(vec!["a".into(), "b".into(), "c".into()])
        );
    }

    #[test]
    fn test_get_account_credit_history_query_params_defaults() {
        let p = GetAccountCreditHistoryQueryParams::default();
        assert_eq!(p.sort_by, SortByCreditHistory::MessageTimestamp);
        assert_eq!(p.sort_order, SortOrder::Descending);
        assert_eq!(p.page, 1);
        assert_eq!(p.pagination, 0);
    }

    #[test]
    fn test_get_account_files_response_roundtrip() {
        let resp = GetAccountFilesResponse {
            address: "0xa".into(),
            total_size: 100,
            files: vec![GetAccountFilesResponseItem {
                file_hash: "h".into(),
                size: 50,
                file_type: FileType::File,
                created: dt(1700000000),
                item_hash: "ih".into(),
            }],
            pagination_page: 1,
            pagination_total: 1,
            pagination_per_page: 100,
        };
        let json = serde_json::to_value(&resp).unwrap();
        let back: GetAccountFilesResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn test_address_balance_response_roundtrip() {
        let resp = AddressBalanceResponse {
            address: "0xa".into(),
            balance: FloatDecimal(Decimal::from(50)),
            chain: Chain::Ethereum,
        };
        let json = serde_json::to_value(&resp).unwrap();
        let back: AddressBalanceResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn test_get_account_post_types_response_roundtrip() {
        let resp = GetAccountPostTypesResponse {
            address: "0xa".into(),
            post_types: vec!["t1".into(), "t2".into()],
        };
        let json = serde_json::to_value(&resp).unwrap();
        let back: GetAccountPostTypesResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }
}
