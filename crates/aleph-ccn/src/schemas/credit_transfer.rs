//! Mirrors `src/aleph/schemas/credit_transfer.py`.
//!
//! Schemas for credit transfer, distribution and expense messages.

use std::collections::HashSet;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

fn validate_address(v: &str) -> Result<(), String> {
    if v.trim().is_empty() {
        return Err("address must not be empty".to_string());
    }
    Ok(())
}

fn validate_positive_int_amount(v: i64) -> Result<(), String> {
    if v <= 0 {
        return Err("amount must be a strictly positive integer".to_string());
    }
    Ok(())
}

/// Deserialize an `i64` amount, rejecting non-integer / boolean inputs.
/// Mirrors the Python `amount_must_be_int` validator.
fn deserialize_amount<'de, D: Deserializer<'de>>(de: D) -> Result<i64, D::Error> {
    let value = serde_json::Value::deserialize(de)?;
    match value {
        serde_json::Value::Bool(_) => Err(serde::de::Error::custom(
            "amount must be an integer, got bool",
        )),
        serde_json::Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| serde::de::Error::custom("amount must be an integer")),
        other => Err(serde::de::Error::custom(format!(
            "amount must be an integer, got {}",
            type_name(&other)
        ))),
    }
}

/// Coerce a price value to string, matching the Python `coerce_price_to_str`
/// `mode="before"` validator. Numbers become their decimal string form, strings
/// pass through, anything else is rejected by the subsequent Decimal check.
fn coerce_price<'de, D: Deserializer<'de>>(de: D) -> Result<String, D::Error> {
    let value = serde_json::Value::deserialize(de)?;
    let s = match value {
        serde_json::Value::Bool(_) => {
            return Err(serde::de::Error::custom("price must not be bool"));
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s,
        other => {
            return Err(serde::de::Error::custom(format!(
                "price must be a string or number, got {}",
                type_name(&other)
            )));
        }
    };
    if Decimal::from_str(&s).is_err() && Decimal::from_scientific(&s).is_err() {
        return Err(serde::de::Error::custom(format!(
            "price must be a valid decimal string, got {s:?}"
        )));
    }
    Ok(s)
}

fn coerce_price_optional<'de, D: Deserializer<'de>>(de: D) -> Result<Option<String>, D::Error> {
    let value = Option::<serde_json::Value>::deserialize(de)?;
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let s = match value {
        serde_json::Value::Bool(_) => {
            return Err(serde::de::Error::custom("price must not be bool"));
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s,
        other => {
            return Err(serde::de::Error::custom(format!(
                "price must be a string or number, got {}",
                type_name(&other)
            )));
        }
    };
    if Decimal::from_str(&s).is_err() && Decimal::from_scientific(&s).is_err() {
        return Err(serde::de::Error::custom(format!(
            "price must be a valid decimal string, got {s:?}"
        )));
    }
    Ok(Some(s))
}

fn type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// ---------------------------------------------------------------------------
// Transfer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditTransferEntry {
    pub address: String,
    #[serde(deserialize_with = "deserialize_amount")]
    pub amount: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration: Option<i64>,
}

impl CreditTransferEntry {
    pub fn validate(&self) -> Result<(), String> {
        validate_address(&self.address)?;
        validate_positive_int_amount(self.amount)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditTransferList {
    pub credits: Vec<CreditTransferEntry>,
}

impl CreditTransferList {
    pub fn validate(&self) -> Result<(), String> {
        if self.credits.is_empty() {
            return Err("credits must contain at least one entry".to_string());
        }
        for c in &self.credits {
            c.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditTransferContent {
    pub transfer: CreditTransferList,
}

impl CreditTransferContent {
    /// Mirrors the `no_duplicate_recipients` model validator.
    pub fn validate(&self) -> Result<(), String> {
        self.transfer.validate()?;
        let mut seen: HashSet<&str> = HashSet::new();
        for entry in &self.transfer.credits {
            if !seen.insert(entry.address.as_str()) {
                return Err(
                    "Duplicate recipient addresses are not allowed in a single transfer"
                        .to_string(),
                );
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Distribution
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditDistributionEntry {
    pub address: String,
    #[serde(deserialize_with = "deserialize_amount")]
    pub amount: i64,
    #[serde(deserialize_with = "coerce_price")]
    pub price: String,
    pub tx_hash: String,
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(rename = "ref", default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payment_method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bonus_amount: Option<serde_json::Value>,
}

impl CreditDistributionEntry {
    pub fn validate(&self) -> Result<(), String> {
        validate_address(&self.address)?;
        validate_positive_int_amount(self.amount)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditDistributionList {
    pub credits: Vec<CreditDistributionEntry>,
    pub token: String,
    pub chain: String,
}

impl CreditDistributionList {
    pub fn validate(&self) -> Result<(), String> {
        if self.credits.is_empty() {
            return Err("credits must contain at least one entry".to_string());
        }
        for c in &self.credits {
            c.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditDistributionContent {
    pub distribution: CreditDistributionList,
}

impl CreditDistributionContent {
    pub fn validate(&self) -> Result<(), String> {
        self.distribution.validate()
    }
}

// ---------------------------------------------------------------------------
// Expense
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditExpenseEntry {
    pub address: String,
    #[serde(deserialize_with = "deserialize_amount")]
    pub amount: i64,
    #[serde(rename = "ref", default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "coerce_price_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub price: Option<String>,
    /// Accepted but ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<f64>,
}

impl CreditExpenseEntry {
    pub fn validate(&self) -> Result<(), String> {
        validate_address(&self.address)?;
        validate_positive_int_amount(self.amount)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditExpenseList {
    pub credits: Vec<CreditExpenseEntry>,
}

impl CreditExpenseList {
    pub fn validate(&self) -> Result<(), String> {
        if self.credits.is_empty() {
            return Err("credits must contain at least one entry".to_string());
        }
        for c in &self.credits {
            c.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreditExpenseContent {
    pub expense: CreditExpenseList,
}

impl CreditExpenseContent {
    pub fn validate(&self) -> Result<(), String> {
        self.expense.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credit_transfer_content_roundtrip() {
        let json = serde_json::json!({
            "transfer": {
                "credits": [
                    {"address": "0xa", "amount": 100, "expiration": 1700000000},
                    {"address": "0xb", "amount": 50}
                ]
            }
        });
        let parsed: CreditTransferContent = serde_json::from_value(json.clone()).unwrap();
        parsed.validate().unwrap();
        assert_eq!(parsed.transfer.credits.len(), 2);
        assert_eq!(parsed.transfer.credits[0].amount, 100);
        let back = serde_json::to_value(&parsed).unwrap();
        assert_eq!(back["transfer"]["credits"][0]["address"], "0xa");
    }

    #[test]
    fn test_credit_transfer_duplicate_address_rejected() {
        let json = serde_json::json!({
            "transfer": {"credits": [
                {"address": "0xa", "amount": 1},
                {"address": "0xa", "amount": 2}
            ]}
        });
        let parsed: CreditTransferContent = serde_json::from_value(json).unwrap();
        assert!(parsed.validate().is_err());
    }

    #[test]
    fn test_credit_transfer_amount_must_be_positive() {
        let parsed = CreditTransferEntry {
            address: "0xa".into(),
            amount: 0,
            expiration: None,
        };
        assert!(parsed.validate().is_err());
    }

    #[test]
    fn test_credit_transfer_amount_bool_rejected_on_deser() {
        let json = serde_json::json!({"address": "0xa", "amount": true});
        let res: Result<CreditTransferEntry, _> = serde_json::from_value(json);
        assert!(res.is_err());
    }

    #[test]
    fn test_credit_distribution_content_roundtrip() {
        let json = serde_json::json!({
            "distribution": {
                "credits": [
                    {"address": "0xa", "amount": 1, "price": "0.5", "tx_hash": "h", "provider": "p"}
                ],
                "token": "ALEPH",
                "chain": "ETH"
            }
        });
        let parsed: CreditDistributionContent = serde_json::from_value(json).unwrap();
        parsed.validate().unwrap();
        assert_eq!(parsed.distribution.credits[0].price, "0.5");
    }

    #[test]
    fn test_credit_distribution_price_from_number() {
        let json = serde_json::json!({
            "address": "0xa", "amount": 1, "price": 2, "tx_hash": "h", "provider": "p"
        });
        let parsed: CreditDistributionEntry = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.price, "2");
    }

    #[test]
    fn test_credit_distribution_price_invalid_rejected() {
        let json = serde_json::json!({
            "address": "0xa", "amount": 1, "price": "abc", "tx_hash": "h", "provider": "p"
        });
        let res: Result<CreditDistributionEntry, _> = serde_json::from_value(json);
        assert!(res.is_err());
    }

    #[test]
    fn test_credit_expense_content_roundtrip() {
        let json = serde_json::json!({
            "expense": {
                "credits": [{"address": "0xa", "amount": 1}]
            }
        });
        let parsed: CreditExpenseContent = serde_json::from_value(json).unwrap();
        parsed.validate().unwrap();
    }

    #[test]
    fn test_credit_expense_price_optional() {
        let json = serde_json::json!({"address": "0xa", "amount": 1, "price": "0.5"});
        let parsed: CreditExpenseEntry = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.price.as_deref(), Some("0.5"));

        let json = serde_json::json!({"address": "0xa", "amount": 1, "price": null});
        let parsed: CreditExpenseEntry = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.price, None);
    }
}
