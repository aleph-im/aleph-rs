//! Per-account cost ledger (`account_costs` table).
//!
//! Mirrors `src/aleph/db/models/account_costs.py`.

use rust_decimal::Decimal;

use crate::types::cost::CostType;

/// Payment type stored as text. Mirrors `aleph_message.models.PaymentType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PaymentType {
    Hold,
    Superfluid,
    Credit,
}

impl PaymentType {
    pub fn as_value_str(self) -> &'static str {
        match self {
            PaymentType::Hold => "hold",
            PaymentType::Superfluid => "superfluid",
            PaymentType::Credit => "credit",
        }
    }
}

impl TryFrom<&str> for PaymentType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "hold" => Ok(PaymentType::Hold),
            "superfluid" => Ok(PaymentType::Superfluid),
            "credit" => Ok(PaymentType::Credit),
            other => Err(format!("unknown PaymentType: {other}")),
        }
    }
}

impl std::fmt::Display for PaymentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_value_str())
    }
}

/// Row of the `account_costs` table.
#[derive(Debug, Clone)]
pub struct AccountCostsDb {
    pub id: i64,
    pub owner: String,
    pub item_hash: String,
    pub r#type: CostType,
    pub name: String,
    pub r#ref: Option<String>,
    pub payment_type: PaymentType,
    pub cost_hold: Decimal,
    pub cost_stream: Decimal,
    pub cost_credit: Decimal,
}

impl AccountCostsDb {
    /// Build an [`AccountCostsDb`] from a database row.
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let type_s: String = row.get("type");
        let payment_s: String = row.get("payment_type");
        let cost_type =
            serde_json::from_value::<CostType>(serde_json::Value::String(type_s.clone()))
                .unwrap_or_else(|_| panic!("unknown CostType in DB: {type_s}"));
        Self {
            id: row.get("id"),
            owner: row.get("owner"),
            item_hash: row.get("item_hash"),
            r#type: cost_type,
            name: row.get("name"),
            r#ref: row.get("ref"),
            payment_type: PaymentType::try_from(payment_s.as_str())
                .expect("valid PaymentType in DB"),
            cost_hold: row.get("cost_hold"),
            cost_stream: row.get("cost_stream"),
            cost_credit: row.get("cost_credit"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn payment_type_roundtrip() {
        for variant in [
            PaymentType::Hold,
            PaymentType::Superfluid,
            PaymentType::Credit,
        ] {
            assert_eq!(
                PaymentType::try_from(variant.as_value_str()).unwrap(),
                variant
            );
        }
        assert!(PaymentType::try_from("nope").is_err());
    }

    #[test]
    fn account_costs_construct() {
        let r = AccountCostsDb {
            id: 1,
            owner: "0xabc".into(),
            item_hash: "deadbeef".into(),
            r#type: CostType::Storage,
            name: "STORAGE".into(),
            r#ref: None,
            payment_type: PaymentType::Hold,
            cost_hold: Decimal::from_str("1.5").unwrap(),
            cost_stream: Decimal::ZERO,
            cost_credit: Decimal::ZERO,
        };
        assert_eq!(r.payment_type, PaymentType::Hold);
        assert_eq!(r.r#type, CostType::Storage);
        assert_eq!(r.cost_hold, Decimal::from_str("1.5").unwrap());
    }
}
