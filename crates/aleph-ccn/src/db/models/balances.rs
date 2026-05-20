//! Balance and credit-history tables.
//!
//! Mirrors `src/aleph/db/models/balances.py`.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use aleph_types::chain::Chain;

use crate::{AlephError, AlephResult};

fn chain_from_text(s: &str) -> Chain {
    try_chain_from_text(s).unwrap_or_else(|_| panic!("unknown Chain in DB: {s}"))
}

fn try_chain_from_text(s: &str) -> AlephResult<Chain> {
    serde_json::from_value::<Chain>(serde_json::Value::String(s.to_string()))
        .map_err(|_| AlephError::InvalidMessage(format!("unknown Chain in DB: {s}")))
}

/// Row of the `balances` table. Mirrors `AlephBalanceDb`.
#[derive(Debug, Clone)]
pub struct AlephBalanceDb {
    pub id: i64,
    pub address: String,
    pub chain: Chain,
    pub dapp: Option<String>,
    pub eth_height: i32,
    pub balance: Decimal,
    pub last_update: DateTime<Utc>,
}

impl AlephBalanceDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self::try_from_row(row).expect("valid AlephBalanceDb row")
    }

    pub fn try_from_row(row: &tokio_postgres::Row) -> AlephResult<Self> {
        let chain_s: String = row.get("chain");
        Ok(Self {
            id: row.get("id"),
            address: row.get("address"),
            chain: try_chain_from_text(&chain_s)?,
            dapp: row.get("dapp"),
            eth_height: row.get("eth_height"),
            balance: row.get("balance"),
            last_update: row.get("last_update"),
        })
    }
}

/// Row of the `credit_history` table. Mirrors `AlephCreditHistoryDb`.
#[derive(Debug, Clone)]
pub struct AlephCreditHistoryDb {
    pub id: i64,
    pub address: String,
    pub amount: i64,
    pub price: Option<Decimal>,
    pub bonus_amount: Option<i64>,
    pub tx_hash: Option<String>,
    pub token: Option<String>,
    pub chain: Option<String>,
    pub provider: Option<String>,
    pub origin: Option<String>,
    pub origin_ref: Option<String>,
    pub payment_method: Option<String>,
    pub credit_ref: String,
    pub credit_index: i32,
    pub expiration_date: Option<DateTime<Utc>>,
    pub message_timestamp: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
}

impl AlephCreditHistoryDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            id: row.get("id"),
            address: row.get("address"),
            amount: row.get("amount"),
            price: row.get("price"),
            bonus_amount: row.get("bonus_amount"),
            tx_hash: row.get("tx_hash"),
            token: row.get("token"),
            chain: row.get("chain"),
            provider: row.get("provider"),
            origin: row.get("origin"),
            origin_ref: row.get("origin_ref"),
            payment_method: row.get("payment_method"),
            credit_ref: row.get("credit_ref"),
            credit_index: row.get("credit_index"),
            expiration_date: row.get("expiration_date"),
            message_timestamp: row.get("message_timestamp"),
            last_update: row.get("last_update"),
        }
    }
}

/// Row of the `credit_balances` table. Mirrors `AlephCreditBalanceDb`.
#[derive(Debug, Clone)]
pub struct AlephCreditBalanceDb {
    pub address: String,
    pub credit_ref: String,
    pub credit_index: i32,
    pub amount_remaining: i64,
    pub expiration_date: Option<DateTime<Utc>>,
    pub message_timestamp: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
}

impl AlephCreditBalanceDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            address: row.get("address"),
            credit_ref: row.get("credit_ref"),
            credit_index: row.get("credit_index"),
            amount_remaining: row.get("amount_remaining"),
            expiration_date: row.get("expiration_date"),
            message_timestamp: row.get("message_timestamp"),
            last_update: row.get("last_update"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn balance_construct() {
        let b = AlephBalanceDb {
            id: 1,
            address: "0xabc".into(),
            chain: Chain::Ethereum,
            dapp: None,
            eth_height: 1000,
            balance: Decimal::from_str("12345.67").unwrap(),
            last_update: Utc::now(),
        };
        assert_eq!(b.chain, Chain::Ethereum);
        assert_eq!(b.eth_height, 1000);
    }

    #[test]
    fn invalid_chain_returns_error() {
        assert!(try_chain_from_text("NOPE").is_err());
    }

    #[test]
    fn credit_history_construct() {
        let h = AlephCreditHistoryDb {
            id: 0,
            address: "0xabc".into(),
            amount: 100,
            price: None,
            bonus_amount: Some(10),
            tx_hash: None,
            token: None,
            chain: None,
            provider: None,
            origin: None,
            origin_ref: None,
            payment_method: None,
            credit_ref: "abc".into(),
            credit_index: 0,
            expiration_date: None,
            message_timestamp: Utc::now(),
            last_update: Utc::now(),
        };
        assert_eq!(h.amount, 100);
        assert_eq!(h.bonus_amount, Some(10));
    }

    #[test]
    fn credit_balance_construct() {
        let cb = AlephCreditBalanceDb {
            address: "0xabc".into(),
            credit_ref: "abc".into(),
            credit_index: 0,
            amount_remaining: 50,
            expiration_date: None,
            message_timestamp: Utc::now(),
            last_update: Utc::now(),
        };
        assert_eq!(cb.amount_remaining, 50);
    }
}
