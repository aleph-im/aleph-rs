//! Server-side handler for `aleph_credit_transfer` POST messages.

use crate::db::balances::{CreditHistoryRow, get_credit_balance, insert_credit_history_full};
use crate::handlers::{ProcessingError, ProcessingResult};
use aleph_sdk::credit_transfer::{CreditTransferContent, CreditTransferError};
use rusqlite::Transaction;

/// Apply a credit transfer inside an existing SQL transaction.
///
/// The caller (the post handler) is responsible for opening and committing the
/// transaction so that the post insert and the credit-transfer apply are atomic.
pub fn process_in_tx(
    tx: &Transaction<'_>,
    sender: &str,
    item_hash: &str,
    raw_content: serde_json::Value,
) -> ProcessingResult<()> {
    // 1. Deserialize.
    let content: CreditTransferContent = serde_json::from_value(raw_content).map_err(|e| {
        ProcessingError::InvalidFormat(format!("invalid credit transfer content: {e}"))
    })?;

    // 2. Schema validate.
    if let Err(e) = content.validate() {
        return Err(ProcessingError::InvalidFormat(e.to_string()));
    }

    // 3. Self-transfer check (the schema does not know the sender).
    for entry in &content.transfer.credits {
        if entry.address.as_str() == sender {
            return Err(ProcessingError::InvalidFormat(
                CreditTransferError::SelfTransfer(entry.address.clone()).to_string(),
            ));
        }
    }

    // 4. Balance check (sum across entries).
    let total: u64 = content.transfer.credits.iter().map(|e| e.amount).sum();
    let total_i64 = i64::try_from(total).map_err(|_| {
        ProcessingError::InvalidFormat(format!("transfer total {total} overflows i64"))
    })?;
    let have = get_credit_balance(tx, sender)
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?
        .unwrap_or(0);
    if have < total_i64 {
        return Err(ProcessingError::CreditInsufficient(format!(
            "insufficient credit balance: have {have}, need {total}"
        )));
    }

    // 5. Apply: per entry, recipient + sender history rows + balance updates.
    for entry in &content.transfer.credits {
        let amount_i64 = i64::try_from(entry.amount).map_err(|_| {
            ProcessingError::InvalidFormat(format!("entry amount {} overflows i64", entry.amount))
        })?;
        let recipient = entry.address.as_str();
        let expiration_str = entry.expiration.map(|dt| dt.to_rfc3339());

        // Recipient history row.
        insert_credit_history_full(
            tx,
            &CreditHistoryRow {
                address: recipient.to_string(),
                amount: amount_i64,
                tx_hash: None,
                message_hash: Some(item_hash.to_string()),
                counterparty: Some(sender.to_string()),
                expiration_at: expiration_str,
            },
        )
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        // Sender history row.
        insert_credit_history_full(
            tx,
            &CreditHistoryRow {
                address: sender.to_string(),
                amount: -amount_i64,
                tx_hash: None,
                message_hash: Some(item_hash.to_string()),
                counterparty: Some(recipient.to_string()),
                expiration_at: None,
            },
        )
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        // Recipient balance upsert.
        tx.execute(
            "INSERT INTO credit_balances (address, balance) VALUES (?1, ?2) \
             ON CONFLICT(address) DO UPDATE SET balance = balance + excluded.balance",
            rusqlite::params![recipient, amount_i64],
        )
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

        // Sender balance decrement (row is guaranteed to exist — the balance check passed).
        tx.execute(
            "UPDATE credit_balances SET balance = balance - ?1 WHERE address = ?2",
            rusqlite::params![amount_i64, sender],
        )
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::db::balances::set_credit_balance;
    use serde_json::json;

    fn open_db_with_schema() -> Db {
        Db::open_in_memory().unwrap()
    }

    #[test]
    fn rejects_garbage_content_with_invalid_format() {
        let db = open_db_with_schema();
        let err = db
            .with_conn(|conn| {
                let tx = conn.unchecked_transaction().unwrap();
                let r = process_in_tx(&tx, "0xsender", "h0", json!({"not": "valid"}));
                let _ = tx.rollback();
                r
            })
            .unwrap_err();
        assert_eq!(
            err.error_code(),
            0,
            "expected InvalidFormat (0), got {err:?}"
        );
    }

    #[test]
    fn rejects_zero_amount_with_invalid_format() {
        let db = open_db_with_schema();
        let content = json!({
            "transfer": { "credits": [
                { "address": "0xrecipient", "amount": 0 }
            ]}
        });
        let err = db
            .with_conn(|conn| {
                let tx = conn.unchecked_transaction().unwrap();
                let r = process_in_tx(&tx, "0xsender", "h0", content);
                let _ = tx.rollback();
                r
            })
            .unwrap_err();
        assert_eq!(err.error_code(), 0);
        assert!(
            err.message().contains("amount must be strictly positive"),
            "msg: {}",
            err.message()
        );
    }

    #[test]
    fn rejects_empty_credits_with_invalid_format() {
        let db = open_db_with_schema();
        let content = json!({ "transfer": { "credits": [] } });
        let err = db
            .with_conn(|conn| {
                let tx = conn.unchecked_transaction().unwrap();
                let r = process_in_tx(&tx, "0xsender", "h0", content);
                let _ = tx.rollback();
                r
            })
            .unwrap_err();
        assert_eq!(err.error_code(), 0);
        assert!(
            err.message().contains("credits list must not be empty"),
            "msg: {}",
            err.message()
        );
    }

    #[test]
    fn rejects_duplicate_recipients_with_invalid_format() {
        let db = open_db_with_schema();
        let content = json!({
            "transfer": { "credits": [
                { "address": "0xrecipient", "amount": 1 },
                { "address": "0xrecipient", "amount": 2 }
            ]}
        });
        let err = db
            .with_conn(|conn| {
                let tx = conn.unchecked_transaction().unwrap();
                let r = process_in_tx(&tx, "0xsender", "h0", content);
                let _ = tx.rollback();
                r
            })
            .unwrap_err();
        assert_eq!(err.error_code(), 0);
        assert!(
            err.message().contains("duplicate recipient"),
            "msg: {}",
            err.message()
        );
    }

    #[test]
    fn rejects_self_transfer_with_invalid_format() {
        let db = open_db_with_schema();
        let content = json!({
            "transfer": { "credits": [
                { "address": "0xsender", "amount": 1 }
            ]}
        });
        let err = db
            .with_conn(|conn| {
                let tx = conn.unchecked_transaction().unwrap();
                let r = process_in_tx(&tx, "0xsender", "h0", content);
                let _ = tx.rollback();
                r
            })
            .unwrap_err();
        assert_eq!(err.error_code(), 0);
        assert!(
            err.message().contains("sender and recipient must differ"),
            "msg: {}",
            err.message()
        );
    }

    #[test]
    fn rejects_when_sender_has_no_balance_with_credit_insufficient() {
        let db = open_db_with_schema();
        let content = json!({
            "transfer": { "credits": [
                { "address": "0xrecipient", "amount": 1 }
            ]}
        });
        let err = db
            .with_conn(|conn| {
                let tx = conn.unchecked_transaction().unwrap();
                let r = process_in_tx(&tx, "0xsender", "h0", content);
                let _ = tx.rollback();
                r
            })
            .unwrap_err();
        assert_eq!(
            err.error_code(),
            6,
            "expected CreditInsufficient (6), got {err:?}"
        );
    }

    #[test]
    fn rejects_when_balance_below_total_with_credit_insufficient() {
        let db = open_db_with_schema();
        db.with_conn(|c| set_credit_balance(c, "0xsender", 100))
            .unwrap();
        let content = json!({
            "transfer": { "credits": [
                { "address": "0xrecipient", "amount": 200 }
            ]}
        });
        let err = db
            .with_conn(|conn| {
                let tx = conn.unchecked_transaction().unwrap();
                let r = process_in_tx(&tx, "0xsender", "h0", content);
                let _ = tx.rollback();
                r
            })
            .unwrap_err();
        assert_eq!(err.error_code(), 6);
        assert!(err.message().contains("have 100"));
        assert!(err.message().contains("need 200"));
    }

    #[test]
    fn applies_transfer_and_writes_both_history_rows() {
        let db = open_db_with_schema();
        db.with_conn(|c| set_credit_balance(c, "0xsender", 5_000))
            .unwrap();

        let content = json!({
            "transfer": { "credits": [
                { "address": "0xrecipient", "amount": 1500, "expiration": 1798761599 }
            ]}
        });

        db.with_conn(|conn| {
            let tx = conn.unchecked_transaction().unwrap();
            let r = process_in_tx(&tx, "0xsender", "itemhashabc", content);
            assert!(r.is_ok(), "process_in_tx failed: {:?}", r.err());
            tx.commit().unwrap();
        });

        // Balances.
        let sender = db.with_conn(|c| get_credit_balance(c, "0xsender")).unwrap();
        let recipient = db
            .with_conn(|c| get_credit_balance(c, "0xrecipient"))
            .unwrap();
        assert_eq!(sender, Some(3_500));
        assert_eq!(recipient, Some(1_500));

        // Recipient history row.
        let (amount, mh, cp, exp): (i64, String, String, Option<String>) = db
            .with_conn(|c| {
                c.query_row(
                    "SELECT amount, message_hash, counterparty, expiration_at \
                     FROM credit_history WHERE address = ?1",
                    ["0xrecipient"],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
            })
            .unwrap();
        assert_eq!(amount, 1500);
        assert_eq!(mh, "itemhashabc");
        assert_eq!(cp, "0xsender");
        assert_eq!(exp.as_deref(), Some("2026-12-31T23:59:59+00:00"));

        // Sender history row.
        let (amount, mh, cp, exp): (i64, String, String, Option<String>) = db
            .with_conn(|c| {
                c.query_row(
                    "SELECT amount, message_hash, counterparty, expiration_at \
                     FROM credit_history WHERE address = ?1",
                    ["0xsender"],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
            })
            .unwrap();
        assert_eq!(amount, -1500);
        assert_eq!(mh, "itemhashabc");
        assert_eq!(cp, "0xrecipient");
        assert_eq!(exp, None);
    }
}
