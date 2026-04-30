/// Database operations for credit balances.
use rusqlite::Connection;

/// Get the credit balance for an address.
///
/// Returns `None` if the address has no balance record yet.
pub fn get_credit_balance(conn: &Connection, address: &str) -> rusqlite::Result<Option<i64>> {
    let mut stmt = conn.prepare_cached("SELECT balance FROM credit_balances WHERE address = ?1")?;
    let mut rows = stmt.query([address])?;
    match rows.next()? {
        Some(row) => Ok(Some(row.get(0)?)),
        None => Ok(None),
    }
}

/// Set (insert or replace) the credit balance for an address.
pub fn set_credit_balance(conn: &Connection, address: &str, balance: i64) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO credit_balances (address, balance) VALUES (?1, ?2)
         ON CONFLICT(address) DO UPDATE SET balance = excluded.balance",
        rusqlite::params![address, balance],
    )?;
    Ok(())
}

/// Insert a credit history entry (for pre-seeding, top-ups, etc.).
pub fn insert_credit_history(
    conn: &Connection,
    address: &str,
    amount: i64,
    tx_hash: Option<&str>,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO credit_history (address, amount, tx_hash) VALUES (?1, ?2, ?3)",
        rusqlite::params![address, amount, tx_hash],
    )?;
    Ok(())
}

/// Full row for `credit_history`. Used when the caller has more context than
/// the legacy `(address, amount, tx_hash)` triple — e.g. a credit transfer
/// recipient row carrying the source post's `item_hash` and the counterparty.
#[derive(Debug, Clone)]
pub struct CreditHistoryRow {
    pub address: String,
    pub amount: i64,
    pub tx_hash: Option<String>,
    pub message_hash: Option<String>,
    pub counterparty: Option<String>,
    pub expiration_at: Option<String>,
}

pub fn insert_credit_history_full(
    conn: &Connection,
    row: &CreditHistoryRow,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO credit_history \
            (address, amount, tx_hash, message_hash, counterparty, expiration_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            row.address,
            row.amount,
            row.tx_hash,
            row.message_hash,
            row.counterparty,
            row.expiration_at,
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    #[test]
    fn test_get_balance_missing() {
        let db = Db::open_in_memory().unwrap();
        let result = db
            .with_conn(|conn| get_credit_balance(conn, "0xunknown"))
            .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_set_and_get_balance() {
        let db = Db::open_in_memory().unwrap();
        let addr = "0xdeadbeef";

        db.with_conn(|conn| set_credit_balance(conn, addr, 42_000))
            .unwrap();

        let balance = db.with_conn(|conn| get_credit_balance(conn, addr)).unwrap();
        assert_eq!(balance, Some(42_000));
    }

    #[test]
    fn test_overwrite_balance() {
        let db = Db::open_in_memory().unwrap();
        let addr = "0xcafe";

        db.with_conn(|conn| set_credit_balance(conn, addr, 100))
            .unwrap();
        db.with_conn(|conn| set_credit_balance(conn, addr, 999))
            .unwrap();

        let balance = db.with_conn(|conn| get_credit_balance(conn, addr)).unwrap();
        assert_eq!(balance, Some(999));
    }

    #[test]
    fn test_insert_credit_history() {
        let db = Db::open_in_memory().unwrap();
        let addr = "0xfeed";

        db.with_conn(|conn| insert_credit_history(conn, addr, 1_000_000, Some("0xabcdef")))
            .unwrap();

        // Verify a row was inserted (just checking it doesn't error).
        let count: i64 = db
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT COUNT(*) FROM credit_history WHERE address = ?1",
                    [addr],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn insert_credit_history_full_persists_all_columns() {
        let db = Db::open_in_memory().unwrap();
        let row = CreditHistoryRow {
            address: "0xrecipient".to_string(),
            amount: 1500,
            tx_hash: None,
            message_hash: Some("itemhashabc".to_string()),
            counterparty: Some("0xsender".to_string()),
            expiration_at: Some("2026-12-31T23:59:59Z".to_string()),
        };
        db.with_conn(|c| insert_credit_history_full(c, &row))
            .unwrap();

        let (amount, tx_hash, message_hash, counterparty, expiration_at): (
            i64,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = db
            .with_conn(|c| {
                c.query_row(
                    "SELECT amount, tx_hash, message_hash, counterparty, expiration_at \
                     FROM credit_history WHERE address = ?1",
                    ["0xrecipient"],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                )
            })
            .unwrap();
        assert_eq!(amount, 1500);
        assert_eq!(tx_hash, None);
        assert_eq!(message_hash.as_deref(), Some("itemhashabc"));
        assert_eq!(counterparty.as_deref(), Some("0xsender"));
        assert_eq!(expiration_at.as_deref(), Some("2026-12-31T23:59:59Z"));
    }

    #[test]
    fn insert_credit_history_full_supports_negative_amounts() {
        let db = Db::open_in_memory().unwrap();
        let row = CreditHistoryRow {
            address: "0xsender".to_string(),
            amount: -1500,
            tx_hash: None,
            message_hash: Some("itemhashabc".to_string()),
            counterparty: Some("0xrecipient".to_string()),
            expiration_at: None,
        };
        db.with_conn(|c| insert_credit_history_full(c, &row))
            .unwrap();
        let amount: i64 = db
            .with_conn(|c| {
                c.query_row(
                    "SELECT amount FROM credit_history WHERE address = ?1",
                    ["0xsender"],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(amount, -1500);
    }
}
