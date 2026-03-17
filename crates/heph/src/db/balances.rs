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
}
