/// Database operations for account cost records.
use rusqlite::Connection;

/// A single entry in the `account_costs` table.
#[derive(Debug, Clone)]
pub struct AccountCostRecord {
    pub owner: String,
    pub item_hash: String,
    pub cost_type: String,
    pub name: String,
    pub ref_hash: Option<String>,
    pub payment_type: String,
    /// Decimal as string (hold-based cost, typically "0").
    pub cost_hold: String,
    /// Decimal as string (stream-based cost, typically "0").
    pub cost_stream: String,
    /// Per-second cost as decimal string (the main cost field for credit payments).
    pub cost_credit: String,
}

/// Insert or replace multiple cost records for a resource.
///
/// Uses `INSERT OR REPLACE` to allow idempotent re-processing.
pub fn insert_account_costs(
    conn: &Connection,
    costs: &[AccountCostRecord],
) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare_cached(
        "INSERT OR REPLACE INTO account_costs
             (owner, item_hash, cost_type, name, ref_hash, payment_type,
              cost_hold, cost_stream, cost_credit)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?;

    for c in costs {
        stmt.execute(rusqlite::params![
            c.owner,
            c.item_hash,
            c.cost_type,
            c.name,
            c.ref_hash,
            c.payment_type,
            c.cost_hold,
            c.cost_stream,
            c.cost_credit,
        ])?;
    }
    Ok(())
}

/// Get the sum of all `cost_credit` values (per-second costs) for an address.
///
/// Returns `0.0` if the address has no cost records.
pub fn get_total_cost_for_address(conn: &Connection, address: &str) -> rusqlite::Result<f64> {
    let total: f64 = conn.query_row(
        "SELECT COALESCE(SUM(CAST(cost_credit AS REAL)), 0.0)
         FROM account_costs
         WHERE owner = ?1",
        [address],
        |row| row.get(0),
    )?;
    Ok(total)
}

/// Get all cost records for a specific item (by item_hash).
pub fn get_costs_for_item(
    conn: &Connection,
    item_hash: &str,
) -> rusqlite::Result<Vec<AccountCostRecord>> {
    let mut stmt = conn.prepare_cached(
        "SELECT owner, item_hash, cost_type, name, ref_hash, payment_type,
                cost_hold, cost_stream, cost_credit
         FROM account_costs
         WHERE item_hash = ?1",
    )?;

    let records = stmt
        .query_map([item_hash], |row| {
            Ok(AccountCostRecord {
                owner: row.get(0)?,
                item_hash: row.get(1)?,
                cost_type: row.get(2)?,
                name: row.get(3)?,
                ref_hash: row.get(4)?,
                payment_type: row.get(5)?,
                cost_hold: row.get(6)?,
                cost_stream: row.get(7)?,
                cost_credit: row.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(records)
}

/// Delete all cost records for a specific item (e.g., when forgotten).
///
/// Returns the number of rows deleted.
pub fn delete_costs_for_item(conn: &Connection, item_hash: &str) -> rusqlite::Result<usize> {
    let count = conn.execute(
        "DELETE FROM account_costs WHERE item_hash = ?1",
        [item_hash],
    )?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    fn sample_cost(owner: &str, item_hash: &str, cost_credit: &str) -> AccountCostRecord {
        AccountCostRecord {
            owner: owner.to_string(),
            item_hash: item_hash.to_string(),
            cost_type: "STORAGE".to_string(),
            name: "test".to_string(),
            ref_hash: None,
            payment_type: "credit".to_string(),
            cost_hold: "0".to_string(),
            cost_stream: "0".to_string(),
            cost_credit: cost_credit.to_string(),
        }
    }

    #[test]
    fn test_insert_and_get_total_cost() {
        let db = Db::open_in_memory().unwrap();
        let addr = "0xowner1";

        let costs = vec![
            sample_cost(
                addr,
                "hash1111111111111111111111111111111111111111111111111111111111111111",
                "0.0001",
            ),
            sample_cost(
                addr,
                "hash2222222222222222222222222222222222222222222222222222222222222222",
                "0.0002",
            ),
        ];

        db.with_conn(|conn| insert_account_costs(conn, &costs))
            .unwrap();

        let total = db
            .with_conn(|conn| get_total_cost_for_address(conn, addr))
            .unwrap();
        assert!((total - 0.0003).abs() < 1e-10, "got {total}");
    }

    #[test]
    fn test_total_cost_no_records() {
        let db = Db::open_in_memory().unwrap();
        let total = db
            .with_conn(|conn| get_total_cost_for_address(conn, "0xnobody"))
            .unwrap();
        assert_eq!(total, 0.0);
    }

    #[test]
    fn test_get_costs_for_item() {
        let db = Db::open_in_memory().unwrap();
        let addr = "0xowner2";
        let hash = "aaaa1111111111111111111111111111111111111111111111111111111111111111";

        let costs = vec![sample_cost(addr, hash, "0.00042")];
        db.with_conn(|conn| insert_account_costs(conn, &costs))
            .unwrap();

        let records = db.with_conn(|conn| get_costs_for_item(conn, hash)).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cost_credit, "0.00042");
    }

    #[test]
    fn test_delete_costs_for_item() {
        let db = Db::open_in_memory().unwrap();
        let addr = "0xowner3";
        let hash = "bbbb1111111111111111111111111111111111111111111111111111111111111111";

        let costs = vec![sample_cost(addr, hash, "0.0005")];
        db.with_conn(|conn| insert_account_costs(conn, &costs))
            .unwrap();

        let deleted = db
            .with_conn(|conn| delete_costs_for_item(conn, hash))
            .unwrap();
        assert_eq!(deleted, 1);

        let records = db.with_conn(|conn| get_costs_for_item(conn, hash)).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_insert_idempotent() {
        let db = Db::open_in_memory().unwrap();
        let addr = "0xowner4";
        let hash = "cccc1111111111111111111111111111111111111111111111111111111111111111";

        let cost1 = sample_cost(addr, hash, "0.001");
        db.with_conn(|conn| insert_account_costs(conn, &[cost1]))
            .unwrap();

        // Insert again with updated cost_credit value — should replace.
        let cost2 = AccountCostRecord {
            cost_credit: "0.002".to_string(),
            ..sample_cost(addr, hash, "0.002")
        };
        db.with_conn(|conn| insert_account_costs(conn, &[cost2]))
            .unwrap();

        let records = db.with_conn(|conn| get_costs_for_item(conn, hash)).unwrap();
        assert_eq!(records.len(), 1, "should have replaced, not duplicated");
        assert_eq!(records[0].cost_credit, "0.002");
    }
}
