pub mod aggregates;
pub mod balances;
pub mod costs;
pub mod files;
pub mod messages;
pub mod posts;
pub mod vms;

use parking_lot::Mutex;
use rusqlite::Connection;
use std::path::Path;

const SCHEMA: &str = include_str!("schema.sql");

/// Thread-safe database handle. Uses a `Mutex<Connection>` for simplicity.
/// For a local dev tool, this is fine — contention is minimal.
pub struct Db {
    conn: Mutex<Connection>,
}

impl Db {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Access the connection under the lock. Use with spawn_blocking from async code.
    pub fn with_conn<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Connection) -> R,
    {
        let conn = self.conn.lock();
        f(&conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_in_memory() {
        let db = Db::open_in_memory().unwrap();
        let count: i64 = db.with_conn(|c| {
            c.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='messages'",
                [],
                |r| r.get(0),
            )
            .unwrap()
        });
        assert_eq!(count, 1);
    }

    #[test]
    fn test_all_tables_created() {
        let db = Db::open_in_memory().unwrap();
        let tables: Vec<String> = db.with_conn(|c| {
            let mut stmt = c
                .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        });
        assert!(tables.contains(&"messages".to_string()));
        assert!(tables.contains(&"aggregates".to_string()));
        assert!(tables.contains(&"aggregate_elements".to_string()));
        assert!(tables.contains(&"posts".to_string()));
        assert!(tables.contains(&"files".to_string()));
        assert!(tables.contains(&"file_pins".to_string()));
        assert!(tables.contains(&"file_tags".to_string()));
        assert!(tables.contains(&"vms".to_string()));
        assert!(tables.contains(&"vm_volumes".to_string()));
        assert!(tables.contains(&"credit_balances".to_string()));
        assert!(tables.contains(&"credit_history".to_string()));
        assert!(tables.contains(&"account_costs".to_string()));
        assert!(tables.contains(&"forgotten_messages".to_string()));
    }
}
