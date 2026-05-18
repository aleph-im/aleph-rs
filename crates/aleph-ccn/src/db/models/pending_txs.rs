//! Pending transactions awaiting decode/dispatch.
//!
//! Mirrors `src/aleph/db/models/pending_txs.py`. The Python class carries a
//! lazy relationship to [`ChainTxDb`]; in Rust the caller joins manually, so
//! we store the foreign key only.

/// Row of the `pending_txs` table.
#[derive(Debug, Clone)]
pub struct PendingTxDb {
    /// Foreign key onto `chain_txs.hash`.
    pub tx_hash: String,
}

impl PendingTxDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            tx_hash: row.get("tx_hash"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_tx_construct() {
        let p = PendingTxDb {
            tx_hash: "0xdead".into(),
        };
        assert_eq!(p.tx_hash, "0xdead");
    }
}
