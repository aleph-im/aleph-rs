//! `pending_txs` accessors. Mirrors `aleph/db/accessors/pending_txs.py`.

use aleph_types::chain::Chain;
use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::db::models::pending_txs::PendingTxDb;

/// Fetch a single pending tx by hash.
pub async fn get_pending_tx(
    client: &impl GenericClient,
    tx_hash: &str,
) -> AlephResult<Option<PendingTxDb>> {
    let row = client
        .query_opt(
            "SELECT tx_hash FROM pending_txs WHERE tx_hash = $1",
            &[&tx_hash],
        )
        .await?;
    Ok(row.as_ref().map(PendingTxDb::from_row))
}

/// Fetch pending txs ordered by chain_tx datetime ascending, up to `limit`.
pub async fn get_pending_txs(
    client: &impl GenericClient,
    limit: i64,
) -> AlephResult<Vec<PendingTxDb>> {
    let sql = "SELECT pt.tx_hash FROM pending_txs pt \
               JOIN chain_txs ct ON pt.tx_hash = ct.hash \
               ORDER BY ct.datetime ASC LIMIT $1";
    let rows = client.query(sql, &[&limit]).await?;
    Ok(rows.iter().map(PendingTxDb::from_row).collect())
}

/// Count pending txs, optionally filtered by chain.
pub async fn count_pending_txs(
    client: &impl GenericClient,
    chain: Option<Chain>,
) -> AlephResult<i64> {
    let row = match chain {
        Some(c) => {
            let chain_s = serde_json::to_value(&c)?
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_default();
            client
                .query_one(
                    "SELECT COUNT(*) FROM pending_txs pt \
                     JOIN chain_txs ct ON pt.tx_hash = ct.hash \
                     WHERE ct.chain = $1",
                    &[&chain_s],
                )
                .await?
        }
        None => {
            client
                .query_one("SELECT COUNT(*) FROM pending_txs", &[])
                .await?
        }
    };
    Ok(row.get::<_, i64>(0))
}

/// Insert (or ignore) a pending tx.
pub async fn upsert_pending_tx(client: &impl GenericClient, tx_hash: &str) -> AlephResult<()> {
    client
        .execute(
            "INSERT INTO pending_txs(tx_hash) VALUES ($1) ON CONFLICT DO NOTHING",
            &[&tx_hash],
        )
        .await?;
    Ok(())
}

/// Delete a pending tx by hash.
pub async fn delete_pending_tx(client: &impl GenericClient, tx_hash: &str) -> AlephResult<()> {
    client
        .execute("DELETE FROM pending_txs WHERE tx_hash = $1", &[&tx_hash])
        .await?;
    Ok(())
}
