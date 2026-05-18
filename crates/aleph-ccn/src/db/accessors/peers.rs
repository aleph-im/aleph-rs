//! `peers` table accessors. Mirrors `aleph/db/accessors/peers.py`.

use chrono::{DateTime, Utc};
use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::db::models::peers::PeerType;
use crate::toolkit::timestamp::utc_now;

/// Fetch all addresses matching `peer_type`, optionally filtered by `last_seen`.
///
/// Mirrors `get_all_addresses_by_peer_type`.
pub async fn get_all_addresses_by_peer_type(
    client: &impl GenericClient,
    peer_type: PeerType,
    last_seen: Option<DateTime<Utc>>,
) -> AlephResult<Vec<String>> {
    let peer_type_s = peer_type.as_value_str();
    let rows = match last_seen {
        Some(ls) => {
            let sql = "SELECT address FROM peers \
                       WHERE peer_type = $1 AND last_seen >= $2";
            client.query(sql, &[&peer_type_s, &ls]).await?
        }
        None => {
            let sql = "SELECT address FROM peers WHERE peer_type = $1";
            client.query(sql, &[&peer_type_s]).await?
        }
    };
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Upsert a peer record. Mirrors `upsert_peer`.
pub async fn upsert_peer(
    client: &impl GenericClient,
    peer_id: &str,
    peer_type: PeerType,
    address: &str,
    source: PeerType,
    last_seen: Option<DateTime<Utc>>,
) -> AlephResult<()> {
    let last_seen = last_seen.unwrap_or_else(utc_now);
    let sql = "INSERT INTO peers(peer_id, peer_type, address, source, last_seen) \
               VALUES ($1, $2, $3, $4, $5) \
               ON CONFLICT ON CONSTRAINT peers_pkey \
               DO UPDATE SET address = EXCLUDED.address, \
                             source = EXCLUDED.source, \
                             last_seen = EXCLUDED.last_seen";
    client
        .execute(
            sql,
            &[
                &peer_id,
                &peer_type.as_value_str(),
                &address,
                &source.as_value_str(),
                &last_seen,
            ],
        )
        .await?;
    Ok(())
}
