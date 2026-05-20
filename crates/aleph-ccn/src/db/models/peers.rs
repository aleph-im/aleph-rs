//! Peer records (`peers` table).
//!
//! Mirrors `src/aleph/db/models/peers.py`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{AlephError, AlephResult};

/// Where a peer was discovered / what protocol announces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PeerType {
    #[serde(rename = "HTTP")]
    Http,
    #[serde(rename = "IPFS")]
    Ipfs,
    #[serde(rename = "P2P")]
    P2p,
}

impl PeerType {
    pub fn as_value_str(self) -> &'static str {
        match self {
            PeerType::Http => "HTTP",
            PeerType::Ipfs => "IPFS",
            PeerType::P2p => "P2P",
        }
    }
}

impl TryFrom<&str> for PeerType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "HTTP" => Ok(PeerType::Http),
            "IPFS" => Ok(PeerType::Ipfs),
            "P2P" => Ok(PeerType::P2p),
            other => Err(format!("unknown PeerType: {other}")),
        }
    }
}

impl std::fmt::Display for PeerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_value_str())
    }
}

/// Row of the `peers` table.
#[derive(Debug, Clone)]
pub struct PeerDb {
    pub peer_id: String,
    pub peer_type: PeerType,
    pub address: String,
    pub source: PeerType,
    pub last_seen: DateTime<Utc>,
}

impl PeerDb {
    /// Build a [`PeerDb`] from a database row. `peer_type` and `source` are
    /// stored as text by the Python `ChoiceType(PeerType)` mapping.
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self::try_from_row(row).expect("valid PeerDb row")
    }

    pub fn try_from_row(row: &tokio_postgres::Row) -> AlephResult<Self> {
        let peer_type_s: String = row.get("peer_type");
        let source_s: String = row.get("source");
        let peer_type = PeerType::try_from(peer_type_s.as_str())
            .map_err(|e| AlephError::InvalidMessage(format!("{e} in DB")))?;
        let source = PeerType::try_from(source_s.as_str())
            .map_err(|e| AlephError::InvalidMessage(format!("{e} in DB")))?;
        Ok(Self {
            peer_id: row.get("peer_id"),
            peer_type,
            address: row.get("address"),
            source,
            last_seen: row.get("last_seen"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_type_roundtrip() {
        for variant in [PeerType::Http, PeerType::Ipfs, PeerType::P2p] {
            let s = variant.as_value_str();
            assert_eq!(PeerType::try_from(s).unwrap(), variant);
            let json = serde_json::to_string(&variant).unwrap();
            let back: PeerType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, variant);
        }
        assert!(PeerType::try_from("nope").is_err());
    }

    #[test]
    fn invalid_peer_type_returns_error() {
        assert!(PeerType::try_from("nope").is_err());
    }

    #[test]
    fn peer_db_construct() {
        let p = PeerDb {
            peer_id: "QmPeer".into(),
            peer_type: PeerType::P2p,
            address: "/ip4/127.0.0.1/tcp/4001".into(),
            source: PeerType::Http,
            last_seen: Utc::now(),
        };
        assert_eq!(p.peer_id, "QmPeer");
        assert_eq!(p.peer_type, PeerType::P2p);
        assert_eq!(p.source, PeerType::Http);
    }
}
