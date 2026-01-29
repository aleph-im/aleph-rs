//! Models for the corechannel aggregate, i.e. the aggregate that describes the nodes currently
//! known on the network.

use aleph_types::address;
use aleph_types::chain::Address;
use aleph_types::item_hash::{AlephItemHash, AlephItemHashError};
use aleph_types::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::LazyLock;

pub static CORECHANNEL_ADDRESS: LazyLock<Address> =
    LazyLock::new(|| address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10"));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeHash(AlephItemHash);

impl From<AlephItemHash> for NodeHash {
    fn from(hash: AlephItemHash) -> Self {
        Self(hash)
    }
}

impl FromStr for NodeHash {
    type Err = AlephItemHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(AlephItemHash::from_str(s)?))
    }
}

impl From<NodeHash> for AlephItemHash {
    fn from(hash: NodeHash) -> Self {
        hash.0
    }
}

impl std::fmt::Display for NodeHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CcnInfo {
    pub hash: NodeHash,
    pub name: String,
    pub time: Timestamp,
    pub owner: Address,
    pub score: f64,
    pub reward: Address,
    pub multiaddress: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum CrnStatus {
    /// Linked to a CCN.
    Linked { parent: NodeHash },
    /// Unlinked, waiting to be linked to a CCN.
    Waiting,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CrnInfo {
    pub hash: NodeHash,
    pub name: String,
    pub time: Timestamp,
    pub owner: Address,
    pub score: f64,
    pub reward: Address,
    pub address: String,
    #[serde(flatten)]
    pub status: CrnStatus,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreChannelContent {
    pub nodes: Vec<CcnInfo>,
    pub resource_nodes: Vec<CrnInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreChannelAggregate {
    pub corechannel: CoreChannelContent,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_deserialize_crn_status() {
        let linked_json = r#"{"status": "linked", "parent": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"}"#;
        let waiting_json = r#"{"status": "waiting"}"#;
        let waiting_json_explicit_parent_json = r#"{"status": "waiting", "parent": null}"#;

        let linked: CrnStatus = serde_json::from_str(linked_json).unwrap();
        let waiting: CrnStatus = serde_json::from_str(waiting_json).unwrap();
        let waiting_with_explicit_parent: CrnStatus =
            serde_json::from_str(waiting_json_explicit_parent_json).unwrap();

        assert!(
            matches!(linked, CrnStatus::Linked { parent } if parent == NodeHash::from_str("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef").unwrap())
        );
        assert!(matches!(waiting, CrnStatus::Waiting));
        assert!(matches!(waiting_with_explicit_parent, CrnStatus::Waiting));
    }
}
