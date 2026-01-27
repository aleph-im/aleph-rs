//! Models for the corechannel aggregate, i.e. the aggregate that describes the nodes currently
//! known on the network.

use aleph_types::address;
use aleph_types::chain::Address;
use aleph_types::item_hash::ItemHash;
use aleph_types::timestamp::Timestamp;
use serde::Deserialize;
use std::fmt::Formatter;
use std::sync::LazyLock;

pub static CORECHANNEL_ADDRESS: LazyLock<Address> =
    LazyLock::new(|| address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10"));

#[derive(Debug, Clone, Deserialize)]
pub struct NodeHash(ItemHash);

impl From<ItemHash> for NodeHash {
    fn from(hash: ItemHash) -> Self {
        Self(hash)
    }
}

impl From<NodeHash> for ItemHash {
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
pub struct CrnInfo {
    pub hash: NodeHash,
    pub name: String,
    pub time: Timestamp,
    pub owner: Address,
    pub score: f64,
    pub reward: Address,
    pub address: String,
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
