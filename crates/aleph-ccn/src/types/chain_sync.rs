//! Chain synchronization protocols and event types.
//!
//! Mirrors `src/aleph/types/chain_sync.py` from pyaleph.

use serde::{Deserialize, Serialize};

/// Protocol used to synchronize messages between the chain and the CCN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChainSyncProtocol {
    /// Message sync tx where the messages are in the tx data.
    #[serde(rename = "aleph")]
    OnChainSync,
    /// Message sync tx where the messages to fetch are in an IPFS hash.
    #[serde(rename = "aleph-offchain")]
    OffChainSync,
    /// Messages sent by a smart contract.
    #[serde(rename = "smart-contract")]
    SmartContract,
}

/// Type of chain event consumed by the CCN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChainEventType {
    /// Messages sent on-chain using the Aleph smart contract.
    #[serde(rename = "message")]
    Message,
    /// Synchronisation messages sent by a CCN to the Aleph smart contract.
    #[serde(rename = "sync")]
    Sync,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_sync_protocol_roundtrip() {
        let cases = [
            (ChainSyncProtocol::OnChainSync, "\"aleph\""),
            (ChainSyncProtocol::OffChainSync, "\"aleph-offchain\""),
            (ChainSyncProtocol::SmartContract, "\"smart-contract\""),
        ];
        for (variant, expected) in cases {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            let parsed: ChainSyncProtocol = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn chain_event_type_roundtrip() {
        let cases = [
            (ChainEventType::Message, "\"message\""),
            (ChainEventType::Sync, "\"sync\""),
        ];
        for (variant, expected) in cases {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            let parsed: ChainEventType = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, variant);
        }
    }
}
