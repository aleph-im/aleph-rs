//! P2P protocol identifiers.
//!
//! Mirrors `src/aleph/types/protocol.py`.

use serde::{Deserialize, Serialize};

/// Underlying transport protocol used to reach a peer/content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Ipfs,
    P2p,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_roundtrip() {
        assert_eq!(serde_json::to_string(&Protocol::Ipfs).unwrap(), "\"ipfs\"");
        assert_eq!(serde_json::to_string(&Protocol::P2p).unwrap(), "\"p2p\"");
        let p: Protocol = serde_json::from_str("\"ipfs\"").unwrap();
        assert_eq!(p, Protocol::Ipfs);
        let p: Protocol = serde_json::from_str("\"p2p\"").unwrap();
        assert_eq!(p, Protocol::P2p);
    }
}
