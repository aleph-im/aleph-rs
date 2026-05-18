//! Mirrors `src/aleph/schemas/message_confirmation.py`.
//!
//! Each `MessageConfirmation` records that the message was published on a
//! particular chain in a particular block, with the publisher's address.

use aleph_types::chain::Chain;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageConfirmation {
    /// Chain from which the confirmation was fetched.
    pub chain: Chain,
    /// Block in which the confirmation was published.
    pub height: i64,
    /// Hash of the transaction/block in which the confirmation was published.
    pub hash: String,
    /// Transaction timestamp, in Unix time (number of seconds since epoch).
    pub time: f64,
    /// Publisher of the confirmation on chain.
    pub publisher: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_confirmation_roundtrip() {
        let json = serde_json::json!({
            "chain": "ETH",
            "height": 12345,
            "hash": "0xdeadbeef",
            "time": 1700000000.5,
            "publisher": "0xABCDEF"
        });
        let parsed: MessageConfirmation = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(parsed.chain, Chain::Ethereum);
        assert_eq!(parsed.height, 12345);
        assert_eq!(parsed.hash, "0xdeadbeef");
        assert_eq!(parsed.time, 1700000000.5);
        assert_eq!(parsed.publisher, "0xABCDEF");
        let back = serde_json::to_value(&parsed).unwrap();
        assert_eq!(back, json);
    }
}
