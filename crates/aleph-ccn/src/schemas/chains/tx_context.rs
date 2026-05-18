//! Mirrors `src/aleph/schemas/chains/tx_context.py`.

pub use crate::schemas::message_confirmation::MessageConfirmation;

/// Transaction context. At the moment a confirmation is just an on-chain
/// transaction, so `TxContext` is an alias for `MessageConfirmation`.
pub type TxContext = MessageConfirmation;

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::chain::Chain;

    #[test]
    fn test_tx_context_alias_roundtrip() {
        let ctx = TxContext {
            chain: Chain::Ethereum,
            height: 1,
            hash: "h".into(),
            time: 1.0,
            publisher: "p".into(),
        };
        let json = serde_json::to_value(&ctx).unwrap();
        let back: TxContext = serde_json::from_value(json).unwrap();
        assert_eq!(back, ctx);
    }
}
