//! Uniswap V3 swap routes: an ordered token path with a fee tier per hop.

use alloy_primitives::{Address, Bytes, U256};

use crate::credit::format_token_amount;

/// 0.05% fee tier, in V3 fee units (hundredths of a basis point).
pub const FEE_005_PERCENT: u32 = 500;
/// 0.3% fee tier.
pub const FEE_03_PERCENT: u32 = 3000;
/// 1% fee tier.
pub const FEE_1_PERCENT: u32 = 10000;

/// An ordered V3 route: `tokens` has exactly one more element than `fees`;
/// hop `i` swaps `tokens[i] -> tokens[i+1]` through the `fees[i]` tier pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapRoute {
    tokens: Vec<Address>,
    fees: Vec<u32>,
}

impl UniswapRoute {
    /// Build a route. Panics if the shape invariant is violated; routes are
    /// only constructed from hardcoded candidates, never user input.
    pub fn new(tokens: Vec<Address>, fees: Vec<u32>) -> Self {
        assert_eq!(
            tokens.len(),
            fees.len() + 1,
            "route needs one more token than fees"
        );
        assert!(!fees.is_empty(), "route needs at least one hop");
        Self { tokens, fees }
    }

    pub fn token_in(&self) -> Address {
        self.tokens[0]
    }

    pub fn token_out(&self) -> Address {
        *self.tokens.last().expect("route has tokens")
    }

    /// The single fee tier if this is a one-hop route, else `None`.
    pub fn single_fee(&self) -> Option<u32> {
        (self.fees.len() == 1).then_some(self.fees[0])
    }

    /// Fee tiers per hop, in V3 fee units.
    pub fn fees(&self) -> &[u32] {
        &self.fees
    }

    /// Packed V3 path: `token(20) ++ fee(uint24 BE, 3) ++ token(20) ++ ...`.
    pub fn encode_path(&self) -> Bytes {
        let mut out = Vec::with_capacity(self.tokens.len() * 20 + self.fees.len() * 3);
        out.extend_from_slice(self.tokens[0].as_slice());
        for (fee, token) in self.fees.iter().zip(&self.tokens[1..]) {
            // uint24 big-endian: the low 3 bytes of the u32.
            out.extend_from_slice(&fee.to_be_bytes()[1..]);
            out.extend_from_slice(token.as_slice());
        }
        out.into()
    }

    /// Human fee display, one percent figure per hop: `1%` or `0.05% + 1%`.
    ///
    /// V3 fee units are hundredths of a basis point, so percent = fee / 1e4;
    /// `format_token_amount` with 4 decimals renders it trimmed.
    pub fn fee_display(&self) -> String {
        self.fees_percent().join(" + ")
    }

    /// Fee tiers as percent strings, e.g. `["0.05%", "1%"]` (for JSON output).
    pub fn fees_percent(&self) -> Vec<String> {
        self.fees
            .iter()
            .map(|f| format!("{}%", format_token_amount(U256::from(*f), 4)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
    const ALEPH: Address = address!("27702a26126e0B3702af63Ee09aC4d1A084EF628");

    #[test]
    fn encode_single_hop_path() {
        let route = UniswapRoute::new(vec![WETH, ALEPH], vec![FEE_1_PERCENT]);
        // token(20) ++ 0x002710 ++ token(20) = 43 bytes, hand-computed.
        let expected = concat!(
            "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "002710",
            "27702a26126e0b3702af63ee09ac4d1a084ef628"
        );
        assert_eq!(hex::encode(route.encode_path()), expected);
        assert_eq!(route.encode_path().len(), 43);
    }

    #[test]
    fn encode_two_hop_path() {
        let route = UniswapRoute::new(
            vec![USDC, WETH, ALEPH],
            vec![FEE_005_PERCENT, FEE_1_PERCENT],
        );
        // 0.05% = 500 = 0x0001f4; 1% = 10000 = 0x002710; 66 bytes total.
        let expected = concat!(
            "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0001f4",
            "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "002710",
            "27702a26126e0b3702af63ee09ac4d1a084ef628"
        );
        assert_eq!(hex::encode(route.encode_path()), expected);
        assert_eq!(route.encode_path().len(), 66);
    }

    #[test]
    fn route_endpoints_and_single_fee() {
        let single = UniswapRoute::new(vec![WETH, ALEPH], vec![FEE_1_PERCENT]);
        assert_eq!(single.token_in(), WETH);
        assert_eq!(single.token_out(), ALEPH);
        assert_eq!(single.single_fee(), Some(FEE_1_PERCENT));

        let multi = UniswapRoute::new(
            vec![USDC, WETH, ALEPH],
            vec![FEE_005_PERCENT, FEE_1_PERCENT],
        );
        assert_eq!(multi.token_in(), USDC);
        assert_eq!(multi.token_out(), ALEPH);
        assert_eq!(multi.single_fee(), None);
    }

    #[test]
    fn fee_display_formats() {
        let single = UniswapRoute::new(vec![WETH, ALEPH], vec![FEE_1_PERCENT]);
        assert_eq!(single.fee_display(), "1%");
        let multi = UniswapRoute::new(
            vec![USDC, WETH, ALEPH],
            vec![FEE_005_PERCENT, FEE_1_PERCENT],
        );
        assert_eq!(multi.fee_display(), "0.05% + 1%");
        assert_eq!(multi.fees_percent(), vec!["0.05%", "1%"]);
        let mid = UniswapRoute::new(vec![USDC, ALEPH], vec![FEE_03_PERCENT]);
        assert_eq!(mid.fee_display(), "0.3%");
    }

    #[test]
    #[should_panic(expected = "route needs one more token than fees")]
    fn bad_shape_panics() {
        let _ = UniswapRoute::new(vec![WETH, ALEPH], vec![FEE_1_PERCENT, FEE_1_PERCENT]);
    }
}
