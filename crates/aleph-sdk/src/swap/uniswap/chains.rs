//! Per-chain Uniswap deployment parameters.

use alloy_primitives::{Address, address};

/// Uniswap deployment parameters for a single chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniswapChain {
    /// SwapRouter02 (V3 swaps; payable, no deadline in params, use multicall).
    pub swap_router02: Address,
    /// QuoterV2 (on-chain quoting via `eth_call`).
    pub quoter_v2: Address,
    /// Wrapped native token (WETH) - the V3 token leg used when selling
    /// native ETH (the router wraps `msg.value`).
    pub weth: Address,
}

/// Look up Uniswap deployment parameters for a chain id. `None` if Uniswap
/// V3 is not deployed (or not curated here) for that chain.
pub fn uniswap_chain(chain_id: u64) -> Option<UniswapChain> {
    match chain_id {
        // Ethereum mainnet, verified against the deployment lists on
        // `developers.uniswap.org`.
        1 => Some(UniswapChain {
            swap_router02: address!("68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"),
            quoter_v2: address!("61fFE014bA17989E743c5F6cB21bF9697530B21e"),
            weth: address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_is_supported() {
        let c = uniswap_chain(1).expect("mainnet supported");
        assert_eq!(
            c.swap_router02,
            address!("68b3465833fb72A70ecDF485E0e4C7bD8665Fc45")
        );
        assert_eq!(
            c.quoter_v2,
            address!("61fFE014bA17989E743c5F6cB21bF9697530B21e")
        );
        assert_eq!(c.weth, address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"));
    }

    #[test]
    fn unknown_chain_is_none() {
        assert!(uniswap_chain(999_999).is_none());
    }
}
