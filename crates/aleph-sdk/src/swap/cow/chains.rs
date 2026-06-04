//! Per-chain CoW Swap deployment parameters.

use alloy_primitives::{Address, address};

/// CoW Swap deployment parameters for a single chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CowChain {
    /// Orderbook API path slug, e.g. "mainnet".
    pub api_slug: &'static str,
    /// CoWSwapEthFlow contract (native-ETH orders).
    pub ethflow: Address,
    /// Wrapped native token (WETH) address - the sell token used when
    /// quoting a native-ETH swap.
    pub weth: Address,
}

/// Look up CoW deployment parameters for a chain id. `None` if CoW Swap is
/// not deployed (or not curated here) for that chain.
pub fn cow_chain(chain_id: u64) -> Option<CowChain> {
    match chain_id {
        // Ethereum mainnet. EthFlow address is the current production
        // deployment, from cowprotocol/ethflowcontract main-artifacts
        // networks.prod.json.
        1 => Some(CowChain {
            api_slug: "mainnet",
            ethflow: address!("BA3cB449bD2B4adddBc894D8697F5170800EAdEC"),
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
        let c = cow_chain(1).expect("mainnet supported");
        assert_eq!(c.api_slug, "mainnet");
        assert_eq!(
            c.ethflow,
            address!("BA3cB449bD2B4adddBc894D8697F5170800EAdEC")
        );
        assert_eq!(c.weth, address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"));
    }

    #[test]
    fn unknown_chain_is_none() {
        assert!(cow_chain(999_999).is_none());
    }
}
