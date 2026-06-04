//! Token swaps for acquiring ALEPH from within the CLI.
//!
//! The swap types abstract the venue; [`cow`] is the CoW Swap
//! implementation. v1 supports selling native ETH or USDC for ALEPH.

pub mod cow;

use alloy_primitives::{Address, U256};
use thiserror::Error;

/// What the user pays with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapToken {
    /// Native ETH (sold via CoW's on-chain ETH-flow contract).
    Eth,
    /// USDC (sold via an off-chain signed order).
    Usdc,
}

impl SwapToken {
    /// Number of decimals for the sell token.
    pub fn decimals(self) -> u8 {
        match self {
            SwapToken::Eth => 18,
            SwapToken::Usdc => 6,
        }
    }

    pub fn symbol(self) -> &'static str {
        match self {
            SwapToken::Eth => "ETH",
            SwapToken::Usdc => "USDC",
        }
    }
}

/// A request to sell `sell_amount` of `sell_token` for ALEPH.
#[derive(Debug, Clone)]
pub struct SwapRequest {
    pub sell_token: SwapToken,
    /// Raw smallest-unit amount of the sell token.
    pub sell_amount: U256,
    /// ALEPH (buy) token address for the active network.
    pub buy_token: Address,
    /// Where the bought ALEPH should land.
    pub receiver: Address,
    /// The owner/signer address paying for the swap.
    pub from: Address,
    /// Max acceptable slippage as a fraction (e.g. 0.005 == 0.5%).
    pub slippage: f64,
    /// Order validity window in seconds.
    pub valid_for_secs: u32,
}

/// A priced quote, before the user confirms.
#[derive(Debug, Clone)]
pub struct SwapQuote {
    /// Sell amount the order will actually consume (atoms).
    pub sell_amount: U256,
    /// Expected ALEPH out at the quoted price (atoms, before slippage).
    pub buy_amount: U256,
    /// Minimum ALEPH out after applying slippage (atoms). Enforced in the order.
    pub min_buy_amount: U256,
    /// Fee amount in the sell token (atoms).
    pub fee_amount: U256,
}

/// An accepted order's identifier (CoW order UID, 56-byte hex; or an
/// ETH-flow on-chain order hash).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderUid(pub String);

#[derive(Debug, Error)]
pub enum SwapError {
    #[error("network (chainId {0}) is not supported by CoW Swap")]
    UnsupportedChain(u64),
    #[error("failed to build HTTP client")]
    HttpClientBuild(#[source] reqwest::Error),
    #[error("CoW API request failed")]
    Request(#[source] reqwest::Error),
    #[error("CoW API returned HTTP {status}: {body}")]
    BadStatus {
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("failed to parse CoW API response")]
    Parse(#[source] reqwest::Error),
    #[error("failed to sign order")]
    Sign(String),
    #[error("on-chain transaction failed")]
    Transaction(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swap_token_decimals_and_symbol() {
        assert_eq!(SwapToken::Eth.decimals(), 18);
        assert_eq!(SwapToken::Usdc.decimals(), 6);
        assert_eq!(SwapToken::Eth.symbol(), "ETH");
        assert_eq!(SwapToken::Usdc.symbol(), "USDC");
    }
}
