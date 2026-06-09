//! Token swaps for acquiring ALEPH from within the CLI.
//!
//! The swap types abstract the venue; [`cow`] is the CoW Swap
//! implementation and [`uniswap`] the Uniswap V3 one. v1 supports selling
//! native ETH or USDC for ALEPH.

pub mod cow;
pub mod uniswap;

use std::time::Duration;

use alloy_network::Network;
use alloy_primitives::{Address, U256};
use alloy_provider::{PendingTransactionBuilder, Provider};
use alloy_sol_types::sol;
use thiserror::Error;

/// What the user pays with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapToken {
    /// Native ETH.
    Eth,
    /// USD Coin (USDC).
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

    /// Display ticker for the sell token.
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

#[derive(Debug, Error)]
pub enum SwapError {
    #[error("swaps are not supported on chainId {0}")]
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
    Sign(#[source] alloy_signer::Error),
    #[error("Uniswap quote failed")]
    Quote(#[source] alloy_contract::Error),
    #[error("no Uniswap route returned a usable quote for this pair")]
    NoRoute,
    #[error("failed to read ERC20 allowance")]
    ReadAllowance(#[source] alloy_contract::Error),
    #[error("failed to send transaction")]
    SendTransaction(#[source] alloy_contract::Error),
    #[error("timed out after {timeout_secs}s waiting for transaction receipt")]
    ReceiptTimeout { timeout_secs: u64 },
    #[error("failed to confirm transaction")]
    Receipt(#[source] alloy_provider::PendingTransactionError),
    #[error("{0} transaction reverted")]
    Reverted(&'static str),
    #[error("invalid amount in CoW response: {0}")]
    InvalidAmount(String),
}

/// Maximum wait for a transaction receipt before giving up.
pub(crate) const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);

/// Wait for `pending` to be mined and return its receipt, failing on timeout
/// or RPC error.
///
/// The Reverted check is left to the caller so each use site can name the
/// offending operation in the error message.
pub(crate) async fn await_receipt<N: Network>(
    pending: PendingTransactionBuilder<N>,
) -> Result<N::ReceiptResponse, SwapError> {
    tokio::time::timeout(RECEIPT_TIMEOUT, pending.get_receipt())
        .await
        .map_err(|_| SwapError::ReceiptTimeout {
            timeout_secs: RECEIPT_TIMEOUT.as_secs(),
        })?
        .map_err(SwapError::Receipt)
}

/// Apply slippage to a buy amount: `floor(buy * (1 - slippage))`.
///
/// `slippage` is a fraction and must be in `[0.0, 1.0)`; callers validate
/// user input before reaching this point.
pub fn apply_slippage(buy_amount: U256, slippage: f64) -> U256 {
    debug_assert!(
        slippage.is_finite() && (0.0..1.0).contains(&slippage),
        "slippage must be in [0, 1); got {slippage}"
    );
    // Scale by 1e9 to keep precision without floats on U256.
    let scale = ((1.0 - slippage) * 1_000_000_000.0).round() as u64;
    buy_amount * U256::from(scale) / U256::from(1_000_000_000u64)
}

sol! {
    #[sol(rpc)]
    interface IERC20Allowance {
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }
}

/// Ensure `owner` has at least `amount` allowance for `spender` on `token`;
/// submit an `approve` tx and await its receipt if not.
///
/// Approves exactly `amount` rather than an unlimited allowance, so a fresh
/// approval transaction may be needed per swap. This is a deliberate tradeoff
/// to minimise standing approval exposure.
pub async fn ensure_allowance(
    provider: &impl Provider,
    token: Address,
    owner: Address,
    spender: Address,
    amount: U256,
) -> Result<(), SwapError> {
    let erc20 = IERC20Allowance::new(token, provider);
    // The allowance read and the subsequent approve are not atomic (inherent
    // ERC20 approve race); the spender pulls at settlement/swap time.
    let current = erc20
        .allowance(owner, spender)
        .call()
        .await
        .map_err(SwapError::ReadAllowance)?;
    if current >= amount {
        return Ok(());
    }
    let pending = erc20
        .approve(spender, amount)
        .send()
        .await
        .map_err(SwapError::SendTransaction)?;
    let receipt = await_receipt(pending).await?;
    if !receipt.status() {
        return Err(SwapError::Reverted("approve"));
    }
    Ok(())
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

    #[test]
    fn apply_slippage_half_percent() {
        let buy = U256::from(1_000_000_000_000_000_000u128); // 1 ALEPH
        let min = apply_slippage(buy, 0.005);
        // 0.5% off 1e18 == 9.95e17.
        assert_eq!(min, U256::from(995_000_000_000_000_000u128));
    }

    #[test]
    fn apply_slippage_zero_is_identity() {
        let buy = U256::from(12_345u64);
        assert_eq!(apply_slippage(buy, 0.0), buy);
    }

    #[test]
    fn apply_slippage_max_fraction() {
        // 0.5 (50%) is the largest slippage the CLI accepts.
        let buy = U256::from(1_000_000u64);
        assert_eq!(apply_slippage(buy, 0.5), U256::from(500_000u64));
    }
}
