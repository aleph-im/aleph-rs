//! Uniswap V3 provider.
//!
//! Quotes on-chain through QuoterV2 (`eth_call`, no API key) and executes
//! through SwapRouter02. Unlike CoW, swaps execute immediately at the pool
//! price and the caller pays gas; the pool fee is embedded in the quoted
//! price rather than charged separately.

pub mod chains;
pub mod route;
pub mod router;

use alloy_primitives::{Address, U256};
use alloy_provider::Provider;
use alloy_sol_types::sol;

use crate::swap::uniswap::chains::UniswapChain;
use crate::swap::uniswap::route::{FEE_1_PERCENT, FEE_03_PERCENT, FEE_005_PERCENT, UniswapRoute};
use crate::swap::{SwapError, SwapRequest, apply_slippage};

sol! {
    #[sol(rpc)]
    interface IQuoterV2 {
        /// State-mutating by design (it reverts internally to compute the
        /// result) but meant to be invoked via `eth_call`.
        function quoteExactInput(bytes memory path, uint256 amountIn)
            external
            returns (
                uint256 amountOut,
                uint160[] memory sqrtPriceX96AfterList,
                uint32[] memory initializedTicksCrossedList,
                uint256 gasEstimate
            );
    }
}

/// A priced Uniswap quote, before the user confirms.
///
/// There is no separate fee amount: the pool fee is embedded in the quoted
/// price ([`UniswapRoute::fee_display`] names the tiers) and gas is paid on
/// top by the sender.
#[derive(Debug, Clone)]
pub struct UniswapQuote {
    /// Sell amount the swap will consume (atoms; equals the user input).
    pub sell_amount: U256,
    /// Expected ALEPH out at the quoted price (atoms, before slippage).
    pub buy_amount: U256,
    /// Minimum ALEPH out after slippage (atoms). Enforced as
    /// `amountOutMinimum` in the swap call.
    pub min_buy_amount: U256,
    /// The route that produced the best quote.
    pub route: UniswapRoute,
}

/// Quote one route via QuoterV2 (an `eth_call`; nothing is sent on-chain).
async fn quote_route(
    provider: &impl Provider,
    quoter: Address,
    route: &UniswapRoute,
    amount_in: U256,
) -> Result<U256, SwapError> {
    let q = IQuoterV2::new(quoter, provider);
    let out = q
        .quoteExactInput(route.encode_path(), amount_in)
        .call()
        .await
        .map_err(SwapError::Quote)?;
    Ok(out.amountOut)
}

/// Pick the route with the highest non-zero output.
///
/// Per-route errors are tolerated as long as one candidate succeeds (a
/// missing pool reverts the quote); if none does, the last error is
/// surfaced, or [`SwapError::NoRoute`] when every quote returned zero.
fn pick_best_route(
    outcomes: Vec<(UniswapRoute, Result<U256, SwapError>)>,
) -> Result<(UniswapRoute, U256), SwapError> {
    let mut best: Option<(UniswapRoute, U256)> = None;
    let mut last_err: Option<SwapError> = None;
    for (route, outcome) in outcomes {
        match outcome {
            Ok(out) if !out.is_zero() => {
                if best.as_ref().is_none_or(|(_, b)| out > *b) {
                    best = Some((route, out));
                }
            }
            Ok(_) => {}
            Err(e) => last_err = Some(e),
        }
    }
    match best {
        Some(found) => Ok(found),
        None => Err(last_err.unwrap_or(SwapError::NoRoute)),
    }
}

fn build_quote(req: &SwapRequest, route: UniswapRoute, buy_amount: U256) -> UniswapQuote {
    UniswapQuote {
        sell_amount: req.sell_amount,
        buy_amount,
        min_buy_amount: apply_slippage(buy_amount, req.slippage),
        route,
    }
}

/// Quote a native-ETH sell. The single route is the deepest ALEPH pool
/// (WETH/ALEPH 1%); SwapRouter02 wraps the attached ETH at execution time.
pub async fn quote_eth(
    provider: &impl Provider,
    chain: &UniswapChain,
    req: &SwapRequest,
) -> Result<UniswapQuote, SwapError> {
    let route = UniswapRoute::new(vec![chain.weth, req.buy_token], vec![FEE_1_PERCENT]);
    let buy = quote_route(provider, chain.quoter_v2, &route, req.sell_amount).await?;
    if buy.is_zero() {
        return Err(SwapError::NoRoute);
    }
    Ok(build_quote(req, route, buy))
}

/// The hardcoded USDC->ALEPH candidates, in quoting order: the direct 0.3%
/// pool, then the two-hop path through the canonical USDC/WETH 0.05% pool
/// and the deepest ALEPH pool (WETH/ALEPH 1%).
fn usdc_candidate_routes(chain: &UniswapChain, usdc: Address, aleph: Address) -> Vec<UniswapRoute> {
    vec![
        UniswapRoute::new(vec![usdc, aleph], vec![FEE_03_PERCENT]),
        UniswapRoute::new(
            vec![usdc, chain.weth, aleph],
            vec![FEE_005_PERCENT, FEE_1_PERCENT],
        ),
    ]
}

/// Quote a USDC sell: quote every candidate route and keep the best output.
pub async fn quote_usdc(
    provider: &impl Provider,
    chain: &UniswapChain,
    usdc_token: Address,
    req: &SwapRequest,
) -> Result<UniswapQuote, SwapError> {
    let mut outcomes = Vec::new();
    for route in usdc_candidate_routes(chain, usdc_token, req.buy_token) {
        let outcome = quote_route(provider, chain.quoter_v2, &route, req.sell_amount).await;
        outcomes.push((route, outcome));
    }
    let (route, buy) = pick_best_route(outcomes)?;
    Ok(build_quote(req, route, buy))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, keccak256};
    use alloy_sol_types::SolCall;

    const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
    const ALEPH: Address = address!("27702a26126e0B3702af63Ee09aC4d1A084EF628");

    fn direct() -> UniswapRoute {
        UniswapRoute::new(vec![USDC, ALEPH], vec![FEE_03_PERCENT])
    }

    fn two_hop() -> UniswapRoute {
        UniswapRoute::new(
            vec![USDC, WETH, ALEPH],
            vec![FEE_005_PERCENT, FEE_1_PERCENT],
        )
    }

    #[test]
    fn quote_exact_input_selector_matches_deployed_abi() {
        // Cross-checks the sol!-generated selector against the canonical
        // QuoterV2 signature string.
        let sig = "quoteExactInput(bytes,uint256)";
        let expected = &keccak256(sig.as_bytes())[..4];
        assert_eq!(IQuoterV2::quoteExactInputCall::SELECTOR, expected);
    }

    #[test]
    fn pick_best_route_prefers_higher_output() {
        let picked = pick_best_route(vec![
            (direct(), Ok(U256::from(100u64))),
            (two_hop(), Ok(U256::from(200u64))),
        ])
        .expect("best");
        assert_eq!(picked.0, two_hop());
        assert_eq!(picked.1, U256::from(200u64));
    }

    #[test]
    fn pick_best_route_tolerates_one_failure() {
        let picked = pick_best_route(vec![
            (direct(), Err(SwapError::NoRoute)),
            (two_hop(), Ok(U256::from(7u64))),
        ])
        .expect("best");
        assert_eq!(picked.0, two_hop());
    }

    #[test]
    fn pick_best_route_skips_zero_outputs() {
        let picked = pick_best_route(vec![
            (direct(), Ok(U256::ZERO)),
            (two_hop(), Ok(U256::from(1u64))),
        ])
        .expect("best");
        assert_eq!(picked.0, two_hop());
    }

    #[test]
    fn pick_best_route_surfaces_error_when_all_fail() {
        let err =
            pick_best_route(vec![(direct(), Err(SwapError::NoRoute))]).expect_err("must fail");
        assert!(matches!(err, SwapError::NoRoute));
    }

    #[test]
    fn pick_best_route_no_route_when_all_zero() {
        let err = pick_best_route(vec![(direct(), Ok(U256::ZERO))]).expect_err("must fail");
        assert!(matches!(err, SwapError::NoRoute));
    }

    #[test]
    fn usdc_candidates_shape() {
        let chain = crate::swap::uniswap::chains::uniswap_chain(1).expect("mainnet");
        let candidates = usdc_candidate_routes(&chain, USDC, ALEPH);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], direct());
        assert_eq!(candidates[1], two_hop());
    }
}
