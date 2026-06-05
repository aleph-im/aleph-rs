//! Swap execution through SwapRouter02.
//!
//! SwapRouter02's param structs carry no deadline, so every swap is wrapped
//! in the router's `multicall(uint256 deadline, bytes[] data)` overload,
//! which applies a deadline check to the batched call. Native ETH input is
//! attached as `msg.value` with `tokenIn = WETH`; the router wraps it.

use alloy_primitives::aliases::{U24, U160};
use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use alloy_sol_types::{SolCall, sol};

use crate::swap::uniswap::route::UniswapRoute;
use crate::swap::{SwapError, await_receipt};

sol! {
    #[sol(rpc)]
    interface IV3SwapRouter {
        /// Mirrors `IV3SwapRouter.ExactInputSingleParams` in
        /// Uniswap/swap-router-contracts (NO deadline field; field order and
        /// types must match the deployed ABI exactly).
        struct ExactInputSingleParams {
            address tokenIn;
            address tokenOut;
            uint24 fee;
            address recipient;
            uint256 amountIn;
            uint256 amountOutMinimum;
            uint160 sqrtPriceLimitX96;
        }
        /// Mirrors `IV3SwapRouter.ExactInputParams` (multi-hop; packed path).
        struct ExactInputParams {
            bytes path;
            address recipient;
            uint256 amountIn;
            uint256 amountOutMinimum;
        }
        function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
        function exactInput(ExactInputParams calldata params) external payable returns (uint256 amountOut);
        /// Deadline-checking multicall from PeripheryValidationExtended.
        function multicall(uint256 deadline, bytes[] calldata data) external payable returns (bytes[] memory results);
    }
}

/// Execute a quoted swap through SwapRouter02. Returns the transaction hash.
///
/// Single-hop routes use `exactInputSingle`, multi-hop routes `exactInput`;
/// either is wrapped in `multicall(deadline, ...)` for deadline enforcement.
/// When `native_sell` is true the full `sell_amount` is attached as
/// `msg.value` (the router wraps it to WETH); ERC20 sells attach no value
/// and require a prior allowance to the router. `min_buy_amount` must be
/// the slippage-adjusted quote: zero would disable slippage protection.
#[allow(clippy::too_many_arguments)]
pub async fn execute_swap(
    provider: &impl Provider,
    router: Address,
    route: &UniswapRoute,
    sell_amount: U256,
    min_buy_amount: U256,
    receiver: Address,
    deadline_secs: u64,
    native_sell: bool,
) -> Result<B256, SwapError> {
    debug_assert!(
        !min_buy_amount.is_zero(),
        "min_buy_amount must come from a slippage-adjusted quote"
    );

    // Deriving msg.value here (rather than taking it as a parameter) makes
    // it impossible to attach ETH that diverges from amountIn; any excess
    // would be wrapped by the router and stranded (no refundETH in the call).
    let value = if native_sell { sell_amount } else { U256::ZERO };

    let call = encode_swap_call(route, sell_amount, min_buy_amount, receiver);

    let contract = IV3SwapRouter::new(router, provider);
    let pending = contract
        .multicall(U256::from(deadline_secs), vec![call.into()])
        .value(value)
        .send()
        .await
        .map_err(SwapError::SendTransaction)?;
    let receipt = await_receipt(pending).await?;
    if !receipt.status() {
        return Err(SwapError::Reverted("swap"));
    }
    Ok(receipt.transaction_hash)
}

/// ABI-encode the inner swap call: `exactInputSingle` for one-hop routes,
/// `exactInput` for multi-hop.
fn encode_swap_call(
    route: &UniswapRoute,
    sell_amount: U256,
    min_buy_amount: U256,
    receiver: Address,
) -> Vec<u8> {
    match route.single_fee() {
        Some(fee) => IV3SwapRouter::exactInputSingleCall {
            params: IV3SwapRouter::ExactInputSingleParams {
                tokenIn: route.token_in(),
                tokenOut: route.token_out(),
                fee: U24::from(fee),
                recipient: receiver,
                amountIn: sell_amount,
                amountOutMinimum: min_buy_amount,
                sqrtPriceLimitX96: U160::ZERO,
            },
        }
        .abi_encode(),
        None => IV3SwapRouter::exactInputCall {
            params: IV3SwapRouter::ExactInputParams {
                path: route.encode_path(),
                recipient: receiver,
                amountIn: sell_amount,
                amountOutMinimum: min_buy_amount,
            },
        }
        .abi_encode(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::keccak256;

    #[test]
    fn selectors_match_deployed_abi() {
        // Cross-check the sol!-generated selectors against the canonical
        // SwapRouter02 signature strings.
        let cases: [(&str, [u8; 4]); 3] = [
            (
                "exactInputSingle((address,address,uint24,address,uint256,uint256,uint160))",
                IV3SwapRouter::exactInputSingleCall::SELECTOR,
            ),
            (
                "exactInput((bytes,address,uint256,uint256))",
                IV3SwapRouter::exactInputCall::SELECTOR,
            ),
            (
                "multicall(uint256,bytes[])",
                IV3SwapRouter::multicallCall::SELECTOR,
            ),
        ];
        for (sig, selector) in cases {
            let expected = &keccak256(sig.as_bytes())[..4];
            assert_eq!(selector, expected, "selector mismatch for {sig}");
        }
    }

    #[test]
    fn encode_swap_call_picks_branch_by_hop_count() {
        use crate::swap::uniswap::route::{FEE_1_PERCENT, FEE_005_PERCENT};
        use alloy_primitives::address;

        let a = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let b = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
        let c = address!("27702a26126e0B3702af63Ee09aC4d1A084EF628");
        let recv = address!("1111111111111111111111111111111111111111");

        let single = UniswapRoute::new(vec![b, c], vec![FEE_1_PERCENT]);
        let call = encode_swap_call(&single, U256::from(1u64), U256::from(1u64), recv);
        assert_eq!(&call[..4], IV3SwapRouter::exactInputSingleCall::SELECTOR);

        let multi = UniswapRoute::new(vec![a, b, c], vec![FEE_005_PERCENT, FEE_1_PERCENT]);
        let call = encode_swap_call(&multi, U256::from(1u64), U256::from(1u64), recv);
        assert_eq!(&call[..4], IV3SwapRouter::exactInputCall::SELECTOR);
    }
}
