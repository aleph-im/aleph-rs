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
/// `value` is the native ETH to attach: the full `sell_amount` when selling
/// ETH (the router wraps it), zero for ERC20 sells (which require a prior
/// allowance to the router).
#[allow(clippy::too_many_arguments)]
pub async fn execute_swap(
    provider: &impl Provider,
    router: Address,
    route: &UniswapRoute,
    sell_amount: U256,
    min_buy_amount: U256,
    receiver: Address,
    deadline_secs: u64,
    value: U256,
) -> Result<B256, SwapError> {
    let call: Vec<u8> = match route.single_fee() {
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
    };

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
}
