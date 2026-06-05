//! Native-ETH swaps via the CoWSwapEthFlow contract.
//!
//! Selling native ETH is a *push*: we send one on-chain `createOrder` tx with
//! the sold ETH attached as `msg.value`. The contract wraps ETH to WETH and
//! posts the order for solvers. No ERC20 approval and no off-chain signature.
//! Per CoW's fee-in-price model the order's `feeAmount` is zero and
//! `sellAmount` (== `msg.value`) carries the full quoted total.

use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use alloy_sol_types::sol;

use crate::swap::SwapError;
use crate::swap::await_receipt;
use crate::swap::cow::order::app_data_hash;

sol! {
    #[sol(rpc)]
    interface CoWSwapEthFlow {
        /// Mirrors `EthFlowOrder.Data` in cowprotocol/ethflowcontract
        /// (field order and types must match the deployed ABI exactly).
        struct EthFlowOrderData {
            address buyToken;
            address receiver;
            uint256 sellAmount;
            uint256 buyAmount;
            bytes32 appData;
            uint256 feeAmount;
            uint32 validTo;
            bool partiallyFillable;
            int64 quoteId;
        }
        function createOrder(EthFlowOrderData calldata order) external payable returns (bytes32 orderHash);
    }
}

/// Submit a native-ETH sell order through the ETH-flow contract. Returns the
/// transaction hash (the order is indexed off the emitted event).
///
/// `sell_amount` is the full quoted total (sell + fee) and is attached as
/// `msg.value`; the order's `feeAmount` is zero per the fee-in-price model.
#[allow(clippy::too_many_arguments)]
pub async fn create_eth_order(
    provider: &impl Provider,
    ethflow: Address,
    buy_token: Address,
    receiver: Address,
    sell_amount: U256,
    min_buy_amount: U256,
    valid_to: u32,
    quote_id: i64,
) -> Result<B256, SwapError> {
    let contract = CoWSwapEthFlow::new(ethflow, provider);
    let order = CoWSwapEthFlow::EthFlowOrderData {
        buyToken: buy_token,
        receiver,
        sellAmount: sell_amount,
        buyAmount: min_buy_amount,
        appData: app_data_hash(),
        feeAmount: U256::ZERO,
        validTo: valid_to,
        partiallyFillable: false,
        quoteId: quote_id,
    };
    let pending = contract
        .createOrder(order)
        .value(sell_amount)
        .send()
        .await
        .map_err(SwapError::SendTransaction)?;
    let receipt = await_receipt(pending).await?;
    if !receipt.status() {
        return Err(SwapError::Reverted("createOrder"));
    }
    Ok(receipt.transaction_hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::keccak256;
    use alloy_sol_types::SolCall;

    #[test]
    fn create_order_selector_matches_deployed_abi() {
        // Cross-checks the sol!-generated selector against the canonical ABI
        // signature string from cowprotocol/ethflowcontract.
        let sig =
            "createOrder((address,address,uint256,uint256,bytes32,uint256,uint32,bool,int64))";
        let expected = &keccak256(sig.as_bytes())[..4];
        assert_eq!(CoWSwapEthFlow::createOrderCall::SELECTOR, expected);
    }
}
