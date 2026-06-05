//! Integration tests for the Uniswap provider.
//!
//! Quote tests use alloy's mocked transport (no network). The execution
//! test uses a spawned anvil node and is `#[ignore]`d (requires foundry in
//! PATH).

use aleph_sdk::swap::uniswap::chains::uniswap_chain;
use aleph_sdk::swap::uniswap::{IQuoterV2, quote_usdc};
use aleph_sdk::swap::{SwapRequest, SwapToken};
use alloy_primitives::aliases::U160;
use alloy_primitives::{Address, Bytes, U256, address};
use alloy_provider::ProviderBuilder;
use alloy_provider::mock::Asserter;
use alloy_sol_types::SolCall;

const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const ALEPH: Address = address!("27702a26126e0B3702af63Ee09aC4d1A084EF628");
const RECEIVER: Address = address!("1111111111111111111111111111111111111111");

fn swap_request(sell_amount: U256) -> SwapRequest {
    SwapRequest {
        sell_token: SwapToken::Usdc,
        sell_amount,
        buy_token: ALEPH,
        receiver: RECEIVER,
        from: RECEIVER,
        slippage: 0.005,
        valid_for_secs: 1200,
    }
}

/// ABI-encode a QuoterV2 `quoteExactInput` return with the given output.
fn quote_return(amount_out: U256) -> Bytes {
    IQuoterV2::quoteExactInputCall::abi_encode_returns(&IQuoterV2::quoteExactInputReturn {
        amountOut: amount_out,
        sqrtPriceX96AfterList: Vec::<U160>::new(),
        initializedTicksCrossedList: Vec::<u32>::new(),
        gasEstimate: U256::ZERO,
    })
    .into()
}

#[tokio::test]
async fn usdc_quote_picks_best_route() {
    let asserter = Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let chain = uniswap_chain(1).expect("mainnet");

    // Candidates are quoted in declaration order: direct USDC|0.3%|ALEPH
    // first, then USDC|0.05%|WETH|1%|ALEPH. Make the two-hop quote better.
    asserter.push_success(&quote_return(U256::from(1_000_000_000_000_000_000u128)));
    asserter.push_success(&quote_return(U256::from(2_000_000_000_000_000_000u128)));

    let req = swap_request(U256::from(50_000_000u64)); // 50 USDC
    let quote = quote_usdc(&provider, &chain, USDC, &req)
        .await
        .expect("quote");

    assert_eq!(quote.sell_amount, U256::from(50_000_000u64));
    assert_eq!(quote.buy_amount, U256::from(2_000_000_000_000_000_000u128));
    // 0.5% slippage off 2e18.
    assert_eq!(
        quote.min_buy_amount,
        U256::from(1_990_000_000_000_000_000u128)
    );
    // The two-hop route won.
    assert_eq!(quote.route.fees(), &[500, 10000]);
    assert_eq!(quote.route.fee_display(), "0.05% + 1%");
}

#[tokio::test]
async fn usdc_quote_tolerates_one_reverting_route() {
    let asserter = Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let chain = uniswap_chain(1).expect("mainnet");

    // Direct pool quote reverts (e.g. no pool); two-hop succeeds.
    asserter.push_failure_msg("execution reverted");
    asserter.push_success(&quote_return(U256::from(3_000_000_000_000_000_000u128)));

    let req = swap_request(U256::from(50_000_000u64));
    let quote = quote_usdc(&provider, &chain, USDC, &req)
        .await
        .expect("quote");
    assert_eq!(quote.buy_amount, U256::from(3_000_000_000_000_000_000u128));
    assert_eq!(quote.route.fees(), &[500, 10000]);
}

#[tokio::test]
async fn usdc_quote_fails_when_all_routes_revert() {
    let asserter = Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let chain = uniswap_chain(1).expect("mainnet");

    asserter.push_failure_msg("execution reverted");
    asserter.push_failure_msg("execution reverted");

    let req = swap_request(U256::from(50_000_000u64));
    let err = quote_usdc(&provider, &chain, USDC, &req)
        .await
        .expect_err("must fail");
    let msg = format!("{err}");
    assert!(msg.contains("quote failed"), "got: {msg}");
}

#[tokio::test]
#[ignore = "requires anvil in PATH (install via foundry)"]
async fn execute_swap_sends_value_with_multicall_calldata() {
    use aleph_sdk::swap::uniswap::route::{FEE_1_PERCENT, UniswapRoute};
    use aleph_sdk::swap::uniswap::router::{IV3SwapRouter, execute_swap};
    use alloy_network::EthereumWallet;
    use alloy_node_bindings::Anvil;
    use alloy_provider::Provider;
    use alloy_rpc_types_eth::TransactionTrait;
    use alloy_signer_local::PrivateKeySigner;

    let anvil = Anvil::new().try_spawn().expect("spawn anvil");
    let signer: PrivateKeySigner = (&anvil.keys()[0]).into();
    let owner = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(anvil.endpoint_url());

    // No router exists on a fresh anvil; a tx to an EOA with calldata +
    // value still succeeds (it is a plain transfer), so this exercises the
    // encoding/value plumbing end to end and then inspects the mined tx.
    let weth = uniswap_chain(1).expect("mainnet").weth;
    let recipient = anvil.addresses()[1]; // stand-in for SwapRouter02
    let route = UniswapRoute::new(vec![weth, ALEPH], vec![FEE_1_PERCENT]);
    let sell_amount = U256::from(1_000_000_000_000_000u128); // 0.001 ETH
    let deadline = 2_000_000_000u64;

    let tx_hash = execute_swap(
        &provider,
        recipient,
        &route,
        sell_amount,
        U256::from(1u64),
        owner,
        deadline,
        true, // native ETH: sell_amount attached as msg.value
    )
    .await
    .expect("send swap tx");

    let tx = provider
        .get_transaction_by_hash(tx_hash)
        .await
        .expect("fetch tx")
        .expect("tx exists");
    // Value attached must equal sell_amount (the router wraps it to WETH).
    assert_eq!(tx.value(), sell_amount);
    // Calldata must be a deadline-checked multicall wrapping one
    // exactInputSingle call.
    let input = tx.input().clone();
    let decoded = IV3SwapRouter::multicallCall::abi_decode(&input).expect("multicall calldata");
    assert_eq!(decoded.deadline, U256::from(deadline));
    assert_eq!(decoded.data.len(), 1, "exactly one wrapped call");
    assert_eq!(
        &decoded.data[0][..4],
        IV3SwapRouter::exactInputSingleCall::SELECTOR,
        "wrapped call must be exactInputSingle"
    );
}
