//! Integration tests for the CoW Swap provider.
//!
//! USDC-path tests use a wiremock server (no network). ETH-flow tests use a
//! spawned anvil node and are `#[ignore]`d (require foundry in PATH).

use aleph_sdk::swap::cow::order::AppData;
use aleph_sdk::swap::cow::{CowApi, place_usdc_order, quote_usdc};
use aleph_sdk::swap::{SwapRequest, SwapToken};
use alloy_primitives::{Address, U256, address};
use alloy_signer_local::PrivateKeySigner;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const USDC: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const ALEPH: Address = address!("27702a26126e0B3702af63Ee09aC4d1A084EF628");

fn test_signer() -> PrivateKeySigner {
    // Deterministic anvil test key (publicly known; test-only).
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        .parse()
        .unwrap()
}

#[tokio::test]
async fn usdc_quote_then_place_order_round_trip() {
    let server = MockServer::start().await;

    // Quote: 50 USDC sell, fee 1 USDC, expect 1 ALEPH out.
    Mock::given(method("POST"))
        .and(path("/api/v1/quote"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "quote": {
                "sellToken": format!("{USDC:#x}"),
                "buyToken": format!("{ALEPH:#x}"),
                "receiver": "0x1111111111111111111111111111111111111111",
                "sellAmount": "49000000",
                "buyAmount": "1000000000000000000",
                "feeAmount": "1000000",
                "validTo": 2000000000u32,
                "appData": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "kind": "sell",
                "partiallyFillable": false,
                "sellTokenBalance": "erc20",
                "buyTokenBalance": "erc20",
                "signingScheme": "eip712"
            },
            "from": "0x1111111111111111111111111111111111111111",
            "expiration": "2026-01-01T00:00:00Z",
            "id": 42
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Orders: assert the fee-in-price construction in the submitted body.
    Mock::given(method("POST"))
        .and(path("/api/v1/orders"))
        .and(body_partial_json(serde_json::json!({
            "sellAmount": "50000000",
            "feeAmount": "0",
            "kind": "sell",
            "signingScheme": "eip712",
            "quoteId": 42,
            "appData": AppData::COW_JSON
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!(
            "0xabc0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000abcd"
        )))
        .expect(1)
        .mount(&server)
        .await;

    let http = reqwest::Client::new();
    let api = CowApi::with_base_url(http, format!("{}/api/v1", server.uri()));

    let signer = test_signer();
    let req = SwapRequest {
        sell_token: SwapToken::Usdc,
        sell_amount: U256::from(50_000_000u64),
        buy_token: ALEPH,
        receiver: signer.address(),
        from: signer.address(),
        slippage: 0.005,
        valid_for_secs: 1200,
    };

    let (quote, resp) = quote_usdc(&api, USDC, &req).await.expect("quote");
    // Fee-in-price: total sell = quoted sell + fee.
    assert_eq!(quote.sell_amount, U256::from(50_000_000u64));
    assert_eq!(quote.buy_amount, U256::from(1_000_000_000_000_000_000u128));
    assert_eq!(
        quote.min_buy_amount,
        U256::from(995_000_000_000_000_000u128)
    );
    assert_eq!(quote.fee_amount, U256::from(1_000_000u64));

    let uid = place_usdc_order(&api, 1, USDC, &AppData::cow(), &req, &quote, &resp, &signer)
        .await
        .expect("place order");
    assert!(uid.starts_with("0x"));
}

#[tokio::test]
async fn put_app_data_posts_full_document_and_returns_hash() {
    let server = MockServer::start().await;
    let hash = "0xec501d43f8cf80098b69d17365c624e98f318601b8347174388be5818d05a80a";
    Mock::given(method("PUT"))
        .and(path(format!("/api/v1/app_data/{hash}")))
        .and(body_partial_json(serde_json::json!({
            "fullAppData": AppData::OPHIS_JSON,
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!(hash)))
        .expect(1)
        .mount(&server)
        .await;

    let api = CowApi::with_base_url(reqwest::Client::new(), format!("{}/api/v1", server.uri()));
    api.put_app_data(hash, AppData::OPHIS_JSON)
        .await
        .expect("put_app_data ok");
}

#[tokio::test]
#[ignore = "requires anvil in PATH (install via foundry)"]
async fn eth_flow_create_order_sends_value_with_calldata() {
    use aleph_sdk::swap::cow::ethflow::create_eth_order;
    use alloy_network::EthereumWallet;
    use alloy_node_bindings::Anvil;
    use alloy_provider::{Provider, ProviderBuilder};
    use alloy_rpc_types_eth::TransactionTrait;
    use alloy_signer_local::PrivateKeySigner;

    let anvil = Anvil::new().try_spawn().expect("spawn anvil");
    let signer: PrivateKeySigner = (&anvil.keys()[0]).into();
    let owner = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(anvil.endpoint_url());

    // No ETH-flow contract exists on a fresh anvil; a tx to an EOA with
    // calldata + value still succeeds (it is a plain transfer), so this
    // exercises the encoding/value plumbing end to end and then inspects
    // the mined transaction.
    let recipient = anvil.addresses()[1];
    let sell_amount = U256::from(1_000_000_000_000_000u128); // 0.001 ETH
    let tx_hash = create_eth_order(
        &provider,
        recipient, // stand-in for the ethflow contract address
        ALEPH,
        owner,
        AppData::ophis().hash,
        sell_amount,
        U256::from(1u64),
        2_000_000_000,
        42,
    )
    .await
    .expect("send createOrder tx");

    let tx = provider
        .get_transaction_by_hash(tx_hash)
        .await
        .expect("fetch tx")
        .expect("tx exists");
    // Value attached must equal sell_amount (fee-in-price: no fee on top).
    assert_eq!(tx.value(), sell_amount);
    // Calldata must encode exactly: 4-byte selector + 9 ABI words (one
    // tuple argument whose 9 fields are each padded to 32 bytes).
    let input = tx.input().clone();
    let selector = &alloy_primitives::keccak256(
        "createOrder((address,address,uint256,uint256,bytes32,uint256,uint32,bool,int64))"
            .as_bytes(),
    )[..4];
    assert_eq!(
        &input[..4],
        selector,
        "first 4 bytes must be the createOrder selector"
    );
    assert_eq!(
        input.len(),
        4 + 9 * 32,
        "calldata must be selector + 9 ABI words"
    );
}

#[tokio::test]
async fn quote_error_is_surfaced() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/quote"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "errorType": "SellAmountDoesNotCoverFee",
            "description": "fee exceeds sell amount"
        })))
        .mount(&server)
        .await;

    let api = CowApi::with_base_url(reqwest::Client::new(), format!("{}/api/v1", server.uri()));
    let signer = test_signer();
    let req = SwapRequest {
        sell_token: SwapToken::Usdc,
        sell_amount: U256::from(1u64),
        buy_token: ALEPH,
        receiver: signer.address(),
        from: signer.address(),
        slippage: 0.005,
        valid_for_secs: 1200,
    };

    let err = quote_usdc(&api, USDC, &req).await.expect_err("must fail");
    let msg = err.to_string();
    assert!(msg.contains("400"), "got: {msg}");
}
