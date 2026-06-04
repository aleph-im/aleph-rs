//! Integration tests for the CoW Swap provider.
//!
//! USDC-path tests use a wiremock server (no network). ETH-flow tests use a
//! spawned anvil node and are `#[ignore]`d (require foundry in PATH).

use aleph_sdk::swap::cow::order::APP_DATA_JSON;
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
            "appData": APP_DATA_JSON
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

    let uid = place_usdc_order(&api, 1, USDC, &req, &quote, &resp, &signer)
        .await
        .expect("place order");
    assert!(uid.starts_with("0x"));
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
