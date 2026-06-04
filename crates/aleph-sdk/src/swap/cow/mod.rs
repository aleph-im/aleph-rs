//! CoW Swap provider.

pub mod chains;
pub mod ethflow;
pub mod order;

use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_provider::Provider;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::sol;
use serde::{Deserialize, Serialize};

use crate::swap::{SwapError, SwapQuote, SwapRequest};

/// CoW quote endpoint computes prices server-side and can be slower than simple REST reads.
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// Maximum wait for an approve transaction receipt before giving up.
pub(crate) const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);

/// CoW orderbook REST client for a single network.
pub struct CowApi {
    http: reqwest::Client,
    /// Base, e.g. "https://api.cow.fi/mainnet/api/v1".
    base_url: String,
}

/// `POST /quote` request body for a sell order. Amounts are decimal strings of
/// atoms, per the CoW API.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteRequest {
    pub sell_token: String,
    pub buy_token: String,
    pub from: String,
    pub receiver: String,
    pub kind: String, // "sell"
    pub sell_amount_before_fee: String,
    pub valid_for: u32,
    /// Set for on-chain (ETH-flow) orders; the API default is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onchain_order: Option<bool>,
    /// "eip712" (default) for signed orders, "eip1271" for ETH-flow quotes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signing_scheme: Option<String>,
}

/// `POST /quote` response (only the fields we consume).
#[derive(Debug, Deserialize)]
pub struct QuoteResponse {
    pub quote: QuoteParams,
    #[serde(default)]
    pub id: Option<i64>,
}

/// The priced order parameters inside a quote response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteParams {
    pub sell_amount: String,
    pub buy_amount: String,
    pub fee_amount: String,
    pub valid_to: u32,
    pub app_data: String,
}

/// `POST /orders` request body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderCreation {
    pub sell_token: String,
    pub buy_token: String,
    pub receiver: String,
    pub sell_amount: String,
    pub buy_amount: String,
    pub valid_to: u32,
    pub app_data: String,
    pub fee_amount: String,
    pub kind: String,
    pub partially_fillable: bool,
    pub sell_token_balance: String,
    pub buy_token_balance: String,
    pub signing_scheme: String,
    pub signature: String,
    pub from: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_id: Option<i64>,
}

impl CowApi {
    /// Build a client for `chain_id`, or `UnsupportedChain` if CoW is not
    /// curated for it.
    pub fn new(chain_id: u64) -> Result<Self, SwapError> {
        let chain = chains::cow_chain(chain_id).ok_or(SwapError::UnsupportedChain(chain_id))?;
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .user_agent(concat!("aleph-sdk/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(SwapError::HttpClientBuild)?;
        Ok(Self {
            http,
            base_url: format!("https://api.cow.fi/{}/api/v1", chain.api_slug),
        })
    }

    /// Override the base URL (used by tests to point at a mock server).
    #[doc(hidden)]
    pub fn with_base_url(http: reqwest::Client, base_url: String) -> Self {
        Self { http, base_url }
    }

    /// POSTs to {base}/quote, returns the priced quote.
    pub async fn quote(&self, req: &QuoteRequest) -> Result<QuoteResponse, SwapError> {
        let resp = self
            .http
            .post(format!("{}/quote", self.base_url))
            .json(req)
            .send()
            .await
            .map_err(SwapError::Request)?;
        Self::parse_json(resp).await
    }

    /// POSTs to {base}/orders, returns the order UID string.
    pub async fn place_order(&self, body: &OrderCreation) -> Result<String, SwapError> {
        let resp = self
            .http
            .post(format!("{}/orders", self.base_url))
            .json(body)
            .send()
            .await
            .map_err(SwapError::Request)?;
        // 201 -> a bare JSON string UID.
        let uid: String = Self::parse_json(resp).await?;
        Ok(uid)
    }

    async fn parse_json<T: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
    ) -> Result<T, SwapError> {
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(SwapError::BadStatus { status, body });
        }
        resp.json::<T>().await.map_err(SwapError::Parse)
    }
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

/// Format an `Address` as a lowercase `0x`-prefixed hex string for the API.
pub fn addr_hex(a: Address) -> String {
    format!("{a:#x}")
}

sol! {
    #[sol(rpc)]
    interface IERC20Allowance {
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }
}

/// Ensure `owner` has at least `amount` allowance for the vault relayer on
/// `token`; submit an `approve` tx and await its receipt if not.
///
/// Approves exactly `amount` rather than an unlimited allowance, so a fresh
/// approval transaction may be needed per swap. This is a deliberate tradeoff
/// to minimise standing approval exposure.
pub async fn ensure_allowance(
    provider: &impl Provider,
    token: Address,
    owner: Address,
    amount: U256,
) -> Result<(), SwapError> {
    let erc20 = IERC20Allowance::new(token, provider);
    // The allowance read and the subsequent approve are not atomic (inherent
    // ERC20 approve race); CoW pulls at settlement time.
    let current = erc20
        .allowance(owner, order::VAULT_RELAYER)
        .call()
        .await
        .map_err(SwapError::ReadAllowance)?;
    if current >= amount {
        return Ok(());
    }
    let pending = erc20
        .approve(order::VAULT_RELAYER, amount)
        .send()
        .await
        .map_err(SwapError::SendTransaction)?;
    let receipt = tokio::time::timeout(RECEIPT_TIMEOUT, pending.get_receipt())
        .await
        .map_err(|_| SwapError::ReceiptTimeout {
            timeout_secs: RECEIPT_TIMEOUT.as_secs(),
        })?
        .map_err(SwapError::Receipt)?;
    if !receipt.status() {
        return Err(SwapError::Reverted("approve"));
    }
    Ok(())
}

/// Fetch a CoW sell quote and translate it into a [`SwapQuote`] with the
/// slippage floor applied.
///
/// Per CoW's fee-in-price model, the returned `sell_amount` is
/// `quote.sellAmount + quote.feeAmount`: the total the order consumes, which
/// the order carries with `feeAmount = 0`.
pub async fn quote_sell(
    api: &CowApi,
    sell_token: Address,
    req: &SwapRequest,
    onchain: bool,
) -> Result<(SwapQuote, QuoteResponse), SwapError> {
    let q = QuoteRequest {
        sell_token: addr_hex(sell_token),
        buy_token: addr_hex(req.buy_token),
        from: addr_hex(req.from),
        receiver: addr_hex(req.receiver),
        kind: "sell".to_string(),
        sell_amount_before_fee: req.sell_amount.to_string(),
        valid_for: req.valid_for_secs,
        onchain_order: onchain.then_some(true),
        signing_scheme: onchain.then(|| "eip1271".to_string()),
    };
    let resp = api.quote(&q).await?;
    let quoted_sell = parse_atoms(&resp.quote.sell_amount)?;
    let buy_amount = parse_atoms(&resp.quote.buy_amount)?;
    let fee_amount = parse_atoms(&resp.quote.fee_amount)?;
    let min_buy_amount = apply_slippage(buy_amount, req.slippage);
    Ok((
        SwapQuote {
            sell_amount: quoted_sell.checked_add(fee_amount).ok_or_else(|| {
                SwapError::InvalidAmount("sellAmount + feeAmount overflows U256".to_string())
            })?,
            buy_amount,
            min_buy_amount,
            fee_amount,
        },
        resp,
    ))
}

fn parse_atoms(s: &str) -> Result<U256, SwapError> {
    s.parse::<U256>()
        .map_err(|e| SwapError::InvalidAmount(format!("'{s}': {e}")))
}

/// Quote a USDC sell (off-chain signed order path).
pub async fn quote_usdc(
    api: &CowApi,
    usdc_token: Address,
    req: &SwapRequest,
) -> Result<(SwapQuote, QuoteResponse), SwapError> {
    quote_sell(api, usdc_token, req, false).await
}

/// Quote a native-ETH sell. The sell token is the chain's WETH (the ETH-flow
/// contract wraps ETH, so CoW prices the WETH leg) and the quote is flagged
/// as an on-chain (eip1271) order.
pub async fn quote_eth(
    api: &CowApi,
    weth_token: Address,
    req: &SwapRequest,
) -> Result<(SwapQuote, QuoteResponse), SwapError> {
    quote_sell(api, weth_token, req, true).await
}

/// Build, sign (EIP-712) and submit a USDC sell order. Returns the order UID.
///
/// The order follows CoW's fee-in-price model: `sellAmount` is the quoted
/// sell + fee total and `feeAmount` is zero. The body carries the full
/// appData JSON document; the signed order carries its keccak256 hash.
pub async fn place_usdc_order(
    api: &CowApi,
    chain_id: u64,
    usdc_token: Address,
    req: &SwapRequest,
    quote: &SwapQuote,
    quote_resp: &QuoteResponse,
    signer: &PrivateKeySigner,
) -> Result<String, SwapError> {
    let order = order::Order {
        sellToken: usdc_token,
        buyToken: req.buy_token,
        receiver: req.receiver,
        sellAmount: quote.sell_amount,
        buyAmount: quote.min_buy_amount,
        validTo: quote_resp.quote.valid_to,
        appData: order::app_data_hash(),
        feeAmount: U256::ZERO,
        kind: "sell".to_string(),
        partiallyFillable: false,
        sellTokenBalance: "erc20".to_string(),
        buyTokenBalance: "erc20".to_string(),
    };
    let signature = order::sign_order(&order, chain_id, signer)?;
    // Keep in sync with the signed `Order` above: same amounts and appData (body
    // carries the full JSON document; the signed struct carries its keccak256 hash).
    let body = OrderCreation {
        sell_token: addr_hex(usdc_token),
        buy_token: addr_hex(req.buy_token),
        receiver: addr_hex(req.receiver),
        sell_amount: quote.sell_amount.to_string(),
        buy_amount: quote.min_buy_amount.to_string(),
        valid_to: quote_resp.quote.valid_to,
        app_data: order::APP_DATA_JSON.to_string(),
        fee_amount: "0".to_string(),
        kind: "sell".to_string(),
        partially_fillable: false,
        sell_token_balance: "erc20".to_string(),
        buy_token_balance: "erc20".to_string(),
        signing_scheme: "eip712".to_string(),
        signature,
        from: addr_hex(req.from),
        quote_id: quote_resp.id,
    };
    api.place_order(&body).await
}

#[cfg(test)]
mod unit_tests {
    use super::*;

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

    #[test]
    fn quote_request_serializes_camel_case() {
        // Without optional fields.
        let req = QuoteRequest {
            sell_token: "0xaaa".into(),
            buy_token: "0xbbb".into(),
            from: "0xccc".into(),
            receiver: "0xddd".into(),
            kind: "sell".into(),
            sell_amount_before_fee: "1000000".into(),
            valid_for: 300,
            onchain_order: None,
            signing_scheme: None,
        };
        let v = serde_json::to_value(&req).expect("serialize");
        let obj = v.as_object().expect("object");
        let keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        assert!(keys.contains(&"sellToken"), "sellToken missing");
        assert!(keys.contains(&"buyToken"), "buyToken missing");
        assert!(keys.contains(&"from"), "from missing");
        assert!(keys.contains(&"receiver"), "receiver missing");
        assert!(keys.contains(&"kind"), "kind missing");
        assert!(
            keys.contains(&"sellAmountBeforeFee"),
            "sellAmountBeforeFee missing"
        );
        assert!(keys.contains(&"validFor"), "validFor missing");
        assert!(
            !keys.contains(&"onchainOrder"),
            "onchainOrder should be absent when None"
        );
        assert!(
            !keys.contains(&"signingScheme"),
            "signingScheme should be absent when None"
        );
        assert_eq!(obj.len(), 7, "unexpected extra keys");

        // With optional fields set.
        let req2 = QuoteRequest {
            onchain_order: Some(true),
            signing_scheme: Some("eip1271".into()),
            ..req
        };
        let v2 = serde_json::to_value(&req2).expect("serialize with optionals");
        let obj2 = v2.as_object().expect("object");
        assert_eq!(obj2["onchainOrder"], serde_json::Value::Bool(true));
        assert_eq!(
            obj2["signingScheme"],
            serde_json::Value::String("eip1271".into())
        );
    }

    #[test]
    fn quote_response_parses_subset() {
        let json = r#"{
            "quote": {
                "sellToken": "0xaaa",
                "buyToken": "0xbbb",
                "receiver": "0xccc",
                "sellAmount": "999000000000000000",
                "buyAmount": "500000000000000000000",
                "feeAmount": "1000000000000000",
                "validTo": 1717000000,
                "appData": "0xdeadbeef",
                "partiallyFillable": false,
                "kind": "sell"
            },
            "id": 42,
            "expiration": "2024-12-31T00:00:00Z",
            "verified": true
        }"#;

        let resp: QuoteResponse = serde_json::from_str(json).expect("deserialize");
        assert_eq!(resp.quote.sell_amount, "999000000000000000");
        assert_eq!(resp.quote.buy_amount, "500000000000000000000");
        assert_eq!(resp.quote.fee_amount, "1000000000000000");
        assert_eq!(resp.quote.valid_to, 1_717_000_000u32);
        assert_eq!(resp.quote.app_data, "0xdeadbeef");
        assert_eq!(resp.id, Some(42));
    }
}
