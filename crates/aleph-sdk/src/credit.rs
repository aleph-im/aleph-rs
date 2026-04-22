use std::time::Duration;

use alloy::primitives::{Address, U256, address};
use alloy::providers::Provider;
use alloy::sol;
use serde::{Deserialize, Serialize};

/// ALEPH ERC20 token on Ethereum mainnet.
pub const MAINNET_ALEPH_TOKEN_ADDRESS: Address =
    address!("27702a26126e0B3702af63Ee09aC4d1A084EF628");

/// USDC ERC20 token on Ethereum mainnet.
pub const MAINNET_USDC_TOKEN_ADDRESS: Address =
    address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

/// Credit contract address on Ethereum mainnet.
pub const MAINNET_CREDIT_CONTRACT: Address = address!("6b55F32Ea969910838defd03746Ced5E2AE8cB8B");

/// Default Ethereum RPC endpoint for mainnet.
pub const MAINNET_RPC_URL: &str = "https://eth.llamarpc.com";

/// Etherscan transaction URL prefix for mainnet.
pub const MAINNET_EXPLORER_TX_BASE: &str = "https://etherscan.io/tx/";

/// Maximum wait for a transaction receipt before giving up.
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);

/// Timeout for outbound HTTP calls (e.g. CoinGecko price lookup).
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// How to determine the USD price for the ALEPH token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PriceSource {
    /// Fetch live ALEPH/USD from CoinGecko.
    CoinGecko,
    /// Hard-coded USD price (useful for testnets with mock tokens).
    Fixed { usd: f64 },
    /// No price feed available; estimate is unknown.
    None,
}

impl std::str::FromStr for PriceSource {
    type Err = String;

    /// Parse the CLI-facing `<coingecko|fixed:N|none>` form.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        match trimmed.to_ascii_lowercase().as_str() {
            "coingecko" => return Ok(PriceSource::CoinGecko),
            "none" => return Ok(PriceSource::None),
            _ => {}
        }
        if let Some(rest) = trimmed
            .strip_prefix("fixed:")
            .or_else(|| trimmed.strip_prefix("FIXED:"))
        {
            let usd: f64 = rest.parse().map_err(|e| {
                format!("invalid fixed price '{rest}': {e} (expected 'fixed:<number>')")
            })?;
            if !usd.is_finite() || usd < 0.0 {
                return Err(format!(
                    "fixed price must be a non-negative number, got {usd}"
                ));
            }
            return Ok(PriceSource::Fixed { usd });
        }
        Err(format!(
            "invalid price source '{s}': expected 'coingecko', 'fixed:<number>', or 'none'"
        ))
    }
}

/// Ethereum settlement config for a network.
///
/// Lives inside [`crate::config`]-style network profiles on the CLI side, but
/// the shape is owned by the SDK because the SDK is what transacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EthereumConfig {
    pub rpc_url: String,
    pub credit_contract: Address,
    pub aleph_token: Address,
    pub usdc_token: Address,
    pub price_source: PriceSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explorer_tx_base: Option<String>,
}

impl EthereumConfig {
    /// Production defaults: Ethereum mainnet tokens + credit contract +
    /// CoinGecko price feed + etherscan explorer prefix.
    pub fn mainnet_defaults() -> Self {
        Self {
            rpc_url: MAINNET_RPC_URL.to_string(),
            credit_contract: MAINNET_CREDIT_CONTRACT,
            aleph_token: MAINNET_ALEPH_TOKEN_ADDRESS,
            usdc_token: MAINNET_USDC_TOKEN_ADDRESS,
            price_source: PriceSource::CoinGecko,
            explorer_tx_base: Some(MAINNET_EXPLORER_TX_BASE.to_string()),
        }
    }

    /// Resolve the ERC20 token address for a given [`CreditToken`] variant.
    pub fn token_address(&self, token: CreditToken) -> Address {
        match token {
            CreditToken::Aleph => self.aleph_token,
            CreditToken::Usdc => self.usdc_token,
        }
    }
}

/// Token types accepted for credit purchase.
#[derive(Debug, Clone, Copy, Serialize)]
pub enum CreditToken {
    Aleph,
    Usdc,
}

impl CreditToken {
    /// Number of decimal places for the token.
    pub fn decimals(&self) -> u8 {
        match self {
            CreditToken::Aleph => 18,
            CreditToken::Usdc => 6,
        }
    }

    /// Bonus ratio applied to credit conversion.
    pub fn bonus_ratio(&self) -> f64 {
        match self {
            CreditToken::Aleph => 0.2,
            CreditToken::Usdc => 0.0,
        }
    }

    pub fn symbol(&self) -> &'static str {
        match self {
            CreditToken::Aleph => "ALEPH",
            CreditToken::Usdc => "USDC",
        }
    }
}

/// Result of a credit estimate.
///
/// `price_usd` and `estimated_credits` are `None` when the network's
/// [`PriceSource`] is `None` — we still parsed the amount but cannot compute
/// a USD-denominated credit figure.
pub struct CreditEstimate {
    pub token: CreditToken,
    pub amount_raw: U256,
    pub estimated_credits: Option<f64>,
    pub price_usd: Option<f64>,
    pub bonus_ratio: f64,
}

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function transfer(address to, uint256 amount) external returns (bool);
    }
}

const COINGECKO_PRICE_URL: &str =
    "https://api.coingecko.com/api/v3/simple/price?ids=aleph&vs_currencies=usd";

#[derive(Deserialize)]
struct CoinGeckoPriceResponse {
    aleph: CoinGeckoPriceEntry,
}

#[derive(Deserialize)]
struct CoinGeckoPriceEntry {
    usd: f64,
}

/// Parse a human-readable decimal amount into the token's smallest unit as U256.
///
/// Examples:
/// - parse_token_amount("100", 18) => 100 * 10^18
/// - parse_token_amount("50.5", 6) => 50_500_000
/// - parse_token_amount("0.000001", 6) => 1
///
/// Rejects amounts with more decimal places than the token supports,
/// negative values, zero, and non-numeric input.
pub fn parse_token_amount(amount_str: &str, decimals: u8) -> Result<U256, String> {
    let amount_str = amount_str.trim();
    if amount_str.is_empty() {
        return Err("amount cannot be empty".to_string());
    }
    if amount_str.starts_with('-') {
        return Err("amount cannot be negative".to_string());
    }

    let (integer_part, decimal_part) = match amount_str.split_once('.') {
        Some((int, dec)) => (int, dec),
        None => (amount_str, ""),
    };

    // Validate parts are numeric
    if !integer_part.chars().all(|c| c.is_ascii_digit()) || integer_part.is_empty() {
        return Err(format!("invalid amount: '{amount_str}'"));
    }
    if !decimal_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("invalid amount: '{amount_str}'"));
    }

    // Check decimal places don't exceed token decimals
    let decimal_len = decimal_part.len();
    if decimal_len > decimals as usize {
        return Err(format!(
            "too many decimal places: {} has at most {} decimals",
            amount_str, decimals
        ));
    }

    // Pad decimal part to full precision: "50.5" with 6 decimals => "50" + "500000"
    let padded_decimal = format!("{:0<width$}", decimal_part, width = decimals as usize);

    // Combine: integer * 10^decimals + padded_decimal
    let integer_value =
        U256::from_str_radix(integer_part, 10).map_err(|e| format!("invalid integer part: {e}"))?;
    let decimal_value = U256::from_str_radix(&padded_decimal, 10)
        .map_err(|e| format!("invalid decimal part: {e}"))?;
    let scale = U256::from(10u64).pow(U256::from(decimals));

    let result = integer_value * scale + decimal_value;
    if result.is_zero() {
        return Err("amount must be greater than zero".to_string());
    }
    Ok(result)
}

/// Resolve the ALEPH/USD price according to the configured [`PriceSource`].
///
/// Returns `Ok(None)` for `PriceSource::None` (caller will render an
/// unknown-estimate state); `Ok(Some(_))` otherwise.
async fn resolve_aleph_price_usd(source: &PriceSource) -> Result<Option<f64>, String> {
    match source {
        PriceSource::CoinGecko => fetch_aleph_price_usd_from_coingecko().await.map(Some),
        PriceSource::Fixed { usd } => Ok(Some(*usd)),
        PriceSource::None => Ok(None),
    }
}

async fn fetch_aleph_price_usd_from_coingecko() -> Result<f64, String> {
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))?;
    let resp = client
        .get(COINGECKO_PRICE_URL)
        .send()
        .await
        .map_err(|e| format!("failed to fetch ALEPH price: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!(
            "CoinGecko API returned HTTP {}: try again later",
            resp.status()
        ));
    }

    let body: CoinGeckoPriceResponse = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse CoinGecko response: {e}"))?;

    Ok(body.aleph.usd)
}

/// Convert a raw token amount to f64 via U256 arithmetic.
///
/// Splits into integer and fractional parts before the final f64 cast so the
/// precision loss happens once, at the end, rather than via a decimal string
/// round-trip. Practical token amounts fit in u128; we saturate on overflow.
pub(crate) fn u256_to_f64(amount_raw: U256, decimals: u8) -> f64 {
    let scale = U256::from(10u64).pow(U256::from(decimals));
    let whole: u128 = (amount_raw / scale).try_into().unwrap_or(u128::MAX);
    let frac: u128 = (amount_raw % scale).try_into().unwrap_or(0);
    let scale_f = 10f64.powi(decimals as i32);
    whole as f64 + frac as f64 / scale_f
}

/// Estimate how many credits will be received for a given token amount.
///
/// For USDC the price is always $1.00. For ALEPH the price comes from the
/// network's [`PriceSource`] — CoinGecko, a fixed USD value, or `None`
/// (estimate cannot be computed; result carries `price_usd: None`).
pub async fn estimate_credits(
    token: CreditToken,
    amount_raw: U256,
    price_source: &PriceSource,
) -> Result<CreditEstimate, String> {
    let price_usd = match token {
        CreditToken::Usdc => Some(1.0),
        CreditToken::Aleph => resolve_aleph_price_usd(price_source).await?,
    };

    let bonus = token.bonus_ratio();
    let estimated_credits = price_usd.map(|p| {
        let amount_f64 = u256_to_f64(amount_raw, token.decimals());
        amount_f64 * p * (1.0 + bonus) * 1_000_000.0
    });

    Ok(CreditEstimate {
        token,
        amount_raw,
        estimated_credits,
        price_usd,
        bonus_ratio: bonus,
    })
}

/// Check the ERC20 token balance of an address.
pub async fn check_balance(
    provider: &impl Provider,
    owner: Address,
    token: CreditToken,
    token_address: Address,
) -> Result<U256, String> {
    let contract = IERC20::new(token_address, provider);
    let result = contract
        .balanceOf(owner)
        .call()
        .await
        .map_err(|e| format!("failed to check {} balance: {e}", token.symbol()))?;
    Ok(result)
}

/// Check the native ETH balance of an address.
pub async fn check_eth_balance(provider: &impl Provider, owner: Address) -> Result<U256, String> {
    provider
        .get_balance(owner)
        .await
        .map_err(|e| format!("failed to check ETH balance: {e}"))
}

/// Format a U256 token amount into a human-readable string with the given decimals.
pub fn format_token_amount(amount: U256, decimals: u8) -> String {
    let scale = U256::from(10u64).pow(U256::from(decimals));
    let integer = amount / scale;
    let remainder = amount % scale;
    if remainder.is_zero() {
        format!("{integer}")
    } else {
        let decimal_str = format!("{:0>width$}", remainder, width = decimals as usize);
        let trimmed = decimal_str.trim_end_matches('0');
        format!("{integer}.{trimmed}")
    }
}

/// Submit an ERC20 transfer to the credit contract.
///
/// The provider must include a signer (e.g. built with `ProviderBuilder::wallet()`).
/// Returns the transaction receipt after confirmation.
pub async fn buy_credits(
    provider: &impl Provider,
    token_address: Address,
    credit_contract: Address,
    amount_raw: U256,
) -> Result<alloy::rpc::types::TransactionReceipt, String> {
    let contract = IERC20::new(token_address, provider);
    let tx = contract.transfer(credit_contract, amount_raw);
    let pending = tx
        .send()
        .await
        .map_err(|e| format!("failed to send transaction: {e}"))?;

    let receipt = tokio::time::timeout(RECEIPT_TIMEOUT, pending.get_receipt())
        .await
        .map_err(|_| {
            format!(
                "timed out after {}s waiting for transaction receipt",
                RECEIPT_TIMEOUT.as_secs()
            )
        })?
        .map_err(|e| format!("failed to get transaction receipt: {e}"))?;

    if !receipt.status() {
        return Err("transaction reverted".to_string());
    }

    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_whole_number_18_decimals() {
        let result = parse_token_amount("100", 18).unwrap();
        assert_eq!(
            result,
            U256::from(100u64) * U256::from(10u64).pow(U256::from(18u64))
        );
    }

    #[test]
    fn parse_whole_number_6_decimals() {
        let result = parse_token_amount("50", 6).unwrap();
        assert_eq!(result, U256::from(50_000_000u64));
    }

    #[test]
    fn parse_decimal_amount() {
        let result = parse_token_amount("50.5", 6).unwrap();
        assert_eq!(result, U256::from(50_500_000u64));
    }

    #[test]
    fn parse_small_decimal() {
        let result = parse_token_amount("0.000001", 6).unwrap();
        assert_eq!(result, U256::from(1u64));
    }

    #[test]
    fn reject_too_many_decimals() {
        let err = parse_token_amount("1.0000001", 6).unwrap_err();
        assert!(err.contains("too many decimal places"));
    }

    #[test]
    fn reject_negative() {
        let err = parse_token_amount("-1", 18).unwrap_err();
        assert!(err.contains("negative"));
    }

    #[test]
    fn reject_zero() {
        let err = parse_token_amount("0", 18).unwrap_err();
        assert!(err.contains("greater than zero"));
    }

    #[test]
    fn reject_empty() {
        let err = parse_token_amount("", 18).unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn reject_non_numeric() {
        let err = parse_token_amount("abc", 18).unwrap_err();
        assert!(err.contains("invalid"));
    }

    #[test]
    fn parse_one_wei() {
        let result = parse_token_amount("0.000000000000000001", 18).unwrap();
        assert_eq!(result, U256::from(1u64));
    }

    #[tokio::test]
    async fn estimate_usdc_credits() {
        let estimate = estimate_credits(
            CreditToken::Usdc,
            U256::from(100_000_000u64),
            // USDC ignores the price source; any value is fine.
            &PriceSource::None,
        )
        .await
        .unwrap();

        assert_eq!(estimate.price_usd, Some(1.0));
        assert_eq!(estimate.bonus_ratio, 0.0);
        assert_eq!(estimate.estimated_credits, Some(100_000_000.0));
    }

    #[tokio::test]
    async fn estimate_aleph_with_fixed_price() {
        let one_aleph = U256::from(10u64).pow(U256::from(18u64));
        let estimate = estimate_credits(
            CreditToken::Aleph,
            one_aleph,
            &PriceSource::Fixed { usd: 0.5 },
        )
        .await
        .unwrap();

        assert_eq!(estimate.price_usd, Some(0.5));
        assert_eq!(estimate.bonus_ratio, 0.2);
        // 1 ALEPH * $0.5 * 1.2 * 1_000_000 = 600_000.
        assert_eq!(estimate.estimated_credits, Some(600_000.0));
    }

    #[tokio::test]
    async fn estimate_aleph_with_none_price_source_leaves_estimate_empty() {
        let one_aleph = U256::from(10u64).pow(U256::from(18u64));
        let estimate = estimate_credits(CreditToken::Aleph, one_aleph, &PriceSource::None)
            .await
            .unwrap();
        assert_eq!(estimate.price_usd, None);
        assert_eq!(estimate.estimated_credits, None);
    }

    #[test]
    fn mainnet_defaults_wire_up_known_constants() {
        let cfg = EthereumConfig::mainnet_defaults();
        assert_eq!(cfg.credit_contract, MAINNET_CREDIT_CONTRACT);
        assert_eq!(cfg.aleph_token, MAINNET_ALEPH_TOKEN_ADDRESS);
        assert_eq!(cfg.usdc_token, MAINNET_USDC_TOKEN_ADDRESS);
        assert_eq!(cfg.rpc_url, MAINNET_RPC_URL);
        assert_eq!(cfg.price_source, PriceSource::CoinGecko);
        assert_eq!(
            cfg.explorer_tx_base.as_deref(),
            Some(MAINNET_EXPLORER_TX_BASE)
        );
    }

    #[test]
    fn price_source_json_roundtrip_coingecko() {
        let s = serde_json::to_string(&PriceSource::CoinGecko).unwrap();
        assert_eq!(s, r#"{"type":"coin_gecko"}"#);
        let back: PriceSource = serde_json::from_str(&s).unwrap();
        assert_eq!(back, PriceSource::CoinGecko);
    }

    #[test]
    fn price_source_json_roundtrip_fixed() {
        let s = serde_json::to_string(&PriceSource::Fixed { usd: 0.25 }).unwrap();
        assert_eq!(s, r#"{"type":"fixed","usd":0.25}"#);
        let back: PriceSource = serde_json::from_str(&s).unwrap();
        assert_eq!(back, PriceSource::Fixed { usd: 0.25 });
    }

    #[test]
    fn price_source_json_roundtrip_none() {
        let s = serde_json::to_string(&PriceSource::None).unwrap();
        assert_eq!(s, r#"{"type":"none"}"#);
        let back: PriceSource = serde_json::from_str(&s).unwrap();
        assert_eq!(back, PriceSource::None);
    }

    #[test]
    fn ethereum_config_token_address_dispatch() {
        let cfg = EthereumConfig::mainnet_defaults();
        assert_eq!(cfg.token_address(CreditToken::Aleph), cfg.aleph_token);
        assert_eq!(cfg.token_address(CreditToken::Usdc), cfg.usdc_token);
    }

    #[test]
    fn price_source_from_str_accepts_cli_forms() {
        use std::str::FromStr;
        assert_eq!(
            PriceSource::from_str("coingecko").unwrap(),
            PriceSource::CoinGecko
        );
        assert_eq!(
            PriceSource::from_str("CoinGecko").unwrap(),
            PriceSource::CoinGecko
        );
        assert_eq!(PriceSource::from_str("none").unwrap(), PriceSource::None);
        assert_eq!(
            PriceSource::from_str("fixed:0.25").unwrap(),
            PriceSource::Fixed { usd: 0.25 }
        );
        assert_eq!(
            PriceSource::from_str("fixed:1").unwrap(),
            PriceSource::Fixed { usd: 1.0 }
        );
    }

    #[test]
    fn price_source_from_str_rejects_garbage() {
        use std::str::FromStr;
        assert!(PriceSource::from_str("chainlink").is_err());
        assert!(PriceSource::from_str("fixed:").is_err());
        assert!(PriceSource::from_str("fixed:abc").is_err());
        assert!(PriceSource::from_str("fixed:-1").is_err());
    }

    #[test]
    fn u256_to_f64_whole_18_decimals() {
        let amount = U256::from(100u64) * U256::from(10u64).pow(U256::from(18u64));
        assert_eq!(u256_to_f64(amount, 18), 100.0);
    }

    #[test]
    fn u256_to_f64_fractional_6_decimals() {
        assert_eq!(u256_to_f64(U256::from(50_500_000u64), 6), 50.5);
    }

    #[test]
    fn u256_to_f64_preserves_large_amount_better_than_string_parse() {
        // 1 billion ALEPH at 18 decimals — exact in both paths.
        let amount = U256::from(1_000_000_000u64) * U256::from(10u64).pow(U256::from(18u64));
        assert_eq!(u256_to_f64(amount, 18), 1_000_000_000.0);
    }

    #[test]
    fn u256_to_f64_one_wei() {
        // Smallest representable fraction at 18 decimals.
        assert!((u256_to_f64(U256::from(1u64), 18) - 1e-18).abs() < 1e-30);
    }

    #[test]
    fn format_18_decimal_whole() {
        let amount = U256::from(100u64) * U256::from(10u64).pow(U256::from(18u64));
        assert_eq!(format_token_amount(amount, 18), "100");
    }

    #[test]
    fn format_6_decimal_fractional() {
        let amount = U256::from(50_500_000u64);
        assert_eq!(format_token_amount(amount, 6), "50.5");
    }

    #[test]
    fn format_zero_remainder() {
        let amount = U256::from(1_000_000u64);
        assert_eq!(format_token_amount(amount, 6), "1");
    }
}
