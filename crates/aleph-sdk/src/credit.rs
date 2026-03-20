use alloy::primitives::{Address, U256, address};
use alloy::providers::Provider;
use alloy::sol;
use serde::Serialize;

/// Token types accepted for credit purchase.
#[derive(Debug, Clone, Copy, Serialize)]
pub enum CreditToken {
    Aleph,
    Usdc,
}

impl CreditToken {
    /// ERC20 contract address on Ethereum mainnet.
    pub fn contract_address(&self) -> Address {
        match self {
            CreditToken::Aleph => address!("27702a26126e0B3702af63Ee09aC4d1A084EF628"),
            CreditToken::Usdc => address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
        }
    }

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

/// Credit contract address on Ethereum mainnet.
pub const CREDIT_CONTRACT: Address = address!("6b55F32Ea969910838defd03746Ced5E2AE8cB8B");

/// Default Ethereum RPC endpoint.
pub const DEFAULT_RPC_URL: &str = "https://eth.llamarpc.com";

/// Result of a credit estimate.
pub struct CreditEstimate {
    pub token: CreditToken,
    pub amount_raw: U256,
    pub amount_display: String,
    pub estimated_credits: f64,
    pub price_usd: f64,
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

/// Fetch the current ALEPH/USD price from CoinGecko.
async fn fetch_aleph_price_usd() -> Result<f64, String> {
    let resp = reqwest::get(COINGECKO_PRICE_URL)
        .await
        .map_err(|e| format!("failed to fetch ALEPH price: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!(
            "CoinGecko API returned HTTP {}: try again later",
            resp.status()
        ));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse CoinGecko response: {e}"))?;

    body["aleph"]["usd"]
        .as_f64()
        .ok_or_else(|| "unexpected CoinGecko response format".to_string())
}

/// Estimate how many credits will be received for a given token amount.
///
/// For USDC: 1 USDC = 1,000,000 credits (1:1, price_usd = 1.0).
/// For ALEPH: credits = amount * price_usd * (1 + 0.2 bonus).
pub async fn estimate_credits(
    token: CreditToken,
    amount_raw: U256,
    amount_display: &str,
) -> Result<CreditEstimate, String> {
    let price_usd = match token {
        CreditToken::Usdc => 1.0,
        CreditToken::Aleph => fetch_aleph_price_usd().await?,
    };

    let amount_f64: f64 = amount_display
        .parse()
        .map_err(|e| format!("invalid amount for estimation: {e}"))?;

    let bonus = token.bonus_ratio();
    let estimated_credits = amount_f64 * price_usd * (1.0 + bonus) * 1_000_000.0;

    Ok(CreditEstimate {
        token,
        amount_raw,
        amount_display: amount_display.to_string(),
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
) -> Result<U256, String> {
    let contract = IERC20::new(token.contract_address(), provider);
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
    token: CreditToken,
    amount_raw: U256,
) -> Result<alloy::rpc::types::TransactionReceipt, String> {
    let contract = IERC20::new(token.contract_address(), provider);
    let tx = contract.transfer(CREDIT_CONTRACT, amount_raw);
    let pending = tx
        .send()
        .await
        .map_err(|e| format!("failed to send transaction: {e}"))?;
    let receipt = pending
        .get_receipt()
        .await
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
        let estimate = estimate_credits(CreditToken::Usdc, U256::from(100_000_000u64), "100")
            .await
            .unwrap();

        assert_eq!(estimate.price_usd, 1.0);
        assert_eq!(estimate.bonus_ratio, 0.0);
        assert_eq!(estimate.estimated_credits, 100_000_000.0);
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
