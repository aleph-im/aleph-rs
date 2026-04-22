use crate::account::CliAccount;
use crate::cli::{BuyCreditArgs, CreditCommand, CreditTokenCli, SigningArgs};
use crate::common::resolve_account;
use aleph_sdk::credit::{self, CREDIT_CONTRACT, CreditEstimate, CreditToken, format_token_amount};
use aleph_types::account::EvmAccount;
use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionReceipt;
use alloy::signers::local::PrivateKeySigner;

impl From<CreditTokenCli> for CreditToken {
    fn from(v: CreditTokenCli) -> Self {
        match v {
            CreditTokenCli::Aleph => CreditToken::Aleph,
            CreditTokenCli::Usdc => CreditToken::Usdc,
        }
    }
}

pub async fn handle_credit_command(
    json: bool,
    command: CreditCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        CreditCommand::Buy(args) => handle_buy(json, args).await,
    }
}

async fn handle_buy(json: bool, args: BuyCreditArgs) -> Result<(), Box<dyn std::error::Error>> {
    let evm_account = resolve_evm_account(&args.signing)?;
    let token: CreditToken = args.token.into();
    let amount_raw = credit::parse_token_amount(&args.amount, token.decimals())
        .map_err(|e| format!("invalid amount: {e}"))?;

    let (provider, alloy_address) = build_signer_provider(evm_account, &args.rpc_url)?;
    let estimate = credit::estimate_credits(token, amount_raw).await?;

    // Dry-run must succeed even for under-funded accounts, so we defer the
    // balance check until we know we're actually submitting.
    if args.signing.dry_run {
        print_dry_run_summary(json, &args.amount, &estimate)?;
        return Ok(());
    }

    ensure_token_balance(&provider, alloy_address, token, amount_raw, &args.amount).await?;

    if !json {
        print_human_estimate(&args.amount, &estimate);
        if !confirm_submission()? {
            eprintln!("Cancelled.");
            return Ok(());
        }
    }

    let receipt = credit::buy_credits(&provider, token, amount_raw).await?;
    print_submission_result(json, &args.amount, &estimate, &receipt)?;
    Ok(())
}

fn resolve_evm_account(signing: &SigningArgs) -> Result<EvmAccount, Box<dyn std::error::Error>> {
    match resolve_account(signing)? {
        CliAccount::Evm(a) => Ok(a),
        CliAccount::LedgerEvm(_) => Err(
            "Ledger accounts are not supported for credit purchases. Use a local account.".into(),
        ),
        CliAccount::Sol(_) => Err("credit purchases require an EVM account (got Solana)".into()),
    }
}

fn build_signer_provider(
    evm_account: EvmAccount,
    rpc_url: &str,
) -> Result<(impl Provider, Address), Box<dyn std::error::Error>> {
    let wallet = PrivateKeySigner::from_signing_key(evm_account.signing_key().clone());
    let address = wallet.address();
    let url = rpc_url
        .parse()
        .map_err(|e| format!("invalid RPC URL: {e}"))?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(wallet))
        .connect_http(url);
    Ok((provider, address))
}

async fn ensure_token_balance(
    provider: &impl Provider,
    owner: Address,
    token: CreditToken,
    amount_raw: U256,
    amount_display: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let balance = credit::check_balance(provider, owner, token).await?;
    if balance < amount_raw {
        let have = format_token_amount(balance, token.decimals());
        return Err(format!(
            "insufficient {} balance: have {have}, need {amount_display}",
            token.symbol(),
        )
        .into());
    }
    Ok(())
}

fn confirm_submission() -> Result<bool, Box<dyn std::error::Error>> {
    eprintln!();
    dialoguer::Confirm::new()
        .with_prompt("Proceed?")
        .default(false)
        .interact()
        .map_err(|e| format!("failed to read confirmation: {e}").into())
}

/// JSON envelope shared by dry-run and post-submit output.
fn summary_json(amount_display: &str, estimate: &CreditEstimate) -> serde_json::Value {
    serde_json::json!({
        "token": estimate.token.symbol(),
        "amount": amount_display,
        "estimated_credits": estimate.estimated_credits,
        "price_usd": estimate.price_usd,
        "bonus_ratio": estimate.bonus_ratio,
        "recipient": format!("{}", CREDIT_CONTRACT),
    })
}

fn print_human_estimate(amount_display: &str, estimate: &CreditEstimate) {
    eprintln!(
        "Buying credits with {amount_display} {}",
        estimate.token.symbol()
    );
    match estimate.token {
        CreditToken::Aleph => eprintln!(
            "Estimated credits: ~{:.0} (at ${:.2}/{}, +{:.0}% bonus)",
            estimate.estimated_credits,
            estimate.price_usd,
            estimate.token.symbol(),
            estimate.bonus_ratio * 100.0,
        ),
        CreditToken::Usdc => {
            eprintln!("Estimated credits: ~{:.0}", estimate.estimated_credits)
        }
    }
    eprintln!("Recipient: {}", CREDIT_CONTRACT);
}

fn print_dry_run_summary(
    json: bool,
    amount_display: &str,
    estimate: &CreditEstimate,
) -> Result<(), Box<dyn std::error::Error>> {
    if json {
        let mut output = summary_json(amount_display, estimate);
        output["dry_run"] = serde_json::Value::Bool(true);
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_human_estimate(amount_display, estimate);
        eprintln!("\nDry run — transaction not submitted.");
    }
    Ok(())
}

fn print_submission_result(
    json: bool,
    amount_display: &str,
    estimate: &CreditEstimate,
    receipt: &TransactionReceipt,
) -> Result<(), Box<dyn std::error::Error>> {
    let tx_hash = receipt.transaction_hash;
    if json {
        let mut output = summary_json(amount_display, estimate);
        output["tx_hash"] = serde_json::Value::String(format!("{tx_hash}"));
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("\nTransaction submitted: {tx_hash}");
        eprintln!("https://etherscan.io/tx/{tx_hash}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credit_token_cli_maps_to_sdk_enum() {
        assert!(matches!(
            CreditToken::from(CreditTokenCli::Aleph),
            CreditToken::Aleph
        ));
        assert!(matches!(
            CreditToken::from(CreditTokenCli::Usdc),
            CreditToken::Usdc
        ));
    }

    fn sample_estimate(token: CreditToken) -> CreditEstimate {
        CreditEstimate {
            token,
            amount_raw: U256::from(100u64) * U256::from(10u64).pow(U256::from(token.decimals())),
            estimated_credits: 120_000_000.0,
            price_usd: 1.0,
            bonus_ratio: token.bonus_ratio(),
        }
    }

    #[test]
    fn summary_json_has_expected_shape() {
        let estimate = sample_estimate(CreditToken::Aleph);
        let v = summary_json("100", &estimate);

        assert_eq!(v["token"], "ALEPH");
        assert_eq!(v["amount"], "100");
        assert_eq!(v["estimated_credits"], 120_000_000.0);
        assert_eq!(v["price_usd"], 1.0);
        assert_eq!(v["bonus_ratio"], 0.2);
        assert_eq!(v["recipient"], format!("{CREDIT_CONTRACT}"));
        assert!(v.get("tx_hash").is_none(), "tx_hash only set after submit");
        assert!(v.get("dry_run").is_none(), "dry_run only set for dry-run");
    }

    #[test]
    fn summary_json_uses_usdc_symbol_for_usdc() {
        let estimate = sample_estimate(CreditToken::Usdc);
        let v = summary_json("50", &estimate);
        assert_eq!(v["token"], "USDC");
        assert_eq!(v["bonus_ratio"], 0.0);
    }
}
