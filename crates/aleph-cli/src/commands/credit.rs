use crate::cli::{BuyCreditArgs, CreditCommand, CreditTokenCli};
use aleph_sdk::credit::{self, CREDIT_CONTRACT, CreditToken, format_token_amount};
use aleph_types::account::{Account, EvmAccount};
use aleph_types::chain::Chain;

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
    // 1. Resolve account — must be EVM
    let key_hex =
        crate::account::resolve_key_hex(args.private_key.as_deref(), args.account.as_deref())?;
    let key_bytes =
        hex::decode(key_hex.as_str()).map_err(|e| format!("invalid hex in private key: {e}"))?;

    let evm_account = EvmAccount::new(Chain::Ethereum, &key_bytes)
        .map_err(|e| format!("failed to create EVM account: {e}"))?;
    let address = evm_account.address().to_string();

    // 2. Parse amount
    let token: CreditToken = args.token.into();
    let amount_raw = credit::parse_token_amount(&args.amount, token.decimals())
        .map_err(|e| format!("invalid amount: {e}"))?;

    // 3. Build alloy provider with signer
    let signing_key = k256::ecdsa::SigningKey::from_slice(&key_bytes)
        .map_err(|e| format!("invalid signing key: {e}"))?;
    let alloy_wallet = alloy::signers::local::PrivateKeySigner::from_signing_key(signing_key);
    let provider = alloy::providers::ProviderBuilder::new()
        .wallet(alloy::network::EthereumWallet::from(alloy_wallet))
        .connect_http(
            args.rpc_url
                .parse()
                .map_err(|e| format!("invalid RPC URL: {e}"))?,
        );

    // Parse the address for alloy
    let alloy_address: alloy::primitives::Address = address
        .parse()
        .map_err(|e| format!("invalid address: {e}"))?;

    // 4. Check token balance
    let balance = credit::check_balance(&provider, alloy_address, token).await?;
    if balance < amount_raw {
        let have = format_token_amount(balance, token.decimals());
        return Err(format!(
            "insufficient {} balance: have {}, need {}",
            token.symbol(),
            have,
            args.amount
        )
        .into());
    }

    // 5. Check ETH balance (warn if low)
    let eth_balance = credit::check_eth_balance(&provider, alloy_address).await?;
    let min_gas = alloy::primitives::U256::from(100_000u64)
        * alloy::primitives::U256::from(50_000_000_000u64); // ~100k gas * 50 gwei
    if eth_balance < min_gas {
        let eth_display = format_token_amount(eth_balance, 18);
        if json {
            return Err(format!("insufficient ETH for gas: have {} ETH", eth_display,).into());
        }
        eprintln!(
            "Warning: low ETH balance ({} ETH) — transaction may fail due to insufficient gas",
            eth_display,
        );
    }

    // 6. Estimate credits
    let estimate = credit::estimate_credits(token, amount_raw, &args.amount).await?;

    // 7-8. Display summary / dry-run
    if json {
        let mut output = serde_json::json!({
            "token": token.symbol(),
            "amount": args.amount,
            "estimated_credits": estimate.estimated_credits,
            "price_usd": estimate.price_usd,
            "bonus_ratio": estimate.bonus_ratio,
            "recipient": format!("{}", CREDIT_CONTRACT),
        });
        if args.dry_run {
            println!("{}", serde_json::to_string_pretty(&output)?);
            return Ok(());
        }
        // 10. Submit transaction (skip prompt in JSON mode)
        let receipt = credit::buy_credits(&provider, token, amount_raw).await?;
        output["tx_hash"] = serde_json::Value::String(format!("{}", receipt.transaction_hash));
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Buying credits with {} {}", args.amount, token.symbol());
        if matches!(token, CreditToken::Aleph) {
            eprintln!(
                "Estimated credits: ~{:.0} (at ${:.2}/{}, +{:.0}% bonus)",
                estimate.estimated_credits,
                estimate.price_usd,
                token.symbol(),
                estimate.bonus_ratio * 100.0,
            );
        } else {
            eprintln!("Estimated credits: ~{:.0}", estimate.estimated_credits,);
        }
        eprintln!("Recipient: {}", CREDIT_CONTRACT);

        if args.dry_run {
            eprintln!("\nDry run — transaction not submitted.");
            return Ok(());
        }

        // 9. Prompt for confirmation
        eprintln!();
        let confirm = dialoguer::Confirm::new()
            .with_prompt("Proceed?")
            .default(false)
            .interact()
            .map_err(|e| format!("failed to read confirmation: {e}"))?;

        if !confirm {
            eprintln!("Cancelled.");
            return Ok(());
        }

        // 10. Submit transaction
        let receipt = credit::buy_credits(&provider, token, amount_raw).await?;
        let tx_hash = receipt.transaction_hash;
        eprintln!("\nTransaction submitted: {tx_hash}");
        eprintln!("https://etherscan.io/tx/{tx_hash}");
    }

    Ok(())
}
