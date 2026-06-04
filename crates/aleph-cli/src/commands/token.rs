use crate::account::CliAccount;
use crate::cli::{SigningArgs, SwapTokenCli, TokenCommand, TokenSwapArgs};
use crate::common::{resolve_account, resolve_address, resolve_network};
use aleph_sdk::credit::{format_token_amount, parse_token_amount};
use aleph_sdk::swap::SwapQuote;
use aleph_sdk::swap::SwapRequest;
use aleph_sdk::swap::SwapToken;
use aleph_sdk::swap::cow::CowApi;
use aleph_sdk::swap::cow::chains::cow_chain;
use aleph_sdk::swap::cow::ethflow::create_eth_order;
use aleph_sdk::swap::cow::{ensure_allowance, place_usdc_order, quote_eth, quote_usdc};
use aleph_types::account::EvmAccount;
use alloy_network::EthereumWallet;
use alloy_primitives::Address;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Result, anyhow};

/// Decimals for ALEPH token (ERC-20, 18 decimals like most Ethereum tokens).
const ALEPH_DECIMALS: u8 = 18;

impl From<SwapTokenCli> for SwapToken {
    fn from(v: SwapTokenCli) -> Self {
        match v {
            SwapTokenCli::Eth => SwapToken::Eth,
            SwapTokenCli::Usdc => SwapToken::Usdc,
        }
    }
}

pub async fn handle_token_command(
    json: bool,
    command: TokenCommand,
    cli_network: Option<&str>,
) -> Result<()> {
    match command {
        TokenCommand::Swap(args) => handle_swap(json, args, cli_network).await,
    }
}

async fn handle_swap(json: bool, args: TokenSwapArgs, cli_network: Option<&str>) -> Result<()> {
    // Validate slippage: 0..=50 percent.
    if !(0.0..=50.0).contains(&args.slippage) {
        return Err(anyhow!(
            "--slippage must be between 0 and 50 (percent); got {}",
            args.slippage
        ));
    }
    let slippage_frac = args.slippage / 100.0;

    let evm_account = resolve_swap_evm_account(&args.signing)?;
    let network = resolve_network(cli_network)?;
    let ethereum = network.ethereum.ok_or_else(|| {
        anyhow!(
            "network '{}' has no ethereum settlement config; \
             run: aleph config network set --network {} --rpc-url <URL> --credit-contract <ADDR> \
                  --aleph-token <ADDR> --usdc-token <ADDR> --price-source <coingecko|fixed:N|none>",
            network.name,
            network.name
        )
    })?;

    let rpc_url = args.rpc_url.as_deref().unwrap_or(&ethereum.rpc_url);

    let sell_token: SwapToken = args.sell_token.into();
    let sell_amount_raw = parse_token_amount(&args.amount, sell_token.decimals())
        .map_err(|e| anyhow!("invalid amount: {e}"))?;

    let signer = PrivateKeySigner::from_signing_key(evm_account.signing_key().clone());
    let owner: Address = signer.address();

    let receiver: Address = match &args.receiver {
        Some(r) => {
            let aleph_addr = resolve_address(r)?;
            aleph_addr
                .to_string()
                .parse::<Address>()
                .map_err(|e| anyhow!("invalid receiver address: {e}"))?
        }
        None => owner,
    };

    let url = rpc_url
        .parse()
        .map_err(|e| anyhow!("invalid RPC URL: {e}"))?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(url);
    let chain_id = provider
        .get_chain_id()
        .await
        .map_err(|e| anyhow!("failed to get chain ID: {e}"))?;

    let chain = cow_chain(chain_id).ok_or_else(|| {
        anyhow!(
            "CoW Swap is not available on chainId {} (network '{}')",
            chain_id,
            network.name
        )
    })?;

    let api = CowApi::new(chain_id).map_err(|e| anyhow!("{e}"))?;

    let req = SwapRequest {
        sell_token,
        sell_amount: sell_amount_raw,
        buy_token: ethereum.aleph_token,
        receiver,
        from: owner,
        slippage: slippage_frac,
        valid_for_secs: args.valid_for,
    };

    let (quote, resp) = match sell_token {
        SwapToken::Usdc => quote_usdc(&api, ethereum.usdc_token, &req)
            .await
            .map_err(|e| anyhow!("{e}"))?,
        SwapToken::Eth => quote_eth(&api, chain.weth, &req)
            .await
            .map_err(|e| anyhow!("{e}"))?,
    };

    // Print quote summary to stderr (human mode).
    if !json {
        let sell_display = format_token_amount(quote.sell_amount, sell_token.decimals());
        let buy_display = format_token_amount(quote.buy_amount, ALEPH_DECIMALS);
        let min_display = format_token_amount(quote.min_buy_amount, ALEPH_DECIMALS);
        let fee_display = format_token_amount(quote.fee_amount, sell_token.decimals());
        eprintln!(
            "Swapping {} {} for ALEPH via CoW Swap",
            sell_display,
            sell_token.symbol()
        );
        eprintln!("  Expected:     ~{buy_display} ALEPH");
        eprintln!("  Min received: {min_display} ALEPH");
        eprintln!(
            "  Fee:          {fee_display} {} (informational, taken from sell amount)",
            sell_token.symbol()
        );
    }

    // Dry-run: stop after printing the quote.
    if args.signing.dry_run {
        if json {
            let v = quote_json(&args.amount, sell_token, &quote);
            let mut output = v;
            output["dry_run"] = serde_json::Value::Bool(true);
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            eprintln!("\nDry run - order not submitted.");
        }
        return Ok(());
    }

    // Confirmation: only prompt in human mode (mirror credit.rs: no prompt when --json).
    if !json && !args.yes {
        eprintln!();
        let confirmed = dialoguer::Confirm::new()
            .with_prompt("Proceed?")
            .default(false)
            .interact()
            .map_err(|e| anyhow!("failed to read confirmation: {e}"))?;
        if !confirmed {
            eprintln!("Cancelled.");
            return Ok(());
        }
    }

    // Submit the order.
    let order_id = match sell_token {
        SwapToken::Usdc => {
            ensure_allowance(&provider, ethereum.usdc_token, owner, quote.sell_amount)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            place_usdc_order(
                &api,
                chain_id,
                ethereum.usdc_token,
                &req,
                &quote,
                &resp,
                &signer,
            )
            .await
            .map_err(|e| anyhow!("{e}"))?
        }
        SwapToken::Eth => {
            let tx_hash = create_eth_order(
                &provider,
                chain.ethflow,
                ethereum.aleph_token,
                receiver,
                quote.sell_amount,
                quote.min_buy_amount,
                resp.quote.valid_to,
                resp.id.unwrap_or(0),
            )
            .await
            .map_err(|e| anyhow!("{e}"))?;
            format!("{:#x}", tx_hash)
        }
    };

    // Output result.
    if json {
        let output = result_json(&args.amount, sell_token, &quote, &order_id);
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Order submitted: {order_id}");
        let explorer_url = match sell_token {
            SwapToken::Usdc => format!("https://explorer.cow.fi/orders/{order_id}"),
            SwapToken::Eth => format!("https://explorer.cow.fi/tx/{order_id}"),
        };
        eprintln!("{explorer_url}");
    }

    Ok(())
}

fn resolve_swap_evm_account(signing: &SigningArgs) -> Result<EvmAccount> {
    match resolve_account(&signing.identity)? {
        CliAccount::Evm(a) => Ok(a),
        CliAccount::LedgerEvm(_) => Err(anyhow!(
            "Ledger accounts are not yet supported for swaps. Use a local account."
        )),
        CliAccount::Sol(_) => Err(anyhow!("swaps require an EVM account (got Solana)")),
    }
}

fn quote_json(amount_display: &str, sell_token: SwapToken, quote: &SwapQuote) -> serde_json::Value {
    let buy_display = format_token_amount(quote.buy_amount, ALEPH_DECIMALS);
    let min_display = format_token_amount(quote.min_buy_amount, ALEPH_DECIMALS);
    serde_json::json!({
        "sell_token": sell_token.symbol(),
        "sell_amount": amount_display,
        "expected_aleph": buy_display,
        "min_aleph": min_display,
    })
}

fn result_json(
    amount_display: &str,
    sell_token: SwapToken,
    quote: &SwapQuote,
    order_id: &str,
) -> serde_json::Value {
    let buy_display = format_token_amount(quote.buy_amount, ALEPH_DECIMALS);
    let min_display = format_token_amount(quote.min_buy_amount, ALEPH_DECIMALS);
    serde_json::json!({
        "sell_token": sell_token.symbol(),
        "sell_amount": amount_display,
        "expected_aleph": buy_display,
        "min_aleph": min_display,
        "order_id": order_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;

    fn sample_quote() -> SwapQuote {
        SwapQuote {
            sell_amount: U256::from(50_000_000u64),
            buy_amount: U256::from(1_000_000_000_000_000_000u128), // 1e18
            min_buy_amount: U256::from(995_000_000_000_000_000u128), // 0.995e18
            fee_amount: U256::ZERO,
        }
    }

    #[test]
    fn swap_token_cli_maps_to_sdk_enum() {
        assert!(matches!(SwapToken::from(SwapTokenCli::Eth), SwapToken::Eth));
        assert!(matches!(
            SwapToken::from(SwapTokenCli::Usdc),
            SwapToken::Usdc
        ));
    }

    #[test]
    fn result_json_shape() {
        let v = result_json("50", SwapToken::Usdc, &sample_quote(), "0xUID");
        assert_eq!(v["sell_token"], "USDC");
        assert_eq!(v["sell_amount"], "50");
        assert_eq!(v["expected_aleph"], "1");
        assert_eq!(v["min_aleph"], "0.995");
        assert_eq!(v["order_id"], "0xUID");
    }
}
