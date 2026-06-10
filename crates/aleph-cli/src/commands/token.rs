use std::time::{SystemTime, UNIX_EPOCH};

use crate::account::CliAccount;
use crate::cli::{SigningArgs, SwapTokenCli, SwapVenueCli, TokenCommand, TokenSwapArgs};
use crate::common::{confirm_submission, resolve_account, resolve_address, resolve_network};
use aleph_sdk::credit::{EthereumConfig, format_token_amount, parse_token_amount};
use aleph_sdk::swap::cow::CowApi;
use aleph_sdk::swap::cow::chains::cow_chain;
use aleph_sdk::swap::cow::ethflow::create_eth_order;
use aleph_sdk::swap::cow::order::{AppData, VAULT_RELAYER};
use aleph_sdk::swap::uniswap::chains::uniswap_chain;
use aleph_sdk::swap::uniswap::router::execute_swap;
use aleph_sdk::swap::uniswap::{self, UniswapQuote};
use aleph_sdk::swap::{SwapQuote, SwapRequest, SwapToken, cow, ensure_allowance};
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

/// Validate that `percent` is in the range `0.0..=50.0` (inclusive) and is
/// not NaN. Returns the fractional form (`percent / 100`) on success.
fn validate_slippage(percent: f64) -> Result<f64> {
    if percent.is_nan() || !(0.0..=50.0).contains(&percent) {
        return Err(anyhow!(
            "--slippage must be between 0 and 50 (percent); got {}",
            percent
        ));
    }
    Ok(percent / 100.0)
}

fn build_swap_provider(
    evm_account: &EvmAccount,
    rpc_url: &str,
) -> Result<(impl Provider, Address, PrivateKeySigner)> {
    let signer = PrivateKeySigner::from_signing_key(evm_account.signing_key().clone());
    let address = signer.address();
    let url = rpc_url
        .parse()
        .map_err(|e| anyhow!("invalid RPC URL: {e}"))?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(url);
    Ok((provider, address, signer))
}

fn print_swap_quote(sell_token: SwapToken, quote: &SwapQuote) {
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

fn print_uniswap_quote(sell_token: SwapToken, quote: &UniswapQuote) {
    let sell_display = format_token_amount(quote.sell_amount, sell_token.decimals());
    let buy_display = format_token_amount(quote.buy_amount, ALEPH_DECIMALS);
    let min_display = format_token_amount(quote.min_buy_amount, ALEPH_DECIMALS);
    eprintln!(
        "Swapping {} {} for ALEPH via Uniswap",
        sell_display,
        sell_token.symbol()
    );
    eprintln!("  Expected:     ~{buy_display} ALEPH");
    eprintln!("  Min received: {min_display} ALEPH");
    eprintln!(
        "  Pool fee:     {} (taken in the pool price; gas paid separately)",
        quote.route.fee_display()
    );
}

async fn handle_swap(json: bool, args: TokenSwapArgs, cli_network: Option<&str>) -> Result<()> {
    let slippage_frac = validate_slippage(args.slippage)?;

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

    let (provider, owner, signer) = build_swap_provider(&evm_account, rpc_url)?;

    let receiver: Address = match &args.receiver {
        Some(r) => {
            let aleph_addr = resolve_address(r)?;
            aleph_addr
                .to_string()
                .parse::<Address>()
                .map_err(|e| anyhow!("invalid receiver address '{}': {e} (the receiver must be an EVM 0x... address)", r))?
        }
        None => owner,
    };

    let chain_id = provider
        .get_chain_id()
        .await
        .map_err(|e| anyhow!("failed to get chain ID: {e}"))?;

    let req = SwapRequest {
        sell_token,
        sell_amount: sell_amount_raw,
        buy_token: ethereum.aleph_token,
        receiver,
        from: owner,
        slippage: slippage_frac,
        valid_for_secs: args.valid_for,
    };

    match args.venue {
        SwapVenueCli::Cow => {
            swap_via_cow(
                json,
                &args,
                &network.name,
                &ethereum,
                chain_id,
                &provider,
                owner,
                receiver,
                &signer,
                &req,
            )
            .await
        }
        SwapVenueCli::Uniswap => {
            swap_via_uniswap(
                json,
                &args,
                &network.name,
                &ethereum,
                chain_id,
                &provider,
                owner,
                receiver,
                &req,
            )
            .await
        }
        SwapVenueCli::Ophis => {
            swap_via_ophis(
                json,
                &args,
                &network.name,
                &ethereum,
                chain_id,
                &provider,
                owner,
                receiver,
                &signer,
                &req,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn swap_via_cow(
    json: bool,
    args: &TokenSwapArgs,
    network_name: &str,
    ethereum: &EthereumConfig,
    chain_id: u64,
    provider: &impl Provider,
    owner: Address,
    receiver: Address,
    signer: &PrivateKeySigner,
    req: &SwapRequest,
) -> Result<()> {
    let sell_token = req.sell_token;

    let chain = cow_chain(chain_id).ok_or_else(|| {
        anyhow!(
            "CoW Swap is not available on chainId {} (network '{}')",
            chain_id,
            network_name
        )
    })?;

    let api = CowApi::new(chain_id).map_err(|e| anyhow!("failed to build CoW API client: {e}"))?;

    let (quote, resp) = match sell_token {
        SwapToken::Usdc => cow::quote_usdc(&api, ethereum.usdc_token, req)
            .await
            .map_err(|e| anyhow!("failed to fetch CoW quote: {e}"))?,
        SwapToken::Eth => cow::quote_eth(&api, chain.weth, req)
            .await
            .map_err(|e| anyhow!("failed to fetch CoW quote: {e}"))?,
    };

    // Print quote summary to stderr (human mode).
    if !json {
        print_swap_quote(sell_token, &quote);
    }

    // Dry-run: stop after printing the quote.
    if args.signing.dry_run {
        if json {
            let mut output = quote_json(sell_token, &quote);
            output["dry_run"] = serde_json::Value::Bool(true);
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            eprintln!("\nDry run - order not submitted.");
        }
        return Ok(());
    }

    // Confirmation: only prompt in human mode (mirror credit.rs: no prompt when --json).
    if !json && !args.yes && !confirm_submission("Proceed?")? {
        eprintln!("Cancelled.");
        return Ok(());
    }

    // Submit the order.
    match sell_token {
        SwapToken::Usdc => {
            ensure_allowance(
                provider,
                ethereum.usdc_token,
                owner,
                VAULT_RELAYER,
                quote.sell_amount,
            )
            .await
            .map_err(|e| anyhow!("failed to ensure USDC allowance: {e}"))?;
            let order_uid = cow::place_usdc_order(
                &api,
                chain_id,
                ethereum.usdc_token,
                &AppData::cow(),
                req,
                &quote,
                &resp,
                signer,
            )
            .await
            .map_err(|e| anyhow!("failed to place CoW order: {e}"))?;

            if json {
                let output = result_json_usdc(sell_token, &quote, &order_uid);
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else {
                eprintln!("Order submitted: {order_uid}");
                eprintln!("https://explorer.cow.fi/orders/{order_uid}");
            }
        }
        SwapToken::Eth => {
            let tx_hash = create_eth_order(
                provider,
                chain.ethflow,
                ethereum.aleph_token,
                receiver,
                AppData::cow().hash,
                quote.sell_amount,
                quote.min_buy_amount,
                resp.quote.valid_to,
                resp.id.unwrap_or(0),
            )
            .await
            .map_err(|e| anyhow!("failed to submit ETH-flow order: {e}"))?;
            let tx_hash_str = format!("{:#x}", tx_hash);

            if json {
                let output = result_json_eth(sell_token, &quote, &tx_hash_str);
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else {
                eprintln!("Transaction submitted: {tx_hash_str}");
                eprintln!("https://explorer.cow.fi/tx/{tx_hash_str}");
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn swap_via_uniswap(
    json: bool,
    args: &TokenSwapArgs,
    network_name: &str,
    ethereum: &EthereumConfig,
    chain_id: u64,
    provider: &impl Provider,
    owner: Address,
    receiver: Address,
    req: &SwapRequest,
) -> Result<()> {
    let sell_token = req.sell_token;

    let chain = uniswap_chain(chain_id).ok_or_else(|| {
        anyhow!(
            "Uniswap is not available on chainId {} (network '{}')",
            chain_id,
            network_name
        )
    })?;

    let quote = match sell_token {
        SwapToken::Eth => uniswap::quote_eth(provider, &chain, req)
            .await
            .map_err(|e| anyhow!("failed to fetch Uniswap quote: {e}"))?,
        SwapToken::Usdc => uniswap::quote_usdc(provider, &chain, ethereum.usdc_token, req)
            .await
            .map_err(|e| anyhow!("failed to fetch Uniswap quote: {e}"))?,
    };

    // Print quote summary to stderr (human mode).
    if !json {
        print_uniswap_quote(sell_token, &quote);
    }

    // Dry-run: stop after printing the quote.
    if args.signing.dry_run {
        if json {
            let mut output = quote_json_uniswap(sell_token, &quote);
            output["dry_run"] = serde_json::Value::Bool(true);
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            eprintln!("\nDry run - swap not submitted.");
        }
        return Ok(());
    }

    // Confirmation: only prompt in human mode (mirror credit.rs: no prompt when --json).
    if !json && !args.yes && !confirm_submission("Proceed?")? {
        eprintln!("Cancelled.");
        return Ok(());
    }

    let deadline_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow!("system clock is before the unix epoch: {e}"))?
        .as_secs()
        + u64::from(args.valid_for);

    // Native ETH rides along as msg.value; USDC needs a router allowance.
    let native_sell = match sell_token {
        SwapToken::Eth => true,
        SwapToken::Usdc => {
            ensure_allowance(
                provider,
                ethereum.usdc_token,
                owner,
                chain.swap_router02,
                quote.sell_amount,
            )
            .await
            .map_err(|e| anyhow!("failed to ensure USDC allowance: {e}"))?;
            false
        }
    };

    let tx_hash = execute_swap(
        provider,
        chain.swap_router02,
        &quote.route,
        quote.sell_amount,
        quote.min_buy_amount,
        receiver,
        deadline_secs,
        native_sell,
    )
    .await
    .map_err(|e| anyhow!("failed to submit Uniswap swap: {e}"))?;
    let tx_hash_str = format!("{:#x}", tx_hash);

    if json {
        let output = result_json_uniswap(sell_token, &quote, &tx_hash_str);
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Transaction submitted: {tx_hash_str}");
        if let Some(base) = &ethereum.explorer_tx_base {
            eprintln!("{base}{tx_hash_str}");
        }
    }

    Ok(())
}

fn print_ophis_quote(sell_token: SwapToken, quote: &SwapQuote) {
    let sell_display = format_token_amount(quote.sell_amount, sell_token.decimals());
    let buy_display = format_token_amount(quote.buy_amount, ALEPH_DECIMALS);
    let min_display = format_token_amount(quote.min_buy_amount, ALEPH_DECIMALS);
    eprintln!(
        "Swapping {} {} for ALEPH via Ophis",
        sell_display,
        sell_token.symbol()
    );
    eprintln!("  Expected:     ~{buy_display} ALEPH (before partner fee)");
    eprintln!("  Min received: {min_display} ALEPH");
    eprintln!("  Partner fee:  0.10% (Ophis), taken on settlement");
}

/// Swap via Ophis: a CoW order whose appData carries Ophis's partner fee.
/// Same mainnet orderbook, settlement and contracts as the `cow` venue; the
/// only difference is the appData document (and the native-ETH path must
/// register that document so the orderbook can resolve the fee).
#[allow(clippy::too_many_arguments)]
async fn swap_via_ophis(
    json: bool,
    args: &TokenSwapArgs,
    network_name: &str,
    ethereum: &EthereumConfig,
    chain_id: u64,
    provider: &impl Provider,
    owner: Address,
    receiver: Address,
    signer: &PrivateKeySigner,
    req: &SwapRequest,
) -> Result<()> {
    let sell_token = req.sell_token;
    let app_data = AppData::ophis();

    let chain = cow_chain(chain_id).ok_or_else(|| {
        anyhow!(
            "Ophis is not available on chainId {} (network '{}')",
            chain_id,
            network_name
        )
    })?;

    let api =
        CowApi::new(chain_id).map_err(|e| anyhow!("failed to build Ophis API client: {e}"))?;

    let (quote, resp) = match sell_token {
        SwapToken::Usdc => cow::quote_usdc(&api, ethereum.usdc_token, req).await,
        SwapToken::Eth => cow::quote_eth(&api, chain.weth, req).await,
    }
    .map_err(|e| anyhow!("failed to fetch Ophis quote: {e}"))?;

    // Print quote summary to stderr (human mode).
    if !json {
        print_ophis_quote(sell_token, &quote);
    }

    // Dry-run: stop after printing the quote.
    if args.signing.dry_run {
        if json {
            let mut output = quote_json_ophis(sell_token, &quote);
            output["dry_run"] = serde_json::Value::Bool(true);
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            eprintln!("\nDry run - order not submitted.");
        }
        return Ok(());
    }

    // Confirmation: only prompt in human mode (mirror credit.rs: no prompt when --json).
    if !json && !args.yes && !confirm_submission("Proceed?")? {
        eprintln!("Cancelled.");
        return Ok(());
    }

    match sell_token {
        SwapToken::Usdc => {
            ensure_allowance(
                provider,
                ethereum.usdc_token,
                owner,
                VAULT_RELAYER,
                quote.sell_amount,
            )
            .await
            .map_err(|e| anyhow!("failed to ensure USDC allowance: {e}"))?;
            let order_uid = cow::place_usdc_order(
                &api,
                chain_id,
                ethereum.usdc_token,
                &app_data,
                req,
                &quote,
                &resp,
                signer,
            )
            .await
            .map_err(|e| anyhow!("failed to place Ophis order: {e}"))?;

            if json {
                let output = result_json_ophis_usdc(sell_token, &quote, &order_uid);
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else {
                eprintln!("Order submitted: {order_uid}");
                eprintln!("https://explorer.cow.fi/orders/{order_uid}");
            }
        }
        SwapToken::Eth => {
            // EthFlow carries only the appData hash on-chain; register the full
            // document first so the orderbook can resolve the partner fee.
            let hash_hex = format!("{:#x}", app_data.hash);
            api.put_app_data(&hash_hex, app_data.json)
                .await
                .map_err(|e| anyhow!("failed to register Ophis appData: {e}"))?;
            let tx_hash = create_eth_order(
                provider,
                chain.ethflow,
                ethereum.aleph_token,
                receiver,
                app_data.hash,
                quote.sell_amount,
                quote.min_buy_amount,
                resp.quote.valid_to,
                resp.id.unwrap_or(0),
            )
            .await
            .map_err(|e| anyhow!("failed to submit Ophis ETH-flow order: {e}"))?;
            let tx_hash_str = format!("{:#x}", tx_hash);

            if json {
                let output = result_json_ophis_eth(sell_token, &quote, &tx_hash_str);
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else {
                eprintln!("Transaction submitted: {tx_hash_str}");
                eprintln!("https://explorer.cow.fi/tx/{tx_hash_str}");
            }
        }
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

/// Common quote fields shared by the dry-run and result JSON outputs.
fn quote_json(sell_token: SwapToken, quote: &SwapQuote) -> serde_json::Value {
    serde_json::json!({
        "venue": "cow",
        "sell_token": sell_token.symbol(),
        "sell_amount": format_token_amount(quote.sell_amount, sell_token.decimals()),
        "expected_aleph": format_token_amount(quote.buy_amount, ALEPH_DECIMALS),
        "min_aleph": format_token_amount(quote.min_buy_amount, ALEPH_DECIMALS),
        "fee": format_token_amount(quote.fee_amount, sell_token.decimals()),
    })
}

fn result_json_usdc(
    sell_token: SwapToken,
    quote: &SwapQuote,
    order_uid: &str,
) -> serde_json::Value {
    let mut v = quote_json(sell_token, quote);
    v["order_id"] = order_uid.into();
    v
}

fn result_json_eth(sell_token: SwapToken, quote: &SwapQuote, tx_hash: &str) -> serde_json::Value {
    let mut v = quote_json(sell_token, quote);
    v["tx_hash"] = tx_hash.into();
    v
}

/// Common quote fields for the Uniswap venue. No `fee` amount: the pool fee
/// is embedded in the price, so `pool_fees` names the tiers instead.
fn quote_json_uniswap(sell_token: SwapToken, quote: &UniswapQuote) -> serde_json::Value {
    serde_json::json!({
        "venue": "uniswap",
        "sell_token": sell_token.symbol(),
        "sell_amount": format_token_amount(quote.sell_amount, sell_token.decimals()),
        "expected_aleph": format_token_amount(quote.buy_amount, ALEPH_DECIMALS),
        "min_aleph": format_token_amount(quote.min_buy_amount, ALEPH_DECIMALS),
        "pool_fees": quote.route.fees_percent(),
    })
}

fn result_json_uniswap(
    sell_token: SwapToken,
    quote: &UniswapQuote,
    tx_hash: &str,
) -> serde_json::Value {
    let mut v = quote_json_uniswap(sell_token, quote);
    v["tx_hash"] = tx_hash.into();
    v
}

/// Common quote fields for the Ophis venue: the CoW fields plus the partner
/// fee taken on settlement (`partner_fee_bps`, 10 == 0.10%).
fn quote_json_ophis(sell_token: SwapToken, quote: &SwapQuote) -> serde_json::Value {
    serde_json::json!({
        "venue": "ophis",
        "sell_token": sell_token.symbol(),
        "sell_amount": format_token_amount(quote.sell_amount, sell_token.decimals()),
        "expected_aleph": format_token_amount(quote.buy_amount, ALEPH_DECIMALS),
        "min_aleph": format_token_amount(quote.min_buy_amount, ALEPH_DECIMALS),
        "fee": format_token_amount(quote.fee_amount, sell_token.decimals()),
        "partner_fee_bps": 10,
    })
}

fn result_json_ophis_usdc(
    sell_token: SwapToken,
    quote: &SwapQuote,
    order_uid: &str,
) -> serde_json::Value {
    let mut v = quote_json_ophis(sell_token, quote);
    v["order_id"] = order_uid.into();
    v
}

fn result_json_ophis_eth(
    sell_token: SwapToken,
    quote: &SwapQuote,
    tx_hash: &str,
) -> serde_json::Value {
    let mut v = quote_json_ophis(sell_token, quote);
    v["tx_hash"] = tx_hash.into();
    v
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
            fee_amount: U256::from(100_000u64),                    // 0.1 USDC (6 dec)
        }
    }

    fn sample_uniswap_quote() -> UniswapQuote {
        use aleph_sdk::swap::uniswap::route::{FEE_1_PERCENT, FEE_005_PERCENT, UniswapRoute};
        use alloy_primitives::address;
        UniswapQuote {
            sell_amount: U256::from(50_000_000u64),
            buy_amount: U256::from(1_000_000_000_000_000_000u128), // 1e18
            min_buy_amount: U256::from(995_000_000_000_000_000u128), // 0.995e18
            route: UniswapRoute::new(
                vec![
                    address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"), // USDC
                    address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), // WETH
                    address!("27702a26126e0B3702af63Ee09aC4d1A084EF628"), // ALEPH
                ],
                vec![FEE_005_PERCENT, FEE_1_PERCENT],
            ),
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

    // --- validate_slippage ---

    #[test]
    fn validate_slippage_accepts_zero() {
        let frac = validate_slippage(0.0).unwrap();
        assert_eq!(frac, 0.0);
    }

    #[test]
    fn validate_slippage_accepts_midrange() {
        let frac = validate_slippage(0.5).unwrap();
        assert!((frac - 0.005).abs() < 1e-12);
    }

    #[test]
    fn validate_slippage_accepts_fifty() {
        let frac = validate_slippage(50.0).unwrap();
        assert!((frac - 0.5).abs() < 1e-12);
    }

    #[test]
    fn validate_slippage_rejects_negative() {
        assert!(validate_slippage(-0.1).is_err());
    }

    #[test]
    fn validate_slippage_rejects_above_fifty() {
        assert!(validate_slippage(50.1).is_err());
    }

    #[test]
    fn validate_slippage_rejects_nan() {
        assert!(validate_slippage(f64::NAN).is_err());
    }

    // --- result_json_usdc ---

    #[test]
    fn result_json_usdc_shape() {
        let v = result_json_usdc(SwapToken::Usdc, &sample_quote(), "0xUID");
        assert_eq!(v["venue"], "cow");
        assert_eq!(v["sell_token"], "USDC");
        // sell_amount: 50_000_000 atoms / 10^6 = 50 USDC
        assert_eq!(v["sell_amount"], "50");
        assert_eq!(v["expected_aleph"], "1");
        assert_eq!(v["min_aleph"], "0.995");
        assert_eq!(v["order_id"], "0xUID");
        // fee: 100_000 / 10^6 = 0.1 USDC
        assert_eq!(v["fee"], "0.1");
        assert!(
            v.get("tx_hash").is_none(),
            "USDC result must not have tx_hash"
        );
    }

    // --- result_json_eth ---

    #[test]
    fn result_json_eth_shape() {
        let v = result_json_eth(SwapToken::Eth, &sample_quote(), "0xdeadbeef");
        assert_eq!(v["venue"], "cow");
        assert_eq!(v["sell_token"], "ETH");
        // sell_amount: 50_000_000 atoms / 10^18 = 0.00000000005 ETH
        assert_eq!(v["sell_amount"], "0.00000000005");
        assert_eq!(v["tx_hash"], "0xdeadbeef");
        assert!(
            v.get("order_id").is_none(),
            "ETH result must not have order_id"
        );
    }

    // --- quote_json ---

    #[test]
    fn quote_json_has_fee_and_no_order_id() {
        let v = quote_json(SwapToken::Usdc, &sample_quote());
        assert_eq!(v["venue"], "cow");
        assert_eq!(v["sell_token"], "USDC");
        // sell_amount: 50_000_000 atoms / 10^6 = 50 USDC
        assert_eq!(v["sell_amount"], "50");
        assert_eq!(v["expected_aleph"], "1");
        assert_eq!(v["min_aleph"], "0.995");
        assert_eq!(v["fee"], "0.1");
        assert!(v.get("order_id").is_none());
        assert!(v.get("tx_hash").is_none());
        assert!(v.get("dry_run").is_none(), "dry_run only set by caller");
    }

    #[test]
    fn quote_json_dry_run_flag_set_by_caller() {
        let mut v = quote_json(SwapToken::Usdc, &sample_quote());
        v["dry_run"] = serde_json::Value::Bool(true);
        assert_eq!(v["dry_run"], true);
    }

    // --- Uniswap JSON helpers ---

    #[test]
    fn quote_json_uniswap_shape() {
        let v = quote_json_uniswap(SwapToken::Usdc, &sample_uniswap_quote());
        assert_eq!(v["venue"], "uniswap");
        assert_eq!(v["sell_token"], "USDC");
        assert_eq!(v["sell_amount"], "50");
        assert_eq!(v["expected_aleph"], "1");
        assert_eq!(v["min_aleph"], "0.995");
        assert_eq!(v["pool_fees"], serde_json::json!(["0.05%", "1%"]));
        assert!(v.get("fee").is_none(), "uniswap has no separate fee amount");
        assert!(v.get("tx_hash").is_none());
        assert!(v.get("order_id").is_none());
    }

    #[test]
    fn result_json_uniswap_shape() {
        let v = result_json_uniswap(SwapToken::Usdc, &sample_uniswap_quote(), "0xdeadbeef");
        assert_eq!(v["venue"], "uniswap");
        assert_eq!(v["tx_hash"], "0xdeadbeef");
        assert!(v.get("order_id").is_none());
    }

    #[test]
    fn result_json_uniswap_snapshot() {
        insta::assert_json_snapshot!(result_json_uniswap(
            SwapToken::Usdc,
            &sample_uniswap_quote(),
            "0xfeed"
        ));
    }

    #[test]
    fn quote_json_uniswap_snapshot() {
        insta::assert_json_snapshot!(quote_json_uniswap(SwapToken::Usdc, &sample_uniswap_quote()));
    }

    // --- Snapshot tests ---

    #[test]
    fn result_json_usdc_snapshot() {
        insta::assert_json_snapshot!(result_json_usdc(SwapToken::Usdc, &sample_quote(), "0xUID"));
    }

    #[test]
    fn result_json_eth_snapshot() {
        insta::assert_json_snapshot!(result_json_eth(SwapToken::Eth, &sample_quote(), "0xfeed"));
    }

    #[test]
    fn quote_json_snapshot() {
        insta::assert_json_snapshot!(quote_json(SwapToken::Eth, &sample_quote()));
    }

    // --- Ophis JSON helpers ---

    #[test]
    fn quote_json_ophis_has_partner_fee() {
        let v = quote_json_ophis(SwapToken::Usdc, &sample_quote());
        assert_eq!(v["venue"], "ophis");
        assert_eq!(v["sell_token"], "USDC");
        assert_eq!(v["sell_amount"], "50");
        assert_eq!(v["expected_aleph"], "1");
        assert_eq!(v["min_aleph"], "0.995");
        assert_eq!(v["fee"], "0.1");
        assert_eq!(v["partner_fee_bps"], 10);
        assert!(v.get("order_id").is_none());
        assert!(v.get("tx_hash").is_none());
    }

    #[test]
    fn result_json_ophis_usdc_has_order_id() {
        let v = result_json_ophis_usdc(SwapToken::Usdc, &sample_quote(), "0xUID");
        assert_eq!(v["venue"], "ophis");
        assert_eq!(v["order_id"], "0xUID");
        assert_eq!(v["partner_fee_bps"], 10);
        assert!(v.get("tx_hash").is_none());
    }

    #[test]
    fn result_json_ophis_eth_has_tx_hash() {
        let v = result_json_ophis_eth(SwapToken::Eth, &sample_quote(), "0xdeadbeef");
        assert_eq!(v["venue"], "ophis");
        assert_eq!(v["tx_hash"], "0xdeadbeef");
        assert!(v.get("order_id").is_none());
    }

    #[test]
    fn quote_json_ophis_snapshot() {
        insta::assert_json_snapshot!(quote_json_ophis(SwapToken::Usdc, &sample_quote()));
    }

    #[test]
    fn result_json_ophis_eth_snapshot() {
        insta::assert_json_snapshot!(result_json_ophis_eth(
            SwapToken::Eth,
            &sample_quote(),
            "0xfeed"
        ));
    }
}
