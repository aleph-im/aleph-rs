use crate::account::CliAccount;
use crate::account::store::AccountStore;
use crate::cli::{
    BuyCreditArgs, CreditCommand, CreditFilterArgs, CreditHistoryArgs, CreditSummaryArgs,
    CreditTokenCli, SigningArgs, TransferCreditArgs,
};
use crate::common::{
    confirm_submission, format_address, resolve_account, resolve_address, resolve_network,
    submit_or_preview,
};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{
    AlephAccountClient, AlephClient, AlephMessageClient, CreditDirection, CreditHistoryFilters,
    MessageWithStatus,
};
use aleph_sdk::credit::{self, CreditEstimate, CreditToken, EthereumConfig, format_token_amount};
use aleph_sdk::credit_transfer::{
    CREDIT_TRANSFER_POST_TYPE, CreditTransferContent, CreditTransferEntry, CreditTransferError,
    CreditTransferList,
};
use aleph_types::account::{Account, EvmAccount};
use aleph_types::chain::Address as AlephAddress;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::MessageType;
use alloy_network::EthereumWallet;
use alloy_primitives::{Address, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::TransactionReceipt;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Utc};
use url::Url;

impl From<CreditTokenCli> for CreditToken {
    fn from(v: CreditTokenCli) -> Self {
        match v {
            CreditTokenCli::Aleph => CreditToken::Aleph,
            CreditTokenCli::Usdc => CreditToken::Usdc,
            CreditTokenCli::Eth => CreditToken::Eth,
        }
    }
}

pub async fn handle_credit_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: CreditCommand,
    cli_network: Option<&str>,
) -> Result<()> {
    match command {
        CreditCommand::Buy(args) => handle_buy(json, args, cli_network).await,
        CreditCommand::Transfer(args) => handle_transfer(aleph_client, ccn_url, json, args).await,
        CreditCommand::History(args) => handle_history(aleph_client, json, args).await,
        CreditCommand::Summary(args) => handle_summary(aleph_client, json, args).await,
    }
}

/// Translate the shared CLI filter flags into the SDK's
/// [`CreditHistoryFilters`].
///
/// `--since` is resolved against the current time here (now - duration) so the
/// lower bound reflects invocation time. `--expenses`/`--top-ups` map to the
/// outgoing/incoming directions; clap guarantees they are not both set.
fn build_filters(filters: &CreditFilterArgs) -> Result<CreditHistoryFilters> {
    let start_date = match filters.since {
        Some(window) => Some((Utc::now() - window).timestamp()),
        None => filters.start.map(|dt| dt.timestamp()),
    };

    let direction = if filters.expenses {
        Some(CreditDirection::Outgoing)
    } else if filters.top_ups {
        Some(CreditDirection::Incoming)
    } else {
        None
    };

    let sdk_filters = CreditHistoryFilters {
        start_date,
        end_date: filters.end.map(|dt| dt.timestamp()),
        direction,
        resource_types: filters
            .resource_type
            .iter()
            .copied()
            .map(Into::into)
            .collect(),
        resource: filters.resource.as_ref().map(ToString::to_string),
    };
    Ok(sdk_filters)
}

async fn handle_history(
    aleph_client: &AlephClient,
    json: bool,
    args: CreditHistoryArgs,
) -> Result<()> {
    let address = resolve_owner_address(aleph_client, &args.filters).await?;
    let filters = build_filters(&args.filters)?;
    let history = aleph_client
        .get_credit_history(&address, args.page, Some(args.page_size), &filters)
        .await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&history)?);
        return Ok(());
    }

    if history.credit_history.is_empty() {
        eprintln!("No credit history for {address}");
        return Ok(());
    }

    eprintln!(
        "Credit history for {} (page {} of {}, {} per page, {} total)",
        history.address,
        history.pagination_page,
        total_pages(history.pagination_total, history.pagination_per_page),
        history.pagination_per_page,
        history.pagination_total,
    );
    eprintln!(
        "{:<19}  {:>15}  {:<15}  {:<20}  {:<20}  {:<10}",
        "Timestamp", "Amount", "Method", "Origin", "Origin ref", "Expires",
    );
    for item in &history.credit_history {
        let expires = item
            .expiration_date
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "Never".to_string());
        eprintln!(
            "{:<19}  {:>15}  {:<15}  {:<20}  {:<20}  {:<10}",
            item.message_timestamp
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            item.amount,
            display_optional(&item.payment_method, 15),
            display_optional(&item.origin, 20),
            display_optional(&item.origin_ref, 20),
            expires,
        );
    }
    Ok(())
}

async fn handle_summary(
    aleph_client: &AlephClient,
    json: bool,
    args: CreditSummaryArgs,
) -> Result<()> {
    let address = resolve_owner_address(aleph_client, &args.filters).await?;
    let filters = build_filters(&args.filters)?;
    let summary = aleph_client
        .get_credit_history_summary(&address, &filters)
        .await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
        return Ok(());
    }

    for line in summary_lines(&summary, args.filters.resource.is_some()) {
        eprintln!("{line}");
    }
    Ok(())
}

/// Render the human-readable summary lines.
///
/// When the totals are scoped to a single resource (`--resource`), every entry
/// is necessarily an expense, so the `Incoming` (top-ups) line is always zero
/// and only adds noise — drop it.
fn summary_lines(
    summary: &aleph_sdk::client::CreditHistorySummary,
    resource_filtered: bool,
) -> Vec<String> {
    let mut lines = vec![
        format!("Credit summary for {}", summary.address),
        format!("  Entries:   {}", summary.entry_count),
        format!("  Net:       {:+}", summary.total_amount),
    ];
    if !resource_filtered {
        lines.push(format!("  Incoming:  {:+}", summary.total_incoming));
    }
    lines.push(format!("  Outgoing:  {:+}", summary.total_outgoing));
    lines
}

/// Resolve the owner address whose credit ledger to query.
///
/// Precedence:
/// 1. An explicit `--address` (raw or local-name) always wins.
/// 2. Otherwise, if `--resource` is set, derive the owner from the resource's
///    own message: its `content.address` is the payer, i.e. exactly the ledger
///    charged for it. This lets `--resource <hash>` alone identify the ledger,
///    so inspecting another owner's resource needs only its hash.
/// 3. Otherwise fall back to the default account from the local store (mirrors
///    `aleph account balance` / `aleph aggregate list`).
async fn resolve_owner_address(
    aleph_client: &AlephClient,
    filters: &CreditFilterArgs,
) -> Result<AlephAddress> {
    if let Some(value) = filters.address.as_deref() {
        return resolve_address(value);
    }
    if let Some(resource) = &filters.resource {
        return owner_of_resource(aleph_client, resource).await;
    }
    let store = AccountStore::open().map_err(|e| anyhow!("failed to open account store: {e}"))?;
    let name = store.default_account_name()?.ok_or_else(|| {
        anyhow!(
            "no --address provided and no default account set; \
             pass --address or set a default with: aleph account use <NAME>"
        )
    })?;
    let entry = store.get_account(&name)?;
    Ok(AlephAddress::from(entry.address))
}

/// Look up the owner (payer) of a resource by fetching its message and reading
/// `content.address`. Backs the `--resource`-implies-owner shortcut above.
async fn owner_of_resource(
    aleph_client: &AlephClient,
    resource: &ItemHash,
) -> Result<AlephAddress> {
    let with_status = aleph_client
        .get_message(resource)
        .await
        .map_err(|e| anyhow!("failed to look up resource {resource}: {e}"))?;
    let message = match with_status {
        MessageWithStatus::Processed { message }
        | MessageWithStatus::Removing { message, .. }
        | MessageWithStatus::Removed { message, .. } => message,
        other => bail!(
            "could not determine the owner of resource {resource} (status: {}); \
             pass --address explicitly",
            other.status()
        ),
    };
    Ok(message.owner().clone())
}

fn total_pages(total: u64, per_page: u32) -> u64 {
    if per_page == 0 {
        return 1;
    }
    let per = u64::from(per_page);
    total.div_ceil(per).max(1)
}

fn display_optional(value: &Option<String>, width: usize) -> String {
    match value {
        Some(s) if !s.is_empty() => truncate(s, width),
        _ => "-".to_string(),
    }
}

fn truncate(s: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if width == 1 {
        return "…".to_string();
    }
    if s.chars().count() <= width {
        return s.to_string();
    }
    let head: String = s.chars().take(width - 1).collect();
    format!("{head}…")
}

#[cfg(test)]
mod history_tests {
    use super::*;
    use crate::cli::ResourceTypeCli;

    #[test]
    fn display_optional_shows_dash_for_none() {
        assert_eq!(display_optional(&None, 10), "-");
        assert_eq!(display_optional(&Some(String::new()), 10), "-");
    }

    #[test]
    fn display_optional_truncates_long_values() {
        let v = Some("0123456789ABCDEF".to_string());
        let rendered = display_optional(&v, 10);
        assert_eq!(rendered.chars().count(), 10);
        assert!(rendered.ends_with('…'));
    }

    #[test]
    fn total_pages_rounds_up() {
        assert_eq!(total_pages(0, 100), 1);
        assert_eq!(total_pages(1, 100), 1);
        assert_eq!(total_pages(100, 100), 1);
        assert_eq!(total_pages(101, 100), 2);
        assert_eq!(total_pages(250, 100), 3);
    }

    #[test]
    fn total_pages_handles_zero_per_page() {
        // Server should always set this, but guard against a divide-by-zero
        // if an empty/odd response slips through.
        assert_eq!(total_pages(0, 0), 1);
        assert_eq!(total_pages(123, 0), 1);
    }

    #[test]
    fn truncate_zero_width_returns_empty() {
        assert_eq!(truncate("hello", 0), "");
    }

    #[test]
    fn truncate_one_width_returns_ellipsis() {
        assert_eq!(truncate("hello", 1), "…");
        // Even when the input is itself one char, the indicator wins so
        // callers know the value was non-empty.
        assert_eq!(truncate("a", 1), "…");
    }

    #[test]
    fn truncate_keeps_short_strings_intact() {
        assert_eq!(truncate("hi", 5), "hi");
        assert_eq!(truncate("12345", 5), "12345");
    }

    fn filter_args(address: &str) -> CreditFilterArgs {
        CreditFilterArgs {
            address: Some(address.to_string()),
            since: None,
            start: None,
            end: None,
            expenses: false,
            top_ups: false,
            resource_type: Vec::new(),
            resource: None,
        }
    }

    const SAMPLE_ADDRESS: &str = "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10";
    const SAMPLE_HASH: &str = "3c5b05761c8f94a7b8fe6d0d43e5fb91f9689c53c078a870e5e300c7da8a1878";

    #[test]
    fn build_filters_maps_expenses_to_outgoing() {
        let mut args = filter_args(SAMPLE_ADDRESS);
        args.expenses = true;
        let filters = build_filters(&args).unwrap();
        assert!(matches!(filters.direction, Some(CreditDirection::Outgoing)));
    }

    #[test]
    fn build_filters_maps_top_ups_to_incoming() {
        let mut args = filter_args(SAMPLE_ADDRESS);
        args.top_ups = true;
        let filters = build_filters(&args).unwrap();
        assert!(matches!(filters.direction, Some(CreditDirection::Incoming)));
    }

    #[test]
    fn build_filters_no_direction_by_default() {
        let filters = build_filters(&filter_args(SAMPLE_ADDRESS)).unwrap();
        assert!(filters.direction.is_none());
    }

    #[test]
    fn build_filters_translates_explicit_bounds_and_resources() {
        let mut args = filter_args(SAMPLE_ADDRESS);
        args.start = Some(DateTime::from_timestamp(1_769_990_400, 0).unwrap());
        args.end = Some(DateTime::from_timestamp(1_770_000_000, 0).unwrap());
        args.resource_type = vec![ResourceTypeCli::Store, ResourceTypeCli::Instance];
        let filters = build_filters(&args).unwrap();
        assert_eq!(filters.start_date, Some(1_769_990_400));
        assert_eq!(filters.end_date, Some(1_770_000_000));
        assert_eq!(
            filters.resource_types,
            vec![MessageType::Store, MessageType::Instance]
        );
    }

    fn sample_summary() -> aleph_sdk::client::CreditHistorySummary {
        aleph_sdk::client::CreditHistorySummary {
            address: SAMPLE_ADDRESS.to_string(),
            entry_count: 3,
            total_amount: -900,
            total_incoming: 0,
            total_outgoing: -900,
        }
    }

    #[test]
    fn summary_lines_include_incoming_without_resource_filter() {
        let lines = summary_lines(&sample_summary(), false);
        assert!(
            lines.iter().any(|l| l.contains("Incoming")),
            "expected an Incoming line, got: {lines:?}"
        );
        assert!(lines.iter().any(|l| l.contains("Outgoing")));
    }

    #[test]
    fn summary_lines_omit_incoming_with_resource_filter() {
        let lines = summary_lines(&sample_summary(), true);
        assert!(
            !lines.iter().any(|l| l.contains("Incoming")),
            "Incoming line must be hidden when filtering by resource, got: {lines:?}"
        );
        // The other totals must still be there.
        assert!(lines.iter().any(|l| l.contains("Entries")));
        assert!(lines.iter().any(|l| l.contains("Net")));
        assert!(lines.iter().any(|l| l.contains("Outgoing")));
    }

    #[test]
    fn build_filters_passes_resource() {
        let mut args = filter_args(SAMPLE_ADDRESS);
        args.resource = Some(SAMPLE_HASH.parse().unwrap());
        let filters = build_filters(&args).unwrap();
        assert_eq!(filters.resource.as_deref(), Some(SAMPLE_HASH));
    }

    #[test]
    fn build_filters_no_resource_by_default() {
        let filters = build_filters(&filter_args(SAMPLE_ADDRESS)).unwrap();
        assert!(filters.resource.is_none());
    }

    #[test]
    fn build_filters_since_sets_lower_bound() {
        let mut args = filter_args(SAMPLE_ADDRESS);
        args.since = Some(chrono::Duration::days(7));
        let before = (Utc::now() - chrono::Duration::days(7)).timestamp();
        let filters = build_filters(&args).unwrap();
        let start = filters.start_date.expect("since sets a lower bound");
        // Resolved against the wall clock; allow a small execution window.
        assert!((start - before).abs() <= 5, "start={start} before={before}");
        assert!(filters.end_date.is_none());
    }
}

async fn handle_buy(json: bool, args: BuyCreditArgs, cli_network: Option<&str>) -> Result<()> {
    let evm_account = resolve_evm_account(&args.signing)?;
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

    let token: CreditToken = args.token.into();
    let amount_raw = credit::parse_token_amount(&args.amount, token.decimals())
        .map_err(|e| anyhow!("invalid amount: {e}"))?;

    let (provider, alloy_address) = build_signer_provider(evm_account, rpc_url)?;
    let estimate = credit::estimate_credits(token, amount_raw, &ethereum.price_source).await?;

    // Dry-run must succeed even for under-funded accounts, so we defer the
    // balance check until we know we're actually submitting.
    if args.signing.dry_run {
        print_dry_run_summary(json, &args.amount, &estimate, &ethereum)?;
        return Ok(());
    }

    ensure_balance(
        &provider,
        alloy_address,
        token,
        &ethereum,
        amount_raw,
        &args.amount,
    )
    .await?;

    if !json {
        print_human_estimate(&args.amount, &estimate, &ethereum);
        if !args.yes && !confirm_submission("Proceed?")? {
            eprintln!("Cancelled.");
            return Ok(());
        }
    }

    // Native ETH is a plain value transfer to the credit contract; ERC20
    // tokens (ALEPH, USDC) call `transfer` on the token contract.
    let receipt = match ethereum.token_address(token) {
        Some(token_address) => {
            credit::buy_credits(
                &provider,
                token_address,
                ethereum.credit_contract,
                amount_raw,
            )
            .await?
        }
        None => credit::buy_credits_eth(&provider, ethereum.credit_contract, amount_raw).await?,
    };
    print_submission_result(json, &args.amount, &estimate, &ethereum, &receipt)?;
    Ok(())
}

async fn handle_transfer(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: TransferCreditArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let recipient = resolve_address(&args.to)?;

    let content = CreditTransferContent {
        transfer: CreditTransferList {
            credits: vec![CreditTransferEntry {
                address: recipient.clone(),
                amount: args.amount,
                expiration: args.expiration,
            }],
        },
    };
    content.validate()?;
    if account.address() == &recipient {
        return Err(CreditTransferError::SelfTransfer(recipient).into());
    }

    let envelope = serde_json::json!({
        "type": CREDIT_TRANSFER_POST_TYPE,
        "content": content,
    });
    let mut builder = MessageBuilder::new(&account, MessageType::Post, envelope);
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;

    if !json && !dry_run {
        print_transfer_summary(&args.to, &recipient, args.amount, args.expiration);
        if !args.yes && !confirm_submission("Proceed?")? {
            eprintln!("Cancelled.");
            return Ok(());
        }
    }
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

fn print_transfer_summary(
    input: &str,
    resolved: &AlephAddress,
    amount: u64,
    expiration: Option<DateTime<Utc>>,
) {
    eprintln!("Transfer {amount} credits");
    eprintln!("  To: {}", format_address(input, resolved));
    if let Some(exp) = expiration {
        eprintln!("  Expiration: {}", exp.to_rfc3339());
    }
}

fn resolve_evm_account(signing: &SigningArgs) -> Result<EvmAccount> {
    match resolve_account(&signing.identity)? {
        CliAccount::Evm(a) => Ok(a),
        CliAccount::LazyKeystore(a) => a.into_evm(),
        CliAccount::LedgerEvm(_) => Err(anyhow!(
            "Ledger accounts are not supported for credit purchases. Use a local account."
        )),
        CliAccount::Sol(_) => Err(anyhow!(
            "credit purchases require an EVM account (got Solana)"
        )),
    }
}

fn build_signer_provider(
    evm_account: EvmAccount,
    rpc_url: &str,
) -> Result<(impl Provider, Address)> {
    let wallet = PrivateKeySigner::from_signing_key(evm_account.signing_key().clone());
    let address = wallet.address();
    let url = rpc_url
        .parse()
        .map_err(|e| anyhow!("invalid RPC URL: {e}"))?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(wallet))
        .connect_http(url);
    Ok((provider, address))
}

async fn ensure_balance(
    provider: &impl Provider,
    owner: Address,
    token: CreditToken,
    ethereum: &EthereumConfig,
    amount_raw: U256,
    amount_display: &str,
) -> Result<()> {
    // ERC20 tokens read `balanceOf`; native ETH reads the account balance.
    let balance = match ethereum.token_address(token) {
        Some(token_address) => credit::check_balance(provider, owner, token, token_address).await?,
        None => credit::check_eth_balance(provider, owner).await?,
    };
    if balance < amount_raw {
        let have = format_token_amount(balance, token.decimals());
        bail!(
            "insufficient {} balance: have {have}, need {amount_display}",
            token.symbol(),
        );
    }
    Ok(())
}

/// JSON envelope shared by dry-run and post-submit output.
fn summary_json(
    amount_display: &str,
    estimate: &CreditEstimate,
    ethereum: &EthereumConfig,
) -> serde_json::Value {
    serde_json::json!({
        "token": estimate.token.symbol(),
        "amount": amount_display,
        "estimated_credits": estimate.estimated_credits,
        "price_usd": estimate.price_usd,
        "bonus_ratio": estimate.bonus_ratio,
        "recipient": format!("{}", ethereum.credit_contract),
    })
}

fn print_human_estimate(
    amount_display: &str,
    estimate: &CreditEstimate,
    ethereum: &EthereumConfig,
) {
    let symbol = estimate.token.symbol();
    eprintln!("Buying credits with {amount_display} {symbol}");
    match (estimate.estimated_credits, estimate.price_usd) {
        // USDC is pegged at $1, so the price line adds no information.
        (Some(credits), _) if matches!(estimate.token, CreditToken::Usdc) => {
            eprintln!("Estimated credits: ~{credits:.0}")
        }
        // Market-priced tokens (ALEPH, ETH): show the price, and the bonus
        // only when one applies.
        (Some(credits), Some(price)) if estimate.bonus_ratio > 0.0 => eprintln!(
            "Estimated credits: ~{credits:.0} (at ${price:.2}/{symbol}, +{bonus:.0}% bonus)",
            bonus = estimate.bonus_ratio * 100.0,
        ),
        (Some(credits), Some(price)) => {
            eprintln!("Estimated credits: ~{credits:.0} (at ${price:.2}/{symbol})")
        }
        _ => eprintln!("Estimated credits: unknown (network has no {symbol} price source)"),
    }
    eprintln!("Recipient: {}", ethereum.credit_contract);
}

fn print_dry_run_summary(
    json: bool,
    amount_display: &str,
    estimate: &CreditEstimate,
    ethereum: &EthereumConfig,
) -> Result<()> {
    if json {
        let mut output = summary_json(amount_display, estimate, ethereum);
        output["dry_run"] = serde_json::Value::Bool(true);
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_human_estimate(amount_display, estimate, ethereum);
        eprintln!("\nDry run — transaction not submitted.");
    }
    Ok(())
}

fn print_submission_result(
    json: bool,
    amount_display: &str,
    estimate: &CreditEstimate,
    ethereum: &EthereumConfig,
    receipt: &TransactionReceipt,
) -> Result<()> {
    let tx_hash = receipt.transaction_hash;
    if json {
        let mut output = summary_json(amount_display, estimate, ethereum);
        output["tx_hash"] = serde_json::Value::String(format!("{tx_hash}"));
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("\nTransaction submitted: {tx_hash}");
        if let Some(base) = &ethereum.explorer_tx_base {
            eprintln!("{}{}", base, tx_hash);
        }
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
        assert!(matches!(
            CreditToken::from(CreditTokenCli::Eth),
            CreditToken::Eth
        ));
    }

    fn sample_estimate(token: CreditToken) -> CreditEstimate {
        CreditEstimate {
            token,
            amount_raw: U256::from(100u64) * U256::from(10u64).pow(U256::from(token.decimals())),
            estimated_credits: Some(120_000_000.0),
            price_usd: Some(1.0),
            bonus_ratio: token.bonus_ratio(),
        }
    }

    #[test]
    fn summary_json_has_expected_shape() {
        let estimate = sample_estimate(CreditToken::Aleph);
        let eth = EthereumConfig::mainnet_defaults();
        let v = summary_json("100", &estimate, &eth);

        assert_eq!(v["token"], "ALEPH");
        assert_eq!(v["amount"], "100");
        assert_eq!(v["estimated_credits"], 120_000_000.0);
        assert_eq!(v["price_usd"], 1.0);
        assert_eq!(v["bonus_ratio"], 0.2);
        assert_eq!(v["recipient"], format!("{}", eth.credit_contract));
        assert!(v.get("tx_hash").is_none(), "tx_hash only set after submit");
        assert!(v.get("dry_run").is_none(), "dry_run only set for dry-run");
    }

    #[test]
    fn summary_json_price_usd_is_null_when_source_is_none() {
        let estimate = CreditEstimate {
            token: CreditToken::Aleph,
            amount_raw: U256::from(1u64),
            estimated_credits: None,
            price_usd: None,
            bonus_ratio: 0.2,
        };
        let eth = EthereumConfig::mainnet_defaults();
        let v = summary_json("1", &estimate, &eth);
        assert!(v["price_usd"].is_null());
        assert!(v["estimated_credits"].is_null());
    }

    #[test]
    fn summary_json_uses_usdc_symbol_for_usdc() {
        let estimate = sample_estimate(CreditToken::Usdc);
        let eth = EthereumConfig::mainnet_defaults();
        let v = summary_json("50", &estimate, &eth);
        assert_eq!(v["token"], "USDC");
        assert_eq!(v["bonus_ratio"], 0.0);
    }

    #[test]
    fn transfer_envelope_shape() {
        use aleph_sdk::credit_transfer::{
            CREDIT_TRANSFER_POST_TYPE, CreditTransferContent, CreditTransferEntry,
            CreditTransferList,
        };
        use aleph_types::chain::Address as AlephAddress;

        let content = CreditTransferContent {
            transfer: CreditTransferList {
                credits: vec![CreditTransferEntry {
                    address: AlephAddress::from("0xrecipient".to_string()),
                    amount: 1500,
                    expiration: None,
                }],
            },
        };
        let envelope = serde_json::json!({
            "type": CREDIT_TRANSFER_POST_TYPE,
            "content": content,
        });

        assert_eq!(envelope["type"], "aleph_credit_transfer");
        assert_eq!(
            envelope["content"]["transfer"]["credits"][0]["address"],
            "0xrecipient"
        );
        assert_eq!(
            envelope["content"]["transfer"]["credits"][0]["amount"],
            1500
        );
        assert!(
            envelope["content"]["transfer"]["credits"][0]
                .get("expiration")
                .is_none()
        );
    }

    #[test]
    fn transfer_self_transfer_error_kind() {
        use aleph_sdk::credit_transfer::CreditTransferError;
        use aleph_types::chain::Address as AlephAddress;
        let addr = AlephAddress::from("0xrecipient".to_string());
        let err = CreditTransferError::SelfTransfer(addr.clone());
        let msg = format!("{err}");
        assert!(
            msg.contains("sender and recipient must differ"),
            "got: {msg}"
        );
        assert!(msg.contains("0xrecipient"), "got: {msg}");
    }
}
