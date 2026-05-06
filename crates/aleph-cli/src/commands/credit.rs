use crate::account::CliAccount;
use crate::account::store::AccountStore;
use crate::cli::{
    BuyCreditArgs, CreditCommand, CreditHistoryArgs, CreditTokenCli, SigningArgs,
    TransferCreditArgs,
};
use crate::common::{
    format_address, resolve_account, resolve_address, resolve_network, submit_or_preview,
};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{AlephAccountClient, AlephClient};
use aleph_sdk::credit::{self, CreditEstimate, CreditToken, EthereumConfig, format_token_amount};
use aleph_sdk::credit_transfer::{
    CREDIT_TRANSFER_POST_TYPE, CreditTransferContent, CreditTransferEntry, CreditTransferError,
    CreditTransferList,
};
use aleph_types::account::{Account, EvmAccount};
use aleph_types::chain::Address as AlephAddress;
use aleph_types::channel::Channel;
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
    }
}

async fn handle_history(
    aleph_client: &AlephClient,
    json: bool,
    args: CreditHistoryArgs,
) -> Result<()> {
    let address = resolve_owner_address(args.address.as_deref())?;
    let history = aleph_client
        .get_credit_history(&address, args.page, Some(args.page_size))
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
        "Credit history for {} (page {} of ~{}, {} per page, {} total)",
        history.address,
        history.pagination_page,
        total_pages(history.pagination_total, history.pagination_per_page),
        history.pagination_per_page,
        history.pagination_total,
    );
    eprintln!(
        "{:<19}  {:>15}  {:<15}  {:<20}  {:<20}  Expires",
        "Timestamp", "Amount", "Method", "Origin", "Origin ref",
    );
    for item in &history.credit_history {
        eprintln!(
            "{:<19}  {:>15}  {:<15}  {:<20}  {:<20}  {}",
            item.message_timestamp.format("%Y-%m-%d %H:%M:%S"),
            item.amount,
            display_optional(&item.payment_method, 15),
            display_optional(&item.origin, 20),
            display_optional(&item.origin_ref, 20),
            item.expiration_date
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "Never".to_string()),
        );
    }
    Ok(())
}

/// Resolve the owner address for read-only credit queries.
///
/// Mirrors `aleph account balance` / `aleph aggregate list`: explicit
/// `--address` (raw or local-name) wins, otherwise we use the default
/// account from the local store.
fn resolve_owner_address(args_address: Option<&str>) -> Result<AlephAddress> {
    if let Some(value) = args_address {
        return resolve_address(value);
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
    if width <= 1 || s.chars().count() <= width {
        return s.to_string();
    }
    let head: String = s.chars().take(width.saturating_sub(1)).collect();
    format!("{head}…")
}

#[cfg(test)]
mod history_tests {
    use super::*;

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

    ensure_token_balance(
        &provider,
        alloy_address,
        token,
        ethereum.token_address(token),
        amount_raw,
        &args.amount,
    )
    .await?;

    if !json {
        print_human_estimate(&args.amount, &estimate, &ethereum);
        if !args.yes && !confirm_submission()? {
            eprintln!("Cancelled.");
            return Ok(());
        }
    }

    let receipt = credit::buy_credits(
        &provider,
        ethereum.token_address(token),
        ethereum.credit_contract,
        amount_raw,
    )
    .await?;
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
        if !args.yes && !confirm_submission()? {
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

async fn ensure_token_balance(
    provider: &impl Provider,
    owner: Address,
    token: CreditToken,
    token_address: Address,
    amount_raw: U256,
    amount_display: &str,
) -> Result<()> {
    let balance = credit::check_balance(provider, owner, token, token_address).await?;
    if balance < amount_raw {
        let have = format_token_amount(balance, token.decimals());
        bail!(
            "insufficient {} balance: have {have}, need {amount_display}",
            token.symbol(),
        );
    }
    Ok(())
}

fn confirm_submission() -> Result<bool> {
    eprintln!();
    dialoguer::Confirm::new()
        .with_prompt("Proceed?")
        .default(false)
        .interact()
        .map_err(|e| anyhow!("failed to read confirmation: {e}"))
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
    eprintln!(
        "Buying credits with {amount_display} {}",
        estimate.token.symbol()
    );
    match (
        estimate.token,
        estimate.estimated_credits,
        estimate.price_usd,
    ) {
        (CreditToken::Aleph, Some(credits), Some(price)) => eprintln!(
            "Estimated credits: ~{credits:.0} (at ${price:.2}/{symbol}, +{bonus:.0}% bonus)",
            symbol = estimate.token.symbol(),
            bonus = estimate.bonus_ratio * 100.0,
        ),
        (CreditToken::Usdc, Some(credits), _) => {
            eprintln!("Estimated credits: ~{credits:.0}")
        }
        _ => eprintln!("Estimated credits: unknown (network has no ALEPH price source)"),
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
