use crate::account::generate::generate_key;
use crate::account::store::{AccountKind, AccountStore};
use crate::cli::{
    AccountBalanceArgs, AccountCommand, AccountCreateArgs, AccountDeleteArgs, AccountExportArgs,
    AccountImportArgs, AccountMigrateArgs, AccountShowArgs, AccountUseArgs, AliasAddArgs,
    AliasCommand, AliasRemoveArgs,
};
use aleph_sdk::client::{AccountBalance, AlephAccountClient, AlephClient};
use aleph_types::account::Account;
use aleph_types::chain::Address;
use anyhow::{Context, Result};
use zeroize::Zeroizing;

pub async fn handle_account_command(
    client: &AlephClient,
    command: AccountCommand,
    json: bool,
) -> Result<()> {
    let store = AccountStore::open().context("failed to open account store")?;

    match command {
        AccountCommand::Create(args) => handle_create(&store, args, json),
        AccountCommand::Import(args) => handle_import(&store, args, json),
        AccountCommand::List => handle_list(client, &store, json).await,
        AccountCommand::Migrate(args) => handle_migrate(&store, args, json),
        AccountCommand::Show(args) => handle_show(client, &store, args, json).await,
        AccountCommand::Balance(args) => handle_balance(client, &store, args, json).await,
        AccountCommand::Delete(args) => handle_delete(&store, args),
        AccountCommand::Use(args) => handle_use(&store, args, json),
        AccountCommand::Export(args) => handle_export(&store, args, json),
        AccountCommand::Alias { command } => handle_alias_command(&store, command, json),
    }
}

fn handle_create(store: &AccountStore, args: AccountCreateArgs, json: bool) -> Result<()> {
    let chain: aleph_types::chain::Chain = args.chain.into();
    let (key_hex, address) = generate_key(chain.clone())?;

    store.add_local_account(&args.name, chain.clone(), address.clone(), &key_hex)?;

    if json {
        let output = serde_json::json!({
            "name": args.name,
            "chain": chain,
            "address": address,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Account '{}' created.", args.name);
        eprintln!("  Chain:   {chain}");
        eprintln!("  Address: {address}");
    }
    Ok(())
}

fn handle_import(store: &AccountStore, args: AccountImportArgs, json: bool) -> Result<()> {
    if args.ledger {
        return handle_import_ledger(store, args, json);
    }

    let chain: aleph_types::chain::Chain = args.chain.into();

    let key_hex = if let Some(path) = &args.from_file {
        // Read from key file (raw binary or hex text)
        crate::account::migrate::read_key_file(path).context("failed to read key file")?
    } else {
        // Existing flow: CLI flag, env var, or interactive stdin
        let raw = match args.private_key {
            Some(k) => k,
            None => match std::env::var("ALEPH_PRIVATE_KEY") {
                Ok(k) => k,
                Err(_) => rpassword::prompt_password("Enter private key (hex): ")
                    .context("failed to read private key from stdin")?,
            },
        };
        Zeroizing::new(raw.strip_prefix("0x").unwrap_or(&raw).to_string())
    };

    let account = crate::account::load_account(Some(&key_hex), chain.clone())?;
    let address = account.address().to_string();

    store.add_local_account(&args.name, chain.clone(), address.clone(), &key_hex)?;

    if json {
        let output = serde_json::json!({
            "name": args.name,
            "chain": chain,
            "address": address,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Account '{}' imported.", args.name);
        eprintln!("  Chain:   {chain}");
        eprintln!("  Address: {address}");
    }
    Ok(())
}

fn handle_import_ledger(store: &AccountStore, args: AccountImportArgs, json: bool) -> Result<()> {
    use crate::account::ledger::{self, DerivationPath};

    let chain: aleph_types::chain::Chain = args.chain.into();

    if !chain.is_evm() {
        anyhow::bail!("Ledger import is only supported for EVM chains");
    }

    let base_path = match &args.derivation_path {
        Some(p) => {
            DerivationPath::parse(p).map_err(|e| anyhow::anyhow!("invalid derivation path: {e}"))?
        }
        None => DerivationPath::default_evm(),
    };

    let count = args.ledger_count;
    if count == 0 {
        anyhow::bail!("--ledger-count must be at least 1");
    }

    if !json {
        eprintln!("Connect your Ledger and open the Ethereum app.");
    }

    // Fetch addresses from device (async -> sync bridge)
    let addresses = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let device = ledger::connect().await?;
            ledger::get_evm_addresses(&device, &base_path, count).await
        })
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Select address
    let (address, path) = if json || addresses.len() == 1 {
        addresses.into_iter().next().unwrap()
    } else {
        let items: Vec<String> = addresses
            .iter()
            .map(|(addr, path)| format!("{}  ({})", addr, path))
            .collect();

        let selection = dialoguer::Select::new()
            .with_prompt("Select an account")
            .items(&items)
            .default(0)
            .interact()
            .context("failed to show account selector")?;

        addresses.into_iter().nth(selection).unwrap()
    };

    let address_str = address.to_string();
    let path_str = path.to_string();

    store.add_ledger_account(
        &args.name,
        chain.clone(),
        address_str.clone(),
        path_str.clone(),
    )?;

    if json {
        let output = serde_json::json!({
            "name": args.name,
            "chain": chain,
            "address": address_str,
            "kind": "ledger",
            "derivation_path": path_str,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Account '{}' imported.", args.name);
        eprintln!("  Type:    ledger");
        eprintln!("  Chain:   {chain}");
        eprintln!("  Address: {address_str}");
        eprintln!("  Path:    {path_str}");
    }
    Ok(())
}

fn handle_migrate(store: &AccountStore, args: AccountMigrateArgs, json: bool) -> Result<()> {
    let python_home = crate::account::migrate::resolve_python_home(args.python_home.as_deref())?;

    if !json {
        if args.dry_run {
            eprintln!("Dry run — no accounts will be imported.\n");
        }
        eprintln!("Scanning {}...\n", python_home.display());
    }

    let result = crate::account::migrate::migrate_accounts(store, &python_home, args.dry_run)?;

    if result.migrated.is_empty() && result.skipped.is_empty() {
        if json {
            println!("{}", serde_json::json!({"migrated": [], "skipped": []}));
        } else {
            eprintln!("No Python CLI accounts found in {}.", python_home.display());
        }
        return Ok(());
    }

    if json {
        let migrated: Vec<_> = result
            .migrated
            .iter()
            .map(|m| {
                let mut obj = serde_json::json!({
                    "name": m.name,
                    "chain": m.chain,
                    "address": m.address,
                    "kind": m.kind,
                    "default": m.is_default,
                });
                if let Some(ref path) = m.derivation_path {
                    obj["derivation_path"] = serde_json::json!(path);
                }
                obj
            })
            .collect();
        let skipped: Vec<_> = result
            .skipped
            .iter()
            .map(|s| {
                serde_json::json!({
                    "filename": s.filename,
                    "reason": s.reason,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "migrated": migrated,
                "skipped": skipped,
            }))?
        );
        return Ok(());
    }

    // Human-readable output
    let verb = if args.dry_run {
        "Would migrate"
    } else {
        "Migrated"
    };
    eprintln!(
        "{verb} {} account(s) from {}:",
        result.migrated.len(),
        python_home.display()
    );
    for m in &result.migrated {
        let mut suffix = String::new();
        if m.kind == "ledger" {
            suffix.push_str(" (ledger");
            if let Some(ref path) = m.derivation_path {
                suffix.push_str(&format!(", {path}"));
            }
            suffix.push(')');
        }
        if m.is_default {
            suffix.push_str(" (default)");
        }
        eprintln!("  {:<16} {:<6} {}{}", m.name, m.chain, m.address, suffix);
    }

    if !result.skipped.is_empty() {
        eprintln!("\nSkipped:");
        for s in &result.skipped {
            eprintln!("  {} — {}", s.filename, s.reason);
        }
    }

    if !args.dry_run {
        eprintln!("\nRun `aleph account list` to verify.");
    }
    Ok(())
}

/// 1,000,000 credits = $1 USD.
const CREDITS_PER_USD: f64 = 1_000_000.0;

fn format_credits(credits: u64) -> String {
    let usd = credits as f64 / CREDITS_PER_USD;
    format!("{credits} (${usd:.2})")
}

async fn fetch_balance(client: &AlephClient, address: &str) -> Option<AccountBalance> {
    let addr = Address::from(address.to_string());
    client.get_balance(&addr).await.ok()
}

async fn handle_list(client: &AlephClient, store: &AccountStore, json: bool) -> Result<()> {
    let manifest = store.load_manifest()?;

    if manifest.accounts.is_empty() && manifest.aliases.is_empty() {
        if json {
            println!("[]");
        } else {
            eprintln!("No accounts. Create one with: aleph account create --name <NAME>");
        }
        return Ok(());
    }

    // Fetch balances for all accounts concurrently.
    let balance_futures: Vec<_> = manifest
        .accounts
        .iter()
        .map(|a| fetch_balance(client, &a.address))
        .collect();
    let balances = futures_util::future::join_all(balance_futures).await;

    if json {
        let mut output: Vec<_> = manifest
            .accounts
            .iter()
            .zip(&balances)
            .map(|(a, bal)| {
                let mut obj = serde_json::json!({
                    "name": a.name,
                    "chain": a.chain,
                    "address": a.address,
                    "kind": a.kind_display(),
                });
                if let Some(bal) = bal {
                    obj["balance"] = serde_json::json!({
                        "aleph_tokens": bal.aleph_tokens,
                        "locked_aleph_tokens": bal.locked_aleph_tokens,
                        "credits": bal.credits,
                    });
                }
                obj
            })
            .collect();
        for alias in &manifest.aliases {
            output.push(serde_json::json!({
                "name": alias.name,
                "address": alias.address,
                "kind": "alias",
            }));
        }
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }

    eprintln!(
        "{:<2} {:<16} {:<6} {:<48} {:<10} {:<14} CREDITS",
        "", "NAME", "CHAIN", "ADDRESS", "TYPE", "ALEPH"
    );
    for (account, balance) in manifest.accounts.iter().zip(&balances) {
        let marker = if manifest.default.as_deref() == Some(&account.name) {
            "*"
        } else {
            " "
        };
        let (aleph, credits) = match balance {
            Some(b) => (format!("{:.4}", b.aleph_tokens), format_credits(b.credits)),
            None => ("N/A".into(), "N/A".into()),
        };
        eprintln!(
            "{:<2} {:<16} {:<6} {:<48} {:<10} {:<14} {}",
            marker,
            account.name,
            account.chain,
            account.address,
            account.kind_display(),
            aleph,
            credits,
        );
    }
    for alias in &manifest.aliases {
        eprintln!(
            "{:<2} {:<16} {:<6} {:<48} {:<10}",
            "", alias.name, "", alias.address, "alias",
        );
    }
    Ok(())
}

async fn handle_show(
    client: &AlephClient,
    store: &AccountStore,
    args: AccountShowArgs,
    json: bool,
) -> Result<()> {
    let name = match args.name {
        Some(n) => n,
        None => store.default_account_name()?.ok_or_else(|| {
            anyhow::anyhow!("no default account set; use: aleph account use <NAME>")
        })?,
    };

    let entry = store.get_account(&name)?;
    let balance = fetch_balance(client, &entry.address).await;

    if json {
        let mut output = serde_json::to_value(&entry)?;
        if let Some(bal) = &balance {
            output["balance"] = serde_json::json!({
                "aleph_tokens": bal.aleph_tokens,
                "locked_aleph_tokens": bal.locked_aleph_tokens,
                "credits": bal.credits,
            });
        }
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        let is_default =
            store.default_account_name().ok().flatten().as_deref() == Some(name.as_str());
        eprintln!("Name:    {}", entry.name);
        eprintln!("Chain:   {}", entry.chain);
        eprintln!("Address: {}", entry.address);
        eprintln!("Type:    {}", entry.kind_display());
        if is_default {
            eprintln!("Default: yes");
        }
        if let Some(path) = &entry.derivation_path {
            eprintln!("Path:    {path}");
        }
        match &balance {
            Some(b) => {
                eprintln!(
                    "ALEPH:   {:.4} (locked: {:.4})",
                    b.aleph_tokens, b.locked_aleph_tokens
                );
                eprintln!("Credits: {}", format_credits(b.credits));
            }
            None => {
                eprintln!("Balance: N/A (could not reach CCN)");
            }
        }
    }
    Ok(())
}

async fn handle_balance(
    client: &AlephClient,
    store: &AccountStore,
    args: AccountBalanceArgs,
    json: bool,
) -> Result<()> {
    let address = match args.address {
        Some(addr) => addr,
        None => {
            let name = store.default_account_name()?.ok_or_else(|| {
                anyhow::anyhow!(
                    "no address provided and no default account set; \
                     use: aleph account balance <ADDRESS>"
                )
            })?;
            store.get_account(&name)?.address
        }
    };

    let balance = fetch_balance(client, &address)
        .await
        .ok_or_else(|| anyhow::anyhow!("could not fetch balance from CCN"))?;

    if json {
        let output = serde_json::json!({
            "address": address,
            "aleph_tokens": balance.aleph_tokens,
            "locked_aleph_tokens": balance.locked_aleph_tokens,
            "credits": balance.credits,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Address: {address}");
        eprintln!(
            "ALEPH:   {:.4} (locked: {:.4})",
            balance.aleph_tokens, balance.locked_aleph_tokens
        );
        eprintln!("Credits: {}", format_credits(balance.credits));
    }
    Ok(())
}

fn handle_delete(store: &AccountStore, args: AccountDeleteArgs) -> Result<()> {
    // Verify account exists before prompting
    store.get_account(&args.name)?;

    eprintln!(
        "Are you sure you want to delete account '{}'? This cannot be undone.",
        args.name
    );
    eprintln!("Type the account name to confirm: ");
    let mut confirmation = String::new();
    std::io::stdin()
        .read_line(&mut confirmation)
        .context("failed to read confirmation")?;
    if confirmation.trim() != args.name {
        eprintln!("Aborted.");
        return Ok(());
    }

    store.delete_account(&args.name)?;
    eprintln!("Account '{}' deleted.", args.name);
    Ok(())
}

fn handle_use(store: &AccountStore, args: AccountUseArgs, json: bool) -> Result<()> {
    store.set_default(&args.name)?;

    if json {
        let output = serde_json::json!({ "default": args.name });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Default account set to '{}'.", args.name);
    }
    Ok(())
}

fn handle_export(store: &AccountStore, args: AccountExportArgs, json: bool) -> Result<()> {
    let entry = store.get_account(&args.name)?;

    if entry.kind != AccountKind::Local {
        anyhow::bail!("cannot export key for non-local account '{}'", args.name);
    }

    if !args.yes {
        eprintln!("WARNING: This will display your private key in the terminal.");
        eprintln!("Type 'yes' to continue: ");
        let mut confirmation = String::new();
        std::io::stdin()
            .read_line(&mut confirmation)
            .context("failed to read confirmation")?;
        if confirmation.trim() != "yes" {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    let key = Zeroizing::new(store.get_private_key(&args.name)?);

    if json {
        let output = serde_json::json!({
            "name": args.name,
            "chain": entry.chain,
            "address": entry.address,
            "private_key": *key,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("0x{}", *key);
    }
    Ok(())
}

fn handle_alias_command(store: &AccountStore, command: AliasCommand, json: bool) -> Result<()> {
    match command {
        AliasCommand::Add(args) => handle_alias_add(store, args, json),
        AliasCommand::List => handle_alias_list(store, json),
        AliasCommand::Remove(args) => handle_alias_remove(store, args, json),
    }
}

fn handle_alias_add(store: &AccountStore, args: AliasAddArgs, json: bool) -> Result<()> {
    store.add_alias(&args.name, args.address.clone())?;

    if json {
        let output = serde_json::json!({
            "name": args.name,
            "address": args.address,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Alias '{}' added ({}).", args.name, args.address);
    }
    Ok(())
}

fn handle_alias_list(store: &AccountStore, json: bool) -> Result<()> {
    let manifest = store.load_manifest()?;

    if manifest.aliases.is_empty() {
        if json {
            println!("[]");
        } else {
            eprintln!("No aliases. Add one with: aleph account alias add <NAME> <ADDRESS>");
        }
        return Ok(());
    }

    if json {
        let output: Vec<_> = manifest
            .aliases
            .iter()
            .map(|a| {
                serde_json::json!({
                    "name": a.name,
                    "address": a.address,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("{:<16} ADDRESS", "NAME");
        for alias in &manifest.aliases {
            eprintln!("{:<16} {}", alias.name, alias.address);
        }
    }
    Ok(())
}

fn handle_alias_remove(store: &AccountStore, args: AliasRemoveArgs, json: bool) -> Result<()> {
    store.remove_alias(&args.name)?;

    if json {
        let output = serde_json::json!({ "removed": args.name });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Alias '{}' removed.", args.name);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_credits_zero() {
        assert_eq!(format_credits(0), "0 ($0.00)");
    }

    #[test]
    fn format_credits_whole_dollars() {
        assert_eq!(format_credits(1_000_000), "1000000 ($1.00)");
        assert_eq!(format_credits(3_900_000), "3900000 ($3.90)");
    }

    #[test]
    fn format_credits_fractional() {
        assert_eq!(format_credits(500_000), "500000 ($0.50)");
        assert_eq!(format_credits(1_234), "1234 ($0.00)");
    }

    #[test]
    fn format_credits_large() {
        assert_eq!(format_credits(1_000_000_000), "1000000000 ($1000.00)");
    }
}
