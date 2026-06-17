use crate::account::generate::generate_key;
use crate::account::store::{AccountKind, AccountStore};
use crate::cli::{
    AccountBalanceArgs, AccountCommand, AccountCreateArgs, AccountExportArgs, AccountImportArgs,
    AccountMigrateArgs, AccountRemoveArgs, AccountSetArgs, AccountShowArgs, AccountUseArgs,
    AliasAddArgs, AliasCommand, AliasRemoveArgs, SshAddArgs, SshCommand, SshListArgs,
    SshRemoveArgs,
};
use crate::commands::message::{ForgetTargets, forget_targets};
use crate::common::{
    confirm_typed_match, format_address, resolve_account, resolve_address,
    resolve_address_or_active, submit_or_preview,
};
use aleph_sdk::client::{AccountBalance, AlephAccountClient, AlephClient};
use aleph_sdk::ssh::{AlephSshClient, SshKey, build_add_ssh_key};
use aleph_types::account::Account;
use aleph_types::chain::Address;
use aleph_types::item_hash::ItemHash;
use anyhow::{Context, Result, anyhow, bail};
use std::io::Read;
use std::path::Path;
use url::Url;
use zeroize::Zeroizing;

pub async fn handle_account_command(
    client: &AlephClient,
    ccn_url: &Url,
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
        AccountCommand::Set(args) => handle_set(&store, args, json),
        AccountCommand::Balance(args) => handle_balance(client, &store, args, json).await,
        AccountCommand::Remove(args) => handle_remove(&store, args),
        AccountCommand::Use(args) => handle_use(&store, args, json),
        AccountCommand::Export(args) => handle_export(&store, args, json),
        AccountCommand::Alias { command } => handle_alias_command(&store, command, json),
        AccountCommand::Ssh { command } => handle_ssh_command(client, ccn_url, command, json).await,
    }
}

fn handle_create(store: &AccountStore, args: AccountCreateArgs, json: bool) -> Result<()> {
    let chain: aleph_types::chain::Chain = args.chain.into();

    if args.encrypted {
        return handle_create_encrypted(store, &args.name, chain, json);
    }

    let (key_hex, address) = generate_key(chain.clone())?;

    store.add_local_account(&args.name, chain.clone(), address.clone(), &key_hex)?;

    print_account_created(&args.name, &chain, &address, "local", json)
}

fn handle_create_encrypted(
    store: &AccountStore,
    name: &str,
    chain: aleph_types::chain::Chain,
    json: bool,
) -> Result<()> {
    use crate::account::{keystore, password};

    if !chain.is_evm() {
        anyhow::bail!("--encrypted accounts are only supported for EVM chains");
    }
    // Fail on invalid/taken names before prompting for a password. The
    // store re-checks uniqueness on write; this is an early exit so the
    // user isn't asked for a password just to be told the name is taken.
    store.check_name_available(name)?;

    let passphrase = password::read_new_password()?;
    let (key_hex, address) = generate_key(chain.clone())?;
    let key_bytes = keystore::decode_key_hex(&key_hex)?;
    let ks = keystore::encrypt_key(&key_bytes, &passphrase, &address)?;
    let ks_json = serde_json::to_string_pretty(&ks)?;

    store.add_keystore_account(name, chain.clone(), address.clone(), &ks_json)?;

    print_account_created(name, &chain, &address, "encrypted", json)
}

fn print_account_created(
    name: &str,
    chain: &aleph_types::chain::Chain,
    address: &str,
    kind: &str,
    json: bool,
) -> Result<()> {
    if json {
        let output = serde_json::json!({
            "name": name,
            "chain": chain,
            "address": address,
            "kind": kind,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Account '{name}' created.");
        eprintln!("  Type:    {kind}");
        eprintln!("  Chain:   {chain}");
        eprintln!("  Address: {address}");
    }
    Ok(())
}

fn handle_import(store: &AccountStore, args: AccountImportArgs, json: bool) -> Result<()> {
    use crate::account::{keystore, password};

    if args.ledger {
        return handle_import_ledger(store, args, json);
    }

    let chain: aleph_types::chain::Chain = args.chain.into();

    let key_hex = if let Some(path) = &args.from_file {
        // The file may hold a raw key, so keep the buffer zeroized.
        let raw = Zeroizing::new(
            std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
        );

        // V3 keystore files are detected and imported as-is (still encrypted
        // with their original password).
        if let Ok(contents) = std::str::from_utf8(&raw) {
            match keystore::try_parse_v3(contents) {
                Ok(Some(ks)) => {
                    return handle_import_keystore_file(store, &args, chain, &ks, contents, json);
                }
                Ok(None) => {} // not a keystore — fall through to raw key handling
                Err(e) => {
                    anyhow::bail!(
                        "{} looks like a keystore file but is invalid: {e}",
                        path.display()
                    );
                }
            }
        }

        // Raw key file (32-byte binary or hex text)
        crate::account::migrate::parse_key_bytes(&raw, path).context("failed to read key file")?
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

    if args.encrypted {
        if !chain.is_evm() {
            anyhow::bail!("--encrypted accounts are only supported for EVM chains");
        }
        // Early exit before the password prompt; the store re-checks on write.
        store.check_name_available(&args.name)?;
        let passphrase = password::read_new_password()?;
        let key_bytes = keystore::decode_key_hex(&key_hex)?;
        let ks = keystore::encrypt_key(&key_bytes, &passphrase, &address)?;
        let ks_json = serde_json::to_string_pretty(&ks)?;
        store.add_keystore_account(&args.name, chain.clone(), address.clone(), &ks_json)?;
        return print_account_imported(&args.name, &chain, &address, "encrypted", json);
    }

    store.add_local_account(&args.name, chain.clone(), address.clone(), &key_hex)?;

    print_account_imported(&args.name, &chain, &address, "local", json)
}

/// Import an existing V3 keystore file: validate its password, derive the
/// address, and copy the file as-is (no re-encryption).
fn handle_import_keystore_file(
    store: &AccountStore,
    args: &AccountImportArgs,
    chain: aleph_types::chain::Chain,
    ks: &crate::account::keystore::KeystoreV3,
    contents: &str,
    json: bool,
) -> Result<()> {
    use crate::account::password;
    use aleph_types::account::EvmAccount;

    if !chain.is_evm() {
        anyhow::bail!("keystore files are only supported for EVM chains");
    }
    store.check_name_available(&args.name)?;

    let key = password::unlock_keystore(ks, &args.name)?;
    let account = EvmAccount::new(chain.clone(), &key[..]).map_err(|e| anyhow::anyhow!(e))?;
    let address = account.address().to_string();

    if let Some(embedded) = &ks.address {
        let derived = address.trim_start_matches("0x").to_lowercase();
        let embedded_norm = embedded.trim_start_matches("0x").to_lowercase();
        if embedded_norm != derived {
            anyhow::bail!(
                "keystore address field ({embedded}) does not match the address \
                 derived from the key ({address}); the file may be corrupt"
            );
        }
    }

    store.add_keystore_account(&args.name, chain.clone(), address.clone(), contents)?;

    print_account_imported(&args.name, &chain, &address, "encrypted", json)
}

fn print_account_imported(
    name: &str,
    chain: &aleph_types::chain::Chain,
    address: &str,
    kind: &str,
    json: bool,
) -> Result<()> {
    if json {
        let output = serde_json::json!({
            "name": name,
            "chain": chain,
            "address": address,
            "kind": kind,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Account '{name}' imported.");
        eprintln!("  Type:    {kind}");
        eprintln!("  Chain:   {chain}");
        eprintln!("  Address: {address}");
    }
    Ok(())
}

fn handle_import_ledger(store: &AccountStore, args: AccountImportArgs, json: bool) -> Result<()> {
    use crate::account::ledger::{self, DerivationPath};

    // Fail fast on invalid / taken names before asking the user to plug in
    // their Ledger and select an address.
    store.check_name_available(&args.name)?;

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
            eprintln!("No accounts. Create one with: aleph account create <NAME>");
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
        // The manifest serializes the kind as "keystore"; user-facing output
        // consistently says "encrypted" (matching create/import/list).
        output["kind"] = serde_json::json!(entry.kind_display());
        if entry.kind == AccountKind::Keystore {
            output["keystore_path"] =
                serde_json::json!(store.keystore_path(&entry.name).display().to_string());
        }
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
        if entry.kind == AccountKind::Keystore {
            eprintln!("File:    {}", store.keystore_path(&entry.name).display());
        }
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
    let (input, address) = match args.address {
        Some(value) => {
            let resolved = resolve_address(&value)
                .map_err(|e| anyhow::anyhow!("failed to resolve '{value}': {e}"))?;
            (value, resolved.to_string())
        }
        None => {
            let name = store.default_account_name()?.ok_or_else(|| {
                anyhow::anyhow!(
                    "no address provided and no default account set; \
                     use: aleph account balance <ADDRESS>"
                )
            })?;
            let addr = store.get_account(&name)?.address;
            (name, addr)
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
        eprintln!(
            "Address: {}",
            format_address(&input, &Address::from(address.clone()))
        );
        eprintln!(
            "ALEPH:   {:.4} (locked: {:.4})",
            balance.aleph_tokens, balance.locked_aleph_tokens
        );
        eprintln!("Credits: {}", format_credits(balance.credits));
    }
    Ok(())
}

fn handle_remove(store: &AccountStore, args: AccountRemoveArgs) -> Result<()> {
    // Verify account exists before prompting
    store.get_account(&args.name)?;

    let warning = format!(
        "Are you sure you want to remove account '{}'? This cannot be undone.",
        args.name
    );
    if !confirm_typed_match(&warning, &args.name, args.yes)
        .context("failed to read confirmation")?
    {
        eprintln!("Aborted.");
        return Ok(());
    }

    store.delete_account(&args.name)?;
    eprintln!("Account '{}' removed.", args.name);
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

fn handle_set(store: &AccountStore, args: AccountSetArgs, json: bool) -> Result<()> {
    if args.chain.is_none() && args.name.is_none() {
        anyhow::bail!("nothing to update: pass --chain and/or --name");
    }

    let target = match args.account {
        Some(n) => n,
        None => store.default_account_name()?.ok_or_else(|| {
            anyhow::anyhow!("no default account set; use: aleph account use <NAME>")
        })?,
    };

    // Verify the account exists before doing anything, for a clean error.
    let before = store.get_account(&target)?;

    // Apply the chain change first (keyed by the current name), then rename.
    if let Some(chain_cli) = args.chain {
        let chain: aleph_types::chain::Chain = chain_cli.into();
        store.set_account_chain(&target, chain)?;
    }

    let final_name = match &args.name {
        Some(new_name) => {
            store.rename_account(&target, new_name)?;
            new_name.clone()
        }
        None => target.clone(),
    };

    let after = store.get_account(&final_name)?;

    if json {
        let output = serde_json::json!({
            "name": after.name,
            "chain": after.chain,
            "address": after.address,
            "renamed_from": (final_name != target).then(|| target.clone()),
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        if before.chain != after.chain {
            eprintln!("Chain: {} -> {}", before.chain, after.chain);
        }
        if final_name != target {
            eprintln!("Name:  {target} -> {final_name}");
        }
        eprintln!("Account '{final_name}' updated.");
    }
    Ok(())
}

fn handle_export(store: &AccountStore, args: AccountExportArgs, json: bool) -> Result<()> {
    let entry = store.get_account(&args.name)?;

    if entry.kind == AccountKind::Ledger {
        anyhow::bail!(
            "cannot export key for ledger account '{}' (the key never leaves the device)",
            args.name
        );
    }

    if !confirm_typed_match(
        "WARNING: This will display your private key in the terminal.",
        "yes",
        args.yes,
    )
    .context("failed to read confirmation")?
    {
        eprintln!("Aborted.");
        return Ok(());
    }

    let key = match entry.kind {
        AccountKind::Local => Zeroizing::new(store.get_private_key(&args.name)?),
        AccountKind::Keystore => {
            use crate::account::{keystore, password};
            let ks_json = store.read_keystore_json(&args.name)?;
            let ks = keystore::parse_keystore(&ks_json)
                .map_err(|e| anyhow::anyhow!("invalid keystore for '{}': {e}", args.name))?;
            let key_bytes = password::unlock_keystore(&ks, &args.name)?;
            Zeroizing::new(hex::encode(&key_bytes[..]))
        }
        AccountKind::Ledger => unreachable!("rejected above"),
    };

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

async fn handle_ssh_command(
    client: &AlephClient,
    ccn_url: &Url,
    command: SshCommand,
    json: bool,
) -> Result<()> {
    match command {
        SshCommand::Add(args) => handle_ssh_add(client, ccn_url, args, json).await,
        SshCommand::List(args) => handle_ssh_list(client, args, json).await,
        SshCommand::Remove(args) => handle_ssh_remove(client, ccn_url, args, json).await,
    }
}

/// Read the key from `--key` or a file path (`-` means stdin), trimmed.
fn read_ssh_key_arg(file: Option<&Path>, key: Option<&str>) -> Result<String> {
    if let Some(k) = key {
        return Ok(k.trim().to_string());
    }
    let path = file.expect("clap ArgGroup guarantees one of file/key is set");
    let content = if path == Path::new("-") {
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .map_err(|e| anyhow!("failed to read SSH public key from stdin: {e}"))?;
        s
    } else {
        std::fs::read_to_string(path).map_err(|e| {
            anyhow!(
                "failed to read SSH public key file '{}': {e}",
                path.display()
            )
        })?
    };
    Ok(content.trim().to_string())
}

async fn handle_ssh_add(
    client: &AlephClient,
    ccn_url: &Url,
    args: SshAddArgs,
    json: bool,
) -> Result<()> {
    let key = read_ssh_key_arg(args.file.as_deref(), args.key.as_deref())?;
    aleph_sdk::ssh::validate_pubkey(&key).map_err(|msg| anyhow!("{msg}"))?;

    let account = resolve_account(&args.signing.identity)?;
    let existing = client.list_ssh_keys(account.address()).await?;

    if existing
        .iter()
        .any(|k| k.label.as_deref() == Some(args.name.as_str()))
    {
        bail!(
            "an SSH key named '{}' already exists. Choose another --name, or remove it first with: \
             aleph account ssh remove {}",
            args.name,
            args.name
        );
    }
    if let Some(dup) = existing.iter().find(|k| k.key == key) {
        bail!(
            "this public key is already registered as '{}'",
            dup.label.as_deref().unwrap_or("(unnamed)")
        );
    }

    let pending = build_add_ssh_key(&account, &key, &args.name)
        .map_err(|e| anyhow!("failed to sign message: {e}"))?;
    submit_or_preview(client, ccn_url, &pending, args.signing.dry_run, json).await
}

async fn handle_ssh_list(client: &AlephClient, args: SshListArgs, json: bool) -> Result<()> {
    let address = resolve_address_or_active(args.address.as_deref())?;
    let keys = client.list_ssh_keys(&address).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&keys)?);
        return Ok(());
    }

    for k in &keys {
        let name = k.label.as_deref().unwrap_or("(unnamed)");
        let preview = ssh_key_preview(&k.key);
        eprintln!(
            "{name}\t{preview}\t{}\t{}",
            k.item_hash,
            k.created.format("%Y-%m-%d")
        );
    }
    Ok(())
}

/// "ssh-ed25519 ...last8" preview of a key for human listing.
fn ssh_key_preview(key: &str) -> String {
    let mut parts = key.split_whitespace();
    let algo = parts.next().unwrap_or("");
    let body = parts.next().unwrap_or("");
    // SSH key bodies are ASCII base64, so byte slicing on the last 8 bytes is safe.
    if body.len() > 8 {
        format!("{algo} ...{}", &body[body.len() - 8..])
    } else {
        format!("{algo} {body}")
    }
}

/// Resolve a remove target (item-hash first, then unique label) to its hash.
fn resolve_ssh_key_target(keys: &[SshKey], target: &str) -> Result<ItemHash> {
    if let Some(k) = keys.iter().find(|k| k.item_hash.to_string() == target) {
        return Ok(k.item_hash.clone());
    }
    let matches: Vec<&SshKey> = keys
        .iter()
        .filter(|k| k.label.as_deref() == Some(target))
        .collect();
    match matches.as_slice() {
        [] => bail!("no SSH key named '{target}'. List your keys with: aleph account ssh list"),
        [one] => Ok(one.item_hash.clone()),
        many => {
            let hashes: Vec<String> = many.iter().map(|k| k.item_hash.to_string()).collect();
            bail!(
                "multiple SSH keys named '{target}'. Remove by item-hash instead, one of: {}",
                hashes.join(", ")
            )
        }
    }
}

async fn handle_ssh_remove(
    client: &AlephClient,
    ccn_url: &Url,
    args: SshRemoveArgs,
    json: bool,
) -> Result<()> {
    let account = resolve_account(&args.signing.identity)?;
    let keys = client.list_ssh_keys(account.address()).await?;
    let hash = resolve_ssh_key_target(&keys, &args.key)?;

    forget_targets(
        client,
        ccn_url,
        json,
        ForgetTargets {
            hashes: vec![hash],
            aggregates: vec![],
            reason: None,
            channel: Some(aleph_sdk::ssh::SSH_CHANNEL.to_string()),
            on_behalf_of: None,
            yes: args.yes,
            confirm_label: "SSH key",
            signing: args.signing,
        },
    )
    .await
}

#[cfg(test)]
mod ssh_tests {
    use super::*;
    use chrono::Utc;

    fn mk(label: Option<&str>, hash: &str) -> SshKey {
        SshKey {
            item_hash: hash.parse().unwrap(),
            key: format!("ssh-ed25519 AAAA{}", label.unwrap_or("x")),
            label: label.map(|s| s.to_string()),
            created: Utc::now(),
        }
    }

    const H1: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const H2: &str = "2222222222222222222222222222222222222222222222222222222222222222";

    #[test]
    fn resolves_unique_label() {
        let keys = vec![mk(Some("laptop"), H1), mk(Some("desktop"), H2)];
        assert_eq!(
            resolve_ssh_key_target(&keys, "laptop").unwrap().to_string(),
            H1
        );
    }

    #[test]
    fn resolves_direct_hash() {
        let keys = vec![mk(Some("laptop"), H1)];
        assert_eq!(resolve_ssh_key_target(&keys, H1).unwrap().to_string(), H1);
    }

    #[test]
    fn errors_on_unknown_label() {
        let keys = vec![mk(Some("laptop"), H1)];
        assert!(resolve_ssh_key_target(&keys, "nope").is_err());
    }

    #[test]
    fn errors_on_ambiguous_label() {
        let keys = vec![mk(Some("dup"), H1), mk(Some("dup"), H2)];
        let err = resolve_ssh_key_target(&keys, "dup")
            .unwrap_err()
            .to_string();
        assert!(err.contains("multiple SSH keys"));
    }

    #[test]
    fn preview_truncates_long_body() {
        let p = ssh_key_preview("ssh-ed25519 AAAAB3NzaC1lZDI1NTE5AAAAIabcdefgh comment");
        assert!(p.starts_with("ssh-ed25519 ..."));
        assert!(p.ends_with("abcdefgh"));
    }
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
