use crate::account::generate::generate_key;
use crate::account::store::{AccountKind, AccountStore};
use crate::cli::{
    AccountCommand, AccountCreateArgs, AccountDeleteArgs, AccountExportArgs, AccountImportArgs,
    AccountShowArgs, AccountUseArgs,
};
use aleph_types::account::Account;
use anyhow::{Context, Result};
use zeroize::Zeroizing;

pub fn handle_account_command(command: AccountCommand, json: bool) -> Result<()> {
    let store = AccountStore::open().context("failed to open account store")?;

    match command {
        AccountCommand::Create(args) => handle_create(&store, args, json),
        AccountCommand::Import(args) => handle_import(&store, args, json),
        AccountCommand::List => handle_list(&store, json),
        AccountCommand::Show(args) => handle_show(&store, args, json),
        AccountCommand::Delete(args) => handle_delete(&store, args),
        AccountCommand::Use(args) => handle_use(&store, args, json),
        AccountCommand::Export(args) => handle_export(&store, args, json),
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

    // Existing private-key import flow
    let chain: aleph_types::chain::Chain = args.chain.into();

    let key_hex = match args.private_key {
        Some(k) => k,
        None => match std::env::var("ALEPH_PRIVATE_KEY") {
            Ok(k) => k,
            Err(_) => rpassword::prompt_password("Enter private key (hex): ")
                .context("failed to read private key from stdin")?,
        },
    };

    let key_hex = Zeroizing::new(key_hex.strip_prefix("0x").unwrap_or(&key_hex).to_string());

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

fn handle_list(store: &AccountStore, json: bool) -> Result<()> {
    let manifest = store.load_manifest()?;

    if json {
        println!("{}", serde_json::to_string_pretty(&manifest.accounts)?);
        return Ok(());
    }

    if manifest.accounts.is_empty() {
        eprintln!("No accounts. Create one with: aleph account create --name <NAME>");
        return Ok(());
    }

    eprintln!(
        "{:<2} {:<16} {:<6} {:<48} TYPE",
        "", "NAME", "CHAIN", "ADDRESS"
    );
    for account in &manifest.accounts {
        let marker = if manifest.default.as_deref() == Some(&account.name) {
            "*"
        } else {
            " "
        };
        eprintln!(
            "{:<2} {:<16} {:<6} {:<48} {}",
            marker,
            account.name,
            account.chain,
            account.address,
            account.kind_display()
        );
    }
    Ok(())
}

fn handle_show(store: &AccountStore, args: AccountShowArgs, json: bool) -> Result<()> {
    let name = match args.name {
        Some(n) => n,
        None => store.default_account_name()?.ok_or_else(|| {
            anyhow::anyhow!("no default account set; use: aleph account use <NAME>")
        })?,
    };

    let entry = store.get_account(&name)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&entry)?);
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
