use crate::cli::{CcnAddArgs, CcnCommand, CcnRemoveArgs, CcnShowArgs, CcnUseArgs, ConfigCommand};
use crate::config::store::ConfigStore;
use anyhow::{Context, Result};

pub async fn handle_config_command(command: ConfigCommand, json: bool) -> Result<()> {
    match command {
        ConfigCommand::Ccn {
            command: ccn_command,
        } => handle_ccn_command(ccn_command, json).await,
    }
}

async fn handle_ccn_command(command: CcnCommand, json: bool) -> Result<()> {
    let store = ConfigStore::open().context("failed to open config store")?;

    match command {
        CcnCommand::Add(args) => handle_add(&store, args, json),
        CcnCommand::Use(args) => handle_use(&store, args, json),
        CcnCommand::List => handle_list(&store, json),
        CcnCommand::Show(args) => handle_show(&store, args, json).await,
        CcnCommand::Remove(args) => handle_remove(&store, args, json),
    }
}

fn handle_add(store: &ConfigStore, args: CcnAddArgs, json: bool) -> Result<()> {
    store.add_ccn(&args.name, &args.url)?;

    if json {
        let output = serde_json::json!({
            "name": args.name,
            "url": args.url,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("CCN '{}' added.", args.name);
        eprintln!("  URL: {}", args.url);
    }
    Ok(())
}

fn handle_use(store: &ConfigStore, args: CcnUseArgs, json: bool) -> Result<()> {
    store.set_default_ccn(&args.name)?;

    if json {
        let output = serde_json::json!({ "default_ccn": args.name });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Default CCN set to '{}'.", args.name);
    }
    Ok(())
}

fn handle_list(store: &ConfigStore, json: bool) -> Result<()> {
    let manifest = store.load_manifest()?;

    if json {
        println!("{}", serde_json::to_string_pretty(&manifest.ccns)?);
        return Ok(());
    }

    eprintln!("{:<2} {:<16} URL", "", "NAME");
    for ccn in &manifest.ccns {
        let marker = if manifest.default_ccn.as_deref() == Some(&ccn.name) {
            "*"
        } else {
            " "
        };
        eprintln!("{:<2} {:<16} {}", marker, ccn.name, ccn.url);
    }
    Ok(())
}

/// Fetch the CCN version from GET /api/v0/version.
/// Returns None on any error (timeout, network, parse).
async fn fetch_ccn_version(url: &str) -> Option<String> {
    let version_url = format!("{}/api/v0/version", url.trim_end_matches('/'));
    let resp = reqwest::Client::new()
        .get(&version_url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .ok()?;
    let body: serde_json::Value = resp.json().await.ok()?;
    body["version"].as_str().map(|s| s.to_string())
}

async fn handle_show(store: &ConfigStore, args: CcnShowArgs, json: bool) -> Result<()> {
    let name = match args.name {
        Some(n) => n,
        None => store.default_ccn_name()?.ok_or_else(|| {
            anyhow::anyhow!("no default CCN set; use: aleph config ccn use <NAME>")
        })?,
    };

    let entry = store.get_ccn(&name)?;
    let is_default = store.default_ccn_name().ok().flatten().as_deref() == Some(name.as_str());
    let version = fetch_ccn_version(&entry.url).await;

    if json {
        let mut output = serde_json::json!({
            "name": entry.name,
            "url": entry.url,
        });
        if let Some(v) = &version {
            output["version"] = serde_json::json!(v);
        }
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("Name:    {}", entry.name);
        eprintln!("URL:     {}", entry.url);
        match &version {
            Some(v) => eprintln!("Version: {v}"),
            None => eprintln!("Version: (unreachable)"),
        }
        if is_default {
            eprintln!("Default: yes");
        }
    }
    Ok(())
}

fn handle_remove(store: &ConfigStore, args: CcnRemoveArgs, json: bool) -> Result<()> {
    store.remove_ccn(&args.name)?;

    if json {
        let output = serde_json::json!({ "removed": args.name });
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        eprintln!("CCN '{}' removed.", args.name);
    }
    Ok(())
}
