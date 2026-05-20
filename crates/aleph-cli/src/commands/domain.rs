//! `aleph domain` commands. See docs/superpowers/specs/2026-04-27-frontend-pages-design.md.

use crate::cli::{
    DomainAddArgs, DomainAttachArgs, DomainCommand, DomainDetachArgs, DomainKindCli,
    DomainListArgs, DomainRemoveArgs,
};
use crate::common::{
    confirm_tty, format_epoch_for_tty, now_secs_f64, resolve_account, resolve_address,
    resolve_address_or_active, submit_or_preview,
};
use aleph_sdk::aggregate_models::domains::{
    DOMAINS_AGGREGATE_KEY, DomainEntry, DomainOptions, DomainTargetType, DomainsAggregate,
};
use aleph_sdk::aggregate_models::websites::{DEFAULT_IPFS_CATCH_ALL_PATH, WEBSITE_CHANNEL};
use aleph_sdk::client::{AlephAggregateClient, AlephClient};
use aleph_sdk::messages::AggregateBuilder;
use aleph_types::account::Account;
use aleph_types::channel::Channel;
use serde::Serialize;
use url::Url;

pub async fn handle_domain_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: DomainCommand,
) -> anyhow::Result<()> {
    match command {
        DomainCommand::List(args) => handle_domain_list(aleph_client, json, args).await,
        DomainCommand::Add(args) => handle_domain_add(aleph_client, ccn_url, json, args).await,
        DomainCommand::Attach(args) => {
            handle_domain_attach(aleph_client, ccn_url, json, args).await
        }
        DomainCommand::Detach(args) => {
            handle_domain_detach(aleph_client, ccn_url, json, args).await
        }
        DomainCommand::Remove(args) => {
            handle_domain_remove(aleph_client, ccn_url, json, args).await
        }
    }
}

#[derive(Serialize)]
struct DomainListRow {
    domain: String,
    #[serde(rename = "type")]
    kind: DomainTargetType,
    message_id: Option<String>,
    updated_at: f64,
}

async fn handle_domain_list(
    aleph_client: &AlephClient,
    json: bool,
    args: DomainListArgs,
) -> anyhow::Result<()> {
    // Determine the address to inspect: explicit --address, otherwise the
    // active (default) account from the local AccountStore.
    let address = resolve_address_or_active(args.address.as_deref())?;

    let agg: DomainsAggregate = aleph_client.get_domains_aggregate(&address).await?;
    let rows: Vec<DomainListRow> = agg
        .into_iter()
        .filter_map(|(domain, entry)| {
            entry.map(|e| DomainListRow {
                domain,
                kind: e.kind,
                message_id: Some(e.message_id),
                updated_at: e.updated_at,
            })
        })
        .collect();

    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else if rows.is_empty() {
        println!("(no domains)");
    } else {
        println!(
            "{:<32} {:<10} {:<48} UPDATED_AT",
            "DOMAIN", "TYPE", "MESSAGE_ID"
        );
        for row in rows {
            println!(
                "{:<32} {:<10} {:<48} {}",
                row.domain,
                serde_json::to_string(&row.kind)
                    .unwrap_or_default()
                    .trim_matches('"'),
                row.message_id.as_deref().unwrap_or("-"),
                format_epoch_for_tty(row.updated_at)
            );
        }
    }
    Ok(())
}

fn cli_kind(k: DomainKindCli) -> DomainTargetType {
    match k {
        DomainKindCli::Ipfs => DomainTargetType::Ipfs,
        DomainKindCli::Program => DomainTargetType::Program,
        DomainKindCli::Instance => DomainTargetType::Instance,
    }
}

/// Returns `true` if `s` looks like an Aleph item hash: exactly 64 ASCII
/// hex characters. Both lowercase and uppercase are accepted, since
/// `char::is_ascii_hexdigit` is case-insensitive (canonical form is
/// lowercase, but the SDK may accept uppercase).
fn looks_like_item_hash(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Resolve a user-supplied target to a `message_id` value for a `DomainEntry`.
///
/// If `target` looks like an item hash (exactly 64 ASCII hex characters),
/// it is returned as-is. Otherwise it is treated as a website name and looked
/// up in `owner`'s `websites` aggregate; the website's `volume_id` is returned.
async fn resolve_target(
    aleph_client: &AlephClient,
    owner: &aleph_types::chain::Address,
    target: &str,
) -> anyhow::Result<String> {
    if looks_like_item_hash(target) {
        return Ok(target.to_string());
    }
    let websites = aleph_client.get_websites_aggregate(owner).await?;
    websites
        .get(target)
        .and_then(|e| e.as_ref())
        .map(|e| e.volume_id.clone())
        .ok_or_else(|| anyhow::anyhow!("target '{target}' not found in your websites aggregate"))
}

async fn handle_domain_add(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: DomainAddArgs,
) -> anyhow::Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let owner_address = match args.on_behalf_of.as_deref() {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    // Resolve the target: either a raw item-hash-looking string or a website
    // name to look up in the owner address's `websites` aggregate.
    let message_id = resolve_target(aleph_client, &owner_address, &args.target).await?;

    // Pre-flight: refuse if the domain already has a non-null entry and
    // --force was not passed. Check under the owner's aggregate.
    let existing = aleph_client.get_domains_aggregate(&owner_address).await?;
    if let Some(Some(_)) = existing.get(&args.domain)
        && !args.force
    {
        return Err(anyhow::anyhow!(
            "domain '{}' already exists; use --force to overwrite",
            args.domain
        ));
    }

    let kind = cli_kind(args.kind);
    let options = if matches!(kind, DomainTargetType::Ipfs) {
        DomainOptions {
            catch_all_path: Some(
                args.catch_all_path
                    .unwrap_or_else(|| DEFAULT_IPFS_CATCH_ALL_PATH.to_string()),
            ),
        }
    } else {
        DomainOptions::default()
    };

    let entry = DomainEntry {
        kind,
        program_type: kind,
        message_id,
        updated_at: now_secs_f64(),
        options,
    };

    let mut content = serde_json::Map::new();
    content.insert(args.domain.clone(), serde_json::to_value(&entry)?);
    let channel = Channel::from(args.channel.unwrap_or_else(|| WEBSITE_CHANNEL.to_string()));
    let mut builder =
        AggregateBuilder::new(&account, DOMAINS_AGGREGATE_KEY, content).channel(channel);
    if args.on_behalf_of.is_some() {
        builder = builder.on_behalf_of(owner_address);
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

async fn handle_domain_attach(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: DomainAttachArgs,
) -> anyhow::Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let owner_address = match args.on_behalf_of.as_deref() {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    let agg = aleph_client.get_domains_aggregate(&owner_address).await?;
    let mut entry = agg
        .get(&args.domain)
        .and_then(|e| e.clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "domain '{}' not found; use 'aleph domain add' first",
                args.domain
            )
        })?;

    let new_message_id = resolve_target(aleph_client, &owner_address, &args.target).await?;
    entry.message_id = new_message_id;
    entry.updated_at = now_secs_f64();

    let mut content = serde_json::Map::new();
    content.insert(args.domain.clone(), serde_json::to_value(&entry)?);
    let channel = Channel::from(args.channel.unwrap_or_else(|| WEBSITE_CHANNEL.to_string()));
    let mut builder =
        AggregateBuilder::new(&account, DOMAINS_AGGREGATE_KEY, content).channel(channel);
    if args.on_behalf_of.is_some() {
        builder = builder.on_behalf_of(owner_address);
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

async fn handle_domain_detach(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: DomainDetachArgs,
) -> anyhow::Result<()> {
    if !args.yes && !confirm_tty(&format!("Detach domain '{}'?", args.domain))? {
        return Err(anyhow::anyhow!("aborted"));
    }
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let owner_address = match args.on_behalf_of.as_deref() {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    let agg = aleph_client.get_domains_aggregate(&owner_address).await?;
    let mut entry = agg
        .get(&args.domain)
        .and_then(|e| e.clone())
        .ok_or_else(|| anyhow::anyhow!("domain {} not found", args.domain))?;
    entry.message_id.clear();
    entry.updated_at = now_secs_f64();

    let mut content = serde_json::Map::new();
    content.insert(args.domain.clone(), serde_json::to_value(&entry)?);
    let channel = Channel::from(args.channel.unwrap_or_else(|| WEBSITE_CHANNEL.to_string()));
    let mut builder =
        AggregateBuilder::new(&account, DOMAINS_AGGREGATE_KEY, content).channel(channel);
    if args.on_behalf_of.is_some() {
        builder = builder.on_behalf_of(owner_address);
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

async fn handle_domain_remove(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: DomainRemoveArgs,
) -> anyhow::Result<()> {
    if !args.yes && !confirm_tty(&format!("Remove domain '{}' (soft-delete)?", args.domain))? {
        return Err(anyhow::anyhow!("aborted"));
    }
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    let mut content = serde_json::Map::new();
    content.insert(args.domain.clone(), serde_json::Value::Null);
    let channel = Channel::from(args.channel.unwrap_or_else(|| WEBSITE_CHANNEL.to_string()));
    let pending = AggregateBuilder::new(&account, DOMAINS_AGGREGATE_KEY, content)
        .channel(channel)
        .build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

#[cfg(test)]
mod resolve_target_tests {
    use super::looks_like_item_hash;

    #[test]
    fn accepts_64_lowercase_hex() {
        let s = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(s.len(), 64);
        assert!(looks_like_item_hash(s));
    }

    #[test]
    fn accepts_64_uppercase_hex() {
        // `char::is_ascii_hexdigit` accepts uppercase too; document the
        // permissive behavior so callers know mixed-case is fine.
        let s = "0123456789ABCDEF0123456789abcdef0123456789ABCDEF0123456789abcdef";
        assert_eq!(s.len(), 64);
        assert!(looks_like_item_hash(s));
    }

    #[test]
    fn rejects_63_char_hex() {
        let s = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde";
        assert_eq!(s.len(), 63);
        assert!(!looks_like_item_hash(s));
    }

    #[test]
    fn rejects_65_char_hex() {
        let s = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0";
        assert_eq!(s.len(), 65);
        assert!(!looks_like_item_hash(s));
    }

    #[test]
    fn rejects_long_dashed_website_name() {
        let s = "my-long-website-name-that-is-very-long-but-not-hex-format-pad-ok";
        // Includes dashes which are not hex digits; length is irrelevant.
        assert!(!looks_like_item_hash(s));
    }

    #[test]
    fn rejects_long_alphanumeric_website_name() {
        let s = "mysuperlongprojectsitenameforproduction";
        assert_eq!(s.len(), 39);
        assert!(!looks_like_item_hash(s));
    }
}
