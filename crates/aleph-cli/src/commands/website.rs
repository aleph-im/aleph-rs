//! `aleph website` commands. See docs/superpowers/specs/2026-04-27-frontend-pages-design.md.

use crate::cli::{
    PaymentTypeCli, WebsiteCommand, WebsiteDeleteArgs, WebsiteDeployArgs, WebsiteListArgs,
    WebsiteShowArgs, WebsiteUpdateArgs,
};
use crate::common::{
    confirm_tty, format_epoch_for_tty, now_secs_f64, resolve_account, resolve_address,
    resolve_address_or_active, submit_or_preview,
};
use aleph_sdk::aggregate_models::domains::DomainsAggregate;
use aleph_sdk::aggregate_models::websites::{
    WEBSITE_CHANNEL, WEBSITES_AGGREGATE_KEY, WebsiteEntry, WebsiteMetadata, WebsitePayment,
    WebsitesAggregate,
};
use aleph_sdk::client::{AlephAggregateClient, AlephClient, AlephMessageClient, MessageWithStatus};
use aleph_sdk::messages::{AggregateBuilder, StoreBuilder};
use aleph_types::account::Account;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::MessageContentEnum;
use aleph_types::message::StorageEngine;
use aleph_types::message::execution::base::Payment;
use serde::Serialize;
use url::Url;

/// Resolve a CLI `--payment-type` choice (absent ⇒ credit) to a [`Payment`]
/// suitable for the STORE message. Defaults to credit because holder-tier
/// storage is being phased out; users still pass `--payment-type hold`
/// explicitly when they want it.
fn resolve_payment(choice: Option<PaymentTypeCli>) -> Payment {
    match choice.unwrap_or(PaymentTypeCli::Credit) {
        PaymentTypeCli::Hold => Payment::hold(),
        PaymentTypeCli::Credit => Payment::credits(),
    }
}

/// String form of a payment choice used by the `websites` aggregate's
/// `WebsitePayment.kind` field, which is a free-form `String` in the SDK
/// (see `aggregate_models::websites::WebsitePayment`).
fn payment_kind_str(choice: Option<PaymentTypeCli>) -> &'static str {
    match choice.unwrap_or(PaymentTypeCli::Credit) {
        PaymentTypeCli::Hold => "hold",
        PaymentTypeCli::Credit => "credit",
    }
}

pub async fn handle_website_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: WebsiteCommand,
) -> anyhow::Result<()> {
    match command {
        WebsiteCommand::List(args) => handle_website_list(aleph_client, json, args).await,
        WebsiteCommand::Show(args) => handle_website_show(aleph_client, json, args).await,
        WebsiteCommand::Deploy(args) => {
            handle_website_deploy(aleph_client, ccn_url, json, args).await
        }
        WebsiteCommand::Update(args) => {
            handle_website_update(aleph_client, ccn_url, json, args).await
        }
        WebsiteCommand::Delete(args) => {
            handle_website_delete(aleph_client, ccn_url, json, args).await
        }
    }?;
    Ok(())
}

#[derive(Serialize)]
struct WebsiteListRow {
    name: String,
    version: u64,
    volume_id: String,
    framework: String,
    tags: Vec<String>,
    domains: Vec<String>,
    created_at: f64,
    updated_at: f64,
}

async fn handle_website_list(
    aleph_client: &AlephClient,
    json: bool,
    args: WebsiteListArgs,
) -> anyhow::Result<()> {
    let address = resolve_address_or_active(args.address.as_deref())?;

    let websites: WebsitesAggregate = aleph_client.get_websites_aggregate(&address).await?;
    let domains: DomainsAggregate = aleph_client.get_domains_aggregate(&address).await?;

    // Build a volume_id -> [domain] map.
    let mut by_volume: std::collections::HashMap<&str, Vec<&str>> = Default::default();
    for (d, entry) in domains.iter() {
        if let Some(e) = entry {
            by_volume
                .entry(e.message_id.as_str())
                .or_default()
                .push(d.as_str());
        }
    }

    let rows: Vec<WebsiteListRow> = websites
        .into_iter()
        .filter_map(|(name, entry)| {
            let e = entry?;
            let domains = by_volume
                .get(e.volume_id.as_str())
                .map(|v| v.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default();
            Some(WebsiteListRow {
                name,
                version: e.version,
                volume_id: e.volume_id,
                framework: e.metadata.framework,
                tags: e.metadata.tags,
                domains,
                created_at: e.created_at,
                updated_at: e.updated_at,
            })
        })
        .collect();

    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else if rows.is_empty() {
        println!("(no websites)");
    } else {
        println!(
            "{:<24} {:<7} {:<48} {:<10} DOMAINS",
            "NAME", "VERSION", "VOLUME_ID", "FRAMEWORK"
        );
        for r in rows {
            let domains = if r.domains.is_empty() {
                "-".to_string()
            } else {
                r.domains.join(",")
            };
            println!(
                "{:<24} {:<7} {:<48} {:<10} {}",
                r.name, r.version, r.volume_id, r.framework, domains,
            );
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct WebsiteShowOut {
    name: String,
    version: u64,
    volume_id: String,
    ipfs_cid: Option<String>,
    framework: String,
    tags: Vec<String>,
    payment: serde_json::Value,
    domains: Vec<String>,
    history: std::collections::BTreeMap<String, String>,
    created_at: f64,
    updated_at: f64,
}

async fn handle_website_show(
    aleph_client: &AlephClient,
    json: bool,
    args: WebsiteShowArgs,
) -> anyhow::Result<()> {
    let address = resolve_address_or_active(args.address.as_deref())?;

    let websites: WebsitesAggregate = aleph_client.get_websites_aggregate(&address).await?;
    let entry = websites
        .get(&args.name)
        .and_then(|e| e.as_ref())
        .ok_or_else(|| anyhow::anyhow!("error: website '{}' not found", args.name))?
        .clone();

    // Resolve volume_id -> STORE message -> ipfs_cid (only when storage_engine = ipfs).
    let ipfs_cid = resolve_store_ipfs_cid(aleph_client, &entry.volume_id)
        .await
        .ok();

    let domains: DomainsAggregate = aleph_client.get_domains_aggregate(&address).await?;
    let attached_domains: Vec<String> = domains
        .iter()
        .filter_map(|(d, e)| {
            e.as_ref()
                .filter(|e| e.message_id == entry.volume_id)
                .map(|_| d.clone())
        })
        .collect();

    let out = WebsiteShowOut {
        name: args.name.clone(),
        version: entry.version,
        volume_id: entry.volume_id.clone(),
        ipfs_cid,
        framework: entry.metadata.framework.clone(),
        tags: entry.metadata.tags.clone(),
        payment: serde_json::to_value(&entry.payment)?,
        domains: attached_domains,
        history: entry.history.clone(),
        created_at: entry.created_at,
        updated_at: entry.updated_at,
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Name:       {}", out.name);
        println!("Version:    {}", out.version);
        println!("Volume ID:  {}", out.volume_id);
        if let Some(cid) = &out.ipfs_cid {
            println!("IPFS CID:   {}", cid);
        }
        println!("Framework:  {}", out.framework);
        if !out.tags.is_empty() {
            println!("Tags:       {}", out.tags.join(", "));
        }
        if !out.domains.is_empty() {
            println!("Domains:    {}", out.domains.join(", "));
        }
        println!("Created:    {}", format_epoch_for_tty(out.created_at));
        println!("Updated:    {}", format_epoch_for_tty(out.updated_at));
        if !out.history.is_empty() {
            println!("History:");
            for (v, vol) in &out.history {
                println!("  v{}: {}", v, vol);
            }
        }
    }
    Ok(())
}

/// Resolve a STORE message item_hash to its underlying IPFS CID.
///
/// Fetches the STORE message at `volume_id` via [`AlephMessageClient::get_message`]
/// and inspects its content. Returns the CID string when the message is processed
/// and uses the `ipfs` storage backend. Returns an `Err` for non-IPFS storage
/// (e.g. native Aleph storage), a non-Processed status, or any fetch/parse error.
///
/// Callers are expected to convert `Err` to `None` and render the rest of the
/// summary without an `IPFS CID` line.
async fn resolve_store_ipfs_cid(
    aleph_client: &AlephClient,
    volume_id: &str,
) -> anyhow::Result<String> {
    let item_hash = ItemHash::try_from(volume_id)
        .map_err(|e| anyhow::anyhow!("invalid volume_id '{volume_id}': {e}"))?;
    let with_status = aleph_client.get_message(&item_hash).await?;
    let message = match with_status {
        MessageWithStatus::Processed { message } => message,
        MessageWithStatus::Removing { message, .. }
        | MessageWithStatus::Removed { message, .. } => message,
        other => {
            return Err(anyhow::anyhow!(
                "STORE message '{volume_id}' is not processed (status={})",
                other.status()
            ));
        }
    };
    let store = match message.content.content {
        MessageContentEnum::Store(s) => s,
        _ => {
            return Err(anyhow::anyhow!(
                "message '{volume_id}' is not a STORE message"
            ));
        }
    };
    // StoreContent's `file_hash` field is private; reconstruct via file_hash() and pattern-match
    // on the resulting ItemHash. Equivalent to inspecting StorageBackend directly, but uses
    // the only public accessor available.
    match store.file_hash() {
        ItemHash::Ipfs(cid) => Ok(cid.to_string()),
        ItemHash::Native(_) => Err(anyhow::anyhow!(
            "STORE message '{volume_id}' uses native storage, not IPFS"
        )),
    }
}

#[derive(Serialize)]
struct DeployOut {
    name: String,
    volume_id: String,
    ipfs_cid: String,
    version: u64,
    domains_attached: Vec<String>,
}

/// Upload a folder to IPFS via the CCN's authenticated CAR endpoint and
/// register the matching STORE message in one round-trip.
///
/// Hashes the folder locally to get the root CID, builds and signs a STORE
/// pending message, and (unless `dry_run`) posts the CARv1 stream plus the
/// signed metadata to `/api/v0/ipfs/add_car` on the CCN. Returns
/// `(ipfs_cid_string, store_item_hash_string)`.
async fn upload_folder_and_store(
    aleph_client: &AlephClient,
    account: &crate::account::CliAccount,
    path: &std::path::Path,
    channel: &str,
    payment: Payment,
    on_behalf_of: Option<&aleph_types::chain::Address>,
    dry_run: bool,
) -> anyhow::Result<(String, String)> {
    let opts = aleph_sdk::ipfs::UploadFolderOptions::default();
    let entries = aleph_sdk::ipfs::collect_folder_files(path, opts.follow_symlinks)?;
    let file_hash = aleph_sdk::folder_hash::hash_folder_root(&entries, &opts)?;
    let cid_str = file_hash.to_string();
    let mut builder = StoreBuilder::new(account, file_hash, StorageEngine::Ipfs)
        .channel(Channel::from(channel.to_string()))
        .payment(payment);
    if let Some(owner) = on_behalf_of {
        builder = builder.on_behalf_of(owner.clone());
    }
    let pending_store = builder.build()?;
    let store_hash = pending_store.item_hash.to_string();
    if !dry_run {
        aleph_client
            .upload_folder_to_ipfs_authenticated(path, &pending_store, true, opts)
            .await?;
    }
    Ok((cid_str, store_hash))
}

/// Deploy a static site as an Aleph "website" entry.
///
/// Two paths:
/// * `--volume-id <hash>`: skip upload+STORE and reuse an existing IPFS volume.
///   The CID is best-effort recovered from the STORE message for display only.
/// * default: upload `<path>` directly to the CCN as a CARv1 stream via the
///   authenticated `/api/v0/ipfs/add_car` endpoint (which both pins the folder
///   and registers the STORE in a single request), then write the `websites`
///   aggregate entry.
///
/// In `--json` mode, only the final [`DeployOut`] envelope is emitted on
/// stdout (the inner aggregate submission is silenced so the output is a
/// single parseable JSON document). In `--dry-run` mode the folder is hashed
/// locally to compute the volume_id, but neither the CAR upload nor the
/// aggregate submission is sent; the [`DeployOut`] envelope is the single
/// document representing the dry-run state.
async fn handle_website_deploy(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: WebsiteDeployArgs,
) -> anyhow::Result<()> {
    // 1. Validate name.
    validate_website_name(&args.name)?;
    // 2. Warn (stderr) if uppercase — suppressed in --json mode.
    warn_if_uppercase_name(&args.name, json);
    // 3. Validate folder unless caller is reusing an existing volume.
    if args.volume_id.is_none() {
        validate_folder(&args.path, false)?;
    }

    // 4. Resolve signing account. Capture a single `now` shared by the
    //    website entry and any domain attachments below, so both records
    //    written by this deploy carry the same `updated_at`.
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let owner_address = match args.on_behalf_of.as_deref() {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };
    let now = now_secs_f64();

    // 5. Refuse if name already exists and is non-null. Look up under the
    //    *owner* address (the one the entry will be written to), not the
    //    signer's, so `--on-behalf-of` writes target the right aggregate.
    let existing = aleph_client.get_websites_aggregate(&owner_address).await?;
    if let Some(Some(_)) = existing.get(&args.name) {
        return Err(anyhow::anyhow!(
            "website '{}' already exists; use 'aleph website update' to publish a new version",
            args.name
        ));
    }

    let channel = args
        .channel
        .clone()
        .unwrap_or_else(|| WEBSITE_CHANNEL.to_string());

    // 6. Either reuse the supplied volume or upload + STORE.
    let store_payment = resolve_payment(args.payment_type);
    let (ipfs_cid, volume_id) = if let Some(vid) = args.volume_id.as_ref() {
        // Best-effort: surface the underlying IPFS CID for the user. If the
        // STORE message can't be fetched (not yet processed, network error,
        // not an IPFS-backed STORE...), fall back to an empty string and keep
        // going - the aggregate write only needs the volume_id.
        let cid = resolve_store_ipfs_cid(aleph_client, vid)
            .await
            .unwrap_or_default();
        (cid, vid.clone())
    } else {
        let owner = (args.on_behalf_of.is_some()).then_some(&owner_address);
        upload_folder_and_store(
            aleph_client,
            &account,
            &args.path,
            &channel,
            store_payment,
            owner,
            dry_run,
        )
        .await?
    };

    // 7. Build the websites aggregate entry and submit the partial update.
    let payment = WebsitePayment {
        chain: args
            .payment_chain
            .clone()
            .unwrap_or_else(|| account.chain().to_string()),
        kind: payment_kind_str(args.payment_type).to_string(),
    };
    let entry = WebsiteEntry {
        metadata: WebsiteMetadata {
            name: args.name.clone(),
            tags: args.tag.clone(),
            framework: args.framework.to_string(),
        },
        payment,
        version: 1,
        volume_id: volume_id.clone(),
        history: Default::default(),
        ens: vec![],
        created_at: now,
        updated_at: now,
    };

    let mut content = serde_json::Map::new();
    content.insert(args.name.clone(), serde_json::to_value(&entry)?);
    let mut agg_builder = AggregateBuilder::new(&account, WEBSITES_AGGREGATE_KEY, content)
        .channel(Channel::from(channel.clone()));
    if args.on_behalf_of.is_some() {
        agg_builder = agg_builder.on_behalf_of(owner_address.clone());
    }
    let pending_agg = agg_builder.build()?;
    // Inner submission passes `false` for the `json` flag (see the STORE
    // call above) and is skipped in dry-run for the same single-document
    // reason. If the aggregate write fails after the STORE has already been
    // submitted, surface the volume_id so the user can retry without
    // re-uploading.
    if !dry_run
        && let Err(e) = submit_or_preview(aleph_client, ccn_url, &pending_agg, dry_run, false).await
    {
        eprintln!(
            "warning: STORE submitted (volume_id={volume_id}); aggregate write failed.\n\
             Retry with: aleph website deploy {name} {path} --volume-id {volume_id} ...",
            name = args.name,
            path = args.path.display(),
        );
        return Err(e);
    }

    // 8. Attach domains, if any. A single aggregate POST carries every new
    //    DomainEntry. On failure we keep the deploy successful, blank out the
    //    `domains_attached` list, and tell the user how to retry per-domain.
    //    In --dry-run mode we skip the inner submission for the same
    //    single-document reason as the STORE/aggregate steps above, but we
    //    still populate `domains_attached` so the previewed envelope answers
    //    "what would happen if I deployed this".
    let mut domains_attached: Vec<String> = vec![];
    if !args.domain.is_empty() {
        use aleph_sdk::aggregate_models::domains::{
            DOMAINS_AGGREGATE_KEY, DomainEntry, DomainOptions, DomainTargetType,
        };
        let mut content = serde_json::Map::new();
        for d in &args.domain {
            let entry = DomainEntry {
                kind: DomainTargetType::Ipfs,
                program_type: DomainTargetType::Ipfs,
                message_id: volume_id.clone(),
                updated_at: now,
                options: DomainOptions {
                    catch_all_path: Some(
                        aleph_sdk::aggregate_models::websites::DEFAULT_IPFS_CATCH_ALL_PATH
                            .to_string(),
                    ),
                },
            };
            content.insert(d.clone(), serde_json::to_value(&entry)?);
            domains_attached.push(d.clone());
        }
        let mut dom_builder = AggregateBuilder::new(&account, DOMAINS_AGGREGATE_KEY, content)
            .channel(Channel::from(channel.clone()));
        if args.on_behalf_of.is_some() {
            dom_builder = dom_builder.on_behalf_of(owner_address.clone());
        }
        let pending = dom_builder.build()?;
        if !dry_run
            && let Err(e) = submit_or_preview(aleph_client, ccn_url, &pending, dry_run, false).await
        {
            eprintln!(
                "warning: site deployed (volume_id={volume_id}) but domain attachment failed: {e}"
            );
            eprintln!(
                "retry with: aleph domain add <DOMAIN> --target {name}",
                name = args.name
            );
            domains_attached.clear();
        }
    }

    // 9. Print summary or --json envelope.
    let out = DeployOut {
        name: args.name,
        volume_id,
        ipfs_cid,
        version: 1,
        domains_attached,
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Deployed '{}' v{}", out.name, out.version);
        println!("  volume_id: {}", out.volume_id);
        if !out.ipfs_cid.is_empty() {
            println!("  ipfs_cid:  {}", out.ipfs_cid);
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct UpdateOut {
    name: String,
    old_volume_id: String,
    new_volume_id: String,
    ipfs_cid: String,
    version: u64,
    domains_repointed: Vec<String>,
}

/// Update an existing website with a new folder (or a pre-uploaded volume).
///
/// Reads the current `websites` aggregate entry, builds a new entry with
/// `version + 1`, pushes the previous `(version → volume_id)` pair into
/// `history`, preserves `payment` / `ens` / `created_at` verbatim, and
/// submits a partial aggregate update.
///
/// Two paths mirror `handle_website_deploy`:
/// * `--volume-id <hash>`: skip upload+STORE; CID is best-effort recovered
///   for display only.
/// * default: upload `<path>` directly to the CCN as a CARv1 stream via the
///   authenticated `/api/v0/ipfs/add_car` endpoint (which pins the folder
///   and registers the STORE in one request), then write the aggregate.
///
/// `--idempotent` short-circuits when the new `volume_id` is identical to
/// the existing one: no aggregate write, exit success.
///
/// Same `--json` / `--dry-run` discipline as deploy: the inner aggregate
/// submission passes `false` for `json`; dry-run hashes the folder locally
/// but skips both the CAR upload and the aggregate submission. Only the
/// final [`UpdateOut`] envelope reaches stdout.
///
/// Domain re-pointing is on by default: every domain entry whose
/// `message_id == old.volume_id` is rewritten to point at `new_volume_id`
/// in a single follow-up aggregate POST. `--skip-domain-update` opts out
/// entirely (no aggregate read either). `--domain <D>` flags restrict the
/// re-pointing to that allowlist. The re-pointing step is best-effort:
/// a failure logs a warning and clears `domains_repointed`, but does not
/// fail the command (the website update itself already succeeded).
async fn handle_website_update(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: WebsiteUpdateArgs,
) -> anyhow::Result<()> {
    // 1. Validate name; validate folder unless reusing a volume.
    validate_website_name(&args.name)?;
    warn_if_uppercase_name(&args.name, json);
    if args.volume_id.is_none() {
        validate_folder(&args.path, false)?;
    }

    // 2. Resolve signing account and the effective owner address.
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let owner_address = match args.on_behalf_of.as_deref() {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    // 3. Read aggregate; refuse if entry is missing or null. Look up under
    //    the *owner* address so `--on-behalf-of` updates the right entry.
    let websites: WebsitesAggregate = aleph_client.get_websites_aggregate(&owner_address).await?;
    let old = websites
        .get(&args.name)
        .and_then(|e| e.clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "website '{}' not found; use 'aleph website deploy' to create it",
                args.name
            )
        })?;

    let channel = args
        .channel
        .clone()
        .unwrap_or_else(|| WEBSITE_CHANNEL.to_string());

    // 4. Either reuse the supplied volume or upload + STORE. The new STORE
    //    picks up `--payment-type` (default credit); the existing aggregate
    //    `WebsitePayment` is left intact (see step 6).
    let store_payment = resolve_payment(args.payment_type);
    let (ipfs_cid, new_volume_id) = if let Some(vid) = args.volume_id.as_ref() {
        // Best-effort CID resolution - same semantics as deploy.
        let cid = resolve_store_ipfs_cid(aleph_client, vid)
            .await
            .unwrap_or_default();
        (cid, vid.clone())
    } else {
        let owner = (args.on_behalf_of.is_some()).then_some(&owner_address);
        upload_folder_and_store(
            aleph_client,
            &account,
            &args.path,
            &channel,
            store_payment,
            owner,
            dry_run,
        )
        .await?
    };

    // 5. Idempotent short-circuit: nothing changed → no aggregate write.
    if args.idempotent && new_volume_id == old.volume_id {
        if !json {
            eprintln!(
                "note: folder content unchanged from version {} — skipping aggregate write",
                old.version
            );
        }
        let out = UpdateOut {
            name: args.name,
            old_volume_id: old.volume_id.clone(),
            new_volume_id: old.volume_id,
            ipfs_cid,
            version: old.version,
            domains_repointed: vec![],
        };
        if json {
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
        return Ok(());
    }

    // 6. Build the new entry: bump version, extend history, preserve
    //    payment / ens / created_at; metadata override-able from args.
    let mut history = old.history.clone();
    history.insert(old.version.to_string(), old.volume_id.clone());

    let new_metadata = WebsiteMetadata {
        name: args.name.clone(),
        tags: args
            .tag
            .clone()
            .unwrap_or_else(|| old.metadata.tags.clone()),
        framework: args
            .framework
            .map(|f| f.to_string())
            .unwrap_or_else(|| old.metadata.framework.clone()),
    };
    let new_entry = WebsiteEntry {
        metadata: new_metadata,
        payment: old.payment.clone(),
        version: old.version + 1,
        volume_id: new_volume_id.clone(),
        history,
        ens: old.ens.clone(),
        created_at: old.created_at,
        updated_at: now_secs_f64(),
    };

    // 7. Submit the partial aggregate update.
    let mut content = serde_json::Map::new();
    content.insert(args.name.clone(), serde_json::to_value(&new_entry)?);
    let mut agg_builder = AggregateBuilder::new(&account, WEBSITES_AGGREGATE_KEY, content)
        .channel(Channel::from(channel.clone()));
    if args.on_behalf_of.is_some() {
        agg_builder = agg_builder.on_behalf_of(owner_address.clone());
    }
    let pending_agg = agg_builder.build()?;
    // Inner submission passes `false` for `json` and is skipped in dry-run
    // (same single-document discipline as deploy). On failure after a STORE
    // succeeded, surface the new volume_id so the user can retry without
    // re-uploading.
    if !dry_run
        && let Err(e) = submit_or_preview(aleph_client, ccn_url, &pending_agg, dry_run, false).await
    {
        eprintln!(
            "warning: STORE submitted (volume_id={new_volume_id}); aggregate write failed.\n\
             Retry with: aleph website update {name} {path} --volume-id {new_volume_id} ...",
            name = args.name,
            path = args.path.display(),
        );
        return Err(e);
    }

    // 8. Domain re-pointing.
    //    Default: read the `domains` aggregate, find every entry whose
    //    `message_id == old.volume_id`, and rewrite each in a single
    //    aggregate POST so they point at `new_volume_id`. Optionally
    //    restricted to `--domain <D>` flags (allowlist).
    //    `--skip-domain-update` short-circuits before the read so we
    //    don't burn an HTTP call we don't need.
    //    Best-effort: failure here logs a warning and clears the list,
    //    but we don't return Err — the website update itself succeeded.
    //    Same `--json` / `--dry-run` discipline as the other inner
    //    submissions (pass `false` for `json`, skip the actual submit
    //    in dry-run while still populating the envelope).
    let mut domains_repointed: Vec<String> = vec![];
    if !args.skip_domain_update {
        use aleph_sdk::aggregate_models::domains::{DOMAINS_AGGREGATE_KEY, DomainEntry};
        let domains = aleph_client.get_domains_aggregate(&owner_address).await?;
        let now = now_secs_f64();
        let mut content = serde_json::Map::new();
        let allowlist: Option<std::collections::HashSet<&String>> = if args.domain.is_empty() {
            None
        } else {
            Some(args.domain.iter().collect())
        };
        for (name, entry) in domains.iter() {
            let Some(e) = entry else { continue };
            if e.message_id != old.volume_id {
                continue;
            }
            if let Some(ref allow) = allowlist
                && !allow.contains(name)
            {
                continue;
            }
            let mut updated: DomainEntry = e.clone();
            updated.message_id = new_volume_id.clone();
            updated.updated_at = now;
            content.insert(name.clone(), serde_json::to_value(&updated)?);
            domains_repointed.push(name.clone());
        }
        if !content.is_empty() {
            let mut dom_builder = AggregateBuilder::new(&account, DOMAINS_AGGREGATE_KEY, content)
                .channel(Channel::from(channel.clone()));
            if args.on_behalf_of.is_some() {
                dom_builder = dom_builder.on_behalf_of(owner_address.clone());
            }
            let pending = dom_builder.build()?;
            if !dry_run
                && let Err(e) =
                    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, false).await
            {
                eprintln!(
                    "warning: site updated but domain re-pointing failed: {e}\n\
                     retry per-domain with: aleph domain attach <D> --to {}",
                    args.name
                );
                domains_repointed.clear();
            }
        }
        if !domains_repointed.is_empty() {
            eprintln!("Re-pointed domains: {}", domains_repointed.join(", "));
        }
    }

    // 9. Print summary or --json envelope.
    let out = UpdateOut {
        name: args.name,
        old_volume_id: old.volume_id,
        new_volume_id,
        ipfs_cid,
        version: new_entry.version,
        domains_repointed,
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Updated '{}' to v{} (volume_id={})",
            out.name, out.version, out.new_volume_id
        );
        if !out.ipfs_cid.is_empty() {
            println!("  ipfs_cid:  {}", out.ipfs_cid);
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct DeleteOut {
    name: String,
    deleted: bool,
    orphaned_volume_id: String,
    orphaned_domains: Vec<String>,
}

/// Soft-delete a website by writing `null` over its `websites` aggregate entry.
///
/// Read-modify-check before submission:
/// 1. Confirm on TTY unless `--yes` was passed; non-TTY without `--yes` is a
///    clean error rather than a silent skip.
/// 2. Refuse if `<name>` is missing or already null.
/// 3. Read the `domains` aggregate and collect every domain whose
///    `message_id == entry.volume_id` — those will be orphaned by the delete
///    (they keep pointing at a STORE message no `websites` entry references).
///    The list is surfaced in the output so the user knows what to clean up
///    next with `aleph domain detach` / `aleph domain remove`.
///
/// `--json` discipline: inner submission passes `false` so the only JSON
/// document on stdout is the final [`DeleteOut`] envelope. `--dry-run` skips
/// the inner submission entirely; the envelope itself answers "what would
/// happen if I deleted this".
async fn handle_website_delete(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: WebsiteDeleteArgs,
) -> anyhow::Result<()> {
    if !args.yes
        && !confirm_tty(&format!(
            "Delete website '{}'? This is a soft-delete.",
            args.name
        ))?
    {
        return Err(anyhow::anyhow!("aborted"));
    }
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;
    let owner_address = match args.on_behalf_of.as_deref() {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    let websites: WebsitesAggregate = aleph_client.get_websites_aggregate(&owner_address).await?;
    let entry = websites
        .get(&args.name)
        .and_then(|e| e.clone())
        .ok_or_else(|| anyhow::anyhow!("website '{}' not found", args.name))?;

    let domains: DomainsAggregate = aleph_client.get_domains_aggregate(&owner_address).await?;
    let orphaned_domains: Vec<String> = domains
        .iter()
        .filter_map(|(d, e)| {
            e.as_ref()
                .filter(|e| e.message_id == entry.volume_id)
                .map(|_| d.clone())
        })
        .collect();

    let channel = args
        .channel
        .clone()
        .unwrap_or_else(|| WEBSITE_CHANNEL.to_string());
    let mut content = serde_json::Map::new();
    content.insert(args.name.clone(), serde_json::Value::Null);
    let mut builder = AggregateBuilder::new(&account, WEBSITES_AGGREGATE_KEY, content)
        .channel(Channel::from(channel));
    if args.on_behalf_of.is_some() {
        builder = builder.on_behalf_of(owner_address);
    }
    let pending = builder.build()?;
    if !dry_run {
        submit_or_preview(aleph_client, ccn_url, &pending, dry_run, false).await?;
    }

    let out = DeleteOut {
        name: args.name,
        deleted: true,
        orphaned_volume_id: entry.volume_id,
        orphaned_domains,
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Deleted '{}'", out.name);
        if !out.orphaned_domains.is_empty() {
            println!("Domains still pointing at the orphaned volume:");
            for d in &out.orphaned_domains {
                println!("  - {}", d);
            }
            println!("Use 'aleph domain detach' or 'aleph domain remove' to clean up.");
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum WebsiteNameError {
    #[error("website name cannot be empty")]
    Empty,
    #[error("website name '{0}' contains invalid characters (whitespace or '.')")]
    InvalidChars(String),
}

pub fn validate_website_name(name: &str) -> Result<(), WebsiteNameError> {
    if name.trim().is_empty() {
        return Err(WebsiteNameError::Empty);
    }
    if name.contains('.') || name.chars().any(|c| c.is_whitespace()) {
        return Err(WebsiteNameError::InvalidChars(name.to_string()));
    }
    Ok(())
}

/// Emit a stderr warning when the name has uppercase characters.
/// Does not reject — just nudges users toward kebab-case to match the dashboard.
/// Suppressed when `json` is true so it can't leak into machine-readable streams.
#[allow(dead_code)] // wired up in later tasks
pub fn warn_if_uppercase_name(name: &str, json: bool) {
    if !json && name.chars().any(|c| c.is_uppercase()) {
        eprintln!(
            "warning: website name '{name}' contains uppercase characters; \
             dashboard convention is lowercase-with-dashes"
        );
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FolderValidationError {
    #[error("path is not a directory: {0}")]
    NotADirectory(String),
    #[error("folder is empty: {0}")]
    Empty(String),
    #[error("folder contains a symlink at {0}; pass --allow-symlinks to opt in")]
    SymlinkInFolder(String),
}

#[allow(dead_code)] // wired up in later tasks
pub fn validate_folder(
    path: &std::path::Path,
    allow_symlinks: bool,
) -> Result<(), FolderValidationError> {
    if !path.is_dir() {
        return Err(FolderValidationError::NotADirectory(
            path.display().to_string(),
        ));
    }
    let mut count = 0usize;
    for entry in walkdir::WalkDir::new(path) {
        let entry = entry.map_err(|e| {
            FolderValidationError::NotADirectory(format!("{} ({})", path.display(), e))
        })?;
        if entry.file_type().is_symlink() && !allow_symlinks {
            return Err(FolderValidationError::SymlinkInFolder(
                entry.path().display().to_string(),
            ));
        }
        if entry.file_type().is_file() {
            count += 1;
        }
    }
    if count == 0 {
        return Err(FolderValidationError::Empty(path.display().to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod folder_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn rejects_non_directory() {
        let d = tempdir().unwrap();
        let f = d.path().join("file.txt");
        std::fs::write(&f, "x").unwrap();
        assert!(matches!(
            validate_folder(&f, false),
            Err(FolderValidationError::NotADirectory(_))
        ));
    }

    #[test]
    fn rejects_empty_folder() {
        let d = tempdir().unwrap();
        assert!(matches!(
            validate_folder(d.path(), false),
            Err(FolderValidationError::Empty(_))
        ));
    }

    #[test]
    fn accepts_folder_with_files() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("a.html"), "<h1>hi</h1>").unwrap();
        assert!(validate_folder(d.path(), false).is_ok());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_name() {
        assert!(matches!(
            validate_website_name(""),
            Err(WebsiteNameError::Empty)
        ));
        assert!(matches!(
            validate_website_name("   "),
            Err(WebsiteNameError::Empty)
        ));
    }

    #[test]
    fn rejects_dots_and_whitespace() {
        assert!(validate_website_name("foo.bar").is_err());
        assert!(validate_website_name("foo bar").is_err());
        assert!(validate_website_name("foo\tbar").is_err());
    }

    #[test]
    fn accepts_kebab_case() {
        assert!(validate_website_name("my-site").is_ok());
        assert!(validate_website_name("my_site_2").is_ok());
        assert!(validate_website_name("MySite").is_ok()); // warned, not rejected
    }

    #[test]
    fn warn_if_uppercase_name_does_not_panic() {
        // Both paths exercised — the warning side-effects to stderr but is
        // suppressed under `--json`. We don't capture stderr here; this just
        // proves the function compiles, signature is correct, and neither
        // branch panics.
        warn_if_uppercase_name("MySite", false);
        warn_if_uppercase_name("MySite", true);
        warn_if_uppercase_name("my-site", false);
        warn_if_uppercase_name("my-site", true);
    }
}
