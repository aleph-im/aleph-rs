use crate::cli::{
    CrnArgs, PaymentTypeCli, ProgramCommand, ProgramCreateArgs, ProgramDeleteArgs, ProgramListArgs,
    ProgramLogsArgs, ProgramPersistArgs, ProgramUpdateArgs, StorageEngineCli,
};
use crate::commands::instance::{
    parse_ephemeral_volumes, parse_immutable_volumes, parse_persistent_volumes,
};
use crate::common::{
    confirm_action, print_submission_result, resolve_account, resolve_address, submit_or_preview,
};
use crate::program::archive::prepare_archive;
use aleph_sdk::client::{
    AlephAggregateClient, AlephClient, AlephMessageClient, AlephStorageClient, MessageError,
    MessageFilter, MessageWithStatus, hash_file,
};
use aleph_sdk::messages::{ForgetBuilder, ProgramBuilder, StoreBuilder};
use aleph_sdk::verify::Hasher;
use aleph_types::account::Account;
use aleph_types::chain::Address;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::base::{Encoding, Payment};
use aleph_types::message::execution::volume::MachineVolume;
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::{Message, MessageContentEnum, MessageType, StorageEngine};
use aleph_types::timestamp::Timestamp;
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use memsizes::MiB;
use std::collections::HashMap;
use url::Url;

pub async fn handle_program_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: ProgramCommand,
) -> Result<()> {
    match command {
        ProgramCommand::Create(args) => handle_create(aleph_client, ccn_url, json, args).await,
        ProgramCommand::List(args) => handle_list(aleph_client, json, args).await,
        ProgramCommand::Delete(args) => handle_delete(aleph_client, ccn_url, json, args).await,
        ProgramCommand::Update(args) => handle_update(aleph_client, ccn_url, json, args).await,
        ProgramCommand::Persist(args) => {
            handle_persist_or_unpersist(aleph_client, ccn_url, json, args, true).await
        }
        ProgramCommand::Unpersist(args) => {
            handle_persist_or_unpersist(aleph_client, ccn_url, json, args, false).await
        }
        ProgramCommand::Logs(args) => handle_logs(json, args).await,
    }
}

async fn handle_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: ProgramCreateArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    // 1. Archive
    let (archive, encoding) = prepare_archive(&args.path)?;

    // 2. Storage engine + payment
    let storage_engine = match args.storage_engine {
        StorageEngineCli::Storage => StorageEngine::Storage,
        StorageEngineCli::Ipfs => StorageEngine::Ipfs,
    };
    let payment = match args.payment_type {
        PaymentTypeCli::Hold => Payment::hold(),
        PaymentTypeCli::Credit => Payment::credits(),
    };

    // 3. Hash
    if !json {
        eprintln!("Hashing {}...", archive.path().display());
    }
    let file_hash = match storage_engine {
        StorageEngine::Storage => hash_file(archive.path(), Hasher::for_storage()).await?,
        StorageEngine::Ipfs => hash_file(archive.path(), Hasher::for_ipfs()).await?,
    };
    if !json {
        eprintln!("  Code hash: {file_hash}");
    }

    // 4. Build STORE
    let mut store_builder =
        StoreBuilder::new(&account, file_hash.clone(), storage_engine).payment(payment.clone());
    if let Some(owner) = &args.on_behalf_of {
        store_builder = store_builder.on_behalf_of(resolve_address(owner)?);
    }
    if let Some(ch) = &args.channel {
        store_builder = store_builder.channel(Channel::from(ch.clone()));
    }
    let store_pending = store_builder.build()?;

    // 5. Resolve resources (size slug or granular flags)
    let (vcpus, memory_mib) =
        resolve_program_resources(aleph_client, args.size.as_deref(), args.vcpus, args.memory)
            .await?;

    // 6. Build PROGRAM. `code.ref` is the STORE message's item_hash, not the
    // raw file hash - the VM supervisor resolves it by looking up the STORE
    // and following its embedded file reference.
    let mut program_builder = ProgramBuilder::new(
        &account,
        store_pending.item_hash.clone(),
        args.entrypoint.clone(),
        args.runtime.clone(),
    )
    .encoding(encoding)
    .internet(args.internet)
    .persistent(args.persistent)
    .allow_amend(args.updatable)
    .timeout_seconds(args.timeout_seconds)
    .vcpus(vcpus)
    .memory(MiB::from(memory_mib))
    .payment(payment);

    if let Some(name) = &args.name {
        let mut metadata = HashMap::new();
        metadata.insert("name".into(), serde_json::Value::String(name.clone()));
        program_builder = program_builder.metadata(metadata);
    }
    if let Some(env_str) = &args.env_vars {
        program_builder = program_builder.variables(parse_env_vars(env_str)?);
    }

    let mut volumes: Vec<MachineVolume> = Vec::new();
    if let Some(specs) = &args.persistent_volume {
        volumes.extend(parse_persistent_volumes(specs)?);
    }
    if let Some(specs) = &args.ephemeral_volume {
        volumes.extend(parse_ephemeral_volumes(specs)?);
    }
    if let Some(specs) = &args.immutable_volume {
        volumes.extend(parse_immutable_volumes(specs)?);
    }
    if !volumes.is_empty() {
        program_builder = program_builder.volumes(volumes);
    }

    if let Some(owner) = &args.on_behalf_of {
        program_builder = program_builder.on_behalf_of(resolve_address(owner)?);
    }
    if let Some(ch) = &args.channel {
        program_builder = program_builder.channel(Channel::from(ch.clone()));
    }

    let program_pending = program_builder.build()?;

    // 7. Dry run: print both envelopes and stop. submit_or_preview prints a
    // single envelope; for create we want STORE first then PROGRAM, so we
    // emit them inline and skip both submission paths.
    if dry_run {
        if json {
            let envelopes = serde_json::json!([store_pending, program_pending]);
            println!("{}", serde_json::to_string_pretty(&envelopes)?);
        } else {
            eprintln!("Dry run - messages not submitted.\n");
            println!("{}", serde_json::to_string_pretty(&store_pending)?);
            println!("{}", serde_json::to_string_pretty(&program_pending)?);
        }
        return Ok(());
    }

    // 8. Upload code archive (mirrors handle_single_file_upload in commands/file.rs)
    if !json {
        eprintln!("Uploading code archive...");
    }
    match storage_engine {
        StorageEngine::Storage => {
            aleph_client
                .upload_file_to_storage(archive.path(), Some(&store_pending), true)
                .await?;
            print_submission_result(ccn_url, &store_pending, "success", "processed", json)?;
        }
        StorageEngine::Ipfs => {
            aleph_client
                .upload_file_to_ipfs(archive.path(), Some(&store_pending), true)
                .await?;
            print_submission_result(ccn_url, &store_pending, "success", "processed", json)?;
        }
    }

    // 9. Submit PROGRAM
    if !json {
        eprintln!("Publishing program message...");
    }
    submit_or_preview(aleph_client, ccn_url, &program_pending, false, json).await?;

    if !json && let Some((host, path)) = vm_run_urls(&program_pending.item_hash) {
        eprintln!("Try it at:");
        eprintln!("  {host}");
        eprintln!("  {path}");
    }

    Ok(())
}

/// Build the two aleph.sh URLs that route to a CRN serving the program.
///
/// Mirrors aleph-client (Python): `https://aleph.sh/vm/{hex}` and
/// `https://{base32}.aleph.sh`, where `base32` is the lowercased
/// no-padding base32 of the 32 raw hash bytes. Only emitted for native
/// hashes; IPFS-stored content has no matching subdomain form.
fn vm_run_urls(item_hash: &ItemHash) -> Option<(String, String)> {
    match item_hash {
        ItemHash::Native(hash) => {
            let hex = hash.to_string();
            let host = format!("https://{}.aleph.sh", base32_lower_nopad(hash.as_bytes()));
            let path = format!("https://aleph.sh/vm/{hex}");
            Some((host, path))
        }
        ItemHash::Ipfs(_) => None,
    }
}

/// RFC 4648 base32 (lowercase, no padding). 32 input bytes -> 52 chars.
fn base32_lower_nopad(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let mut buffer: u32 = 0;
    let mut bits_in_buffer: u32 = 0;
    for &byte in bytes {
        buffer = (buffer << 8) | byte as u32;
        bits_in_buffer += 8;
        while bits_in_buffer >= 5 {
            bits_in_buffer -= 5;
            let idx = ((buffer >> bits_in_buffer) & 0x1f) as usize;
            out.push(ALPHABET[idx] as char);
        }
    }
    if bits_in_buffer > 0 {
        let idx = ((buffer << (5 - bits_in_buffer)) & 0x1f) as usize;
        out.push(ALPHABET[idx] as char);
    }
    out
}

/// Resolve (vcpus, memory_mib) from either a `--size <slug>` lookup against
/// the CCN's pricing aggregate or from explicit `--vcpus` / `--memory` flags.
///
/// Programs and instances share compute-unit shape, so we reuse the instance
/// pricing tiers for slug resolution. Per-program pricing is computed
/// separately and isn't relevant for sizing.
async fn resolve_program_resources(
    aleph_client: &AlephClient,
    size: Option<&str>,
    vcpus: Option<u32>,
    memory_mib: Option<u64>,
) -> Result<(u32, u64)> {
    if let Some(slug) = size {
        let pricing = aleph_client
            .get_pricing_aggregate()
            .await
            .map_err(|e| anyhow::anyhow!("failed to fetch pricing tiers: {e}"))?;
        let tier = pricing
            .pricing
            .instance
            .find_tier_by_slug(slug)
            .ok_or_else(|| {
                let available = pricing.pricing.instance.available_slugs().join(", ");
                anyhow::anyhow!("unknown size '{slug}'. Available: {available}")
            })?;
        let v = vcpus.unwrap_or(tier.vcpus);
        let m = memory_mib.unwrap_or(tier.memory_mib);
        return Ok((v, m));
    }
    let v = vcpus.context("--size or --vcpus must be specified")?;
    let m = memory_mib.context("--size or --memory must be specified")?;
    Ok((v, m))
}

/// Parse a comma-separated `KEY=value` list into a map.
fn parse_env_vars(s: &str) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for piece in s.split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let (k, v) = piece
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("invalid env var `{piece}`, expected KEY=value"))?;
        out.insert(k.trim().to_string(), v.trim().to_string());
    }
    Ok(out)
}

/// One row of `aleph program list` output, extracted from a PROGRAM message.
#[derive(Debug, Clone)]
struct ProgramRow {
    item_hash: ItemHash,
    name: Option<String>,
    sender: Address,
    persistent: bool,
    internet: bool,
    updatable: bool,
    vcpus: u32,
    memory_mib: u64,
    runtime: ItemHash,
    created_at: Timestamp,
}

/// Convert a PROGRAM message into a row. Returns `None` for non-program
/// messages (defensive - callers already filter by `MessageType::Program`,
/// but the CCN can occasionally return a mis-typed payload).
fn extract_program_row(message: &Message) -> Option<ProgramRow> {
    let MessageContentEnum::Program(program) = message.content() else {
        return None;
    };
    let name = program
        .base
        .metadata
        .as_ref()
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    Some(ProgramRow {
        item_hash: message.item_hash.clone(),
        name,
        sender: message.sender.clone(),
        persistent: program.on.persistent.unwrap_or(false),
        internet: program.environment.internet,
        updatable: program.base.allow_amend,
        vcpus: program.base.resources.vcpus,
        memory_mib: u64::from(program.base.resources.memory),
        runtime: program.runtime.reference.clone(),
        created_at: message.content.time.clone(),
    })
}

/// Fetch all PROGRAM rows for `address`. Programs do not have an `owner`
/// distinct from `sender` (instances do, because they may be paid for by
/// another account), so a single sender filter is sufficient.
async fn fetch_program_rows(
    aleph_client: &AlephClient,
    address: &Address,
) -> Result<Vec<ProgramRow>> {
    let filter = MessageFilter {
        message_type: Some(MessageType::Program),
        addresses: Some(vec![address.clone()]),
        ..Default::default()
    };
    let mut rows = Vec::new();
    let mut stream = Box::pin(aleph_client.get_messages_iterator(filter, None));
    while let Some(message) = stream.next().await {
        let message = message?;
        if let Some(row) = extract_program_row(&message) {
            rows.push(row);
        } else {
            eprintln!(
                "warning: skipping message {} with non-program content",
                message.item_hash
            );
        }
    }
    // Newest first: sort by content.time descending.
    rows.sort_by(|a, b| {
        b.created_at
            .as_f64()
            .partial_cmp(&a.created_at.as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(rows)
}

const MISSING_VALUE: &str = "-";

fn format_program_rows_json(rows: &[ProgramRow]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "item_hash": r.item_hash.to_string(),
                "name": r.name,
                "sender": r.sender.to_string(),
                "persistent": r.persistent,
                "internet": r.internet,
                "updatable": r.updatable,
                "vcpus": r.vcpus,
                "memory_mib": r.memory_mib,
                "runtime": r.runtime.to_string(),
                "created_at": r.created_at
                    .to_datetime()
                    .ok()
                    .map(|dt| dt.to_rfc3339()),
            })
        })
        .collect();
    serde_json::Value::Array(items)
}

fn format_program_rows_text(rows: &[ProgramRow]) -> String {
    use std::fmt::Write;

    let hash_w = rows
        .iter()
        .map(|r| r.item_hash.to_string().len())
        .chain(std::iter::once("ITEM HASH".len()))
        .max()
        .unwrap_or("ITEM HASH".len());
    let name_w = rows
        .iter()
        .map(|r| r.name.as_deref().unwrap_or(MISSING_VALUE).len())
        .chain(std::iter::once("NAME".len()))
        .max()
        .unwrap_or("NAME".len());

    let mut out = String::new();
    writeln!(
        out,
        "{:<hash_w$}  {:<name_w$}  {:>5}  {:>9}  {:>10}  {:>8}  {:>9}",
        "ITEM HASH",
        "NAME",
        "VCPUS",
        "MEMORY",
        "PERSISTENT",
        "INTERNET",
        "UPDATABLE",
        hash_w = hash_w,
        name_w = name_w,
    )
    .expect("writing to String cannot fail");

    for row in rows {
        let name = row.name.as_deref().unwrap_or(MISSING_VALUE);
        writeln!(
            out,
            "{:<hash_w$}  {:<name_w$}  {:>5}  {:>9}  {:>10}  {:>8}  {:>9}",
            row.item_hash,
            name,
            row.vcpus,
            format!("{} MiB", row.memory_mib),
            row.persistent,
            row.internet,
            row.updatable,
            hash_w = hash_w,
            name_w = name_w,
        )
        .expect("writing to String cannot fail");
    }
    out
}

fn render_program_rows(rows: &[ProgramRow], json: bool) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&format_program_rows_json(rows))?
        );
    } else {
        print!("{}", format_program_rows_text(rows));
    }
    Ok(())
}

async fn handle_list(aleph_client: &AlephClient, json: bool, args: ProgramListArgs) -> Result<()> {
    let address = match args.address.as_deref() {
        Some(value) => resolve_address(value)?,
        None => {
            // Fall back to the current default signing account's address.
            // No --private-key is passed so chain is unused.
            let identity = crate::cli::IdentityArgs {
                account: None,
                private_key: None,
                chain: None,
            };
            let account = resolve_account(&identity)?;
            account.address().clone()
        }
    };
    let rows = fetch_program_rows(aleph_client, &address).await?;
    render_program_rows(&rows, json)
}

async fn handle_update(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: ProgramUpdateArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    // Resolve the effective owner: the address whose program we're updating
    // and whose name the new STORE will be signed in. When --on-behalf-of is
    // set this differs from the signing account (delegated authoring).
    let owner_address = match &args.on_behalf_of {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    // 1. Fetch the existing program message and verify ownership / amend flag.
    // Compare against `owner()` (= content.address), not `sender`: when the
    // program was created via delegation, `sender` is the delegate's key.
    let program = fetch_program_message(aleph_client, &args.item_hash).await?;
    if program.owner() != &owner_address {
        bail!(
            "address {owner_address} does not own program {} (owner: {})",
            args.item_hash,
            program.owner()
        );
    }
    let MessageContentEnum::Program(program_content) = program.content() else {
        bail!("expected PROGRAM message, got {:?}", program.message_type);
    };
    if !program_content.base.allow_amend {
        bail!(
            "program {} is not updatable; was it created without --updatable?",
            args.item_hash
        );
    }
    let original_code_ref = program_content.code.reference.clone();

    // 2. Prepare the new archive and ensure its encoding matches the existing program.
    let (archive, encoding) = prepare_archive(&args.path)?;
    check_update_encoding(&program, encoding.clone())?;

    // 3. Storage engine is derived from the original code's hash variant: an
    //    IPFS-hosted code STORE must be amended with another IPFS STORE, and
    //    likewise for native (storage) backed codes.
    let storage_engine = match &original_code_ref {
        ItemHash::Ipfs(_) => StorageEngine::Ipfs,
        ItemHash::Native(_) => StorageEngine::Storage,
    };

    if !json {
        eprintln!("Hashing {}...", archive.path().display());
    }
    let file_hash = match storage_engine {
        StorageEngine::Storage => hash_file(archive.path(), Hasher::for_storage()).await?,
        StorageEngine::Ipfs => hash_file(archive.path(), Hasher::for_ipfs()).await?,
    };
    if !json {
        eprintln!("  Code hash: {file_hash}");
    }

    // 4. Build the amending STORE. `reference_hash` records the previous code
    //    STORE so the network can chain amendments back to the original.
    let mut store_builder = StoreBuilder::new(&account, file_hash.clone(), storage_engine)
        .reference_hash(original_code_ref)
        .payment(Payment::credits());
    if args.on_behalf_of.is_some() {
        store_builder = store_builder.on_behalf_of(owner_address);
    }
    if let Some(ch) = &args.channel {
        store_builder = store_builder.channel(Channel::from(ch.clone()));
    }
    let store_pending = store_builder.build()?;

    // 5. Dry run: print the STORE envelope and stop. No PROGRAM is emitted on
    //    update because the program item hash is unchanged - only the code
    //    STORE changes.
    if dry_run {
        if !json {
            eprintln!("Dry run - message not submitted.\n");
        }
        println!("{}", serde_json::to_string_pretty(&store_pending)?);
        return Ok(());
    }

    // 6. Upload the new archive (mirrors handle_create).
    if !json {
        eprintln!("Uploading new code archive...");
    }
    match storage_engine {
        StorageEngine::Storage => {
            aleph_client
                .upload_file_to_storage(archive.path(), Some(&store_pending), true)
                .await?;
            print_submission_result(ccn_url, &store_pending, "success", "processed", json)?;
        }
        StorageEngine::Ipfs => {
            aleph_client
                .upload_file_to_ipfs(archive.path(), Some(&store_pending), true)
                .await?;
            print_submission_result(ccn_url, &store_pending, "success", "processed", json)?;
        }
    }

    if !json {
        eprintln!(
            "Program {} updated. Code amended to {}.",
            args.item_hash, file_hash
        );
    }
    Ok(())
}

/// Pure helper: verify that a candidate new archive's encoding matches the
/// existing program's `code.encoding`. The PROGRAM message records the
/// encoding once at creation, and the network expects subsequent code
/// amendments to keep it stable.
fn check_update_encoding(program: &Message, new_encoding: Encoding) -> Result<()> {
    let MessageContentEnum::Program(content) = program.content() else {
        bail!("expected PROGRAM message");
    };
    if content.code.encoding != new_encoding {
        bail!(
            "new code encoding `{:?}` does not match existing program encoding `{:?}`",
            new_encoding,
            content.code.encoding
        );
    }
    Ok(())
}

/// Pure helper: clone a PROGRAM message's `ProgramContent` with `on.persistent`
/// flipped to `new_persistent`. Used by both `persist` and `unpersist` to build
/// the replacement PROGRAM message.
fn clone_program_for_repersist(
    program: &Message,
    new_persistent: bool,
) -> Result<aleph_types::message::ProgramContent> {
    let MessageContentEnum::Program(content) = program.content() else {
        bail!("expected PROGRAM message");
    };
    let mut cloned = content.clone();
    cloned.on.persistent = Some(new_persistent);
    Ok(cloned)
}

/// Shared handler for `aleph program persist` (with `new_persistent = true`)
/// and `aleph program unpersist` (with `new_persistent = false`). Publishes a
/// new PROGRAM message with the updated `on.persistent` flag and, unless
/// `--keep-prev`, forgets the previous PROGRAM (keeping its code STORE intact
/// so the new program can reuse it).
async fn handle_persist_or_unpersist(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: ProgramPersistArgs,
    new_persistent: bool,
) -> Result<()> {
    use aleph_sdk::builder::MessageBuilder;

    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    // Effective owner: the program's authoring address. Differs from the
    // signing account when --on-behalf-of is set (delegated authoring).
    let owner_address = match &args.on_behalf_of {
        Some(value) => resolve_address(value)?,
        None => account.address().clone(),
    };

    // Compare against `owner()` (= content.address), not `sender`: when the
    // program was created via delegation, `sender` is the delegate's key.
    let program = fetch_program_message(aleph_client, &args.item_hash).await?;
    if program.owner() != &owner_address {
        bail!(
            "address {owner_address} does not own program {} (owner: {})",
            args.item_hash,
            program.owner()
        );
    }
    let MessageContentEnum::Program(program_content) = program.content() else {
        bail!("expected PROGRAM message, got {:?}", program.message_type);
    };
    if !program_content.base.allow_amend {
        bail!(
            "program {} is not updatable; was it created without --updatable?",
            args.item_hash
        );
    }

    let new_content = clone_program_for_repersist(&program, new_persistent)?;
    let value = serde_json::to_value(&new_content)?;
    let mut new_builder = MessageBuilder::new(&account, MessageType::Program, value);
    if args.on_behalf_of.is_some() {
        new_builder = new_builder.on_behalf_of(owner_address.clone());
    }
    let new_pending = new_builder.build()?;

    let action_label = if new_persistent {
        "persistent"
    } else {
        "ephemeral"
    };
    let prompt = if args.keep_prev {
        format!(
            "Publish a new {action_label} program message? Previous program {} stays.",
            args.item_hash
        )
    } else {
        format!(
            "Publish a new {action_label} program message and forget the previous one ({})?",
            args.item_hash
        )
    };
    if !confirm_action(&prompt, args.yes)? {
        bail!("aborted");
    }

    submit_or_preview(aleph_client, ccn_url, &new_pending, dry_run, json).await?;

    if !args.keep_prev {
        // The code STORE is reused by the new program; do not forget it.
        let forget = build_forget_for_program(
            &account,
            &program,
            "Re-persisted",
            None,
            args.on_behalf_of.as_ref().map(|_| owner_address.clone()),
        )?;
        submit_or_preview(aleph_client, ccn_url, &forget, dry_run, json).await?;
    }

    if !json {
        eprintln!("New {action_label} program: {}", new_pending.item_hash);
    }
    Ok(())
}

async fn handle_delete(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: ProgramDeleteArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    let program = fetch_program_message(aleph_client, &args.item_hash).await?;
    if &program.sender != account.address() {
        bail!(
            "you are not the owner of program {} (sender: {})",
            args.item_hash,
            program.sender
        );
    }

    let program_forget = build_forget_for_program(&account, &program, &args.reason, None, None)?;

    let code_ref = if args.keep_code {
        None
    } else {
        let MessageContentEnum::Program(content) = program.content() else {
            bail!("expected PROGRAM message, got {:?}", program.message_type);
        };
        Some(content.code.reference.clone())
    };

    let action_summary = if args.keep_code {
        format!("Forget program {} (keep code)?", args.item_hash)
    } else {
        format!("Forget program {} and its code STORE?", args.item_hash)
    };
    if !confirm_action(&action_summary, args.yes)? {
        bail!("aborted");
    }

    // pyaleph rejects a single FORGET that targets a PROGRAM and the STORE it
    // still references (error code 503: "not allowed"). Forget them in two
    // steps - program first, then code STORE - to match aleph-client (Python).
    submit_or_preview(aleph_client, ccn_url, &program_forget, dry_run, json).await?;

    if let Some(code_ref) = code_ref {
        if dry_run {
            // Skip the existence check: in dry-run we just want to show the
            // envelope that would be sent.
            let store_forget = build_forget_for_code_store(
                &account,
                &code_ref,
                &format!("Deletion of program {}", args.item_hash),
                None,
            )?;
            submit_or_preview(aleph_client, ccn_url, &store_forget, dry_run, json).await?;
        } else {
            forget_code_store(
                aleph_client,
                ccn_url,
                &account,
                &args.item_hash,
                &code_ref,
                json,
            )
            .await?;
        }
    }

    Ok(())
}

/// Fetch the STORE referenced by `code.ref`, check ownership, and submit a
/// FORGET for it. Missing / already-forgotten STOREs are skipped with a note,
/// matching the Python CLI - the program FORGET has already been submitted so
/// we don't want to fail loudly on the cleanup step.
async fn forget_code_store<A: Account>(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    account: &A,
    program_hash: &ItemHash,
    code_ref: &ItemHash,
    json: bool,
) -> Result<()> {
    let with_status = match aleph_client.get_message(code_ref).await {
        Ok(s) => s,
        Err(MessageError::NotFound(_)) => {
            if !json {
                eprintln!("Code STORE {code_ref} not found; skipping.");
            }
            return Ok(());
        }
        Err(e) => return Err(e).with_context(|| format!("failed to fetch code STORE {code_ref}")),
    };
    let store = match with_status {
        MessageWithStatus::Processed { message } => message,
        MessageWithStatus::Removing { message, .. } => message,
        MessageWithStatus::Forgotten { .. } | MessageWithStatus::Removed { .. } => {
            if !json {
                eprintln!("Code STORE {code_ref} already forgotten; skipping.");
            }
            return Ok(());
        }
        MessageWithStatus::Pending { .. } => {
            bail!("code STORE {code_ref} is still pending; wait for it to be processed")
        }
        MessageWithStatus::Rejected { .. } => {
            if !json {
                eprintln!("Code STORE {code_ref} was rejected by the network; skipping.");
            }
            return Ok(());
        }
    };
    if &store.sender != account.address() {
        if !json {
            eprintln!(
                "Code STORE {code_ref} is owned by {}; skipping.",
                store.sender
            );
        }
        return Ok(());
    }
    let store_forget = build_forget_for_code_store(
        account,
        code_ref,
        &format!("Deletion of program {program_hash}"),
        None,
    )?;
    submit_or_preview(aleph_client, ccn_url, &store_forget, false, json).await?;
    Ok(())
}

/// Fetch a PROGRAM message by item hash and assert it is currently usable
/// (processed or in the process of being removed). Returns a clean error for
/// pending / forgotten / rejected statuses.
async fn fetch_program_message(
    aleph_client: &AlephClient,
    item_hash: &ItemHash,
) -> Result<Message> {
    let with_status = aleph_client
        .get_message(item_hash)
        .await
        .with_context(|| format!("failed to fetch program {item_hash}"))?;
    let message = match with_status {
        MessageWithStatus::Processed { message } => message,
        MessageWithStatus::Removing { message, .. } => message,
        MessageWithStatus::Removed { .. } => {
            bail!("program {item_hash} has been removed")
        }
        MessageWithStatus::Pending { .. } => {
            bail!(
                "program {item_hash} is still pending; wait for it to be processed before deleting"
            )
        }
        MessageWithStatus::Forgotten { .. } => {
            bail!("program {item_hash} has already been forgotten")
        }
        MessageWithStatus::Rejected { .. } => {
            bail!("program {item_hash} was rejected by the network")
        }
    };
    if message.message_type != MessageType::Program {
        bail!(
            "item {item_hash} is not a PROGRAM message (got {:?})",
            message.message_type
        );
    }
    Ok(message)
}

/// Build the FORGET targeting a PROGRAM message.
fn build_forget_for_program<A: Account>(
    account: &A,
    program: &Message,
    reason: &str,
    channel: Option<Channel>,
    on_behalf_of: Option<Address>,
) -> Result<PendingMessage> {
    if program.message_type != MessageType::Program {
        bail!("expected PROGRAM message, got {:?}", program.message_type);
    }
    let mut builder = ForgetBuilder::new(account, vec![program.item_hash.clone()]).reason(reason);
    if let Some(owner) = on_behalf_of {
        builder = builder.on_behalf_of(owner);
    }
    if let Some(ch) = channel {
        builder = builder.channel(ch);
    }
    Ok(builder.build()?)
}

/// Build the FORGET targeting the STORE message that holds a program's code.
fn build_forget_for_code_store<A: Account>(
    account: &A,
    code_ref: &ItemHash,
    reason: &str,
    channel: Option<Channel>,
) -> Result<PendingMessage> {
    let mut builder = ForgetBuilder::new(account, vec![code_ref.clone()]).reason(reason);
    if let Some(ch) = channel {
        builder = builder.channel(ch);
    }
    Ok(builder.build()?)
}

/// Stream logs from one CRN. The CRN's log endpoint is identical for instances
/// and programs (it indexes by VM hash regardless of message type), so we
/// route through the existing instance handler.
async fn handle_logs(json: bool, args: ProgramLogsArgs) -> Result<()> {
    crate::commands::crn::handle_logs(
        json,
        CrnArgs {
            crn_url: args.crn.to_string(),
            vm_id: args.item_hash,
            signing: args.signing,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base32_lower_nopad_matches_python_b32encode() {
        // Reference vectors produced with Python:
        //   import base64
        //   base64.b32encode(bytes.fromhex(HEX)).rstrip(b"=").lower().decode()
        let cases = [
            (
                "0000000000000000000000000000000000000000000000000000000000000000",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            (
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                "777777777777777777777777777777777777777777777777777q",
            ),
            (
                "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e",
                "tjdtlpfa2p3qglo5mzm4gu4hwv5uobkqzeyyihtimlwoj2pgki7a",
            ),
        ];
        for (hex, expected) in cases {
            let bytes = hex::decode(hex).unwrap();
            assert_eq!(base32_lower_nopad(&bytes), expected, "hex={hex}");
        }
    }

    #[test]
    fn vm_run_urls_emit_both_forms_for_native_hash() {
        let h =
            ItemHash::try_from("9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e")
                .unwrap();
        let (host, path) = vm_run_urls(&h).unwrap();
        assert_eq!(
            host,
            "https://tjdtlpfa2p3qglo5mzm4gu4hwv5uobkqzeyyihtimlwoj2pgki7a.aleph.sh"
        );
        assert_eq!(
            path,
            "https://aleph.sh/vm/9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e"
        );
    }

    #[test]
    fn parse_env_vars_basic() {
        let m = parse_env_vars("FOO=1,BAR=hello").unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("FOO"), Some(&"1".to_string()));
        assert_eq!(m.get("BAR"), Some(&"hello".to_string()));
    }

    #[test]
    fn parse_env_vars_empty_pieces_skipped() {
        let m = parse_env_vars("FOO=1,,BAR=2,").unwrap();
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn parse_env_vars_missing_equals_errors() {
        let err = parse_env_vars("FOO").unwrap_err();
        assert!(format!("{err:#}").contains("expected KEY=value"));
    }

    #[test]
    fn extract_program_row_from_fixture() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let message: Message = serde_json::from_str(raw).expect("fixture is valid program message");
        let row = extract_program_row(&message).expect("fixture downcasts to program");
        assert_eq!(
            row.item_hash.to_string(),
            "acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c"
        );
        assert_eq!(row.name.as_deref(), Some("Hoymiles"));
        assert_eq!(row.vcpus, 2);
        assert_eq!(row.memory_mib, 4096);
        assert!(row.internet);
        assert!(!row.persistent);
        assert!(!row.updatable);
    }

    #[test]
    fn format_program_rows_text_includes_columns_and_data() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let message: Message = serde_json::from_str(raw).unwrap();
        let row = extract_program_row(&message).unwrap();
        let out = format_program_rows_text(&[row]);
        assert!(out.contains("ITEM HASH"));
        assert!(out.contains("NAME"));
        assert!(out.contains("VCPUS"));
        assert!(out.contains("PERSISTENT"));
        assert!(out.contains("Hoymiles"));
        assert!(out.contains("acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c"));
    }

    /// Minimal test account that produces a dummy signature. Mirrors the
    /// `TestAccount` used in `aleph-sdk/src/builder.rs` tests; the actual
    /// signing key does not matter for forget-builder unit tests.
    struct TestAccount {
        address: Address,
    }

    impl TestAccount {
        fn new() -> Self {
            Self {
                address: Address::from("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".to_string()),
            }
        }
    }

    impl Account for TestAccount {
        fn chain(&self) -> aleph_types::chain::Chain {
            aleph_types::chain::Chain::Ethereum
        }
        fn address(&self) -> &Address {
            &self.address
        }
        fn sign_raw(
            &self,
            _buffer: &[u8],
        ) -> Result<aleph_types::chain::Signature, aleph_types::account::SignError> {
            Ok(aleph_types::chain::Signature::from("0xDUMMY".to_string()))
        }
    }

    #[test]
    fn build_forget_for_program_targets_only_the_program_hash() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let program: Message = serde_json::from_str(raw).unwrap();
        let account = TestAccount::new();
        let pending =
            build_forget_for_program(&account, &program, "User deletion", None, None).unwrap();
        let value: serde_json::Value = serde_json::from_str(&pending.item_content).unwrap();
        let hashes = value["hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].as_str().unwrap(), program.item_hash.to_string());
        assert_eq!(value["reason"], "User deletion");
    }

    #[test]
    fn check_update_encoding_accepts_matching() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let program: Message = serde_json::from_str(raw).unwrap();
        // Fixture's code.encoding is Encoding::Zip
        check_update_encoding(&program, Encoding::Zip).unwrap();
    }

    #[test]
    fn check_update_encoding_rejects_mismatch() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let program: Message = serde_json::from_str(raw).unwrap();
        let err = check_update_encoding(&program, Encoding::Squashfs).unwrap_err();
        assert!(format!("{err:#}").contains("encoding"));
    }

    #[test]
    fn clone_program_for_repersist_flips_only_the_flag() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let program: Message = serde_json::from_str(raw).unwrap();
        let original_content = match program.content() {
            MessageContentEnum::Program(c) => c.clone(),
            _ => unreachable!(),
        };

        let cloned = clone_program_for_repersist(&program, true).unwrap();
        // The fixture has on.persistent = Some(false); make sure we flipped to true.
        assert_eq!(cloned.on.persistent, Some(true));
        // And nothing else moved.
        assert_eq!(cloned.code.encoding, original_content.code.encoding);
        assert_eq!(cloned.code.entrypoint, original_content.code.entrypoint);
        assert_eq!(cloned.code.reference, original_content.code.reference);
        assert_eq!(cloned.runtime.reference, original_content.runtime.reference);
        assert_eq!(cloned.on.http, original_content.on.http);
        assert_eq!(
            cloned.environment.internet,
            original_content.environment.internet
        );
        assert_eq!(
            cloned.base.resources.vcpus,
            original_content.base.resources.vcpus
        );
        assert_eq!(cloned.base.allow_amend, original_content.base.allow_amend);
    }

    #[test]
    fn clone_program_for_repersist_to_false_works_too() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let program: Message = serde_json::from_str(raw).unwrap();
        let cloned = clone_program_for_repersist(&program, false).unwrap();
        assert_eq!(cloned.on.persistent, Some(false));
    }

    #[test]
    fn build_forget_for_code_store_targets_only_the_store_hash() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let program: Message = serde_json::from_str(raw).unwrap();
        let MessageContentEnum::Program(content) = program.content() else {
            unreachable!()
        };
        let account = TestAccount::new();
        let pending = build_forget_for_code_store(
            &account,
            &content.code.reference,
            "Deletion of program ...",
            None,
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&pending.item_content).unwrap();
        let hashes = value["hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(
            hashes[0].as_str().unwrap(),
            content.code.reference.to_string()
        );
    }
}
