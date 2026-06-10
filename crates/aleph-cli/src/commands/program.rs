use crate::cli::{
    ImageRef, PaymentTypeCli, ProgramCommand, ProgramCreateArgs, ProgramDeleteArgs,
    ProgramListArgs, ProgramShowArgs, ProgramUpdateArgs, StorageEngineCli,
};
use crate::commands::instance::{
    parse_ephemeral_volumes, parse_immutable_volumes, parse_persistent_volumes, resolve_runtime_ref,
};
use crate::common::{
    confirm_action, print_submission_result, resolve_account, resolve_address,
    resolve_address_or_active, submit_or_preview,
};
use crate::program::archive::prepare_archive;
use aleph_sdk::aggregate_models::vm_images::VmImagesData;
use aleph_sdk::client::{
    AlephAggregateClient, AlephClient, AlephMessageClient, MessageError, MessageFilter,
    MessageWithStatus, PaginationParams, SortBy, SortOrder, hash_file,
};
use aleph_sdk::messages::{ForgetBuilder, ProgramBuilder, StoreBuilder};
use aleph_sdk::verify::Hasher;
use aleph_types::account::Account;
use aleph_types::chain::Address;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::base::{Encoding, Payment, PaymentType};
use aleph_types::message::execution::volume::MachineVolume;
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::{
    Message, MessageContentEnum, MessageHeader, MessageType, StorageEngine,
};
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
        ProgramCommand::Show(args) => handle_show(aleph_client, json, args).await,
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

    // 4. Resolve runtime. Hash inputs short-circuit the aggregate fetch; presets
    // and a missing flag both require the aggregate (the latter to read
    // `defaults.runtime`).
    let needs_aggregate = !matches!(args.runtime, Some(ImageRef::Hash(_)));
    let vm_images = if needs_aggregate {
        aleph_client
            .get_vm_images_aggregate()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "failed to fetch vm-images aggregate: {e}. \
                     As a fallback, pass --runtime with a raw item hash."
                )
            })?
            .vm_images
    } else {
        VmImagesData::default()
    };
    let runtime = resolve_runtime_ref(args.runtime.clone(), &vm_images)?;

    // 5. Build STORE
    let mut store_builder =
        StoreBuilder::new(&account, file_hash.clone(), storage_engine).payment(payment.clone());
    if let Some(owner) = &args.on_behalf_of {
        store_builder = store_builder.on_behalf_of(resolve_address(owner)?);
    }
    if let Some(ch) = &args.channel {
        store_builder = store_builder.channel(Channel::from(ch.clone()));
    }
    let store_pending = store_builder.build()?;

    // 6. Resolve resources (size slug or granular flags)
    let (vcpus, memory_mib) =
        resolve_program_resources(aleph_client, args.size.as_deref(), args.vcpus, args.memory)
            .await?;

    // 7. Build PROGRAM. `code.ref` is the STORE message's item_hash, not the
    // raw file hash - the VM supervisor resolves it by looking up the STORE
    // and following its embedded file reference.
    let mut program_builder = ProgramBuilder::new(
        &account,
        store_pending.item_hash.clone(),
        args.entrypoint.clone(),
        runtime,
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

    // 8. Dry run: print both envelopes and stop. submit_or_preview prints a
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

    // 9. Upload code archive (mirrors handle_single_file_upload in commands/file.rs)
    if !json {
        eprintln!("Uploading code archive...");
    }
    let on_tick: fn(u64, u64) = if json {
        |_, _| {}
    } else {
        crate::common::render_upload_progress
    };
    let upload = match storage_engine {
        StorageEngine::Storage => {
            aleph_client
                .upload_file_to_storage_with_progress(
                    archive.path(),
                    Some(&store_pending),
                    true,
                    on_tick,
                )
                .await
        }
        StorageEngine::Ipfs => {
            aleph_client
                .upload_file_to_ipfs_with_progress(
                    archive.path(),
                    Some(&store_pending),
                    true,
                    on_tick,
                )
                .await
        }
    };
    if !json {
        eprintln!();
    }
    upload?;
    print_submission_result(ccn_url, &store_pending, "success", "processed", json)?;

    // 10. Submit PROGRAM
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

// =============================================================================
// `aleph program show` data model
// =============================================================================

/// Serialize a `Timestamp` as an RFC3339 string (e.g. `"2025-10-04T12:34:56Z"`).
fn serialize_ts_as_rfc3339<S>(ts: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let dt = ts.to_datetime().map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&dt.to_rfc3339())
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProgramInterface {
    Asgi,
    Binary,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ProgramShowInfo {
    pub item_hash: ItemHash,
    pub name: Option<String>,
    #[serde(serialize_with = "serialize_ts_as_rfc3339")]
    pub created_at: Timestamp,
    pub sender: Address,
    pub owner: Address,
    pub channel: Option<aleph_types::channel::Channel>,
    pub entrypoint: String,
    pub interface: ProgramInterface,
    pub encoding: String,
    pub vcpus: u32,
    pub memory_mib: u64,
    pub timeout_seconds: u32,
    pub internet: bool,
    pub persistent: bool,
    pub updatable: bool,
    pub env_vars: std::collections::BTreeMap<String, String>,
    pub payment_kind: Option<String>,
    pub payment_chain: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum RefLabel {
    Code,
    Runtime,
    Data,
    Immutable { mount: String },
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct StoreSummary {
    pub sender: Address,
    pub owner: Address,
    #[serde(serialize_with = "serialize_ts_as_rfc3339")]
    pub created_at: Timestamp,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum LatestStatus {
    Pinned,
    UpToDate,
    Updated {
        hash: ItemHash,
        #[serde(serialize_with = "serialize_ts_as_rfc3339")]
        updated_at: Timestamp,
    },
    Unresolved {
        reason: String,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct RefInfo {
    #[serde(flatten)]
    pub label: RefLabel,
    #[serde(rename = "ref")]
    pub ref_hash: ItemHash,
    pub use_latest: bool,
    pub original: Option<StoreSummary>,
    pub latest: LatestStatus,
}

#[derive(Debug, Clone)]
pub(crate) struct RefSpec {
    pub label: RefLabel,
    pub hash: ItemHash,
    pub use_latest: bool,
}

pub(crate) fn collect_refs(program: &aleph_types::message::ProgramContent) -> Vec<RefSpec> {
    let mut out = Vec::new();
    out.push(RefSpec {
        label: RefLabel::Code,
        hash: program.code.reference.clone(),
        use_latest: program.code.use_latest,
    });
    out.push(RefSpec {
        label: RefLabel::Runtime,
        hash: program.runtime.reference.clone(),
        use_latest: program.runtime.use_latest,
    });
    if let Some(data) = program.data.as_ref() {
        out.push(RefSpec {
            label: RefLabel::Data,
            hash: data.reference.clone(),
            use_latest: data.use_latest.unwrap_or(false),
        });
    }
    for v in &program.base.volumes {
        if let MachineVolume::Immutable(iv) = v {
            let mount = iv
                .base
                .mount
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            out.push(RefSpec {
                label: RefLabel::Immutable { mount },
                hash: iv.reference.clone(),
                use_latest: iv.use_latest,
            });
        }
    }
    out
}

pub(crate) fn collect_non_ref_volumes(volumes: &[MachineVolume]) -> Vec<NonRefVolume> {
    use aleph_types::message::execution::volume::VolumePersistence;

    volumes
        .iter()
        .filter_map(|v| match v {
            MachineVolume::Immutable(_) => None,
            MachineVolume::Ephemeral(e) => Some(NonRefVolume::Ephemeral {
                mount: e
                    .base
                    .mount
                    .as_ref()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                size_mib: u64::from(e.size_mib),
            }),
            MachineVolume::Persistent(p) => Some(NonRefVolume::Persistent {
                mount: p
                    .base
                    .mount
                    .as_ref()
                    .map(|x| x.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                size_mib: u64::from(p.size_mib),
                persistence: match p.persistence {
                    Some(VolumePersistence::Host) => "host".into(),
                    Some(VolumePersistence::Store) => "store".into(),
                    None => "host".into(), // CCN default
                },
                name: p.name.clone(),
            }),
        })
        .collect()
}

/// Map a `MessageWithStatus<Message>` (from an SDK `get_message` call) into
/// a `StoreSummary`, or return a short reason string if the message is not a
/// live STORE.
///
/// Pure helper with no I/O - takes ownership of the SDK response.
pub(crate) fn store_summary_from(
    status: MessageWithStatus<Message>,
) -> std::result::Result<StoreSummary, String> {
    let message = match status {
        MessageWithStatus::Processed { message } => message,
        MessageWithStatus::Removing { message, .. } => message,
        MessageWithStatus::Removed { .. } => return Err("removed".into()),
        MessageWithStatus::Forgotten { .. } => return Err("forgotten".into()),
        MessageWithStatus::Pending { .. } => return Err("pending".into()),
        MessageWithStatus::Rejected { .. } => return Err("rejected".into()),
    };
    if message.message_type != MessageType::Store {
        return Err(format!("not a STORE (got {:?})", message.message_type));
    }
    Ok(StoreSummary {
        sender: message.sender.clone(),
        owner: message.owner().clone(),
        created_at: message.content.time.clone(),
    })
}

/// Map an amend-lookup response (headers sorted newest-first) into a
/// `LatestStatus`. An empty slice means no amendments exist, so the original
/// is current (`UpToDate`). The first (newest) header otherwise describes the
/// latest amendment.
///
/// Pure helper with no I/O.
pub(crate) fn latest_status_from(headers: &[MessageHeader]) -> LatestStatus {
    match headers.first() {
        None => LatestStatus::UpToDate,
        Some(h) => LatestStatus::Updated {
            hash: h.item_hash.clone(),
            updated_at: h.time.clone(),
        },
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum NonRefVolume {
    Ephemeral {
        mount: String,
        size_mib: u64,
    },
    Persistent {
        mount: String,
        size_mib: u64,
        persistence: String,
        name: Option<String>,
    },
}

fn program_interface_from(
    interface: Option<&aleph_types::message::execution::base::Interface>,
) -> ProgramInterface {
    use aleph_types::message::execution::base::Interface;
    match interface {
        Some(Interface::Asgi) | None => ProgramInterface::Asgi,
        Some(Interface::Binary) => ProgramInterface::Binary,
    }
}

fn payment_kind_str(payment: &Payment) -> &'static str {
    match payment.payment_type {
        PaymentType::Hold => "hold",
        PaymentType::Superfluid => "superfluid",
        PaymentType::Credit => "credit",
    }
}

fn encoding_str(encoding: &Encoding) -> &'static str {
    match encoding {
        Encoding::Plain => "plain",
        Encoding::Zip => "zip",
        Encoding::Squashfs => "squashfs",
    }
}

pub(crate) fn build_program_show_info(message: &Message) -> ProgramShowInfo {
    let MessageContentEnum::Program(program) = message.content() else {
        panic!(
            "build_program_show_info called on non-PROGRAM message {}",
            message.item_hash
        );
    };

    let name = program
        .base
        .metadata
        .as_ref()
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let env_vars = program
        .base
        .variables
        .as_ref()
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<std::collections::BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let (payment_kind, payment_chain) = match program.base.payment.as_ref() {
        Some(p) => (
            Some(payment_kind_str(p).to_string()),
            p.chain.as_ref().map(|c| c.to_string()),
        ),
        None => (None, None),
    };

    ProgramShowInfo {
        item_hash: message.item_hash.clone(),
        name,
        created_at: message.content.time.clone(),
        sender: message.sender.clone(),
        owner: message.owner().clone(),
        channel: message.channel.clone(),
        entrypoint: program.code.entrypoint.clone(),
        interface: program_interface_from(program.code.interface.as_ref()),
        encoding: encoding_str(&program.code.encoding).to_string(),
        vcpus: program.base.resources.vcpus,
        memory_mib: u64::from(program.base.resources.memory),
        timeout_seconds: program.base.resources.seconds,
        internet: program.environment.internet,
        persistent: program.on.persistent.unwrap_or(false),
        updatable: program.base.allow_amend,
        env_vars,
        payment_kind,
        payment_chain,
    }
}

async fn handle_list(aleph_client: &AlephClient, json: bool, args: ProgramListArgs) -> Result<()> {
    // Read-only: resolve the address from the manifest without loading the
    // account (loading an encrypted account would prompt for its password).
    let address = resolve_address_or_active(args.address.as_deref())?;
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
    let on_tick: fn(u64, u64) = if json {
        |_, _| {}
    } else {
        crate::common::render_upload_progress
    };
    let upload = match storage_engine {
        StorageEngine::Storage => {
            aleph_client
                .upload_file_to_storage_with_progress(
                    archive.path(),
                    Some(&store_pending),
                    true,
                    on_tick,
                )
                .await
        }
        StorageEngine::Ipfs => {
            aleph_client
                .upload_file_to_ipfs_with_progress(
                    archive.path(),
                    Some(&store_pending),
                    true,
                    on_tick,
                )
                .await
        }
    };
    if !json {
        eprintln!();
    }
    upload?;
    print_submission_result(ccn_url, &store_pending, "success", "processed", json)?;

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

    let program_forget = build_forget_for_program(&account, &program, &args.reason, None)?;

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
) -> Result<PendingMessage> {
    if program.message_type != MessageType::Program {
        bail!("expected PROGRAM message, got {:?}", program.message_type);
    }
    let mut builder = ForgetBuilder::new(account, vec![program.item_hash.clone()]).reason(reason);
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

async fn resolve_ref(aleph_client: &AlephClient, spec: RefSpec) -> RefInfo {
    let label = spec.label.clone();
    let ref_hash = spec.hash.clone();
    let use_latest = spec.use_latest;

    // 1. Original STORE.
    let original_res = aleph_client.get_message(&ref_hash).await;
    let (original, unresolved_reason) = match original_res {
        Ok(status) => match store_summary_from(status) {
            Ok(s) => (Some(s), None),
            Err(reason) => {
                eprintln!(
                    "warning: cannot resolve STORE {} for {}: {}",
                    ref_hash,
                    label_display(&label),
                    reason
                );
                (None, Some(reason))
            }
        },
        Err(e) => {
            let reason = format!("fetch error: {e}");
            eprintln!(
                "warning: cannot resolve STORE {} for {}: {}",
                ref_hash,
                label_display(&label),
                reason
            );
            (None, Some(reason))
        }
    };

    // 2. Latest status.
    let latest = if !use_latest {
        LatestStatus::Pinned
    } else if let Some(reason) = unresolved_reason {
        // If the original STORE didn't resolve, we still ran no amend query.
        LatestStatus::Unresolved { reason }
    } else {
        let filter = MessageFilter {
            message_type: Some(MessageType::Store),
            refs: Some(vec![ref_hash.to_string()]),
            sort_by: Some(SortBy::Time),
            sort_order: Some(SortOrder::Desc),
            ..Default::default()
        };
        let pagination = PaginationParams {
            pagination: Some(1),
            page: Some(1),
        };
        match aleph_client.get_messages(&filter, pagination).await {
            Ok(messages) => {
                let headers: Vec<aleph_types::message::MessageHeader> = messages
                    .into_iter()
                    .map(aleph_types::message::MessageHeader::from)
                    .collect();
                latest_status_from(&headers)
            }
            Err(e) => {
                let reason = format!("amend query failed: {e}");
                eprintln!(
                    "warning: cannot check latest for {} ({}): {}",
                    ref_hash,
                    label_display(&label),
                    reason
                );
                LatestStatus::Unresolved { reason }
            }
        }
    };

    RefInfo {
        label,
        ref_hash,
        use_latest,
        original,
        latest,
    }
}

fn label_display(label: &RefLabel) -> String {
    match label {
        RefLabel::Code => "code".into(),
        RefLabel::Runtime => "runtime".into(),
        RefLabel::Data => "data".into(),
        RefLabel::Immutable { mount } => format!("immutable {mount}"),
    }
}

pub(crate) fn render_show_json(
    info: &ProgramShowInfo,
    refs: &[RefInfo],
    volumes: &[NonRefVolume],
) -> serde_json::Value {
    serde_json::json!({
        "program": info,
        "refs": refs,
        "volumes": volumes,
    })
}

pub(crate) fn render_show_text(
    info: &ProgramShowInfo,
    refs: &[RefInfo],
    volumes: &[NonRefVolume],
) -> String {
    use std::fmt::Write;

    let mut out = String::new();

    writeln!(out, "PROGRAM {}", info.item_hash).unwrap();
    if let Some(name) = info.name.as_deref() {
        writeln!(out, "  Name           {name}").unwrap();
    }
    writeln!(out, "  Created        {}", format_ts(&info.created_at)).unwrap();
    if info.sender != info.owner {
        writeln!(out, "  Sender         {}", info.sender).unwrap();
    }
    writeln!(out, "  Owner          {}", info.owner).unwrap();
    if let Some(c) = info.channel.as_ref() {
        let channel_str = serde_json::to_value(c)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default();
        writeln!(out, "  Channel        {channel_str}").unwrap();
    }
    if let (Some(k), Some(c)) = (&info.payment_kind, &info.payment_chain) {
        writeln!(out, "  Payment        {k} ({c})").unwrap();
    }
    writeln!(out, "  Entrypoint     {}", info.entrypoint).unwrap();
    writeln!(
        out,
        "  Interface      {}",
        match info.interface {
            ProgramInterface::Asgi => "ASGI",
            ProgramInterface::Binary => "binary",
        }
    )
    .unwrap();
    writeln!(
        out,
        "  Resources      {} vCPUs, {} MiB",
        info.vcpus, info.memory_mib
    )
    .unwrap();
    writeln!(out, "  Timeout        {}s", info.timeout_seconds).unwrap();
    writeln!(
        out,
        "  Flags          internet={} persistent={} updatable={}",
        info.internet, info.persistent, info.updatable
    )
    .unwrap();
    if !info.env_vars.is_empty() {
        let joined: Vec<String> = info
            .env_vars
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        writeln!(out, "  Env vars       {}", joined.join(" ")).unwrap();
    }

    writeln!(out).unwrap();
    writeln!(out, "REFS").unwrap();
    for r in refs {
        let header_label = match &r.label {
            RefLabel::Code => "code".to_string(),
            RefLabel::Runtime => "runtime".to_string(),
            RefLabel::Data => "data".to_string(),
            RefLabel::Immutable { mount } => format!("immutable {mount}"),
        };
        writeln!(out, "  {header_label:<14} {}", r.ref_hash).unwrap();
        match &r.original {
            Some(s) => {
                writeln!(out, "    Owner        {}", s.owner).unwrap();
                writeln!(out, "    Created      {}", format_ts(&s.created_at)).unwrap();
            }
            None => {
                writeln!(out, "    Owner        ?").unwrap();
            }
        }
        match &r.latest {
            LatestStatus::Pinned => {
                writeln!(out, "    Pinned (use_latest=false)").unwrap();
            }
            LatestStatus::UpToDate => {
                writeln!(out, "    Up to date (use_latest=true, no amends found)").unwrap();
            }
            LatestStatus::Updated { hash, updated_at } => {
                writeln!(
                    out,
                    "    Latest         {hash} (updated {})",
                    format_ts(updated_at)
                )
                .unwrap();
            }
            LatestStatus::Unresolved { reason } => {
                writeln!(out, "    Status         {reason}").unwrap();
            }
        }
        writeln!(out).unwrap();
    }

    if !volumes.is_empty() {
        writeln!(out, "OTHER VOLUMES").unwrap();
        for v in volumes {
            match v {
                NonRefVolume::Ephemeral { mount, size_mib } => {
                    writeln!(out, "  ephemeral {mount:<20} size={size_mib} MiB").unwrap();
                }
                NonRefVolume::Persistent {
                    mount,
                    size_mib,
                    persistence,
                    name,
                } => {
                    let n = name.as_deref().unwrap_or("-");
                    writeln!(
                        out,
                        "  persistent {n} {mount:<20} size={size_mib} MiB, persistence={persistence}"
                    )
                    .unwrap();
                }
            }
        }
    }

    out
}

fn format_ts(t: &Timestamp) -> String {
    t.to_datetime()
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|_| format!("{}", t.as_f64()))
}

async fn handle_show(aleph_client: &AlephClient, json: bool, args: ProgramShowArgs) -> Result<()> {
    // 1. Fetch the PROGRAM message (reuses existing helper; bails on
    //    pending/forgotten/removed/wrong-type).
    let message = fetch_program_message(aleph_client, &args.item_hash).await?;

    // 2. Build the program-level info (pure).
    let info = build_program_show_info(&message);

    // 3. Extract refs and non-ref volumes from the content.
    let MessageContentEnum::Program(program) = message.content() else {
        // fetch_program_message already enforces this, but the borrow
        // checker likes us to handle it explicitly.
        anyhow::bail!("expected PROGRAM message");
    };
    let specs = collect_refs(program);
    let volumes = collect_non_ref_volumes(&program.base.volumes);

    // 4. Resolve all refs in parallel.
    let futs = specs
        .into_iter()
        .map(|spec| resolve_ref(aleph_client, spec));
    let refs: Vec<RefInfo> = futures_util::future::join_all(futs).await;

    // 5. Render.
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&render_show_json(&info, &refs, &volumes))?
        );
    } else {
        print!("{}", render_show_text(&info, &refs, &volumes));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROGRAM_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/program/program.json"
    ));

    #[test]
    fn build_program_show_info_matches_fixture() {
        let message: Message = serde_json::from_str(PROGRAM_FIXTURE).unwrap();
        let info = build_program_show_info(&message);

        assert_eq!(
            info.item_hash.to_string(),
            "acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c"
        );
        assert_eq!(info.name.as_deref(), Some("Hoymiles"));
        assert_eq!(info.sender, message.sender);
        assert_eq!(&info.owner, message.owner());
        assert_eq!(info.entrypoint, "main:app");
        assert_eq!(info.encoding, "zip");
        assert!(matches!(info.interface, ProgramInterface::Asgi));
        assert_eq!(info.vcpus, 2);
        assert_eq!(info.memory_mib, 4096);
        assert_eq!(info.timeout_seconds, 30);
        assert!(info.internet);
        assert!(!info.persistent);
        assert!(!info.updatable);
        assert!(info.env_vars.is_empty());
        assert_eq!(info.payment_kind.as_deref(), Some("hold"));
        assert_eq!(info.payment_chain.as_deref(), Some("ETH"));
        // Channel has no Display impl; serialize to extract the inner string.
        let channel_str = info.channel.as_ref().and_then(|c| {
            if let Ok(serde_json::Value::String(s)) = serde_json::to_value(c) {
                Some(s)
            } else {
                None
            }
        });
        assert_eq!(channel_str.as_deref(), Some("ALEPH-CLOUDSOLUTIONS"));
    }

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
        let pending = build_forget_for_program(&account, &program, "User deletion", None).unwrap();
        let value: serde_json::Value = serde_json::from_str(&pending.item_content).unwrap();
        let hashes = value["hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].as_str().unwrap(), program.item_hash.to_string());
        assert_eq!(value["reason"], "User deletion");
    }

    #[test]
    fn check_update_encoding_accepts_matching() {
        let program: Message = serde_json::from_str(PROGRAM_FIXTURE).unwrap();
        // Fixture's code.encoding is Encoding::Zip
        check_update_encoding(&program, Encoding::Zip).unwrap();
    }

    #[test]
    fn check_update_encoding_rejects_mismatch() {
        let program: Message = serde_json::from_str(PROGRAM_FIXTURE).unwrap();
        // Fixture's code.encoding is Encoding::Zip; pass a different one.
        let err = check_update_encoding(&program, Encoding::Squashfs).unwrap_err();
        let msg = format!("{err:#}");
        // The error must name both the rejected new encoding and the existing
        // one, so a swapped-argument regression would fail this assertion.
        assert!(msg.contains("Squashfs"), "missing new encoding: {msg}");
        assert!(msg.contains("Zip"), "missing existing encoding: {msg}");
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

    #[test]
    fn show_types_serialize_round_trip() {
        let info = ProgramShowInfo {
            item_hash: ItemHash::try_from(
                "acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c",
            )
            .unwrap(),
            name: Some("demo".into()),
            created_at: Timestamp::from(1_700_000_000.0),
            sender: Address::from("0xABCD".to_string()),
            owner: Address::from("0xABCD".to_string()),
            channel: None,
            entrypoint: "main:app".into(),
            interface: ProgramInterface::Asgi,
            encoding: "zip".into(),
            vcpus: 1,
            memory_mib: 2048,
            timeout_seconds: 30,
            internet: true,
            persistent: false,
            updatable: true,
            env_vars: std::collections::BTreeMap::new(),
            payment_kind: Some("credit".into()),
            payment_chain: Some("ETH".into()),
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(
            json["item_hash"],
            "acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c"
        );
        assert_eq!(json["interface"], "asgi");
        // Timestamp must serialize as an RFC3339 string, not a float.
        assert!(
            json["created_at"].is_string(),
            "created_at must serialize as RFC3339 string"
        );
    }

    #[test]
    fn ref_info_serializes_with_ref_key_and_flattened_label() {
        let store = StoreSummary {
            sender: Address::from("0xABC".to_string()),
            owner: Address::from("0xABC".to_string()),
            created_at: Timestamp::from(1_700_000_000.0),
        };
        let code_ref = RefInfo {
            label: RefLabel::Code,
            ref_hash: ItemHash::try_from(
                "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e",
            )
            .unwrap(),
            use_latest: false,
            original: Some(store.clone()),
            latest: LatestStatus::Pinned,
        };
        let v = serde_json::to_value(&code_ref).unwrap();
        assert_eq!(v["kind"], "code");
        assert_eq!(
            v["ref"],
            "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e"
        );
        assert_eq!(v["latest"]["kind"], "pinned");
        // Confirm timestamps render as RFC3339 strings, not floats.
        assert!(
            v["original"]["created_at"].is_string(),
            "created_at must serialize as RFC3339 string"
        );

        let imm = RefInfo {
            label: RefLabel::Immutable {
                mount: "/data".into(),
            },
            ref_hash: ItemHash::try_from(
                "8df728d560ed6e9103b040a6b5fc5417e0a52e890c12977464ebadf9becf1bf6",
            )
            .unwrap(),
            use_latest: true,
            original: None,
            latest: LatestStatus::Updated {
                hash: ItemHash::try_from(
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                )
                .unwrap(),
                updated_at: Timestamp::from(1_720_000_000.0),
            },
        };
        let v = serde_json::to_value(&imm).unwrap();
        assert_eq!(v["kind"], "immutable");
        assert_eq!(v["mount"], "/data");
        assert_eq!(v["latest"]["kind"], "updated");
        assert!(v["latest"]["updated_at"].is_string());
    }

    #[test]
    fn collect_refs_from_fixture() {
        let message: Message = serde_json::from_str(PROGRAM_FIXTURE).unwrap();
        let MessageContentEnum::Program(program) = message.content() else {
            panic!("fixture must be a PROGRAM");
        };
        let refs = collect_refs(program);

        // Fixture: code (use_latest=true), runtime (use_latest=true),
        // 1 immutable volume /opt/packages (use_latest=true), no data.
        assert_eq!(refs.len(), 3);

        assert!(matches!(refs[0].label, RefLabel::Code));
        assert_eq!(
            refs[0].hash.to_string(),
            "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e"
        );
        assert!(refs[0].use_latest);

        assert!(matches!(refs[1].label, RefLabel::Runtime));
        assert_eq!(
            refs[1].hash.to_string(),
            "63f07193e6ee9d207b7d1fcf8286f9aee34e6f12f101d2ec77c1229f92964696"
        );
        assert!(refs[1].use_latest);

        match &refs[2].label {
            RefLabel::Immutable { mount } => assert_eq!(mount, "/opt/packages"),
            other => panic!("expected Immutable, got {:?}", other),
        }
        assert_eq!(
            refs[2].hash.to_string(),
            "8df728d560ed6e9103b040a6b5fc5417e0a52e890c12977464ebadf9becf1bf6"
        );
        assert!(refs[2].use_latest);
    }

    #[test]
    fn collect_non_ref_volumes_filters_immutable() {
        use aleph_types::message::execution::volume::{
            BaseVolume, EphemeralVolume, ImmutableVolume, MachineVolume, PersistentVolume,
            PersistentVolumeSize, VolumePersistence,
        };
        use std::path::PathBuf;

        let imm = MachineVolume::Immutable(ImmutableVolume {
            base: BaseVolume {
                comment: None,
                mount: Some(PathBuf::from("/opt/packages")),
            },
            reference: ItemHash::try_from(
                "8df728d560ed6e9103b040a6b5fc5417e0a52e890c12977464ebadf9becf1bf6",
            )
            .unwrap(),
            use_latest: true,
        });
        let eph = MachineVolume::Ephemeral(EphemeralVolume::new(512, "/tmp").unwrap());
        let per = MachineVolume::Persistent(PersistentVolume {
            base: BaseVolume {
                comment: None,
                mount: Some(PathBuf::from("/cache")),
            },
            parent: None,
            name: Some("cache".into()),
            persistence: Some(VolumePersistence::Host),
            size_mib: PersistentVolumeSize::try_from(10_240u64).unwrap(),
        });

        let out = collect_non_ref_volumes(&[imm, eph, per]);
        assert_eq!(out.len(), 2);
        match &out[0] {
            NonRefVolume::Ephemeral { mount, size_mib } => {
                assert_eq!(mount, "/tmp");
                assert_eq!(size_mib, &512u64);
            }
            other => panic!("expected Ephemeral, got {:?}", other),
        }
        match &out[1] {
            NonRefVolume::Persistent {
                mount,
                size_mib,
                persistence,
                name,
            } => {
                assert_eq!(mount, "/cache");
                assert_eq!(size_mib, &10_240u64);
                assert_eq!(persistence, "host");
                assert_eq!(name.as_deref(), Some("cache"));
            }
            other => panic!("expected Persistent, got {:?}", other),
        }
    }

    #[test]
    fn store_summary_from_wrong_type_returns_not_a_store() {
        let message: Message = serde_json::from_str(PROGRAM_FIXTURE).unwrap();
        let status = MessageWithStatus::Processed { message };
        let err = store_summary_from(status).unwrap_err();
        assert!(
            err.contains("not a STORE"),
            "expected wrong-type error, got: {err}"
        );
    }

    #[test]
    fn store_summary_from_pending_returns_pending_reason() {
        let status: MessageWithStatus<Message> = MessageWithStatus::Pending { messages: vec![] };
        let err = store_summary_from(status).unwrap_err();
        assert_eq!(err, "pending");
    }

    #[test]
    fn latest_status_from_empty_means_up_to_date() {
        let headers: Vec<MessageHeader> = vec![];
        assert!(matches!(
            latest_status_from(&headers),
            LatestStatus::UpToDate
        ));
    }

    fn fixture_show_input() -> (ProgramShowInfo, Vec<RefInfo>, Vec<NonRefVolume>) {
        let info = ProgramShowInfo {
            item_hash: ItemHash::try_from(
                "acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c",
            )
            .unwrap(),
            name: Some("Hoymiles".into()),
            created_at: Timestamp::from(1_757_026_128.0),
            sender: Address::from("0x9C2FD74F9CA2B7C4941690316B0Ebc35ce55c885".to_string()),
            owner: Address::from("0x9C2FD74F9CA2B7C4941690316B0Ebc35ce55c885".to_string()),
            channel: Some(aleph_types::channel::Channel::from(
                "ALEPH-CLOUDSOLUTIONS".to_string(),
            )),
            entrypoint: "main:app".into(),
            interface: ProgramInterface::Asgi,
            encoding: "zip".into(),
            vcpus: 2,
            memory_mib: 4096,
            timeout_seconds: 30,
            internet: true,
            persistent: false,
            updatable: false,
            env_vars: Default::default(),
            payment_kind: Some("hold".into()),
            payment_chain: Some("ETH".into()),
        };
        let store = StoreSummary {
            sender: Address::from("0xDEADBEEF".to_string()),
            owner: Address::from("0xDEADBEEF".to_string()),
            created_at: Timestamp::from(1_700_000_000.0),
        };
        let refs = vec![
            RefInfo {
                label: RefLabel::Code,
                ref_hash: ItemHash::try_from(
                    "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e",
                )
                .unwrap(),
                use_latest: false,
                original: Some(store.clone()),
                latest: LatestStatus::Pinned,
            },
            RefInfo {
                label: RefLabel::Runtime,
                ref_hash: ItemHash::try_from(
                    "63f07193e6ee9d207b7d1fcf8286f9aee34e6f12f101d2ec77c1229f92964696",
                )
                .unwrap(),
                use_latest: true,
                original: Some(store.clone()),
                latest: LatestStatus::Updated {
                    hash: ItemHash::try_from(
                        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    )
                    .unwrap(),
                    updated_at: Timestamp::from(1_720_000_000.0),
                },
            },
            RefInfo {
                label: RefLabel::Immutable {
                    mount: "/data".into(),
                },
                ref_hash: ItemHash::try_from(
                    "8df728d560ed6e9103b040a6b5fc5417e0a52e890c12977464ebadf9becf1bf6",
                )
                .unwrap(),
                use_latest: true,
                original: None,
                latest: LatestStatus::Unresolved {
                    reason: "forgotten".into(),
                },
            },
        ];
        (info, refs, vec![])
    }

    #[test]
    fn render_show_text_snapshot() {
        let (info, refs, volumes) = fixture_show_input();
        let out = render_show_text(&info, &refs, &volumes);
        assert!(
            out.contains(
                "PROGRAM acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c"
            )
        );
        assert!(out.contains("Name           Hoymiles"));
        assert!(out.contains("Owner          0x9C2FD74F9CA2B7C4941690316B0Ebc35ce55c885"));
        assert!(
            !out.contains("Sender         0x9C2FD74F9CA2B7C4941690316B0Ebc35ce55c885"),
            "sender line should be suppressed when equal to owner"
        );
        assert!(out.contains("Resources      2 vCPUs, 4096 MiB"));
        assert!(out.contains("Pinned (use_latest=false)"));
        assert!(out.contains("Latest         ffff"));
        assert!(out.contains("Status         forgotten"));
        assert!(!out.contains("Env vars"));
        assert!(!out.contains("OTHER VOLUMES"));
    }

    #[test]
    fn render_show_text_includes_sender_when_different_from_owner() {
        let (mut info, refs, volumes) = fixture_show_input();
        info.sender = Address::from("0xDIFFERENT".to_string());
        let out = render_show_text(&info, &refs, &volumes);
        assert!(out.contains("Sender         0xDIFFERENT"));
    }

    #[test]
    fn render_show_json_contains_all_sections() {
        let info = ProgramShowInfo {
            item_hash: ItemHash::try_from(
                "acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c",
            )
            .unwrap(),
            name: Some("Hoymiles".into()),
            created_at: Timestamp::from(1_757_026_128.773),
            sender: Address::from("0x9C2FD74F9CA2B7C4941690316B0Ebc35ce55c885".to_string()),
            owner: Address::from("0x9C2FD74F9CA2B7C4941690316B0Ebc35ce55c885".to_string()),
            channel: Some(aleph_types::channel::Channel::from(
                "ALEPH-CLOUDSOLUTIONS".to_string(),
            )),
            entrypoint: "main:app".into(),
            interface: ProgramInterface::Asgi,
            encoding: "zip".into(),
            vcpus: 2,
            memory_mib: 4096,
            timeout_seconds: 30,
            internet: true,
            persistent: false,
            updatable: false,
            env_vars: Default::default(),
            payment_kind: Some("hold".into()),
            payment_chain: Some("ETH".into()),
        };
        let refs = vec![RefInfo {
            label: RefLabel::Code,
            ref_hash: ItemHash::try_from(
                "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e",
            )
            .unwrap(),
            use_latest: true,
            original: Some(StoreSummary {
                sender: Address::from("0xABC".to_string()),
                owner: Address::from("0xABC".to_string()),
                created_at: Timestamp::from(1_700_000_000.0),
            }),
            latest: LatestStatus::UpToDate,
        }];
        let volumes: Vec<NonRefVolume> = vec![];

        let value = render_show_json(&info, &refs, &volumes);
        assert_eq!(value["program"]["name"], "Hoymiles");
        assert_eq!(value["program"]["interface"], "asgi");
        assert_eq!(value["refs"][0]["kind"], "code");
        assert_eq!(value["refs"][0]["latest"]["kind"], "up_to_date");
        assert!(value["volumes"].as_array().unwrap().is_empty());
    }
}
