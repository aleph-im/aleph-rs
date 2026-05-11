use crate::cli::{
    CrnArgs, PaymentTypeCli, ProgramCommand, ProgramCreateArgs, ProgramDeleteArgs, ProgramListArgs,
    ProgramLogsArgs, ProgramPersistArgs, ProgramShowArgs, ProgramUpdateArgs, StorageEngineCli,
};
use crate::commands::instance::{
    parse_ephemeral_volumes, parse_immutable_volumes, parse_persistent_volumes,
};
use crate::common::{
    confirm_action, print_submission_result, resolve_account, resolve_address, submit_or_preview,
};
use crate::program::archive::prepare_archive;
use aleph_sdk::client::{
    AlephAggregateClient, AlephClient, AlephMessageClient, AlephStorageClient, MessageFilter,
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
use aleph_types::message::{Message, MessageContentEnum, MessageHeader, MessageType, StorageEngine};
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

    // 6. Build PROGRAM
    let mut program_builder = ProgramBuilder::new(
        &account,
        file_hash.clone(),
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
        if !json {
            eprintln!("Dry run - messages not submitted.\n");
        }
        println!("{}", serde_json::to_string_pretty(&store_pending)?);
        println!("{}", serde_json::to_string_pretty(&program_pending)?);
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
            aleph_client.upload_file_to_ipfs(archive.path()).await?;
            submit_or_preview(aleph_client, ccn_url, &store_pending, false, json).await?;
        }
    }

    // 9. Submit PROGRAM
    if !json {
        eprintln!("Publishing program message...");
    }
    submit_or_preview(aleph_client, ccn_url, &program_pending, false, json).await?;

    Ok(())
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
    let dt = ts
        .to_datetime()
        .map_err(serde::ser::Error::custom)?;
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
    Unresolved { reason: String },
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
    Ephemeral { mount: String, size_mib: u64 },
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

    // 1. Fetch the existing program message and verify ownership / amend flag.
    let program = fetch_program_message(aleph_client, &args.item_hash).await?;
    if &program.sender != account.address() {
        bail!(
            "you are not the owner of program {} (sender: {})",
            args.item_hash,
            program.sender
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
            aleph_client.upload_file_to_ipfs(archive.path()).await?;
            submit_or_preview(aleph_client, ccn_url, &store_pending, false, json).await?;
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

    let program = fetch_program_message(aleph_client, &args.item_hash).await?;
    if &program.sender != account.address() {
        bail!(
            "you are not the owner of program {} (sender: {})",
            args.item_hash,
            program.sender
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
    let new_pending = MessageBuilder::new(&account, MessageType::Program, value).build()?;

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
            true, // keep_code
            "Re-persisted",
            None,
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

    let forget = build_forget_for_program(
        &account,
        &program,
        args.keep_code,
        &args.reason,
        None, // no --channel flag on delete; pending messages inherit the default
    )?;

    let action_summary = if args.keep_code {
        format!("Forget program {} (keep code)?", args.item_hash)
    } else {
        format!("Forget program {} and its code STORE?", args.item_hash)
    };
    if !confirm_action(&action_summary, args.yes)? {
        bail!("aborted");
    }

    submit_or_preview(aleph_client, ccn_url, &forget, dry_run, json).await?;
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

/// Build the FORGET that targets a program (and optionally its code STORE).
///
/// Pure helper, easy to unit-test against a fixture: takes the already-fetched
/// PROGRAM message and produces a signed `PendingMessage` whose `hashes`
/// payload contains the program hash plus, unless `keep_code` is set, the
/// program's `code.ref` STORE hash.
fn build_forget_for_program<A: Account>(
    account: &A,
    program: &Message,
    keep_code: bool,
    reason: &str,
    channel: Option<Channel>,
) -> Result<PendingMessage> {
    let MessageContentEnum::Program(program_content) = program.content() else {
        bail!("expected PROGRAM message, got {:?}", program.message_type);
    };
    let mut hashes = vec![program.item_hash.clone()];
    if !keep_code {
        hashes.push(program_content.code.reference.clone());
    }
    let mut builder = ForgetBuilder::new(account, hashes).reason(reason);
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
    } else if let Some(reason) = unresolved_reason.clone() {
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
                let headers: Vec<aleph_types::message::MessageHeader> =
                    messages.into_iter().map(aleph_types::message::MessageHeader::from).collect();
                latest_status_from(&headers)
            }
            Err(e) => {
                let reason = format!("amend query failed: {e}");
                eprintln!(
                    "warning: cannot check latest for {}: {}",
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

async fn handle_show(
    _aleph_client: &AlephClient,
    _json: bool,
    _args: ProgramShowArgs,
) -> Result<()> {
    anyhow::bail!("aleph program show: not implemented yet")
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
    fn build_forget_includes_code_by_default() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let program: Message = serde_json::from_str(raw).unwrap();
        let account = TestAccount::new();
        let pending =
            build_forget_for_program(&account, &program, false, "User deletion", None).unwrap();
        let value: serde_json::Value = serde_json::from_str(&pending.item_content).unwrap();
        let hashes = value["hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 2);
        let hashes_str: Vec<String> = hashes
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(
            hashes_str
                .iter()
                .any(|h| h == &program.item_hash.to_string())
        );
        let MessageContentEnum::Program(content) = program.content() else {
            unreachable!()
        };
        assert!(
            hashes_str
                .iter()
                .any(|h| h == &content.code.reference.to_string())
        );
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
        assert_eq!(json["item_hash"], "acab01087137c68a5e84734e75145482651accf3bea80fb9b723b761639ecc1c");
        assert_eq!(json["interface"], "asgi");
        // Timestamp must serialize as an RFC3339 string, not a float.
        assert!(json["created_at"].is_string(), "created_at must serialize as RFC3339 string");
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
        assert_eq!(v["ref"], "9a4735bca0d3f7032ddd6659c35387b57b470550c931841e6862ece4e9e6523e");
        assert_eq!(v["latest"]["kind"], "pinned");
        // Confirm timestamps render as RFC3339 strings, not floats.
        assert!(v["original"]["created_at"].is_string(), "created_at must serialize as RFC3339 string");

        let imm = RefInfo {
            label: RefLabel::Immutable { mount: "/data".into() },
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
            NonRefVolume::Persistent { mount, size_mib, persistence, name } => {
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
        assert!(err.contains("not a STORE"), "expected wrong-type error, got: {err}");
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
        assert!(matches!(latest_status_from(&headers), LatestStatus::UpToDate));
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
    fn build_forget_keeps_code_when_flag_set() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/messages/program/program.json"
        ));
        let program: Message = serde_json::from_str(raw).unwrap();
        let account = TestAccount::new();
        let pending =
            build_forget_for_program(&account, &program, true, "User deletion", None).unwrap();
        let value: serde_json::Value = serde_json::from_str(&pending.item_content).unwrap();
        let hashes = value["hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].as_str().unwrap(), program.item_hash.to_string());
    }
}
