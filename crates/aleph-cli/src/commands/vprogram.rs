//! `aleph vprogram` command tree.

use std::path::{Path, PathBuf};

use aleph_sdk::attest::attested_request;
use aleph_sdk::client::{
    AlephClient, AlephMessageClient, AlephStorageClient, MessageWithStatus, hash_file,
};
use aleph_sdk::crn::{ActiveVmNetworking, fetch_active_vms};
use aleph_sdk::messages::{StoreBuilder, VProgramBuilder};
use aleph_sdk::scheduler::SchedulerClient;
use aleph_sdk::verify::Hasher;
use aleph_sdk::vprogram::bundle::fetch_bundle_artifacts;
use aleph_sdk::vprogram::cmdline::instantiate_cmdline;
use aleph_sdk::vprogram::manifest::RuntimeManifest;
use aleph_sdk::vprogram::measure::compute_measurements;
use aleph_sdk::vprogram::status::resolve_attested_endpoint;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::environment::{LaunchMeasurement, validate_snp_policy};
use aleph_types::message::{
    MAX_VERIFIED_VOLUMES, Message, MessageContentEnum, MessageType, StorageEngine, TeeVerification,
    VerifiableProgramContent, VerifiedVolume, VerifiedWorkload,
};
use anyhow::{Context, Result, anyhow, bail};
use memsizes::MiB;
use url::Url;

use crate::account::CliAccount;
use crate::cli::{VProgramCallArgs, VProgramCommand, VProgramCreateArgs, VProgramShowArgs};
use crate::common::{render_upload_progress, resolve_account, submit_or_preview};
use crate::config::store::ConfigStore;
use crate::veritysetup::Veritysetup;

/// Fixed RA-TLS attestation transport port advertised by the runtime manifest
/// (`aleph.ra-tls`). A future task may resolve this dynamically from the
/// manifest instead of hardcoding it here.
const ATTEST_PORT: u16 = 8443;

/// A SEV-SNP launch measurement is a SHA-384 digest: 48 bytes, 96 hex chars.
const SEV_SNP_MEASUREMENT_BYTES: usize = 48;

pub async fn dispatch(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    network_override: Option<&str>,
    json: bool,
    cmd: VProgramCommand,
) -> Result<()> {
    match cmd {
        VProgramCommand::Create(args) => {
            handle_create(aleph_client, ccn_url, network_override, json, *args).await
        }
        VProgramCommand::Show(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            handle_show(aleph_client, scheduler_url, json, args).await
        }
        VProgramCommand::Call(args) => {
            handle_call(aleph_client, network_override, json, *args).await
        }
    }
}

async fn handle_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    network_override: Option<&str>,
    json: bool,
    args: VProgramCreateArgs,
) -> Result<()> {
    // 0. Fail fast on local prerequisites before any network call.
    let veritysetup = Veritysetup::find()?;
    let account = resolve_account(&args.signing.identity)?;
    validate_snp_policy(args.policy)?;
    if args.volumes.len() > MAX_VERIFIED_VOLUMES {
        bail!("at most {MAX_VERIFIED_VOLUMES} --volume flags are supported");
    }
    if !args.workload.exists() {
        bail!("workload image not found: {}", args.workload.display());
    }
    for path in &args.volumes {
        if !path.exists() {
            bail!("volume image not found: {}", path.display());
        }
    }
    let dry_run = args.signing.dry_run;

    // 1. Runtime manifest (args.runtime is a STORE message hash).
    if !json {
        eprintln!("Fetching runtime manifest {}...", args.runtime);
    }
    let manifest_bytes = aleph_client
        .download_file_by_message_hash(&args.runtime)
        .await?
        .with_verification()
        .bytes()
        .await?;
    let manifest = RuntimeManifest::parse(&manifest_bytes)?;

    // Cheap slot check right after the manifest is known: instantiate_cmdline
    // only needs the template, the platform roothash, and how many volumes
    // were passed, so a bad template fails here instead of after verity
    // hashing and uploads. Placeholder roothashes are fine since only slot
    // presence/absence is being checked; the real cmdline is built at step 5.
    instantiate_cmdline(
        &manifest.boot.cmdline_template,
        &manifest.boot.platform_roothash,
        &"0".repeat(64),
        &vec!["0".repeat(64); args.volumes.len()],
    )?;

    // 2. Bundle artifacts (cached locally by bundle sha256).
    if !json {
        eprintln!("Fetching runtime bundle...");
    }
    let cache_dir = ConfigStore::vprogram_bundle_cache_dir()?;
    let artifacts = fetch_bundle_artifacts(aleph_client, &manifest, &cache_dir).await?;

    // 3. Verity-hash the workload and any extra volumes. Hash trees land
    //    next to the images as <name>.<ext>.verity (content-derived, so
    //    overwriting an existing one is fine).
    let workload_verity = verity_format(&veritysetup, &args.workload, json).await?;
    let mut volume_verities = Vec::new();
    for path in &args.volumes {
        volume_verities.push(verity_format(&veritysetup, path, json).await?);
    }

    // 4. Upload each data image + hash tree as STORE messages. Under
    //    --dry-run, uploads are skipped entirely: the file hash stands in
    //    for the STORE message hash so the pending message can still be
    //    previewed without ever touching the network for the upload.
    let workload_refs =
        upload_pair(aleph_client, &account, json, dry_run, &workload_verity).await?;
    let mut volume_refs = Vec::new();
    for v in &volume_verities {
        volume_refs.push(upload_pair(aleph_client, &account, json, dry_run, v).await?);
    }

    // 5. Cmdline + measurements.
    let volume_roothashes: Vec<String> = volume_verities
        .iter()
        .map(|v| v.root_hash.clone())
        .collect();
    let cmdline = instantiate_cmdline(
        &manifest.boot.cmdline_template,
        &manifest.boot.platform_roothash,
        &workload_verity.root_hash,
        &volume_roothashes,
    )?;
    if !json {
        eprintln!(
            "Computing measurements ({} cpu model(s))...",
            manifest.boot.cpu_models.len()
        );
    }
    let measurements =
        compute_measurements(&artifacts, &cmdline, args.vcpus, &manifest.boot.cpu_models)?;

    // 6. Assemble and publish.
    let verification = serde_json::from_value::<TeeVerification>(serde_json::json!({
        "backend": "sev_snp",
        "policy": args.policy,
        "measurements": measurements,
    }))?;
    let workload = serde_json::from_value::<VerifiedWorkload>(serde_json::json!({
        "ref": workload_refs.data_message,
        "hash_tree": workload_refs.tree_message,
        "roothash": workload_verity.root_hash,
    }))?;
    let mut volumes = Vec::with_capacity(volume_refs.len());
    for (refs, verity) in volume_refs.iter().zip(volume_verities.iter()) {
        volumes.push(serde_json::from_value::<VerifiedVolume>(
            serde_json::json!({
                "ref": refs.data_message,
                "hash_tree": refs.tree_message,
                "roothash": verity.root_hash,
            }),
        )?);
    }

    let wait = args.wait;
    let mut builder = VProgramBuilder::new(&account, args.runtime, workload, verification)
        .vcpus(args.vcpus)
        .memory(MiB::from(u64::from(args.memory)))
        .internet(!args.no_internet)
        .volumes(volumes);
    if let Some(crn_hash) = args.crn_hash {
        builder = builder.node_hash(crn_hash.to_string());
    }
    if let Some(channel) = args.channel {
        builder = builder.channel(Channel::from(channel));
    }
    let pending = builder.build()?;
    let vm_id = pending.item_hash.clone();

    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await?;

    // The scheduler auto-dispatches V-Programs same as instances, so creation
    // does not notify a CRN; with --wait we only poll until it is reachable.
    // Skip on --dry-run (nothing was submitted).
    if let Some(secs) = wait
        && !dry_run
    {
        let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
        let wait_timeout = std::time::Duration::from_secs(secs);
        match crate::commands::instance_wait::wait_until_ready(&scheduler_url, &vm_id, wait_timeout)
            .await?
        {
            crate::commands::instance_wait::WaitOutcome::Ready(_) => {
                let scheduler = SchedulerClient::new(scheduler_url);
                let net = fetch_live_networking(&scheduler, &vm_id).await;
                let attested_endpoint = net
                    .as_ref()
                    .and_then(|n| resolve_attested_endpoint(n, ATTEST_PORT));
                report_create_ready(&vm_id, attested_endpoint.as_ref(), json);
            }
            crate::commands::instance_wait::WaitOutcome::Timeout => {
                report_create_timeout(&vm_id, json);
            }
        }
    }
    Ok(())
}

/// Report a successful `--wait` to the user: the V-Program is reachable and
/// (usually) its attestation port is mapped. Human output goes to stderr,
/// mirroring `instance create --wait`'s `report_ready`; `--json` merges the
/// endpoint into a small JSON object instead.
fn report_create_ready(vm_id: &ItemHash, attested_endpoint: Option<&Url>, json: bool) {
    if json {
        println!("{}", create_ready_payload(attested_endpoint));
    } else {
        eprintln!("V-Program ready.");
        match attested_endpoint {
            Some(url) => eprintln!("  Attested endpoint: {url}"),
            None => eprintln!(
                "  warning: attestation port ({ATTEST_PORT}) not yet mapped by the CRN; \
                 check with `aleph vprogram show {vm_id}`"
            ),
        }
    }
}

/// Report a `--wait` timeout: the create itself succeeded, this only says the
/// V-Program is not reachable yet.
fn report_create_timeout(vm_id: &ItemHash, json: bool) {
    if json {
        println!("{}", create_timeout_payload());
    } else {
        eprintln!("warning: V-Program not reachable yet; check with `aleph vprogram show {vm_id}`");
    }
}

/// The `--json` payload for a successful `--wait`. Pure so the output shape
/// (`ready` + `attested_endpoint`), which `--json` consumers rely on, stays
/// unit-tested.
fn create_ready_payload(attested_endpoint: Option<&Url>) -> serde_json::Value {
    serde_json::json!({
        "ready": true,
        "attested_endpoint": attested_endpoint.map(|u| u.to_string()),
    })
}

/// The `--json` payload for a `--wait` timeout; same shape as
/// [`create_ready_payload`] so consumers can parse both uniformly.
fn create_timeout_payload() -> serde_json::Value {
    serde_json::json!({
        "ready": false,
        "attested_endpoint": serde_json::Value::Null,
    })
}

/// A verity-formatted data image: the original image path, the generated
/// hash tree path, and the dm-verity root hash printed by `veritysetup format`.
struct VerityArtifact {
    data: PathBuf,
    hash_tree: PathBuf,
    root_hash: String,
}

/// Run `veritysetup format` on `data`, writing the hash tree next to it as
/// `<file_name>.verity` (appending, not replacing, the existing extension).
async fn verity_format(vs: &Veritysetup, data: &Path, json: bool) -> Result<VerityArtifact> {
    if !json {
        eprintln!("Computing dm-verity hash for {}...", data.display());
    }
    let hash_tree = {
        let mut name = data
            .file_name()
            .map(|n| n.to_os_string())
            .unwrap_or_default();
        name.push(".verity");
        data.with_file_name(name)
    };
    let root_hash = vs.format(data, &hash_tree).await?;
    if !json {
        eprintln!("  Root hash: {root_hash}");
    }
    Ok(VerityArtifact {
        data: data.to_path_buf(),
        hash_tree,
        root_hash,
    })
}

/// The pair of STORE message hashes (data image + hash tree) that a
/// [`VerifiedWorkload`] or [`VerifiedVolume`] carries as `ref` / `hash_tree`.
struct UploadedPair {
    data_message: ItemHash,
    tree_message: ItemHash,
}

async fn upload_pair(
    client: &AlephClient,
    account: &CliAccount,
    json: bool,
    dry_run: bool,
    v: &VerityArtifact,
) -> Result<UploadedPair> {
    Ok(UploadedPair {
        data_message: upload_file(client, account, &v.data, json, dry_run).await?,
        tree_message: upload_file(client, account, &v.hash_tree, json, dry_run).await?,
    })
}

/// Upload one file as a STORE message on the native storage engine (default
/// payment) and return the STORE message item hash - which is what
/// `VerifiedWorkload`/`VerifiedVolume`'s `ref` and `hash_tree` fields carry.
///
/// Under `dry_run`, the network upload is skipped entirely: the file's own
/// content hash is returned as a stand-in for the STORE message hash, since
/// no STORE message is ever built or sent in that mode.
async fn upload_file(
    client: &AlephClient,
    account: &CliAccount,
    path: &Path,
    json: bool,
    dry_run: bool,
) -> Result<ItemHash> {
    if !json {
        eprintln!("Hashing {}...", path.display());
    }
    let file_hash = hash_file(path, Hasher::for_storage()).await?;
    if !json {
        eprintln!("  File hash: {file_hash}");
    }

    if dry_run {
        return Ok(file_hash);
    }

    let pending = StoreBuilder::new(account, file_hash, StorageEngine::Storage).build()?;

    if !json {
        eprintln!("Uploading {}...", path.display());
    }
    let on_tick: fn(u64, u64) = if json {
        |_, _| {}
    } else {
        render_upload_progress
    };
    let upload = client
        .upload_file_to_storage_with_progress(path, Some(&pending), true, on_tick)
        .await;
    if !json {
        eprintln!();
    }
    upload?;

    Ok(pending.item_hash)
}

// ---------------------------------------------------------------------
// `aleph vprogram show <hash>`
// ---------------------------------------------------------------------

const MISSING: &str = "-";

/// One pinned launch measurement, as shown by `render_show`.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct MeasurementSummary {
    pub platform: String,
    pub digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vcpu_type: Option<String>,
}

/// Everything `aleph vprogram show` renders: the message-side fields (always
/// present) plus the live CRN fields (present only when the scheduler has
/// placed the VM and the CRN reports it as running).
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct VProgramShow {
    pub item_hash: String,
    pub runtime_ref: String,
    pub workload_ref: String,
    pub measurements: Vec<MeasurementSummary>,
    /// Whether the CRN currently reports this VM as active. `false` when the
    /// scheduler/CRN has no record of it (not yet placed, or unreachable) -
    /// this never errors, it just means the live fields below are absent.
    pub running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_ipv4: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv4_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv6_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapped_ports: Option<std::collections::BTreeMap<u16, u16>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attested_endpoint: Option<String>,
}

/// `TeePlatform` has no public string accessor outside its own module; go
/// through its serde representation (the wire form, e.g. `"sev_snp"`)
/// instead, mirroring how `instance_show::render_text` reads `Channel`.
fn platform_str(p: aleph_types::message::execution::environment::TeePlatform) -> String {
    serde_json::to_value(p)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Pure builder: assembles the render model from a V-PROGRAM message's
/// content plus optional live CRN state. No I/O, so it is unit-testable
/// without a network.
pub(crate) fn build_show(
    item_hash: &ItemHash,
    content: &VerifiableProgramContent,
    net: Option<&ActiveVmNetworking>,
    attested_endpoint: Option<&Url>,
) -> VProgramShow {
    let measurements = content
        .verification
        .measurements
        .iter()
        .map(|m| MeasurementSummary {
            platform: platform_str(m.platform),
            digest: m.digest.clone(),
            vcpu_type: m.vcpu_type.clone(),
        })
        .collect();

    VProgramShow {
        item_hash: item_hash.to_string(),
        runtime_ref: content.runtime.reference.to_string(),
        workload_ref: content.workload.reference.to_string(),
        measurements,
        running: net.is_some(),
        host_ipv4: net.and_then(|n| n.host_ipv4.clone()),
        ipv4_ip: net.and_then(|n| n.ipv4_ip.clone()),
        ipv6_ip: net.and_then(|n| n.ipv6_ip.clone()),
        mapped_ports: net.map(|n| n.mapped_ports.iter().map(|(k, v)| (*k, v.host)).collect()),
        attested_endpoint: attested_endpoint.map(|u| u.to_string()),
    }
}

fn render_text(s: &VProgramShow) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    writeln!(out, "VPROGRAM {}", s.item_hash).unwrap();
    writeln!(out, "  Runtime        {}", s.runtime_ref).unwrap();
    writeln!(out, "  Workload       {}", s.workload_ref).unwrap();

    writeln!(out).unwrap();
    writeln!(out, "MEASUREMENTS ({})", s.measurements.len()).unwrap();
    for m in &s.measurements {
        writeln!(
            out,
            "  {} digest={} vcpu_type={}",
            m.platform,
            m.digest,
            m.vcpu_type.as_deref().unwrap_or(MISSING)
        )
        .unwrap();
    }

    writeln!(out).unwrap();
    writeln!(out, "STATUS").unwrap();
    writeln!(
        out,
        "  Running        {}",
        if s.running { "yes" } else { "no" }
    )
    .unwrap();
    writeln!(
        out,
        "  Host IPv4      {}",
        s.host_ipv4.as_deref().unwrap_or(MISSING)
    )
    .unwrap();
    writeln!(
        out,
        "  IPv4           {}",
        s.ipv4_ip.as_deref().unwrap_or(MISSING)
    )
    .unwrap();
    writeln!(
        out,
        "  IPv6           {}",
        s.ipv6_ip.as_deref().unwrap_or(MISSING)
    )
    .unwrap();
    writeln!(
        out,
        "  Attested       {}",
        s.attested_endpoint.as_deref().unwrap_or(MISSING)
    )
    .unwrap();

    if let Some(mapped) = &s.mapped_ports {
        writeln!(out).unwrap();
        writeln!(out, "MAPPED PORTS").unwrap();
        if mapped.is_empty() {
            writeln!(out, "  {MISSING}").unwrap();
        } else {
            for (vm_port, host_port) in mapped {
                writeln!(out, "  {vm_port:<5} -> {host_port}").unwrap();
            }
        }
    }

    out
}

/// Render the fetched message content (+ optional live CRN state) as either
/// a text table or pretty JSON. Pure (no I/O): [`handle_show`] does the
/// fetching and calls this.
pub(crate) fn render_show(
    item_hash: &ItemHash,
    content: &VerifiableProgramContent,
    net: Option<&ActiveVmNetworking>,
    attested_endpoint: Option<&Url>,
    json: bool,
) -> String {
    let show = build_show(item_hash, content, net, attested_endpoint);
    if json {
        serde_json::to_string_pretty(&show).expect("VProgramShow always serializes")
    } else {
        render_text(&show)
    }
}

async fn fetch_vprogram_message(
    aleph_client: &AlephClient,
    item_hash: &ItemHash,
) -> Result<Message> {
    let with_status = aleph_client
        .get_message(item_hash)
        .await
        .with_context(|| format!("failed to fetch V-Program {item_hash}"))?;
    let message = match with_status {
        MessageWithStatus::Processed { message } => message,
        MessageWithStatus::Removing { message, .. } => message,
        MessageWithStatus::Removed { .. } => {
            bail!("V-Program {item_hash} has been removed")
        }
        MessageWithStatus::Pending { .. } => {
            bail!("V-Program {item_hash} is still pending; try again in a few seconds")
        }
        MessageWithStatus::Forgotten { .. } => {
            bail!("V-Program {item_hash} has already been forgotten")
        }
        MessageWithStatus::Rejected { .. } => {
            bail!("V-Program {item_hash} was rejected by the network")
        }
    };
    if message.message_type != MessageType::VProgram {
        bail!(
            "item {item_hash} is not a V-PROGRAM message (got {:?})",
            message.message_type
        );
    }
    Ok(message)
}

/// Best-effort live-CRN lookup: resolves the scheduler placement for
/// `item_hash`, then the CRN's active-VM networking for it. Returns `None`
/// (never an error) whenever the VM isn't placed yet or any hop along the
/// way is unreachable - `render_show` treats that as "not running" and
/// shows only the message-side fields, mirroring how `instance show`
/// degrades when the scheduler/CRN is unreachable.
async fn fetch_live_networking(
    scheduler: &SchedulerClient,
    item_hash: &ItemHash,
) -> Option<ActiveVmNetworking> {
    let entry = match scheduler.get_vm(item_hash).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return None,
        Err(e) => {
            eprintln!("warning: scheduler unreachable, live status unavailable: {e}");
            return None;
        }
    };
    let node_hash = entry.allocated_node?;
    let node = match scheduler.get_node(&node_hash).await {
        Ok(Some(node)) => node,
        Ok(None) => {
            eprintln!(
                "warning: scheduler has no record of node {node_hash}; live status unavailable"
            );
            return None;
        }
        Err(e) => {
            eprintln!("warning: scheduler unreachable for node {node_hash}: {e}");
            return None;
        }
    };
    let Some(addr) = node.address.as_deref() else {
        eprintln!(
            "warning: scheduler reports no address for node {node_hash}; live status unavailable"
        );
        return None;
    };
    let crn_url = match Url::parse(addr) {
        Ok(url) => url,
        Err(e) => {
            eprintln!("warning: invalid CRN address `{addr}` for node {node_hash}: {e}");
            return None;
        }
    };
    let http = reqwest::Client::new();
    match fetch_active_vms(&http, &crn_url).await {
        Ok(list) => list.0.get(item_hash).and_then(|vm| vm.networking.clone()),
        Err(e) => {
            eprintln!("warning: CRN {crn_url} unreachable, live status unavailable: {e}");
            None
        }
    }
}

async fn handle_show(
    aleph_client: &AlephClient,
    scheduler_url: Url,
    json: bool,
    args: VProgramShowArgs,
) -> Result<()> {
    let message = fetch_vprogram_message(aleph_client, &args.item_hash).await?;
    let MessageContentEnum::VProgram(content) = message.content() else {
        bail!(
            "item {} is not a V-PROGRAM message (got {:?})",
            args.item_hash,
            message.message_type
        );
    };

    let scheduler = SchedulerClient::new(scheduler_url);
    let net = fetch_live_networking(&scheduler, &args.item_hash).await;
    let attested_endpoint = net
        .as_ref()
        .and_then(|n| resolve_attested_endpoint(n, ATTEST_PORT));

    let out = render_show(
        &args.item_hash,
        content,
        net.as_ref(),
        attested_endpoint.as_ref(),
        json,
    );
    if json {
        println!("{out}");
    } else {
        print!("{out}");
    }
    Ok(())
}

// ---------------------------------------------------------------------
// `aleph vprogram call <hash> <path>`
// ---------------------------------------------------------------------

/// The expected launch measurement(s) an attested call must match, resolved
/// by [`resolve_expected_measurement`].
///
/// - `Pin`: a single digest, known before the TLS handshake. Passed straight
///   into `attested_request`'s `expected_measurement`, so a mismatch fails
///   the handshake itself.
/// - `MemberOf`: more than one digest is pinned on the message (a
///   mixed-CPU-model fleet where different nodes measure differently).
///   Nothing can be pinned at handshake time since it isn't known in advance
///   which one the guest will present, so the handshake pins nothing and the
///   caller must check the verified measurement against this set
///   *after* `attested_request` returns - before trusting the response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MeasurementExpectation {
    Pin(Vec<u8>),
    MemberOf(Vec<Vec<u8>>),
}

/// Resolve which measurement(s) an attested call must match, per the
/// security invariant: the call is only trusted if the verified report's
/// measurement is among the ones pinned on-chain (or the explicit
/// `--expected-measurement` override).
///
/// Pure and I/O-free so it's directly unit-testable. `measurements` should
/// be `content.verification.measurements` restricted to whatever platform
/// the caller is targeting (currently always `sev_snp`, the only platform
/// V-PROGRAM messages carry).
pub(crate) fn resolve_expected_measurement(
    measurements: &[LaunchMeasurement],
    override_hex: Option<&str>,
) -> Result<MeasurementExpectation> {
    if let Some(hex_str) = override_hex {
        let bytes = hex::decode(hex_str)
            .with_context(|| format!("--expected-measurement is not valid hex: {hex_str:?}"))?;
        // A SEV-SNP launch measurement is a 48-byte SHA-384 digest; anything
        // else can never match a real report, so fail here with a clear
        // message instead of a cryptic measurement mismatch at call time.
        if bytes.len() != SEV_SNP_MEASUREMENT_BYTES {
            bail!(
                "--expected-measurement must be {} hex characters (a {}-byte SEV-SNP launch \
                 measurement), got {}",
                SEV_SNP_MEASUREMENT_BYTES * 2,
                SEV_SNP_MEASUREMENT_BYTES,
                hex_str.len()
            );
        }
        return Ok(MeasurementExpectation::Pin(bytes));
    }

    match measurements.len() {
        0 => bail!(
            "V-Program message pins no launch measurements; cannot verify attestation without \
             --expected-measurement"
        ),
        1 => {
            let bytes = hex::decode(&measurements[0].digest).with_context(|| {
                format!(
                    "message measurement digest is not valid hex: {:?}",
                    measurements[0].digest
                )
            })?;
            Ok(MeasurementExpectation::Pin(bytes))
        }
        _ => {
            let mut digests = Vec::with_capacity(measurements.len());
            for m in measurements {
                let bytes = hex::decode(&m.digest).with_context(|| {
                    format!(
                        "message measurement digest is not valid hex: {:?}",
                        m.digest
                    )
                })?;
                digests.push(bytes);
            }
            Ok(MeasurementExpectation::MemberOf(digests))
        }
    }
}

/// Parse a curl-style `-H "Key: Value"` header into `(name, value)`. Accepts
/// both `"Key: Value"` and `"Key:Value"` (splits on the first colon, trims
/// surrounding whitespace off both sides).
pub(crate) fn parse_header(raw: &str) -> Result<(String, String)> {
    let (name, value) = raw
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid header {raw:?}: expected \"Key: Value\""))?;
    let name = name.trim();
    let value = value.trim();
    if name.is_empty() {
        bail!("invalid header {raw:?}: header name is empty");
    }
    Ok((name.to_string(), value.to_string()))
}

/// Render an [`aleph_sdk::attest::AttestedResponse`] for `call`'s output.
/// Pure (no I/O), so it's unit-testable without a network or TLS server.
pub(crate) fn render_call_result(
    response: &aleph_sdk::attest::AttestedResponse,
    json: bool,
) -> String {
    if json {
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&response.body).into_owned())
        });
        // No validity flag: `attested_request` only ever returns a response
        // whose attestation verified, so the measurement is the evidence.
        let out = serde_json::json!({
            "measurement": response.measurement,
            "status": response.status,
            "body": body,
        });
        serde_json::to_string_pretty(&out).expect("call result always serializes")
    } else {
        format!(
            "HTTP {}\n{}",
            response.status,
            String::from_utf8_lossy(&response.body)
        )
    }
}

async fn handle_call(
    aleph_client: &AlephClient,
    network_override: Option<&str>,
    json: bool,
    args: VProgramCallArgs,
) -> Result<()> {
    let message = fetch_vprogram_message(aleph_client, &args.item_hash).await?;
    let MessageContentEnum::VProgram(content) = message.content() else {
        bail!(
            "item {} is not a V-PROGRAM message (got {:?})",
            args.item_hash,
            message.message_type
        );
    };

    let expected = resolve_expected_measurement(
        &content.verification.measurements,
        args.expected_measurement.as_deref(),
    )?;

    let base_url = match &args.url {
        Some(url) => url.clone(),
        None => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            let scheduler = SchedulerClient::new(scheduler_url);
            let net = fetch_live_networking(&scheduler, &args.item_hash)
                .await
                .ok_or_else(|| {
                    anyhow!(
                        "V-Program {} is not running (not yet placed on a CRN, or the \
                         scheduler/CRN is unreachable); pass --url to bypass discovery",
                        args.item_hash
                    )
                })?;
            resolve_attested_endpoint(&net, ATTEST_PORT).ok_or_else(|| {
                anyhow!(
                    "V-Program {} is running but its attestation port ({ATTEST_PORT}) is not \
                     yet mapped by the CRN; try again shortly",
                    args.item_hash
                )
            })?
        }
    };

    let headers = args
        .header
        .iter()
        .map(|h| parse_header(h))
        .collect::<Result<Vec<_>>>()?;
    let body = args.data.clone().map(bytes::Bytes::from);

    let handshake_pin = match &expected {
        MeasurementExpectation::Pin(bytes) => Some(bytes.as_slice()),
        MeasurementExpectation::MemberOf(_) => None,
    };

    let response = attested_request(
        &base_url,
        args.method.clone(),
        &args.path,
        &headers,
        body,
        handshake_pin,
        args.amd_product,
    )
    .await
    .map_err(|e| anyhow!("attestation failed: {e}"))?;

    // Post-handshake measurement checks against the now-verified (SIGNED)
    // measurement `attested_request` returns. The report chain, signature,
    // and key binding are already fully verified at this point
    // (`attested_request` fails closed on all of that).
    match &expected {
        // Multi-model fleet: the handshake pinned nothing (it couldn't know
        // which model the guest would present), so the membership check is
        // deferred to here.
        MeasurementExpectation::MemberOf(set) => {
            let got = hex::decode(&response.measurement)
                .context("attested_request returned a non-hex measurement")?;
            if !set.iter().any(|m| m == &got) {
                bail!(
                    "measurement mismatch: guest presented {} which matches none of the \
                     measurements pinned on the V-Program message",
                    response.measurement
                );
            }
        }
        // Single pin: already enforced at the handshake against the SIGNED
        // measurement (see `SnpCertVerifier::verify_server_cert`). This is a
        // belt-and-suspenders re-check on the verified value, mirroring the
        // `MemberOf` post-check above, so the trust decision never rests on a
        // single site.
        MeasurementExpectation::Pin(pin) => {
            let got = hex::decode(&response.measurement)
                .context("attested_request returned a non-hex measurement")?;
            if &got != pin {
                bail!(
                    "measurement mismatch: guest presented {} which does not match the \
                     pinned launch measurement",
                    response.measurement
                );
            }
        }
    }

    let out = render_call_result(&response, json);
    println!("{out}");
    Ok(())
}

#[cfg(test)]
mod wait_report_tests {
    use super::*;

    #[test]
    fn create_ready_payload_includes_endpoint_when_resolved() {
        let url = Url::parse("https://203.0.113.5:24101/").unwrap();
        let v = create_ready_payload(Some(&url));

        assert_eq!(v["ready"], serde_json::json!(true));
        assert_eq!(
            v["attested_endpoint"],
            serde_json::json!("https://203.0.113.5:24101/")
        );
    }

    #[test]
    fn create_ready_payload_has_explicit_null_endpoint_when_unmapped() {
        let v = create_ready_payload(None);

        assert_eq!(v["ready"], serde_json::json!(true));
        assert!(v["attested_endpoint"].is_null());
        // The key must be present (not absent) so consumers can rely on it.
        assert!(v.as_object().unwrap().contains_key("attested_endpoint"));
    }

    #[test]
    fn create_timeout_payload_mirrors_the_ready_shape() {
        let v = create_timeout_payload();

        assert_eq!(v["ready"], serde_json::json!(false));
        assert!(v["attested_endpoint"].is_null());
        assert!(v.as_object().unwrap().contains_key("attested_endpoint"));
    }
}

#[cfg(test)]
mod show_tests {
    use super::*;
    use aleph_sdk::crn::MappedPort;
    use aleph_types::message::Message;
    use std::collections::BTreeMap;

    const VPROGRAM_FIXTURE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/messages/vprogram/vprogram-credit.json"
    ));

    fn fixture_content() -> (ItemHash, VerifiableProgramContent) {
        let message: Message = serde_json::from_str(VPROGRAM_FIXTURE).expect("fixture parses");
        let MessageContentEnum::VProgram(content) = message.content().clone() else {
            panic!("fixture is not a V-PROGRAM message");
        };
        (message.item_hash.clone(), content)
    }

    fn active_networking() -> ActiveVmNetworking {
        let mut mapped_ports = BTreeMap::new();
        mapped_ports.insert(
            ATTEST_PORT,
            MappedPort {
                host: 24101,
                extra: Default::default(),
            },
        );
        ActiveVmNetworking {
            mapped_ports,
            ipv6_ip: Some("fc00::1".to_string()),
            ipv6_network: None,
            ipv4_ip: Some("172.16.7.2".to_string()),
            ipv4_network: None,
            host_ipv4: Some("203.0.113.5".to_string()),
        }
    }

    #[test]
    fn render_show_text_running_includes_expected_fields() {
        let (item_hash, content) = fixture_content();
        let net = active_networking();
        let endpoint = resolve_attested_endpoint(&net, ATTEST_PORT).expect("resolves");

        let out = render_show(&item_hash, &content, Some(&net), Some(&endpoint), false);

        assert!(out.contains("Running        yes"));
        assert!(out.contains("https://203.0.113.5:24101/"));
        assert!(out.contains(&"ab".repeat(48)), "pinned digest must appear");
        assert!(out.contains("8443"));
        assert!(out.contains("24101"));
    }

    #[test]
    fn render_show_json_running_includes_expected_fields() {
        let (item_hash, content) = fixture_content();
        let net = active_networking();
        let endpoint = resolve_attested_endpoint(&net, ATTEST_PORT).expect("resolves");

        let out = render_show(&item_hash, &content, Some(&net), Some(&endpoint), true);
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");

        assert_eq!(v["running"], serde_json::json!(true));
        assert_eq!(
            v["attested_endpoint"],
            serde_json::json!("https://203.0.113.5:24101/")
        );
        assert_eq!(v["host_ipv4"], serde_json::json!("203.0.113.5"));
        assert_eq!(v["mapped_ports"]["8443"], serde_json::json!(24101));
        assert_eq!(
            v["measurements"][0]["digest"],
            serde_json::json!("ab".repeat(48))
        );
        assert_eq!(v["runtime_ref"], serde_json::json!("cafe".repeat(16)));
    }

    #[test]
    fn render_show_without_net_does_not_panic_and_shows_message_side_fields() {
        let (item_hash, content) = fixture_content();

        let out = render_show(&item_hash, &content, None, None, false);

        assert!(out.contains("Running        no"));
        assert!(out.contains(&"ab".repeat(48)));
        // No live fields to show, so the mapped-ports section is absent.
        assert!(!out.contains("MAPPED PORTS"));

        let out_json = render_show(&item_hash, &content, None, None, true);
        let v: serde_json::Value = serde_json::from_str(&out_json).expect("valid json");
        assert_eq!(v["running"], serde_json::json!(false));
        assert!(v.get("host_ipv4").is_none());
        assert!(v.get("mapped_ports").is_none());
        assert!(v.get("attested_endpoint").is_none());
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aleph_sdk::attest::AttestedResponse;

    fn measurement(digest: &str, vcpu_type: Option<&str>) -> LaunchMeasurement {
        let json = serde_json::json!({
            "platform": "sev_snp",
            "digest": digest,
            "vcpu_type": vcpu_type,
        });
        serde_json::from_value(json).expect("valid measurement fixture")
    }

    #[test]
    fn resolve_expected_measurement_rejects_wrong_length_override() {
        let err = resolve_expected_measurement(&[], Some("ab"))
            .expect_err("a 1-byte override must be rejected");
        assert!(
            err.to_string().contains("96 hex characters"),
            "error should state the expected length, got: {err}"
        );
    }

    #[test]
    fn resolve_expected_measurement_accepts_a_full_length_override() {
        let digest = "cd".repeat(48);
        let expected = resolve_expected_measurement(&[], Some(&digest)).unwrap();
        assert_eq!(
            expected,
            MeasurementExpectation::Pin(hex::decode(&digest).unwrap())
        );
    }

    #[test]
    fn resolve_expected_measurement_pins_the_single_measurement() {
        let digest = "ab".repeat(48);
        let measurements = vec![measurement(&digest, Some("EPYC-v4"))];

        let expected = resolve_expected_measurement(&measurements, None).unwrap();

        assert_eq!(
            expected,
            MeasurementExpectation::Pin(hex::decode(&digest).unwrap())
        );
    }

    #[test]
    fn resolve_expected_measurement_override_takes_precedence() {
        let digest = "ab".repeat(48);
        let override_digest = "cd".repeat(48);
        let measurements = vec![measurement(&digest, Some("EPYC-v4"))];

        let expected = resolve_expected_measurement(&measurements, Some(&override_digest)).unwrap();

        assert_eq!(
            expected,
            MeasurementExpectation::Pin(hex::decode(&override_digest).unwrap())
        );
    }

    #[test]
    fn resolve_expected_measurement_multiple_yields_member_of_set() {
        let digest_a = "ab".repeat(48);
        let digest_b = "cd".repeat(48);
        let measurements = vec![
            measurement(&digest_a, Some("EPYC-v4")),
            measurement(&digest_b, Some("EPYC-Genoa")),
        ];

        let expected = resolve_expected_measurement(&measurements, None).unwrap();

        assert_eq!(
            expected,
            MeasurementExpectation::MemberOf(vec![
                hex::decode(&digest_a).unwrap(),
                hex::decode(&digest_b).unwrap(),
            ])
        );
    }

    #[test]
    fn resolve_expected_measurement_zero_measurements_errors() {
        let result = resolve_expected_measurement(&[], None);

        assert!(
            result.is_err(),
            "a message pinning no measurements must fail closed without --expected-measurement"
        );
    }

    #[test]
    fn resolve_expected_measurement_bad_override_hex_errors() {
        let digest = "ab".repeat(48);
        let measurements = vec![measurement(&digest, Some("EPYC-v4"))];

        let result = resolve_expected_measurement(&measurements, Some("not-hex"));

        assert!(result.is_err());
    }

    #[test]
    fn parse_header_splits_on_colon_and_trims() {
        assert_eq!(
            parse_header("X-Y: z").unwrap(),
            ("X-Y".to_string(), "z".to_string())
        );
        assert_eq!(
            parse_header("X-Y:z").unwrap(),
            ("X-Y".to_string(), "z".to_string())
        );
        assert_eq!(
            parse_header("  X-Y  :   z  ").unwrap(),
            ("X-Y".to_string(), "z".to_string())
        );
    }

    #[test]
    fn parse_header_rejects_missing_colon() {
        assert!(parse_header("no-colon-here").is_err());
    }

    #[test]
    fn parse_header_rejects_empty_name() {
        assert!(parse_header(": value").is_err());
    }

    fn dummy_response(measurement: &str, body: &[u8]) -> AttestedResponse {
        AttestedResponse {
            measurement: measurement.to_string(),
            status: 200,
            headers: vec![],
            body: bytes::Bytes::copy_from_slice(body),
        }
    }

    #[test]
    fn render_call_result_json_parses_json_body() {
        let response = dummy_response(&"ab".repeat(48), br#"{"fib":55}"#);

        let out = render_call_result(&response, true);
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");

        assert!(
            v.get("attestation_valid").is_none(),
            "no redundant always-true validity flag in call output"
        );
        assert_eq!(v["measurement"], serde_json::json!("ab".repeat(48)));
        assert_eq!(v["status"], serde_json::json!(200));
        assert_eq!(v["body"]["fib"], serde_json::json!(55));
    }

    #[test]
    fn render_call_result_json_falls_back_to_string_body_for_non_json() {
        let response = dummy_response(&"ab".repeat(48), b"plain text body");

        let out = render_call_result(&response, true);
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");

        assert_eq!(v["body"], serde_json::json!("plain text body"));
    }

    #[test]
    fn render_call_result_text_includes_status_and_body() {
        let response = dummy_response(&"ab".repeat(48), b"55");

        let out = render_call_result(&response, false);

        assert!(out.contains("HTTP 200"));
        assert!(out.contains("55"));
    }
}
