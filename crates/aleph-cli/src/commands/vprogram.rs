//! `aleph vprogram` command tree.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::instance::{InstanceRow, format_item_hash_short, format_node_short};
use super::instance_target::{VmKind, pick_unique_match};
use aleph_sdk::aggregate_models::vm_images::{
    VPROGRAM_CONTRACT_COMPOSE, VPROGRAM_MODEL_COMPOSE, VPROGRAM_MODEL_EXEC, VmImagesData,
};
use aleph_sdk::attest::{
    MeasurementPin, PlatformPolicy, PlatformPosture, PolicyPin, attested_request,
};
use aleph_sdk::caching_aggregate_client::CachingAggregateClient;
use aleph_sdk::client::{
    AlephAggregateClient, AlephClient, AlephMessageClient, AlephStorageClient, MessageWithStatus,
};
use aleph_sdk::crn::{ActiveVmNetworking, fetch_active_vms};
use aleph_sdk::messages::{ForgetBuilder, VProgramBuilder};
use aleph_sdk::scheduler::SchedulerClient;
use aleph_sdk::vprogram::bundle::fetch_bundle_artifacts;
use aleph_sdk::vprogram::cmdline::instantiate_cmdline;
use aleph_sdk::vprogram::manifest::{RuntimeManifest, WorkloadSpec};
use aleph_sdk::vprogram::measure::compute_measurements;
use aleph_sdk::vprogram::status::resolve_attested_endpoint;
use aleph_types::account::Account;
use aleph_types::chain::Address;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::environment::{
    LaunchMeasurement, SevSnpRegisters, validate_snp_policy,
};
use aleph_types::message::{
    MAX_VERIFIED_VOLUMES, Message, MessageContentEnum, MessageType, TeeVerification,
    VerifiableProgramContent, VerifiedVolume, VerifiedWorkload,
};
use anyhow::{Context, Result, anyhow, bail};
use memsizes::MiB;
use url::Url;

use crate::account::CliAccount;
use crate::cli::{
    ImageRef, PlatformRequirement, VProgramCallArgs, VProgramCommand, VProgramCreateArgs,
    VProgramDeleteArgs, VProgramListArgs, VProgramShowArgs,
};
use crate::common::{
    confirm_action, resolve_account, resolve_address, resolve_address_or_active, submit_or_preview,
};
use crate::compose;
use crate::config::store::ConfigStore;
use crate::container::ContainerTool;
use crate::mkfs::MkfsExt4;
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
        VProgramCommand::List(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            handle_list(aleph_client, scheduler_url, json, args).await
        }
        VProgramCommand::Call(args) => {
            handle_call(aleph_client, network_override, json, *args).await
        }
        VProgramCommand::Delete(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            handle_delete(aleph_client, ccn_url, scheduler_url, json, args).await
        }
        VProgramCommand::Logs(args) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            super::crn::handle_logs(scheduler_url, json, args, VmKind::VProgram).await
        }
    }
}

/// Build the FORGET for a V-PROGRAM message. Only the V-PROGRAM hash is
/// forgotten: the runtime bundle and workload STORE messages may be shared
/// with other deployments and stay untouched.
fn build_forget_for_vprogram<A: Account>(
    account: &A,
    vprogram: &Message,
    reason: &str,
) -> Result<aleph_types::message::pending::PendingMessage> {
    if vprogram.message_type != MessageType::VProgram {
        bail!(
            "expected V-PROGRAM message, got {:?}",
            vprogram.message_type
        );
    }
    Ok(
        ForgetBuilder::new(account, vec![vprogram.item_hash.clone()])
            .reason(reason)
            .build()?,
    )
}

async fn handle_delete(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    scheduler_url: Url,
    json: bool,
    args: VProgramDeleteArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    let scheduler = SchedulerClient::new(scheduler_url);
    let item_hash = resolve_vprogram_id(&scheduler, &args.vm_id).await?;
    let message = fetch_vprogram_message(aleph_client, &item_hash).await?;
    if &message.sender != account.address() {
        bail!(
            "you are not the owner of V-Program {item_hash} (sender: {})",
            message.sender
        );
    }

    let prompt = format!("Forget V-Program {item_hash}? This is irreversible.");
    if !dry_run && !confirm_action(&prompt, args.yes)? {
        bail!("aborted");
    }

    let pending = build_forget_for_vprogram(&account, &message, &args.reason)?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
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
    check_debug_policy(args.policy, args.allow_debug)?;
    if args.volumes.len() > MAX_VERIFIED_VOLUMES {
        bail!("at most {MAX_VERIFIED_VOLUMES} --volume flags are supported");
    }
    let compose_input = match (&args.workload, &args.compose) {
        (Some(path), None) => {
            if !path.exists() {
                bail!("workload image not found: {}", path.display());
            }
            None
        }
        (None, Some(compose_path)) => {
            let text = std::fs::read_to_string(compose_path)
                .with_context(|| format!("reading compose file {}", compose_path.display()))?;
            let validated = compose::parse_and_validate(&text, args.volumes.len())?;
            for w in &validated.warnings {
                eprintln!("warning: {w}");
            }
            let archives = parse_image_archives(&args.image_archives)?;
            for path in archives.values() {
                if !path.exists() {
                    bail!("image archive not found: {}", path.display());
                }
            }
            let images = compose::image_names(&validated.file);
            check_archive_keys_are_known_images(&archives, &images)?;
            check_archives_do_not_cover_digest_images(&archives)?;
            let mkfs = MkfsExt4::find()?;
            let needs_pull = images.iter().any(|i| !archives.contains_key(i));
            let container = if needs_pull {
                Some(ContainerTool::find()?)
            } else {
                None
            };
            Some((validated, archives, mkfs, container))
        }
        _ => unreachable!("clap enforces exactly one of --workload/--compose"),
    };
    for path in &args.volumes {
        if !path.exists() {
            bail!("volume image not found: {}", path.display());
        }
    }
    let dry_run = args.signing.dry_run;

    // Resolve --crn up front so a typo or ambiguous fragment fails
    // before any verity hashing or uploads. A full hash passes through
    // without a scheduler round-trip.
    let crn_hash = match args.crn.as_deref() {
        Some(input) => {
            let scheduler_url = crate::common::resolve_scheduler_url(network_override)?;
            Some(super::instance_target::resolve_node_hash(&scheduler_url, input).await?)
        }
        None => None,
    };

    // 1. Runtime manifest: --runtime is a STORE message hash, a contract or
    //    runtime name from the vm-images aggregate, or absent (the model's
    //    current contract, then its default runtime). The aggregate is only
    //    fetched when a hash is not given.
    let model = if compose_input.is_some() {
        VPROGRAM_MODEL_COMPOSE
    } else {
        VPROGRAM_MODEL_EXEC
    };
    let vm_images = if matches!(args.runtime, Some(ImageRef::Hash(_))) {
        VmImagesData::default()
    } else {
        CachingAggregateClient::new(aleph_client)
            .get_vm_images_aggregate()
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to fetch vm-images aggregate: {e}. \
                     As a fallback, pass --runtime with the runtime manifest's item hash."
                )
            })?
            .vm_images
    };
    let runtime = resolve_vprogram_runtime(args.runtime.clone(), model, &vm_images)?;
    if !json {
        eprintln!("Fetching runtime manifest {}...", runtime.hash);
    }
    let manifest_bytes = aleph_client
        .download_file_by_message_hash(&runtime.hash)
        .await?
        .with_verification()
        .bytes()
        .await?;
    let manifest = RuntimeManifest::parse(&manifest_bytes)?;

    match &runtime.contract {
        // Catalogue-resolved: the manifest must implement exactly the
        // contract the aggregate claims for it.
        Some(contract) => check_contract_matches(contract, manifest.workload.as_ref())?,
        // Raw hash: only the model-level gates apply.
        None if compose_input.is_some() => check_compose_contract(manifest.workload.as_ref())?,
        None => check_exec_contract(manifest.workload.as_ref())?,
    }
    if !json {
        eprintln!("{}", runtime_identity_line(&runtime, &manifest));
    }

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

    // Materialize the workload image: either the prebuilt path from
    // --workload, or (for --compose) pull/resolve/save every image not
    // covered by --image-archive, stage the compose file verbatim, and build
    // an ext4 image from it. Network access (image pulls) only happens here,
    // after every cheap local/manifest gate above has already passed.
    //
    // `_built_workload` (the compose-built ext4 image) and
    // `_built_workload_dir` (its containing tempdir) are `None` for
    // --workload, where the file is caller-owned. Both are bound by this
    // `let`, in this function's scope, which is what keeps them alive - and
    // their backing files un-deleted - until after `upload_pair` runs; a
    // narrower scope (e.g. dropping them at the end of the match arm) would
    // delete the image before it could be uploaded.
    let (workload_path, _built_workload, _built_workload_dir): (
        PathBuf,
        Option<tempfile::NamedTempFile>,
        Option<tempfile::TempDir>,
    ) = match compose_input {
        None => (
            args.workload.clone().expect("clap: workload set"),
            None,
            None,
        ),
        Some((mut validated, archives, mkfs, container)) => {
            let mut resolved: Vec<(String, PathBuf)> = Vec::new();
            let mut save_tmp = Vec::new(); // keep pulled archives alive until staged
            for image in compose::image_names(&validated.file) {
                if let Some(path) = archives.get(&image) {
                    resolved.push((image, path.clone()));
                } else {
                    let tool = container.as_ref().expect("find() ran when pulls needed");
                    if !json {
                        eprintln!("Pulling {image}...");
                    }
                    let tmp = tempfile::Builder::new().suffix(".tar").tempfile()?;
                    // The archive is always saved from a tag, never a digest
                    // reference (#348): only a tag survives the save/load
                    // round trip that the guest's `podman load` performs. A
                    // digest reference is pulled by digest (the engine
                    // enforces the declared hash and fails loudly on
                    // mismatch) and staged under its deterministic pinned
                    // tag; a tag reference is saved as-is and the resolved
                    // digest reported for provenance. Either way the image
                    // bytes are pinned by the verity-measured workload
                    // volume.
                    if let Some(tag) = compose::pinned_tag(&image) {
                        tool.pull_and_save_pinned(&image, &tag, tmp.path()).await?;
                        if !json {
                            eprintln!("Pulled {image} (staged as {tag})");
                        }
                        resolved.push((tag, tmp.path().to_path_buf()));
                    } else {
                        let digest = tool.pull_and_save(&image, tmp.path()).await?;
                        if !json {
                            eprintln!("Pulled {image} ({digest})");
                        }
                        resolved.push((image, tmp.path().to_path_buf()));
                    }
                    save_tmp.push(tmp);
                }
            }
            // Stage tag references verbatim and digest references as their
            // pinned tag, matching the tags embedded in the archives.
            compose::retag_digest_images(&mut validated.file);
            let yaml = compose::to_yaml(&validated.file)?;
            let (dir, image) = compose::build_workload_image(&mkfs, &yaml, &resolved).await?;
            let path = image.path().to_path_buf();
            drop(save_tmp); // archives are copied into the image; safe to drop now
            (path, Some(image), Some(dir))
        }
    };

    // 3. Verity-hash the workload and any extra volumes. Hash trees are
    //    build artifacts, not user files, so they go in a tempdir rather
    //    than next to a caller-owned --workload/--volume path. `verity_dir`
    //    must outlive the uploads in step 4: it is bound here, in this
    //    function's scope, for the same reason as `_built_workload_dir`.
    let verity_dir = tempfile::tempdir().context("creating verity scratch dir")?;
    let workload_verity = verity_format(
        &veritysetup,
        &workload_path,
        &verity_dir.path().join("workload.verity"),
        json,
    )
    .await?;
    let mut volume_verities = Vec::new();
    for (i, path) in args.volumes.iter().enumerate() {
        volume_verities.push(
            verity_format(
                &veritysetup,
                path,
                &verity_dir.path().join(format!("volume-{i}.verity")),
                json,
            )
            .await?,
        );
    }

    // 4. Upload each data image + hash tree as STORE messages. Under
    //    --dry-run, uploads are skipped entirely: the file hash stands in
    //    for the STORE message hash so the pending message can still be
    //    previewed without ever touching the network for the upload.
    let owner = args
        .on_behalf_of
        .as_deref()
        .map(resolve_address)
        .transpose()?;
    let workload_refs = upload_pair(
        aleph_client,
        &account,
        owner.as_ref(),
        json,
        dry_run,
        &workload_verity,
    )
    .await?;
    let mut volume_refs = Vec::new();
    for v in &volume_verities {
        volume_refs
            .push(upload_pair(aleph_client, &account, owner.as_ref(), json, dry_run, v).await?);
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
    let mut builder = VProgramBuilder::new(&account, runtime.hash, workload, verification)
        .vcpus(args.vcpus)
        .memory(MiB::from(u64::from(args.memory)))
        .internet(!args.no_internet)
        .volumes(volumes)
        .metadata(std::collections::HashMap::from([(
            "name".to_string(),
            serde_json::json!(args.name),
        )]));
    if let Some(crn_hash) = crn_hash {
        builder = builder.node_hash(crn_hash.to_string());
    }
    if let Some(channel) = args.channel {
        builder = builder.channel(Channel::from(channel));
    }
    if let Some(owner) = owner {
        builder = builder.on_behalf_of(owner);
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
        let wait_started = std::time::Instant::now();
        match crate::commands::instance_wait::wait_until_ready(&scheduler_url, &vm_id, wait_timeout)
            .await?
        {
            crate::commands::instance_wait::WaitOutcome::Ready(_) => {
                let scheduler = SchedulerClient::new(scheduler_url);
                if !json {
                    eprintln!("V-Program reachable; waiting for the attestation port mapping...");
                }
                let attested_endpoint = poll_attested_endpoint(
                    || fetch_attested_endpoint(&scheduler, &vm_id),
                    tokio::time::sleep,
                    wait_timeout.saturating_sub(wait_started.elapsed()),
                    crate::commands::instance_wait::WAIT_POLL_INTERVAL,
                )
                .await;
                report_create_ready(&vm_id, attested_endpoint.as_ref(), json);
            }
            crate::commands::instance_wait::WaitOutcome::Timeout => {
                report_create_timeout(&vm_id, json);
            }
        }
    }
    Ok(())
}

/// What `--runtime` resolved to. `contract` / `label` are `None` when the
/// user pinned a raw manifest hash (nothing in the aggregate was consulted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedRuntime {
    pub hash: ItemHash,
    pub contract: Option<String>,
    /// The catalogue runtime name, for display.
    pub label: Option<String>,
}

/// Resolve `--runtime` for workload `model` against an in-memory
/// `VmImagesData`. Pure: does no network I/O.
pub(crate) fn resolve_vprogram_runtime(
    runtime: Option<ImageRef>,
    model: &str,
    data: &VmImagesData,
) -> Result<ResolvedRuntime> {
    let selector = match runtime {
        Some(ImageRef::Hash(hash)) => {
            return Ok(ResolvedRuntime {
                hash,
                contract: None,
                label: None,
            });
        }
        Some(ImageRef::Preset(name)) => Some(name),
        None => None,
    };
    let hint = |found: &str| match found {
        VPROGRAM_MODEL_COMPOSE => " (did you mean --compose?)",
        VPROGRAM_MODEL_EXEC => " (did you mean --workload?)",
        _ => "",
    };
    let resolved = data.resolve_vprogram_runtime(model, selector.as_deref(), hint)?;
    Ok(ResolvedRuntime {
        hash: resolved.hash,
        contract: Some(resolved.contract),
        label: Some(resolved.runtime),
    })
}

/// One-line description of the runtime a create resolved to, e.g.
/// `Using runtime exec-1.0 (aleph.exec/1; aleph-snp-attest 2026.08.20, aleph-vm@ba690c65)`.
/// Falls back to the hash when no catalogue entry was involved and omits
/// the provenance when the manifest carries no `source` block.
pub(crate) fn runtime_identity_line(
    runtime: &ResolvedRuntime,
    manifest: &RuntimeManifest,
) -> String {
    let mut details = String::new();
    if let Some(w) = &manifest.workload {
        details.push_str(&format!("{}; ", w.contract));
    }
    details.push_str(&format!("{} {}", manifest.name, manifest.version));
    if let Some(source) = &manifest.source {
        let repo = source
            .repo
            .as_deref()
            .map(|r| r.trim_end_matches('/'))
            .and_then(|r| r.rsplit('/').next())
            .filter(|r| !r.is_empty());
        match (repo, source.rev.as_deref()) {
            (Some(repo), Some(rev)) => details.push_str(&format!(", {repo}@{rev}")),
            (Some(repo), None) => details.push_str(&format!(", {repo}")),
            (None, Some(rev)) => details.push_str(&format!(", rev {rev}")),
            (None, None) => {}
        }
    }
    let what = runtime
        .label
        .clone()
        .unwrap_or_else(|| runtime.hash.to_string());
    format!("Using runtime {what} ({details})")
}

/// Refuse a catalogue-resolved runtime whose manifest does not declare the
/// contract the aggregate lists it under: either the aggregate is wrong or
/// the bundle was swapped, and both mean the workload would not boot.
pub(crate) fn check_contract_matches(
    expected: &str,
    workload: Option<&WorkloadSpec>,
) -> Result<()> {
    match workload {
        Some(w) if w.contract == expected => Ok(()),
        Some(w) => bail!(
            "the vm-images aggregate lists this runtime under workload contract {expected:?}, \
             but its manifest declares {:?}",
            w.contract
        ),
        None => bail!(
            "the vm-images aggregate lists this runtime under workload contract {expected:?}, \
             but its manifest declares no workload contract"
        ),
    }
}

/// Refuse a plain (--workload) create against a runtime that declares the
/// compose contract: such a bundle expects a compose-built workload volume
/// and would fail to boot a raw image. Pure and I/O-free.
pub(crate) fn check_exec_contract(workload: Option<&WorkloadSpec>) -> Result<()> {
    match workload {
        Some(w) if w.contract == VPROGRAM_CONTRACT_COMPOSE => bail!(
            "this runtime declares workload contract {VPROGRAM_CONTRACT_COMPOSE:?}; build the \
             workload with --compose instead of --workload"
        ),
        _ => Ok(()),
    }
}

/// Refuse --compose against a runtime that does not declare the compose
/// workload contract, so a compose workload cannot land on e.g. a builtin
/// runtime. Pure and I/O-free so it's directly unit-testable.
pub(crate) fn check_compose_contract(workload: Option<&WorkloadSpec>) -> Result<()> {
    match workload {
        Some(w) if w.contract == VPROGRAM_CONTRACT_COMPOSE => Ok(()),
        Some(w) => bail!(
            "--compose requires a runtime declaring workload contract \
             {VPROGRAM_CONTRACT_COMPOSE:?}, but this runtime declares {:?}",
            w.contract
        ),
        None => bail!(
            "--compose requires a runtime declaring workload contract \
             {VPROGRAM_CONTRACT_COMPOSE:?}, but this runtime manifest declares no \
             workload contract"
        ),
    }
}

/// Parse an --image-archive spec, "IMAGE=PATH", split on the first '='
/// (image references may contain ':' and '/' but never '=', so the first
/// '=' unambiguously separates image from path).
pub(crate) fn parse_image_archive(spec: &str) -> Result<(String, PathBuf)> {
    match spec.split_once('=') {
        Some((image, path)) if !image.is_empty() && !path.is_empty() => {
            Ok((image.to_string(), PathBuf::from(path)))
        }
        _ => bail!("invalid --image-archive {spec:?}; expected IMAGE=PATH"),
    }
}

/// Parse every repeated --image-archive spec into a map, rejecting a
/// duplicate IMAGE key instead of letting the last one silently win (a plain
/// `.collect::<BTreeMap<_, _>>()` would do that). Pure and I/O-free so it's
/// directly unit-testable.
pub(crate) fn parse_image_archives(specs: &[String]) -> Result<BTreeMap<String, PathBuf>> {
    let mut archives = BTreeMap::new();
    for spec in specs {
        let (image, path) = parse_image_archive(spec)?;
        if let Some(previous) = archives.insert(image.clone(), path) {
            bail!(
                "duplicate --image-archive for {image:?} (already mapped to {}); \
                 each IMAGE may be supplied once",
                previous.display()
            );
        }
    }
    Ok(archives)
}

/// Error out if any --image-archive key does not match a compose `image:`
/// value: an unmatched key currently falls back to a registry pull for that
/// image with no indication the archive was ignored (invisible under
/// --json). Pure and I/O-free so it's directly unit-testable.
pub(crate) fn check_archive_keys_are_known_images(
    archives: &BTreeMap<String, PathBuf>,
    images: &[String],
) -> Result<()> {
    let known: std::collections::BTreeSet<&str> = images.iter().map(String::as_str).collect();
    let unknown: Vec<&str> = archives
        .keys()
        .map(String::as_str)
        .filter(|key| !known.contains(key))
        .collect();
    if unknown.is_empty() {
        Ok(())
    } else {
        bail!(
            "--image-archive key(s) {unknown:?} do not match any compose `image:` value \
             ({images:?}); IMAGE must match the compose file's image string exactly"
        );
    }
}

/// Error out if an --image-archive key is a digest reference: the archive's
/// bytes carry no registry digest to verify the declared identity against,
/// so accepting it would stage the digest claim unenforced. Pure and
/// I/O-free so it's directly unit-testable.
pub(crate) fn check_archives_do_not_cover_digest_images(
    archives: &BTreeMap<String, PathBuf>,
) -> Result<()> {
    for image in archives.keys() {
        if compose::pinned_tag(image).is_some() {
            bail!(
                "--image-archive cannot supply {image:?}: the image is referenced by \
                 digest, and a prebuilt archive cannot be verified against it; drop \
                 --image-archive for it so the digest-enforced pull runs, or reference \
                 the image by tag"
            );
        }
    }
    Ok(())
}

/// One sample of the VM's attested endpoint via the scheduler + CRN.
async fn fetch_attested_endpoint(scheduler: &SchedulerClient, vm_id: &ItemHash) -> Option<Url> {
    let net = fetch_live_networking(scheduler, vm_id).await?;
    resolve_attested_endpoint(&net, ATTEST_PORT)
}

/// Poll `fetch` until it yields an attested endpoint, or until `timeout`
/// elapses (always sampling at least once).
///
/// The CRN maps the attestation port to a host port only after the guest
/// finishes booting, which for a SEV-SNP V-Program (runtime bundle download
/// and measured boot) comes well after the scheduler/networking readiness
/// that `--wait` observes first; a single sample taken right at readiness
/// would nearly always miss the mapping. Generic over `fetch` and `sleep`,
/// mirroring `instance_wait::poll_until_ready`, so tests drive it without a
/// network or a clock.
async fn poll_attested_endpoint<F, Fut, S, SFut>(
    mut fetch: F,
    mut sleep: S,
    timeout: std::time::Duration,
    poll_interval: std::time::Duration,
) -> Option<Url>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<Url>>,
    S: FnMut(std::time::Duration) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    let start = std::time::Instant::now();
    loop {
        if let Some(endpoint) = fetch().await {
            return Some(endpoint);
        }
        if start.elapsed() >= timeout {
            return None;
        }
        sleep(poll_interval).await;
    }
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

/// Run `veritysetup format` on `data`, writing the hash tree to `hash_tree`.
async fn verity_format(
    vs: &Veritysetup,
    data: &Path,
    hash_tree: &Path,
    json: bool,
) -> Result<VerityArtifact> {
    if !json {
        eprintln!("Computing dm-verity hash for {}...", data.display());
    }
    let root_hash = vs.format(data, hash_tree).await?;
    if !json {
        eprintln!("  Root hash: {root_hash}");
    }
    Ok(VerityArtifact {
        data: data.to_path_buf(),
        hash_tree: hash_tree.to_path_buf(),
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
    owner: Option<&Address>,
    json: bool,
    dry_run: bool,
    v: &VerityArtifact,
) -> Result<UploadedPair> {
    Ok(UploadedPair {
        data_message: super::upload::upload_file(client, account, owner, &v.data, json, dry_run)
            .await?,
        tree_message: super::upload::upload_file(
            client,
            account,
            owner,
            &v.hash_tree,
            json,
            dry_run,
        )
        .await?,
    })
}

// ---------------------------------------------------------------------
// `aleph vprogram show <hash>`
// ---------------------------------------------------------------------

const MISSING: &str = "-";

/// One pinned launch measurement, as shown by `render_show`.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct MeasurementSummary {
    pub platform: String,
    pub registers: BTreeMap<String, String>,
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
    pub resources: ShowResources,
    pub internet: bool,
    pub storage: ShowStorage,
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

/// Compute resources pinned on the message. A V-PROGRAM has no disk
/// allocation: every disk it boots from is a read-only dm-verity image
/// (see [`ShowStorage`]), and writable scratch is guest tmpfs out of memory.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub(crate) struct ShowResources {
    pub vcpus: u32,
    pub memory_mib: u64,
}

/// One read-only artifact the VM boots from, with its size from the CCN's
/// storage metadata when it is known.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub(crate) struct ShowArtifact {
    #[serde(rename = "ref")]
    pub reference: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// Disk footprint of the workload image and verified volumes (data images
/// only; hash trees are a few percent on top). This is the figure the
/// scheduler checks against a node's free disk before placing the VM.
/// `total_bytes` is only present when every artifact's size is known.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub(crate) struct ShowStorage {
    pub workload: ShowArtifact,
    pub volumes: Vec<ShowArtifact>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
}

/// Artifact sizes keyed by STORE message hash, as resolved by
/// [`fetch_artifact_sizes`]. Missing entries render as unknown.
pub(crate) type ArtifactSizes = std::collections::HashMap<ItemHash, u64>;

/// Pure builder: assembles the render model from a V-PROGRAM message's
/// content plus optional live CRN state and artifact sizes. No I/O, so it
/// is unit-testable without a network.
pub(crate) fn build_show(
    item_hash: &ItemHash,
    content: &VerifiableProgramContent,
    net: Option<&ActiveVmNetworking>,
    attested_endpoint: Option<&Url>,
    sizes: &ArtifactSizes,
) -> VProgramShow {
    let artifact = |reference: &ItemHash, comment: Option<&str>| ShowArtifact {
        reference: reference.to_string(),
        comment: comment.filter(|c| !c.is_empty()).map(str::to_string),
        size_bytes: sizes.get(reference).copied(),
    };
    let workload = artifact(&content.workload.reference, None);
    let volumes: Vec<ShowArtifact> = content
        .volumes
        .iter()
        .map(|v| artifact(&v.reference, Some(&v.comment)))
        .collect();
    let total_bytes = std::iter::once(&workload)
        .chain(volumes.iter())
        .map(|a| a.size_bytes)
        .try_fold(0u64, |acc, size| size.map(|s| acc.saturating_add(s)));
    let storage = ShowStorage {
        workload,
        volumes,
        total_bytes,
    };

    let measurements = content
        .verification
        .measurements
        .iter()
        .map(|m| MeasurementSummary {
            platform: m.platform.as_str().to_string(),
            // rendered as a map so every platform's register set shows
            // without a per-platform arm here
            registers: m
                .registers
                .entries()
                .into_iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            vcpu_type: m.vcpu_type.clone(),
        })
        .collect();

    VProgramShow {
        item_hash: item_hash.to_string(),
        runtime_ref: content.runtime.reference.to_string(),
        workload_ref: content.workload.reference.to_string(),
        resources: ShowResources {
            vcpus: content.base.resources.vcpus,
            memory_mib: u64::from(content.base.resources.memory),
        },
        internet: content.environment.internet,
        storage,
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
    writeln!(
        out,
        "  Resources      {} vCPUs, {} MiB",
        s.resources.vcpus, s.resources.memory_mib
    )
    .unwrap();
    writeln!(
        out,
        "  Internet       {}",
        if s.internet { "yes" } else { "no" }
    )
    .unwrap();

    writeln!(out).unwrap();
    writeln!(out, "STORAGE").unwrap();
    let size_or_missing =
        |size: Option<u64>| size.map(format_size).unwrap_or_else(|| MISSING.to_string());
    writeln!(
        out,
        "  Workload       {}  {}",
        s.storage.workload.reference,
        size_or_missing(s.storage.workload.size_bytes)
    )
    .unwrap();
    for volume in &s.storage.volumes {
        write!(
            out,
            "  Volume         {}  {}",
            volume.reference,
            size_or_missing(volume.size_bytes)
        )
        .unwrap();
        if let Some(comment) = &volume.comment {
            write!(out, "  {comment}").unwrap();
        }
        writeln!(out).unwrap();
    }
    writeln!(
        out,
        "  Total          {}",
        size_or_missing(s.storage.total_bytes)
    )
    .unwrap();

    writeln!(out).unwrap();
    writeln!(out, "MEASUREMENTS ({})", s.measurements.len()).unwrap();
    for m in &s.measurements {
        let registers = m
            .registers
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(" ");
        writeln!(
            out,
            "  {} {} vcpu_type={}",
            m.platform,
            registers,
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

/// Human-readable binary size with one decimal (`512.0 MiB`, `1.2 GiB`);
/// plain bytes below 1 KiB.
pub(crate) fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// Render the fetched message content (+ optional live CRN state and
/// artifact sizes) as either a text table or pretty JSON. Pure (no I/O):
/// [`handle_show`] does the fetching and calls this.
pub(crate) fn render_show(
    item_hash: &ItemHash,
    content: &VerifiableProgramContent,
    net: Option<&ActiveVmNetworking>,
    attested_endpoint: Option<&Url>,
    sizes: &ArtifactSizes,
    json: bool,
) -> String {
    let show = build_show(item_hash, content, net, attested_endpoint, sizes);
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

/// Best-effort artifact size lookup via the CCN's storage metadata
/// endpoint (`/api/v0/storage/by-message-hash/<ref>`), which always
/// carries the file size; the STORE message's own `size` field is often
/// absent. Refs whose metadata cannot be fetched are skipped with a stderr
/// warning and the caller renders them as unknown. Lookups run concurrently.
async fn fetch_artifact_sizes(aleph_client: &AlephClient, refs: &[ItemHash]) -> ArtifactSizes {
    let lookups = refs.iter().map(|reference| async move {
        match aleph_client
            .get_file_metadata_by_message_hash(reference)
            .await
        {
            Ok(meta) => Some((reference.clone(), u64::from(meta.size))),
            Err(e) => {
                eprintln!(
                    "warning: could not fetch metadata for artifact {reference}: {e}; size unknown"
                );
                None
            }
        }
    });
    futures_util::future::join_all(lookups)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Resolve a user-supplied V-Program id (full item hash or a prefix such as
/// the 12-char form printed by `aleph vprogram list`) to its item hash. A
/// full hash is returned as-is without any network call; a prefix is
/// expanded server-side by the scheduler (restricted to V-Programs, so an
/// instance sharing the prefix is not a collision) and must match exactly
/// one VM.
async fn resolve_vprogram_id(scheduler: &SchedulerClient, input: &str) -> Result<ItemHash> {
    if let Ok(hash) = ItemHash::try_from(input) {
        return Ok(hash);
    }
    let matches = scheduler
        .find_vms_by_hash_prefix_and_type(input, VmKind::VProgram.scheduler_vm_type())
        .await
        .with_context(|| format!("looking up VMs matching prefix `{input}` in the scheduler"))?;
    pick_unique_match(input, matches, VmKind::VProgram).map(|(hash, _)| hash)
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
    let crn_url = match crate::common::parse_crn_address(addr, &node_hash.to_string()) {
        Some(url) => url,
        None => return None,
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
    let scheduler = SchedulerClient::new(scheduler_url);
    let item_hash = resolve_vprogram_id(&scheduler, &args.vm_id).await?;
    let message = fetch_vprogram_message(aleph_client, &item_hash).await?;
    let MessageContentEnum::VProgram(content) = message.content() else {
        bail!(
            "item {item_hash} is not a V-PROGRAM message (got {:?})",
            message.message_type
        );
    };

    let artifact_refs: Vec<ItemHash> = std::iter::once(&content.workload.reference)
        .chain(content.volumes.iter().map(|v| &v.reference))
        .cloned()
        .collect();
    let (net, sizes) = tokio::join!(
        fetch_live_networking(&scheduler, &item_hash),
        fetch_artifact_sizes(aleph_client, &artifact_refs),
    );
    let attested_endpoint = net
        .as_ref()
        .and_then(|n| resolve_attested_endpoint(n, ATTEST_PORT));

    let out = render_show(
        &item_hash,
        content,
        net.as_ref(),
        attested_endpoint.as_ref(),
        &sizes,
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
// `aleph vprogram list`
// ---------------------------------------------------------------------

/// Same pipeline as `aleph instance list` (CCN sender+owner queries, bulk
/// scheduler enrichment, best-effort CRN networking), filtered on V-PROGRAM
/// messages. The text table swaps the IPv6 column for the attested (RA-TLS)
/// endpoint, which is what a V-PROGRAM is reached through.
async fn handle_list(
    aleph_client: &AlephClient,
    scheduler_url: Url,
    json: bool,
    args: VProgramListArgs,
) -> Result<()> {
    use super::instance::{
        enrich_by_sender, enrich_rows_with_ips, fetch_scheduler_map, fetch_vm_rows,
        merge_scheduler_into_rows,
    };

    // Read-only: resolve the address from the manifest without loading the
    // account (loading an encrypted account would prompt for its password).
    let address = resolve_address_or_active(args.address.as_deref())?;

    let mut rows = fetch_vm_rows(aleph_client, &address, MessageType::VProgram).await?;

    let scheduler = SchedulerClient::new(scheduler_url);
    let mut scheduler_map = fetch_scheduler_map(&scheduler, &address).await;
    enrich_by_sender(&scheduler, &address, &rows, &mut scheduler_map).await;
    merge_scheduler_into_rows(&mut rows, &scheduler_map);
    enrich_rows_with_ips(&scheduler, &mut rows).await;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&format_list_json(&rows))?
        );
    } else {
        print!("{}", format_list_text(&rows));
    }
    Ok(())
}

const MISSING_VALUE: &str = "-";

/// Attested endpoint for a row, when the CRN reported networking that maps
/// the attestation port.
fn row_attested_endpoint(row: &InstanceRow) -> Option<Url> {
    row.networking
        .as_ref()
        .and_then(|net| resolve_attested_endpoint(net, ATTEST_PORT))
}

fn format_list_json(rows: &[InstanceRow]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "item_hash": r.item_hash.to_string(),
                "name": r.name,
                "owner": r.owner.to_string(),
                "node_hash": r.node_hash,
                "attested_endpoint": row_attested_endpoint(r).map(|u| u.to_string()),
                "ipv4": r.ipv4,
                "created_at": r.created_at
                    .to_datetime()
                    .ok()
                    .map(|dt| dt.to_rfc3339()),
                "scheduler": r.scheduler_raw,
            })
        })
        .collect();
    serde_json::Value::Array(items)
}

fn format_list_text(rows: &[InstanceRow]) -> String {
    use std::fmt::Write;

    const HASH_HEADER: &str = "ITEM_HASH";
    const NAME_HEADER: &str = "NAME";
    const OWNER_HEADER: &str = "OWNER";
    const STATUS_HEADER: &str = "STATUS";
    const ALLOC_HEADER: &str = "ALLOCATED";
    const ENDPOINT_HEADER: &str = "ENDPOINT";

    fn width<'a>(header: &str, values: impl Iterator<Item = &'a str>) -> usize {
        values.map(str::len).fold(header.len(), usize::max)
    }

    let names: Vec<&str> = rows
        .iter()
        .map(|r| r.name.as_deref().unwrap_or(MISSING_VALUE))
        .collect();
    let owners: Vec<String> = rows.iter().map(|r| r.owner.to_string()).collect();
    let statuses: Vec<&str> = rows
        .iter()
        .map(|r| r.status.as_deref().unwrap_or(MISSING_VALUE))
        .collect();
    let allocated: Vec<String> = rows
        .iter()
        .map(|r| {
            r.allocated_node
                .as_deref()
                .map(format_node_short)
                .unwrap_or_else(|| MISSING_VALUE.to_string())
        })
        .collect();
    let endpoints: Vec<String> = rows
        .iter()
        .map(|r| {
            row_attested_endpoint(r)
                .map(|u| u.to_string())
                .unwrap_or_else(|| MISSING_VALUE.to_string())
        })
        .collect();

    let hash_w = HASH_HEADER.len().max(12);
    let name_w = width(NAME_HEADER, names.iter().copied());
    let owner_w = width(OWNER_HEADER, owners.iter().map(String::as_str));
    let status_w = width(STATUS_HEADER, statuses.iter().copied());
    let alloc_w = width(ALLOC_HEADER, allocated.iter().map(String::as_str));

    let mut out = String::new();
    writeln!(
        out,
        "{HASH_HEADER:<hash_w$}  {NAME_HEADER:<name_w$}  {OWNER_HEADER:<owner_w$}  \
         {STATUS_HEADER:<status_w$}  {ALLOC_HEADER:<alloc_w$}  {ENDPOINT_HEADER}"
    )
    .expect("writing to String cannot fail");

    for (i, row) in rows.iter().enumerate() {
        let hash = format_item_hash_short(&row.item_hash);
        writeln!(
            out,
            "{hash:<hash_w$}  {:<name_w$}  {:<owner_w$}  {:<status_w$}  {:<alloc_w$}  {}",
            names[i], owners[i], statuses[i], allocated[i], endpoints[i],
        )
        .expect("writing to String cannot fail");
    }
    out
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
    Pin(SevSnpRegisters),
    MemberOf(Vec<SevSnpRegisters>),
}

/// Resolve which measurement(s) an attested call must match, per the
/// security invariant: the call is only trusted if the verified report's
/// measurement is among the ones pinned on-chain (or the explicit
/// `--expected-measurement` override).
///
/// Pure and I/O-free so it's directly unit-testable. Pass
/// `content.verification.measurements` as-is: an entry for a platform this
/// client cannot verify fails the resolution rather than being skipped.
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
        // Re-encode so the pin compares structurally against the lowercase
        // hex the SDK derives from the signed report.
        return Ok(MeasurementExpectation::Pin(SevSnpRegisters {
            launch: hex::encode(bytes),
        }));
    }

    // The message's registers are the pin, same type both sides. An entry
    // this client cannot check (another platform's register set) fails
    // closed rather than being skipped, since skipping would narrow the
    // accepted set silently. Unreachable through a valid message today: the
    // schema restricts the V-PROGRAM backend to sev_snp.
    let snp_registers: Vec<SevSnpRegisters> = measurements
        .iter()
        .map(|m| {
            m.registers.as_sev_snp().cloned().ok_or_else(|| {
                anyhow!(
                    "the message pins a {} measurement, which this client cannot verify",
                    m.platform.as_str()
                )
            })
        })
        .collect::<Result<_>>()?;

    match snp_registers.len() {
        0 => bail!(
            "V-Program message pins no launch measurements; cannot verify attestation without \
             --expected-measurement"
        ),
        1 => Ok(MeasurementExpectation::Pin(
            snp_registers.into_iter().next().expect("len checked"),
        )),
        _ => Ok(MeasurementExpectation::MemberOf(snp_registers)),
    }
}

/// True if `policy` has the SEV-SNP DEBUG bit (19) set: the host may then
/// decrypt guest memory via the firmware debug API, so the deployment is
/// not confidential in any meaningful sense.
pub(crate) fn policy_debug_allowed(policy: u64) -> bool {
    const SNP_POLICY_DEBUG_BIT: u64 = 1 << 19;
    policy & SNP_POLICY_DEBUG_BIT != 0
}

/// Reject a DEBUG-enabled policy unless the caller explicitly acknowledged
/// it with `--allow-debug`. When the DEBUG bit is set and acknowledged, emit
/// a loud warning so the operator knows the deployment is not confidential.
///
/// Extracted from `handle_create` so the guard logic is unit-testable without
/// a network client.
pub(crate) fn check_debug_policy(policy: u64, allow_debug: bool) -> Result<()> {
    if !policy_debug_allowed(policy) {
        return Ok(());
    }
    if !allow_debug {
        bail!(
            "--policy {:#x} has the SEV-SNP DEBUG bit (19) set: the host will be able to \
             decrypt guest memory, so this deployment will NOT be confidential. \
             Pass --allow-debug to acknowledge and publish anyway",
            policy
        );
    }
    eprintln!(
        "warning: --policy {:#x} has the SEV-SNP DEBUG bit (19) set: the host will be \
         able to decrypt guest memory, so this deployment will NOT be confidential",
        policy
    );
    Ok(())
}

/// Pure: fold an override patch onto every silicon line's floor of the
/// network policy, gating any lowering behind `accept_outdated`. The
/// lowering gate fires if the patch lowers ANY line's floor (a `--min-tcb`
/// between the Zen4c and classic floors still weakens the classic gate,
/// and the silicon line is only known once a report is in hand). I/O-free
/// so it is directly unit-testable.
pub(crate) fn resolve_effective_floor(
    network: &aleph_sdk::attest::TcbFloorPolicy,
    override_patch: Option<&aleph_sdk::attest::TcbFloorOverride>,
    accept_outdated: bool,
) -> Result<aleph_sdk::attest::TcbFloorPolicy> {
    let Some(patch) = override_patch else {
        return Ok(*network);
    };
    let (eff_default, mut lowered) = patch.apply_to(&network.default);
    let eff_x_variant = network.x_variant.as_ref().map(|floor| {
        let (eff, lowered_x) = patch.apply_to(floor);
        lowered.extend(lowered_x);
        eff
    });
    let eff_zen4c = network.zen4c.as_ref().map(|floor| {
        let (eff, lowered_zen4c) = patch.apply_to(floor);
        lowered.extend(lowered_zen4c);
        eff
    });
    lowered.sort();
    lowered.dedup();
    if !lowered.is_empty() {
        if !accept_outdated {
            bail!(
                "--min-tcb lowers {lowered:?} below the network floor; \
                 pass --accept-outdated-tcb to accept the risk"
            );
        }
        eprintln!(
            "warning: accepting a TCB below the network floor for {lowered:?}: the node \
             runs known-outdated firmware, so the guest may be exposed"
        );
    }
    Ok(aleph_sdk::attest::TcbFloorPolicy {
        default: eff_default,
        x_variant: eff_x_variant,
        zen4c: eff_zen4c,
    })
}

/// Resolve the network floor policy (per-family builtin baselines raised by
/// the settings aggregate), then apply the override. Aggregate failure falls
/// back to the baselines with a warning.
async fn resolve_tcb_floor(
    aleph_client: &AlephClient,
    product: aleph_sdk::attest::AmdProduct,
    override_patch: Option<&aleph_sdk::attest::TcbFloorOverride>,
    accept_outdated: bool,
) -> Result<aleph_sdk::attest::TcbFloorPolicy> {
    let baseline = aleph_sdk::attest::builtin_baseline_policy(product);
    let network = match aleph_client.get_settings_aggregate().await {
        Ok(agg) => match agg.settings.snp_min_tcb.floor_for(product) {
            Some(f) => baseline.raise_to(&f),
            None => baseline,
        },
        Err(e) => {
            eprintln!(
                "warning: could not fetch the network TCB floor ({e}); using the built-in baseline"
            );
            baseline
        }
    };
    resolve_effective_floor(&network, override_patch, accept_outdated)
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

/// Structured `--json` representation of a TCB view or floor:
/// `{"fmc": <null|n>, "bootloader": n, "tee": n, "snp": n, "microcode": n}`.
/// Keys match the component names `--min-tcb` accepts (so input and evidence
/// are symmetric), and `fmc` is `null` off Turin. Structured rather than a
/// flat string so downstream consumers of `--json` need no ad-hoc parsing.
fn tcb_json(fmc: Option<u8>, bootloader: u8, tee: u8, snp: u8, microcode: u8) -> serde_json::Value {
    serde_json::json!({
        "fmc": fmc,
        "bootloader": bootloader,
        "tee": tee,
        "snp": snp,
        "microcode": microcode,
    })
}

fn tcb_floor_json(floor: &aleph_sdk::attest::TcbFloor) -> serde_json::Value {
    tcb_json(
        floor.fmc,
        floor.bootloader,
        floor.tee,
        floor.snp,
        floor.microcode,
    )
}

/// Whether this call's attestation evidence includes a verified
/// fresh-nonce liveness challenge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Freshness {
    Verified,
    Skipped,
}

/// Transfer the liveness proof from the challenge exchange to the workload
/// exchange: both must have been answered by the same TLS identity
/// presenting the same measured stack. Guards against a load balancer (or
/// an attacker) splitting the two requests across different guests.
///
/// This is also what pins the measurement/policy of the fresh report in the
/// `MeasurementExpectation::MemberOf` fleet case: there the handshake pin is
/// `None` (the fleet's exact model is not known ahead of the call), so the
/// fresh report carries no pin of its own. Closing the loop against
/// `response.measurement`/`response.policy`, which the caller's
/// post-handshake checks already validated, is what pins it instead.
fn check_fresh_consistency(
    fresh: &aleph_sdk::attest::FreshAttestation,
    response: &aleph_sdk::attest::AttestedResponse,
) -> Result<()> {
    if fresh.served_public_key != response.served_public_key {
        bail!(
            "the fresh attestation challenge was answered by a different TLS identity \
             than the one that served the response; refusing to transfer liveness"
        );
    }
    if fresh.registers != response.registers {
        bail!(
            "fresh report launch measurement {} does not match the response's verified \
             launch measurement {}",
            fresh.registers.launch,
            response.registers.launch
        );
    }
    if fresh.policy != response.policy {
        bail!(
            "fresh report policy {:#x} does not match the response's verified policy {:#x}",
            fresh.policy,
            response.policy
        );
    }
    Ok(())
}

/// Build the SDK's [`PlatformPolicy`] from the `--require-platform` values.
/// An empty list is [`PlatformPolicy::NONE`]: posture is surfaced, never
/// gated (the current fleet fails every bit, so requiring is opt-in).
fn platform_policy_from(requirements: &[PlatformRequirement]) -> PlatformPolicy {
    let mut policy = PlatformPolicy::NONE;
    for requirement in requirements {
        match requirement {
            PlatformRequirement::SmtOff => policy.require_smt_disabled = true,
            PlatformRequirement::Tsme => policy.require_tsme = true,
            PlatformRequirement::RaplOff => policy.require_rapl_disabled = true,
            PlatformRequirement::CiphertextHiding => policy.require_ciphertext_hiding = true,
            PlatformRequirement::AliasCheck => policy.require_alias_check = true,
        }
    }
    policy
}

/// Render a [`PlatformPosture`] as the one-line text form used in the meta
/// block, e.g. `SMT=on TSME=off ECC=off RAPL=on ciphertext-hiding=off
/// alias-check=no (0x1)`. RAPL renders enablement (`on` = telemetry active),
/// matching how an operator reads the risk.
fn platform_posture_line(p: &PlatformPosture) -> String {
    fn on(b: bool) -> &'static str {
        if b { "on" } else { "off" }
    }
    format!(
        "SMT={} TSME={} ECC={} RAPL={} ciphertext-hiding={} alias-check={} ({:#x})",
        on(p.smt_enabled),
        on(p.tsme_enabled),
        on(p.ecc_enabled),
        on(!p.rapl_disabled),
        on(p.ciphertext_hiding_enabled),
        if p.alias_check_complete { "yes" } else { "no" },
        p.raw,
    )
}

/// The one-line attestation verdict for text mode. Every listed check is
/// enforced fail-closed upstream (`attested_request` plus the CLI re-checks
/// in `handle_call`), so the line only ever describes a verified response:
/// the point is to tell the reader WHAT "verified" covers, not whether.
fn attestation_verdict_line(freshness: Freshness) -> String {
    let checks = "AMD SEV-SNP; certificate chain and report signature, TLS key binding, \
                  launch measurement pinned, guest policy pinned, TCB floor";
    match freshness {
        Freshness::Verified => format!("verified ({checks}, fresh nonce)"),
        Freshness::Skipped => {
            format!("verified ({checks}; fresh nonce SKIPPED by --allow-stale-attestation)")
        }
    }
}

/// `component=value` rendering of a TCB for the verbose text meta.
fn tcb_line(fmc: Option<u8>, bootloader: u8, tee: u8, snp: u8, microcode: u8) -> String {
    let mut parts = Vec::new();
    if let Some(fmc) = fmc {
        parts.push(format!("fmc={fmc}"));
    }
    parts.push(format!("bootloader={bootloader}"));
    parts.push(format!("tee={tee}"));
    parts.push(format!("snp={snp}"));
    parts.push(format!("microcode={microcode}"));
    parts.join(" ")
}

/// Render an [`aleph_sdk::attest::AttestedResponse`] for `call`'s output as
/// `(stdout, stderr_meta)`. In text mode stdout is the raw response body so
/// the command pipes like curl; stderr gets a one-line attestation verdict
/// naming every check that passed, the `HTTP <status>` line, and (with
/// `verbose`) the evidence: measurement, policy, launch TCB, platform
/// posture. In JSON mode everything is in the stdout document and there is no meta line,
/// and the effective TCB floor (the one selected for the guest's silicon
/// family, from the report's signed CPUID fields) plus the verified
/// launch/reported TCB are included as evidence alongside the
/// measurement/policy.
/// Pure (no I/O), so it's unit-testable without a network or TLS server.
pub(crate) fn render_call_result(
    response: &aleph_sdk::attest::AttestedResponse,
    min_tcb: &aleph_sdk::attest::TcbFloorPolicy,
    freshness: Freshness,
    json: bool,
    verbose: bool,
) -> (String, Option<String>) {
    if json {
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&response.body).into_owned())
        });
        // No validity flag: `attested_request` only ever returns a response
        // whose attestation verified, so the measurement is the evidence.
        let out = serde_json::json!({
            "registers": response.registers,
            "policy": format!("{:#x}", response.policy),
            "effective_tcb_floor": tcb_floor_json(min_tcb.for_silicon(response.cpuid_family, response.cpuid_model, response.cpuid_stepping)),
            "cpuid": {
                "family": response.cpuid_family,
                "model": response.cpuid_model,
                "stepping": response.cpuid_stepping,
            },
            "launch_tcb": tcb_json(
                response.launch_tcb.fmc,
                response.launch_tcb.bootloader,
                response.launch_tcb.tee,
                response.launch_tcb.snp,
                response.launch_tcb.microcode,
            ),
            "reported_tcb": tcb_json(
                response.reported_tcb.fmc,
                response.reported_tcb.bootloader,
                response.reported_tcb.tee,
                response.reported_tcb.snp,
                response.reported_tcb.microcode,
            ),
            "platform_info": {
                "raw": format!("{:#x}", response.platform.raw),
                "smt_enabled": response.platform.smt_enabled,
                "tsme_enabled": response.platform.tsme_enabled,
                "ecc_enabled": response.platform.ecc_enabled,
                "rapl_disabled": response.platform.rapl_disabled,
                "ciphertext_hiding_enabled": response.platform.ciphertext_hiding_enabled,
                "alias_check_complete": response.platform.alias_check_complete,
            },
            "status": response.status,
            "body": body,
            "freshness": match freshness {
                Freshness::Verified => "verified",
                Freshness::Skipped => "skipped",
            },
        });
        (
            serde_json::to_string_pretty(&out).expect("call result always serializes"),
            None,
        )
    } else {
        let mut meta = format!(
            "Attestation: {}\nHTTP {}",
            attestation_verdict_line(freshness),
            response.status
        );
        if verbose {
            meta.push_str(&format!(
                "\nmeasurement: {}\npolicy: {:#x}\nlaunch TCB: {}\nplatform: {}",
                response.registers.launch,
                response.policy,
                tcb_line(
                    response.launch_tcb.fmc,
                    response.launch_tcb.bootloader,
                    response.launch_tcb.tee,
                    response.launch_tcb.snp,
                    response.launch_tcb.microcode,
                ),
                platform_posture_line(&response.platform),
            ));
        }
        (
            String::from_utf8_lossy(&response.body).into_owned(),
            Some(meta),
        )
    }
}

async fn handle_call(
    aleph_client: &AlephClient,
    network_override: Option<&str>,
    json: bool,
    args: VProgramCallArgs,
) -> Result<()> {
    // The scheduler is only consulted when something actually needs it (a
    // hash prefix, or endpoint discovery without --url), so a full hash plus
    // --url keeps working even when no scheduler is configured.
    let scheduler =
        || crate::common::resolve_scheduler_url(network_override).map(SchedulerClient::new);
    let item_hash = match ItemHash::try_from(args.vm_id.as_str()) {
        Ok(hash) => hash,
        Err(_) => resolve_vprogram_id(&scheduler()?, &args.vm_id).await?,
    };
    let message = fetch_vprogram_message(aleph_client, &item_hash).await?;
    let MessageContentEnum::VProgram(content) = message.content() else {
        bail!(
            "item {item_hash} is not a V-PROGRAM message (got {:?})",
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
            let net = fetch_live_networking(&scheduler()?, &item_hash)
                .await
                .ok_or_else(|| {
                    anyhow!(
                        "V-Program {} is not running (not yet placed on a CRN, or the \
                         scheduler/CRN is unreachable); pass --url to bypass discovery",
                        item_hash
                    )
                })?;
            resolve_attested_endpoint(&net, ATTEST_PORT).ok_or_else(|| {
                anyhow!(
                    "V-Program {} is running but its attestation port ({ATTEST_PORT}) is not \
                     yet mapped by the CRN; try again shortly",
                    item_hash
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
        MeasurementExpectation::Pin(registers) => MeasurementPin::Exact(registers),
        // Fleet flow: the exact model is only known from the response, so
        // the handshake pin is explicitly deferred; the MemberOf allow-list
        // check below is what discharges the CallerVerified obligation.
        MeasurementExpectation::MemberOf(_) => MeasurementPin::CallerVerified,
    };
    let policy_pin = PolicyPin::Exact(content.verification.policy);
    let platform_policy = platform_policy_from(&args.require_platform);

    let min_tcb = resolve_tcb_floor(
        aleph_client,
        args.amd_product,
        args.min_tcb.as_ref(),
        args.accept_outdated_tcb,
    )
    .await?;

    // Fresh-nonce liveness challenge (G4a): runs first and fails closed. No
    // response is trusted or surfaced unless this challenge and the served-key
    // consistency check both pass. A live-key-copy MITM can still receive the
    // request body; only the response is gated (a stated limit in the design doc).
    let fresh = if args.allow_stale_attestation {
        None
    } else {
        Some(
            aleph_sdk::attest::fresh_attestation(
                &base_url,
                handshake_pin,
                policy_pin,
                args.amd_product,
                &min_tcb,
                &platform_policy,
            )
            .await
            .map_err(|e| anyhow!("fresh attestation challenge failed: {e}"))?,
        )
    };

    let response = attested_request(
        &base_url,
        args.method.clone(),
        &args.path,
        &headers,
        body,
        handshake_pin,
        policy_pin,
        args.amd_product,
        &min_tcb,
        &platform_policy,
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
            if !set.contains(&response.registers) {
                bail!(
                    "measurement mismatch: guest presented {} which matches none of the \
                     measurements pinned on the V-Program message",
                    response.registers.launch
                );
            }
        }
        // Single pin: already enforced at the handshake against the SIGNED
        // measurement (see `SnpCertVerifier::verify_server_cert`). This is a
        // belt-and-suspenders re-check on the verified value, mirroring the
        // `MemberOf` post-check above, so the trust decision never rests on a
        // single site.
        MeasurementExpectation::Pin(pin) => {
            if &response.registers != pin {
                bail!(
                    "measurement mismatch: guest presented {} which does not match the \
                     pinned launch measurement",
                    response.registers.launch
                );
            }
        }
    }

    // Policy re-check on the verified value, mirroring the measurement
    // re-check above: the handshake already pinned the SIGNED policy, but
    // the trust decision never rests on a single site. The policy is not
    // part of the launch measurement, so this (and the handshake pin) is
    // what stops a host from launching the measured stack with a weaker
    // policy, e.g. debug allowed.
    if response.policy != content.verification.policy {
        bail!(
            "guest policy mismatch: the V-Program message pins {:#x}, but the guest was \
             launched with {:#x}",
            content.verification.policy,
            response.policy
        );
    }
    if policy_debug_allowed(response.policy) {
        eprintln!(
            "warning: this V-Program's guest policy ({:#x}) allows debugging: the host \
             can decrypt guest memory, so the response is not confidential",
            response.policy
        );
    }

    // Belt-and-suspenders TCB re-check on the verified evidence, mirroring
    // the measurement/policy re-checks above: `attested_request` already
    // enforced `min_tcb` internally (its `check_tcb_floor` gates all four TCB
    // views), but the trust decision never rests on a single site. This
    // re-check deliberately covers only `launch_tcb`: under Option A that is
    // the view that decides whether the VM was launched under a safe TCB, so
    // it is the load-bearing one to re-assert here. The floor is selected
    // from the response's (signed) CPUID family/model/stepping, like the SDK
    // did.
    let applied_floor = min_tcb.for_silicon(
        response.cpuid_family,
        response.cpuid_model,
        response.cpuid_stepping,
    );
    if let Err(defs) = applied_floor.satisfied_by(&response.launch_tcb) {
        bail!("guest launch TCB is below the required floor: {defs:?}");
    }

    let freshness = match &fresh {
        Some(fresh) => {
            check_fresh_consistency(fresh, &response)?;
            Freshness::Verified
        }
        None => Freshness::Skipped,
    };

    // Posture is what --require-platform gates on, so it is worth showing
    // whenever the user asked for a requirement, verbose or not.
    let verbose = args.verbose || !args.require_platform.is_empty();
    let (out, meta) = render_call_result(&response, &min_tcb, freshness, json, verbose);
    if let Some(meta) = meta {
        eprintln!("{meta}");
    }
    println!("{out}");
    Ok(())
}

#[cfg(test)]
mod wait_endpoint_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn resolves_once_the_mapping_appears() {
        let calls = AtomicUsize::new(0);
        let url = Url::parse("https://203.0.113.5:24101/").unwrap();

        let got = poll_attested_endpoint(
            || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let url = url.clone();
                async move { (n >= 2).then_some(url) }
            },
            |_| async {},
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .await;

        assert_eq!(got, Some(url));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn samples_at_least_once_then_gives_up_at_the_deadline() {
        let calls = AtomicUsize::new(0);
        let sleeps = AtomicUsize::new(0);

        let got = poll_attested_endpoint(
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { None }
            },
            |_| {
                sleeps.fetch_add(1, Ordering::SeqCst);
                async {}
            },
            Duration::ZERO,
            Duration::from_secs(5),
        )
        .await;

        assert_eq!(got, None);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "an exhausted budget still samples once"
        );
        assert_eq!(sleeps.load(Ordering::SeqCst), 0);
    }
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
mod list_tests {
    use super::*;
    use aleph_types::message::Message;

    fn fixture_message() -> Message {
        serde_json::from_str(show_tests::VPROGRAM_FIXTURE).expect("fixture parses")
    }

    use aleph_types::account::SignError;
    use aleph_types::chain::{Chain, Signature};

    /// Minimal test account that produces a dummy signature. Mirrors the
    /// `TestAccount` in `commands/instance.rs` tests.
    struct TestAccount {
        address: Address,
    }

    impl Account for TestAccount {
        fn chain(&self) -> Chain {
            Chain::Ethereum
        }
        fn address(&self) -> &Address {
            &self.address
        }
        fn sign_raw(&self, _buffer: &[u8]) -> Result<Signature, SignError> {
            Ok(Signature::from("0xDUMMY".to_string()))
        }
    }

    #[test]
    fn build_forget_for_vprogram_targets_only_the_vprogram_hash() {
        let message = fixture_message();
        let account = TestAccount {
            address: Address::from("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".to_string()),
        };
        let pending = build_forget_for_vprogram(&account, &message, "User deletion").unwrap();
        assert_eq!(pending.message_type, MessageType::Forget);
        let value: serde_json::Value = serde_json::from_str(&pending.item_content).unwrap();
        let hashes = value["hashes"].as_array().unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].as_str().unwrap(), message.item_hash.to_string());
        assert_eq!(value["reason"], "User deletion");
    }

    #[test]
    fn extract_vm_row_accepts_a_vprogram_message() {
        let message = fixture_message();
        let row = super::super::instance::extract_vm_row(&message).expect("row extracted");
        assert_eq!(row.item_hash, message.item_hash);
        assert_eq!(&row.owner, message.owner());
        assert!(row.status.is_none());
        assert!(row.networking.is_none());
    }

    fn placed_row() -> InstanceRow {
        let message = fixture_message();
        let mut row = super::super::instance::extract_vm_row(&message).expect("row extracted");
        row.status = Some("dispatched".to_string());
        row.allocated_node = Some("node-0123456789abcdef".to_string());
        row.networking = Some(show_tests::active_networking());
        row
    }

    #[test]
    fn format_list_text_shows_endpoint_and_placeholders() {
        let placed = placed_row();
        let mut unplaced = placed.clone();
        unplaced.status = None;
        unplaced.allocated_node = None;
        unplaced.networking = None;

        let out = format_list_text(&[placed.clone(), unplaced]);
        let mut lines = out.lines();
        let header = lines.next().expect("header");
        assert!(header.starts_with("ITEM_HASH"));
        assert!(header.ends_with("ENDPOINT"));
        assert!(!header.contains("IPV6"));

        let first = lines.next().expect("placed row");
        assert!(first.starts_with(&format_item_hash_short(&placed.item_hash)));
        assert!(first.contains("dispatched"));
        assert!(
            first.contains("6789abcdef"),
            "allocated node is shortened to its last 10 chars"
        );
        assert!(first.ends_with("https://203.0.113.5:24101/"));

        let second = lines.next().expect("unplaced row");
        let cells: Vec<&str> = second.split_whitespace().collect();
        assert_eq!(
            &cells[cells.len() - 3..],
            ["-", "-", "-"],
            "got: {second:?}"
        );
    }

    #[test]
    fn format_list_json_shape() {
        let placed = placed_row();
        let v = format_list_json(std::slice::from_ref(&placed));
        let item = &v.as_array().expect("array")[0];
        assert_eq!(item["item_hash"], placed.item_hash.to_string());
        assert_eq!(item["owner"], placed.owner.to_string());
        assert_eq!(item["attested_endpoint"], "https://203.0.113.5:24101/");
        assert_eq!(item["ipv4"], serde_json::Value::Null);
        assert!(item["created_at"].is_string());
        assert!(item.get("scheduler").is_some());
    }
}

#[cfg(test)]
mod show_tests {
    use super::*;
    use aleph_sdk::crn::MappedPort;
    use aleph_types::message::Message;
    use std::collections::BTreeMap;

    pub(super) const VPROGRAM_FIXTURE: &str = include_str!(concat!(
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

    pub(super) fn active_networking() -> ActiveVmNetworking {
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

        let out = render_show(
            &item_hash,
            &content,
            Some(&net),
            Some(&endpoint),
            &ArtifactSizes::new(),
            false,
        );

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

        let out = render_show(
            &item_hash,
            &content,
            Some(&net),
            Some(&endpoint),
            &ArtifactSizes::new(),
            true,
        );
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");

        assert_eq!(v["running"], serde_json::json!(true));
        assert_eq!(
            v["attested_endpoint"],
            serde_json::json!("https://203.0.113.5:24101/")
        );
        assert_eq!(v["host_ipv4"], serde_json::json!("203.0.113.5"));
        assert_eq!(v["mapped_ports"]["8443"], serde_json::json!(24101));
        assert_eq!(
            v["measurements"][0]["registers"]["launch"],
            serde_json::json!("ab".repeat(48))
        );
        assert_eq!(v["runtime_ref"], serde_json::json!("cafe".repeat(16)));
    }

    #[test]
    fn render_show_without_net_does_not_panic_and_shows_message_side_fields() {
        let (item_hash, content) = fixture_content();

        let out = render_show(
            &item_hash,
            &content,
            None,
            None,
            &ArtifactSizes::new(),
            false,
        );

        assert!(out.contains("Running        no"));
        assert!(out.contains(&"ab".repeat(48)));
        // No live fields to show, so the mapped-ports section is absent.
        assert!(!out.contains("MAPPED PORTS"));

        let out_json = render_show(
            &item_hash,
            &content,
            None,
            None,
            &ArtifactSizes::new(),
            true,
        );
        let v: serde_json::Value = serde_json::from_str(&out_json).expect("valid json");
        assert_eq!(v["running"], serde_json::json!(false));
        assert!(v.get("host_ipv4").is_none());
        assert!(v.get("mapped_ports").is_none());
        assert!(v.get("attested_endpoint").is_none());
    }

    #[test]
    fn render_show_text_includes_resources_internet_and_storage() {
        let (item_hash, content) = fixture_content();
        let sizes = ArtifactSizes::from([
            (content.workload.reference.clone(), 512 * 1024 * 1024),
            (content.volumes[0].reference.clone(), 1_288_490_189), // 1.2 GiB
        ]);

        let out = render_show(&item_hash, &content, None, None, &sizes, false);

        assert!(out.contains("Resources      2 vCPUs, 2048 MiB"), "{out}");
        assert!(out.contains("Internet       yes"), "{out}");
        assert!(out.contains("STORAGE"), "{out}");
        assert!(
            out.contains(&format!("Workload       {}  512.0 MiB", "beef".repeat(16))),
            "{out}"
        );
        assert!(
            out.contains(&format!(
                "Volume         {}  1.2 GiB  model weights",
                "dada".repeat(16)
            )),
            "{out}"
        );
        assert!(out.contains("Total          1.7 GiB"), "{out}");
    }

    #[test]
    fn render_show_storage_degrades_when_sizes_are_unknown() {
        let (item_hash, content) = fixture_content();
        // Only the workload size is known: the volume and the total render
        // as unknown rather than under-reporting.
        let sizes = ArtifactSizes::from([(content.workload.reference.clone(), 1024)]);

        let out = render_show(&item_hash, &content, None, None, &sizes, false);
        assert!(
            out.contains("Workload       ") && out.contains("  1.0 KiB"),
            "{out}"
        );
        assert!(
            out.contains(&format!(
                "Volume         {}  -  model weights",
                "dada".repeat(16)
            )),
            "{out}"
        );
        assert!(out.contains("Total          -"), "{out}");

        let out_json = render_show(&item_hash, &content, None, None, &sizes, true);
        let v: serde_json::Value = serde_json::from_str(&out_json).expect("valid json");
        assert_eq!(
            v["resources"],
            serde_json::json!({"vcpus": 2, "memory_mib": 2048})
        );
        assert_eq!(v["internet"], serde_json::json!(true));
        assert_eq!(
            v["storage"]["workload"]["size_bytes"],
            serde_json::json!(1024)
        );
        assert_eq!(
            v["storage"]["volumes"][0]["ref"],
            serde_json::json!("dada".repeat(16))
        );
        assert!(v["storage"]["volumes"][0].get("size_bytes").is_none());
        assert!(v["storage"].get("total_bytes").is_none());
    }

    #[test]
    fn format_size_picks_binary_units() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(1023), "1023 B");
        assert_eq!(format_size(1024), "1.0 KiB");
        assert_eq!(format_size(512 * 1024 * 1024), "512.0 MiB");
        assert_eq!(format_size(1_288_490_189), "1.2 GiB");
        assert_eq!(format_size(4 << 40), "4.0 TiB");
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aleph_sdk::attest::{
        AttestedResponse, FreshAttestation, TcbFloor, TcbFloorOverride, TcbFloorPolicy,
    };

    fn measurement(digest: &str, vcpu_type: Option<&str>) -> LaunchMeasurement {
        let json = serde_json::json!({
            "platform": "sev_snp",
            "registers": {"launch": digest},
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

    /// Registers pinning `launch` as given, the shape the resolver now
    /// yields and the SDK derives.
    fn regs(launch: &str) -> SevSnpRegisters {
        SevSnpRegisters {
            launch: launch.to_string(),
        }
    }

    #[test]
    fn resolve_expected_measurement_accepts_a_full_length_override() {
        let digest = "cd".repeat(48);
        let expected = resolve_expected_measurement(&[], Some(&digest)).unwrap();
        assert_eq!(expected, MeasurementExpectation::Pin(regs(&digest)));
    }

    #[test]
    fn resolve_expected_measurement_pins_the_single_measurement() {
        let digest = "ab".repeat(48);
        let measurements = vec![measurement(&digest, Some("EPYC-v4"))];

        let expected = resolve_expected_measurement(&measurements, None).unwrap();

        assert_eq!(expected, MeasurementExpectation::Pin(regs(&digest)));
    }

    #[test]
    fn resolve_expected_measurement_override_takes_precedence() {
        let digest = "ab".repeat(48);
        let override_digest = "cd".repeat(48);
        let measurements = vec![measurement(&digest, Some("EPYC-v4"))];

        let expected = resolve_expected_measurement(&measurements, Some(&override_digest)).unwrap();

        assert_eq!(
            expected,
            MeasurementExpectation::Pin(regs(&override_digest))
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
            MeasurementExpectation::MemberOf(vec![regs(&digest_a), regs(&digest_b)])
        );
    }

    #[test]
    fn resolve_expected_measurement_fails_closed_on_foreign_platform() {
        // A register set this client cannot verify must error, never be
        // skipped: skipping would silently narrow the accepted set.
        // Unreachable through a valid message today (the schema restricts
        // the V-PROGRAM backend to sev_snp), so pin the branch directly.
        let r = "11".repeat(48);
        let json = serde_json::json!({
            "platform": "tdx",
            "registers": {"mrtd": r, "rtmr1": r, "rtmr2": r, "mrconfigid": r},
        });
        let tdx: LaunchMeasurement = serde_json::from_value(json).expect("valid tdx measurement");
        let measurements = vec![measurement(&"ab".repeat(48), Some("EPYC-v4")), tdx];

        let err = resolve_expected_measurement(&measurements, None)
            .expect_err("a tdx measurement must fail the resolution");
        assert!(
            err.to_string().contains("tdx measurement"),
            "unexpected error: {err}"
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
            registers: SevSnpRegisters {
                launch: measurement.to_string(),
            },
            policy: 0x30000,
            launch_tcb: Default::default(),
            reported_tcb: Default::default(),
            cpuid_family: None,
            cpuid_model: None,
            cpuid_stepping: None,
            platform: PlatformPosture {
                smt_enabled: true,
                tsme_enabled: false,
                ecc_enabled: false,
                rapl_disabled: false,
                ciphertext_hiding_enabled: false,
                alias_check_complete: false,
                raw: 0x1,
            },
            served_public_key: b"dummy-served-key".to_vec(),
            status: 200,
            headers: vec![],
            body: bytes::Bytes::copy_from_slice(body),
        }
    }

    #[test]
    fn platform_requirement_parses_every_cli_spelling() {
        use clap::ValueEnum;
        for (spelling, expected) in [
            ("smt-off", PlatformRequirement::SmtOff),
            ("tsme", PlatformRequirement::Tsme),
            ("rapl-off", PlatformRequirement::RaplOff),
            ("ciphertext-hiding", PlatformRequirement::CiphertextHiding),
            ("alias-check", PlatformRequirement::AliasCheck),
        ] {
            assert_eq!(
                PlatformRequirement::from_str(spelling, false).expect(spelling),
                expected
            );
        }
    }

    #[test]
    fn platform_requirements_build_the_policy() {
        let policy = platform_policy_from(&[
            PlatformRequirement::RaplOff,
            PlatformRequirement::AliasCheck,
        ]);
        assert!(policy.require_rapl_disabled);
        assert!(policy.require_alias_check);
        assert!(!policy.require_smt_disabled);
        assert!(!policy.require_tsme);
        assert!(!policy.require_ciphertext_hiding);
    }

    #[test]
    fn render_call_result_reports_platform_posture_in_json() {
        let response = dummy_response(&"ab".repeat(48), b"x");
        let (out, _) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Verified,
            true,
            false,
        );
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");
        assert_eq!(v["platform_info"]["raw"], serde_json::json!("0x1"));
        assert_eq!(v["platform_info"]["smt_enabled"], serde_json::json!(true));
        assert_eq!(
            v["platform_info"]["rapl_disabled"],
            serde_json::json!(false)
        );
        assert_eq!(
            v["platform_info"]["alias_check_complete"],
            serde_json::json!(false)
        );
    }

    #[test]
    fn render_call_result_reports_platform_posture_in_verbose_text_meta() {
        let response = dummy_response(&"ab".repeat(48), b"x");
        let (_, meta) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Verified,
            false,
            true,
        );
        let meta = meta.expect("text mode always has a meta block");
        assert!(
            meta.contains(
                "platform: SMT=on TSME=off ECC=off RAPL=on ciphertext-hiding=off alias-check=no (0x1)"
            ),
            "meta should carry the posture line, got: {meta}"
        );
    }

    #[test]
    fn policy_debug_allowed_detects_the_snp_debug_bit() {
        // 0x30000 is the recommended default: SMT allowed, no debug.
        assert!(!policy_debug_allowed(0x30000));
        // Bit 19 set: the host may decrypt guest memory.
        assert!(policy_debug_allowed(0x30000 | (1 << 19)));
    }

    fn dummy_floor() -> TcbFloor {
        TcbFloor {
            fmc: None,
            bootloader: 4,
            tee: 0,
            snp: 21,
            microcode: 84,
        }
    }

    #[test]
    fn check_debug_policy_accepts_non_debug_policy() {
        assert!(check_debug_policy(0x30000, false).is_ok());
        // allow_debug is irrelevant when the DEBUG bit is not set.
        assert!(check_debug_policy(0x30000, true).is_ok());
    }

    #[test]
    fn check_debug_policy_rejects_debug_without_allow_debug() {
        // 0xa0000 = bit 17 (reserved) | bit 19 (DEBUG): a valid policy that
        // passes validate_snp_policy but has the DEBUG bit set.
        let err = check_debug_policy(0xa0000, false).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("DEBUG bit (19) set") && msg.contains("--allow-debug"),
            "expected a DEBUG rejection mentioning --allow-debug, got: {msg}"
        );
    }

    #[test]
    fn check_debug_policy_accepts_debug_with_allow_debug() {
        // The --allow-debug ack clears the gate; the warning goes to stderr.
        assert!(check_debug_policy(0xa0000, true).is_ok());
    }

    #[test]
    fn render_call_result_json_parses_json_body() {
        let response = dummy_response(&"ab".repeat(48), br#"{"fib":55}"#);

        let (out, meta) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Verified,
            true,
            false,
        );
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");

        assert!(
            v.get("attestation_valid").is_none(),
            "no redundant always-true validity flag in call output"
        );
        assert_eq!(v["registers"]["launch"], serde_json::json!("ab".repeat(48)));
        assert!(
            v.get("measurement").is_none(),
            "scalar key must be gone: {v}"
        );
        // The verified policy is evidence alongside the measurement: hex,
        // matching the format used in --policy and error messages.
        assert_eq!(v["policy"], serde_json::json!("0x30000"));
        // The effective floor and verified TCB views are evidence alongside
        // the measurement/policy: structured objects keyed by the same
        // component names `--min-tcb` accepts, with `fmc` null off Turin.
        assert_eq!(
            v["effective_tcb_floor"],
            serde_json::json!({"fmc": null, "bootloader": 4, "tee": 0, "snp": 21, "microcode": 84})
        );
        for view in ["launch_tcb", "reported_tcb"] {
            assert!(v[view].is_object(), "{view} must be a structured object");
            assert!(
                v[view]["microcode"].is_number() && v[view]["bootloader"].is_number(),
                "{view} carries numeric component fields"
            );
        }
        assert_eq!(v["status"], serde_json::json!(200));
        assert_eq!(v["body"]["fib"], serde_json::json!(55));
        assert_eq!(meta, None, "JSON mode carries the status in the document");
    }

    #[test]
    fn render_call_result_json_falls_back_to_string_body_for_non_json() {
        let response = dummy_response(&"ab".repeat(48), b"plain text body");

        let (out, _meta) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Verified,
            true,
            false,
        );
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");

        assert_eq!(v["body"], serde_json::json!("plain text body"));
    }

    #[test]
    fn render_call_result_text_puts_only_the_body_on_stdout() {
        let response = dummy_response(&"ab".repeat(48), br#"{"status":"ok"}"#);

        let (out, meta) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Verified,
            false,
            false,
        );

        // The body must be machine-consumable as-is (aleph-testnets#35 run
        // 31474678768: `json.loads` on stdout choked on the status line).
        serde_json::from_str::<serde_json::Value>(&out)
            .expect("stdout is exactly the response body");
        assert_eq!(out, r#"{"status":"ok"}"#);
        let meta = meta.expect("text mode has a stderr meta");
        assert!(meta.starts_with("Attestation: verified ("), "{meta}");
        assert!(meta.ends_with("\nHTTP 200"), "{meta}");
        // Not verbose: no evidence lines.
        assert!(!meta.contains("platform:"), "{meta}");
        assert!(!meta.contains("measurement:"), "{meta}");
    }

    #[test]
    fn render_call_result_text_verbose_adds_evidence_lines() {
        let response = dummy_response(&"ab".repeat(48), b"55");
        let (_, meta) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Verified,
            false,
            true,
        );
        let meta = meta.unwrap();
        assert!(
            meta.contains(&format!("\nmeasurement: {}", "ab".repeat(48))),
            "{meta}"
        );
        assert!(meta.contains("\npolicy: 0x"), "{meta}");
        assert!(meta.contains("\nlaunch TCB: bootloader="), "{meta}");
        assert!(
            meta.contains(
                "\nplatform: SMT=on TSME=off ECC=off RAPL=on \
                 ciphertext-hiding=off alias-check=no (0x1)"
            ),
            "{meta}"
        );
    }

    fn net() -> TcbFloorPolicy {
        TcbFloorPolicy {
            default: TcbFloor {
                fmc: None,
                bootloader: 4,
                tee: 0,
                snp: 21,
                microcode: 84,
            },
            // A Genoa-shaped policy: the Genoa-X and Zen4c lines each
            // follow their own, lower microcode sequence.
            x_variant: Some(TcbFloor {
                fmc: None,
                bootloader: 4,
                tee: 0,
                snp: 21,
                microcode: 79,
            }),
            zen4c: Some(TcbFloor {
                fmc: None,
                bootloader: 4,
                tee: 0,
                snp: 21,
                microcode: 28,
            }),
        }
    }

    #[test]
    fn no_override_yields_the_network_floor() {
        assert_eq!(resolve_effective_floor(&net(), None, false).unwrap(), net());
    }

    #[test]
    fn raising_override_needs_no_acknowledgement() {
        let o: TcbFloorOverride = "snp=30".parse().unwrap();
        let eff = resolve_effective_floor(&net(), Some(&o), false).unwrap();
        // The raise lands on every family floor.
        assert_eq!(eff.default.snp, 30);
        assert_eq!(eff.zen4c.unwrap().snp, 30);
    }

    #[test]
    fn lowering_override_without_ack_is_rejected() {
        let o: TcbFloorOverride = "snp=9".parse().unwrap();
        assert!(resolve_effective_floor(&net(), Some(&o), false).is_err());
    }

    #[test]
    fn lowering_override_with_ack_is_accepted() {
        let o: TcbFloorOverride = "snp=9".parse().unwrap();
        let eff = resolve_effective_floor(&net(), Some(&o), true).unwrap();
        assert_eq!(eff.default.snp, 9);
        assert_eq!(eff.zen4c.unwrap().snp, 9);
    }

    #[test]
    fn lowering_only_the_zen4c_floor_still_needs_the_ack() {
        // microcode=50 sits between the zen4c floor (28, raised) and the
        // classic floor (84, lowered): the gate fires because the silicon
        // family is unknown until a report is in hand, so ANY family's
        // weakened floor needs the explicit acknowledgement.
        let o: TcbFloorOverride = "microcode=50".parse().unwrap();
        assert!(resolve_effective_floor(&net(), Some(&o), false).is_err());
        let eff = resolve_effective_floor(&net(), Some(&o), true).unwrap();
        assert_eq!(eff.default.microcode, 50);
        assert_eq!(eff.x_variant.unwrap().microcode, 50);
        assert_eq!(eff.zen4c.unwrap().microcode, 50);
    }

    #[test]
    fn render_call_result_selects_the_zen4c_floor_for_zen4c_evidence() {
        // A response whose signed report says Bergamo/Siena (family 19h,
        // model A0h-AFh) must show the zen4c floor as the effective one,
        // and the CPUID fields as evidence.
        let mut response = dummy_response(&"ab".repeat(48), br#"{"fib":55}"#);
        response.cpuid_family = Some(0x19);
        response.cpuid_model = Some(0xA1);
        response.cpuid_stepping = Some(2);

        let (out, _meta) = render_call_result(&response, &net(), Freshness::Verified, true, false);
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");

        assert_eq!(v["effective_tcb_floor"]["microcode"], serde_json::json!(28));
        assert_eq!(v["cpuid"]["family"], serde_json::json!(0x19));
        assert_eq!(v["cpuid"]["model"], serde_json::json!(0xA1));
        assert_eq!(v["cpuid"]["stepping"], serde_json::json!(2));
    }

    #[test]
    fn render_call_result_reports_freshness_in_json() {
        let response = dummy_response(&"ab".repeat(48), br#"{"fib":55}"#);
        let (out, _) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Verified,
            true,
            false,
        );
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");
        assert_eq!(v["freshness"], serde_json::json!("verified"));

        let (out, _) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Skipped,
            true,
            false,
        );
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid json");
        assert_eq!(v["freshness"], serde_json::json!("skipped"));
    }

    #[test]
    fn render_call_result_reports_freshness_in_text_meta() {
        let response = dummy_response(&"ab".repeat(48), b"55");
        let (_, meta) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Verified,
            false,
            false,
        );
        let meta = meta.unwrap();
        assert!(meta.contains("fresh nonce)"), "{meta}");
        assert!(!meta.contains("SKIPPED"), "{meta}");
        let (_, meta) = render_call_result(
            &response,
            &TcbFloorPolicy::uniform(dummy_floor()),
            Freshness::Skipped,
            false,
            false,
        );
        let meta = meta.unwrap();
        assert!(meta.starts_with("Attestation: verified ("), "{meta}");
        assert!(
            meta.contains("fresh nonce SKIPPED by --allow-stale-attestation"),
            "{meta}"
        );
    }

    #[test]
    fn fresh_consistency_rejects_key_measurement_and_policy_drift() {
        let response = dummy_response(&"ab".repeat(48), b"ok");
        let fresh = FreshAttestation {
            registers: response.registers.clone(),
            policy: response.policy,
            launch_tcb: response.launch_tcb,
            reported_tcb: response.reported_tcb,
            cpuid_family: None,
            cpuid_model: None,
            cpuid_stepping: None,
            platform: response.platform,
            served_public_key: response.served_public_key.clone(),
        };
        check_fresh_consistency(&fresh, &response).expect("matching evidence must pass");

        let mut wrong_key = fresh.clone();
        wrong_key.served_public_key = b"someone else".to_vec();
        assert!(check_fresh_consistency(&wrong_key, &response).is_err());

        let mut wrong_measurement = fresh.clone();
        wrong_measurement.registers.launch = "cd".repeat(48);
        assert!(check_fresh_consistency(&wrong_measurement, &response).is_err());

        let mut wrong_policy = fresh;
        wrong_policy.policy ^= 1;
        assert!(check_fresh_consistency(&wrong_policy, &response).is_err());
    }
}

#[cfg(test)]
mod compose_wiring_tests {
    use super::*;

    #[test]
    fn compose_contract_gate_accepts_the_compose_contract() {
        let w = WorkloadSpec {
            contract: "aleph.compose/1".into(),
            upstream_port: Some(8080),
        };
        check_compose_contract(Some(&w)).unwrap();
    }

    #[test]
    fn compose_contract_gate_names_both_contracts_on_mismatch() {
        let w = WorkloadSpec {
            contract: "aleph.builtin/1".into(),
            upstream_port: None,
        };
        let err = check_compose_contract(Some(&w)).unwrap_err().to_string();
        assert!(
            err.contains("aleph.compose/1") && err.contains("aleph.builtin/1"),
            "{err}"
        );
    }

    #[test]
    fn compose_contract_gate_rejects_a_contractless_runtime() {
        assert!(check_compose_contract(None).is_err());
    }

    #[test]
    fn exec_contract_gate_rejects_only_the_compose_contract() {
        let compose = WorkloadSpec {
            contract: "aleph.compose/1".into(),
            upstream_port: Some(8080),
        };
        let err = check_exec_contract(Some(&compose)).unwrap_err().to_string();
        assert!(err.contains("--compose"), "{err}");
        let exec = WorkloadSpec {
            contract: "aleph.exec/1".into(),
            upstream_port: Some(8080),
        };
        check_exec_contract(Some(&exec)).unwrap();
        // The base runtime contract used by the manifest fixture is accepted too.
        let builtin = WorkloadSpec {
            contract: "aleph.builtin/1".into(),
            upstream_port: Some(8080),
        };
        check_exec_contract(Some(&builtin)).unwrap();
        check_exec_contract(None).unwrap();
    }

    fn manifest_with_source(source: Option<&str>) -> RuntimeManifest {
        let source = source.map_or(String::new(), |s| format!(r#", "source": {s}"#));
        let json = format!(
            r#"{{
              "format": "aleph-vprogram-runtime", "format_version": 1,
              "name": "aleph-snp-attest", "version": "2026.08.20", "platform": "sev_snp",
              "workload": {{ "contract": "aleph.exec/1", "upstream_port": 8080 }},
              "bundle": {{ "ref": "{h}", "sha256": "{h}", "size": 1,
                "members": {{ "ovmf": "a", "kernel": "b", "initrd": "c",
                  "platform_rootfs": "d", "platform_hash_tree": "e" }} }},
              "boot": {{ "method": "qemu-direct-kernel", "kernel_hashes": true, "cpu_models": ["EPYC-Genoa"],
                "platform_roothash": "{h}",
                "cmdline_template": "roothash={{platform_roothash}} workload_roothash={{workload_roothash}}" }},
              "attestation": []{source}
            }}"#,
            h = "cb".repeat(32)
        );
        RuntimeManifest::parse(json.as_bytes()).expect("test manifest parses")
    }

    fn resolved(label: Option<&str>, contract: Option<&str>, hash: &ItemHash) -> ResolvedRuntime {
        ResolvedRuntime {
            hash: hash.clone(),
            contract: contract.map(str::to_string),
            label: label.map(str::to_string),
        }
    }

    #[test]
    fn runtime_identity_line_names_runtime_contract_manifest_and_provenance() {
        let hash: ItemHash = "afde".repeat(16).parse().unwrap();
        let m = manifest_with_source(Some(
            r#"{"repo": "https://github.com/aleph-im/aleph-vm", "rev": "ba690c65", "build": "nix build"}"#,
        ));
        assert_eq!(
            runtime_identity_line(&resolved(Some("exec-1.0"), Some("aleph.exec/1"), &hash), &m),
            "Using runtime exec-1.0 (aleph.exec/1; aleph-snp-attest 2026.08.20, aleph-vm@ba690c65)"
        );
        assert_eq!(
            runtime_identity_line(&resolved(None, None, &hash), &m),
            format!(
                "Using runtime {hash} (aleph.exec/1; aleph-snp-attest 2026.08.20, aleph-vm@ba690c65)"
            )
        );
    }

    #[test]
    fn runtime_identity_line_degrades_without_source() {
        let hash: ItemHash = "afde".repeat(16).parse().unwrap();
        let m = manifest_with_source(None);
        assert_eq!(
            runtime_identity_line(&resolved(Some("exec-1.0"), Some("aleph.exec/1"), &hash), &m),
            "Using runtime exec-1.0 (aleph.exec/1; aleph-snp-attest 2026.08.20)"
        );
        let m = manifest_with_source(Some(r#"{"rev": "ba690c65"}"#));
        assert!(
            runtime_identity_line(&resolved(None, None, &hash), &m).ends_with(", rev ba690c65)")
        );
    }

    #[test]
    fn contract_match_gate() {
        let w = WorkloadSpec {
            contract: "aleph.exec/1".into(),
            upstream_port: Some(8080),
        };
        check_contract_matches("aleph.exec/1", Some(&w)).unwrap();
        let err = check_contract_matches("aleph.exec/2", Some(&w))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("aleph.exec/2") && err.contains("aleph.exec/1"),
            "{err}"
        );
        assert!(check_contract_matches("aleph.exec/1", None).is_err());
    }

    mod runtime_catalogue {
        use super::*;
        use aleph_sdk::aggregate_models::vm_images::{VProgramRuntimeEntry, VmImageDefaults};
        use std::collections::BTreeMap;

        fn entry(hash: &str, contract: &str) -> VProgramRuntimeEntry {
            VProgramRuntimeEntry {
                hash: hash.repeat(16).parse().unwrap(),
                contract: contract.into(),
                display_name: None,
                description: None,
                deprecated: false,
            }
        }

        fn data() -> VmImagesData {
            VmImagesData {
                vprogram_runtimes: BTreeMap::from([
                    ("exec-1.0".to_string(), entry("aaaa", "aleph.exec/1")),
                    ("compose-1.0".to_string(), entry("bbbb", "aleph.compose/1")),
                ]),
                vprogram_contracts: BTreeMap::from([
                    ("aleph.exec/1".to_string(), "exec-1.0".to_string()),
                    ("aleph.compose/1".to_string(), "compose-1.0".to_string()),
                ]),
                defaults: VmImageDefaults {
                    vprogram_models: BTreeMap::from([
                        ("exec".to_string(), "aleph.exec/1".to_string()),
                        ("compose".to_string(), "aleph.compose/1".to_string()),
                    ]),
                    ..Default::default()
                },
                ..Default::default()
            }
        }

        #[test]
        fn hash_bypasses_the_aggregate() {
            let raw: ItemHash = "cafe".repeat(16).parse().unwrap();
            let got = resolve_vprogram_runtime(
                Some(ImageRef::Hash(raw.clone())),
                VPROGRAM_MODEL_EXEC,
                &VmImagesData::default(),
            )
            .unwrap();
            assert_eq!(
                got,
                ResolvedRuntime {
                    hash: raw,
                    contract: None,
                    label: None
                }
            );
        }

        #[test]
        fn omitted_picks_the_default_for_the_model() {
            let exec = resolve_vprogram_runtime(None, VPROGRAM_MODEL_EXEC, &data()).unwrap();
            assert_eq!(exec.hash.to_string(), "aaaa".repeat(16));
            assert_eq!(exec.contract.as_deref(), Some("aleph.exec/1"));
            assert_eq!(exec.label.as_deref(), Some("exec-1.0"));
            let compose = resolve_vprogram_runtime(None, VPROGRAM_MODEL_COMPOSE, &data()).unwrap();
            assert_eq!(compose.hash.to_string(), "bbbb".repeat(16));
        }

        #[test]
        fn contract_or_runtime_selectors() {
            let by_contract = resolve_vprogram_runtime(
                Some(ImageRef::Preset("aleph.compose/1".into())),
                VPROGRAM_MODEL_COMPOSE,
                &data(),
            )
            .unwrap();
            let by_impl = resolve_vprogram_runtime(
                Some(ImageRef::Preset("compose-1.0".into())),
                VPROGRAM_MODEL_COMPOSE,
                &data(),
            )
            .unwrap();
            assert_eq!(by_contract, by_impl);
        }

        #[test]
        fn wrong_model_hints_at_the_other_flag() {
            let err = resolve_vprogram_runtime(
                Some(ImageRef::Preset("compose-1.0".into())),
                VPROGRAM_MODEL_EXEC,
                &data(),
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains("did you mean --compose?"), "{err}");

            let err = resolve_vprogram_runtime(
                Some(ImageRef::Preset("aleph.exec/1".into())),
                VPROGRAM_MODEL_COMPOSE,
                &data(),
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains("did you mean --workload?"), "{err}");
        }

        #[test]
        fn omitted_without_catalogue_is_an_error() {
            let err = resolve_vprogram_runtime(None, VPROGRAM_MODEL_EXEC, &VmImagesData::default())
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("no default contract for V-Program workload model \"exec\""),
                "{err}"
            );
        }

        #[test]
        fn unknown_runtime_lists_available() {
            let err = resolve_vprogram_runtime(
                Some(ImageRef::Preset("nope".into())),
                VPROGRAM_MODEL_EXEC,
                &data(),
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains("exec-1.0"), "{err}");
        }
    }

    #[test]
    fn image_archive_specs_parse_and_reject_garbage() {
        let (name, path) = parse_image_archive("fib-service:latest=./fib.tar").unwrap();
        assert_eq!(name, "fib-service:latest");
        assert_eq!(path, PathBuf::from("./fib.tar"));
        assert!(parse_image_archive("no-equals-sign").is_err());
    }

    #[test]
    fn parse_image_archives_rejects_a_duplicate_image_key() {
        let specs = vec!["web=./a.tar".to_string(), "web=./b.tar".to_string()];
        let err = parse_image_archives(&specs).unwrap_err().to_string();
        assert!(err.contains("web") && err.contains("duplicate"), "{err}");
    }

    #[test]
    fn parse_image_archives_accepts_distinct_images() {
        let specs = vec!["web=./a.tar".to_string(), "db=./b.tar".to_string()];
        let archives = parse_image_archives(&specs).unwrap();
        assert_eq!(archives.len(), 2);
    }

    #[test]
    fn check_archive_keys_rejects_a_key_with_no_matching_image() {
        let archives = BTreeMap::from([("typo-nginx".to_string(), PathBuf::from("./a.tar"))]);
        let images = vec!["nginx:1.27".to_string()];
        let err = check_archive_keys_are_known_images(&archives, &images)
            .unwrap_err()
            .to_string();
        assert!(err.contains("typo-nginx"), "{err}");
    }

    #[test]
    fn check_archive_keys_accepts_an_exact_match() {
        let archives = BTreeMap::from([("nginx:1.27".to_string(), PathBuf::from("./a.tar"))]);
        let images = vec!["nginx:1.27".to_string()];
        check_archive_keys_are_known_images(&archives, &images).unwrap();
    }

    /// A digest-referenced image declares an identity the CLI must enforce;
    /// a caller-supplied archive has no registry digest to enforce it
    /// against, so the combination is refused rather than staged unverified.
    #[test]
    fn check_archives_do_not_cover_digest_images_rejects_the_combination() {
        let image = format!("nginx@sha256:{}", "ab".repeat(32));
        let archives = BTreeMap::from([(image.clone(), PathBuf::from("./a.tar"))]);
        let err = check_archives_do_not_cover_digest_images(&archives)
            .unwrap_err()
            .to_string();
        assert!(err.contains("digest"), "{err}");
        assert!(err.contains(&image), "{err}");
    }

    #[test]
    fn check_archives_do_not_cover_digest_images_accepts_tagged_keys() {
        let archives = BTreeMap::from([("nginx:1.27".to_string(), PathBuf::from("./a.tar"))]);
        check_archives_do_not_cover_digest_images(&archives).unwrap();
    }
}
