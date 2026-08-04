//! `aleph vprogram` command tree.

use std::path::{Path, PathBuf};

use aleph_sdk::client::{AlephClient, AlephStorageClient, hash_file};
use aleph_sdk::messages::{StoreBuilder, VProgramBuilder};
use aleph_sdk::verify::Hasher;
use aleph_sdk::vprogram::bundle::fetch_bundle_artifacts;
use aleph_sdk::vprogram::cmdline::instantiate_cmdline;
use aleph_sdk::vprogram::manifest::RuntimeManifest;
use aleph_sdk::vprogram::measure::compute_measurements;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::{
    MAX_VERIFIED_VOLUMES, StorageEngine, TeeVerification, VerifiedVolume, VerifiedWorkload,
};
use anyhow::{Result, bail};
use memsizes::MiB;
use url::Url;

use crate::account::CliAccount;
use crate::cli::{VProgramCommand, VProgramCreateArgs};
use crate::common::{render_upload_progress, resolve_account, submit_or_preview};
use crate::config::store::ConfigStore;
use crate::veritysetup::Veritysetup;

pub async fn dispatch(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    cmd: VProgramCommand,
) -> Result<()> {
    match cmd {
        VProgramCommand::Create(args) => handle_create(aleph_client, ccn_url, json, *args).await,
    }
}

async fn handle_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: VProgramCreateArgs,
) -> Result<()> {
    // 0. Fail fast on local prerequisites before any network call.
    let veritysetup = Veritysetup::find()?;
    let account = resolve_account(&args.signing.identity)?;
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
        .bytes()
        .await?;
    let manifest = RuntimeManifest::parse(&manifest_bytes)?;

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

    let mut builder = VProgramBuilder::new(&account, args.runtime.clone(), workload, verification)
        .vcpus(args.vcpus)
        .memory(MiB::from(u64::from(args.memory)))
        .internet(!args.no_internet)
        .volumes(volumes);
    if let Some(node_hash) = args.node_hash {
        builder = builder.node_hash(node_hash);
    }
    if let Some(channel) = args.channel {
        builder = builder.channel(Channel::from(channel));
    }
    let pending = builder.build()?;

    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
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
