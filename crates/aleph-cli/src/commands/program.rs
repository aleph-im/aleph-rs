use crate::cli::{
    PaymentTypeCli, ProgramCommand, ProgramCreateArgs, ProgramDeleteArgs, ProgramListArgs,
    StorageEngineCli,
};
use crate::commands::instance::{
    parse_ephemeral_volumes, parse_immutable_volumes, parse_persistent_volumes,
};
use crate::common::{print_submission_result, resolve_account, resolve_address, submit_or_preview};
use crate::program::archive::prepare_archive;
use aleph_sdk::client::{AlephAggregateClient, AlephClient, AlephStorageClient, hash_file};
use aleph_sdk::messages::{ProgramBuilder, StoreBuilder};
use aleph_sdk::verify::Hasher;
use aleph_types::channel::Channel;
use aleph_types::message::StorageEngine;
use aleph_types::message::execution::base::Payment;
use aleph_types::message::execution::volume::MachineVolume;
use anyhow::{Context, Result, bail};
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
        ProgramCommand::Update(_) => {
            bail!("`aleph program update` lands in PR 2 of the program CLI work")
        }
        ProgramCommand::Persist(_) => {
            bail!("`aleph program persist` lands in PR 2 of the program CLI work")
        }
        ProgramCommand::Unpersist(_) => {
            bail!("`aleph program unpersist` lands in PR 2 of the program CLI work")
        }
        ProgramCommand::Logs(_) => {
            bail!("`aleph program logs` lands in PR 2 of the program CLI work")
        }
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

async fn handle_list(
    _aleph_client: &AlephClient,
    _json: bool,
    _args: ProgramListArgs,
) -> Result<()> {
    bail!("`aleph program list` not yet implemented (PR1 T10)")
}

async fn handle_delete(
    _aleph_client: &AlephClient,
    _ccn_url: &Url,
    _json: bool,
    _args: ProgramDeleteArgs,
) -> Result<()> {
    bail!("`aleph program delete` not yet implemented (PR1 T11)")
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
