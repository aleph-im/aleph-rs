use aleph_sdk::aggregate_models::pricing::PRICING_ADDRESS;
use aleph_sdk::aggregate_models::vm_images::{VM_IMAGES_KEY, VmImagesAggregate, VmImagesData};
use aleph_sdk::builder::MessageBuilder;
use aleph_sdk::client::{AlephAggregateClient, AlephClient};
use aleph_types::channel::Channel;
use aleph_types::message::MessageType;
use anyhow::{Context, Result};
use url::Url;

use crate::cli::{
    AdminEntryByNameArgs, AdminRootfsUpdateArgs, AdminRuntimeAddArgs, AdminRuntimeUpdateArgs,
    AdminTargetArgs, ImagesCommand, ImagesFirmwareCommand, ImagesRootfsCommand, ImagesRuntimeCommand,
};
use crate::commands::admin::vm_images_diff::render_diff;
use crate::commands::admin::vm_images_mutate::{
    apply_mutation, AdminImagesError, EntryPatch, Kind, Mutation, NewEntry,
};
use crate::common::{confirm_action, resolve_account, resolve_address, submit_or_preview};


pub async fn handle_images_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: ImagesCommand,
) -> Result<()> {
    match command {
        ImagesCommand::Rootfs { command } => {
            handle_rootfs(aleph_client, ccn_url, json, command).await
        }
        ImagesCommand::Runtime { command } => {
            handle_runtime_or_firmware(
                aleph_client,
                ccn_url,
                json,
                Kind::Runtime,
                command.into(),
            )
            .await
        }
        ImagesCommand::Firmware { command } => {
            handle_runtime_or_firmware(
                aleph_client,
                ccn_url,
                json,
                Kind::Firmware,
                command.into(),
            )
            .await
        }
    }
}

async fn handle_rootfs(
    client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    cmd: ImagesRootfsCommand,
) -> Result<()> {
    match cmd {
        ImagesRootfsCommand::Add(args) => {
            let mutation = Mutation::Add {
                kind: Kind::Rootfs,
                name: args.name.clone(),
                entry: NewEntry {
                    hash: args.hash,
                    display_name: args.display_name,
                    description: args.description,
                    min_disk_mib: args.min_disk_mib,
                    deprecated: args.deprecated,
                },
            };
            run_mutation(client, ccn_url, json, &args.target, mutation).await
        }
        ImagesRootfsCommand::Update(args) => {
            run_rootfs_update(client, ccn_url, json, args).await
        }
        ImagesRootfsCommand::Deprecate(args) => {
            let mutation = Mutation::Deprecate {
                kind: Kind::Rootfs,
                name: args.name.clone(),
            };
            run_mutation(client, ccn_url, json, &args.target, mutation).await
        }
        ImagesRootfsCommand::Undeprecate(args) => {
            let mutation = Mutation::Undeprecate {
                kind: Kind::Rootfs,
                name: args.name.clone(),
            };
            run_mutation(client, ccn_url, json, &args.target, mutation).await
        }
        ImagesRootfsCommand::Default(args) => {
            let mutation = Mutation::SetDefault {
                kind: Kind::Rootfs,
                name: args.name.clone(),
            };
            run_mutation(client, ccn_url, json, &args.target, mutation).await
        }
        ImagesRootfsCommand::ClearDefault(target) => {
            let mutation = Mutation::ClearDefault {
                kind: Kind::Rootfs,
            };
            run_mutation(client, ccn_url, json, &target, mutation).await
        }
    }
}

// Internal command form shared by runtime and firmware (their shapes are identical).
enum RuntimeLikeCommand {
    Add(AdminRuntimeAddArgs),
    Update(AdminRuntimeUpdateArgs),
    Deprecate(AdminEntryByNameArgs),
    Undeprecate(AdminEntryByNameArgs),
    Default(AdminEntryByNameArgs),
    ClearDefault(AdminTargetArgs),
}

impl From<ImagesRuntimeCommand> for RuntimeLikeCommand {
    fn from(c: ImagesRuntimeCommand) -> Self {
        match c {
            ImagesRuntimeCommand::Add(a) => Self::Add(a),
            ImagesRuntimeCommand::Update(a) => Self::Update(a),
            ImagesRuntimeCommand::Deprecate(a) => Self::Deprecate(a),
            ImagesRuntimeCommand::Undeprecate(a) => Self::Undeprecate(a),
            ImagesRuntimeCommand::Default(a) => Self::Default(a),
            ImagesRuntimeCommand::ClearDefault(a) => Self::ClearDefault(a),
        }
    }
}

impl From<ImagesFirmwareCommand> for RuntimeLikeCommand {
    fn from(c: ImagesFirmwareCommand) -> Self {
        match c {
            ImagesFirmwareCommand::Add(a) => Self::Add(a),
            ImagesFirmwareCommand::Update(a) => Self::Update(a),
            ImagesFirmwareCommand::Deprecate(a) => Self::Deprecate(a),
            ImagesFirmwareCommand::Undeprecate(a) => Self::Undeprecate(a),
            ImagesFirmwareCommand::Default(a) => Self::Default(a),
            ImagesFirmwareCommand::ClearDefault(a) => Self::ClearDefault(a),
        }
    }
}

async fn handle_runtime_or_firmware(
    client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    kind: Kind,
    cmd: RuntimeLikeCommand,
) -> Result<()> {
    match cmd {
        RuntimeLikeCommand::Add(args) => {
            let mutation = Mutation::Add {
                kind,
                name: args.name.clone(),
                entry: NewEntry {
                    hash: args.hash,
                    display_name: args.display_name,
                    description: args.description,
                    min_disk_mib: None,
                    deprecated: args.deprecated,
                },
            };
            run_mutation(client, ccn_url, json, &args.target, mutation).await
        }
        RuntimeLikeCommand::Update(args) => {
            let patch = EntryPatch {
                hash: args.hash,
                display_name: opt_or_clear(args.display_name, args.clear_display_name),
                description: opt_or_clear(args.description, args.clear_description),
                min_disk_mib: None,
            };
            if patch.is_empty() {
                anyhow::bail!(AdminImagesError::NoFieldsToUpdate);
            }
            let mutation = Mutation::Update {
                kind,
                name: args.name.clone(),
                patch,
            };
            run_mutation(client, ccn_url, json, &args.target, mutation).await
        }
        RuntimeLikeCommand::Deprecate(args) => {
            let mutation = Mutation::Deprecate {
                kind,
                name: args.name.clone(),
            };
            run_mutation(client, ccn_url, json, &args.target, mutation).await
        }
        RuntimeLikeCommand::Undeprecate(args) => {
            let mutation = Mutation::Undeprecate {
                kind,
                name: args.name.clone(),
            };
            run_mutation(client, ccn_url, json, &args.target, mutation).await
        }
        RuntimeLikeCommand::Default(args) => {
            let mutation = Mutation::SetDefault {
                kind,
                name: args.name.clone(),
            };
            run_mutation(client, ccn_url, json, &args.target, mutation).await
        }
        RuntimeLikeCommand::ClearDefault(target) => {
            let mutation = Mutation::ClearDefault { kind };
            run_mutation(client, ccn_url, json, &target, mutation).await
        }
    }
}

async fn run_rootfs_update(
    client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: AdminRootfsUpdateArgs,
) -> Result<()> {
    let patch = EntryPatch {
        hash: args.hash,
        display_name: opt_or_clear(args.display_name, args.clear_display_name),
        description: opt_or_clear(args.description, args.clear_description),
        min_disk_mib: opt_or_clear(args.min_disk_mib, args.clear_min_disk_mib),
    };
    if patch.is_empty() {
        anyhow::bail!(AdminImagesError::NoFieldsToUpdate);
    }
    let mutation = Mutation::Update {
        kind: Kind::Rootfs,
        name: args.name.clone(),
        patch,
    };
    run_mutation(client, ccn_url, json, &args.target, mutation).await
}

fn opt_or_clear<T>(value: Option<T>, clear: bool) -> Option<Option<T>> {
    match (value, clear) {
        (Some(v), false) => Some(Some(v)),
        (None, true) => Some(None),
        (None, false) => None,
        (Some(_), true) => unreachable!("clap conflicts_with prevents this"),
    }
}

async fn run_mutation(
    client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    target: &AdminTargetArgs,
    mutation: Mutation,
) -> Result<()> {
    let account = resolve_account(&target.signing.identity)?;
    let target_address = match target.address.as_deref() {
        Some(s) => resolve_address(s)?,
        None => PRICING_ADDRESS.clone(),
    };
    let target_key = target.key.clone().unwrap_or_else(|| VM_IMAGES_KEY.to_string());

    let mut data: VmImagesData = match client
        .get_aggregate::<VmImagesAggregate>(&target_address, &target_key)
        .await
    {
        Ok(agg) => agg.vm_images,
        Err(e) if e.is_not_found() => VmImagesData::default(),
        Err(e) => return Err(e).context("failed to fetch current vm-images aggregate"),
    };

    let before = data.clone();
    apply_mutation(&mut data, mutation).map_err(anyhow::Error::from)?;
    let diff = render_diff(&before, &data);
    eprint!("Aggregate: {target_address}/{target_key}\n\n{diff}");

    if !confirm_action("Proceed?", target.yes)? {
        eprintln!("Aborted.");
        return Ok(());
    }

    let envelope = serde_json::json!({
        "key": &target_key,
        "content": { "vm-images": &data },
    });
    let mut builder = MessageBuilder::new(&account, MessageType::Aggregate, envelope);
    if let Some(ch) = target.channel.as_ref() {
        builder = builder.channel(Channel::from(ch.clone()));
    }
    if let Some(addr) = target.on_behalf_of.as_deref() {
        builder = builder.on_behalf_of(resolve_address(addr)?);
    }
    let pending = builder.build()?;
    submit_or_preview(client, ccn_url, &pending, target.signing.dry_run, json).await
}
