//! `aleph instance backup` subcommands. Each handler resolves
//! `(vm_id, crn_url)` via the shared scheduler-based helper from
//! `instance_target`, then drives `CrnClient` backup methods.

use aleph_sdk::crn::CrnClient;
use anyhow::Result;
use url::Url;

use crate::cli::{
    InstanceBackupCommand, InstanceBackupCreateArgs, InstanceBackupDeleteArgs,
    InstanceBackupDownloadArgs, InstanceBackupInfoArgs, InstanceBackupRestoreArgs, SigningArgs,
};
use crate::commands::instance_target::resolve_target;
use crate::common::resolve_account;

pub async fn dispatch(scheduler_url: Url, json: bool, sub: InstanceBackupCommand) -> Result<()> {
    match sub {
        InstanceBackupCommand::Create(args) => handle_create(scheduler_url, json, args).await,
        InstanceBackupCommand::Info(args) => handle_info(scheduler_url, json, args).await,
        InstanceBackupCommand::Download(args) => handle_download(scheduler_url, json, args).await,
        InstanceBackupCommand::Delete(args) => handle_delete(scheduler_url, json, args).await,
        InstanceBackupCommand::Restore(args) => handle_restore(scheduler_url, json, args).await,
    }
}

fn build_client(crn_url: &Url, signing: &SigningArgs) -> Result<CrnClient> {
    let account = resolve_account(&signing.identity)?;
    Ok(CrnClient::new(&account, crn_url.clone())?)
}

async fn handle_create(
    _scheduler_url: Url,
    _json: bool,
    _args: InstanceBackupCreateArgs,
) -> Result<()> {
    anyhow::bail!("instance backup create: not yet implemented")
}

async fn handle_info(
    _scheduler_url: Url,
    _json: bool,
    _args: InstanceBackupInfoArgs,
) -> Result<()> {
    anyhow::bail!("instance backup info: not yet implemented")
}

async fn handle_download(
    _scheduler_url: Url,
    _json: bool,
    _args: InstanceBackupDownloadArgs,
) -> Result<()> {
    anyhow::bail!("instance backup download: not yet implemented")
}

async fn handle_delete(
    _scheduler_url: Url,
    _json: bool,
    _args: InstanceBackupDeleteArgs,
) -> Result<()> {
    anyhow::bail!("instance backup delete: not yet implemented")
}

async fn handle_restore(
    _scheduler_url: Url,
    _json: bool,
    _args: InstanceBackupRestoreArgs,
) -> Result<()> {
    anyhow::bail!("instance backup restore: not yet implemented")
}
