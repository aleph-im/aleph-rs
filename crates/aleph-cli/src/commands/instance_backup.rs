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
    scheduler_url: Url,
    json: bool,
    args: InstanceBackupInfoArgs,
) -> Result<()> {
    use aleph_sdk::crn::BackupStatus;
    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id, args.crn_url.as_deref()).await?;
    let client = build_client(&crn_url, &args.signing)?;
    let status = client.get_backup(&vm_id).await?;

    if json {
        match status {
            BackupStatus::InProgress => {
                println!("{}", serde_json::json!({"status": "in_progress"}));
            }
            BackupStatus::Complete(meta) => {
                println!("{}", serde_json::to_string_pretty(&meta)?);
            }
            BackupStatus::NotFound => {
                println!("{}", serde_json::json!({"status": "not_found"}));
            }
        }
    } else {
        match status {
            BackupStatus::InProgress => eprintln!("Backup in progress for {vm_id}."),
            BackupStatus::NotFound => eprintln!("No backup found for {vm_id}."),
            BackupStatus::Complete(meta) => {
                println!("backup_id    {}", meta.backup_id);
                println!("size         {} bytes", meta.size);
                println!("checksum     {}", meta.checksum);
                println!("expires_at   {}", meta.expires_at);
                println!("download_url {}", meta.download_url);
                if !meta.volumes.is_empty() {
                    println!("volumes      {}", meta.volumes.join(", "));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{ChainCli, IdentityArgs, SigningArgs};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn evm_signing_args() -> SigningArgs {
        SigningArgs {
            identity: IdentityArgs {
                account: None,
                private_key: Some(
                    "0x0101010101010101010101010101010101010101010101010101010101010101"
                        .to_string(),
                ),
                chain: Some(ChainCli::Eth),
            },
            dry_run: false,
        }
    }

    const FULL_HASH: &str = "5a586d6f59f6c2e6862f155204626dcf01a6ec1107e7aba67063cd48ffe41d99";

    #[tokio::test]
    async fn info_complete_renders_metadata_in_json_mode() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/control/machine/{FULL_HASH}/backup")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "backup_id": "abc_1",
                "size": 100,
                "checksum": "sha256:beef",
                "expires_at": "2026-05-24T12:00:00.000000Z",
                "download_url": "https://crn.example/path"
            })))
            .mount(&server)
            .await;

        // scheduler_url is unused because args.crn_url is set + vm_id is full hash.
        let scheduler_url = Url::parse("http://unused.invalid/").unwrap();
        let args = InstanceBackupInfoArgs {
            vm_id: FULL_HASH.to_string(),
            crn_url: Some(server.uri()),
            signing: evm_signing_args(),
        };
        handle_info(scheduler_url, true, args).await.unwrap();
    }
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
