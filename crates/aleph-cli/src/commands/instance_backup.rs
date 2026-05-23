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
    scheduler_url: Url,
    json: bool,
    args: InstanceBackupCreateArgs,
) -> Result<()> {
    use aleph_sdk::crn::CreateBackupOpts;

    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id, args.crn_url.as_deref()).await?;
    let client = build_client(&crn_url, &args.signing)?;
    let opts = CreateBackupOpts {
        include_volumes: args.include_volumes,
        skip_fsfreeze: args.skip_fsfreeze,
    };
    let initial = client.create_backup(&vm_id, opts).await?;

    let result = if args.follow {
        use aleph_sdk::crn::CreateBackup;
        match initial {
            CreateBackup::Complete(meta) => CreateBackup::Complete(meta),
            CreateBackup::Started => {
                eprintln!("Backup queued for {vm_id}, polling...");
                let outcome = poll_until_complete(
                    || async { Ok(client.get_backup(&vm_id).await?) },
                    |d| async move { tokio::time::sleep(d).await },
                    FOLLOW_TIMEOUT,
                    FOLLOW_POLL_INTERVAL,
                )
                .await?;
                match outcome {
                    FollowOutcome::Complete(meta) => CreateBackup::Complete(meta),
                    FollowOutcome::NotFound => {
                        anyhow::bail!(
                            "backup vanished while polling for {vm_id} (CRN returned 404)"
                        );
                    }
                    FollowOutcome::Timeout => {
                        anyhow::bail!(
                            "backup still in progress after 30 minutes; run 'aleph instance backup info {vm_id}' later"
                        );
                    }
                }
            }
        }
    } else {
        initial
    };
    render_create_result(&vm_id, json, &result);
    Ok(())
}

fn render_create_result(
    vm_id: &aleph_types::item_hash::ItemHash,
    json: bool,
    result: &aleph_sdk::crn::CreateBackup,
) {
    use aleph_sdk::crn::CreateBackup;
    if json {
        match result {
            CreateBackup::Started => println!(
                "{}",
                serde_json::json!({"vm_id": vm_id.to_string(), "status": "queued"})
            ),
            CreateBackup::Complete(meta) => {
                println!("{}", serde_json::to_string_pretty(meta).unwrap())
            }
        }
    } else {
        match result {
            CreateBackup::Started => eprintln!(
                "Backup queued for {vm_id}. Run 'aleph instance backup info {vm_id}' to check status."
            ),
            CreateBackup::Complete(meta) => {
                eprintln!("Backup complete for {vm_id}.");
                println!("backup_id    {}", meta.backup_id);
                println!("size         {} bytes", meta.size);
                println!("checksum     {}", meta.checksum);
                println!("expires_at   {}", meta.expires_at);
                println!("download_url {}", meta.download_url);
            }
        }
    }
}

const FOLLOW_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const FOLLOW_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Outcome of `poll_until_complete`. `Timeout` carries no data; the caller
/// already knows which VM it was polling.
pub(crate) enum FollowOutcome {
    Complete(aleph_sdk::crn::BackupMetadata),
    NotFound,
    Timeout,
}

/// Poll `fetch_status` until it returns `Complete` / `NotFound`, or until
/// `timeout` elapses. `sleep` lets tests inject a no-op delay.
pub(crate) async fn poll_until_complete<F, Fut, S, SFut>(
    mut fetch_status: F,
    mut sleep: S,
    timeout: std::time::Duration,
    poll_interval: std::time::Duration,
) -> anyhow::Result<FollowOutcome>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<aleph_sdk::crn::BackupStatus>>,
    S: FnMut(std::time::Duration) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    use aleph_sdk::crn::BackupStatus;
    let start = std::time::Instant::now();
    loop {
        match fetch_status().await? {
            BackupStatus::Complete(meta) => return Ok(FollowOutcome::Complete(meta)),
            BackupStatus::NotFound => return Ok(FollowOutcome::NotFound),
            BackupStatus::InProgress => {
                if start.elapsed() >= timeout {
                    return Ok(FollowOutcome::Timeout);
                }
                sleep(poll_interval).await;
            }
        }
    }
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

    #[tokio::test]
    async fn delete_succeeds_against_mock_crn() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path(format!("/control/machine/{FULL_HASH}/backup/abc_1")))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let scheduler_url = Url::parse("http://unused.invalid/").unwrap();
        let args = InstanceBackupDeleteArgs {
            vm_id: FULL_HASH.to_string(),
            backup_id: "abc_1".to_string(),
            crn_url: Some(server.uri()),
            signing: evm_signing_args(),
        };
        handle_delete(scheduler_url, true, args).await.unwrap();
    }

    #[tokio::test]
    async fn create_returns_queued_on_202() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("/control/machine/{FULL_HASH}/backup")))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let scheduler_url = Url::parse("http://unused.invalid/").unwrap();
        let args = InstanceBackupCreateArgs {
            vm_id: FULL_HASH.to_string(),
            include_volumes: false,
            skip_fsfreeze: false,
            follow: false,
            crn_url: Some(server.uri()),
            signing: evm_signing_args(),
        };
        handle_create(scheduler_url, true, args).await.unwrap();
    }

    #[tokio::test]
    async fn poll_until_complete_returns_complete_immediately() {
        use aleph_sdk::crn::{BackupMetadata, BackupStatus};
        let meta = BackupMetadata {
            backup_id: "x".into(),
            size: 1,
            checksum: "sha256:00".into(),
            expires_at: "now".into(),
            download_url: "https://x".into(),
            volumes: vec![],
            extra: Default::default(),
        };
        let mut returned = Some(BackupStatus::Complete(meta));
        let outcome = poll_until_complete(
            || {
                let v = returned.take().unwrap();
                async move { Ok(v) }
            },
            |_| async {},
            std::time::Duration::from_secs(60),
            std::time::Duration::from_millis(0),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, FollowOutcome::Complete(_)));
    }

    #[tokio::test]
    async fn poll_until_complete_polls_then_completes() {
        use aleph_sdk::crn::{BackupMetadata, BackupStatus};
        use std::cell::RefCell;
        let calls = RefCell::new(0usize);
        let outcome = poll_until_complete(
            || async {
                let n = {
                    let mut c = calls.borrow_mut();
                    *c += 1;
                    *c
                };
                Ok(if n < 3 {
                    BackupStatus::InProgress
                } else {
                    BackupStatus::Complete(BackupMetadata {
                        backup_id: "x".into(),
                        size: 1,
                        checksum: "sha256:00".into(),
                        expires_at: "now".into(),
                        download_url: "https://x".into(),
                        volumes: vec![],
                        extra: Default::default(),
                    })
                })
            },
            |_| async {},
            std::time::Duration::from_secs(60),
            std::time::Duration::from_millis(0),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, FollowOutcome::Complete(_)));
        assert_eq!(*calls.borrow(), 3);
    }

    #[tokio::test]
    async fn poll_until_complete_times_out() {
        use aleph_sdk::crn::BackupStatus;
        let outcome = poll_until_complete(
            || async { Ok(BackupStatus::InProgress) },
            |_| async {},
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(0),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, FollowOutcome::Timeout));
    }

    #[tokio::test]
    async fn poll_until_complete_propagates_not_found() {
        use aleph_sdk::crn::BackupStatus;
        let outcome = poll_until_complete(
            || async { Ok(BackupStatus::NotFound) },
            |_| async {},
            std::time::Duration::from_secs(60),
            std::time::Duration::from_millis(0),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, FollowOutcome::NotFound));
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
    scheduler_url: Url,
    json: bool,
    args: InstanceBackupDeleteArgs,
) -> Result<()> {
    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id, args.crn_url.as_deref()).await?;
    let client = build_client(&crn_url, &args.signing)?;
    client.delete_backup(&vm_id, &args.backup_id).await?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "vm_id": vm_id.to_string(),
                "backup_id": args.backup_id,
                "status": "deleted"
            })
        );
    } else {
        eprintln!("Deleted backup {} for {vm_id}.", args.backup_id);
    }
    Ok(())
}

async fn handle_restore(
    _scheduler_url: Url,
    _json: bool,
    _args: InstanceBackupRestoreArgs,
) -> Result<()> {
    anyhow::bail!("instance backup restore: not yet implemented")
}
