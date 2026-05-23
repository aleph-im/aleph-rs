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

use std::path::Path;

/// Default output filename for a downloaded backup: ./backup-<first-12-of-hash>.tar.
fn default_output_path(vm_id: &aleph_types::item_hash::ItemHash) -> std::path::PathBuf {
    let s = vm_id.to_string();
    let short: String = s.chars().take(12).collect();
    std::path::PathBuf::from(format!("backup-{short}.tar"))
}

/// Strip the optional `sha256:` prefix and lower-case the hex digest.
fn normalize_sha256(value: &str) -> String {
    let trimmed = value.trim();
    trimmed
        .strip_prefix("sha256:")
        .unwrap_or(trimmed)
        .trim()
        .to_ascii_lowercase()
}

/// Stream `response.bytes_stream()` to `dest_part`, return the SHA-256 hex
/// of the bytes written and the total byte count. Reports progress to stderr
/// every ~500 ms when `Content-Length` is known.
async fn stream_to_part_file(
    response: reqwest::Response,
    dest_part: &Path,
) -> anyhow::Result<(String, u64)> {
    use futures_util::StreamExt;
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncWriteExt;

    let total = response.content_length();
    let mut file = tokio::fs::File::create(dest_part).await?;
    let mut stream = response.bytes_stream();
    let mut hasher = Sha256::new();
    let mut written: u64 = 0;
    let mut last_report = std::time::Instant::now();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        hasher.update(&chunk);
        written += chunk.len() as u64;
        if last_report.elapsed() >= std::time::Duration::from_millis(500) {
            match total {
                Some(t) if t > 0 => {
                    let pct = (written as f64 / t as f64 * 100.0).min(100.0);
                    eprint!("\r  downloaded {written}/{t} bytes ({pct:.1}%)");
                }
                _ => eprint!("\r  downloaded {written} bytes"),
            }
            last_report = std::time::Instant::now();
        }
    }
    file.flush().await?;
    eprintln!();
    Ok((hex::encode(hasher.finalize()), written))
}

async fn download_from_url(
    url: Url,
    output: Option<std::path::PathBuf>,
    json: bool,
) -> Result<()> {
    let output = output.unwrap_or_else(|| std::path::PathBuf::from("backup.tar"));
    let mut part = output.clone();
    part.as_mut_os_string().push(".part");

    let http = reqwest::Client::new();
    let response = http.get(url.clone()).send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("backup download failed: HTTP {status}: {body}");
    }
    let expected_checksum = response
        .headers()
        .get("X-Backup-Checksum")
        .and_then(|v| v.to_str().ok())
        .map(|s| normalize_sha256(s));
    let (digest, written) = stream_to_part_file(response, &part).await?;
    if let Some(expected) = expected_checksum {
        if digest != expected {
            let _ = tokio::fs::remove_file(&part).await;
            anyhow::bail!(
                "checksum mismatch: expected {expected}, computed {digest}. Partial file deleted."
            );
        }
    } else {
        eprintln!(
            "warning: download response had no X-Backup-Checksum header; integrity not verified."
        );
    }
    tokio::fs::rename(&part, &output).await?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "path": output.to_string_lossy(),
                "bytes": written,
                "checksum": format!("sha256:{digest}")
            })
        );
    } else {
        eprintln!(
            "Saved {written} bytes to {} (sha256:{digest}).",
            output.display()
        );
    }
    Ok(())
}

async fn restore_from_file(
    client: &CrnClient,
    vm_id: &aleph_types::item_hash::ItemHash,
    path: &std::path::Path,
) -> Result<aleph_sdk::crn::RestoreResponse> {
    let file_size = tokio::fs::metadata(path).await?.len();
    let file = tokio::fs::File::open(path).await?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = reqwest::Body::wrap_stream(stream);
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("rootfs.qcow2")
        .to_string();
    let part = reqwest::multipart::Part::stream_with_length(body, file_size)
        .file_name(file_name)
        .mime_str("application/octet-stream")?;
    let form = reqwest::multipart::Form::new().part("rootfs", part);

    let (url, headers) = client.restore_endpoint(vm_id)?;
    let http = reqwest::Client::new();
    let mut request = http.post(url).multipart(form);
    for (name, value) in &headers {
        request = request.header(*name, value);
    }
    eprintln!("Uploading {file_size} bytes from {}...", path.display());
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        if status.as_u16() == 413 {
            anyhow::bail!(
                "instance rejected the upload: too large (413). The CRN typically caps restores; check the CRN's documented limit."
            );
        }
        anyhow::bail!("restore failed: HTTP {status}: {body}");
    }
    Ok(response.json().await?)
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

    #[test]
    fn default_output_path_uses_short_hash() {
        let hash: aleph_types::item_hash::ItemHash = FULL_HASH.parse().unwrap();
        assert_eq!(
            default_output_path(&hash),
            std::path::PathBuf::from("backup-5a586d6f59f6.tar")
        );
    }

    #[test]
    fn normalize_sha256_strips_prefix() {
        assert_eq!(normalize_sha256("sha256:DEADBEEF"), "deadbeef");
        assert_eq!(normalize_sha256("deadbeef"), "deadbeef");
        assert_eq!(normalize_sha256(" sha256:abc "), "abc");
    }

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
    async fn download_streams_atomically_and_verifies_checksum() {
        use sha2::{Digest, Sha256};

        let download_server = MockServer::start().await;
        let crn_server = MockServer::start().await;
        let body = b"hello backup tar".to_vec();
        let expected_digest = hex::encode(Sha256::digest(&body));

        Mock::given(method("GET"))
            .and(path("/dl"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
            .mount(&download_server)
            .await;

        let metadata = serde_json::json!({
            "backup_id": "abc_1",
            "size": body.len(),
            "checksum": format!("sha256:{expected_digest}"),
            "expires_at": "2026-05-24T12:00:00.000000Z",
            "download_url": format!("{}/dl", download_server.uri()),
        });
        Mock::given(method("GET"))
            .and(path(format!("/control/machine/{FULL_HASH}/backup")))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata))
            .mount(&crn_server)
            .await;

        let tmpdir = tempfile::tempdir().unwrap();
        let output = tmpdir.path().join("out.tar");
        let scheduler_url = Url::parse("http://unused.invalid/").unwrap();
        let args = InstanceBackupDownloadArgs {
            vm_id_or_url: FULL_HASH.to_string(),
            output: Some(output.clone()),
            crn_url: Some(crn_server.uri()),
            signing: evm_signing_args(),
        };
        handle_download(scheduler_url, true, args).await.unwrap();

        let written = tokio::fs::read(&output).await.unwrap();
        assert_eq!(written, body);
        assert!(!tmpdir.path().join("out.tar.part").exists());
    }

    #[tokio::test]
    async fn download_aborts_on_checksum_mismatch() {
        let download_server = MockServer::start().await;
        let crn_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/dl"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"corrupt-data".to_vec()))
            .mount(&download_server)
            .await;
        let metadata = serde_json::json!({
            "backup_id": "abc_1",
            "size": 12,
            "checksum": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            "expires_at": "2026-05-24T12:00:00.000000Z",
            "download_url": format!("{}/dl", download_server.uri()),
        });
        Mock::given(method("GET"))
            .and(path(format!("/control/machine/{FULL_HASH}/backup")))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata))
            .mount(&crn_server)
            .await;

        let tmpdir = tempfile::tempdir().unwrap();
        let output = tmpdir.path().join("out.tar");
        let scheduler_url = Url::parse("http://unused.invalid/").unwrap();
        let args = InstanceBackupDownloadArgs {
            vm_id_or_url: FULL_HASH.to_string(),
            output: Some(output.clone()),
            crn_url: Some(crn_server.uri()),
            signing: evm_signing_args(),
        };
        let err = handle_download(scheduler_url, true, args).await.unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
        assert!(!output.exists());
        assert!(!tmpdir.path().join("out.tar.part").exists());
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

    #[tokio::test]
    async fn download_url_form_verifies_via_header() {
        use sha2::{Digest, Sha256};

        let server = MockServer::start().await;
        let body = b"some-tar-content".to_vec();
        let digest = hex::encode(Sha256::digest(&body));
        Mock::given(method("GET"))
            .and(path("/dl"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(body.clone())
                    .insert_header("X-Backup-Checksum", format!("sha256:{digest}").as_str()),
            )
            .mount(&server)
            .await;

        let tmpdir = tempfile::tempdir().unwrap();
        let output = tmpdir.path().join("out.tar");
        let scheduler_url = Url::parse("http://unused.invalid/").unwrap();
        let args = InstanceBackupDownloadArgs {
            vm_id_or_url: format!("{}/dl", server.uri()),
            output: Some(output.clone()),
            crn_url: None,
            signing: evm_signing_args(),
        };
        handle_download(scheduler_url, true, args).await.unwrap();
        assert_eq!(tokio::fs::read(&output).await.unwrap(), body);
    }

    #[tokio::test]
    async fn restore_file_uploads_multipart() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("/control/machine/{FULL_HASH}/restore")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "restored",
                "vm_hash": FULL_HASH
            })))
            .mount(&server)
            .await;

        let tmpdir = tempfile::tempdir().unwrap();
        let qcow = tmpdir.path().join("rootfs.qcow2");
        tokio::fs::write(&qcow, b"fake-qcow2-data").await.unwrap();

        let scheduler_url = Url::parse("http://unused.invalid/").unwrap();
        let args = InstanceBackupRestoreArgs {
            vm_id: FULL_HASH.to_string(),
            file: Some(qcow.clone()),
            volume_ref: None,
            crn_url: Some(server.uri()),
            signing: evm_signing_args(),
        };
        handle_restore(scheduler_url, true, args).await.unwrap();
    }

    #[tokio::test]
    async fn restore_from_volume_ref_calls_crn() {
        let server = MockServer::start().await;
        let volume_ref = "d704be0b15e2fb600c5998581cb9af01bd74a9cf61b586ccc849ad78e0709d77";
        Mock::given(method("POST"))
            .and(path(format!("/control/machine/{FULL_HASH}/restore")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "restored",
                "vm_hash": FULL_HASH,
                "old_rootfs_backup": null
            })))
            .mount(&server)
            .await;

        let scheduler_url = Url::parse("http://unused.invalid/").unwrap();
        let args = InstanceBackupRestoreArgs {
            vm_id: FULL_HASH.to_string(),
            file: None,
            volume_ref: Some(volume_ref.to_string()),
            crn_url: Some(server.uri()),
            signing: evm_signing_args(),
        };
        handle_restore(scheduler_url, true, args).await.unwrap();
    }
}

async fn handle_download(
    scheduler_url: Url,
    json: bool,
    args: InstanceBackupDownloadArgs,
) -> Result<()> {
    use aleph_sdk::crn::BackupStatus;

    // VM-id form vs URL form. The URL form is added in Task 16.
    let parsed_url = Url::parse(&args.vm_id_or_url)
        .ok()
        .filter(|u| !u.scheme().is_empty() && u.has_host());
    if let Some(direct_url) = parsed_url {
        return download_from_url(direct_url, args.output, json).await;
    }

    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id_or_url, args.crn_url.as_deref()).await?;
    let client = build_client(&crn_url, &args.signing)?;
    let meta = match client.get_backup(&vm_id).await? {
        BackupStatus::Complete(m) => m,
        BackupStatus::InProgress => anyhow::bail!(
            "backup for {vm_id} is still in progress; wait or pass --follow on create"
        ),
        BackupStatus::NotFound => anyhow::bail!(
            "no backup found for {vm_id}; create one with 'aleph instance backup create'"
        ),
    };

    let output = args.output.unwrap_or_else(|| default_output_path(&vm_id));
    let mut part = output.clone();
    part.as_mut_os_string().push(".part");

    eprintln!("Downloading backup for {vm_id} ({} bytes)...", meta.size);
    let http = reqwest::Client::new();
    let response = http.get(&meta.download_url).send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("backup download failed: HTTP {status}: {body}");
    }
    let (digest, written) = stream_to_part_file(response, &part).await?;
    let expected = normalize_sha256(&meta.checksum);
    if digest != expected {
        let _ = tokio::fs::remove_file(&part).await;
        anyhow::bail!(
            "checksum mismatch: expected {expected}, computed {digest}. Partial file deleted."
        );
    }
    tokio::fs::rename(&part, &output).await?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "path": output.to_string_lossy(),
                "bytes": written,
                "checksum": format!("sha256:{digest}")
            })
        );
    } else {
        eprintln!(
            "Saved {written} bytes to {} (sha256:{digest}).",
            output.display()
        );
    }
    Ok(())
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
    scheduler_url: Url,
    json: bool,
    args: InstanceBackupRestoreArgs,
) -> Result<()> {
    let (vm_id, crn_url) =
        resolve_target(&scheduler_url, &args.vm_id, args.crn_url.as_deref()).await?;
    let client = build_client(&crn_url, &args.signing)?;

    let response = match (&args.file, &args.volume_ref) {
        (Some(path), None) => restore_from_file(&client, &vm_id, path).await?,
        (None, Some(volume_ref)) => client.restore_from_volume(&vm_id, volume_ref).await?,
        _ => unreachable!("clap arg group enforces exactly one"),
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        eprintln!("Restored {} (status: {}).", response.vm_hash, response.status);
        if let Some(old) = &response.old_rootfs_backup {
            eprintln!("Previous rootfs backed up at {old}.");
        }
    }
    Ok(())
}
