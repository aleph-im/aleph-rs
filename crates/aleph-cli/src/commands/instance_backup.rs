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

async fn handle_info(scheduler_url: Url, json: bool, args: InstanceBackupInfoArgs) -> Result<()> {
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

/// Default output filename for a downloaded backup: `./backup-<first-12-of-hash>.tar`.
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

/// Policy for verifying the SHA-256 of a downloaded backup.
enum ChecksumPolicy {
    /// Caller knows the expected hex digest (with or without `sha256:`). A
    /// mismatch is a hard error; the partial file is deleted before bailing.
    Required(String),
    /// Read the digest from the response's `X-Backup-Checksum` header. If the
    /// header is absent, emit a stderr warning and skip verification. A
    /// mismatch is still a hard error when the header is present.
    FromHeader,
}

/// GET `url`, stream to `<output>.part`, verify SHA-256 per `policy`, rename
/// atomically, and print the final path + size + checksum. Used by both forms
/// of `aleph instance backup download` (VM-id resolution and direct URL).
///
/// The caller passes the `reqwest::Client` so the VM-id form can reuse the
/// `CrnClient`'s underlying client (and therefore its TLS connection) instead
/// of opening a fresh one for the presigned-URL GET.
async fn download_and_render(
    http: &reqwest::Client,
    url: &str,
    output: std::path::PathBuf,
    policy: ChecksumPolicy,
    json: bool,
) -> Result<()> {
    let mut part = output.clone();
    part.as_mut_os_string().push(".part");

    let response = http.get(url).send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("backup download failed: HTTP {status}: {body}");
    }

    // Extract the expected digest BEFORE stream_to_part_file consumes the response.
    let expected = match &policy {
        ChecksumPolicy::Required(c) => Some(normalize_sha256(c)),
        ChecksumPolicy::FromHeader => response
            .headers()
            .get("X-Backup-Checksum")
            .and_then(|v| v.to_str().ok())
            .map(normalize_sha256),
    };
    let header_was_missing = matches!(policy, ChecksumPolicy::FromHeader) && expected.is_none();

    let (digest, written) = stream_to_part_file(response, &part).await?;
    if let Some(expected) = &expected {
        if &digest != expected {
            let _ = tokio::fs::remove_file(&part).await;
            anyhow::bail!(
                "checksum mismatch: expected {expected}, computed {digest}. Partial file deleted."
            );
        }
    } else if header_was_missing {
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

/// Derive a default output filename from the presigned download URL.
///
/// The CRN's URL ends in `/control/machine/<vm>/backup/<backup_id>?...`, so
/// the last non-empty path segment is the `<backup_id>` (e.g.
/// `5a586d6f...-2026-05-26-14-30-00`). Returns `<segment>.tar`, or
/// `backup.tar` as a last-resort fallback when the URL exposes no usable
/// segment.
fn default_url_output(url: &Url) -> std::path::PathBuf {
    let last_segment = url
        .path_segments()
        .and_then(|segments| segments.rev().find(|s| !s.is_empty()));
    match last_segment {
        Some(name) if name.ends_with(".tar") => std::path::PathBuf::from(name),
        Some(name) => std::path::PathBuf::from(format!("{name}.tar")),
        None => std::path::PathBuf::from("backup.tar"),
    }
}

async fn download_from_url(url: Url, output: Option<std::path::PathBuf>, json: bool) -> Result<()> {
    let output = output.unwrap_or_else(|| default_url_output(&url));
    let http = reqwest::Client::new();
    download_and_render(
        &http,
        url.as_str(),
        output,
        ChecksumPolicy::FromHeader,
        json,
    )
    .await
}

/// What kind of file the user passed to `restore --file`.
#[derive(Debug)]
enum RestoreSource {
    /// A raw QCOW2 image. Uploaded as-is.
    Qcow2 { size: u64 },
    /// An `aleph instance backup download` archive. The `rootfs.qcow2`
    /// member is streamed out of the tar and uploaded; other members
    /// (data volumes) are ignored, since the CRN's restore endpoint only
    /// accepts a rootfs replacement.
    BackupTar { rootfs_size: u64 },
}

const QCOW2_MAGIC: &[u8; 4] = b"QFI\xfb";
const TAR_MAGIC_OFFSET: usize = 257;
const TAR_MAGIC: &[u8; 5] = b"ustar";
const ROOTFS_MEMBER_NAME: &str = "rootfs.qcow2";

/// Inspect the first 512 bytes of `path` and classify it as a raw QCOW2 or
/// an aleph backup tar. Walks tar headers (cheap, microseconds) to find the
/// rootfs size up front so the multipart `Content-Length` is known.
async fn detect_restore_source(path: &std::path::Path) -> Result<RestoreSource> {
    use tokio::io::AsyncReadExt;
    let mut header = [0u8; 512];
    let mut file = tokio::fs::File::open(path).await?;
    let read = file.read(&mut header).await?;
    if read >= QCOW2_MAGIC.len() && &header[..QCOW2_MAGIC.len()] == QCOW2_MAGIC {
        let size = tokio::fs::metadata(path).await?.len();
        return Ok(RestoreSource::Qcow2 { size });
    }
    if read >= TAR_MAGIC_OFFSET + TAR_MAGIC.len()
        && &header[TAR_MAGIC_OFFSET..TAR_MAGIC_OFFSET + TAR_MAGIC.len()] == TAR_MAGIC
    {
        let path_owned = path.to_path_buf();
        let rootfs_size = tokio::task::spawn_blocking(move || find_rootfs_in_tar(&path_owned))
            .await
            .map_err(|e| anyhow::anyhow!("tar inspection task failed: {e}"))??;
        return Ok(RestoreSource::BackupTar { rootfs_size });
    }
    anyhow::bail!(
        "{} is neither a QCOW2 image nor an aleph backup archive. Pass a raw \
         .qcow2 or a backup .tar produced by 'aleph instance backup download'.",
        path.display()
    );
}

/// Walk tar headers to find the size of the `rootfs.qcow2` member. Reads
/// only the headers, not the payload; runs in microseconds even on a
/// multi-gigabyte archive.
fn find_rootfs_in_tar(path: &std::path::Path) -> Result<u64> {
    let file = std::fs::File::open(path)?;
    let mut archive = tar::Archive::new(file);
    for entry in archive.entries()? {
        let entry = entry?;
        if entry.path()?.to_str() == Some(ROOTFS_MEMBER_NAME) {
            return Ok(entry.header().size()?);
        }
    }
    anyhow::bail!(
        "'{ROOTFS_MEMBER_NAME}' not found inside {}. This does not look like an \
         aleph backup archive.",
        path.display()
    );
}

/// Synchronously stream the `rootfs.qcow2` member's bytes out of a tar into
/// `tx`. Runs inside `tokio::task::spawn_blocking`. On any I/O error or if
/// the member is missing, sends the error through `tx` so the consumer's
/// stream surfaces it.
fn stream_rootfs_from_tar(
    path: &std::path::Path,
    tx: &tokio::sync::mpsc::Sender<std::io::Result<bytes::Bytes>>,
) {
    use std::io::Read;
    let result = (|| -> std::io::Result<bool> {
        let file = std::fs::File::open(path)?;
        let mut archive = tar::Archive::new(file);
        for entry in archive.entries()? {
            let mut entry = entry?;
            if entry.path()?.to_str() == Some(ROOTFS_MEMBER_NAME) {
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    let n = entry.read(&mut buf)?;
                    if n == 0 {
                        return Ok(true);
                    }
                    let chunk = bytes::Bytes::copy_from_slice(&buf[..n]);
                    if tx.blocking_send(Ok(chunk)).is_err() {
                        // Receiver dropped (e.g. upload aborted); exit cleanly.
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    })();
    match result {
        Ok(true) => {}
        Ok(false) => {
            let _ = tx.blocking_send(Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("'{ROOTFS_MEMBER_NAME}' not found in tar"),
            )));
        }
        Err(e) => {
            let _ = tx.blocking_send(Err(e));
        }
    }
}

/// Wrap an upload byte stream so it reports progress to stderr every ~500 ms.
/// Symmetric with `stream_to_part_file` on the download path; emits a final
/// 100% line on the last chunk so the rendered percentage doesn't get stuck
/// just below 100% when the last chunk arrives between ticks.
///
/// Delegates the byte-counting/throttling to the SDK's shared combinator and
/// the rendering to [`crate::common::render_upload_progress`], so backup
/// restore and `aleph file`/`aleph program` uploads stay in lockstep.
fn with_upload_progress<S>(stream: S, total: u64) -> impl futures_util::Stream<Item = S::Item>
where
    S: futures_util::Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
{
    aleph_sdk::progress::report_upload_progress(
        stream,
        total,
        crate::common::render_upload_progress,
    )
}

async fn restore_from_file(
    client: &CrnClient,
    vm_id: &aleph_types::item_hash::ItemHash,
    path: &std::path::Path,
) -> Result<aleph_sdk::crn::RestoreResponse> {
    let source = detect_restore_source(path).await?;

    let (upload_size, body, display, tar_task) = match source {
        RestoreSource::Qcow2 { size } => {
            let file = tokio::fs::File::open(path).await?;
            let stream = tokio_util::io::ReaderStream::new(file);
            let body = reqwest::Body::wrap_stream(with_upload_progress(stream, size));
            (size, body, format!("{}", path.display()), None)
        }
        RestoreSource::BackupTar { rootfs_size } => {
            let path_owned = path.to_path_buf();
            let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<bytes::Bytes>>(8);
            let handle = tokio::task::spawn_blocking(move || {
                stream_rootfs_from_tar(&path_owned, &tx);
            });
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            let body = reqwest::Body::wrap_stream(with_upload_progress(stream, rootfs_size));
            (
                rootfs_size,
                body,
                format!("{} ({ROOTFS_MEMBER_NAME} inside)", path.display()),
                Some(handle),
            )
        }
    };

    let part = reqwest::multipart::Part::stream_with_length(body, upload_size)
        .file_name(ROOTFS_MEMBER_NAME.to_string())
        .mime_str("application/octet-stream")?;
    let form = reqwest::multipart::Form::new().part("rootfs", part);

    let endpoint = client.restore_endpoint(vm_id)?;
    let mut request = client.http_client().post(endpoint.url).multipart(form);
    for (name, value) in &endpoint.headers {
        request = request.header(*name, value);
    }
    eprintln!("Uploading {upload_size} bytes from {display}...");
    let upload_result = request.send().await;
    eprintln!();

    // If the tar producer panicked, the channel closed early and the upload
    // likely failed with a truncation error. Surface the panic as the cause
    // instead so the user sees the actual problem, not the downstream symptom.
    if let Some(handle) = tar_task
        && let Err(join_err) = handle.await
        && join_err.is_panic()
    {
        anyhow::bail!(
            "tar streaming task panicked while reading {ROOTFS_MEMBER_NAME}; \
             the upload was likely truncated"
        );
    }

    let response = upload_result?;
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

async fn handle_download(
    scheduler_url: Url,
    json: bool,
    args: InstanceBackupDownloadArgs,
) -> Result<()> {
    use aleph_sdk::crn::BackupStatus;

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
    eprintln!("Downloading backup for {vm_id} ({} bytes)...", meta.size);
    download_and_render(
        client.http_client(),
        &meta.download_url,
        output,
        ChecksumPolicy::Required(meta.checksum),
        json,
    )
    .await
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
        eprintln!(
            "Restored {} (status: {}).",
            response.vm_hash, response.status
        );
        if let Some(old) = &response.old_rootfs_backup {
            eprintln!("Previous rootfs backed up at {old}.");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{ChainCli, IdentityArgs, SigningArgs};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    /// Custom matcher: pass when the request body contains the given byte
    /// sequence anywhere. Unlike wiremock's `body_string_contains`, this does
    /// not require the whole body to be valid UTF-8, which matters for
    /// multipart payloads carrying binary (e.g. QCOW2 magic bytes).
    struct BodyContains(&'static [u8]);
    impl wiremock::Match for BodyContains {
        fn matches(&self, request: &Request) -> bool {
            request.body.windows(self.0.len()).any(|w| w == self.0)
        }
    }

    /// Inverse of `BodyContains`. Lets us assert "this multipart body was
    /// NOT the wrapping tar" by checking the tar magic (`ustar`) is absent.
    struct BodyDoesNotContain(&'static [u8]);
    impl wiremock::Match for BodyDoesNotContain {
        fn matches(&self, request: &Request) -> bool {
            !request.body.windows(self.0.len()).any(|w| w == self.0)
        }
    }

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

    #[test]
    fn default_url_output_extracts_backup_id_from_crn_path() {
        let url = Url::parse(
            "https://crn.example/control/machine/abc/backup/abc-2026-05-26?signature=x&expires=1",
        )
        .unwrap();
        assert_eq!(
            default_url_output(&url),
            std::path::PathBuf::from("abc-2026-05-26.tar")
        );
    }

    #[test]
    fn default_url_output_keeps_existing_tar_suffix() {
        let url = Url::parse("https://crn.example/dl/backup-foo.tar?sig=x").unwrap();
        assert_eq!(
            default_url_output(&url),
            std::path::PathBuf::from("backup-foo.tar")
        );
    }

    #[test]
    fn default_url_output_falls_back_when_no_segment() {
        let url = Url::parse("https://crn.example/?sig=x").unwrap();
        assert_eq!(
            default_url_output(&url),
            std::path::PathBuf::from("backup.tar")
        );
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
        let err = handle_download(scheduler_url, true, args)
            .await
            .unwrap_err();
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
    async fn download_url_form_warns_when_checksum_header_missing() {
        // No X-Backup-Checksum header on the response: download_and_render
        // should warn and write the file anyway (vs. the
        // ChecksumPolicy::Required path used by the VM-id branch, which
        // would have errored on a missing/mismatched digest).
        let server = MockServer::start().await;
        let body = b"unverified-bytes".to_vec();
        Mock::given(method("GET"))
            .and(path("/dl"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
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
        // The .part file is gone (rename happened) even with no checksum.
        assert!(!tmpdir.path().join("out.tar.part").exists());
    }

    /// Build a fake QCOW2-like blob: the 4-byte magic followed by `filler`
    /// bytes of payload. The CLI's source-detection only reads the magic,
    /// and the test CRN doesn't decode the payload either.
    fn fake_qcow2(filler: &[u8]) -> Vec<u8> {
        let mut bytes = QCOW2_MAGIC.to_vec();
        bytes.extend_from_slice(filler);
        bytes
    }

    /// Build a fake aleph backup tar in memory containing a single
    /// `rootfs.qcow2` member whose payload is `qcow2_bytes`. Returns the
    /// fully-serialized tar.
    fn fake_backup_tar(qcow2_bytes: &[u8]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_path(ROOTFS_MEMBER_NAME).unwrap();
        header.set_size(qcow2_bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, qcow2_bytes).unwrap();
        builder.into_inner().unwrap()
    }

    #[tokio::test]
    async fn restore_file_uploads_qcow2_directly() {
        let server = MockServer::start().await;
        // Assert the multipart body actually contains the QCOW2 payload bytes,
        // not just that the request hit the right path.
        Mock::given(method("POST"))
            .and(path(format!("/control/machine/{FULL_HASH}/restore")))
            .and(BodyContains(b"qcow2-payload"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "restored",
                "vm_hash": FULL_HASH
            })))
            .mount(&server)
            .await;

        let tmpdir = tempfile::tempdir().unwrap();
        let qcow = tmpdir.path().join("rootfs.qcow2");
        tokio::fs::write(&qcow, fake_qcow2(b"qcow2-payload"))
            .await
            .unwrap();

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
    async fn restore_file_extracts_rootfs_from_backup_tar() {
        let server = MockServer::start().await;
        // Stricter than just method+path: assert the extracted QCOW2 content
        // is present in the multipart body, AND that the wrapping tar's
        // `ustar` header magic is NOT - catching a regression where the
        // whole backup tar gets uploaded instead of just rootfs.qcow2.
        Mock::given(method("POST"))
            .and(path(format!("/control/machine/{FULL_HASH}/restore")))
            .and(BodyContains(b"qcow2-from-tar"))
            .and(BodyDoesNotContain(b"ustar"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "restored",
                "vm_hash": FULL_HASH
            })))
            .mount(&server)
            .await;

        let tmpdir = tempfile::tempdir().unwrap();
        let tar_path = tmpdir.path().join("backup-deadbeef.tar");
        let qcow2 = fake_qcow2(b"qcow2-from-tar");
        tokio::fs::write(&tar_path, fake_backup_tar(&qcow2))
            .await
            .unwrap();

        let scheduler_url = Url::parse("http://unused.invalid/").unwrap();
        let args = InstanceBackupRestoreArgs {
            vm_id: FULL_HASH.to_string(),
            file: Some(tar_path.clone()),
            volume_ref: None,
            crn_url: Some(server.uri()),
            signing: evm_signing_args(),
        };
        handle_restore(scheduler_url, true, args).await.unwrap();
    }

    #[tokio::test]
    async fn restore_file_rejects_unrecognized_format() {
        let tmpdir = tempfile::tempdir().unwrap();
        let junk = tmpdir.path().join("garbage.bin");
        tokio::fs::write(&junk, b"this is not a qcow2 or a tar archive")
            .await
            .unwrap();
        let err = detect_restore_source(&junk).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not a QCOW2 image") || msg.contains("aleph backup archive"));
    }

    #[tokio::test]
    async fn restore_file_rejects_tar_without_rootfs() {
        let tmpdir = tempfile::tempdir().unwrap();
        let tar_path = tmpdir.path().join("no-rootfs.tar");
        // Build a tar whose only member is something other than rootfs.qcow2.
        let mut builder = tar::Builder::new(Vec::new());
        let mut header = tar::Header::new_gnu();
        header.set_path("other.bin").unwrap();
        header.set_size(3);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, &b"abc"[..]).unwrap();
        let tar_bytes = builder.into_inner().unwrap();
        tokio::fs::write(&tar_path, &tar_bytes).await.unwrap();

        let err = detect_restore_source(&tar_path).await.unwrap_err();
        assert!(err.to_string().contains(ROOTFS_MEMBER_NAME));
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
