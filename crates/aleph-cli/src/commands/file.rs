use crate::cli::{
    FileCommand, FileDeleteArgs, FileDownloadArgs, FileListArgs, FilePinArgs, FileUploadArgs,
    PaymentTypeCli, SortOrderCli, StorageEngineCli,
};
use crate::common::{
    print_submission_result, report_authenticated_upload_status, resolve_account, resolve_address,
    submit_or_preview,
};
use aleph_sdk::client::{
    AccountFilesResponse, AlephAccountClient, AlephClient, AlephStorageClient, hash_file,
};
use aleph_sdk::messages::StoreBuilder;
use aleph_sdk::verify::Hasher;
use aleph_types::account::Account;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::base::Payment;
use aleph_types::message::{FileRef, StorageEngine};
use anyhow::{Context, Result, bail};
use url::Url;

use super::message::{ForgetTargets, forget_targets};

fn resolve_payment(choice: Option<PaymentTypeCli>) -> Payment {
    match choice.unwrap_or(PaymentTypeCli::Credit) {
        PaymentTypeCli::Hold => Payment::hold(),
        PaymentTypeCli::Credit => Payment::credits(),
    }
}

pub async fn handle_file_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: FileCommand,
) -> Result<()> {
    match command {
        FileCommand::Upload(args) => {
            handle_file_upload(aleph_client, ccn_url, json, args).await?;
        }
        FileCommand::Pin(args) => {
            handle_file_pin(aleph_client, ccn_url, json, args).await?;
        }
        FileCommand::Download(args) => {
            handle_file_download(aleph_client, json, args).await?;
        }
        FileCommand::List(args) => {
            handle_file_list(aleph_client, json, args).await?;
        }
        FileCommand::Delete(args) => {
            handle_file_delete(aleph_client, ccn_url, json, args).await?;
        }
    }
    Ok(())
}

async fn handle_file_upload(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FileUploadArgs,
) -> Result<()> {
    if !args.path.exists() {
        bail!("path not found: {}", args.path.display());
    }
    if args.path.is_file() {
        handle_single_file_upload(aleph_client, ccn_url, json, args).await
    } else if args.path.is_dir() {
        handle_folder_upload(aleph_client, ccn_url, json, args).await
    } else {
        bail!("not a regular file or directory: {}", args.path.display())
    }
}

async fn handle_single_file_upload(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FileUploadArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    let storage_engine = match args.storage_engine.unwrap_or(StorageEngineCli::Storage) {
        StorageEngineCli::Storage => StorageEngine::Storage,
        StorageEngineCli::Ipfs => StorageEngine::Ipfs,
    };

    if !json {
        eprintln!("Hashing {}...", args.path.display());
    }
    let file_hash = match storage_engine {
        StorageEngine::Storage => hash_file(&args.path, Hasher::for_storage()).await?,
        StorageEngine::Ipfs => hash_file(&args.path, Hasher::for_ipfs()).await?,
    };
    if !json {
        eprintln!("  File hash: {file_hash}");
    }

    let mut builder = StoreBuilder::new(&account, file_hash.clone(), storage_engine)
        .payment(resolve_payment(args.payment_type));
    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(reference) = args.reference {
        builder = builder.reference(reference);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;

    if dry_run {
        if json {
            println!("{}", serde_json::to_string_pretty(&pending)?);
        } else {
            eprintln!("Dry run: message not submitted.\n");
            println!("{}", serde_json::to_string_pretty(&pending)?);
        }
        return Ok(());
    }

    if !json {
        eprintln!("Uploading {}...", args.path.display());
    }

    match storage_engine {
        StorageEngine::Storage => {
            aleph_client
                .upload_file_to_storage(&args.path, Some(&pending), true)
                .await?;
        }
        StorageEngine::Ipfs => {
            aleph_client
                .upload_file_to_ipfs(&args.path, Some(&pending), true)
                .await?;
        }
    }
    // The upload endpoint returns 2xx once the file is pinned and the STORE
    // is queued for ingest. pyaleph may still reject the message after that
    // (e.g. insufficient credits), so fetch the final status before claiming
    // success.
    report_authenticated_upload_status(aleph_client, ccn_url, &pending, json).await?;

    Ok(())
}

async fn handle_folder_upload(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FileUploadArgs,
) -> Result<()> {
    use aleph_sdk::folder_hash::hash_folder_root;
    use aleph_sdk::ipfs::{UploadFolderOptions, collect_folder_files};

    // Directory uploads always use IPFS. Reject only when the user explicitly
    // asked for native storage; an unset flag silently picks IPFS.
    if matches!(args.storage_engine, Some(StorageEngineCli::Storage)) {
        bail!(
            "native storage does not support directory uploads; omit --storage-engine or pass --storage-engine ipfs"
        );
    }

    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    let opts = UploadFolderOptions::default();

    if dry_run {
        let entries = walk_folder_summary(&args.path)?;
        let total_bytes: u64 = entries.iter().map(|(_, size)| size).sum();
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "dry_run": true,
                    "files": entries.len(),
                    "size_bytes": total_bytes,
                }))?
            );
        } else {
            eprintln!(
                "Dry run: would upload {} file(s), {} bytes total. No HTTP calls made.",
                entries.len(),
                total_bytes
            );
        }
        return Ok(());
    }

    if args.use_gateway_relay {
        return handle_folder_upload_via_gateway(aleph_client, ccn_url, json, args, opts, &account)
            .await;
    }

    // Authenticated CAR path (default).
    if !json {
        eprintln!("Hashing folder {}...", args.path.display());
    }
    let entries = collect_folder_files(&args.path, opts.follow_symlinks)?;
    let file_hash = hash_folder_root(&entries, &opts)?;
    if !json {
        eprintln!("  Directory CID: {file_hash}");
    }

    let mut builder = StoreBuilder::new(&account, file_hash.clone(), StorageEngine::Ipfs)
        .payment(resolve_payment(args.payment_type));
    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(reference) = args.reference {
        builder = builder.reference(reference);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;

    if !json {
        eprintln!("Uploading folder to CCN...");
    }
    aleph_client
        .upload_folder_to_ipfs_authenticated(&args.path, &pending, true, opts)
        .await?;
    print_submission_result(ccn_url, &pending, "success", "processed", json)?;
    Ok(())
}

async fn handle_folder_upload_via_gateway(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FileUploadArgs,
    opts: aleph_sdk::ipfs::UploadFolderOptions,
    account: &crate::account::CliAccount,
) -> Result<()> {
    let client = if let Some(gateway) = args.ipfs_gateway.clone() {
        aleph_client.clone().with_ipfs_gateway(gateway)
    } else {
        aleph_client.clone()
    };

    if !json {
        eprintln!(
            "Hashing and uploading folder via kubo gateway {}...",
            args.path.display()
        );
    }
    let file_hash = client.upload_folder_to_ipfs(&args.path, opts).await?;
    if !json {
        eprintln!("  Directory CID (verified): {file_hash}");
    }

    let mut builder = StoreBuilder::new(account, file_hash.clone(), StorageEngine::Ipfs)
        .payment(resolve_payment(args.payment_type));
    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(reference) = args.reference {
        builder = builder.reference(reference);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, false, json).await?;
    Ok(())
}

fn walk_folder_summary(root: &std::path::Path) -> Result<Vec<(std::path::PathBuf, u64)>> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(root).follow_links(true).min_depth(1) {
        let entry = entry?;
        if entry.file_type().is_file() {
            let size = entry.metadata()?.len();
            out.push((entry.path().to_path_buf(), size));
        }
    }
    if out.is_empty() {
        bail!("empty folder: {}", root.display());
    }
    Ok(out)
}

async fn handle_file_pin(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FilePinArgs,
) -> Result<()> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing.identity)?;

    // Storage engine is implied by the item hash variant; StoreBuilder::build
    // enforces the pairing so a mismatch is structurally impossible.
    let storage_engine = match args.item_hash {
        ItemHash::Native(_) => StorageEngine::Storage,
        ItemHash::Ipfs(_) => StorageEngine::Ipfs,
    };

    let mut builder =
        StoreBuilder::new(&account, args.item_hash, storage_engine).payment(Payment::credits());
    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }
    if let Some(reference) = args.reference {
        builder = builder.reference(reference);
    }
    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }
    let pending = builder.build()?;

    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

async fn handle_file_download(
    aleph_client: &AlephClient,
    json: bool,
    args: FileDownloadArgs,
) -> Result<()> {
    // Resolve the file hash — for indirect lookups, fetch metadata first.
    let file_hash = if let Some(hash) = args.hash {
        hash
    } else if let Some(message_hash) = args.message_hash {
        if !json {
            eprintln!("Resolving file hash from message {message_hash}...");
        }
        let metadata = aleph_client
            .get_file_metadata_by_message_hash(&message_hash)
            .await?;
        metadata.file_hash
    } else if let Some(reference) = args.reference {
        let owner = args
            .owner
            .context("--owner is required when downloading by --ref")?;
        let file_ref = FileRef::UserDefined {
            owner: resolve_address(&owner)?,
            reference,
        };
        if !json {
            eprintln!("Resolving file hash from ref {file_ref}...");
        }
        let metadata = aleph_client.get_file_metadata_by_ref(&file_ref).await?;
        metadata.file_hash
    } else {
        unreachable!("clap group ensures one of hash/message-hash/ref is provided")
    };

    if !json {
        eprintln!("Downloading {file_hash}...");
    }

    let download = aleph_client.download_file_by_hash(&file_hash).await?;

    if args.stdout {
        let bytes = download.bytes().await?;
        use std::io::Write;
        std::io::stdout().write_all(&bytes)?;
    } else {
        let output = args.output.unwrap_or_else(|| file_hash.to_string().into());
        download.to_file(&output).await?;
        if !json {
            eprintln!("Saved to {}", output.display());
        }
    }

    Ok(())
}

async fn handle_file_list(
    aleph_client: &AlephClient,
    json: bool,
    args: FileListArgs,
) -> Result<()> {
    let address = match args.address.as_deref() {
        Some(value) => resolve_address(value)?,
        None => {
            let identity = crate::cli::IdentityArgs {
                account: None,
                private_key: None,
                chain: None,
            };
            let account = resolve_account(&identity)?;
            account.address().clone()
        }
    };

    let sort_order = match args.sort_order {
        SortOrderCli::Asc => 1,
        SortOrderCli::Desc => -1,
    };

    let response = aleph_client
        .get_account_files(&address, args.count, args.page, sort_order)
        .await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
    } else {
        print!("{}", format_files_table(&response));
    }
    Ok(())
}

async fn handle_file_delete(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FileDeleteArgs,
) -> Result<()> {
    forget_targets(
        aleph_client,
        ccn_url,
        json,
        ForgetTargets {
            hashes: args.hashes,
            aggregates: Vec::new(),
            reason: args.reason,
            channel: args.channel,
            on_behalf_of: args.on_behalf_of,
            yes: args.yes,
            confirm_label: "STORE message",
            signing: args.signing,
        },
    )
    .await
}

fn size_in_mb(size: memsizes::Bytes) -> f64 {
    size.count() as f64 / (1024.0 * 1024.0)
}

fn format_files_table(response: &AccountFilesResponse) -> String {
    use std::fmt::Write;

    const FILE_HASH_HEADER: &str = "FILE_HASH";
    const SIZE_HEADER: &str = "SIZE (MB)";
    const TYPE_HEADER: &str = "TYPE";
    const CREATED_HEADER: &str = "CREATED";
    const ITEM_HASH_HEADER: &str = "ITEM_HASH";

    let file_hash_w = response
        .files
        .iter()
        .map(|f| f.file_hash.len())
        .chain(std::iter::once(FILE_HASH_HEADER.len()))
        .max()
        .unwrap_or(FILE_HASH_HEADER.len());

    let size_strings: Vec<String> = response
        .files
        .iter()
        .map(|f| format!("{:.4}", size_in_mb(f.size)))
        .collect();
    let size_w = size_strings
        .iter()
        .map(|s| s.len())
        .chain(std::iter::once(SIZE_HEADER.len()))
        .max()
        .unwrap_or(SIZE_HEADER.len());

    let type_w = response
        .files
        .iter()
        .map(|f| f.storage_engine.len())
        .chain(std::iter::once(TYPE_HEADER.len()))
        .max()
        .unwrap_or(TYPE_HEADER.len());

    // "%Y-%m-%d %H:%M:%S" is always 19 chars; pad against the header anyway.
    let created_w = CREATED_HEADER.len().max(19);

    let mut out = String::new();
    writeln!(
        out,
        "{:<file_hash_w$}  {:>size_w$}  {:<type_w$}  {:<created_w$}  {}",
        FILE_HASH_HEADER,
        SIZE_HEADER,
        TYPE_HEADER,
        CREATED_HEADER,
        ITEM_HASH_HEADER,
        file_hash_w = file_hash_w,
        size_w = size_w,
        type_w = type_w,
        created_w = created_w,
    )
    .expect("writing to String cannot fail");

    for (file, size_str) in response.files.iter().zip(size_strings.iter()) {
        let created = file.created.format("%Y-%m-%d %H:%M:%S").to_string();
        writeln!(
            out,
            "{:<file_hash_w$}  {:>size_w$}  {:<type_w$}  {:<created_w$}  {}",
            file.file_hash,
            size_str,
            file.storage_engine,
            created,
            file.item_hash,
            file_hash_w = file_hash_w,
            size_w = size_w,
            type_w = type_w,
            created_w = created_w,
        )
        .expect("writing to String cannot fail");
    }

    if response.files.is_empty() {
        writeln!(out, "(no files)").expect("writing to String cannot fail");
    } else {
        let total_mb = size_in_mb(response.total_size);
        let page = if response.pagination_per_page == 0 {
            "1/1".to_string()
        } else {
            let total_pages = (response.pagination_total as f64
                / response.pagination_per_page as f64)
                .ceil() as u64;
            let total_pages = total_pages.max(1);
            format!("{}/{}", response.pagination_page, total_pages)
        };
        writeln!(
            out,
            "\nTotal: {} file(s), ~ {:.4} MB (page {})",
            response.pagination_total, total_mb, page,
        )
        .expect("writing to String cannot fail");
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn walk_folder_summary_counts_files() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("a.txt"), "a").unwrap();
        fs::create_dir(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("sub/b.txt"), "bb").unwrap();
        let entries = walk_folder_summary(tmp.path()).unwrap();
        assert_eq!(entries.len(), 2);
        let total: u64 = entries.iter().map(|(_, s)| s).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn walk_folder_summary_rejects_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let err = walk_folder_summary(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("empty folder"));
    }

    use aleph_sdk::client::AccountFile;
    use chrono::{TimeZone, Utc};
    use memsizes::Bytes;

    fn sample_response() -> AccountFilesResponse {
        AccountFilesResponse {
            address: "0xabc".into(),
            files: vec![
                AccountFile {
                    file_hash: "QmYzN9wJgkRfTDopwzCG7VkrcU8xKZxxJzAv4dQk2tSx9".into(),
                    size: Bytes::from(13_000_000),
                    storage_engine: "ipfs".into(),
                    created: Utc.with_ymd_and_hms(2025, 4, 12, 9, 21, 3).unwrap(),
                    item_hash: "4a0f62da42f4478544616519e6f5d58adb1096e069b392b151d47c3609492d0c"
                        .parse()
                        .unwrap(),
                },
                AccountFile {
                    file_hash: "abc123def456000000000000000000000000000000000000000000000000aabb"
                        .into(),
                    size: Bytes::from(3200),
                    storage_engine: "storage".into(),
                    created: Utc.with_ymd_and_hms(2025, 4, 10, 14, 8, 0).unwrap(),
                    item_hash: "5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e"
                        .parse()
                        .unwrap(),
                },
            ],
            total_size: Bytes::from(13_003_200),
            pagination_page: 1,
            pagination_total: 2,
            pagination_per_page: 25,
        }
    }

    #[test]
    fn format_files_table_includes_headers_and_rows() {
        let out = format_files_table(&sample_response());
        let mut lines = out.lines();
        let header = lines.next().expect("header");
        assert!(header.contains("FILE_HASH"));
        assert!(header.contains("SIZE (MB)"));
        assert!(header.contains("TYPE"));
        assert!(header.contains("CREATED"));
        assert!(header.contains("ITEM_HASH"));

        let row1 = lines.next().expect("row1");
        assert!(row1.contains("QmYzN9wJgkRfTDopwzCG7VkrcU8xKZxxJzAv4dQk2tSx9"));
        assert!(row1.contains("12.3978")); // 13_000_000 / 1024^2
        assert!(row1.contains("ipfs"));
        assert!(row1.contains("2025-04-12 09:21:03"));

        let row2 = lines.next().expect("row2");
        assert!(row2.contains("storage"));
        assert!(row2.contains("0.0031")); // 3200 / 1024^2

        assert!(out.contains("Total: 2 file(s)"));
        assert!(out.contains("page 1/1"));
    }

    #[test]
    fn format_files_table_empty_says_no_files() {
        let empty = AccountFilesResponse {
            address: "0xabc".into(),
            files: vec![],
            total_size: Bytes::from(0),
            pagination_page: 1,
            pagination_total: 0,
            pagination_per_page: 25,
        };
        let out = format_files_table(&empty);
        assert!(out.contains("FILE_HASH"));
        assert!(out.contains("(no files)"));
        // No Total line for the empty case.
        assert!(!out.contains("Total:"));
    }

    #[test]
    fn format_files_table_paginates_total_pages() {
        let mut r = sample_response();
        r.pagination_total = 75;
        r.pagination_per_page = 25;
        r.pagination_page = 2;
        let out = format_files_table(&r);
        assert!(out.contains("page 2/3"));
    }

    #[test]
    fn size_in_mb_converts_using_mib() {
        assert!((size_in_mb(Bytes::from(1_048_576)) - 1.0).abs() < 1e-6);
        assert_eq!(size_in_mb(Bytes::from(0)), 0.0);
    }
}
