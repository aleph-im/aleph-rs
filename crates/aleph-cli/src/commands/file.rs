use crate::cli::{FileCommand, FileDownloadArgs, FilePinArgs, FileUploadArgs, StorageEngineCli};
use crate::common::{print_submission_result, resolve_account, resolve_address, submit_or_preview};
use aleph_sdk::client::{AlephClient, AlephStorageClient, hash_file};
use aleph_sdk::messages::StoreBuilder;
use aleph_sdk::verify::Hasher;
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::execution::base::Payment;
use aleph_types::message::{FileRef, StorageEngine};
use url::Url;

pub async fn handle_file_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: FileCommand,
) -> Result<(), Box<dyn std::error::Error>> {
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
    }
    Ok(())
}

async fn handle_file_upload(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FileUploadArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    if !args.path.exists() {
        return Err(format!("path not found: {}", args.path.display()).into());
    }
    if args.path.is_file() {
        handle_single_file_upload(aleph_client, ccn_url, json, args).await
    } else if args.path.is_dir() {
        handle_folder_upload(aleph_client, ccn_url, json, args).await
    } else {
        Err(format!("not a regular file or directory: {}", args.path.display()).into())
    }
}

async fn handle_single_file_upload(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FileUploadArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing)?;

    let storage_engine = match args.storage_engine {
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

    let mut builder =
        StoreBuilder::new(&account, file_hash.clone(), storage_engine).payment(Payment::credits());
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
            eprintln!("Dry run — message not submitted.\n");
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
            print_submission_result(ccn_url, &pending, "success", "processed", json)?;
        }
        StorageEngine::Ipfs => {
            aleph_client.upload_file_to_ipfs(&args.path).await?;
            submit_or_preview(aleph_client, ccn_url, &pending, false, json).await?;
        }
    }

    Ok(())
}

async fn handle_folder_upload(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FileUploadArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    use aleph_sdk::ipfs::{CidVersion, UploadFolderOptions};

    if matches!(args.storage_engine, StorageEngineCli::Storage) {
        return Err(
            "native storage does not support directory uploads; use --storage-engine ipfs".into(),
        );
    }

    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing)?;

    let opts = UploadFolderOptions {
        cid_version: CidVersion::V1,
        ..Default::default()
    };

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
                "Dry run — would upload {} file(s), {} bytes total. No HTTP calls made.",
                entries.len(),
                total_bytes
            );
        }
        return Ok(());
    }

    let client = if let Some(gateway) = args.ipfs_gateway.clone() {
        aleph_client.clone().with_ipfs_gateway(gateway)
    } else {
        aleph_client.clone()
    };

    if !json {
        eprintln!("Uploading folder {}...", args.path.display());
    }
    let file_hash = client.upload_folder_to_ipfs(&args.path, opts).await?;
    if !json {
        eprintln!("  Directory CID: {file_hash}");
    }

    let mut builder = StoreBuilder::new(&account, file_hash.clone(), StorageEngine::Ipfs)
        .payment(Payment::credits());
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

fn walk_folder_summary(
    root: &std::path::Path,
) -> Result<Vec<(std::path::PathBuf, u64)>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(root).follow_links(true).min_depth(1) {
        let entry = entry?;
        if entry.file_type().is_file() {
            let size = entry.metadata()?.len();
            out.push((entry.path().to_path_buf(), size));
        }
    }
    if out.is_empty() {
        return Err(format!("empty folder: {}", root.display()).into());
    }
    Ok(out)
}

async fn handle_file_pin(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: FilePinArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing)?;

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
) -> Result<(), Box<dyn std::error::Error>> {
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
            .ok_or("--owner is required when downloading by --ref")?;
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
}
