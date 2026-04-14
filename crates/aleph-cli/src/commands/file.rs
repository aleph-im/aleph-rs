use crate::cli::{FileCommand, FileDownloadArgs, FileUploadArgs, StorageEngineCli};
use crate::common::{resolve_account, resolve_address, submit_or_preview};
use aleph_sdk::client::{AlephClient, AlephStorageClient, hash_file};
use aleph_sdk::messages::StoreBuilder;
use aleph_sdk::verify::Hasher;
use aleph_types::channel::Channel;
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
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing)?;

    let storage_engine = match args.storage_engine {
        StorageEngineCli::Storage => StorageEngine::Storage,
        StorageEngineCli::Ipfs => StorageEngine::Ipfs,
    };

    if !args.path.exists() {
        return Err(format!("file not found: {}", args.path.display()).into());
    }
    if !args.path.is_file() {
        return Err(format!("not a file: {}", args.path.display()).into());
    }

    // Hash file locally
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

    // Build STORE message
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

    // Dry run: print message preview without uploading
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
            // Authenticated upload: file + signed STORE message in one request
            aleph_client
                .upload_file_to_storage(&args.path, Some(&pending))
                .await?;
        }
        StorageEngine::Ipfs => {
            // IPFS: fall back to old two-step flow (no authenticated upload support)
            aleph_client.upload_file_to_ipfs(&args.path).await?;
            submit_or_preview(aleph_client, ccn_url, &pending, false, json).await?;
        }
    }

    if !json {
        eprintln!("  File hash: {file_hash}");
    }

    Ok(())
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
