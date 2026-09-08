//! Shared STORE-message upload helper.
//!
//! Used by `aleph vprogram create` (data image + hash tree pairs) and by
//! `aleph instance create --encrypt-rootfs` (the locally LUKS-wrapped
//! rootfs), both of which need to push a local file to the network as a
//! STORE message and get back the item hash other message fields reference.

use std::path::Path;

use aleph_sdk::client::{AlephClient, hash_file};
use aleph_sdk::messages::StoreBuilder;
use aleph_sdk::verify::Hasher;
use aleph_types::chain::Address;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::StorageEngine;
use aleph_types::message::execution::base::Payment;
use anyhow::{Context, Result};

use super::file::select_default_engine;
use crate::account::CliAccount;
use crate::cli::StorageEngineCli;
use crate::common::render_upload_progress;

/// Upload one file as a STORE message (default payment) and return the STORE
/// message item hash - which is what
/// `VerifiedWorkload`/`VerifiedVolume`'s `ref` and `hash_tree` fields carry.
///
/// Under `dry_run`, the network upload is skipped entirely: the file's own
/// content hash is returned as a stand-in for the STORE message hash, since
/// no STORE message is ever built or sent in that mode.
pub(crate) async fn upload_file(
    client: &AlephClient,
    account: &CliAccount,
    owner: Option<&Address>,
    path: &Path,
    json: bool,
    dry_run: bool,
    engine: Option<StorageEngineCli>,
) -> Result<ItemHash> {
    let engine = resolve_engine(path, engine).await?;
    if !json {
        // The engine decides where the artifact is served from at launch, so
        // it is reported rather than silently chosen.
        let name = match engine {
            StorageEngine::Storage => "native storage",
            StorageEngine::Ipfs => "ipfs",
        };
        eprintln!("Hashing {} ({name})...", path.display());
    }
    let file_hash = match engine {
        StorageEngine::Storage => hash_file(path, Hasher::for_storage()).await?,
        StorageEngine::Ipfs => hash_file(path, Hasher::for_ipfs()).await?,
    };
    if !json {
        eprintln!("  File hash: {file_hash}");
    }

    if dry_run {
        return Ok(file_hash);
    }

    // V-Programs are credit-only; without an explicit payment type the store
    // defaults to hold on the CCN, which rejects token-less wallets with 402.
    let mut builder = StoreBuilder::new(account, file_hash, engine).payment(Payment::credits());
    if let Some(owner) = owner {
        builder = builder.on_behalf_of(owner.clone());
    }
    let pending = builder.build()?;

    if !json {
        eprintln!("Uploading {}...", path.display());
    }
    let on_tick: fn(u64, u64) = if json {
        |_, _| {}
    } else {
        render_upload_progress
    };
    let upload = match engine {
        StorageEngine::Storage => {
            client
                .upload_file_to_storage_with_progress(path, Some(&pending), true, on_tick)
                .await
        }
        StorageEngine::Ipfs => {
            client
                .upload_file_to_ipfs_with_progress(path, Some(&pending), true, on_tick)
                .await
        }
    };
    if !json {
        eprintln!();
    }
    upload?;

    Ok(pending.item_hash)
}

/// The engine one artifact is uploaded on: `--storage-engine` when given,
/// otherwise size-selected exactly as `aleph file upload` does, since native
/// storage rejects anything past its limit.
async fn resolve_engine(path: &Path, override_: Option<StorageEngineCli>) -> Result<StorageEngine> {
    match override_ {
        Some(StorageEngineCli::Storage) => Ok(StorageEngine::Storage),
        Some(StorageEngineCli::Ipfs) => Ok(StorageEngine::Ipfs),
        None => {
            let meta = tokio::fs::metadata(path)
                .await
                .with_context(|| format!("stat {}", path.display()))?;
            Ok(select_default_engine(meta.len()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_volume_over_the_native_limit_goes_to_ipfs() {
        let dir = tempfile::tempdir().unwrap();
        let big = dir.path().join("weights.ext4");
        let file = std::fs::File::create(&big).unwrap();
        // Sparse: 100 MiB + 1 byte, the first size native storage rejects.
        file.set_len(100 * 1024 * 1024 + 1).unwrap();

        assert_eq!(
            resolve_engine(&big, None).await.unwrap(),
            StorageEngine::Ipfs
        );
    }

    #[tokio::test]
    async fn a_volume_at_the_native_limit_stays_on_native_storage() {
        let dir = tempfile::tempdir().unwrap();
        let at_limit = dir.path().join("weights.ext4");
        let file = std::fs::File::create(&at_limit).unwrap();
        file.set_len(100 * 1024 * 1024).unwrap();

        assert_eq!(
            resolve_engine(&at_limit, None).await.unwrap(),
            StorageEngine::Storage
        );
    }

    #[tokio::test]
    async fn an_explicit_engine_wins_over_the_size() {
        let dir = tempfile::tempdir().unwrap();
        let small = dir.path().join("small.ext4");
        std::fs::write(&small, b"tiny").unwrap();

        assert_eq!(
            resolve_engine(&small, Some(StorageEngineCli::Ipfs))
                .await
                .unwrap(),
            StorageEngine::Ipfs
        );
        // Forced never stats, so it answers for a path that does not exist.
        assert_eq!(
            resolve_engine(Path::new("/nonexistent"), Some(StorageEngineCli::Storage))
                .await
                .unwrap(),
            StorageEngine::Storage
        );
    }
}
