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

use crate::account::CliAccount;
use crate::common::render_upload_progress;

/// Upload one file as a STORE message and return the STORE message item
/// hash - which is what `VerifiedWorkload`/`VerifiedVolume`'s `ref` and
/// `hash_tree` fields carry. The storage engine is size-selected the same
/// way `aleph file upload` does it: native storage is rejected by the CCN
/// above its size limit, so larger files (e.g. an encrypted rootfs) go to
/// IPFS instead.
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
) -> Result<ItemHash> {
    let size = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    let engine = super::file::select_default_engine(size);

    if !json {
        eprintln!("Hashing {}...", path.display());
    }
    let hasher = match engine {
        StorageEngine::Storage => Hasher::for_storage(),
        StorageEngine::Ipfs => Hasher::for_ipfs(),
    };
    let file_hash = hash_file(path, hasher).await?;
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
