pub mod aggregate_models;
pub mod authorization;
pub mod builder;
pub mod car;
pub mod client;
pub mod corechannel;
#[cfg(feature = "credits")]
pub mod credit;
pub mod credit_transfer;
pub mod crn;
pub mod crns_list;
pub mod folder_hash;
pub mod ipfs;
pub mod messages;
mod proto;
pub mod verify;
pub mod ws;

/// Test-only re-export of `folder_hash::hash_folder_root`.
///
/// `folder_hash` is `pub(crate)`; this hidden function lets integration tests
/// in `tests/folder_hash.rs` exercise the hasher without exposing the module.
#[cfg(feature = "test-helpers")]
#[doc(hidden)]
pub fn __test_only_hash_folder_root(
    entries: &[crate::ipfs::FolderEntry],
    opts: &crate::ipfs::UploadFolderOptions,
) -> Result<aleph_types::item_hash::ItemHash, String> {
    crate::folder_hash::hash_folder_root(entries, opts).map_err(|e| e.to_string())
}
