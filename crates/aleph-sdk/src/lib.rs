pub mod aggregate_models;
pub mod authorization;
pub mod builder;
pub mod car;
pub mod client;
pub mod confidential;
pub mod corechannel;
#[cfg(feature = "credits")]
pub mod credit;
#[cfg(feature = "swap")]
pub mod swap;
pub mod credit_transfer;
pub mod crn;
pub mod crns_list;
pub mod folder_hash;
pub mod ipfs;
pub mod messages;
pub mod progress;
mod proto;
pub mod scheduler;
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

/// Test-only re-export of `car::write_carv1_header`.
///
/// `car` module functions are `pub(crate)`; this hidden function lets
/// integration tests in `tests/car_roundtrip.rs` write CARv1 headers.
#[cfg(feature = "test-helpers")]
#[doc(hidden)]
pub fn __test_only_write_carv1_header<W: std::io::Write>(
    w: &mut W,
    root_cid_bytes: &[u8],
) -> std::io::Result<()> {
    crate::car::write_carv1_header(w, root_cid_bytes)
}

/// Test-only re-export of `car::write_block_frame`.
///
/// `car` module functions are `pub(crate)`; this hidden function lets
/// integration tests in `tests/car_roundtrip.rs` write block frames.
#[cfg(feature = "test-helpers")]
#[doc(hidden)]
pub fn __test_only_write_block_frame<W: std::io::Write>(
    w: &mut W,
    cid_bytes: &[u8],
    block_bytes: &[u8],
) -> std::io::Result<()> {
    crate::car::write_block_frame(w, cid_bytes, block_bytes)
}
