//! kubo-compatible IPFS CID computation for Aleph Cloud.
//!
//! This crate is the single source of truth for client-side content
//! addressing in the Aleph Rust workspace:
//!
//! - [`verify`] — streaming hashers ([`verify::Hasher`],
//!   [`verify::HashVerifier`]) for Aleph native SHA-256 item hashes and IPFS
//!   CIDv0/CIDv1 (UnixFS dag-pb, 256 KiB chunks, raw leaves), plus
//!   [`verify::compute_cid`] for one-shot CIDv0 computation.
//! - [`folder_hash`] — UnixFS directory DAG construction matching
//!   `ipfs add -r` (plain directories and HAMT shards), with a block sink for
//!   streaming the DAG into a CAR file.
//! - [`car`] — CARv1 framing: header/block writers and a strict root reader.
//!
//! It deliberately contains no networking, signing, or async code so that it
//! can be reused as-is from FFI bindings (e.g. a Python wheel). Golden CIDs in
//! `tests/folder_hash.rs` are regenerated against real kubo via
//! `tests/regen-folder-hash-goldens.sh`.

pub mod car;
pub mod folder_hash;
mod proto;
pub mod verify;

use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CidVersion {
    V0,
    #[default]
    V1,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct UploadFolderOptions {
    pub cid_version: CidVersion,
    pub pin: bool,
    pub follow_symlinks: bool,
}

impl Default for UploadFolderOptions {
    fn default() -> Self {
        Self {
            cid_version: CidVersion::V1,
            pin: true,
            follow_symlinks: true,
        }
    }
}

#[non_exhaustive]
#[derive(Debug)]
pub struct FolderEntry {
    /// Relative path from the upload root, forward-slash separated.
    pub relative_path: String,
    pub absolute_path: PathBuf,
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum CollectError {
    #[error("empty folder: {0}")]
    Empty(PathBuf),
    #[error("non-UTF-8 path: {0}")]
    NonUtf8(PathBuf),
    #[error("walk failed at {path}: {source}")]
    Walk {
        path: PathBuf,
        #[source]
        source: walkdir::Error,
    },
}

/// Walks `root` and returns one entry per regular file, with the relative
/// path normalized to forward-slash separators.
///
/// Symlinks are followed when `follow_symlinks` is true (matches kubo's
/// `ipfs add -r` default). Walk errors abort the collection.
pub fn collect_folder_files(
    root: &Path,
    follow_symlinks: bool,
) -> Result<Vec<FolderEntry>, CollectError> {
    let mut out = Vec::new();
    let walker = walkdir::WalkDir::new(root)
        .follow_links(follow_symlinks)
        .min_depth(1);

    for entry in walker {
        let entry = entry.map_err(|e| {
            let path = e
                .path()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| root.to_path_buf());
            CollectError::Walk { path, source: e }
        })?;
        if !entry.file_type().is_file() {
            continue;
        }
        let abs = entry.path().to_path_buf();
        let rel = entry
            .path()
            .strip_prefix(root)
            .expect("walkdir entries are descendants of root");
        let rel_str = rel
            .to_str()
            .ok_or_else(|| CollectError::NonUtf8(abs.clone()))?
            .replace(std::path::MAIN_SEPARATOR, "/");
        out.push(FolderEntry {
            relative_path: rel_str,
            absolute_path: abs,
        });
    }

    if out.is_empty() {
        return Err(CollectError::Empty(root.to_path_buf()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn default_options_are_v1_pinned_follow_symlinks() {
        let opts = UploadFolderOptions::default();
        assert_eq!(opts.cid_version, CidVersion::V1);
        assert!(opts.pin);
        assert!(opts.follow_symlinks);
    }

    fn make_tree(tmp: &TempDir, files: &[(&str, &str)]) {
        for (rel, content) in files {
            let abs = tmp.path().join(rel);
            if let Some(parent) = abs.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&abs, content).unwrap();
        }
    }

    #[test]
    fn collect_files_flat_directory() {
        let tmp = TempDir::new().unwrap();
        make_tree(&tmp, &[("a.txt", "a"), ("b.txt", "b")]);
        let mut entries = collect_folder_files(tmp.path(), true).unwrap();
        entries.sort_by(|x, y| x.relative_path.cmp(&y.relative_path));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].relative_path, "a.txt");
        assert_eq!(entries[1].relative_path, "b.txt");
    }

    #[test]
    fn collect_files_nested_directory() {
        let tmp = TempDir::new().unwrap();
        make_tree(
            &tmp,
            &[
                ("a.txt", "a"),
                ("sub/b.txt", "b"),
                ("sub/deeper/c.txt", "c"),
            ],
        );
        let mut paths: Vec<String> = collect_folder_files(tmp.path(), true)
            .unwrap()
            .into_iter()
            .map(|e| e.relative_path)
            .collect();
        paths.sort();
        assert_eq!(paths, vec!["a.txt", "sub/b.txt", "sub/deeper/c.txt"]);
    }

    #[test]
    fn collect_files_empty_directory_errors() {
        let tmp = TempDir::new().unwrap();
        let err = collect_folder_files(tmp.path(), true).unwrap_err();
        assert!(matches!(err, CollectError::Empty(_)));
    }

    #[test]
    fn collect_files_uses_forward_slashes() {
        let tmp = TempDir::new().unwrap();
        make_tree(&tmp, &[("sub/x.txt", "x")]);
        let entries = collect_folder_files(tmp.path(), true).unwrap();
        assert_eq!(entries[0].relative_path, "sub/x.txt");
        assert!(!entries[0].relative_path.contains('\\'));
    }
}
