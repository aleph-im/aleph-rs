//! IPFS gateway helpers for directory uploads.
//!
//! Targets kubo's HTTP API (`/api/v0/add`). The default endpoint is currently
//! the public `ipfs.aleph.cloud` kubo, matching the production frontend. This
//! is **temporary** until pyaleph exposes an authenticated directory-ingest
//! endpoint; the gateway URL is overridable via `AlephClient::with_ipfs_gateway`.

/// Default kubo host used by the SDK when no override is configured. The SDK
/// appends `/api/v0/...` paths internally; only the scheme + host (+ optional
/// port) belong here.
///
/// Temporary: public unauthenticated endpoint. Replace with pyaleph-side
/// ingestion once that lands.
pub const DEFAULT_IPFS_GATEWAY: &str = "https://ipfs.aleph.cloud";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CidVersion {
    V0,
    #[default]
    V1,
}

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

use aleph_types::cid::Cid;

/// Parses kubo's NDJSON `/api/v0/add` response and returns the root CID.
///
/// With `wrap-with-directory=true`, kubo emits the wrapping directory as the
/// final entry, so the last non-empty line's `Hash` field is the root.
pub(crate) fn parse_ndjson_root(body: &str) -> Result<Cid, ParseRootError> {
    #[derive(serde::Deserialize)]
    struct AddEntry {
        #[serde(rename = "Hash")]
        hash: String,
    }

    let last = body
        .lines()
        .map(str::trim)
        .rfind(|l| !l.is_empty())
        .ok_or(ParseRootError::Empty)?;

    let entry: AddEntry = serde_json::from_str(last).map_err(|e| {
        let preview: String = last.chars().take(120).collect();
        ParseRootError::Malformed(format!("{e}: {preview}"))
    })?;

    Cid::try_from(entry.hash.as_str())
        .map_err(|e| ParseRootError::InvalidCid(format!("{}: {e}", entry.hash)))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParseRootError {
    #[error("empty NDJSON response")]
    Empty,
    #[error("malformed NDJSON line: {0}")]
    Malformed(String),
    #[error("invalid CID in response: {0}")]
    InvalidCid(String),
}

use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct FolderEntry {
    /// Relative path from the upload root, forward-slash separated.
    pub relative_path: String,
    pub absolute_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CollectError {
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
pub(crate) fn collect_folder_files(
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

/// Builds the query string for `POST /api/v0/add`.
pub(crate) fn build_add_query(opts: &UploadFolderOptions) -> String {
    let cid_version = match opts.cid_version {
        CidVersion::V0 => "0",
        CidVersion::V1 => "1",
    };
    let pin = if opts.pin { "true" } else { "false" };
    let mut q = format!("wrap-with-directory=true&pin={pin}&cid-version={cid_version}");
    if matches!(opts.cid_version, CidVersion::V1) {
        q.push_str("&raw-leaves=true");
    }
    q
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_are_v1_pinned_follow_symlinks() {
        let opts = UploadFolderOptions::default();
        assert_eq!(opts.cid_version, CidVersion::V1);
        assert!(opts.pin);
        assert!(opts.follow_symlinks);
    }

    #[test]
    fn default_gateway_is_aleph_cloud() {
        assert_eq!(DEFAULT_IPFS_GATEWAY, "https://ipfs.aleph.cloud");
    }

    #[test]
    fn parse_ndjson_root_single_entry() {
        let body = r#"{"Name":"hello.txt","Hash":"QmTudJSaoKxtbEnTddJ9vh8hbN84ZLVvD5pNpUaSbxwGoa","Size":"12"}"#;
        let cid = parse_ndjson_root(body).unwrap();
        assert_eq!(
            cid.as_str(),
            "QmTudJSaoKxtbEnTddJ9vh8hbN84ZLVvD5pNpUaSbxwGoa"
        );
    }

    #[test]
    fn parse_ndjson_root_takes_last_entry_wrap_dir() {
        let body = "\
{\"Name\":\"hello.txt\",\"Hash\":\"QmFile1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"12\"}
{\"Name\":\"world.txt\",\"Hash\":\"QmFile2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"12\"}
{\"Name\":\"\",\"Hash\":\"QmRoot1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"Size\":\"100\"}
";
        let cid = parse_ndjson_root(body).unwrap();
        assert_eq!(
            cid.as_str(),
            "QmRoot1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn parse_ndjson_root_skips_blank_lines() {
        let body = "\n\n{\"Name\":\"\",\"Hash\":\"QmTudJSaoKxtbEnTddJ9vh8hbN84ZLVvD5pNpUaSbxwGoa\",\"Size\":\"12\"}\n\n";
        let cid = parse_ndjson_root(body).unwrap();
        assert_eq!(
            cid.as_str(),
            "QmTudJSaoKxtbEnTddJ9vh8hbN84ZLVvD5pNpUaSbxwGoa"
        );
    }

    #[test]
    fn parse_ndjson_root_empty_errors() {
        let err = parse_ndjson_root("").unwrap_err();
        assert!(matches!(err, ParseRootError::Empty));
    }

    #[test]
    fn parse_ndjson_root_only_blanks_errors() {
        let err = parse_ndjson_root("\n\n   \n").unwrap_err();
        assert!(matches!(err, ParseRootError::Empty));
    }

    #[test]
    fn parse_ndjson_root_malformed_errors() {
        let err = parse_ndjson_root("not json").unwrap_err();
        assert!(matches!(err, ParseRootError::Malformed(_)));
    }

    #[test]
    fn parse_ndjson_root_invalid_cid_errors() {
        let body = r#"{"Name":"x","Hash":"not-a-cid","Size":"0"}"#;
        let err = parse_ndjson_root(body).unwrap_err();
        assert!(matches!(err, ParseRootError::InvalidCid(_)));
    }

    use std::fs;
    use tempfile::TempDir;

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

    #[test]
    fn build_add_query_v1_includes_raw_leaves() {
        let q = build_add_query(&UploadFolderOptions::default());
        assert!(q.contains("wrap-with-directory=true"));
        assert!(q.contains("cid-version=1"));
        assert!(q.contains("raw-leaves=true"));
        assert!(q.contains("pin=true"));
    }

    #[test]
    fn build_add_query_v0_omits_raw_leaves() {
        let opts = UploadFolderOptions {
            cid_version: CidVersion::V0,
            ..Default::default()
        };
        let q = build_add_query(&opts);
        assert!(q.contains("cid-version=0"));
        assert!(!q.contains("raw-leaves"));
    }
}
