//! IPFS gateway helpers for directory uploads.
//!
//! Targets kubo's HTTP API (`/api/v0/add`). The default endpoint is currently
//! the public `ipfs.aleph.cloud` kubo, matching the production frontend. This
//! is **temporary** until pyaleph exposes an authenticated directory-ingest
//! endpoint; the gateway URL is overridable via `AlephClient::with_ipfs_gateway`.
//!
//! The filesystem-walking and CID types (`FolderEntry`, `UploadFolderOptions`,
//! `collect_folder_files`, ...) live in the `aleph-cid` crate and are
//! re-exported here at their historical paths.

pub use aleph_cid::{
    CidVersion, CollectError, FolderEntry, UploadFolderOptions, collect_folder_files,
};

/// Default kubo host used by the SDK when no override is configured. The SDK
/// appends `/api/v0/...` paths internally; only the scheme + host (+ optional
/// port) belong here.
///
/// Temporary: public unauthenticated endpoint. Replace with pyaleph-side
/// ingestion once that lands.
pub const DEFAULT_IPFS_GATEWAY: &str = "https://ipfs.aleph.cloud";

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

    let entry: AddEntry = serde_json::from_str(last).map_err(|source| {
        let preview: String = last.chars().take(120).collect();
        ParseRootError::Malformed { preview, source }
    })?;

    Cid::try_from(entry.hash.as_str()).map_err(|source| ParseRootError::InvalidCid {
        hash: entry.hash.clone(),
        source,
    })
}

/// Errors parsing a kubo `/api/v0/add` NDJSON response.
#[derive(Debug, thiserror::Error)]
pub enum ParseRootError {
    /// The response body had no non-empty lines.
    #[error("empty NDJSON response")]
    Empty,
    /// A line could not be deserialized as a kubo add-entry.
    #[error("malformed NDJSON line: {source} (preview: {preview})")]
    Malformed {
        preview: String,
        #[source]
        source: serde_json::Error,
    },
    /// The `Hash` field was not a valid CID.
    #[error("invalid CID '{hash}' in response: {source}")]
    InvalidCid {
        hash: String,
        #[source]
        source: aleph_types::cid::CidError,
    },
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
        assert!(matches!(err, ParseRootError::Malformed { .. }));
    }

    #[test]
    fn parse_ndjson_root_invalid_cid_errors() {
        let body = r#"{"Name":"x","Hash":"not-a-cid","Size":"0"}"#;
        let err = parse_ndjson_root(body).unwrap_err();
        assert!(matches!(err, ParseRootError::InvalidCid { .. }));
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
        let mut opts = UploadFolderOptions::default();
        opts.cid_version = CidVersion::V0;
        let q = build_add_query(&opts);
        assert!(q.contains("cid-version=0"));
        assert!(!q.contains("raw-leaves"));
    }
}
