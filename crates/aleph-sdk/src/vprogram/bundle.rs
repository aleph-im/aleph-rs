//! V-Program runtime bundle download, verification, and extraction.
//!
//! A runtime bundle is a `.tar.gz` referenced by a [`RuntimeManifest`]'s
//! `bundle` field. This module fetches it from aleph storage (with a local
//! disk cache keyed by the manifest's declared sha256), verifies its hash and
//! size, and extracts the OVMF firmware, kernel, and initrd members that the
//! V-Program launch pipeline needs.

use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::client::{AlephClient, AlephStorageClient};
use crate::vprogram::manifest::{BundleMembers, RuntimeManifest};

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("downloading bundle: {0}")]
    Download(#[from] crate::client::MessageError),
    #[error("bundle sha256 mismatch: manifest says {expected}, downloaded {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("bundle size mismatch: manifest says {expected} bytes, downloaded {actual}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("bundle member has unsafe path {0:?}")]
    UnsafeMemberPath(String),
    #[error("bundle is missing declared member for role {0}")]
    MissingMember(&'static str),
    #[error("bundle.sha256 is not a valid storage hash: {0}")]
    BadBundleHash(String),
    #[error("reading/writing bundle cache: {0}")]
    Io(#[from] std::io::Error),
}

/// Local filesystem paths of the runtime artifacts extracted from a bundle.
#[derive(Debug, Clone)]
pub struct BundleArtifacts {
    pub ovmf: PathBuf,
    pub kernel: PathBuf,
    pub initrd: PathBuf,
}

/// Fetch, verify, and extract the runtime bundle referenced by `manifest`.
///
/// Caches extracted artifacts under `cache_dir/<bundle.sha256>/`. If `ovmf`,
/// `kernel`, and `initrd` already exist there, returns immediately without
/// touching the network.
pub async fn fetch_bundle_artifacts(
    client: &AlephClient,
    manifest: &RuntimeManifest,
    cache_dir: &Path,
) -> Result<BundleArtifacts, BundleError> {
    let cache_key = bundle_cache_key(&manifest.bundle.sha256)?;
    let bundle_dir = cache_dir.join(cache_key);
    let artifacts = BundleArtifacts {
        ovmf: bundle_dir.join("ovmf"),
        kernel: bundle_dir.join("kernel"),
        initrd: bundle_dir.join("initrd"),
    };

    if artifacts.ovmf.exists() && artifacts.kernel.exists() && artifacts.initrd.exists() {
        return Ok(artifacts);
    }

    let hash =
        manifest
            .bundle
            .sha256
            .parse()
            .map_err(|e: aleph_types::item_hash::ItemHashError| {
                BundleError::BadBundleHash(e.to_string())
            })?;

    // Deliberately no `.with_verification()`: `verify_bundle_bytes` below
    // checks both size and sha256 against the manifest, which is strictly
    // stronger than the download-layer hash-only check and avoids hashing
    // the payload twice.
    let download = client.download_file_by_hash(&hash).await?;
    let bytes = download.bytes().await?;

    verify_bundle_bytes(&bytes, &manifest.bundle.sha256, manifest.bundle.size)?;

    fs::create_dir_all(&bundle_dir)?;
    extract_members(&bytes, &manifest.bundle.members, &bundle_dir)?;

    Ok(artifacts)
}

/// Validates `sha256` as a well-formed 64-character lowercase hex digest
/// before it is ever used as a filesystem path segment, and returns it
/// unchanged as the cache directory key.
///
/// `manifest.bundle.sha256` is untrusted input (it comes from a downloaded
/// manifest). Without this check a crafted value such as `"../../etc"` (or
/// an absolute path) would be joined directly onto `cache_dir`, escaping it;
/// the cache-hit fast path in `fetch_bundle_artifacts` would then treat
/// attacker-placed files at that location as "verified" bundle artifacts
/// without ever hashing anything.
///
/// This deliberately does *not* reuse `ItemHash`'s `FromStr`/`TryFrom<&str>`
/// for the safety check: `Cid::try_from` (the IPFS-CID arm `ItemHash`
/// falls back to for anything that isn't a 64-hex native hash) only checks
/// the first character and a minimum length, not the actual base32/58/64
/// charset, so a string like `"b" + "../".repeat(13)` (40 chars, starts with
/// `b`) parses as a "valid" CID while still containing `..` components. A
/// field literally named `sha256` should be a raw digest, not a CID, so
/// enforcing the strict hex format here is both the correct semantics and
/// the only check immune to that parser's leniency.
fn bundle_cache_key(sha256: &str) -> Result<&str, BundleError> {
    let is_hex64 = sha256.len() == 64
        && sha256
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    if !is_hex64 {
        return Err(BundleError::BadBundleHash(format!(
            "expected 64 lowercase hex characters, got {sha256:?}"
        )));
    }
    Ok(sha256)
}

/// Verify that `bytes` matches the manifest-declared sha256 digest and size.
///
/// Size is checked first (cheap), then the digest, so a truncated download
/// fails fast without hashing the whole buffer.
fn verify_bundle_bytes(
    bytes: &[u8],
    expected_sha256: &str,
    expected_size: u64,
) -> Result<(), BundleError> {
    let actual_size = bytes.len() as u64;
    if actual_size != expected_size {
        return Err(BundleError::SizeMismatch {
            expected: expected_size,
            actual: actual_size,
        });
    }

    let actual = hex::encode(Sha256::digest(bytes));
    if actual != expected_sha256 {
        return Err(BundleError::ChecksumMismatch {
            expected: expected_sha256.to_string(),
            actual,
        });
    }

    Ok(())
}

/// Extract the `ovmf`, `kernel`, and `initrd` members declared in `members`
/// from the gzipped tar archive `bytes` into `dir`, named after their roles.
///
/// Every entry's path is checked for traversal (absolute paths or `..`
/// components) before it is compared against the declared member paths;
/// an unsafe path fails the whole extraction even if that entry is not one
/// of the members we care about. Each matched entry is written to
/// `<role>.part` and atomically renamed into place. A role whose member
/// path never shows up in the archive is reported as `MissingMember`.
fn extract_members(bytes: &[u8], members: &BundleMembers, dir: &Path) -> Result<(), BundleError> {
    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);

    let roles: [(&str, &'static str); 3] = [
        (members.ovmf.as_str(), "ovmf"),
        (members.kernel.as_str(), "kernel"),
        (members.initrd.as_str(), "initrd"),
    ];
    let mut found = [false; 3];

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if !is_safe_relative_path(&path) {
            return Err(BundleError::UnsafeMemberPath(
                path.to_string_lossy().into_owned(),
            ));
        }
        // Only regular file entries can be bundle members: a symlink or
        // directory entry at a member path would otherwise be io::copy'd
        // into an empty artifact instead of surfacing as MissingMember.
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path_str = path.to_string_lossy().into_owned();

        for (i, (member_path, role)) in roles.iter().enumerate() {
            if path_str == *member_path {
                let target = dir.join(role);
                let part = dir.join(format!("{role}.part"));
                {
                    let mut file = fs::File::create(&part)?;
                    std::io::copy(&mut entry, &mut file)?;
                }
                fs::rename(&part, &target)?;
                found[i] = true;
                break;
            }
        }
    }

    for (i, (_, role)) in roles.iter().enumerate() {
        if !found[i] {
            return Err(BundleError::MissingMember(role));
        }
    }

    Ok(())
}

/// A relative path with no `..` components. Rejects absolute paths and
/// parent-directory traversal so archive members can't be written outside
/// the target directory.
fn is_safe_relative_path(path: &Path) -> bool {
    if path.is_absolute() {
        return false;
    }
    !path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
}

#[cfg(test)]
mod test {
    use super::*;

    /// Builds a tiny in-memory tar.gz. Sets each entry's name directly on the
    /// raw header bytes (rather than via `Header::set_path`/`append_data`,
    /// which reject `..` components) so tests can construct archives with
    /// path-traversal entries to exercise `extract_members`'s rejection.
    fn make_test_bundle(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut builder = tar::Builder::new(enc);
        for (path, content) in entries {
            let mut header = tar::Header::new_gnu();
            let name_bytes = path.as_bytes();
            assert!(name_bytes.len() < 100, "test entry name too long: {path}");
            header.as_old_mut().name[..name_bytes.len()].copy_from_slice(name_bytes);
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            header.set_cksum();
            builder.append(&header, *content).unwrap();
        }
        let enc = builder.into_inner().unwrap();
        enc.finish().unwrap()
    }

    fn test_members() -> BundleMembers {
        BundleMembers {
            ovmf: "image/OVMF.fd".to_string(),
            kernel: "image/bzImage".to_string(),
            initrd: "image/initrd".to_string(),
            platform_rootfs: "image/rootfs.ext4".to_string(),
            platform_hash_tree: "image/rootfs.ext4.verity".to_string(),
        }
    }

    fn make_symlink_bundle(path: &str, target: &str) -> Vec<u8> {
        let enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut builder = tar::Builder::new(enc);
        let mut header = tar::Header::new_gnu();
        let name_bytes = path.as_bytes();
        assert!(name_bytes.len() < 100, "test entry name too long: {path}");
        header.as_old_mut().name[..name_bytes.len()].copy_from_slice(name_bytes);
        header.set_size(0);
        header.set_mode(0o777);
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_link_name(target).unwrap();
        header.set_cksum();
        builder.append(&header, &b""[..]).unwrap();
        let enc = builder.into_inner().unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn cache_key_rejects_path_traversal() {
        assert!(matches!(
            bundle_cache_key("../escape"),
            Err(BundleError::BadBundleHash(_))
        ));
    }

    #[test]
    fn cache_key_rejects_absolute_path() {
        assert!(matches!(
            bundle_cache_key("/etc/passwd"),
            Err(BundleError::BadBundleHash(_))
        ));
    }

    #[test]
    fn cache_key_rejects_cid_shaped_traversal() {
        // `Cid::try_from` (which `ItemHash`'s `FromStr` falls back to for
        // non-hex strings) only checks the first character and a minimum
        // length, not the actual base32/58/64 charset. A 40+ char string
        // starting with a multibase prefix like `b` would pass that check
        // even though it's still full of `..` components; bundle_cache_key
        // must not rely on ItemHash parsing alone.
        let sneaky = format!("b{}", "../".repeat(13));
        assert_eq!(sneaky.len(), 40);
        assert!(matches!(
            bundle_cache_key(&sneaky),
            Err(BundleError::BadBundleHash(_))
        ));
    }

    #[test]
    fn cache_key_accepts_valid_hash() {
        let good = "0".repeat(64);
        assert_eq!(bundle_cache_key(&good).unwrap(), good);
    }

    #[test]
    fn verify_rejects_wrong_hash_and_size() {
        let bytes = b"hello".to_vec();
        let good = hex::encode(Sha256::digest(&bytes));
        verify_bundle_bytes(&bytes, &good, 5).unwrap();
        assert!(matches!(
            verify_bundle_bytes(&bytes, &good, 4),
            Err(BundleError::SizeMismatch { .. })
        ));
        let bad = "0".repeat(64);
        assert!(matches!(
            verify_bundle_bytes(&bytes, &bad, 5),
            Err(BundleError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn extract_pulls_named_members() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = make_test_bundle(&[
            ("image/OVMF.fd", b"o"),
            ("image/bzImage", b"k"),
            ("image/initrd", b"i"),
        ]);
        extract_members(&bytes, &test_members(), dir.path()).unwrap();
        assert_eq!(std::fs::read(dir.path().join("ovmf")).unwrap(), b"o");
        assert_eq!(std::fs::read(dir.path().join("kernel")).unwrap(), b"k");
        assert_eq!(std::fs::read(dir.path().join("initrd")).unwrap(), b"i");
    }

    #[test]
    fn extract_rejects_traversal_and_missing_members() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = make_test_bundle(&[("../evil", b"x")]);
        assert!(matches!(
            extract_members(&bytes, &test_members(), dir.path()).unwrap_err(),
            BundleError::UnsafeMemberPath(_)
        ));
        let bytes = make_test_bundle(&[("image/OVMF.fd", b"o")]);
        assert!(matches!(
            extract_members(&bytes, &test_members(), dir.path()).unwrap_err(),
            BundleError::MissingMember(_)
        ));
    }

    #[test]
    fn extract_skips_non_file_entries() {
        // a symlink entry at a member path must not satisfy the member: it
        // would yield an empty artifact instead of the declared content
        let dir = tempfile::tempdir().unwrap();
        let bytes = make_symlink_bundle("image/OVMF.fd", "/etc/passwd");
        assert!(matches!(
            extract_members(&bytes, &test_members(), dir.path()).unwrap_err(),
            BundleError::MissingMember("ovmf")
        ));
        assert!(!dir.path().join("ovmf").exists());
    }
}
