//! V-Program runtime bundle download, verification, and extraction.
//!
//! A runtime bundle is a `.tar.gz` referenced by a [`RuntimeManifest`]'s
//! `bundle` field. This module fetches it by `bundle.ref`, the STORE message
//! pinning the tarball (native storage or IPFS), with a local disk cache keyed
//! by the manifest's declared sha256, verifies its hash and size, and
//! extracts the OVMF firmware, kernel, initrd, platform rootfs and its hash
//! tree.

use std::fs;
use std::path::{Path, PathBuf};

use futures_util::{Stream, StreamExt};
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
    #[error("bundle download exceeded the manifest-declared size of {expected} bytes; aborted")]
    TooLarge { expected: u64 },
    #[error("bundle member has unsafe path {0:?}")]
    UnsafeMemberPath(String),
    #[error(
        "bundle member for role {role} declares {size} bytes, above the {limit}-byte per-member limit"
    )]
    MemberTooLarge {
        role: &'static str,
        size: u64,
        limit: u64,
    },
    #[error("bundle is missing declared member for role {0}")]
    MissingMember(&'static str),
    #[error("bundle.sha256 is not a valid storage hash: {0}")]
    BadBundleHash(String),
    #[error("bundle.ref is not a valid message hash: {0}")]
    BadBundleRef(String),
    #[error("reading/writing bundle cache: {0}")]
    Io(#[from] std::io::Error),
}

/// Local filesystem paths of the runtime artifacts extracted from a bundle.
///
/// `ovmf`, `kernel`, `initrd` feed the launch measurement; `platform_rootfs`
/// and `platform_hash_tree` are the guest's vda/vdb and are only needed to
/// boot the runtime locally (`aleph vprogram run`). All five are extracted
/// together: the tarball is fully downloaded whenever any member is
/// missing, so a partial layout would save disk, not network.
#[derive(Debug, Clone)]
pub struct BundleArtifacts {
    pub ovmf: PathBuf,
    pub kernel: PathBuf,
    pub initrd: PathBuf,
    pub platform_rootfs: PathBuf,
    pub platform_hash_tree: PathBuf,
}

/// Cache file name per bundle role, in extraction order.
const ROLES: [&str; 5] = [
    "ovmf",
    "kernel",
    "initrd",
    "platform_rootfs",
    "platform_hash_tree",
];

impl BundleArtifacts {
    /// The five member paths under `dir`, named after their roles.
    pub fn in_dir(dir: &Path) -> Self {
        Self {
            ovmf: dir.join("ovmf"),
            kernel: dir.join("kernel"),
            initrd: dir.join("initrd"),
            platform_rootfs: dir.join("platform_rootfs"),
            platform_hash_tree: dir.join("platform_hash_tree"),
        }
    }

    /// True when every member file exists (a cache hit). A directory written
    /// by an older CLI holds only the first three and is not a hit.
    pub fn all_present(&self) -> bool {
        [
            &self.ovmf,
            &self.kernel,
            &self.initrd,
            &self.platform_rootfs,
            &self.platform_hash_tree,
        ]
        .iter()
        .all(|p| p.exists())
    }
}

/// Fetch, verify, and extract the runtime bundle referenced by `manifest`.
///
/// Caches extracted artifacts under `cache_dir/<bundle.sha256>/`. If all five
/// members already exist there, returns immediately without touching the
/// network.
pub async fn fetch_bundle_artifacts(
    client: &AlephClient,
    manifest: &RuntimeManifest,
    cache_dir: &Path,
) -> Result<BundleArtifacts, BundleError> {
    let cache_key = bundle_cache_key(&manifest.bundle.sha256)?;
    let bundle_dir = cache_dir.join(cache_key);
    let artifacts = BundleArtifacts::in_dir(&bundle_dir);
    if artifacts.all_present() {
        return Ok(artifacts);
    }

    // Download by `bundle.ref`, the STORE message pinning the tarball, not
    // by `bundle.sha256`: the sha256 is only a valid storage path for
    // native-storage uploads, and a bundle above the network's native size
    // limit lives on IPFS under a CID (the mainnet aleph.compose/1 runtime,
    // 297 MB). Resolving the message gives whichever file hash the node
    // serves it under, the same way the CRN fetches it.
    let bundle_ref =
        manifest
            .bundle
            .reference
            .parse()
            .map_err(|e: aleph_types::item_hash::ItemHashError| {
                BundleError::BadBundleRef(e.to_string())
            })?;

    // Deliberately no `.with_verification()`: `verify_bundle_bytes` below
    // checks both size and sha256 against the manifest, which is strictly
    // stronger than the download-layer hash-only check and avoids hashing
    // the payload twice. The body is streamed and capped at the declared
    // size so a manifest/node disagreement fails at the cap instead of
    // after buffering an arbitrarily large response.
    let download = client.download_file_by_message_hash(&bundle_ref).await?;
    let bytes = read_capped(download.into_stream(), manifest.bundle.size).await?;

    verify_bundle_bytes(&bytes, &manifest.bundle.sha256, manifest.bundle.size)?;

    fs::create_dir_all(&bundle_dir)?;
    extract_members(&bytes, &manifest.bundle.members, &bundle_dir)?;

    Ok(artifacts)
}

/// Verify and extract a bundle tarball that is already on disk (an
/// unpublished runtime built from an aleph-vm checkout), into the same
/// cache layout `fetch_bundle_artifacts` uses, so a later fetch of the
/// published bundle is a cache hit. `bundle.ref` is not consulted.
///
/// The whole tarball is read into memory, like the download path; the
/// manifest's `bundle.size` bounds it the same way (`MAX_BUNDLE_SIZE`).
pub fn import_bundle_file(
    bundle_path: &Path,
    manifest: &RuntimeManifest,
    cache_dir: &Path,
) -> Result<BundleArtifacts, BundleError> {
    let cache_key = bundle_cache_key(&manifest.bundle.sha256)?;
    let bundle_dir = cache_dir.join(cache_key);
    let artifacts = BundleArtifacts::in_dir(&bundle_dir);
    if artifacts.all_present() {
        return Ok(artifacts);
    }
    let bytes = fs::read(bundle_path)?;
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

/// Preallocation ceiling for the download buffer: `expected` is untrusted
/// manifest input, so it must not drive an arbitrarily large allocation
/// before a single byte has arrived.
const READ_PREALLOC_CAP: u64 = 64 * 1024 * 1024;

/// Collect `stream` into memory, aborting with [`BundleError::TooLarge`] as
/// soon as the running total exceeds `expected` bytes. A short body is not
/// an error here; `verify_bundle_bytes` reports it as a size mismatch.
async fn read_capped<S>(mut stream: S, expected: u64) -> Result<Vec<u8>, BundleError>
where
    S: Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin,
{
    let mut out = Vec::with_capacity(expected.min(READ_PREALLOC_CAP) as usize);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(reqwest_middleware::Error::from)
            .map_err(crate::client::MessageError::from)?;
        if (out.len() as u64).saturating_add(chunk.len() as u64) > expected {
            return Err(BundleError::TooLarge { expected });
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
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

/// Extract the five members declared in `members` from the gzipped tar
/// archive `bytes` into `dir`, named after their roles.
///
/// Every entry's path is checked for traversal (absolute paths or `..`
/// components) before it is compared against the declared member paths;
/// an unsafe path fails the whole extraction even if that entry is not one
/// of the members we care about. Each matched entry is written to
/// `<role>.part` and atomically renamed into place. A role whose member
/// path never shows up in the archive is reported as `MissingMember`.
/// Per-member cap on extracted size. `bundle.size` bounds the *compressed*
/// tarball (see `read_capped`), but gzip inflates a crafted archive by
/// orders of magnitude; this bounds what any one member can write to disk.
/// Generous against real artifacts (OVMF is a few MiB, kernel tens of MiB,
/// initrd at most a few hundred MiB).
const MAX_MEMBER_SIZE: u64 = 1024 * 1024 * 1024;

fn extract_members(bytes: &[u8], members: &BundleMembers, dir: &Path) -> Result<(), BundleError> {
    extract_members_with_limit(bytes, members, dir, MAX_MEMBER_SIZE)
}

fn extract_members_with_limit(
    bytes: &[u8],
    members: &BundleMembers,
    dir: &Path,
    max_member_size: u64,
) -> Result<(), BundleError> {
    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);

    let roles: [(&str, &'static str); 5] = [
        (members.ovmf.as_str(), ROLES[0]),
        (members.kernel.as_str(), ROLES[1]),
        (members.initrd.as_str(), ROLES[2]),
        (members.platform_rootfs.as_str(), ROLES[3]),
        (members.platform_hash_tree.as_str(), ROLES[4]),
    ];
    let mut found = [false; 5];

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
                let size = entry.header().size()?;
                if size > max_member_size {
                    return Err(BundleError::MemberTooLarge {
                        role,
                        size,
                        limit: max_member_size,
                    });
                }
                let target = dir.join(role);
                let part = dir.join(format!("{role}.part"));
                {
                    let mut file = fs::File::create(&part)?;
                    // `take` enforces the declared size even if the stream
                    // carries more; the header check above bounds the
                    // declaration itself.
                    std::io::copy(&mut std::io::Read::take(&mut entry, size), &mut file)?;
                }
                fs::rename(&part, &target)?;
                found[i] = true;
                break;
            }
        }
        // Stop reading once every member is extracted: the rest of the
        // archive is not needed, and iterating it is untrusted work.
        if found.iter().all(|&f| f) {
            break;
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

    /// Cache-hit fast path of the public entry point: when all five
    /// artifacts already sit under `cache_dir/<sha256>/`, no download is
    /// attempted (the client points at an unroutable host) and the returned
    /// paths are the cached ones.
    #[tokio::test]
    async fn fetch_bundle_artifacts_uses_the_cache_without_downloading() {
        let manifest =
            RuntimeManifest::parse(crate::vprogram::manifest::test::VALID_MANIFEST.as_bytes())
                .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let bundle_dir = cache.path().join(&manifest.bundle.sha256);
        std::fs::create_dir_all(&bundle_dir).unwrap();
        for role in [
            "ovmf",
            "kernel",
            "initrd",
            "platform_rootfs",
            "platform_hash_tree",
        ] {
            std::fs::write(bundle_dir.join(role), role).unwrap();
        }
        let client =
            crate::client::AlephClient::new(url::Url::parse("http://test.invalid").unwrap());
        let artifacts = fetch_bundle_artifacts(&client, &manifest, cache.path())
            .await
            .unwrap();
        assert_eq!(artifacts.ovmf, bundle_dir.join("ovmf"));
        assert_eq!(artifacts.kernel, bundle_dir.join("kernel"));
        assert_eq!(artifacts.initrd, bundle_dir.join("initrd"));
        assert_eq!(
            artifacts.platform_rootfs,
            bundle_dir.join("platform_rootfs")
        );
        assert_eq!(
            artifacts.platform_hash_tree,
            bundle_dir.join("platform_hash_tree")
        );
    }

    /// A cache dir written by an older CLI holds only ovmf/kernel/initrd.
    /// It must NOT count as a hit: the boot path needs the rootfs and its
    /// hash tree, so the bundle is fetched again (here: fails at download,
    /// which proves the fast path was skipped).
    #[tokio::test]
    async fn fetch_bundle_artifacts_refetches_a_three_member_legacy_cache() {
        let manifest =
            RuntimeManifest::parse(crate::vprogram::manifest::test::VALID_MANIFEST.as_bytes())
                .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let bundle_dir = cache.path().join(&manifest.bundle.sha256);
        std::fs::create_dir_all(&bundle_dir).unwrap();
        for role in ["ovmf", "kernel", "initrd"] {
            std::fs::write(bundle_dir.join(role), role).unwrap();
        }
        let client =
            crate::client::AlephClient::new(url::Url::parse("http://test.invalid").unwrap());
        let err = fetch_bundle_artifacts(&client, &manifest, cache.path())
            .await
            .unwrap_err();
        assert!(matches!(err, BundleError::Download(_)), "{err}");
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

    #[tokio::test]
    async fn read_capped_aborts_once_the_declared_size_is_exceeded() {
        let chunks: Vec<Result<bytes::Bytes, reqwest::Error>> = vec![
            Ok(bytes::Bytes::from_static(b"aaaa")),
            Ok(bytes::Bytes::from_static(b"bbbb")),
            Ok(bytes::Bytes::from_static(b"cc")),
        ];
        let err = read_capped(futures_util::stream::iter(chunks), 9)
            .await
            .unwrap_err();
        assert!(
            matches!(err, BundleError::TooLarge { expected: 9 }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn read_capped_collects_a_body_within_the_declared_size() {
        let chunks = || -> Vec<Result<bytes::Bytes, reqwest::Error>> {
            vec![
                Ok(bytes::Bytes::from_static(b"aaaa")),
                Ok(bytes::Bytes::from_static(b"bb")),
            ]
        };
        // Exactly at the cap.
        let out = read_capped(futures_util::stream::iter(chunks()), 6)
            .await
            .unwrap();
        assert_eq!(out, b"aaaabb");
        // Short bodies are left to `verify_bundle_bytes`.
        let out = read_capped(futures_util::stream::iter(chunks()), 100)
            .await
            .unwrap();
        assert_eq!(out, b"aaaabb");
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
            ("image/rootfs.ext4", b"r"),
            ("image/rootfs.ext4.verity", b"v"),
        ]);
        extract_members(&bytes, &test_members(), dir.path()).unwrap();
        assert_eq!(std::fs::read(dir.path().join("ovmf")).unwrap(), b"o");
        assert_eq!(std::fs::read(dir.path().join("kernel")).unwrap(), b"k");
        assert_eq!(std::fs::read(dir.path().join("initrd")).unwrap(), b"i");
        assert_eq!(
            std::fs::read(dir.path().join("platform_rootfs")).unwrap(),
            b"r"
        );
        assert_eq!(
            std::fs::read(dir.path().join("platform_hash_tree")).unwrap(),
            b"v"
        );
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
    fn extract_stops_after_the_last_member() {
        // A traversal entry *after* the five members must never be seen:
        // with the early exit it is not iterated, so extraction succeeds.
        let dir = tempfile::tempdir().unwrap();
        let bytes = make_test_bundle(&[
            ("image/OVMF.fd", b"ovmf"),
            ("image/bzImage", b"kernel"),
            ("image/initrd", b"initrd"),
            ("image/rootfs.ext4", b"r"),
            ("image/rootfs.ext4.verity", b"v"),
            ("../escape", b"never read"),
        ]);
        extract_members(&bytes, &test_members(), dir.path()).unwrap();
        assert_eq!(fs::read(dir.path().join("initrd")).unwrap(), b"initrd");
        // ...and the same entry *before* the members is still rejected.
        let dir = tempfile::tempdir().unwrap();
        let bytes = make_test_bundle(&[
            ("../escape", b"seen"),
            ("image/OVMF.fd", b"ovmf"),
            ("image/bzImage", b"kernel"),
            ("image/initrd", b"initrd"),
            ("image/rootfs.ext4", b"r"),
            ("image/rootfs.ext4.verity", b"v"),
        ]);
        assert!(matches!(
            extract_members(&bytes, &test_members(), dir.path()).unwrap_err(),
            BundleError::UnsafeMemberPath(_)
        ));
    }

    #[test]
    fn extract_rejects_members_above_the_size_limit() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = make_test_bundle(&[
            ("image/OVMF.fd", b"ovmf"),
            ("image/bzImage", b"a kernel that is larger than the limit"),
            ("image/initrd", b"initrd"),
            ("image/rootfs.ext4", b"r"),
            ("image/rootfs.ext4.verity", b"v"),
        ]);
        let err = extract_members_with_limit(&bytes, &test_members(), dir.path(), 16).unwrap_err();
        assert!(
            matches!(
                err,
                BundleError::MemberTooLarge {
                    role: "kernel",
                    size: 38,
                    limit: 16
                }
            ),
            "{err}"
        );
        // Nothing partial is left behind for the rejected member.
        assert!(!dir.path().join("kernel").exists());
        assert!(!dir.path().join("kernel.part").exists());
        // Members at or under the limit extract normally.
        extract_members_with_limit(&bytes, &test_members(), dir.path(), 38).unwrap();
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

    /// Builds a five-member bundle on disk plus a manifest whose bundle
    /// sha256/size match it, for the local-file import path.
    fn local_bundle_fixture(dir: &Path) -> (PathBuf, RuntimeManifest) {
        let bytes = make_test_bundle(&[
            ("image/OVMF.fd", b"o"),
            ("image/bzImage", b"k"),
            ("image/initrd", b"i"),
            ("image/rootfs.ext4", b"r"),
            ("image/rootfs.ext4.verity", b"v"),
        ]);
        let path = dir.join("snp-image.tar.gz");
        std::fs::write(&path, &bytes).unwrap();
        let mut json: serde_json::Value =
            serde_json::from_str(crate::vprogram::manifest::test::VALID_MANIFEST).unwrap();
        json["bundle"]["sha256"] = serde_json::Value::String(hex::encode(Sha256::digest(&bytes)));
        json["bundle"]["size"] = serde_json::Value::from(bytes.len() as u64);
        json["bundle"]["ref"] = serde_json::Value::String("0".repeat(64));
        let manifest = RuntimeManifest::parse(json.to_string().as_bytes()).unwrap();
        (path, manifest)
    }

    #[test]
    fn import_bundle_file_verifies_and_extracts_into_the_cache_layout() {
        let work = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let (path, manifest) = local_bundle_fixture(work.path());
        let artifacts = import_bundle_file(&path, &manifest, cache.path()).unwrap();
        let expected_dir = cache.path().join(&manifest.bundle.sha256);
        assert_eq!(
            artifacts.platform_rootfs,
            expected_dir.join("platform_rootfs")
        );
        assert!(artifacts.all_present());
        assert_eq!(std::fs::read(&artifacts.platform_hash_tree).unwrap(), b"v");
    }

    #[test]
    fn import_bundle_file_rejects_a_tarball_that_does_not_match_the_manifest() {
        let work = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let (path, manifest) = local_bundle_fixture(work.path());
        // Same size, different bytes: the size check passes, sha256 fails.
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        let err = import_bundle_file(&path, &manifest, cache.path()).unwrap_err();
        assert!(matches!(err, BundleError::ChecksumMismatch { .. }), "{err}");
        assert!(!cache.path().join(&manifest.bundle.sha256).exists());
    }
}
