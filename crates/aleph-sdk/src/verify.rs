//! `ItemHash`-aware streaming hashing and verification.
//!
//! Wraps `aleph_cid`'s CID hashers with the Aleph-native SHA-256 storage
//! mode, dispatching on [`ItemHash`]: `Native` hashes are plain SHA-256 of
//! the content (the `storage` engine), `Ipfs` hashes are recomputed with the
//! kubo-compatible hasher matching the CID's version and codec.

use aleph_types::item_hash::{AlephItemHash, ItemHash};
use sha2::{Digest, Sha256};

pub use aleph_cid::verify::compute_cid;

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("integrity check failed: expected {expected}, computed {actual}")]
    IntegrityMismatch {
        expected: ItemHash,
        actual: ItemHash,
    },
    #[error("unsupported CID format for verification: {0}")]
    UnsupportedCid(String),
}

/// A streaming hasher that accumulates data and produces an `ItemHash` on
/// finalization.
pub enum Hasher {
    /// Aleph native storage hash: SHA-256 of the raw content.
    Native { hasher: Sha256 },
    /// IPFS CID computation, delegated to `aleph_cid`.
    Cid(aleph_cid::verify::Hasher),
}

impl Hasher {
    /// Creates a hasher for Aleph native storage (SHA-256).
    pub fn for_storage() -> Self {
        Self::Native {
            hasher: Sha256::new(),
        }
    }

    /// Creates a hasher for IPFS CIDv0 dag-pb (wrapped leaves, balanced DAG).
    pub fn for_ipfs() -> Self {
        Self::Cid(aleph_cid::verify::Hasher::for_ipfs())
    }

    /// Creates a hasher appropriate for the given expected hash.
    pub(crate) fn from_expected(expected: &ItemHash) -> Result<Self, VerifyError> {
        match expected {
            ItemHash::Native(_) => Ok(Self::for_storage()),
            ItemHash::Ipfs(cid) => aleph_cid::verify::Hasher::for_expected(cid)
                .map(Self::Cid)
                .map_err(|e| VerifyError::UnsupportedCid(e.0)),
        }
    }

    /// Feed data into the hasher.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Native { hasher } => hasher.update(data),
            Self::Cid(hasher) => hasher.update(data),
        }
    }

    /// Finalize the hasher and return the computed `ItemHash`.
    pub fn finalize(self) -> ItemHash {
        match self {
            Self::Native { hasher } => {
                let computed = AlephItemHash::new(hasher.finalize().into());
                ItemHash::Native(computed)
            }
            Self::Cid(hasher) => ItemHash::Ipfs(hasher.finalize()),
        }
    }
}

pub struct HashVerifier {
    hasher: Hasher,
    expected: ItemHash,
}

impl HashVerifier {
    pub fn new(expected: &ItemHash) -> Result<Self, VerifyError> {
        Ok(Self {
            hasher: Hasher::from_expected(expected)?,
            expected: expected.clone(),
        })
    }

    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    pub fn finalize(self) -> Result<(), VerifyError> {
        let computed = self.hasher.finalize();
        if computed == self.expected {
            Ok(())
        } else {
            Err(VerifyError::IntegrityMismatch {
                expected: self.expected,
                actual: computed,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_native_hash_success() {
        let data = b"hello world";
        let expected = ItemHash::Native(AlephItemHash::from_bytes(data));
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(data);
        verifier.finalize().expect("should verify successfully");
    }

    #[test]
    fn test_verify_native_hash_failure() {
        let expected = ItemHash::Native(AlephItemHash::from_bytes(b"hello world"));
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(b"wrong data");
        let err = verifier.finalize().unwrap_err();
        assert!(matches!(err, VerifyError::IntegrityMismatch { .. }));
    }

    #[test]
    fn test_verify_native_hash_chunked() {
        let data = b"hello world";
        let expected = ItemHash::Native(AlephItemHash::from_bytes(data));
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(b"hello");
        verifier.update(b" ");
        verifier.update(b"world");
        verifier.finalize().expect("chunked update should verify");
    }

    #[test]
    fn test_verify_ipfs_cidv0_success() {
        let data = b"hello dag-pb world";
        let expected = ItemHash::Ipfs(compute_cid(data));
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(data);
        verifier.finalize().expect("computed CID should verify");
    }

    #[test]
    fn test_verify_ipfs_cidv0_failure() {
        let expected = ItemHash::Ipfs(compute_cid(b"hello dag-pb world"));
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(b"wrong data");
        let err = verifier.finalize().unwrap_err();
        assert!(matches!(err, VerifyError::IntegrityMismatch { .. }));
    }

    #[test]
    fn test_verify_ipfs_multi_chunk() {
        let data = vec![0xABu8; 262144 + 100];
        let expected = ItemHash::Ipfs(compute_cid(&data));
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(&data);
        verifier.finalize().expect("multi-chunk CID should verify");
    }

    #[test]
    fn test_hasher_for_storage() {
        let data = b"hello world";
        let mut hasher = Hasher::for_storage();
        hasher.update(data);
        let hash = hasher.finalize();
        assert_eq!(hash, ItemHash::Native(AlephItemHash::from_bytes(data)));
    }

    #[test]
    fn test_hasher_for_storage_chunked() {
        let data = b"hello world";
        let mut hasher = Hasher::for_storage();
        hasher.update(b"hello ");
        hasher.update(b"world");
        let hash = hasher.finalize();
        assert_eq!(hash, ItemHash::Native(AlephItemHash::from_bytes(data)));
    }

    #[test]
    fn test_hasher_for_ipfs() {
        let data = b"hello dag-pb world";
        let mut hasher = Hasher::for_ipfs();
        hasher.update(data);
        let hash = hasher.finalize();
        assert_eq!(hash, ItemHash::Ipfs(compute_cid(data)));
    }
}
