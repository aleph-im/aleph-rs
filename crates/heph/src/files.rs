use sha2::{Digest, Sha256};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Simple filesystem storage for file blobs.
///
/// Files are stored at `{base_dir}/{hash[0..2]}/{hash}` (2-char prefix sharding).
pub struct FileStore {
    base_dir: PathBuf,
}

impl FileStore {
    /// Create a new `FileStore` rooted at `base_dir`, creating the directory if needed.
    pub fn new(base_dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(base_dir)?;
        Ok(Self {
            base_dir: base_dir.to_path_buf(),
        })
    }

    /// Write `data` to the store and return the lowercase hex SHA-256 hash.
    ///
    /// If a file with the same hash already exists it is left untouched (content-addressable).
    pub fn write(&self, data: &[u8]) -> io::Result<String> {
        let hash = sha256_hex(data);
        let path = self.path(&hash);
        // Create the shard directory.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Only write if the file doesn't exist yet.
        if !path.exists() {
            fs::write(&path, data)?;
        }
        Ok(hash)
    }

    /// Read the file with the given hex hash.
    pub fn read(&self, hash: &str) -> io::Result<Vec<u8>> {
        fs::read(self.path(hash))
    }

    /// Return `true` if a file with the given hash is present.
    pub fn exists(&self, hash: &str) -> bool {
        self.path(hash).exists()
    }

    /// Return the size in bytes of the stored file.
    pub fn size(&self, hash: &str) -> io::Result<u64> {
        Ok(fs::metadata(self.path(hash))?.len())
    }

    /// Compute the on-disk path for `hash`.
    fn path(&self, hash: &str) -> PathBuf {
        let prefix = &hash[..2.min(hash.len())];
        self.base_dir.join(prefix).join(hash)
    }
}

/// Compute the lowercase hex SHA-256 of `data`.
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_write_read_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = FileStore::new(dir.path()).unwrap();

        let data = b"hello, aleph!";
        let hash = store.write(data).unwrap();

        assert_eq!(hash.len(), 64, "SHA-256 hex should be 64 chars");
        assert!(store.exists(&hash));
        assert_eq!(store.read(&hash).unwrap(), data);
        assert_eq!(store.size(&hash).unwrap(), data.len() as u64);
    }

    #[test]
    fn test_write_idempotent() {
        let dir = TempDir::new().unwrap();
        let store = FileStore::new(dir.path()).unwrap();

        let data = b"idempotent";
        let hash1 = store.write(data).unwrap();
        let hash2 = store.write(data).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_shard_path() {
        let dir = TempDir::new().unwrap();
        let store = FileStore::new(dir.path()).unwrap();

        let data = b"shard test";
        let hash = store.write(data).unwrap();

        // File should be at base_dir/<hash[0..2]>/<hash>
        let expected = dir.path().join(&hash[..2]).join(&hash);
        assert!(expected.exists(), "file should exist at sharded path");
    }

    #[test]
    fn test_read_missing_returns_error() {
        let dir = TempDir::new().unwrap();
        let store = FileStore::new(dir.path()).unwrap();

        let result = store.read("0000000000000000000000000000000000000000000000000000000000000000");
        assert!(result.is_err());
    }
}
