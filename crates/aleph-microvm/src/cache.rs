use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;

use crate::error::{MicrovmError, Result};

/// Content-addressed cache for immutable VM artifacts (kernel, runtime rootfs).
pub struct ArtifactCache {
    root: PathBuf,
}

impl ArtifactCache {
    pub fn with_root(root: PathBuf) -> Self {
        ArtifactCache { root }
    }

    /// Default cache root: $XDG_CACHE_HOME/aleph/microvm or ~/.cache/aleph/microvm.
    pub fn default_location() -> Self {
        let base = std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        ArtifactCache::with_root(base.join("aleph").join("microvm"))
    }

    pub fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// Ensure the artifact identified by `key` exists locally, downloading from `url` if absent.
    /// Download is atomic (temp file + rename).
    pub async fn ensure(&self, key: &str, url: &str) -> Result<PathBuf> {
        let target = self.path_for(key);
        if target.exists() {
            return Ok(target);
        }
        tokio::fs::create_dir_all(&self.root).await?;
        let tmp = self.root.join(format!("{key}.partial"));
        download_to(url, &tmp).await?;
        tokio::fs::rename(&tmp, &target).await?;
        Ok(target)
    }
}

async fn download_to(url: &str, dest: &Path) -> Result<()> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| MicrovmError::Download(e.to_string()))?
        .error_for_status()
        .map_err(|e| MicrovmError::Download(e.to_string()))?;
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| MicrovmError::Download(e.to_string()))?;
    let mut f = tokio::fs::File::create(dest).await?;
    f.write_all(&bytes).await?;
    f.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_path_is_content_addressed() {
        let cache = ArtifactCache::with_root("/tmp/aleph-cache-test".into());
        let p = cache.path_for("abc123");
        assert!(p.ends_with("abc123"));
        assert!(p.starts_with("/tmp/aleph-cache-test"));
    }

    #[tokio::test]
    async fn ensure_returns_existing_without_download() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ArtifactCache::with_root(dir.path().to_path_buf());
        let target = cache.path_for("deadbeef");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, b"cached").unwrap();
        // url unreachable on purpose; must not be hit since file exists.
        let got = cache
            .ensure("deadbeef", "http://127.0.0.1:1/never")
            .await
            .unwrap();
        assert_eq!(got, target);
    }
}
