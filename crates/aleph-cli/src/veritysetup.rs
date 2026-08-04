//! Shell-out helpers for the `veritysetup` binary (part of cryptsetup).
//! Kept off the SDK so the SDK stays library-clean (no subprocess invocations).

use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VerityError {
    #[error(
        "veritysetup not found in PATH. Install cryptsetup (which includes veritysetup) and ensure it is executable."
    )]
    NotFound,
    #[error("veritysetup command failed (exit code {code}):\n{stderr}")]
    CommandFailed { code: i32, stderr: String },
    #[error("failed to invoke veritysetup: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct Veritysetup {
    pub(crate) path: PathBuf,
}

impl Veritysetup {
    /// Locate the `veritysetup` binary on PATH. Returns `VerityError::NotFound`
    /// (with an install hint in the message) if it's missing.
    pub fn find() -> Result<Self, VerityError> {
        which::which("veritysetup")
            .map(|path| Self { path })
            .map_err(|_| VerityError::NotFound)
    }

    /// Shell out to `veritysetup format <data> <hash_tree_out>` and return the
    /// verity root hash (64 lowercase hex). Returns `VerityError::CommandFailed`
    /// with stderr on non-zero exit.
    pub async fn format(&self, data: &Path, hash_tree_out: &Path) -> Result<String, VerityError> {
        let output = tokio::process::Command::new(&self.path)
            .arg("format")
            .arg(data)
            .arg(hash_tree_out)
            .output()
            .await?;
        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            return Err(VerityError::CommandFailed { code, stderr });
        }
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        Self::parse_root_hash(&stdout)
    }

    /// Parse the root hash from `veritysetup format` output.
    /// Finds the line starting with "Root hash:", extracts the hash,
    /// and validates it's 64 lowercase hex digits.
    fn parse_root_hash(stdout: &str) -> Result<String, VerityError> {
        for line in stdout.lines() {
            if line.starts_with("Root hash:") {
                let hash = line.strip_prefix("Root hash:").unwrap().trim();
                // Validate: 64 lowercase hex characters
                if hash.len() == 64
                    && hash == hash.to_lowercase()
                    && hash.chars().all(|c| c.is_ascii_hexdigit())
                {
                    return Ok(hash.to_string());
                } else {
                    return Err(VerityError::CommandFailed {
                        code: -1,
                        stderr: format!("Invalid root hash format: {}", hash),
                    });
                }
            }
        }
        Err(VerityError::CommandFailed {
            code: -1,
            stderr: "Root hash not found in output".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_root_hash_from_format_output() {
        let out = "VERITY header information for /tmp/tree.img\n\
UUID:            5b2eeb54-4f3f-4c2f-9c1c-9e1a3d0f6a2f\n\
Hash type:       1\n\
Data blocks:     256\n\
Data block size: 4096\n\
Hash block size: 4096\n\
Hash algorithm:  sha256\n\
Salt:            f1a2b3c4d5e6f7a8f1a2b3c4d5e6f7a8f1a2b3c4d5e6f7a8f1a2b3c4d5e6f7a8\n\
Root hash:       cb121a317be7dc7969dd633ca9b6c3718ffe9ea6715b64e0e35a871d484b56b8\n";
        assert_eq!(
            Veritysetup::parse_root_hash(out).unwrap(),
            "cb121a317be7dc7969dd633ca9b6c3718ffe9ea6715b64e0e35a871d484b56b8"
        );
    }

    #[test]
    fn missing_root_hash_is_an_error() {
        assert!(Veritysetup::parse_root_hash("no hash here").is_err());
    }
}
