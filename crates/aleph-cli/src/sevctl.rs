//! Shell-out helpers for AMD's `sevctl` binary. Kept off the SDK so the SDK
//! stays library-clean (no subprocess invocations).

use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SevctlError {
    #[error(
        "sevctl not found in PATH. Install AMD's sevctl (https://github.com/virtee/sevctl) and ensure it is executable."
    )]
    NotFound,
    #[error("sevctl {command} failed (exit code {code}):\n{stderr}")]
    NonZeroExit {
        command: &'static str,
        code: i32,
        stderr: String,
    },
    #[error("failed to invoke sevctl: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct Sevctl {
    pub(crate) path: PathBuf,
}

impl Sevctl {
    /// Locate the `sevctl` binary on PATH. Returns `SevctlError::NotFound`
    /// (with an install hint in the message) if it's missing.
    pub fn find() -> Result<Self, SevctlError> {
        which::which("sevctl")
            .map(|path| Self { path })
            .map_err(|_| SevctlError::NotFound)
    }

    /// The full path to the discovered binary. Useful for diagnostic output.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_reports_not_found_when_path_is_empty() {
        let prev = std::env::var_os("PATH");
        // SAFETY: tests in this crate run with --test-threads=1 so env
        // mutation is single-threaded; the prev value is restored after.
        unsafe { std::env::set_var("PATH", "") };
        let result = Sevctl::find();
        if let Some(prev) = prev {
            unsafe { std::env::set_var("PATH", prev) };
        } else {
            unsafe { std::env::remove_var("PATH") };
        }
        assert!(matches!(result, Err(SevctlError::NotFound)));
    }
}
