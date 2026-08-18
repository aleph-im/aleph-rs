//! Shell-out helpers for `mkfs.ext4` (part of e2fsprogs). Kept off the SDK so
//! the SDK stays library-clean (no subprocess invocations).

use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MkfsError {
    #[error("mkfs.ext4 not found in PATH. Install e2fsprogs and ensure it is executable.")]
    NotFound,
    #[error("mkfs.ext4 command failed (exit code {code}):\n{stderr}")]
    CommandFailed { code: i32, stderr: String },
    #[error("failed to invoke mkfs.ext4: {0}")]
    Io(#[from] std::io::Error),
}

/// Fixed filesystem UUID stamped onto every workload image so two builds of
/// the same staged content produce byte-identical output (mke2fs otherwise
/// draws a random UUID per run). Arbitrary but fixed and documented here;
/// `mke2fs` rejects the all-zeros UUID, so this is a fixed non-nil one
/// instead. Also reused as the `hash_seed` (see below) so the whole build is
/// pinned to a single documented constant.
const FIXED_FS_UUID: &str = "decade00-c0de-4000-8000-000000000001";

#[derive(Debug, Clone)]
pub struct MkfsExt4 {
    pub(crate) path: PathBuf,
}

impl MkfsExt4 {
    /// Locate the `mkfs.ext4` binary on PATH. Returns `MkfsError::NotFound`
    /// (with an install hint in the message) if it's missing.
    pub fn find() -> Result<Self, MkfsError> {
        which::which("mkfs.ext4")
            .map(|path| Self { path })
            .map_err(|_| MkfsError::NotFound)
    }

    /// Shell out to `mkfs.ext4 -b 4096 -d <staging> <out>`. `out` must already
    /// exist at its final size (mkfs.ext4 sizes the filesystem to the file).
    /// Returns `MkfsError::CommandFailed` with stderr on non-zero exit.
    ///
    /// The build is pinned to be reproducible: a fixed filesystem UUID and
    /// directory-hash seed (both `FIXED_FS_UUID`, so a third party rebuilding
    /// the image from the published compose file gets byte-identical output
    /// and can verify the published measurement), a fixed `root_owner`, and
    /// `E2FSPROGS_FAKE_TIME=0` so superblock timestamps don't leak the build
    /// time. Staged file/dir mtimes are normalized by the caller
    /// (`compose::build_workload_image`); reproducibility also depends on
    /// that.
    pub async fn build(&self, staging: &Path, out: &Path) -> Result<(), MkfsError> {
        let output = tokio::process::Command::new(&self.path)
            .arg("-b")
            .arg("4096")
            .arg("-U")
            .arg(FIXED_FS_UUID)
            .arg("-E")
            .arg(format!("hash_seed={FIXED_FS_UUID},root_owner=0:0"))
            .arg("-d")
            .arg(staging)
            .arg(out)
            .env("E2FSPROGS_FAKE_TIME", "0")
            .output()
            .await?;
        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            return Err(MkfsError::CommandFailed { code, stderr });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spawning a binary that was just written can intermittently fail with
    /// `ETXTBSY` ("text file busy") on Linux: a `fork` on another runtime
    /// thread (e.g. tokio's blocking/reactor pool) can momentarily hold a
    /// writable fd to the file open across the child's `exec`. The fix the
    /// kernel expects is to retry, so absorb the race here rather than letting
    /// it flake the suite under load.
    #[cfg(unix)]
    async fn retry_text_file_busy<F, Fut, T>(mut op: F) -> Result<T, MkfsError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, MkfsError>>,
    {
        use std::io::ErrorKind;
        for _ in 0..20 {
            match op().await {
                Err(MkfsError::Io(e)) if e.kind() == ErrorKind::ExecutableFileBusy => {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                result => return result,
            }
        }
        op().await
    }

    #[test]
    fn find_reports_not_found_when_path_is_empty() {
        let prev = std::env::var_os("PATH");
        // SAFETY: tests in this crate run with --test-threads=1 so env
        // mutation is single-threaded; the prev value is restored after.
        unsafe { std::env::set_var("PATH", "") };
        let result = MkfsExt4::find();
        if let Some(prev) = prev {
            unsafe { std::env::set_var("PATH", prev) };
        } else {
            unsafe { std::env::remove_var("PATH") };
        }
        assert!(matches!(result, Err(MkfsError::NotFound)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn build_invokes_mkfs_with_block_size_and_staging_dir() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("mkfs.ext4");
        let argv_log = dir.path().join("argv");
        let env_log = dir.path().join("env");
        std::fs::write(
            &fake,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nprintf '%s' \"$E2FSPROGS_FAKE_TIME\" > {}\nexit 0\n",
                argv_log.display(),
                env_log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mkfs = MkfsExt4 { path: fake };
        let staging = dir.path().join("staging");
        std::fs::create_dir(&staging).unwrap();
        let out = dir.path().join("out.ext4");
        std::fs::write(&out, []).unwrap();

        retry_text_file_busy(|| mkfs.build(&staging, &out))
            .await
            .unwrap();

        let argv = std::fs::read_to_string(&argv_log).unwrap();
        let args: Vec<&str> = argv.lines().collect();
        assert_eq!(
            args,
            vec![
                "-b",
                "4096",
                "-U",
                FIXED_FS_UUID,
                "-E",
                &format!("hash_seed={FIXED_FS_UUID},root_owner=0:0"),
                "-d",
                staging.to_str().unwrap(),
                out.to_str().unwrap()
            ]
        );
        let env = std::fs::read_to_string(&env_log).unwrap();
        assert_eq!(
            env, "0",
            "E2FSPROGS_FAKE_TIME must be pinned for reproducible superblock timestamps"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn build_surfaces_non_zero_exit_with_stderr() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("mkfs.ext4");
        std::fs::write(&fake, "#!/bin/sh\necho 'bad geometry' >&2\nexit 1\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mkfs = MkfsExt4 { path: fake };
        let staging = dir.path().join("staging");
        std::fs::create_dir(&staging).unwrap();
        let out = dir.path().join("out.ext4");
        std::fs::write(&out, []).unwrap();

        let err = retry_text_file_busy(|| mkfs.build(&staging, &out))
            .await
            .unwrap_err();
        let MkfsError::CommandFailed { code, stderr } = err else {
            panic!("expected CommandFailed");
        };
        assert_eq!(code, 1);
        assert!(stderr.contains("bad geometry"));
    }
}
