//! Shell-out helpers for `mkfs.ext4` and `debugfs` (both part of e2fsprogs).
//! Kept off the SDK so the SDK stays library-clean (no subprocess
//! invocations).

use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MkfsError {
    #[error(
        "mkfs.ext4 and debugfs not found in PATH. Install e2fsprogs and ensure they are executable."
    )]
    NotFound,
    #[error("{command} command failed (exit code {code}):\n{stderr}")]
    CommandFailed {
        command: &'static str,
        code: i32,
        stderr: String,
    },
    #[error("failed to invoke e2fsprogs tool: {0}")]
    Io(#[from] std::io::Error),
}

/// Fixed filesystem UUID stamped onto every workload image so two builds of
/// the same staged content produce byte-identical output (mke2fs otherwise
/// draws a random UUID per run). Arbitrary but fixed and documented here;
/// `mke2fs` rejects the all-zeros UUID, so this is a fixed non-nil one
/// instead. Also reused as the `hash_seed` (see below) so the whole build is
/// pinned to a single documented constant.
const FIXED_FS_UUID: &str = "decade00-c0de-4000-8000-000000000001";

/// Fixed Unix timestamp stamped onto every superblock and inode time field
/// (as `E2FSPROGS_FAKE_TIME`, `SOURCE_DATE_EPOCH`, and the `debugfs`
/// post-pass value). Deliberately not `0`: e2fsprogs treats
/// `E2FSPROGS_FAKE_TIME=0` as unset and falls back to the wall clock.
const FIXED_BUILD_TIME: &str = "1";

#[derive(Debug, Clone)]
pub struct MkfsExt4 {
    pub(crate) path: PathBuf,
    pub(crate) debugfs: PathBuf,
}

impl MkfsExt4 {
    /// Locate the `mkfs.ext4` binary on PATH. Returns `MkfsError::NotFound`
    /// (with an install hint in the message) if it's missing.
    pub fn find() -> Result<Self, MkfsError> {
        Self::find_in(std::env::var_os("PATH"))
    }

    /// `find`, searching `path` (a PATH-style list) instead of the process
    /// environment.
    pub(crate) fn find_in<P: AsRef<std::ffi::OsStr>>(path: Option<P>) -> Result<Self, MkfsError> {
        let path = path.as_ref().map(AsRef::as_ref);
        let locate = |name: &str| {
            which::which_in_global(name, path)
                .ok()
                .and_then(|mut found| found.next())
        };
        match (locate("mkfs.ext4"), locate("debugfs")) {
            (Some(path), Some(debugfs)) => Ok(Self { path, debugfs }),
            _ => Err(MkfsError::NotFound),
        }
    }

    /// Shell out to `mkfs.ext4 -b 4096 -d <staging> <out>`. `out` must already
    /// exist at its final size (mkfs.ext4 sizes the filesystem to the file).
    /// Returns `MkfsError::CommandFailed` with stderr on non-zero exit.
    ///
    /// The build is pinned to be reproducible, so a third party rebuilding
    /// the image from the published compose file gets byte-identical output
    /// and can verify the published measurement:
    ///
    /// - a fixed filesystem UUID and directory-hash seed (both
    ///   `FIXED_FS_UUID`) and a fixed `root_owner`;
    /// - `E2FSPROGS_FAKE_TIME` and `SOURCE_DATE_EPOCH` pinned to
    ///   `FIXED_BUILD_TIME`, which fixes the superblock timestamps and, on
    ///   e2fsprogs >= 1.47.1, clamps every inode timestamp;
    /// - a `debugfs` post-pass that sets every inode's atime/mtime/ctime/
    ///   crtime to `FIXED_BUILD_TIME`. This is what makes the build
    ///   reproducible on e2fsprogs 1.47.0 (Ubuntu 24.04), which lacks
    ///   `SOURCE_DATE_EPOCH` support and copies each staged file's ctime
    ///   (the wall clock at staging time, which userspace cannot set) into
    ///   the image. It runs unconditionally so the output does not depend
    ///   on the e2fsprogs version.
    ///
    /// Staged file/dir mtimes are normalized by the caller
    /// (`compose::build_workload_image`); the post-pass overwrites them
    /// anyway, but keeping the staging input canonical is cheap.
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
            .env("E2FSPROGS_FAKE_TIME", FIXED_BUILD_TIME)
            .env("SOURCE_DATE_EPOCH", FIXED_BUILD_TIME)
            .output()
            .await?;
        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            return Err(MkfsError::CommandFailed {
                command: "mkfs.ext4",
                code,
                stderr,
            });
        }
        self.normalize_inode_times(staging, out).await
    }

    /// Run `debugfs -w -f <script> <out>` with one `set_inode_field` line per
    /// time field for every inode in the image: `/`, `/lost+found` (created
    /// by mke2fs itself), and every staged path. `E2FSPROGS_FAKE_TIME` is
    /// pinned for the debugfs run too, so its own superblock write-time
    /// update doesn't reintroduce the wall clock.
    async fn normalize_inode_times(&self, staging: &Path, out: &Path) -> Result<(), MkfsError> {
        let mut script = String::new();
        for image_path in image_paths(staging)? {
            for field in ["atime", "mtime", "ctime", "crtime"] {
                script.push_str(&format!(
                    "set_inode_field {image_path} {field} {FIXED_BUILD_TIME}\n"
                ));
            }
        }
        let script_file = tempfile::NamedTempFile::new()?;
        std::fs::write(script_file.path(), script)?;

        let output = tokio::process::Command::new(&self.debugfs)
            .arg("-w")
            .arg("-f")
            .arg(script_file.path())
            .arg(out)
            .env("E2FSPROGS_FAKE_TIME", FIXED_BUILD_TIME)
            .output()
            .await?;
        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            return Err(MkfsError::CommandFailed {
                command: "debugfs",
                code,
                stderr,
            });
        }
        Ok(())
    }
}

/// Every path inside the built image, in the `/`-rooted, `/`-separated form
/// `debugfs` expects: the root, mke2fs's own `lost+found`, and each entry
/// under `staging` (sorted, so the generated script is itself
/// deterministic).
fn image_paths(staging: &Path) -> Result<Vec<String>, MkfsError> {
    let mut paths = vec!["/".to_string(), "/lost+found".to_string()];
    for entry in walkdir::WalkDir::new(staging)
        .min_depth(1)
        .sort_by_file_name()
    {
        let entry = entry.map_err(std::io::Error::other)?;
        let rel = entry
            .path()
            .strip_prefix(staging)
            .expect("walkdir entries are under the staging root");
        let rel = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        paths.push(format!("/{rel}"));
    }
    Ok(paths)
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
        // Inject the search path rather than mutating the process-wide PATH:
        // tests run in parallel, and the other `find_*` tests in this crate
        // do the same lookup, so an env round-trip races and flakes.
        let result = MkfsExt4::find_in(Some(""));
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
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nprintf '%s %s' \"$E2FSPROGS_FAKE_TIME\" \"$SOURCE_DATE_EPOCH\" > {}\nexit 0\n",
                argv_log.display(),
                env_log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let debugfs = dir.path().join("debugfs");
        let debugfs_argv_log = dir.path().join("debugfs-argv");
        let debugfs_script_log = dir.path().join("debugfs-script");
        let debugfs_env_log = dir.path().join("debugfs-env");
        std::fs::write(
            &debugfs,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\ncp \"$3\" {}\nprintf '%s' \"$E2FSPROGS_FAKE_TIME\" > {}\nexit 0\n",
                debugfs_argv_log.display(),
                debugfs_script_log.display(),
                debugfs_env_log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&debugfs, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mkfs = MkfsExt4 {
            path: fake,
            debugfs,
        };
        let staging = dir.path().join("staging");
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join("docker-compose.yml"), "services: {}\n").unwrap();
        std::fs::create_dir(staging.join("images")).unwrap();
        std::fs::write(staging.join("images").join("000-nginx.tar"), "tar").unwrap();
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
            env,
            format!("{FIXED_BUILD_TIME} {FIXED_BUILD_TIME}"),
            "E2FSPROGS_FAKE_TIME and SOURCE_DATE_EPOCH must be pinned for reproducible timestamps"
        );

        // The debugfs post-pass runs against the same image, under the same
        // fake time, with one line per (inode, time field).
        let argv = std::fs::read_to_string(&debugfs_argv_log).unwrap();
        let args: Vec<&str> = argv.lines().collect();
        assert_eq!(args.len(), 4);
        assert_eq!(&args[..2], ["-w", "-f"]);
        assert_eq!(args[3], out.to_str().unwrap());
        assert_eq!(
            std::fs::read_to_string(&debugfs_env_log).unwrap(),
            FIXED_BUILD_TIME
        );
        let script = std::fs::read_to_string(&debugfs_script_log).unwrap();
        let lines: Vec<&str> = script.lines().collect();
        for path in [
            "/",
            "/lost+found",
            "/docker-compose.yml",
            "/images",
            "/images/000-nginx.tar",
        ] {
            for field in ["atime", "mtime", "ctime", "crtime"] {
                let expected = format!("set_inode_field {path} {field} {FIXED_BUILD_TIME}");
                assert!(lines.contains(&expected.as_str()), "missing {expected:?}");
            }
        }
        assert_eq!(lines.len(), 5 * 4, "unexpected extra lines:\n{script}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn build_surfaces_non_zero_exit_with_stderr() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("mkfs.ext4");
        std::fs::write(&fake, "#!/bin/sh\necho 'bad geometry' >&2\nexit 1\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mkfs = MkfsExt4 {
            path: fake.clone(),
            debugfs: fake,
        };
        let staging = dir.path().join("staging");
        std::fs::create_dir(&staging).unwrap();
        let out = dir.path().join("out.ext4");
        std::fs::write(&out, []).unwrap();

        let err = retry_text_file_busy(|| mkfs.build(&staging, &out))
            .await
            .unwrap_err();
        let MkfsError::CommandFailed {
            command: "mkfs.ext4",
            code,
            stderr,
        } = err
        else {
            panic!("expected CommandFailed");
        };
        assert_eq!(code, 1);
        assert!(stderr.contains("bad geometry"));
    }
}
