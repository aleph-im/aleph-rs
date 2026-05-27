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

/// Output files produced by `sevctl session`. Paths are derived from the
/// `<prefix>` argument by appending the four well-known suffixes. `tek` and
/// `tik` are read later by `handle_start` directly from the session directory,
/// so they are kept here for descriptive completeness even though no caller
/// uses them via this struct today.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SessionFiles {
    pub godh: PathBuf,
    pub session: PathBuf,
    pub tek: PathBuf,
    pub tik: PathBuf,
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

    /// Shell out to `sevctl verify <cert_path>`. Validates the platform cert
    /// chain against AMD's embedded roots. Returns `Ok(())` on exit 0; surfaces
    /// stderr on non-zero exit.
    pub async fn verify(&self, cert_path: &Path) -> Result<(), SevctlError> {
        let output = tokio::process::Command::new(&self.path)
            .arg("verify")
            .arg(cert_path)
            .output()
            .await?;
        if output.status.success() {
            Ok(())
        } else {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            Err(SevctlError::NonZeroExit {
                command: "verify",
                code,
                stderr,
            })
        }
    }

    /// Shell out to `sevctl session --name <prefix> <cert_path> <policy>`.
    /// Writes four files (`<prefix>_godh.b64`, `<prefix>_session.b64`,
    /// `<prefix>_tek.bin`, `<prefix>_tik.bin`) and returns their paths.
    pub async fn session(
        &self,
        prefix: &Path,
        cert_path: &Path,
        policy: u32,
    ) -> Result<SessionFiles, SevctlError> {
        let output = tokio::process::Command::new(&self.path)
            .arg("session")
            .arg("--name")
            .arg(prefix)
            .arg(cert_path)
            .arg(policy.to_string())
            .output()
            .await?;
        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            return Err(SevctlError::NonZeroExit {
                command: "session",
                code,
                stderr,
            });
        }
        let prefix_str = prefix.display().to_string();
        Ok(SessionFiles {
            godh: PathBuf::from(format!("{prefix_str}_godh.b64")),
            session: PathBuf::from(format!("{prefix_str}_session.b64")),
            tek: PathBuf::from(format!("{prefix_str}_tek.bin")),
            tik: PathBuf::from(format!("{prefix_str}_tik.bin")),
        })
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

    #[cfg(unix)]
    #[tokio::test]
    async fn verify_returns_ok_when_binary_exits_zero() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("sevctl");
        std::fs::write(&fake, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let sevctl = Sevctl { path: fake };
        let cert = dir.path().join("cert.pem");
        std::fs::write(&cert, b"dummy").unwrap();
        sevctl.verify(&cert).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn verify_surfaces_non_zero_exit_with_stderr() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("sevctl");
        std::fs::write(&fake, "#!/bin/sh\necho 'chain invalid' >&2\nexit 2\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let sevctl = Sevctl { path: fake };
        let cert = dir.path().join("cert.pem");
        std::fs::write(&cert, b"dummy").unwrap();
        let err = sevctl.verify(&cert).await.unwrap_err();
        let SevctlError::NonZeroExit {
            code,
            stderr,
            command,
        } = err
        else {
            panic!("expected NonZeroExit");
        };
        assert_eq!(code, 2);
        assert_eq!(command, "verify");
        assert!(stderr.contains("chain invalid"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn session_returns_four_output_paths_and_writes_files() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("sevctl");
        // $1=session, $2=--name, $3=<prefix>, $4=<cert>, $5=<policy>
        // Emulate sevctl: write the four expected files at <prefix>_*.
        std::fs::write(
            &fake,
            "#!/bin/sh\nprefix=$3\necho godh > ${prefix}_godh.b64\necho session > ${prefix}_session.b64\nprintf 'tek-bytes' > ${prefix}_tek.bin\nprintf 'tik-bytes' > ${prefix}_tik.bin\nexit 0\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let sevctl = Sevctl { path: fake };
        let cert = dir.path().join("cert.pem");
        std::fs::write(&cert, b"dummy").unwrap();
        let prefix = dir.path().join("vm");
        let files = sevctl.session(&prefix, &cert, 1).await.unwrap();

        assert!(files.godh.exists());
        assert!(files.session.exists());
        assert!(files.tek.exists());
        assert!(files.tik.exists());
        assert_eq!(std::fs::read(&files.tek).unwrap(), b"tek-bytes");
        assert_eq!(std::fs::read(&files.tik).unwrap(), b"tik-bytes");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn session_surfaces_non_zero_exit() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("sevctl");
        std::fs::write(
            &fake,
            "#!/bin/sh\necho 'session derivation failed' >&2\nexit 3\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

        let sevctl = Sevctl { path: fake };
        let cert = dir.path().join("cert.pem");
        std::fs::write(&cert, b"dummy").unwrap();
        let prefix = dir.path().join("vm");
        let err = sevctl.session(&prefix, &cert, 1).await.unwrap_err();
        let SevctlError::NonZeroExit { code, command, .. } = err else {
            panic!("expected NonZeroExit");
        };
        assert_eq!(code, 3);
        assert_eq!(command, "session");
    }
}
