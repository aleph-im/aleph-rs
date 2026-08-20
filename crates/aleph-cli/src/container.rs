//! Shell-out helpers for `podman`/`docker`. Kept off the SDK so the SDK
//! stays library-clean (no subprocess invocations).

use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    Podman,
    Docker,
}

#[derive(Debug, Error)]
pub enum ContainerError {
    #[error(
        "no container tool found: install podman (preferred) or docker, \
         or supply prebuilt archives with --image-archive"
    )]
    NotFound,
    #[error("{command} failed with status {code}: {stderr}")]
    CommandFailed {
        command: &'static str,
        code: i32,
        stderr: String,
    },
    #[error(
        "image {0:?} has no repo digest after pull; was it built locally? \
         pass it with --image-archive instead"
    )]
    NoDigest(String),
    #[error("failed to invoke container tool: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct ContainerTool {
    pub(crate) path: PathBuf,
    pub(crate) flavor: Flavor,
}

impl ContainerTool {
    /// Locate a container tool on PATH, preferring `podman` over `docker`.
    /// Returns `ContainerError::NotFound` (with an install hint in the
    /// message) if neither is present.
    pub fn find() -> Result<Self, ContainerError> {
        if let Ok(path) = which::which("podman") {
            return Ok(Self {
                path,
                flavor: Flavor::Podman,
            });
        }
        if let Ok(path) = which::which("docker") {
            return Ok(Self {
                path,
                flavor: Flavor::Docker,
            });
        }
        Err(ContainerError::NotFound)
    }

    /// Shell out to `<tool> pull <image>`. Returns `Ok(())` on exit 0;
    /// surfaces stderr via `ContainerError::CommandFailed` on non-zero exit.
    pub async fn pull(&self, image: &str) -> Result<(), ContainerError> {
        let output = tokio::process::Command::new(&self.path)
            .arg("pull")
            .arg(image)
            .output()
            .await?;
        if output.status.success() {
            Ok(())
        } else {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            Err(ContainerError::CommandFailed {
                command: "pull",
                code,
                stderr,
            })
        }
    }

    /// Shell out to `<tool> image inspect --format {{index .RepoDigests 0}}
    /// <image>` and return the trimmed `name@sha256:...` string. Errors with
    /// `ContainerError::NoDigest` if the output is empty or `<no value>`
    /// (podman/docker's placeholder for a missing template field), which
    /// happens for locally-built images that were never pushed to or pulled
    /// from a registry.
    pub async fn resolve_digest(&self, image: &str) -> Result<String, ContainerError> {
        let output = tokio::process::Command::new(&self.path)
            .arg("image")
            .arg("inspect")
            .arg("--format")
            .arg("{{index .RepoDigests 0}}")
            .arg(image)
            .output()
            .await?;
        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            return Err(ContainerError::CommandFailed {
                command: "image inspect",
                code,
                stderr,
            });
        }
        let digest = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if digest.is_empty() || digest == "<no value>" {
            return Err(ContainerError::NoDigest(image.to_string()));
        }
        Ok(digest)
    }

    /// Shell out to save `image` to an archive at `out`. Podman writes an
    /// OCI archive (`save --format oci-archive -o <out> <image>`); docker
    /// writes its own docker-archive format (`save -o <out> <image>`), which
    /// `podman load` in the guest also accepts.
    pub async fn save_archive(&self, image: &str, out: &Path) -> Result<(), ContainerError> {
        let mut cmd = tokio::process::Command::new(&self.path);
        cmd.arg("save");
        if self.flavor == Flavor::Podman {
            cmd.arg("--format").arg("oci-archive");
        }
        cmd.arg("-o").arg(out).arg(image);
        let output = cmd.output().await?;
        if output.status.success() {
            Ok(())
        } else {
            let code = output.status.code().unwrap_or(-1);
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            Err(ContainerError::CommandFailed {
                command: "save",
                code,
                stderr,
            })
        }
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
    async fn retry_text_file_busy<F, Fut, T>(mut op: F) -> Result<T, ContainerError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ContainerError>>,
    {
        use std::io::ErrorKind;
        for _ in 0..20 {
            match op().await {
                Err(ContainerError::Io(e)) if e.kind() == ErrorKind::ExecutableFileBusy => {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                result => return result,
            }
        }
        op().await
    }

    #[cfg(unix)]
    fn write_fake_tool(dir: &Path, script: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let fake = dir.join("container-tool");
        std::fs::write(&fake, script).unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        fake
    }

    #[test]
    fn find_reports_not_found_when_path_is_empty() {
        let prev = std::env::var_os("PATH");
        // SAFETY: tests in this crate run with --test-threads=1 so env
        // mutation is single-threaded; the prev value is restored after.
        unsafe { std::env::set_var("PATH", "") };
        let result = ContainerTool::find();
        if let Some(prev) = prev {
            unsafe { std::env::set_var("PATH", prev) };
        } else {
            unsafe { std::env::remove_var("PATH") };
        }
        assert!(matches!(result, Err(ContainerError::NotFound)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolve_digest_invokes_image_inspect() {
        let dir = tempfile::tempdir().unwrap();
        let argv_log = dir.path().join("argv");
        let fake = write_fake_tool(
            dir.path(),
            &format!(
                "#!/bin/sh\necho \"$@\" >> {}\nprintf 'docker.io/library/nginx@sha256:abc\\n'\n",
                argv_log.display()
            ),
        );

        let tool = ContainerTool {
            path: fake,
            flavor: Flavor::Podman,
        };
        let digest = retry_text_file_busy(|| tool.resolve_digest("nginx:1.27"))
            .await
            .unwrap();
        assert_eq!(digest, "docker.io/library/nginx@sha256:abc");

        let argv = std::fs::read_to_string(&argv_log).unwrap();
        assert!(argv.contains("image inspect --format {{index .RepoDigests 0}} nginx:1.27"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolve_digest_rejects_an_empty_result() {
        let dir = tempfile::tempdir().unwrap();
        let fake = write_fake_tool(dir.path(), "#!/bin/sh\nprintf '\\n'\n");

        let tool = ContainerTool {
            path: fake,
            flavor: Flavor::Podman,
        };
        let err = retry_text_file_busy(|| tool.resolve_digest("nginx:1.27"))
            .await
            .unwrap_err();
        assert!(matches!(err, ContainerError::NoDigest(image) if image == "nginx:1.27"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolve_digest_rejects_no_value_placeholder() {
        let dir = tempfile::tempdir().unwrap();
        let fake = write_fake_tool(dir.path(), "#!/bin/sh\nprintf '<no value>\\n'\n");

        let tool = ContainerTool {
            path: fake,
            flavor: Flavor::Podman,
        };
        let err = retry_text_file_busy(|| tool.resolve_digest("nginx:1.27"))
            .await
            .unwrap_err();
        assert!(matches!(err, ContainerError::NoDigest(image) if image == "nginx:1.27"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn save_archive_uses_oci_archive_for_podman() {
        let dir = tempfile::tempdir().unwrap();
        let argv_log = dir.path().join("argv");
        let fake = write_fake_tool(
            dir.path(),
            &format!("#!/bin/sh\necho \"$@\" >> {}\nexit 0\n", argv_log.display()),
        );

        let tool = ContainerTool {
            path: fake,
            flavor: Flavor::Podman,
        };
        let out = dir.path().join("nginx.tar");
        retry_text_file_busy(|| tool.save_archive("nginx:1.27", &out))
            .await
            .unwrap();

        let argv = std::fs::read_to_string(&argv_log).unwrap();
        assert!(argv.contains("save --format oci-archive -o"));
        assert!(argv.contains(out.to_str().unwrap()));
        assert!(argv.contains("nginx:1.27"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn save_archive_uses_plain_save_for_docker() {
        let dir = tempfile::tempdir().unwrap();
        let argv_log = dir.path().join("argv");
        let fake = write_fake_tool(
            dir.path(),
            &format!("#!/bin/sh\necho \"$@\" >> {}\nexit 0\n", argv_log.display()),
        );

        let tool = ContainerTool {
            path: fake,
            flavor: Flavor::Docker,
        };
        let out = dir.path().join("nginx.tar");
        retry_text_file_busy(|| tool.save_archive("nginx:1.27", &out))
            .await
            .unwrap();

        let argv = std::fs::read_to_string(&argv_log).unwrap();
        let expected = format!("save -o {} nginx:1.27", out.to_str().unwrap());
        assert_eq!(argv.trim(), expected);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pull_surfaces_stderr_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let fake = write_fake_tool(
            dir.path(),
            "#!/bin/sh\necho 'unable to pull image' >&2\nexit 1\n",
        );

        let tool = ContainerTool {
            path: fake,
            flavor: Flavor::Podman,
        };
        let err = retry_text_file_busy(|| tool.pull("nginx:1.27"))
            .await
            .unwrap_err();
        let ContainerError::CommandFailed {
            command,
            code,
            stderr,
        } = err
        else {
            panic!("expected CommandFailed");
        };
        assert_eq!(command, "pull");
        assert_eq!(code, 1);
        assert!(stderr.contains("unable to pull image"));
    }
}
