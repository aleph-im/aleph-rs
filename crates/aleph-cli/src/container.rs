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
    #[error(
        "image {image:?}: unexpected `image inspect` output {output:?}; expected a single \
         `<name>@sha256:<64 hex>` repo digest"
    )]
    MalformedDigest { image: String, output: String },
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
        Self::find_in(std::env::var_os("PATH"))
    }

    /// `find`, searching `path` (a PATH-style list) instead of the process
    /// environment.
    pub(crate) fn find_in<P: AsRef<std::ffi::OsStr>>(
        path: Option<P>,
    ) -> Result<Self, ContainerError> {
        let path = path.as_ref().map(AsRef::as_ref);
        for (name, flavor) in [("podman", Flavor::Podman), ("docker", Flavor::Docker)] {
            if let Some(found) = which::which_in_global(name, path)
                .ok()
                .and_then(|mut found| found.next())
            {
                return Ok(Self {
                    path: found,
                    flavor,
                });
            }
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
        // The digest is reported to the caller as the image's provenance,
        // so anything the tool prints on stdout that is not a repo digest
        // (a warning line, a multi-line template result) must not be
        // accepted as one.
        if !is_repo_digest(&digest) {
            return Err(ContainerError::MalformedDigest {
                image: image.to_string(),
                output: digest,
            });
        }
        Ok(digest)
    }

    /// Pull `image`, resolve its registry digest, and save it to `out`.
    /// Returns the resolved `name@sha256:...` digest for reporting.
    ///
    /// The archive is deliberately saved from the ORIGINAL reference, not the
    /// resolved digest (#348): `docker save name@sha256:...` writes a
    /// docker-archive with `RepoTags: null` and no RepoDigests, so `podman
    /// load` in the guest imports a bare image ID that compose cannot match
    /// against the `image:` string. A tagged archive round-trips: the tag is
    /// restored on load and compose resolves it from local storage. Image
    /// integrity is carried by the verity-measured workload volume bytes, not
    /// by the name in the compose file; the digest is still resolved so a
    /// locally-built image is refused (`ContainerError::NoDigest`) and the
    /// exact identity can be surfaced to the caller.
    pub async fn pull_and_save(&self, image: &str, out: &Path) -> Result<String, ContainerError> {
        self.pull(image).await?;
        let digest = self.resolve_digest(image).await?;
        self.save_archive(image, out).await?;
        Ok(digest)
    }

    /// Shell out to save `image` to an archive at `out`. Podman writes an
    /// OCI archive (`save --format oci-archive -o <out> <image>`); docker
    /// writes its own docker-archive format (`save -o <out> <image>`), which
    /// `podman load` in the guest also accepts.
    ///
    /// `image` should be a tagged reference (see [`Self::pull_and_save`]);
    /// only tags survive the save/load round trip.
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

/// `<name>@sha256:<64 lowercase hex>` on a single line, where `<name>` is a
/// non-empty reference with no whitespace or `@`.
fn is_repo_digest(s: &str) -> bool {
    let Some((name, digest)) = s.rsplit_once('@') else {
        return false;
    };
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return false;
    };
    !name.is_empty()
        && !name.contains(|c: char| c.is_whitespace() || c == '@')
        && hex.len() == 64
        && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    #[test]
    fn repo_digest_format() {
        let good = format!("docker.io/library/nginx@sha256:{}", "ab".repeat(32));
        assert!(is_repo_digest(&good));
        assert!(is_repo_digest(&format!(
            "localhost:5000/x/y@sha256:{}",
            "0".repeat(64)
        )));
        for bad in [
            String::new(),
            "nginx".to_string(),
            format!("@sha256:{}", "ab".repeat(32)),
            format!("nginx@sha256:{}", "ab".repeat(31)),
            format!("nginx@sha256:{}", "AB".repeat(32)),
            format!("nginx@sha512:{}", "ab".repeat(32)),
            format!(
                "WARN: something\ndocker.io/library/nginx@sha256:{}",
                "ab".repeat(32)
            ),
            format!("nginx@sha256:{} trailing", "ab".repeat(32)),
        ] {
            assert!(!is_repo_digest(&bad), "{bad:?} should be rejected");
        }
    }

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

    #[cfg(unix)]
    #[test]
    fn find_in_prefers_podman_over_docker_on_the_given_path() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        for name in ["podman", "docker"] {
            let p = dir.path().join(name);
            std::fs::write(&p, "#!/bin/sh\nexit 0\n").unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let tool = ContainerTool::find_in(Some(dir.path())).unwrap();
        assert_eq!(tool.flavor, Flavor::Podman);
        assert_eq!(tool.path, dir.path().join("podman"));

        std::fs::remove_file(dir.path().join("podman")).unwrap();
        let tool = ContainerTool::find_in(Some(dir.path())).unwrap();
        assert_eq!(tool.flavor, Flavor::Docker);
    }

    #[test]
    fn find_reports_not_found_when_path_is_empty() {
        // Inject the search path rather than mutating the process-wide PATH:
        // tests run in parallel, and the other `find_*` tests in this crate
        // do the same lookup, so an env round-trip races and flakes.
        let result = ContainerTool::find_in(Some(""));
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
                "#!/bin/sh\necho \"$@\" >> {}\nprintf 'docker.io/library/nginx@sha256:{}\\n'\n",
                argv_log.display(),
                "ab".repeat(32)
            ),
        );

        let tool = ContainerTool {
            path: fake,
            flavor: Flavor::Podman,
        };
        let digest = retry_text_file_busy(|| tool.resolve_digest("nginx:1.27"))
            .await
            .unwrap();
        assert_eq!(
            digest,
            format!("docker.io/library/nginx@sha256:{}", "ab".repeat(32))
        );

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
    async fn resolve_digest_rejects_output_that_is_not_a_digest() {
        let dir = tempfile::tempdir().unwrap();
        let fake = write_fake_tool(
            dir.path(),
            &format!(
                "#!/bin/sh\nprintf 'WARN: storage.conf is deprecated\\ndocker.io/library/nginx@sha256:{}\\n'\n",
                "ab".repeat(32)
            ),
        );
        let tool = ContainerTool {
            path: fake,
            flavor: Flavor::Podman,
        };
        let err = retry_text_file_busy(|| tool.resolve_digest("nginx:1.27"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, ContainerError::MalformedDigest { ref image, .. } if image == "nginx:1.27"),
            "{err}"
        );
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

    /// Regression test for #348: the archive must be saved from the ORIGINAL
    /// reference. `docker save name@sha256:...` writes `RepoTags: null` and
    /// docker-archives carry no RepoDigests, so `podman load` in the guest
    /// imports a bare image ID that compose cannot match against `image:`.
    #[cfg(unix)]
    #[tokio::test]
    async fn pull_and_save_saves_from_the_original_reference_not_the_digest() {
        let dir = tempfile::tempdir().unwrap();
        let argv_log = dir.path().join("argv");
        let digest = format!("docker.io/library/nginx@sha256:{}", "ab".repeat(32));
        let fake = write_fake_tool(
            dir.path(),
            &format!(
                "#!/bin/sh\necho \"$@\" >> {}\ncase \"$1\" in inspect|image) printf '{digest}\\n';; esac\nexit 0\n",
                argv_log.display()
            ),
        );

        let tool = ContainerTool {
            path: fake,
            flavor: Flavor::Docker,
        };
        let out = dir.path().join("nginx.tar");
        let resolved = retry_text_file_busy(|| tool.pull_and_save("nginx:1.27", &out))
            .await
            .unwrap();
        assert_eq!(resolved, digest);

        let argv = std::fs::read_to_string(&argv_log).unwrap();
        let lines: Vec<&str> = argv.lines().collect();
        assert_eq!(lines[0], "pull nginx:1.27");
        assert_eq!(
            lines[1],
            "image inspect --format {{index .RepoDigests 0}} nginx:1.27"
        );
        assert_eq!(
            lines[2],
            format!("save -o {} nginx:1.27", out.to_str().unwrap())
        );
        assert!(
            !lines[2].contains("@sha256:"),
            "save must not receive the digest-pinned reference: {}",
            lines[2]
        );
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
