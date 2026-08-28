//! Docker Compose subset validation and workload staging for
//! `vprogram create --compose`.
//!
//! The subset is closed: every key is either accepted, rejected with a
//! specific reason, or a schema error. Loosened deliberately, never
//! accidentally (mirrors the V-PROGRAM message format's philosophy).

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_norway::Value;

use crate::mkfs::{MkfsError, MkfsExt4};

#[derive(Debug, thiserror::Error)]
pub enum ComposeError {
    #[error("compose file is not valid YAML or uses an unsupported key: {0}")]
    Yaml(#[from] serde_norway::Error),
    #[error("compose file defines no services")]
    NoServices,
    #[error("service {0:?} has no image; every service must reference a prebuilt image")]
    MissingImage(String),
    #[error("service {service:?}: `{key}` is not supported: {reason}")]
    RejectedKey {
        service: String,
        key: &'static str,
        reason: &'static str,
    },
    #[error("top-level `{key}` is not supported: {reason}")]
    RejectedTopLevel {
        key: &'static str,
        reason: &'static str,
    },
    #[error(
        "service {service:?} must set `network_mode: host` (found {found:?}); \
         v1 stacks share the guest network namespace and talk over localhost"
    )]
    BadNetworkMode { service: String, found: String },
    #[error(
        "service {service:?} references image {image:?} with a malformed \
         digest; a digest reference must be `name@sha256:<64 lowercase hex>`"
    )]
    BadDigestImage { service: String, image: String },
    #[error("service {service:?}: volume bind {entry:?}: {reason}")]
    BadVolumeBind {
        service: String,
        entry: String,
        reason: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComposeFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub services: BTreeMap<String, ComposeService>,
    // Known-rejected top-level keys, captured so validation can explain
    // instead of serde reporting "unknown field".
    #[serde(default, skip_serializing)]
    volumes: Option<Value>,
    #[serde(default, skip_serializing)]
    networks: Option<Value>,
    #[serde(default, skip_serializing)]
    secrets: Option<Value>,
    #[serde(default, skip_serializing)]
    configs: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComposeService {
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmpfs: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart: Option<String>,
    /// Verified-volume binds: `/volumes/<i>[/subpath]:<target>:ro` short
    /// syntax only, validated by `volume_bind_checks`. Serialized (unlike
    /// the rejected keys) so accepted binds reach the guest's compose file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<String>>,
    // Known-rejected service keys.
    #[serde(default, skip_serializing)]
    build: Option<Value>,
    #[serde(default, skip_serializing)]
    ports: Option<Value>,
    #[serde(default, skip_serializing)]
    secrets: Option<Value>,
    #[serde(default, skip_serializing)]
    env_file: Option<Value>,
    #[serde(default, skip_serializing)]
    privileged: Option<Value>,
    #[serde(default, skip_serializing)]
    devices: Option<Value>,
    #[serde(default, skip_serializing)]
    cap_add: Option<Value>,
}

#[derive(Debug)]
pub struct ValidatedCompose {
    pub file: ComposeFile,
    pub warnings: Vec<String>,
}

/// Parse and validate a compose file against the aleph.compose/1 subset.
/// Pure and I/O-free so it's directly unit-testable.
pub(crate) fn parse_and_validate(
    text: &str,
    declared_volumes: usize,
) -> Result<ValidatedCompose, ComposeError> {
    let file: ComposeFile = serde_norway::from_str(text)?;

    for (key, present, reason) in [
        (
            "volumes",
            file.volumes.is_some(),
            "top-level named volumes are not supported; verified volumes are declared with \
             --volume and bound per-service as /volumes/<i>:<target>:ro",
        ),
        (
            "networks",
            file.networks.is_some(),
            "v1 stacks run with `network_mode: host`; custom networks are not available",
        ),
        (
            "secrets",
            file.secrets.is_some(),
            "there is no unmeasured input channel; secrets would be public and measured",
        ),
        (
            "configs",
            file.configs.is_some(),
            "bake configuration into the image or the environment (both are measured)",
        ),
    ] {
        if present {
            return Err(ComposeError::RejectedTopLevel { key, reason });
        }
    }

    if file.services.is_empty() {
        return Err(ComposeError::NoServices);
    }

    let mut warnings = Vec::new();
    for (name, service) in &file.services {
        service_checks(name, service, declared_volumes)?;
        if service.restart.is_some() {
            warnings.push(format!(
                "service {name:?}: `restart` is accepted but ignored in v1 \
                 (a stack that exits powers the VM off)"
            ));
        }
    }

    Ok(ValidatedCompose { file, warnings })
}

fn service_checks(
    name: &str,
    s: &ComposeService,
    declared_volumes: usize,
) -> Result<(), ComposeError> {
    let rejected: [(&'static str, bool, &'static str); 7] = [
        (
            "build",
            s.build.is_some(),
            "nothing can be built in-guest; reference a prebuilt image",
        ),
        (
            "ports",
            s.ports.is_some(),
            "a V-Program exposes exactly one attested endpoint: the guest agent \
          serves RA-TLS on 8443 and proxies to the service listening on \
          127.0.0.1:8080; direct port publishing is not available",
        ),
        (
            "secrets",
            s.secrets.is_some(),
            "there is no unmeasured input channel; secrets would be public and measured",
        ),
        (
            "env_file",
            s.env_file.is_some(),
            "environment must be inline (it is public and measured); env_file hides that",
        ),
        (
            "privileged",
            s.privileged.is_some(),
            "privileged containers are not available",
        ),
        (
            "devices",
            s.devices.is_some(),
            "host device passthrough is not available",
        ),
        (
            "cap_add",
            s.cap_add.is_some(),
            "extra capabilities are not available",
        ),
    ];
    for (key, present, reason) in rejected {
        if present {
            return Err(ComposeError::RejectedKey {
                service: name.to_string(),
                key,
                reason,
            });
        }
    }
    let image = s.image.as_deref().unwrap_or("");
    if image.is_empty() {
        return Err(ComposeError::MissingImage(name.to_string()));
    }
    // A digest reference is accepted as the user's audited identity claim:
    // the pull enforces it and staging retags it (see `retag_digest_images`).
    // Anything else containing `@` can neither be pulled nor enforced.
    if image.contains('@') && crate::container::split_repo_digest(image).is_none() {
        return Err(ComposeError::BadDigestImage {
            service: name.to_string(),
            image: image.to_string(),
        });
    }
    if let Some(binds) = &s.volumes {
        volume_bind_checks(name, binds, declared_volumes)?;
    }
    match s.network_mode.as_deref() {
        Some("host") => Ok(()),
        other => Err(ComposeError::BadNetworkMode {
            service: name.to_string(),
            found: other.unwrap_or("<unset>").to_string(),
        }),
    }
}

/// aleph.compose/1 verified-volume binds: the only accepted service-level
/// `volumes` entries are short-syntax read-only binds sourced from the
/// guest's verified-volume mounts: `/volumes/<i>[/subpath]:<target>:ro`.
/// `<i>` is the volume's message list order (the CLI's `--volume` flag
/// order) and must reference a volume the message actually declares. The
/// compose file is verity-bound, so an accepted bind is measured intent;
/// everything else about `volumes:` (named volumes, writable binds, host
/// paths) stays rejected.
fn volume_bind_checks(
    name: &str,
    binds: &[String],
    declared_volumes: usize,
) -> Result<(), ComposeError> {
    for entry in binds {
        let reject = |reason: String| ComposeError::BadVolumeBind {
            service: name.to_string(),
            entry: entry.clone(),
            reason,
        };
        let parts: Vec<&str> = entry.split(':').collect();
        let &[source, target, mode] = parts.as_slice() else {
            return Err(reject(
                "must be `/volumes/<i>[/subpath]:<target>:ro` short syntax (ro is required)".into(),
            ));
        };
        if mode != "ro" {
            return Err(reject(
                "mode must be exactly `ro`; verified volumes are read-only".into(),
            ));
        }
        let Some(rel) = source.strip_prefix("/volumes/") else {
            return Err(reject(
                "source must live under /volumes/ (the verified-volume mounts; --volume order is the index)".into(),
            ));
        };
        let mut segments = rel.split('/');
        let Some(index) = segments
            .next()
            .filter(|s| !s.is_empty())
            .and_then(|s| s.parse::<usize>().ok())
        else {
            return Err(reject(
                "source must start with a volume index: /volumes/<i>".into(),
            ));
        };
        if segments.any(|s| s.is_empty() || s == "." || s == "..") {
            return Err(reject(
                "source subpath must not contain empty, `.` or `..` segments".into(),
            ));
        }
        if index >= declared_volumes {
            return Err(reject(format!(
                "references /volumes/{index} but the message declares {declared_volumes} volume(s); \
                 `--volume` order is the index"
            )));
        }
        if !target.starts_with('/') {
            return Err(reject("target must be an absolute path".into()));
        }
    }
    Ok(())
}

/// Unique image references in service-name order.
pub(crate) fn image_names(file: &ComposeFile) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    file.services
        .values()
        .filter_map(|s| s.image.clone())
        .filter(|i| seen.insert(i.clone()))
        .collect()
}

/// The deterministic local tag a digest reference is staged under:
/// `name@sha256:<hex>` becomes `name:sha256-<hex>`. The full digest is kept
/// in the tag (a tag may be up to 128 characters, `sha256-` plus 64 hex
/// fits) so the measured compose file still carries the audited identity
/// verbatim. `None` for tag references, which are staged as-is.
pub(crate) fn pinned_tag(image: &str) -> Option<String> {
    crate::container::split_repo_digest(image).map(|(name, hex)| format!("{name}:sha256-{hex}"))
}

/// Rewrite every digest-referenced service image to its [`pinned_tag`], the
/// name `podman load` restores from the archive saved under that same tag
/// (#348); tag references stay verbatim. Run after pulling and before
/// [`to_yaml`], so the staged compose file matches the staged archives.
pub(crate) fn retag_digest_images(file: &mut ComposeFile) {
    for service in file.services.values_mut() {
        if let Some(image) = service.image.as_mut()
            && let Some(tag) = pinned_tag(image)
        {
            *image = tag;
        }
    }
}

/// Serialize the validated compose file for staging. Tag references are
/// written verbatim and digest references as their [`pinned_tag`]: in both
/// cases the archive saved alongside carries that same tag, which is what
/// `podman load` in the guest restores and compose matches against (#348).
/// The exact image bytes are pinned by the verity-measured workload volume,
/// not by the name.
pub(crate) fn to_yaml(file: &ComposeFile) -> Result<String, ComposeError> {
    Ok(serde_norway::to_string(file)?)
}

#[derive(Debug, thiserror::Error)]
pub enum ComposeBuildError {
    #[error(transparent)]
    Mkfs(#[from] MkfsError),
    #[error("failed to stage compose workload image: {0}")]
    Io(#[from] std::io::Error),
}

/// Content size -> image size: content plus slack (max of 20% of content or
/// 16 MiB), rounded up to a 4096-byte block boundary. Pure for unit testing.
pub(crate) fn image_size_bytes(content_bytes: u64) -> u64 {
    const MIN_SLACK: u64 = 16 * 1024 * 1024;
    const BLOCK: u64 = 4096;
    let slack = (content_bytes / 5).max(MIN_SLACK);
    (content_bytes + slack).div_ceil(BLOCK) * BLOCK
}

/// Sanitize an image name for use as a filename component: lowercase,
/// keep `[a-z0-9-]`, replace everything else with `-`. Filenames are
/// informational per the `aleph.compose/1` contract.
fn sanitize_image_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Stage the pinned compose file and image archives into the
/// `aleph.compose/1` layout (`docker-compose.yml` at the root, archives under
/// `images/NNN-<sanitized-name>.tar`) and build an ext4 workload image from
/// it via `mkfs`.
///
/// The image is written inside a dedicated [`tempfile::TempDir`] (not the
/// staging dir, which is dropped at the end of this function, and not the
/// bare system temp dir) so that `verity_format`'s `<file_name>.verity`
/// sidecar - written next to the image by the caller - lands in that same
/// directory. Returns the directory together with the image file; the caller
/// must keep both alive (the directory removes its contents, sidecar
/// included, on drop) until after upload.
pub(crate) async fn build_workload_image(
    mkfs: &MkfsExt4,
    pinned_yaml: &str,
    archives: &[(String, PathBuf)],
) -> Result<(tempfile::TempDir, tempfile::NamedTempFile), ComposeBuildError> {
    let staging = tempfile::tempdir()?;

    let compose_path = staging.path().join("docker-compose.yml");
    std::fs::write(&compose_path, pinned_yaml)?;
    set_fixed_mtime(&compose_path)?;
    let mut content_bytes = pinned_yaml.len() as u64;

    let images_dir = staging.path().join("images");
    std::fs::create_dir(&images_dir)?;
    for (index, (name, archive_path)) in archives.iter().enumerate() {
        let dest = images_dir.join(format!("{index:03}-{}.tar", sanitize_image_name(name)));
        std::fs::copy(archive_path, &dest)?;
        set_fixed_mtime(&dest)?;
        content_bytes += std::fs::metadata(&dest)?.len();
    }
    set_fixed_mtime(&images_dir)?;
    set_fixed_mtime(staging.path())?;

    let output_dir = tempfile::tempdir()?;
    let out = tempfile::Builder::new()
        .prefix("aleph-compose-")
        .suffix(".ext4")
        .tempfile_in(output_dir.path())?;
    out.as_file().set_len(image_size_bytes(content_bytes))?;

    mkfs.build(staging.path(), out.path()).await?;

    Ok((output_dir, out))
}

/// Pin a staged file or directory's mtime (and atime) to the Unix epoch so
/// two builds of the same content produce byte-identical staging input for
/// `mkfs.ext4`, and in turn a byte-identical, independently reproducible
/// image (see `MkfsExt4::build`'s doc comment for the filesystem-level half
/// of this).
fn set_fixed_mtime(path: &std::path::Path) -> std::io::Result<()> {
    let epoch = std::fs::FileTimes::new()
        .set_accessed(std::time::SystemTime::UNIX_EPOCH)
        .set_modified(std::time::SystemTime::UNIX_EPOCH);
    // A read-only open is enough: `set_times` checks file ownership at
    // syscall time, not the fd's open mode, and this process owns every
    // path passed here (it just created them under the staging tempdir).
    // Works for both regular files and directories on Unix.
    std::fs::File::open(path)?.set_times(epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = "services:\n  web:\n    image: nginx:1.27\n    network_mode: host\n";

    fn svc(extra: &str) -> String {
        format!("services:\n  web:\n    image: nginx:1.27\n    network_mode: host\n{extra}")
    }

    #[test]
    fn accepts_the_full_supported_subset() {
        let text = "services:\n  web:\n    image: nginx:1.27\n    network_mode: host\n    \
                    command: [\"nginx\", \"-g\", \"daemon off;\"]\n    entrypoint: /entry.sh\n    \
                    environment:\n      A: b\n    depends_on: [db]\n    tmpfs:\n      - /scratch\n  \
                    db:\n    image: postgres:16\n    network_mode: host\n";
        let v = parse_and_validate(text, 0).unwrap();
        assert_eq!(v.file.services.len(), 2);
        assert!(v.warnings.is_empty());
    }

    #[test]
    fn restart_is_accepted_with_a_warning() {
        let v = parse_and_validate(&svc("    restart: always\n"), 0).unwrap();
        assert_eq!(v.warnings.len(), 1);
        assert!(v.warnings[0].contains("restart"));
    }

    #[test]
    fn rejects_build() {
        let err = parse_and_validate(&svc("    build: .\n"), 0).unwrap_err();
        assert!(err.to_string().contains("build"), "{err}");
    }

    #[test]
    fn accepts_a_verified_volume_bind_and_serializes_it() {
        let text = svc("    volumes:\n      - /volumes/0:/weights:ro\n");
        let validated = parse_and_validate(&text, 1).unwrap();
        let yaml = to_yaml(&validated.file).unwrap();
        assert!(
            yaml.contains("/volumes/0:/weights:ro"),
            "the bind must survive re-serialization into the workload compose file: {yaml}"
        );
    }

    #[test]
    fn accepts_a_subpath_bind() {
        let text = svc("    volumes:\n      - /volumes/1/model/q4:/weights:ro\n");
        parse_and_validate(&text, 2).unwrap();
    }

    #[test]
    fn rejects_a_bind_past_the_declared_volume_count() {
        let text = svc("    volumes:\n      - /volumes/2:/weights:ro\n");
        let err = parse_and_validate(&text, 2).unwrap_err();
        assert!(err.to_string().contains("declares 2 volume"), "{err}");
    }

    #[test]
    fn rejects_a_writable_bind() {
        for entry in ["/volumes/0:/weights:rw", "/volumes/0:/weights"] {
            let text = svc(&format!("    volumes:\n      - {entry}\n"));
            let err = parse_and_validate(&text, 1).unwrap_err();
            assert!(err.to_string().contains("ro"), "{err}");
        }
    }

    #[test]
    fn rejects_a_bind_outside_the_volume_mounts() {
        let err =
            parse_and_validate(&svc("    volumes:\n      - /data:/weights:ro\n"), 1).unwrap_err();
        assert!(err.to_string().contains("/volumes/"), "{err}");
    }

    #[test]
    fn rejects_a_traversing_bind() {
        let err = parse_and_validate(
            &svc("    volumes:\n      - /volumes/0/../1:/weights:ro\n"),
            2,
        )
        .unwrap_err();
        assert!(err.to_string().contains("segments"), "{err}");
    }

    #[test]
    fn rejects_a_relative_bind_target() {
        let err = parse_and_validate(&svc("    volumes:\n      - /volumes/0:weights:ro\n"), 1)
            .unwrap_err();
        assert!(err.to_string().contains("absolute"), "{err}");
    }

    #[test]
    fn rejects_long_syntax_volume_entries() {
        // Mapping-form entries fail at deserialization (Vec<String>), which
        // is fine: the accepted grammar is the short string syntax only.
        let text = svc(
            "    volumes:\n      - type: bind\n        source: /volumes/0\n        target: /w\n",
        );
        assert!(parse_and_validate(&text, 1).is_err());
    }

    #[test]
    fn rejects_named_volume_entries() {
        let err =
            parse_and_validate(&svc("    volumes:\n      - data:/var/lib:ro\n"), 1).unwrap_err();
        assert!(err.to_string().contains("/volumes/"), "{err}");
    }

    #[test]
    fn rejects_ports_and_explains_the_entrypoint_contract() {
        let err = parse_and_validate(&svc("    ports:\n      - \"80:80\"\n"), 0).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ports") && msg.contains("8080"), "{msg}");
    }

    #[test]
    fn rejects_secrets() {
        assert!(parse_and_validate(&svc("    secrets: [s]\n"), 0).is_err());
    }
    #[test]
    fn rejects_env_file() {
        assert!(parse_and_validate(&svc("    env_file: .env\n"), 0).is_err());
    }
    #[test]
    fn rejects_privileged() {
        assert!(parse_and_validate(&svc("    privileged: true\n"), 0).is_err());
    }
    #[test]
    fn rejects_devices() {
        assert!(parse_and_validate(&svc("    devices: [\"/dev/kvm\"]\n"), 0).is_err());
    }
    #[test]
    fn rejects_cap_add() {
        assert!(parse_and_validate(&svc("    cap_add: [NET_ADMIN]\n"), 0).is_err());
    }

    #[test]
    fn rejects_unknown_service_keys() {
        let err = parse_and_validate(&svc("    gpus: all\n"), 0).unwrap_err();
        assert!(err.to_string().contains("gpus"), "{err}");
    }

    #[test]
    fn rejects_top_level_volumes() {
        let text = format!("{MINIMAL}volumes:\n  data: {{}}\n");
        assert!(parse_and_validate(&text, 0).is_err());
    }

    #[test]
    fn rejects_a_service_without_an_image() {
        let text = "services:\n  web:\n    network_mode: host\n";
        let err = parse_and_validate(text, 0).unwrap_err();
        assert!(err.to_string().contains("image"), "{err}");
    }

    #[test]
    fn requires_host_network_mode_on_every_service() {
        let text = "services:\n  web:\n    image: nginx:1.27\n";
        let err = parse_and_validate(text, 0).unwrap_err();
        assert!(err.to_string().contains("network_mode"), "{err}");
        let text = "services:\n  web:\n    image: nginx:1.27\n    network_mode: bridge\n";
        assert!(parse_and_validate(text, 0).is_err());
    }

    /// #348: a digest-form `image:` is the user's audited identity claim and
    /// must be accepted; the pull enforces it and staging retags it.
    #[test]
    fn accepts_a_well_formed_digest_image_reference() {
        let text = format!(
            "services:\n  web:\n    image: nginx@sha256:{}\n    network_mode: host\n",
            "ab".repeat(32)
        );
        parse_and_validate(&text, 0).unwrap();
    }

    /// An `image:` containing `@` that is not `name@sha256:<64 hex>` can
    /// neither be pulled nor enforced; refuse it with a specific error.
    #[test]
    fn rejects_a_malformed_digest_image_reference() {
        for bad in [
            format!("nginx@sha256:{}", "ab".repeat(31)),
            format!("nginx@sha512:{}", "ab".repeat(32)),
            format!("nginx@sha256:{}", "AB".repeat(32)),
            format!("@sha256:{}", "ab".repeat(32)),
            "nginx@latest".to_string(),
        ] {
            let text = format!("services:\n  web:\n    image: \"{bad}\"\n    network_mode: host\n");
            let err = parse_and_validate(&text, 0).unwrap_err();
            assert!(
                matches!(err, ComposeError::BadDigestImage { ref service, .. } if service == "web"),
                "{bad}: {err}"
            );
            assert!(err.to_string().contains("sha256"), "{err}");
        }
    }

    /// The pinned tag is a pure function of the digest reference and embeds
    /// the full digest, so the measured compose file keeps the audited
    /// identity visible and two builds stage identical bytes.
    #[test]
    fn pinned_tag_embeds_the_full_digest_and_ignores_tag_references() {
        let hex = "ab".repeat(32);
        assert_eq!(
            pinned_tag(&format!("nginx@sha256:{hex}")).as_deref(),
            Some(format!("nginx:sha256-{hex}").as_str())
        );
        assert_eq!(
            pinned_tag(&format!("localhost:5000/x/y@sha256:{hex}")).as_deref(),
            Some(format!("localhost:5000/x/y:sha256-{hex}").as_str())
        );
        assert_eq!(pinned_tag("nginx:1.27"), None);
        assert_eq!(pinned_tag("nginx"), None);
    }

    /// Digest references are rewritten to their pinned tag in the staged
    /// compose file - the name `podman load` restores from the archive saved
    /// under that same tag - while plain tags stay verbatim.
    #[test]
    fn retag_digest_images_rewrites_services_to_pinned_tags() {
        let hex = "ab".repeat(32);
        let text = format!(
            "services:\n  db:\n    image: postgres:16\n    network_mode: host\n  \
             web:\n    image: nginx@sha256:{hex}\n    network_mode: host\n"
        );
        let mut v = parse_and_validate(&text, 0).unwrap();
        retag_digest_images(&mut v.file);
        let yaml = to_yaml(&v.file).unwrap();
        assert!(
            yaml.contains(&format!("image: nginx:sha256-{hex}")),
            "{yaml}"
        );
        assert!(yaml.contains("image: postgres:16"), "{yaml}");
        assert!(!yaml.contains('@'), "{yaml}");
        parse_and_validate(&yaml, 0).unwrap();
    }

    #[test]
    fn rejects_an_empty_services_map() {
        assert!(parse_and_validate("services: {}\n", 0).is_err());
    }

    #[test]
    fn version_and_name_are_tolerated() {
        let text = format!("version: \"3.9\"\nname: demo\n{MINIMAL}");
        parse_and_validate(&text, 0).unwrap();
    }

    /// #348: the staged compose file keeps the original tag so it matches
    /// what `podman load` restores from the saved archive.
    #[test]
    fn to_yaml_keeps_image_references_verbatim() {
        let v = parse_and_validate(MINIMAL, 0).unwrap();
        let yaml = to_yaml(&v.file).unwrap();
        assert!(yaml.contains("image: nginx:1.27"), "{yaml}");
        assert!(!yaml.contains("@sha256:"), "{yaml}");
    }

    #[test]
    fn serialized_yaml_round_trips_through_the_validator() {
        let v = parse_and_validate(MINIMAL, 0).unwrap();
        parse_and_validate(&to_yaml(&v.file).unwrap(), 0).unwrap();
    }

    #[test]
    fn image_size_adds_slack_and_rounds_to_blocks() {
        assert_eq!(image_size_bytes(0) % 4096, 0);
        assert!(image_size_bytes(0) >= 16 * 1024 * 1024);
        let one_gib = 1 << 30;
        assert!(image_size_bytes(one_gib) >= one_gib + (one_gib / 5));
        assert_eq!(image_size_bytes(one_gib) % 4096, 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn build_workload_image_stages_compose_and_archives() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let snapshot_dir = dir.path().join("snapshot");
        let fake = dir.path().join("mkfs.ext4");
        std::fs::write(
            &fake,
            format!(
                "#!/bin/sh\ncp -r \"$8\" {}\nexit 0\n",
                snapshot_dir.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let debugfs = dir.path().join("debugfs");
        std::fs::write(&debugfs, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&debugfs, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mkfs = MkfsExt4 {
            path: fake,
            debugfs,
        };

        let archive_path = dir.path().join("nginx.tar");
        let archive_bytes = b"fake tar bytes";
        std::fs::write(&archive_path, archive_bytes).unwrap();

        let pinned_yaml = "services:\n  web:\n    image: nginx@sha256:abc\n";
        let archives = vec![("nginx".to_string(), archive_path)];

        let (_dir, _out) = build_workload_image(&mkfs, pinned_yaml, &archives)
            .await
            .unwrap();

        let staged_compose =
            std::fs::read_to_string(snapshot_dir.join("docker-compose.yml")).unwrap();
        assert_eq!(staged_compose, pinned_yaml);

        let staged_archive =
            std::fs::read(snapshot_dir.join("images").join("000-nginx.tar")).unwrap();
        assert_eq!(staged_archive, archive_bytes);
    }

    /// Two builds of the same staged content must be byte-identical, so a
    /// third party can rebuild a workload from a published compose file and
    /// verify the published measurement. Requires a real `mkfs.ext4` (the
    /// fixed UUID/hash-seed/timestamp flags only matter against the real
    /// tool); skips silently when it's not on PATH rather than failing CI
    /// images that lack e2fsprogs.
    #[tokio::test]
    async fn build_workload_image_is_reproducible() {
        let Ok(mkfs) = MkfsExt4::find() else {
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        let archive_path = dir.path().join("nginx.tar");
        std::fs::write(&archive_path, b"fake tar bytes for determinism check").unwrap();

        let pinned_yaml = "services:\n  web:\n    image: nginx@sha256:abc\n";
        let archives = vec![("nginx".to_string(), archive_path)];

        let (_first_dir, first) = build_workload_image(&mkfs, pinned_yaml, &archives)
            .await
            .unwrap();
        // Cross a wall-clock second boundary between the two builds: staged
        // files' ctime (which userspace cannot set) differs across it, and
        // that is exactly what the build must normalize away. Without this
        // the test only caught the bug when the builds happened to straddle
        // a second by chance.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let (_second_dir, second) = build_workload_image(&mkfs, pinned_yaml, &archives)
            .await
            .unwrap();

        let first_bytes = std::fs::read(first.path()).unwrap();
        let second_bytes = std::fs::read(second.path()).unwrap();

        use sha2::{Digest, Sha256};
        let first_hash = Sha256::digest(&first_bytes);
        let second_hash = Sha256::digest(&second_bytes);
        assert_eq!(
            first_hash, second_hash,
            "two builds of identical staged content produced different images; \
             the build is not reproducible"
        );
    }
}
