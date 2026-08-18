//! Docker Compose subset validation and workload staging for
//! `vprogram create --compose`.
//!
//! The subset is closed: every key is either accepted, rejected with a
//! specific reason, or a schema error. Loosened deliberately, never
//! accidentally (mirrors the V-PROGRAM message format's philosophy).

// This module has no non-test consumers yet: `aleph vprogram create --compose`
// lands in a later task (Task 6), which is what will actually call
// `parse_and_validate` and construct these types from the CLI.
#![allow(dead_code)]

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
    #[error("no digest or archive resolved for image {0:?}")]
    UnpinnedImage(String),
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
    // Known-rejected service keys.
    #[serde(default, skip_serializing)]
    build: Option<Value>,
    #[serde(default, skip_serializing)]
    volumes: Option<Value>,
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
pub(crate) fn parse_and_validate(text: &str) -> Result<ValidatedCompose, ComposeError> {
    let file: ComposeFile = serde_norway::from_str(text)?;

    for (key, present, reason) in [
        (
            "volumes",
            file.volumes.is_some(),
            "persistent volumes are not supported in v1; use service-level `tmpfs:` for scratch space",
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
        service_checks(name, service)?;
        if service.restart.is_some() {
            warnings.push(format!(
                "service {name:?}: `restart` is accepted but ignored in v1 \
                 (a stack that exits powers the VM off)"
            ));
        }
    }

    Ok(ValidatedCompose { file, warnings })
}

fn service_checks(name: &str, s: &ComposeService) -> Result<(), ComposeError> {
    let rejected: [(&'static str, bool, &'static str); 8] = [
        (
            "build",
            s.build.is_some(),
            "nothing can be built in-guest; reference a prebuilt image",
        ),
        (
            "volumes",
            s.volumes.is_some(),
            "persistent volumes are not supported in v1; `tmpfs:` mounts are",
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
    if s.image.as_deref().unwrap_or("").is_empty() {
        return Err(ComposeError::MissingImage(name.to_string()));
    }
    match s.network_mode.as_deref() {
        Some("host") => Ok(()),
        other => Err(ComposeError::BadNetworkMode {
            service: name.to_string(),
            found: other.unwrap_or("<unset>").to_string(),
        }),
    }
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

/// Rewrite every service's image to its digest-pinned form so the measured
/// compose file names exact image identities, not floating tags.
pub(crate) fn pin_images(
    file: &mut ComposeFile,
    pins: &BTreeMap<String, String>,
) -> Result<(), ComposeError> {
    for service in file.services.values_mut() {
        let image = service.image.as_mut().expect("validated: image present");
        match pins.get(image) {
            Some(pinned) => *image = pinned.clone(),
            None => return Err(ComposeError::UnpinnedImage(image.clone())),
        }
    }
    Ok(())
}

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
/// it via `mkfs`. Returns the tempfile; the caller keeps it alive until after
/// upload.
pub(crate) async fn build_workload_image(
    mkfs: &MkfsExt4,
    pinned_yaml: &str,
    archives: &[(String, PathBuf)],
) -> Result<tempfile::NamedTempFile, ComposeBuildError> {
    let staging = tempfile::tempdir()?;

    let compose_path = staging.path().join("docker-compose.yml");
    std::fs::write(&compose_path, pinned_yaml)?;
    let mut content_bytes = pinned_yaml.len() as u64;

    let images_dir = staging.path().join("images");
    std::fs::create_dir(&images_dir)?;
    for (index, (name, archive_path)) in archives.iter().enumerate() {
        let dest = images_dir.join(format!("{index:03}-{}.tar", sanitize_image_name(name)));
        std::fs::copy(archive_path, &dest)?;
        content_bytes += std::fs::metadata(&dest)?.len();
    }

    let out = tempfile::Builder::new()
        .prefix("aleph-compose-")
        .suffix(".ext4")
        .tempfile()?;
    out.as_file().set_len(image_size_bytes(content_bytes))?;

    mkfs.build(staging.path(), out.path()).await?;

    Ok(out)
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
        let v = parse_and_validate(text).unwrap();
        assert_eq!(v.file.services.len(), 2);
        assert!(v.warnings.is_empty());
    }

    #[test]
    fn restart_is_accepted_with_a_warning() {
        let v = parse_and_validate(&svc("    restart: always\n")).unwrap();
        assert_eq!(v.warnings.len(), 1);
        assert!(v.warnings[0].contains("restart"));
    }

    #[test]
    fn rejects_build() {
        let err = parse_and_validate(&svc("    build: .\n")).unwrap_err();
        assert!(err.to_string().contains("build"), "{err}");
    }

    #[test]
    fn rejects_volumes() {
        let err = parse_and_validate(&svc("    volumes:\n      - data:/var/lib\n")).unwrap_err();
        assert!(err.to_string().contains("volumes"), "{err}");
    }

    #[test]
    fn rejects_ports_and_explains_the_entrypoint_contract() {
        let err = parse_and_validate(&svc("    ports:\n      - \"80:80\"\n")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ports") && msg.contains("8080"), "{msg}");
    }

    #[test]
    fn rejects_secrets() {
        assert!(parse_and_validate(&svc("    secrets: [s]\n")).is_err());
    }
    #[test]
    fn rejects_env_file() {
        assert!(parse_and_validate(&svc("    env_file: .env\n")).is_err());
    }
    #[test]
    fn rejects_privileged() {
        assert!(parse_and_validate(&svc("    privileged: true\n")).is_err());
    }
    #[test]
    fn rejects_devices() {
        assert!(parse_and_validate(&svc("    devices: [\"/dev/kvm\"]\n")).is_err());
    }
    #[test]
    fn rejects_cap_add() {
        assert!(parse_and_validate(&svc("    cap_add: [NET_ADMIN]\n")).is_err());
    }

    #[test]
    fn rejects_unknown_service_keys() {
        let err = parse_and_validate(&svc("    gpus: all\n")).unwrap_err();
        assert!(err.to_string().contains("gpus"), "{err}");
    }

    #[test]
    fn rejects_top_level_volumes() {
        let text = format!("{MINIMAL}volumes:\n  data: {{}}\n");
        assert!(parse_and_validate(&text).is_err());
    }

    #[test]
    fn rejects_a_service_without_an_image() {
        let text = "services:\n  web:\n    network_mode: host\n";
        let err = parse_and_validate(text).unwrap_err();
        assert!(err.to_string().contains("image"), "{err}");
    }

    #[test]
    fn requires_host_network_mode_on_every_service() {
        let text = "services:\n  web:\n    image: nginx:1.27\n";
        let err = parse_and_validate(text).unwrap_err();
        assert!(err.to_string().contains("network_mode"), "{err}");
        let text = "services:\n  web:\n    image: nginx:1.27\n    network_mode: bridge\n";
        assert!(parse_and_validate(text).is_err());
    }

    #[test]
    fn rejects_an_empty_services_map() {
        assert!(parse_and_validate("services: {}\n").is_err());
    }

    #[test]
    fn version_and_name_are_tolerated() {
        let text = format!("version: \"3.9\"\nname: demo\n{MINIMAL}");
        parse_and_validate(&text).unwrap();
    }

    #[test]
    fn pin_images_rewrites_every_reference() {
        let mut v = parse_and_validate(MINIMAL).unwrap();
        let pins = BTreeMap::from([(
            "nginx:1.27".to_string(),
            "docker.io/library/nginx@sha256:abc123".to_string(),
        )]);
        pin_images(&mut v.file, &pins).unwrap();
        let yaml = to_yaml(&v.file).unwrap();
        assert!(yaml.contains("docker.io/library/nginx@sha256:abc123"));
        assert!(!yaml.contains("nginx:1.27"));
    }

    #[test]
    fn pin_images_fails_on_a_missing_pin() {
        let mut v = parse_and_validate(MINIMAL).unwrap();
        let err = pin_images(&mut v.file, &BTreeMap::new()).unwrap_err();
        assert!(err.to_string().contains("nginx:1.27"), "{err}");
    }

    #[test]
    fn rewritten_yaml_round_trips_through_the_validator() {
        let mut v = parse_and_validate(MINIMAL).unwrap();
        let pins = BTreeMap::from([(
            "nginx:1.27".to_string(),
            "docker.io/library/nginx@sha256:abc123".to_string(),
        )]);
        pin_images(&mut v.file, &pins).unwrap();
        parse_and_validate(&to_yaml(&v.file).unwrap()).unwrap();
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
                "#!/bin/sh\ncp -r \"$4\" {}\nexit 0\n",
                snapshot_dir.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mkfs = MkfsExt4 { path: fake };

        let archive_path = dir.path().join("nginx.tar");
        let archive_bytes = b"fake tar bytes";
        std::fs::write(&archive_path, archive_bytes).unwrap();

        let pinned_yaml = "services:\n  web:\n    image: nginx@sha256:abc\n";
        let archives = vec![("nginx".to_string(), archive_path)];

        let _out = build_workload_image(&mkfs, pinned_yaml, &archives)
            .await
            .unwrap();

        let staged_compose =
            std::fs::read_to_string(snapshot_dir.join("docker-compose.yml")).unwrap();
        assert_eq!(staged_compose, pinned_yaml);

        let staged_archive =
            std::fs::read(snapshot_dir.join("images").join("000-nginx.tar")).unwrap();
        assert_eq!(staged_archive, archive_bytes);
    }
}
