//! Typed model for aleph-instance-runtime/1, the manifest format SEV-SNP
//! confidential instances boot from.
//!
//! Structural twin of the `vprogram` flavor's runtime manifest: same serde
//! idioms (deny unknown fields, `ref` renamed to `reference`), reusing that
//! module's `SourceInfo` and `AttestationDescriptor` types and its shared
//! `ManifestError` rather than redefining them.

use crate::vprogram::manifest::{
    AttestationDescriptor, GpuArchSpec, MAX_BUNDLE_SIZE, ManifestError, SourceInfo,
    validate_driver_version, validate_gpu_archs, validate_gpu_vendor,
};
use serde::Deserialize;
use std::collections::BTreeMap;

pub const INSTANCE_MANIFEST_FORMAT: &str = "aleph-instance-runtime";
pub const INSTANCE_MANIFEST_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceRuntimeManifest {
    pub format: String,
    pub format_version: u32,
    pub name: String,
    pub version: String,
    pub platform: String,
    pub bundle: InstanceBundleRef,
    pub boot: InstanceBootSpec,
    pub attestation: Vec<AttestationDescriptor>,
    /// Provenance of the bundle, as recorded by the publisher (not verified
    /// by anything: the bundle's own sha256 is what pins the runtime).
    pub source: SourceInfo,
    /// Confidential GPU requirements, absent on CPU-only runtimes.
    #[serde(default)]
    pub gpu: Option<InstanceGpuRuntimeSpec>,
}

/// Confidential GPU spec pinned by the instance runtime manifest.
/// Structural twin of `vprogram::manifest::GpuRuntimeSpec` minus `library_path`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceGpuRuntimeSpec {
    pub vendor: String,
    pub driver_version: String,
    pub archs: BTreeMap<String, GpuArchSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceBundleRef {
    /// STORE message hash pinning the bundle tarball.
    #[serde(rename = "ref")]
    pub reference: String,
    pub sha256: String,
    pub size: u64,
    pub members: InstanceBundleMembers,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceBundleMembers {
    pub ovmf: String,
    pub kernel: String,
    pub initrd: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceBootSpec {
    pub method: String,
    pub kernel_hashes: bool,
    pub cpu_models: Vec<String>,
    pub cmdline_template: String,
}

fn is_lowercase_hex_64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// A safe bundle member path: non-empty, relative (no leading `/`), and
/// free of `..` components (so a crafted manifest can't steer extraction
/// outside the bundle directory).
fn check_member_path(role: &'static str, value: &str) -> Result<(), ManifestError> {
    let safe = !value.is_empty()
        && !value.starts_with('/')
        && value.split('/').all(|segment| segment != "..");
    if safe {
        Ok(())
    } else {
        Err(ManifestError::UnsafeMemberPath {
            role,
            value: value.to_string(),
        })
    }
}

/// Validates a boot cmdline template: it must carry an `{owner}` slot, and
/// every placeholder must be one of `owner`, `gpu_arch`, `gpu_count`, `gpu_models`.
fn check_cmdline_template(template: &str) -> Result<(), ManifestError> {
    if !template.contains("{owner}") {
        return Err(ManifestError::MissingOwnerSlot);
    }
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        let after = &rest[start + 1..];
        let Some(relative_end) = after.find('}') else {
            return Err(ManifestError::UnknownPlaceholder(after.to_string()));
        };
        let name = &after[..relative_end];
        if !matches!(name, "owner" | "gpu_arch" | "gpu_count" | "gpu_models") {
            return Err(ManifestError::UnknownPlaceholder(name.to_string()));
        }
        rest = &after[relative_end + 1..];
    }
    Ok(())
}

fn validate_instance_gpu(gpu: &InstanceGpuRuntimeSpec) -> Result<(), ManifestError> {
    validate_gpu_vendor(&gpu.vendor)?;
    validate_gpu_archs(&gpu.archs)?;
    validate_driver_version(&gpu.driver_version)?;
    Ok(())
}

impl InstanceRuntimeManifest {
    pub fn parse(bytes: &[u8]) -> Result<Self, ManifestError> {
        let manifest: InstanceRuntimeManifest = serde_json::from_slice(bytes)?;
        if manifest.format != INSTANCE_MANIFEST_FORMAT
            || manifest.format_version != INSTANCE_MANIFEST_FORMAT_VERSION
        {
            return Err(ManifestError::UnsupportedFormat {
                format: manifest.format,
                version: manifest.format_version,
                expected_format: INSTANCE_MANIFEST_FORMAT.to_string(),
                expected_version: INSTANCE_MANIFEST_FORMAT_VERSION,
            });
        }
        if manifest.platform != "sev_snp" {
            return Err(ManifestError::UnsupportedPlatform(manifest.platform));
        }
        if manifest.boot.method != "qemu-direct-kernel" {
            return Err(ManifestError::UnsupportedBootMethod(manifest.boot.method));
        }
        if !manifest.boot.kernel_hashes {
            return Err(ManifestError::KernelHashesRequired);
        }
        if manifest.boot.cpu_models.is_empty() {
            return Err(ManifestError::NoCpuModels);
        }
        // Same reasoning as the vprogram flavor: `bundle.size` is the
        // download cap, so bound it here rather than let the manifest
        // dictate an unbounded buffer.
        if manifest.bundle.size == 0 || manifest.bundle.size > MAX_BUNDLE_SIZE {
            return Err(ManifestError::BadBundleSize(manifest.bundle.size));
        }
        if !is_lowercase_hex_64(&manifest.bundle.sha256) {
            return Err(ManifestError::BadBundleSha256);
        }
        check_member_path("ovmf", &manifest.bundle.members.ovmf)?;
        check_member_path("kernel", &manifest.bundle.members.kernel)?;
        check_member_path("initrd", &manifest.bundle.members.initrd)?;
        check_cmdline_template(&manifest.boot.cmdline_template)?;
        if let Some(gpu) = &manifest.gpu {
            validate_instance_gpu(gpu)?;
        }
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_json(cmdline: &str) -> String {
        format!(
            r#"{{
              "format": "aleph-instance-runtime", "format_version": 1,
              "name": "aleph-snp-instance", "version": "2026.09.02",
              "platform": "sev_snp",
              "bundle": {{"ref": "{h}", "sha256": "{h}", "size": 1234,
                          "members": {{"ovmf": "ovmf", "kernel": "kernel", "initrd": "initrd"}}}},
              "boot": {{"method": "qemu-direct-kernel", "kernel_hashes": true,
                        "cpu_models": ["EPYC-v4"],
                        "cmdline_template": "{cmdline}"}},
              "attestation": [{{"protocol": "aleph.ra-tls", "version": "1",
                                "transport": {{"type": "tcp", "port": 8443}}}}],
              "source": {{"repo": "r", "rev": "v", "build": "b"}}
            }}"#,
            h = "ab".repeat(32),
        )
    }

    #[test]
    fn parses_a_valid_manifest() {
        let m = InstanceRuntimeManifest::parse(
            manifest_json("console=ttyS0 luks=1 owner={owner}").as_bytes(),
        )
        .expect("valid manifest parses");
        assert_eq!(m.format, INSTANCE_MANIFEST_FORMAT);
        assert_eq!(m.boot.cpu_models, vec!["EPYC-v4".to_string()]);
        assert_eq!(m.bundle.members.initrd, "initrd");
    }

    #[test]
    fn rejects_unknown_fields() {
        let json = manifest_json("console=ttyS0 luks=1 owner={owner}")
            .replace(r#""format""#, r#""surprise": 1, "format""#);
        assert!(InstanceRuntimeManifest::parse(json.as_bytes()).is_err());
    }

    #[test]
    fn rejects_wrong_format_name_and_version() {
        let json = manifest_json("console=ttyS0 luks=1 owner={owner}")
            .replace("aleph-instance-runtime", "aleph-vprogram-runtime");
        assert!(InstanceRuntimeManifest::parse(json.as_bytes()).is_err());
        let json = manifest_json("console=ttyS0 luks=1 owner={owner}")
            .replace(r#"_version": 1"#, r#"_version": 2"#);
        assert!(InstanceRuntimeManifest::parse(json.as_bytes()).is_err());
    }

    #[test]
    fn rejects_cmdline_without_owner_slot() {
        assert!(
            InstanceRuntimeManifest::parse(manifest_json("console=ttyS0 luks=1").as_bytes())
                .is_err()
        );
    }

    #[test]
    fn rejects_unknown_cmdline_placeholder() {
        assert!(
            InstanceRuntimeManifest::parse(
                manifest_json("console=ttyS0 luks=1 owner={owner} x={platform_roothash}")
                    .as_bytes()
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_traversing_member_paths() {
        let json = manifest_json("console=ttyS0 luks=1 owner={owner}")
            .replace(r#""ovmf": "ovmf""#, r#""ovmf": "../ovmf""#);
        assert!(InstanceRuntimeManifest::parse(json.as_bytes()).is_err());
        let json = manifest_json("console=ttyS0 luks=1 owner={owner}")
            .replace(r#""ovmf": "ovmf""#, r#""ovmf": "/ovmf""#);
        assert!(InstanceRuntimeManifest::parse(json.as_bytes()).is_err());
    }

    #[test]
    fn rejects_empty_cpu_models_and_wrong_platform() {
        let json =
            manifest_json("console=ttyS0 luks=1 owner={owner}").replace(r#"["EPYC-v4"]"#, "[]");
        assert!(InstanceRuntimeManifest::parse(json.as_bytes()).is_err());
        let json =
            manifest_json("console=ttyS0 luks=1 owner={owner}").replace(r#""sev_snp""#, r#""tdx""#);
        assert!(InstanceRuntimeManifest::parse(json.as_bytes()).is_err());
    }

    /// A cmdline template that carries the GPU slots alongside `{owner}`.
    const GPU_CMDLINE: &str = "console=ttyS0 luks=1 swiotlb=262144 owner={owner} gpu_arch={gpu_arch} gpu_count={gpu_count} gpu_models={gpu_models}";

    fn manifest_with_gpu(cmdline: &str, gpu: serde_json::Value) -> Vec<u8> {
        let mut json: serde_json::Value = serde_json::from_str(&manifest_json(cmdline)).unwrap();
        json["gpu"] = gpu;
        json.to_string().into_bytes()
    }

    #[test]
    fn gpu_block_is_absent_by_default() {
        let m = InstanceRuntimeManifest::parse(
            manifest_json("console=ttyS0 luks=1 owner={owner}").as_bytes(),
        )
        .unwrap();
        assert!(m.gpu.is_none());
    }

    #[test]
    fn cmdline_template_with_gpu_slots_is_valid() {
        assert!(check_cmdline_template(GPU_CMDLINE).is_ok());
    }

    #[test]
    fn gpu_block_parses() {
        let gpu = serde_json::json!({
            "vendor": "nvidia",
            "driver_version": "595.71.05",
            "archs": {
                "hopper": {"accepted_models": ["GH100 A01 GSP BROM"]},
            }
        });
        let m = InstanceRuntimeManifest::parse(&manifest_with_gpu(GPU_CMDLINE, gpu)).unwrap();
        let gpu = m.gpu.expect("gpu block was set");
        assert_eq!(gpu.vendor, "nvidia");
        assert_eq!(gpu.driver_version, "595.71.05");
        assert_eq!(
            gpu.archs["hopper"].accepted_models,
            vec!["GH100 A01 GSP BROM"]
        );
    }

    #[test]
    fn gpu_block_rejects_invariant_violations() {
        for (gpu, needle) in [
            (
                serde_json::json!({"vendor": "amd", "driver_version": "595.71.05", "archs": {"hopper": {"accepted_models": ["GH100"]}}}),
                "vendor",
            ),
            (
                serde_json::json!({"vendor": "nvidia", "driver_version": "595.71.05", "archs": {}}),
                "archs must not be empty",
            ),
            (
                serde_json::json!({"vendor": "nvidia", "driver_version": "not.a.version", "archs": {"hopper": {"accepted_models": ["GH100"]}}}),
                "driver_version",
            ),
        ] {
            let err =
                InstanceRuntimeManifest::parse(&manifest_with_gpu(GPU_CMDLINE, gpu)).unwrap_err();
            assert!(err.to_string().contains(needle), "got {err}");
        }
    }
}
