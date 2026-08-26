use serde::Deserialize;

pub const MANIFEST_FORMAT: &str = "aleph-vprogram-runtime";
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(
        "unsupported manifest format {format:?} version {version}; expected {MANIFEST_FORMAT:?} version {MANIFEST_FORMAT_VERSION}"
    )]
    UnsupportedFormat { format: String, version: u32 },
    #[error("manifest platform must be sev_snp, got {0:?}")]
    UnsupportedPlatform(String),
    #[error("unsupported boot method {0:?}; v1 supports qemu-direct-kernel")]
    UnsupportedBootMethod(String),
    #[error("v1 requires boot.kernel_hashes = true (measured direct boot)")]
    KernelHashesRequired,
    #[error("boot.cpu_models must not be empty")]
    NoCpuModels,
    #[error("boot.platform_roothash must be 64 lowercase hex chars")]
    BadPlatformRoothash,
}

/// Contract the runtime imposes on the workload volume's contents, e.g.
/// "aleph.compose/1". Absent means: opaque ext4, runtime-defined.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkloadSpec {
    pub contract: String,
    #[serde(default)]
    pub upstream_port: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeManifest {
    pub format: String,
    pub format_version: u32,
    pub name: String,
    pub version: String,
    pub platform: String,
    pub bundle: BundleRef,
    pub boot: BootSpec,
    pub attestation: Vec<AttestationDescriptor>,
    #[serde(default)]
    pub workload: Option<WorkloadSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BundleRef {
    /// STORE message hash pinning the bundle tarball; the download key used
    /// by `fetch_bundle_artifacts`, which then verifies size + sha256 against
    /// this manifest. The file behind the message may sit on native storage
    /// (addressed by its sha256) or on IPFS (addressed by a CID), so `ref`
    /// and `sha256` are not cross-validated.
    #[serde(rename = "ref")]
    pub reference: String,
    pub sha256: String,
    pub size: u64,
    pub members: BundleMembers,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BundleMembers {
    pub ovmf: String,
    pub kernel: String,
    pub initrd: String,
    pub platform_rootfs: String,
    pub platform_hash_tree: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BootSpec {
    pub method: String,
    pub kernel_hashes: bool,
    pub cpu_models: Vec<String>,
    pub platform_roothash: String,
    pub cmdline_template: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AttestationDescriptor {
    pub protocol: String,
    pub version: String,
    /// Deliberately opaque: the transport schema varies per attestation
    /// protocol, and the v1 CLI validates the manifest without
    /// interpreting transports (that is the verifier client's job).
    pub transport: serde_json::Value,
}

fn is_lowercase_hex_64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

impl RuntimeManifest {
    pub fn parse(bytes: &[u8]) -> Result<Self, ManifestError> {
        let manifest: RuntimeManifest = serde_json::from_slice(bytes)?;
        if manifest.format != MANIFEST_FORMAT || manifest.format_version != MANIFEST_FORMAT_VERSION
        {
            return Err(ManifestError::UnsupportedFormat {
                format: manifest.format,
                version: manifest.format_version,
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
        if !is_lowercase_hex_64(&manifest.boot.platform_roothash) {
            return Err(ManifestError::BadPlatformRoothash);
        }
        Ok(manifest)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const VALID_MANIFEST: &str = r#"{
  "format": "aleph-vprogram-runtime",
  "format_version": 1,
  "name": "aleph-snp-attest",
  "version": "2026.07.08",
  "platform": "sev_snp",
  "bundle": {
    "ref": "87287e4a5c8d7554a50f982cd681b64b2600c0bbb1c0b1e618465e022e01b977",
    "sha256": "1db0d69c96dc7ed6c8a6cbb8c63f8de516ef4ed668e95c468cc216e4c44d911b",
    "size": 57522386,
    "members": {
      "ovmf": "image/OVMF.fd",
      "kernel": "image/bzImage",
      "initrd": "image/initrd",
      "platform_rootfs": "image/rootfs.ext4",
      "platform_hash_tree": "image/rootfs.ext4.verity"
    }
  },
  "boot": {
    "method": "qemu-direct-kernel",
    "kernel_hashes": true,
    "cpu_models": ["EPYC-v4"],
    "platform_roothash": "cb121a317be7dc7969dd633ca9b6c3718ffe9ea6715b64e0e35a871d484b56b8",
    "cmdline_template": "console=ttyS0 root=/dev/mapper/verity-root ro roothash={platform_roothash} workload_roothash={workload_roothash}"
  },
  "attestation": [
    { "protocol": "aleph.ra-tls", "version": "1", "transport": { "type": "tcp", "port": 8443 } }
  ],
  "workload": { "contract": "aleph.builtin/1", "upstream_port": 8080 },
  "source": { "repo": "https://github.com/aleph-im/aleph-vm", "rev": "4d90abaf", "build": "nix build" }
}"#;

    fn mutate(base: &str, patch: &str) -> String {
        match patch {
            patch if patch.contains("platform_roothash") => {
                base.replace(
                    r#""platform_roothash": "cb121a317be7dc7969dd633ca9b6c3718ffe9ea6715b64e0e35a871d484b56b8""#,
                    patch,
                )
            }
            patch if patch.contains("platform") => {
                base.replace(r#""platform": "sev_snp""#, patch)
            }
            patch if patch.contains("format_version") => {
                base.replace(r#""format_version": 1"#, patch)
            }
            patch if patch.contains("format") => {
                base.replace(r#""format": "aleph-vprogram-runtime""#, patch)
            }
            patch if patch.contains("method") => {
                base.replace(r#""method": "qemu-direct-kernel""#, patch)
            }
            patch if patch.contains("kernel_hashes") => {
                base.replace(r#""kernel_hashes": true"#, patch)
            }
            patch if patch.contains("cpu_models") => {
                base.replace(r#""cpu_models": ["EPYC-v4"]"#, patch)
            }
            _ => base.to_string(),
        }
    }

    #[test]
    fn parse_valid_manifest() {
        let m = RuntimeManifest::parse(VALID_MANIFEST.as_bytes()).unwrap();
        assert_eq!(m.name, "aleph-snp-attest");
        assert_eq!(m.bundle.members.ovmf, "image/OVMF.fd");
        assert_eq!(m.boot.cpu_models, vec!["EPYC-v4"]);
        assert_eq!(m.attestation[0].protocol, "aleph.ra-tls");
    }

    #[test]
    fn parse_rejects_invariant_violations() {
        for (patch, needle) in [
            (
                r#""format": "something-else""#,
                "unsupported manifest format",
            ),
            (r#""format_version": 2"#, "unsupported manifest format"),
            (r#""platform": "tdx""#, "platform must be sev_snp"),
            (r#""method": "igvm""#, "unsupported boot method"),
            (r#""kernel_hashes": false"#, "kernel_hashes"),
            (r#""cpu_models": []"#, "cpu_models"),
            (r#""platform_roothash": "abc""#, "platform_roothash"),
        ] {
            let json = mutate(VALID_MANIFEST, patch);
            let err = RuntimeManifest::parse(json.as_bytes()).unwrap_err();
            assert!(err.to_string().contains(needle), "{patch}: got {err}");
        }
    }

    #[test]
    fn parses_the_workload_contract() {
        let m = RuntimeManifest::parse(VALID_MANIFEST.as_bytes()).unwrap();
        let w = m.workload.expect("fixture declares a workload contract");
        assert_eq!(w.contract, "aleph.builtin/1");
        assert_eq!(w.upstream_port, Some(8080));
    }

    #[test]
    fn workload_is_optional() {
        let stripped = VALID_MANIFEST.replace(
            r#""workload": { "contract": "aleph.builtin/1", "upstream_port": 8080 },"#,
            "",
        );
        assert_ne!(stripped, VALID_MANIFEST);
        let m = RuntimeManifest::parse(stripped.as_bytes()).unwrap();
        assert!(m.workload.is_none());
    }

    #[test]
    fn the_published_compose_runtime_manifest_parses() {
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/vprogram/compose-runtime-manifest.json"
        ));
        let m = RuntimeManifest::parse(bytes).unwrap();
        assert_eq!(m.workload.unwrap().contract, "aleph.compose/1");
        assert_eq!(
            m.boot.cmdline_template,
            "console=ttyS0 root=/dev/mapper/verity-root ro roothash={platform_roothash} workload_roothash={workload_roothash}"
        );
    }
}
