//! Models for the vm-images aggregate, which lists rootfs presets, runtimes,
//! and confidential UEFI firmware curated on the network.

use aleph_types::item_hash::ItemHash;
use serde::Deserialize;
use std::collections::BTreeMap;

pub const VM_IMAGES_KEY: &str = "vm-images";

#[derive(Debug, Clone, Deserialize)]
pub struct VmImagesAggregate {
    #[serde(rename = "vm-images")]
    pub vm_images: VmImagesData,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct VmImagesData {
    #[serde(default)]
    pub rootfs: BTreeMap<String, RootfsEntry>,
    #[serde(default)]
    pub runtimes: BTreeMap<String, ImageEntry>,
    #[serde(default)]
    pub firmwares: BTreeMap<String, ImageEntry>,
    /// V-Program runtime bundles (the STORE message of a runtime manifest),
    /// keyed by preset slug. Each declares the workload contract it serves.
    #[serde(default)]
    pub vprogram_runtimes: BTreeMap<String, VProgramRuntimeEntry>,
    #[serde(default)]
    pub defaults: VmImageDefaults,
}

/// Workload contract served by the plain `aleph vprogram create` flow.
pub const VPROGRAM_CONTRACT_EXEC: &str = "aleph.exec/1";
/// Workload contract served by `aleph vprogram create --compose`.
pub const VPROGRAM_CONTRACT_COMPOSE: &str = "aleph.compose/1";

#[derive(Debug, Clone, Deserialize)]
pub struct VProgramRuntimeEntry {
    /// STORE message hash of the runtime manifest.
    pub hash: ItemHash,
    /// Workload contract the bundle implements, e.g. `aleph.exec/1`.
    pub contract: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub deprecated: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImageEntry {
    pub hash: ItemHash,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub deprecated: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RootfsEntry {
    pub hash: ItemHash,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub min_disk_mib: Option<u64>,
    #[serde(default)]
    pub deprecated: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct VmImageDefaults {
    #[serde(default)]
    pub rootfs: Option<String>,
    #[serde(default)]
    pub firmware: Option<String>,
    #[serde(default)]
    pub runtime: Option<String>,
    /// Default V-Program runtime preset per workload contract, e.g.
    /// `{"aleph.exec/1": "exec", "aleph.compose/1": "compose"}`.
    #[serde(default)]
    pub vprogram_runtimes: BTreeMap<String, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum VmImagesError {
    #[error("unknown {kind} preset '{name}' (available: {available})")]
    UnknownPreset {
        kind: &'static str,
        name: String,
        available: String,
    },
    #[error("vm-images aggregate has no default {kind} configured")]
    NoDefault { kind: &'static str },
    #[error(
        "vm-images aggregate has no default V-Program runtime for workload contract {contract:?}"
    )]
    NoDefaultForContract { contract: String },
}

impl VmImagesData {
    pub fn active_rootfs(&self) -> Vec<(&str, &RootfsEntry)> {
        self.rootfs
            .iter()
            .filter(|(_, e)| !e.deprecated)
            .map(|(k, v)| (k.as_str(), v))
            .collect()
    }

    pub fn active_runtimes(&self) -> Vec<(&str, &ImageEntry)> {
        self.runtimes
            .iter()
            .filter(|(_, e)| !e.deprecated)
            .map(|(k, v)| (k.as_str(), v))
            .collect()
    }

    pub fn active_firmwares(&self) -> Vec<(&str, &ImageEntry)> {
        self.firmwares
            .iter()
            .filter(|(_, e)| !e.deprecated)
            .map(|(k, v)| (k.as_str(), v))
            .collect()
    }

    pub fn rootfs(&self, name: &str) -> Result<&RootfsEntry, VmImagesError> {
        self.rootfs
            .get(name)
            .ok_or_else(|| VmImagesError::UnknownPreset {
                kind: "rootfs",
                name: name.to_string(),
                available: join_active_names(self.active_rootfs().iter().map(|(n, _)| *n)),
            })
    }

    pub fn firmware(&self, name: &str) -> Result<&ImageEntry, VmImagesError> {
        self.firmwares
            .get(name)
            .ok_or_else(|| VmImagesError::UnknownPreset {
                kind: "firmware",
                name: name.to_string(),
                available: join_active_names(self.active_firmwares().iter().map(|(n, _)| *n)),
            })
    }

    pub fn active_vprogram_runtimes(&self) -> Vec<(&str, &VProgramRuntimeEntry)> {
        self.vprogram_runtimes
            .iter()
            .filter(|(_, e)| !e.deprecated)
            .map(|(k, v)| (k.as_str(), v))
            .collect()
    }

    pub fn vprogram_runtime(&self, name: &str) -> Result<&VProgramRuntimeEntry, VmImagesError> {
        self.vprogram_runtimes
            .get(name)
            .ok_or_else(|| VmImagesError::UnknownPreset {
                kind: "V-Program runtime",
                name: name.to_string(),
                available: join_active_names(
                    self.active_vprogram_runtimes().iter().map(|(n, _)| *n),
                ),
            })
    }

    /// The default V-Program runtime for a workload contract, per
    /// `defaults.vprogram_runtimes`. Like `rootfs()` / `runtime()`, a
    /// deprecated entry is still returned if the defaults point at it:
    /// deprecation hides a preset from listings, it does not disable it.
    pub fn default_vprogram_runtime(
        &self,
        contract: &str,
    ) -> Result<&VProgramRuntimeEntry, VmImagesError> {
        let name = self
            .defaults
            .vprogram_runtimes
            .get(contract)
            .ok_or_else(|| VmImagesError::NoDefaultForContract {
                contract: contract.to_string(),
            })?;
        self.vprogram_runtime(name)
    }

    pub fn runtime(&self, name: &str) -> Result<&ImageEntry, VmImagesError> {
        self.runtimes
            .get(name)
            .ok_or_else(|| VmImagesError::UnknownPreset {
                kind: "runtime",
                name: name.to_string(),
                available: join_active_names(self.active_runtimes().iter().map(|(n, _)| *n)),
            })
    }
}

fn join_active_names<'a>(names: impl IntoIterator<Item = &'a str>) -> String {
    let mut v: Vec<&str> = names.into_iter().collect();
    v.sort_unstable();
    v.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_fixture() -> &'static str {
        r#"{
          "vm-images": {
            "rootfs": {
              "ubuntu24": {
                "hash": "5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e",
                "display_name": "Ubuntu 24.04 LTS",
                "description": "Ubuntu Noble, cloud-init enabled",
                "min_disk_mib": 20480,
                "deprecated": false
              },
              "ubuntu22": {
                "hash": "4a0f62da42f4478544616519e6f5d58adb1096e069b392b151d47c3609492d0c"
              },
              "old-image": {
                "hash": "1111111111111111111111111111111111111111111111111111111111111111",
                "deprecated": true
              }
            },
            "runtimes": {
              "py311": {
                "hash": "2222222222222222222222222222222222222222222222222222222222222222",
                "display_name": "Python 3.11"
              }
            },
            "firmwares": {
              "ovmf-default": {
                "hash": "ba5bb13f3abca960b101a759be162b229e2b7e93ecad9d1307e54de887f177ff",
                "display_name": "OVMF (default)"
              }
            },
            "vprogram_runtimes": {
              "exec": {
                "hash": "3333333333333333333333333333333333333333333333333333333333333333",
                "contract": "aleph.exec/1",
                "display_name": "V-Program exec runtime"
              },
              "compose": {
                "hash": "4444444444444444444444444444444444444444444444444444444444444444",
                "contract": "aleph.compose/1"
              },
              "exec-old": {
                "hash": "5555555555555555555555555555555555555555555555555555555555555555",
                "contract": "aleph.exec/1",
                "deprecated": true
              }
            },
            "defaults": {
              "rootfs": "ubuntu24",
              "firmware": "ovmf-default",
              "vprogram_runtimes": {"aleph.exec/1": "exec", "aleph.compose/1": "compose"}
            },
            "unknown_section": {"ignored": true}
          }
        }"#
    }

    #[test]
    fn deserialize_full_aggregate() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let data = &agg.vm_images;
        assert_eq!(data.rootfs.len(), 3);
        assert_eq!(data.runtimes.len(), 1);
        assert_eq!(data.firmwares.len(), 1);

        let ubuntu24 = data.rootfs.get("ubuntu24").unwrap();
        assert_eq!(ubuntu24.display_name.as_deref(), Some("Ubuntu 24.04 LTS"));
        assert_eq!(ubuntu24.min_disk_mib, Some(20480));
        assert!(!ubuntu24.deprecated);

        let ubuntu22 = data.rootfs.get("ubuntu22").unwrap();
        assert_eq!(ubuntu22.display_name, None);
        assert_eq!(ubuntu22.description, None);
        assert_eq!(ubuntu22.min_disk_mib, None);
        assert!(!ubuntu22.deprecated);

        assert!(data.rootfs.get("old-image").unwrap().deprecated);

        assert_eq!(data.defaults.rootfs.as_deref(), Some("ubuntu24"));
        assert_eq!(data.defaults.firmware.as_deref(), Some("ovmf-default"));
        assert_eq!(data.defaults.runtime, None);
    }

    #[test]
    fn deserialize_empty_aggregate() {
        let json = r#"{"vm-images": {}}"#;
        let agg: VmImagesAggregate = serde_json::from_str(json).unwrap();
        assert!(agg.vm_images.rootfs.is_empty());
        assert!(agg.vm_images.runtimes.is_empty());
        assert!(agg.vm_images.firmwares.is_empty());
        assert_eq!(agg.vm_images.defaults.rootfs, None);
        assert_eq!(agg.vm_images.defaults.firmware, None);
        assert_eq!(agg.vm_images.defaults.runtime, None);
    }

    #[test]
    fn deserialize_unknown_per_entry_field_ignored() {
        let json = r#"{"vm-images": {"rootfs": {"x": {"hash": "5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e", "future_field": 42}}}}"#;
        let agg: VmImagesAggregate = serde_json::from_str(json).unwrap();
        assert!(agg.vm_images.rootfs.contains_key("x"));
    }

    #[test]
    fn active_rootfs_excludes_deprecated_and_is_sorted() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let active: Vec<&str> = agg
            .vm_images
            .active_rootfs()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(active, vec!["ubuntu22", "ubuntu24"]);
    }

    #[test]
    fn active_runtimes_and_firmwares() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let runtimes: Vec<&str> = agg
            .vm_images
            .active_runtimes()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(runtimes, vec!["py311"]);

        let firmwares: Vec<&str> = agg
            .vm_images
            .active_firmwares()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(firmwares, vec!["ovmf-default"]);
    }

    #[test]
    fn lookup_returns_active_entry() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let entry = agg.vm_images.rootfs("ubuntu24").unwrap();
        assert_eq!(
            entry.hash.to_string(),
            "5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e"
        );
    }

    #[test]
    fn lookup_returns_deprecated_entry() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let entry = agg.vm_images.rootfs("old-image").unwrap();
        assert!(entry.deprecated);
    }

    #[test]
    fn lookup_unknown_lists_active_names() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let err = agg.vm_images.rootfs("nope").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("rootfs"), "msg={msg}");
        assert!(msg.contains("nope"), "msg={msg}");
        assert!(msg.contains("ubuntu22"), "msg={msg}");
        assert!(msg.contains("ubuntu24"), "msg={msg}");
        assert!(
            !msg.contains("old-image"),
            "msg should hide deprecated: {msg}"
        );
    }

    #[test]
    fn lookup_firmware_and_runtime() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        assert_eq!(
            agg.vm_images
                .firmware("ovmf-default")
                .unwrap()
                .hash
                .to_string(),
            "ba5bb13f3abca960b101a759be162b229e2b7e93ecad9d1307e54de887f177ff"
        );
        assert_eq!(
            agg.vm_images
                .runtime("py311")
                .unwrap()
                .display_name
                .as_deref(),
            Some("Python 3.11")
        );
    }

    #[test]
    fn vprogram_runtime_lookup_and_defaults_by_contract() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let data = &agg.vm_images;
        assert_eq!(data.vprogram_runtimes.len(), 3);
        assert_eq!(data.active_vprogram_runtimes().len(), 2);

        let exec = data
            .default_vprogram_runtime(VPROGRAM_CONTRACT_EXEC)
            .unwrap();
        assert_eq!(exec.hash.to_string(), "3".repeat(64));
        assert_eq!(exec.contract, "aleph.exec/1");
        let compose = data
            .default_vprogram_runtime(VPROGRAM_CONTRACT_COMPOSE)
            .unwrap();
        assert_eq!(compose.hash.to_string(), "4".repeat(64));

        assert!(data.vprogram_runtime("exec-old").unwrap().deprecated);
        let err = data.vprogram_runtime("nope").unwrap_err().to_string();
        assert!(
            err.contains("unknown V-Program runtime preset 'nope'"),
            "{err}"
        );
        assert!(err.contains("compose, exec"), "{err}");
        assert!(!err.contains("exec-old"), "{err}");

        let err = data
            .default_vprogram_runtime("aleph.other/1")
            .unwrap_err()
            .to_string();
        assert!(err.contains("aleph.other/1"), "{err}");
    }

    #[test]
    fn vprogram_runtimes_absent_in_old_aggregates() {
        let json = r#"{"vm-images": {"defaults": {"rootfs": "ubuntu24"}}}"#;
        let agg: VmImagesAggregate = serde_json::from_str(json).unwrap();
        assert!(agg.vm_images.vprogram_runtimes.is_empty());
        assert!(agg.vm_images.defaults.vprogram_runtimes.is_empty());
        assert!(
            agg.vm_images
                .default_vprogram_runtime(VPROGRAM_CONTRACT_EXEC)
                .is_err()
        );
    }
}
