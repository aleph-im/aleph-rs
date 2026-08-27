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
    /// V-Program runtime bundles keyed by name (`exec-1.0`, ...); each
    /// names the workload contract it implements.
    #[serde(default)]
    pub vprogram_runtimes: BTreeMap<String, VProgramRuntimeEntry>,
    /// Default runtime per workload contract: `{"aleph.exec/1": "exec-1.0"}`.
    /// Expected to point at the latest / most secure bundle.
    #[serde(default)]
    pub vprogram_contracts: BTreeMap<String, String>,
    #[serde(default)]
    pub defaults: VmImageDefaults,
}

/// Workload model served by the plain `aleph vprogram create` flow.
pub const VPROGRAM_MODEL_EXEC: &str = "exec";
/// Workload model served by `aleph vprogram create --compose`.
pub const VPROGRAM_MODEL_COMPOSE: &str = "compose";
/// Workload contract served by `aleph vprogram create --compose`.
pub const VPROGRAM_CONTRACT_COMPOSE: &str = "aleph.compose/1";

/// One published V-Program runtime bundle.
///
/// The catalogue is three levels deep, each with a default: a workload
/// **model** (`exec`: a prebuilt binary image; `compose`: a Docker Compose
/// stack) is how the user hands over a workload and, via
/// `defaults.vprogram_models`, names its current **contract**
/// (`aleph.exec/1`, ...: the convention a runtime imposes on the workload
/// volume); `vprogram_contracts` names each contract's default **runtime**;
/// and every runtime here declares the contract it implements. A contract's
/// model is read off its name, `aleph.<model>/<version>`.
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

/// The workload model a contract name encodes: `aleph.exec/1` -> `exec`.
/// `None` when the name does not follow `aleph.<model>/<version>`.
pub fn vprogram_contract_model(contract: &str) -> Option<&str> {
    let rest = contract.strip_prefix("aleph.")?;
    let (model, version) = rest.split_once('/')?;
    (!model.is_empty() && !version.is_empty()).then_some(model)
}

/// A fully resolved V-Program runtime choice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedVProgramRuntime {
    pub contract: String,
    pub runtime: String,
    pub hash: ItemHash,
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
    /// Current workload contract per model: `{"exec": "aleph.exec/1"}`.
    #[serde(default)]
    pub vprogram_models: BTreeMap<String, String>,
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
    #[error("vm-images aggregate has no default contract for V-Program workload model {model:?}")]
    NoDefaultContract { model: String },
    #[error(
        "vm-images aggregate has no default runtime for V-Program workload contract {contract:?}"
    )]
    NoDefaultRuntime { contract: String },
    #[error("unknown V-Program runtime {name:?} (available for this model: {available})")]
    UnknownVProgramRuntime { name: String, available: String },
    #[error(
        "V-Program workload contract {contract:?} does not follow `aleph.<model>/<version>`, \
         so its model cannot be determined"
    )]
    MalformedContract { contract: String },
    #[error(
        "{what} belongs to workload model {found:?}, but this invocation uses model {wanted:?}{hint}"
    )]
    ModelMismatch {
        what: String,
        found: String,
        wanted: String,
        hint: &'static str,
    },
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

    pub fn runtime(&self, name: &str) -> Result<&ImageEntry, VmImagesError> {
        self.runtimes
            .get(name)
            .ok_or_else(|| VmImagesError::UnknownPreset {
                kind: "runtime",
                name: name.to_string(),
                available: join_active_names(self.active_runtimes().iter().map(|(n, _)| *n)),
            })
    }

    /// Non-deprecated V-Program runtimes implementing a contract of `model`,
    /// as `(name, entry)`.
    pub fn active_vprogram_runtimes(&self, model: &str) -> Vec<(&str, &VProgramRuntimeEntry)> {
        self.vprogram_runtimes
            .iter()
            .filter(|(_, e)| !e.deprecated && vprogram_contract_model(&e.contract) == Some(model))
            .map(|(k, v)| (k.as_str(), v))
            .collect()
    }

    /// Resolve a V-Program runtime for `model`. `selector` is `None` (the
    /// model's default contract, then that contract's default runtime), a
    /// contract name such as `aleph.exec/1` (its default runtime), or a
    /// runtime name such as `exec-1.0`. A contract or runtime belonging to
    /// another model is rejected with `hint(found_model)` appended to the
    /// error (e.g. " (did you mean --compose?)").
    pub fn resolve_vprogram_runtime(
        &self,
        model: &str,
        selector: Option<&str>,
        hint: impl Fn(&str) -> &'static str,
    ) -> Result<ResolvedVProgramRuntime, VmImagesError> {
        let (contract, runtime) = match selector {
            None => {
                let contract = self.defaults.vprogram_models.get(model).ok_or_else(|| {
                    VmImagesError::NoDefaultContract {
                        model: model.to_string(),
                    }
                })?;
                (contract.clone(), None)
            }
            Some(sel) if sel.contains('/') => (sel.to_string(), None),
            Some(sel) => {
                let entry = self.vprogram_runtimes.get(sel).ok_or_else(|| {
                    VmImagesError::UnknownVProgramRuntime {
                        name: sel.to_string(),
                        available: join_active_names(
                            self.active_vprogram_runtimes(model).iter().map(|(n, _)| *n),
                        ),
                    }
                })?;
                (entry.contract.clone(), Some(sel.to_string()))
            }
        };
        let found =
            vprogram_contract_model(&contract).ok_or_else(|| VmImagesError::MalformedContract {
                contract: contract.clone(),
            })?;
        if found != model {
            return Err(VmImagesError::ModelMismatch {
                what: match &runtime {
                    Some(name) => format!("runtime {name:?}"),
                    None => format!("workload contract {contract:?}"),
                },
                found: found.to_string(),
                wanted: model.to_string(),
                hint: hint(found),
            });
        }
        let runtime = match runtime {
            Some(name) => name,
            None => self
                .vprogram_contracts
                .get(&contract)
                .cloned()
                .ok_or_else(|| VmImagesError::NoDefaultRuntime {
                    contract: contract.clone(),
                })?,
        };
        let entry = self.vprogram_runtimes.get(&runtime).ok_or_else(|| {
            VmImagesError::UnknownVProgramRuntime {
                name: runtime.clone(),
                available: join_active_names(
                    self.active_vprogram_runtimes(model).iter().map(|(n, _)| *n),
                ),
            }
        })?;
        Ok(ResolvedVProgramRuntime {
            hash: entry.hash.clone(),
            contract,
            runtime,
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
              "exec-1.0": {
                "hash": "3333333333333333333333333333333333333333333333333333333333333333",
                "contract": "aleph.exec/1",
                "display_name": "V-Program exec runtime 1.0"
              },
              "exec-0.9": {
                "hash": "5555555555555555555555555555555555555555555555555555555555555555",
                "contract": "aleph.exec/1",
                "deprecated": true
              },
              "compose-1.0": {
                "hash": "4444444444444444444444444444444444444444444444444444444444444444",
                "contract": "aleph.compose/1"
              },
              "exec-2.0-rc1": {
                "hash": "6666666666666666666666666666666666666666666666666666666666666666",
                "contract": "aleph.exec/2"
              },
              "weird": {
                "hash": "7777777777777777777777777777777777777777777777777777777777777777",
                "contract": "legacy"
              }
            },
            "vprogram_contracts": {
              "aleph.exec/1": "exec-1.0",
              "aleph.compose/1": "compose-1.0"
            },
            "defaults": {
              "rootfs": "ubuntu24",
              "firmware": "ovmf-default",
              "vprogram_models": {"exec": "aleph.exec/1", "compose": "aleph.compose/1"}
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
    fn vprogram_contract_model_parses_the_name() {
        assert_eq!(vprogram_contract_model("aleph.exec/1"), Some("exec"));
        assert_eq!(vprogram_contract_model("aleph.compose/12"), Some("compose"));
        for bad in ["exec/1", "aleph.exec", "aleph./1", "aleph.exec/", "legacy"] {
            assert_eq!(vprogram_contract_model(bad), None, "{bad}");
        }
    }

    #[test]
    fn vprogram_runtime_resolution_walks_model_contract_runtime() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let data = &agg.vm_images;
        let no_hint = |_: &str| "";

        // Defaults all the way down.
        let exec = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_EXEC, None, no_hint)
            .unwrap();
        assert_eq!(exec.contract, "aleph.exec/1");
        assert_eq!(exec.runtime, "exec-1.0");
        assert_eq!(exec.hash.to_string(), "3".repeat(64));
        let compose = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_COMPOSE, None, no_hint)
            .unwrap();
        assert_eq!(compose.hash.to_string(), "4".repeat(64));

        // Explicit contract: its default runtime.
        let exec = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_EXEC, Some("aleph.exec/1"), no_hint)
            .unwrap();
        assert_eq!(exec.runtime, "exec-1.0");

        // Explicit runtime, including a deprecated one and a non-default contract.
        let old = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_EXEC, Some("exec-0.9"), no_hint)
            .unwrap();
        assert_eq!(old.hash.to_string(), "5".repeat(64));
        let rc = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_EXEC, Some("exec-2.0-rc1"), no_hint)
            .unwrap();
        assert_eq!(rc.contract, "aleph.exec/2");

        // A contract without a default runtime cannot be picked implicitly.
        let err = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_EXEC, Some("aleph.exec/2"), no_hint)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no default runtime"), "{err}");

        // Listings hide deprecated runtimes and other models.
        let active: Vec<&str> = data
            .active_vprogram_runtimes(VPROGRAM_MODEL_EXEC)
            .iter()
            .map(|(name, _)| *name)
            .collect();
        assert_eq!(active, vec!["exec-1.0", "exec-2.0-rc1"]);
    }

    #[test]
    fn vprogram_runtime_resolution_errors() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let data = &agg.vm_images;
        let hint = |model: &str| {
            if model == "compose" {
                " (did you mean --compose?)"
            } else {
                ""
            }
        };

        let err = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_EXEC, Some("compose-1.0"), hint)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("belongs to workload model \"compose\""),
            "{err}"
        );
        assert!(err.ends_with("(did you mean --compose?)"), "{err}");

        let err = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_COMPOSE, Some("aleph.exec/1"), hint)
            .unwrap_err()
            .to_string();
        assert!(err.contains("workload contract \"aleph.exec/1\""), "{err}");
        assert!(!err.contains("did you mean"), "{err}");

        let err = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_EXEC, Some("nope"), hint)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown V-Program runtime \"nope\""), "{err}");
        assert!(err.contains("exec-1.0, exec-2.0-rc1"), "{err}");
        assert!(
            !err.contains("exec-0.9") && !err.contains("compose-1.0"),
            "{err}"
        );

        let err = data
            .resolve_vprogram_runtime(VPROGRAM_MODEL_EXEC, Some("weird"), hint)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not follow"), "{err}");

        let err = data
            .resolve_vprogram_runtime("bogus", None, hint)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no default contract"), "{err}");
    }

    #[test]
    fn vprogram_runtimes_absent_in_old_aggregates() {
        let json = r#"{"vm-images": {"defaults": {"rootfs": "ubuntu24"}}}"#;
        let agg: VmImagesAggregate = serde_json::from_str(json).unwrap();
        let data = &agg.vm_images;
        assert!(data.vprogram_runtimes.is_empty() && data.vprogram_contracts.is_empty());
        assert!(data.defaults.vprogram_models.is_empty());
        assert!(
            data.resolve_vprogram_runtime(VPROGRAM_MODEL_EXEC, None, |_| "")
                .is_err()
        );
    }
}
