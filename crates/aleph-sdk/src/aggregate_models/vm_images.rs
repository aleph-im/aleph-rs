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
    /// V-Program runtimes: workload models -> contracts -> implementations.
    #[serde(default)]
    pub vprogram_runtimes: VProgramRuntimes,
    #[serde(default)]
    pub defaults: VmImageDefaults,
}

/// Workload model served by the plain `aleph vprogram create` flow.
pub const VPROGRAM_MODEL_EXEC: &str = "exec";
/// Workload model served by `aleph vprogram create --compose`.
pub const VPROGRAM_MODEL_COMPOSE: &str = "compose";
/// Workload contract served by the plain `aleph vprogram create` flow.
pub const VPROGRAM_CONTRACT_EXEC: &str = "aleph.exec/1";
/// Workload contract served by `aleph vprogram create --compose`.
pub const VPROGRAM_CONTRACT_COMPOSE: &str = "aleph.compose/1";

/// The V-Program runtime catalogue, three levels deep:
///
/// - a **model** is how the user hands over a workload (`exec`: a prebuilt
///   binary image; `compose`: a Docker Compose stack) and names its default
///   contract;
/// - a **contract** (`aleph.exec/1`, ...) is the on-the-wire convention a
///   runtime imposes on the workload volume, belongs to one model, and names
///   its default implementation;
/// - an **implementation** (`exec-1.0`, ...) is one published runtime bundle:
///   the STORE message hash of its manifest.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct VProgramRuntimes {
    #[serde(default)]
    pub models: BTreeMap<String, VProgramModel>,
    #[serde(default)]
    pub contracts: BTreeMap<String, VProgramContract>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VProgramModel {
    /// Contract used when the caller does not pick one explicitly.
    pub default_contract: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VProgramContract {
    /// The model this contract belongs to.
    pub model: String,
    /// Implementation used when the caller does not pick one explicitly.
    /// Expected to point at the latest / most secure bundle.
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub implementations: BTreeMap<String, VProgramImplementation>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VProgramImplementation {
    /// STORE message hash of the runtime manifest.
    pub hash: ItemHash,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub deprecated: bool,
}

/// A fully resolved V-Program runtime choice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedVProgramRuntime {
    pub contract: String,
    pub implementation: String,
    pub hash: ItemHash,
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
    #[error("unknown V-Program workload model {model:?} (available: {available})")]
    UnknownModel { model: String, available: String },
    #[error("unknown V-Program workload contract {contract:?} (available: {available})")]
    UnknownContract { contract: String, available: String },
    #[error("unknown V-Program runtime implementation {name:?} (available: {available})")]
    UnknownImplementation { name: String, available: String },
    #[error(
        "V-Program runtime implementation {name:?} is ambiguous, it exists under contracts {contracts}"
    )]
    AmbiguousImplementation { name: String, contracts: String },
    #[error("V-Program workload contract {contract:?} has no default implementation configured")]
    NoDefaultImplementation { contract: String },
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
}

impl VProgramRuntimes {
    pub fn model(&self, name: &str) -> Result<&VProgramModel, VmImagesError> {
        self.models
            .get(name)
            .ok_or_else(|| VmImagesError::UnknownModel {
                model: name.to_string(),
                available: join_active_names(self.models.keys().map(String::as_str)),
            })
    }

    pub fn contract(&self, name: &str) -> Result<&VProgramContract, VmImagesError> {
        self.contracts
            .get(name)
            .ok_or_else(|| VmImagesError::UnknownContract {
                contract: name.to_string(),
                available: join_active_names(self.contracts.keys().map(String::as_str)),
            })
    }

    /// Non-deprecated implementations of every contract of `model`, as
    /// `(contract, implementation, entry)`.
    pub fn active_implementations(
        &self,
        model: &str,
    ) -> Vec<(&str, &str, &VProgramImplementation)> {
        self.contracts
            .iter()
            .filter(|(_, c)| c.model == model)
            .flat_map(|(contract, c)| {
                c.implementations
                    .iter()
                    .filter(|(_, i)| !i.deprecated)
                    .map(move |(name, i)| (contract.as_str(), name.as_str(), i))
            })
            .collect()
    }

    /// Resolve a runtime for `model`. `selector` is `None` (the model's
    /// default contract, then that contract's default implementation), a
    /// contract name such as `aleph.exec/1` (its default implementation),
    /// or an implementation name such as `exec-1.0`. A contract or
    /// implementation belonging to another model is rejected with `hint`
    /// appended to the error (e.g. " (did you mean --compose?)").
    pub fn resolve(
        &self,
        model: &str,
        selector: Option<&str>,
        hint: impl Fn(&str) -> &'static str,
    ) -> Result<ResolvedVProgramRuntime, VmImagesError> {
        let (contract_name, implementation) = match selector {
            None => (self.model(model)?.default_contract.clone(), None),
            Some(sel) if sel.contains('/') => (sel.to_string(), None),
            Some(sel) => {
                let mut owners: Vec<&str> = self
                    .contracts
                    .iter()
                    .filter(|(_, c)| c.implementations.contains_key(sel))
                    .map(|(name, _)| name.as_str())
                    .collect();
                owners.sort_unstable();
                match owners.as_slice() {
                    [] => {
                        return Err(VmImagesError::UnknownImplementation {
                            name: sel.to_string(),
                            available: join_active_names(
                                self.active_implementations(model)
                                    .iter()
                                    .map(|(_, name, _)| *name),
                            ),
                        });
                    }
                    [one] => (one.to_string(), Some(sel.to_string())),
                    many => {
                        return Err(VmImagesError::AmbiguousImplementation {
                            name: sel.to_string(),
                            contracts: many.join(", "),
                        });
                    }
                }
            }
        };
        let contract = self.contract(&contract_name)?;
        if contract.model != model {
            return Err(VmImagesError::ModelMismatch {
                what: match &implementation {
                    Some(name) => format!("runtime implementation {name:?}"),
                    None => format!("workload contract {contract_name:?}"),
                },
                found: contract.model.clone(),
                wanted: model.to_string(),
                hint: hint(&contract.model),
            });
        }
        let implementation =
            match implementation {
                Some(name) => name,
                None => contract.default.clone().ok_or_else(|| {
                    VmImagesError::NoDefaultImplementation {
                        contract: contract_name.clone(),
                    }
                })?,
            };
        let entry = contract
            .implementations
            .get(&implementation)
            .ok_or_else(|| VmImagesError::UnknownImplementation {
                name: implementation.clone(),
                available: join_active_names(
                    contract
                        .implementations
                        .iter()
                        .filter(|(_, i)| !i.deprecated)
                        .map(|(n, _)| n.as_str()),
                ),
            })?;
        Ok(ResolvedVProgramRuntime {
            contract: contract_name,
            implementation,
            hash: entry.hash.clone(),
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
              "models": {
                "exec": { "default_contract": "aleph.exec/1", "display_name": "Binary workload" },
                "compose": { "default_contract": "aleph.compose/1" }
              },
              "contracts": {
                "aleph.exec/1": {
                  "model": "exec",
                  "default": "exec-1.0",
                  "implementations": {
                    "exec-1.0": {
                      "hash": "3333333333333333333333333333333333333333333333333333333333333333",
                      "display_name": "V-Program exec runtime 1.0"
                    },
                    "exec-0.9": {
                      "hash": "5555555555555555555555555555555555555555555555555555555555555555",
                      "deprecated": true
                    }
                  }
                },
                "aleph.compose/1": {
                  "model": "compose",
                  "default": "compose-1.0",
                  "implementations": {
                    "compose-1.0": {
                      "hash": "4444444444444444444444444444444444444444444444444444444444444444"
                    }
                  }
                },
                "aleph.exec/2": {
                  "model": "exec",
                  "implementations": {
                    "exec-2.0-rc1": {
                      "hash": "6666666666666666666666666666666666666666666666666666666666666666"
                    }
                  }
                }
              }
            },
            "defaults": {
              "rootfs": "ubuntu24",
              "firmware": "ovmf-default"
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
    fn vprogram_runtime_resolution_walks_model_contract_implementation() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let rt = &agg.vm_images.vprogram_runtimes;
        let no_hint = |_: &str| "";

        // Defaults all the way down.
        let exec = rt.resolve(VPROGRAM_MODEL_EXEC, None, no_hint).unwrap();
        assert_eq!(exec.contract, "aleph.exec/1");
        assert_eq!(exec.implementation, "exec-1.0");
        assert_eq!(exec.hash.to_string(), "3".repeat(64));
        let compose = rt.resolve(VPROGRAM_MODEL_COMPOSE, None, no_hint).unwrap();
        assert_eq!(compose.hash.to_string(), "4".repeat(64));

        // Explicit contract: its default implementation.
        let exec = rt
            .resolve(VPROGRAM_MODEL_EXEC, Some("aleph.exec/1"), no_hint)
            .unwrap();
        assert_eq!(exec.implementation, "exec-1.0");

        // Explicit implementation, including a deprecated one.
        let old = rt
            .resolve(VPROGRAM_MODEL_EXEC, Some("exec-0.9"), no_hint)
            .unwrap();
        assert_eq!(old.hash.to_string(), "5".repeat(64));
        let rc = rt
            .resolve(VPROGRAM_MODEL_EXEC, Some("exec-2.0-rc1"), no_hint)
            .unwrap();
        assert_eq!(rc.contract, "aleph.exec/2");

        // A contract without a default cannot be picked implicitly.
        let err = rt
            .resolve(VPROGRAM_MODEL_EXEC, Some("aleph.exec/2"), no_hint)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no default implementation"), "{err}");

        // Listings hide deprecated implementations.
        let active: Vec<&str> = rt
            .active_implementations(VPROGRAM_MODEL_EXEC)
            .iter()
            .map(|(_, name, _)| *name)
            .collect();
        assert_eq!(active, vec!["exec-1.0", "exec-2.0-rc1"]);
    }

    #[test]
    fn vprogram_runtime_resolution_errors() {
        let agg: VmImagesAggregate = serde_json::from_str(full_fixture()).unwrap();
        let rt = &agg.vm_images.vprogram_runtimes;
        let hint = |model: &str| {
            if model == "compose" {
                " (did you mean --compose?)"
            } else {
                ""
            }
        };

        let err = rt
            .resolve(VPROGRAM_MODEL_EXEC, Some("compose-1.0"), hint)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("belongs to workload model \"compose\""),
            "{err}"
        );
        assert!(err.ends_with("(did you mean --compose?)"), "{err}");

        let err = rt
            .resolve(VPROGRAM_MODEL_COMPOSE, Some("aleph.exec/1"), hint)
            .unwrap_err()
            .to_string();
        assert!(err.contains("workload contract \"aleph.exec/1\""), "{err}");
        assert!(!err.contains("did you mean"), "{err}");

        let err = rt
            .resolve(VPROGRAM_MODEL_EXEC, Some("nope"), hint)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("unknown V-Program runtime implementation \"nope\""),
            "{err}"
        );
        assert!(err.contains("exec-1.0, exec-2.0-rc1"), "{err}");
        assert!(!err.contains("exec-0.9"), "{err}");

        let err = rt
            .resolve(VPROGRAM_MODEL_EXEC, Some("aleph.nope/1"), hint)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown V-Program workload contract"), "{err}");

        let err = rt.resolve("bogus", None, hint).unwrap_err().to_string();
        assert!(
            err.contains("unknown V-Program workload model \"bogus\""),
            "{err}"
        );
        assert!(err.contains("compose, exec"), "{err}");
    }

    #[test]
    fn vprogram_runtimes_absent_in_old_aggregates() {
        let json = r#"{"vm-images": {"defaults": {"rootfs": "ubuntu24"}}}"#;
        let agg: VmImagesAggregate = serde_json::from_str(json).unwrap();
        let rt = &agg.vm_images.vprogram_runtimes;
        assert!(rt.models.is_empty() && rt.contracts.is_empty());
        assert!(rt.resolve(VPROGRAM_MODEL_EXEC, None, |_| "").is_err());
    }
}
