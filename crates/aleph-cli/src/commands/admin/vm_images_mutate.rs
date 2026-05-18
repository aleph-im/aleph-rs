use aleph_sdk::aggregate_models::vm_images::{
    ImageEntry, RootfsEntry, VmImagesData,
};
use aleph_types::item_hash::ItemHash;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Rootfs,
    Runtime,
    Firmware,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Rootfs => "rootfs",
            Kind::Runtime => "runtime",
            Kind::Firmware => "firmware",
        }
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct NewEntry {
    pub hash: ItemHash,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub min_disk_mib: Option<u64>,
    pub deprecated: bool,
}

#[derive(Clone, Debug, Default)]
pub struct EntryPatch {
    pub hash: Option<ItemHash>,
    pub display_name: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub min_disk_mib: Option<Option<u64>>,
}

impl EntryPatch {
    pub fn is_empty(&self) -> bool {
        self.hash.is_none()
            && self.display_name.is_none()
            && self.description.is_none()
            && self.min_disk_mib.is_none()
    }
}

#[derive(Clone, Debug)]
pub enum Mutation {
    Add {
        kind: Kind,
        name: String,
        entry: NewEntry,
    },
    Update {
        kind: Kind,
        name: String,
        patch: EntryPatch,
    },
    Deprecate {
        kind: Kind,
        name: String,
    },
    Undeprecate {
        kind: Kind,
        name: String,
    },
    SetDefault {
        kind: Kind,
        name: String,
    },
    ClearDefault {
        kind: Kind,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum AdminImagesError {
    #[error("{kind} '{name}' already exists")]
    AlreadyExists { kind: Kind, name: String },

    #[error("{kind} '{name}' does not exist (available: {available})")]
    NotFound {
        kind: Kind,
        name: String,
        available: String,
    },

    #[error("{kind} '{name}' is already deprecated")]
    AlreadyDeprecated { kind: Kind, name: String },

    #[error("{kind} '{name}' is not deprecated")]
    NotDeprecated { kind: Kind, name: String },

    #[error("default cannot point at deprecated entry '{name}'")]
    DefaultPointsAtDeprecated { name: String },

    #[error("--min-disk-mib only applies to rootfs entries")]
    IrrelevantField,

    #[error("update needs at least one field flag")]
    NoFieldsToUpdate,
}

pub fn apply_mutation(
    data: &mut VmImagesData,
    mutation: Mutation,
) -> Result<(), AdminImagesError> {
    match mutation {
        Mutation::Add { kind, name, entry } => apply_add(data, kind, name, entry),
        Mutation::Update { kind, name, patch } => apply_update(data, kind, name, patch),
        Mutation::Deprecate { kind, name } => apply_deprecate(data, kind, name),
        Mutation::Undeprecate { kind, name } => apply_undeprecate(data, kind, name),
        Mutation::SetDefault { .. } | Mutation::ClearDefault { .. } => {
            unimplemented!("Task 9 implements these")
        }
    }
}

fn apply_add(
    data: &mut VmImagesData,
    kind: Kind,
    name: String,
    entry: NewEntry,
) -> Result<(), AdminImagesError> {
    match kind {
        Kind::Rootfs => {
            if data.rootfs.contains_key(&name) {
                return Err(AdminImagesError::AlreadyExists { kind, name });
            }
            data.rootfs.insert(
                name,
                RootfsEntry {
                    hash: entry.hash,
                    display_name: entry.display_name,
                    description: entry.description,
                    min_disk_mib: entry.min_disk_mib,
                    deprecated: entry.deprecated,
                },
            );
        }
        Kind::Runtime => {
            if entry.min_disk_mib.is_some() {
                return Err(AdminImagesError::IrrelevantField);
            }
            if data.runtimes.contains_key(&name) {
                return Err(AdminImagesError::AlreadyExists { kind, name });
            }
            data.runtimes.insert(
                name,
                ImageEntry {
                    hash: entry.hash,
                    display_name: entry.display_name,
                    description: entry.description,
                    deprecated: entry.deprecated,
                },
            );
        }
        Kind::Firmware => {
            if entry.min_disk_mib.is_some() {
                return Err(AdminImagesError::IrrelevantField);
            }
            if data.firmwares.contains_key(&name) {
                return Err(AdminImagesError::AlreadyExists { kind, name });
            }
            data.firmwares.insert(
                name,
                ImageEntry {
                    hash: entry.hash,
                    display_name: entry.display_name,
                    description: entry.description,
                    deprecated: entry.deprecated,
                },
            );
        }
    }
    Ok(())
}

fn apply_update(
    data: &mut VmImagesData,
    kind: Kind,
    name: String,
    patch: EntryPatch,
) -> Result<(), AdminImagesError> {
    if patch.is_empty() {
        return Err(AdminImagesError::NoFieldsToUpdate);
    }
    // min_disk_mib is rootfs-only.
    if !matches!(kind, Kind::Rootfs) && patch.min_disk_mib.is_some() {
        return Err(AdminImagesError::IrrelevantField);
    }

    match kind {
        Kind::Rootfs => {
            if !data.rootfs.contains_key(&name) {
                return Err(AdminImagesError::NotFound {
                    kind,
                    name,
                    available: join_active_names(data, kind),
                });
            }
            let entry = data.rootfs.get_mut(&name).unwrap();
            if let Some(hash) = patch.hash {
                entry.hash = hash;
            }
            if let Some(v) = patch.display_name {
                entry.display_name = v;
            }
            if let Some(v) = patch.description {
                entry.description = v;
            }
            if let Some(v) = patch.min_disk_mib {
                entry.min_disk_mib = v;
            }
        }
        Kind::Runtime => {
            if !data.runtimes.contains_key(&name) {
                return Err(AdminImagesError::NotFound {
                    kind,
                    name,
                    available: join_active_names(data, kind),
                });
            }
            let entry = data.runtimes.get_mut(&name).unwrap();
            if let Some(hash) = patch.hash {
                entry.hash = hash;
            }
            if let Some(v) = patch.display_name {
                entry.display_name = v;
            }
            if let Some(v) = patch.description {
                entry.description = v;
            }
        }
        Kind::Firmware => {
            if !data.firmwares.contains_key(&name) {
                return Err(AdminImagesError::NotFound {
                    kind,
                    name,
                    available: join_active_names(data, kind),
                });
            }
            let entry = data.firmwares.get_mut(&name).unwrap();
            if let Some(hash) = patch.hash {
                entry.hash = hash;
            }
            if let Some(v) = patch.display_name {
                entry.display_name = v;
            }
            if let Some(v) = patch.description {
                entry.description = v;
            }
        }
    }
    Ok(())
}

fn apply_deprecate(
    data: &mut VmImagesData,
    kind: Kind,
    name: String,
) -> Result<(), AdminImagesError> {
    set_deprecated_flag(data, kind, name, true)
}

fn apply_undeprecate(
    data: &mut VmImagesData,
    kind: Kind,
    name: String,
) -> Result<(), AdminImagesError> {
    set_deprecated_flag(data, kind, name, false)
}

fn set_deprecated_flag(
    data: &mut VmImagesData,
    kind: Kind,
    name: String,
    target: bool,
) -> Result<(), AdminImagesError> {
    let exists = match kind {
        Kind::Rootfs => data.rootfs.contains_key(&name),
        Kind::Runtime => data.runtimes.contains_key(&name),
        Kind::Firmware => data.firmwares.contains_key(&name),
    };
    if !exists {
        let available = join_active_names(data, kind);
        return Err(AdminImagesError::NotFound { kind, name, available });
    }
    let was = match kind {
        Kind::Rootfs => data.rootfs.get(&name).unwrap().deprecated,
        Kind::Runtime => data.runtimes.get(&name).unwrap().deprecated,
        Kind::Firmware => data.firmwares.get(&name).unwrap().deprecated,
    };
    if was == target {
        return if target {
            Err(AdminImagesError::AlreadyDeprecated { kind, name })
        } else {
            Err(AdminImagesError::NotDeprecated { kind, name })
        };
    }
    match kind {
        Kind::Rootfs => data.rootfs.get_mut(&name).unwrap().deprecated = target,
        Kind::Runtime => data.runtimes.get_mut(&name).unwrap().deprecated = target,
        Kind::Firmware => data.firmwares.get_mut(&name).unwrap().deprecated = target,
    }
    Ok(())
}

fn join_active_names(data: &VmImagesData, kind: Kind) -> String {
    let mut names: Vec<&str> = match kind {
        Kind::Rootfs => data
            .active_rootfs()
            .into_iter()
            .map(|(n, _)| n)
            .collect(),
        Kind::Runtime => data
            .active_runtimes()
            .into_iter()
            .map(|(n, _)| n)
            .collect(),
        Kind::Firmware => data
            .active_firmwares()
            .into_iter()
            .map(|(n, _)| n)
            .collect(),
    };
    names.sort_unstable();
    names.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_sdk::aggregate_models::vm_images::{
        ImageEntry, RootfsEntry, VmImagesData,
    };
    use std::str::FromStr;

    fn rootfs_hash() -> ItemHash {
        ItemHash::from_str(
            "5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e",
        )
        .unwrap()
    }

    fn other_hash() -> ItemHash {
        ItemHash::from_str(
            "4a0f62da42f4478544616519e6f5d58adb1096e069b392b151d47c3609492d0c",
        )
        .unwrap()
    }

    fn rootfs_entry() -> RootfsEntry {
        RootfsEntry {
            hash: rootfs_hash(),
            display_name: Some("Ubuntu 24.04".into()),
            description: None,
            min_disk_mib: Some(20480),
            deprecated: false,
        }
    }

    fn image_entry() -> ImageEntry {
        ImageEntry {
            hash: rootfs_hash(),
            display_name: None,
            description: None,
            deprecated: false,
        }
    }

    fn new_rootfs() -> NewEntry {
        NewEntry {
            hash: other_hash(),
            display_name: Some("Ubuntu 22.04".into()),
            description: Some("Jammy".into()),
            min_disk_mib: Some(20480),
            deprecated: false,
        }
    }

    fn new_runtime() -> NewEntry {
        NewEntry {
            hash: other_hash(),
            display_name: Some("Python 3.11".into()),
            description: None,
            min_disk_mib: None,
            deprecated: false,
        }
    }

    #[test]
    fn apply_add_rootfs_happy() {
        let mut data = VmImagesData::default();
        apply_mutation(
            &mut data,
            Mutation::Add {
                kind: Kind::Rootfs,
                name: "ubuntu22".into(),
                entry: new_rootfs(),
            },
        )
        .unwrap();
        let entry = data.rootfs.get("ubuntu22").unwrap();
        assert_eq!(entry.hash, other_hash());
        assert_eq!(entry.display_name.as_deref(), Some("Ubuntu 22.04"));
        assert_eq!(entry.min_disk_mib, Some(20480));
        assert!(!entry.deprecated);
    }

    #[test]
    fn apply_add_runtime_happy() {
        let mut data = VmImagesData::default();
        apply_mutation(
            &mut data,
            Mutation::Add {
                kind: Kind::Runtime,
                name: "py311".into(),
                entry: new_runtime(),
            },
        )
        .unwrap();
        let entry = data.runtimes.get("py311").unwrap();
        assert_eq!(entry.hash, other_hash());
        assert_eq!(entry.display_name.as_deref(), Some("Python 3.11"));
    }

    #[test]
    fn apply_add_firmware_happy() {
        let mut data = VmImagesData::default();
        apply_mutation(
            &mut data,
            Mutation::Add {
                kind: Kind::Firmware,
                name: "ovmf".into(),
                entry: new_runtime(),
            },
        )
        .unwrap();
        assert!(data.firmwares.contains_key("ovmf"));
    }

    #[test]
    fn apply_add_rejects_existing_name() {
        let mut data = VmImagesData::default();
        data.rootfs.insert("ubuntu24".into(), rootfs_entry());
        let err = apply_mutation(
            &mut data,
            Mutation::Add {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
                entry: new_rootfs(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminImagesError::AlreadyExists { .. }));
    }

    #[test]
    fn apply_add_runtime_with_min_disk_mib_rejected() {
        let mut data = VmImagesData::default();
        let mut entry = new_runtime();
        entry.min_disk_mib = Some(1024);
        let err = apply_mutation(
            &mut data,
            Mutation::Add {
                kind: Kind::Runtime,
                name: "py311".into(),
                entry,
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminImagesError::IrrelevantField));
    }

    #[test]
    fn apply_add_firmware_with_min_disk_mib_rejected() {
        let mut data = VmImagesData::default();
        let mut entry = new_runtime();
        entry.min_disk_mib = Some(1024);
        let err = apply_mutation(
            &mut data,
            Mutation::Add {
                kind: Kind::Firmware,
                name: "ovmf".into(),
                entry,
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminImagesError::IrrelevantField));
    }

    #[test]
    fn apply_update_patches_only_provided_fields() {
        let mut data = VmImagesData::default();
        data.rootfs.insert("ubuntu24".into(), rootfs_entry());

        let patch = EntryPatch {
            display_name: Some(Some("Ubuntu 24.04 LTS".into())),
            ..Default::default()
        };
        apply_mutation(
            &mut data,
            Mutation::Update {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
                patch,
            },
        )
        .unwrap();
        let entry = data.rootfs.get("ubuntu24").unwrap();
        assert_eq!(entry.display_name.as_deref(), Some("Ubuntu 24.04 LTS"));
        assert_eq!(entry.hash, rootfs_hash());
        assert_eq!(entry.min_disk_mib, Some(20480));
    }

    #[test]
    fn apply_update_clear_display_name() {
        let mut data = VmImagesData::default();
        data.rootfs.insert("ubuntu24".into(), rootfs_entry());

        let patch = EntryPatch {
            display_name: Some(None),
            ..Default::default()
        };
        apply_mutation(
            &mut data,
            Mutation::Update {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
                patch,
            },
        )
        .unwrap();
        assert!(data.rootfs.get("ubuntu24").unwrap().display_name.is_none());
    }

    #[test]
    fn apply_update_clear_min_disk_mib() {
        let mut data = VmImagesData::default();
        data.rootfs.insert("ubuntu24".into(), rootfs_entry());

        let patch = EntryPatch {
            min_disk_mib: Some(None),
            ..Default::default()
        };
        apply_mutation(
            &mut data,
            Mutation::Update {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
                patch,
            },
        )
        .unwrap();
        assert_eq!(data.rootfs.get("ubuntu24").unwrap().min_disk_mib, None);
    }

    #[test]
    fn apply_update_change_hash() {
        let mut data = VmImagesData::default();
        data.rootfs.insert("ubuntu24".into(), rootfs_entry());

        let patch = EntryPatch {
            hash: Some(other_hash()),
            ..Default::default()
        };
        apply_mutation(
            &mut data,
            Mutation::Update {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
                patch,
            },
        )
        .unwrap();
        assert_eq!(data.rootfs.get("ubuntu24").unwrap().hash, other_hash());
    }

    #[test]
    fn apply_update_rejects_unknown_name() {
        let mut data = VmImagesData::default();
        data.rootfs.insert("ubuntu24".into(), rootfs_entry());

        let patch = EntryPatch {
            display_name: Some(Some("foo".into())),
            ..Default::default()
        };
        let err = apply_mutation(
            &mut data,
            Mutation::Update {
                kind: Kind::Rootfs,
                name: "nope".into(),
                patch,
            },
        )
        .unwrap_err();
        match err {
            AdminImagesError::NotFound { kind, name, available } => {
                assert_eq!(kind, Kind::Rootfs);
                assert_eq!(name, "nope");
                assert!(available.contains("ubuntu24"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn apply_update_runtime_rejects_min_disk_mib() {
        let mut data = VmImagesData::default();
        data.runtimes.insert("py311".into(), image_entry());

        let patch = EntryPatch {
            min_disk_mib: Some(Some(1024)),
            ..Default::default()
        };
        let err = apply_mutation(
            &mut data,
            Mutation::Update {
                kind: Kind::Runtime,
                name: "py311".into(),
                patch,
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminImagesError::IrrelevantField));
    }

    #[test]
    fn apply_update_runtime_rejects_clear_min_disk_mib() {
        let mut data = VmImagesData::default();
        data.runtimes.insert("py311".into(), image_entry());

        let patch = EntryPatch {
            min_disk_mib: Some(None),
            ..Default::default()
        };
        let err = apply_mutation(
            &mut data,
            Mutation::Update {
                kind: Kind::Runtime,
                name: "py311".into(),
                patch,
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminImagesError::IrrelevantField));
    }

    #[test]
    fn apply_update_rejects_empty_patch() {
        let mut data = VmImagesData::default();
        data.rootfs.insert("ubuntu24".into(), rootfs_entry());

        let err = apply_mutation(
            &mut data,
            Mutation::Update {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
                patch: EntryPatch::default(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminImagesError::NoFieldsToUpdate));
    }

    #[test]
    fn apply_deprecate_flips_flag() {
        let mut data = VmImagesData::default();
        data.rootfs.insert("ubuntu24".into(), rootfs_entry());

        apply_mutation(
            &mut data,
            Mutation::Deprecate {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
            },
        )
        .unwrap();
        assert!(data.rootfs.get("ubuntu24").unwrap().deprecated);
    }

    #[test]
    fn apply_deprecate_runtime_flips_flag() {
        let mut data = VmImagesData::default();
        data.runtimes.insert("py311".into(), image_entry());

        apply_mutation(
            &mut data,
            Mutation::Deprecate {
                kind: Kind::Runtime,
                name: "py311".into(),
            },
        )
        .unwrap();
        assert!(data.runtimes.get("py311").unwrap().deprecated);
    }

    #[test]
    fn apply_deprecate_rejects_already_deprecated() {
        let mut data = VmImagesData::default();
        let mut entry = rootfs_entry();
        entry.deprecated = true;
        data.rootfs.insert("ubuntu24".into(), entry);

        let err = apply_mutation(
            &mut data,
            Mutation::Deprecate {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminImagesError::AlreadyDeprecated { .. }));
    }

    #[test]
    fn apply_deprecate_rejects_unknown_name() {
        let mut data = VmImagesData::default();
        let err = apply_mutation(
            &mut data,
            Mutation::Deprecate {
                kind: Kind::Rootfs,
                name: "nope".into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminImagesError::NotFound { .. }));
    }

    #[test]
    fn apply_undeprecate_flips_flag() {
        let mut data = VmImagesData::default();
        let mut entry = rootfs_entry();
        entry.deprecated = true;
        data.rootfs.insert("ubuntu24".into(), entry);

        apply_mutation(
            &mut data,
            Mutation::Undeprecate {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
            },
        )
        .unwrap();
        assert!(!data.rootfs.get("ubuntu24").unwrap().deprecated);
    }

    #[test]
    fn apply_undeprecate_rejects_not_deprecated() {
        let mut data = VmImagesData::default();
        data.rootfs.insert("ubuntu24".into(), rootfs_entry());

        let err = apply_mutation(
            &mut data,
            Mutation::Undeprecate {
                kind: Kind::Rootfs,
                name: "ubuntu24".into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, AdminImagesError::NotDeprecated { .. }));
    }
}
