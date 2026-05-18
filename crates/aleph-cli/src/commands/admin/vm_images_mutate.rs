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
        Mutation::Update { .. }
        | Mutation::Deprecate { .. }
        | Mutation::Undeprecate { .. }
        | Mutation::SetDefault { .. }
        | Mutation::ClearDefault { .. } => {
            unimplemented!("subsequent tasks implement these variants")
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
}
