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
