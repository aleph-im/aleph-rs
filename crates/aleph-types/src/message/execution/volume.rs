use crate::item_hash::ItemHash;
use crate::storage_size::{MemorySize, MiB, gigabyte_to_mebibyte};
use crate::toolkit::serde::default_true;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum VolumeError {
    #[error("value {size} is out of range ({min}..={max})")]
    OutOfRange { size: u64, min: u64, max: u64 },
}

pub trait IsReadOnly {
    fn is_read_only() -> bool;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseVolume {
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub mount: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImmutableVolume {
    #[serde(flatten)]
    pub base: BaseVolume,
    #[serde(default, rename = "ref")]
    pub reference: Option<ItemHash>,
    #[serde(default = "default_true")]
    pub use_latest: bool,
}

impl IsReadOnly for ImmutableVolume {
    fn is_read_only() -> bool {
        true
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct EphemeralVolumeSize(MiB);

impl EphemeralVolumeSize {
    const MIN: u64 = 1;
    const MAX: u64 = 1000;
}

impl TryFrom<u64> for EphemeralVolumeSize {
    type Error = VolumeError;

    fn try_from(size: u64) -> Result<Self, Self::Error> {
        if (Self::MIN..=Self::MAX).contains(&size) {
            Ok(Self(MiB::from_units(size)))
        } else {
            Err(VolumeError::OutOfRange {
                size,
                min: Self::MIN,
                max: Self::MAX,
            })
        }
    }
}

impl From<EphemeralVolumeSize> for u64 {
    fn from(size: EphemeralVolumeSize) -> Self {
        size.0.units()
    }
}

/// Ephemeral volume.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EphemeralVolume {
    #[serde(flatten)]
    pub base: BaseVolume,
    ephemeral: bool,
    size_mib: EphemeralVolumeSize,
}

impl IsReadOnly for EphemeralVolume {
    fn is_read_only() -> bool {
        false
    }
}

/// A reference volume to copy as a persistent volume.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParentVolume {
    #[serde(rename = "ref")]
    pub reference: ItemHash,
    #[serde(default = "default_true")]
    pub use_latest: bool,
}

/// Where to persist the volume.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumePersistence {
    Host,
    Store,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct PersistentVolumeSize(MiB);

impl PersistentVolumeSize {
    const MIN: u64 = 1;
    const MAX: u64 = gigabyte_to_mebibyte(2048);
}

impl TryFrom<u64> for PersistentVolumeSize {
    type Error = VolumeError;

    fn try_from(size: u64) -> Result<Self, Self::Error> {
        if (Self::MIN..=Self::MAX).contains(&size) {
            Ok(Self(MiB::from_units(size)))
        } else {
            Err(VolumeError::OutOfRange {
                size,
                min: Self::MIN,
                max: Self::MAX,
            })
        }
    }
}

impl From<PersistentVolumeSize> for u64 {
    fn from(size: PersistentVolumeSize) -> Self {
        size.0.units()
    }
}

impl From<MiB> for PersistentVolumeSize {
    fn from(size: MiB) -> Self {
        Self(size)
    }
}

impl From<PersistentVolumeSize> for MiB {
    fn from(value: PersistentVolumeSize) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistentVolume {
    #[serde(flatten)]
    pub base: BaseVolume,
    #[serde(default)]
    pub parent: Option<ParentVolume>,
    #[serde(default)]
    pub persistence: Option<VolumePersistence>,
    #[serde(default)]
    pub name: Option<String>,
    size_mib: PersistentVolumeSize,
}

impl IsReadOnly for PersistentVolume {
    fn is_read_only() -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MachineVolume {
    Immutable(ImmutableVolume),
    Ephemeral(EphemeralVolume),
    Persistent(PersistentVolume),
}

/// Root file system of a VM instance.
///
/// The root file system of an instance is built as a copy of a reference image, named parent
/// image. The user determines a custom size and persistence model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RootfsVolume {
    pub parent: ParentVolume,
    pub persistence: VolumePersistence,
    pub size_mib: PersistentVolumeSize,
    #[serde(default)]
    pub forgotten_by: Option<Vec<ItemHash>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// Sanity test for the read-only property on each volume type.
    fn test_is_read_only() {
        assert!(ImmutableVolume::is_read_only());
        assert!(!EphemeralVolume::is_read_only());
        assert!(!PersistentVolume::is_read_only());
    }
}
