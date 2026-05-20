//! VM-related tables.
//!
//! Mirrors `src/aleph/db/models/vms.py`. The Python module uses
//! polymorphic inheritance (`VmBaseDb` → `VmInstanceDb` / `ProgramDb` and
//! `MachineVolumeBaseDb` → `ImmutableVolumeDb` / `EphemeralVolumeDb` /
//! `PersistentVolumeDb`). In Rust we keep one struct per table with the
//! polymorphic discriminator stored as a typed enum, and per-subtype-only
//! columns as optional fields.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use aleph_types::message::execution::base::Encoding;
use aleph_types::message::execution::volume::VolumePersistence;

use crate::types::vms::{CpuArchitecture, VmType, VmVersion};
use crate::{AlephError, AlephResult};

fn vm_type_from_text(s: &str) -> VmType {
    try_vm_type_from_text(s).unwrap_or_else(|_| panic!("unknown VmType in DB: {s}"))
}

fn try_vm_type_from_text(s: &str) -> AlephResult<VmType> {
    serde_json::from_value::<VmType>(serde_json::Value::String(s.to_string()))
        .map_err(|_| AlephError::InvalidMessage(format!("unknown VmType in DB: {s}")))
}

fn encoding_from_text(s: &str) -> Encoding {
    try_encoding_from_text(s).unwrap_or_else(|_| panic!("unknown Encoding in DB: {s}"))
}

fn try_encoding_from_text(s: &str) -> AlephResult<Encoding> {
    serde_json::from_value::<Encoding>(serde_json::Value::String(s.to_string()))
        .map_err(|_| AlephError::InvalidMessage(format!("unknown Encoding in DB: {s}")))
}

fn persistence_from_text(s: &str) -> VolumePersistence {
    try_persistence_from_text(s)
        .unwrap_or_else(|_| panic!("unknown VolumePersistence in DB: {s}"))
}

fn try_persistence_from_text(s: &str) -> AlephResult<VolumePersistence> {
    serde_json::from_value::<VolumePersistence>(serde_json::Value::String(s.to_string()))
        .map_err(|_| AlephError::InvalidMessage(format!("unknown VolumePersistence in DB: {s}")))
}

fn cpu_arch_from_text(s: &str) -> CpuArchitecture {
    try_cpu_arch_from_text(s).unwrap_or_else(|_| panic!("unknown CpuArchitecture in DB: {s}"))
}

fn try_cpu_arch_from_text(s: &str) -> AlephResult<CpuArchitecture> {
    serde_json::from_value::<CpuArchitecture>(serde_json::Value::String(s.to_string()))
        .map_err(|_| AlephError::InvalidMessage(format!("unknown CpuArchitecture in DB: {s}")))
}

/// Machine type, as known to `aleph_message`. Mirrors
/// `aleph_message.models.execution.MachineType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MachineType {
    #[serde(rename = "vm-instance")]
    VmInstance,
    #[serde(rename = "vm-function")]
    VmFunction,
}

impl MachineType {
    pub fn as_value_str(self) -> &'static str {
        match self {
            MachineType::VmInstance => "vm-instance",
            MachineType::VmFunction => "vm-function",
        }
    }
}

impl TryFrom<&str> for MachineType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "vm-instance" => Ok(MachineType::VmInstance),
            "vm-function" => Ok(MachineType::VmFunction),
            other => Err(format!("unknown MachineType: {other}")),
        }
    }
}

/// Discriminator for [`MachineVolumeDb`] — mirrors the polymorphic identity
/// values used by `MachineVolumeBaseDb`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MachineVolumeKind {
    Immutable,
    Ephemeral,
    Persistent,
}

impl MachineVolumeKind {
    pub fn as_value_str(self) -> &'static str {
        match self {
            MachineVolumeKind::Immutable => "immutable",
            MachineVolumeKind::Ephemeral => "ephemeral",
            MachineVolumeKind::Persistent => "persistent",
        }
    }
}

impl TryFrom<&str> for MachineVolumeKind {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "immutable" => Ok(MachineVolumeKind::Immutable),
            "ephemeral" => Ok(MachineVolumeKind::Ephemeral),
            "persistent" => Ok(MachineVolumeKind::Persistent),
            other => Err(format!("unknown MachineVolumeKind: {other}")),
        }
    }
}

/// Row of the `instance_rootfs` table.
#[derive(Debug, Clone)]
pub struct RootfsVolumeDb {
    pub instance_hash: String,
    pub parent_ref: String,
    pub parent_use_latest: bool,
    pub size_mib: i32,
    pub persistence: VolumePersistence,
}

impl RootfsVolumeDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let persistence_s: String = row.get("persistence");
        Self {
            instance_hash: row.get("instance_hash"),
            parent_ref: row.get("parent_ref"),
            parent_use_latest: row.get("parent_use_latest"),
            size_mib: row.get("size_mib"),
            persistence: persistence_from_text(&persistence_s),
        }
    }
}

/// Row of the `program_code_volumes` table.
#[derive(Debug, Clone)]
pub struct CodeVolumeDb {
    pub program_hash: String,
    pub encoding: Encoding,
    pub r#ref: Option<String>,
    pub use_latest: Option<bool>,
    pub entrypoint: String,
}

impl CodeVolumeDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let enc_s: String = row.get("encoding");
        Self {
            program_hash: row.get("program_hash"),
            encoding: encoding_from_text(&enc_s),
            r#ref: row.get("ref"),
            use_latest: row.get("use_latest"),
            entrypoint: row.get("entrypoint"),
        }
    }
}

/// Row of the `program_data_volumes` table.
#[derive(Debug, Clone)]
pub struct DataVolumeDb {
    pub program_hash: String,
    pub encoding: Encoding,
    pub r#ref: Option<String>,
    pub use_latest: Option<bool>,
    pub mount: String,
}

impl DataVolumeDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let enc_s: String = row.get("encoding");
        Self {
            program_hash: row.get("program_hash"),
            encoding: encoding_from_text(&enc_s),
            r#ref: row.get("ref"),
            use_latest: row.get("use_latest"),
            mount: row.get("mount"),
        }
    }
}

/// Row of the `program_export_volumes` table.
#[derive(Debug, Clone)]
pub struct ExportVolumeDb {
    pub program_hash: String,
    pub encoding: Encoding,
}

impl ExportVolumeDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let enc_s: String = row.get("encoding");
        Self {
            program_hash: row.get("program_hash"),
            encoding: encoding_from_text(&enc_s),
        }
    }
}

/// Row of the `program_runtimes` table.
#[derive(Debug, Clone)]
pub struct RuntimeDb {
    pub program_hash: String,
    pub r#ref: Option<String>,
    pub use_latest: Option<bool>,
    pub comment: String,
}

impl RuntimeDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self {
            program_hash: row.get("program_hash"),
            r#ref: row.get("ref"),
            use_latest: row.get("use_latest"),
            comment: row.get("comment"),
        }
    }
}

/// Row of the `vm_machine_volumes` table. Polymorphic shape: each kind uses a
/// subset of the optional fields.
#[derive(Debug, Clone)]
pub struct MachineVolumeDb {
    pub id: i32,
    pub kind: MachineVolumeKind,
    pub vm_hash: String,
    pub comment: Option<String>,
    pub mount: Option<String>,
    pub size_mib: Option<i32>,
    // Immutable-only
    pub r#ref: Option<String>,
    pub use_latest: Option<bool>,
    // Persistent-only
    pub parent_ref: Option<String>,
    pub parent_use_latest: Option<bool>,
    pub persistence: Option<VolumePersistence>,
    pub name: Option<String>,
}

impl MachineVolumeDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self::try_from_row(row).expect("valid MachineVolumeDb row")
    }

    pub fn try_from_row(row: &tokio_postgres::Row) -> AlephResult<Self> {
        let type_s: String = row.get("type");
        let persistence: Option<String> = row.try_get("persistence").ok().flatten();
        let kind = MachineVolumeKind::try_from(type_s.as_str())
            .map_err(|e| AlephError::InvalidMessage(format!("{e} in DB")))?;
        let persistence = persistence
            .as_deref()
            .map(try_persistence_from_text)
            .transpose()?;
        Ok(Self {
            id: row.get("id"),
            kind,
            vm_hash: row.get("vm_hash"),
            comment: row.get("comment"),
            mount: row.get("mount"),
            size_mib: row.get("size_mib"),
            r#ref: row.try_get("ref").ok().flatten(),
            use_latest: row.try_get("use_latest").ok().flatten(),
            parent_ref: row.try_get("parent_ref").ok().flatten(),
            parent_use_latest: row.try_get("parent_use_latest").ok().flatten(),
            persistence,
            name: row.try_get("name").ok().flatten(),
        })
    }
}

/// Row of the `vms` table. Carries fields common to both VM kinds. Program-only
/// fields are exposed on [`ProgramDb`], which packages a `VmBaseDb` with the
/// extra program columns.
#[derive(Debug, Clone)]
pub struct VmBaseDb {
    pub item_hash: String,
    pub owner: String,
    pub r#type: VmType,
    pub allow_amend: bool,
    pub metadata: Option<Value>,
    pub variables: Option<Value>,
    pub message_triggers: Option<Value>,
    pub environment_reproducible: bool,
    pub environment_internet: bool,
    pub environment_aleph_api: bool,
    pub environment_shared_cache: bool,
    pub environment_trusted_execution_policy: Option<i32>,
    pub environment_trusted_execution_firmware: Option<String>,
    pub payment_type: Option<String>,
    pub resources_vcpus: i32,
    pub resources_memory: i32,
    pub resources_seconds: i32,
    pub cpu_architecture: Option<CpuArchitecture>,
    pub cpu_vendor: Option<String>,
    pub node_owner: Option<String>,
    pub node_address_regex: Option<String>,
    pub node_hash: Option<String>,
    pub replaces: Option<String>,
    pub created: DateTime<Utc>,
    pub authorized_keys: Option<Value>,
    // Program-only columns (NULL for instances)
    pub program_type: Option<MachineType>,
    pub http_trigger: Option<bool>,
    pub persistent: Option<bool>,
}

impl VmBaseDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        Self::try_from_row(row).expect("valid VmBaseDb row")
    }

    pub fn try_from_row(row: &tokio_postgres::Row) -> AlephResult<Self> {
        let type_s: String = row.get("type");
        let cpu_arch: Option<String> = row.get("cpu_architecture");
        let program_type: Option<String> = row.try_get("program_type").ok().flatten();
        let cpu_architecture = cpu_arch
            .as_deref()
            .map(try_cpu_arch_from_text)
            .transpose()?;
        let program_type = program_type
            .as_deref()
            .map(MachineType::try_from)
            .transpose()
            .map_err(|e| AlephError::InvalidMessage(format!("{e} in DB")))?;
        Ok(Self {
            item_hash: row.get("item_hash"),
            owner: row.get("owner"),
            r#type: try_vm_type_from_text(&type_s)?,
            allow_amend: row.get("allow_amend"),
            metadata: row.get("metadata"),
            variables: row.get("variables"),
            message_triggers: row.get("message_triggers"),
            environment_reproducible: row.get("environment_reproducible"),
            environment_internet: row.get("environment_internet"),
            environment_aleph_api: row.get("environment_aleph_api"),
            environment_shared_cache: row.get("environment_shared_cache"),
            environment_trusted_execution_policy: row.get("environment_trusted_execution_policy"),
            environment_trusted_execution_firmware: row
                .get("environment_trusted_execution_firmware"),
            payment_type: row.get("payment_type"),
            resources_vcpus: row.get("resources_vcpus"),
            resources_memory: row.get("resources_memory"),
            resources_seconds: row.get("resources_seconds"),
            cpu_architecture,
            cpu_vendor: row.get("cpu_vendor"),
            node_owner: row.get("node_owner"),
            node_address_regex: row.get("node_address_regex"),
            node_hash: row.get("node_hash"),
            replaces: row.get("replaces"),
            created: row.get("created"),
            authorized_keys: row.get("authorized_keys"),
            program_type,
            http_trigger: row.try_get("http_trigger").ok().flatten(),
            persistent: row.try_get("persistent").ok().flatten(),
        })
    }
}

/// Row of the `vm_versions` table.
#[derive(Debug, Clone)]
pub struct VmVersionDb {
    pub vm_hash: String,
    pub owner: String,
    pub current_version: VmVersion,
    pub last_updated: DateTime<Utc>,
}

impl VmVersionDb {
    pub fn from_row(row: &tokio_postgres::Row) -> Self {
        let cv: String = row.get("current_version");
        Self {
            vm_hash: row.get("vm_hash"),
            owner: row.get("owner"),
            current_version: VmVersion::from(cv),
            last_updated: row.get("last_updated"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_type_roundtrip() {
        for v in [MachineType::VmInstance, MachineType::VmFunction] {
            let s = v.as_value_str();
            assert_eq!(MachineType::try_from(s).unwrap(), v);
            let json = serde_json::to_string(&v).unwrap();
            let back: MachineType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v);
        }
        assert!(MachineType::try_from("nope").is_err());
    }

    #[test]
    fn machine_volume_kind_roundtrip() {
        for v in [
            MachineVolumeKind::Immutable,
            MachineVolumeKind::Ephemeral,
            MachineVolumeKind::Persistent,
        ] {
            let s = v.as_value_str();
            assert_eq!(MachineVolumeKind::try_from(s).unwrap(), v);
        }
        assert!(MachineVolumeKind::try_from("nope").is_err());
    }

    #[test]
    fn invalid_db_enums_return_errors() {
        assert!(try_vm_type_from_text("nope").is_err());
        assert!(try_encoding_from_text("nope").is_err());
        assert!(try_persistence_from_text("nope").is_err());
        assert!(try_cpu_arch_from_text("nope").is_err());
        assert!(MachineType::try_from("nope").is_err());
        assert!(MachineVolumeKind::try_from("nope").is_err());
    }

    #[test]
    fn rootfs_volume_construct() {
        let r = RootfsVolumeDb {
            instance_hash: "deadbeef".into(),
            parent_ref: "ref".into(),
            parent_use_latest: true,
            size_mib: 1024,
            persistence: VolumePersistence::Host,
        };
        assert_eq!(r.size_mib, 1024);
        assert_eq!(r.persistence, VolumePersistence::Host);
    }

    #[test]
    fn vm_base_construct() {
        let v = VmBaseDb {
            item_hash: "h".into(),
            owner: "0x".into(),
            r#type: VmType::Instance,
            allow_amend: false,
            metadata: None,
            variables: None,
            message_triggers: None,
            environment_reproducible: false,
            environment_internet: true,
            environment_aleph_api: true,
            environment_shared_cache: false,
            environment_trusted_execution_policy: None,
            environment_trusted_execution_firmware: None,
            payment_type: None,
            resources_vcpus: 2,
            resources_memory: 2048,
            resources_seconds: 86400,
            cpu_architecture: Some(CpuArchitecture::X86_64),
            cpu_vendor: None,
            node_owner: None,
            node_address_regex: None,
            node_hash: None,
            replaces: None,
            created: Utc::now(),
            authorized_keys: None,
            program_type: None,
            http_trigger: None,
            persistent: None,
        };
        assert_eq!(v.resources_vcpus, 2);
        assert_eq!(v.cpu_architecture, Some(CpuArchitecture::X86_64));
    }

    #[test]
    fn vm_version_construct() {
        let v = VmVersionDb {
            vm_hash: "h".into(),
            owner: "0x".into(),
            current_version: VmVersion::from("v1"),
            last_updated: Utc::now(),
        };
        assert_eq!(v.current_version.as_str(), "v1");
    }
}
