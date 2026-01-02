use crate::chain::Address;
use crate::item_hash::ItemHash;
use crate::memory_size::{MemorySize, MiB};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionTriggers {
    /// Route HTTP requests to the program.
    pub http: bool,
    #[serde(default)]
    pub persistent: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NetworkProtocol {
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "udp")]
    Udp,
}

fn default_tcp() -> NetworkProtocol {
    NetworkProtocol::Tcp
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Port(u16);

/// IPv4 port to forward from a randomly assigned port on the host to the VM.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishedPort {
    #[serde(default = "default_tcp")]
    protocol: NetworkProtocol,
    /// Port to expose on the guest.
    port: Port,
}

fn default_vcpus() -> u32 {
    1
}

fn default_memory() -> MiB {
    MiB::from_units(128)
}

fn default_seconds() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MachineResources {
    #[serde(default = "default_vcpus")]
    pub vcpus: u32,
    #[serde(default = "default_memory")]
    pub memory: MiB,
    #[serde(default = "default_seconds")]
    pub seconds: u32,
    /// Guest IPv4 ports to map to open ports on the host.
    #[serde(default)]
    pub published_ports: Option<Vec<PublishedPort>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Architecture {
    #[serde(rename = "x86_64")]
    X86_64,
    #[serde(rename = "arm64")]
    Arm64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Vendor {
    #[serde(rename = "AuthenticAMD")]
    Amd,
    #[serde(rename = "GenuineIntel")]
    Intel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// CPU features required by the virtual machine. Examples: 'sev', 'sev_es', 'sev_snp'.
pub struct CpuFeature(String);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CpuProperties {
    /// CPU architecture.
    pub architecture: Architecture,
    /// CPU vendor.
    pub vendor: Option<Vendor>,
    /// CPU features required by the virtual machine. Examples: 'sev', 'sev_es', 'sev_snp'.
    pub features: Vec<CpuFeature>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// GPU device class. See <https://admin.pci-ids.ucw.cz/read/PD/03>.
pub enum GpuDeviceClass {
    #[serde(rename = "0300")]
    VgaCompatibleController,
    #[serde(rename = "0302")]
    _3DController,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GpuProperties {
    /// GPU vendor name.
    pub vendor: String,
    /// GPU vendor card name.
    pub device_name: String,
    /// GPU device class. See <https://admin.pci-ids.ucw.cz/read/PD/03>.
    pub device_class: GpuDeviceClass,
    /// GPU vendor & device IDs.
    pub device_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Hypervisor {
    #[serde(rename = "firecracker")]
    Firecracker,
    #[serde(rename = "qemu")]
    Qemu,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionEnvironment {
    #[serde(default)]
    pub reproducible: bool,
    #[serde(default)]
    pub internet: bool,
    #[serde(default)]
    pub aleph_api: bool,
    #[serde(default)]
    pub shared_cache: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AmdSevPolicy {
    /// Debugging of the guest is disallowed.
    NoDebug = 0b1,
    /// Sharing keys with other guests is disallowed.
    NoKeySharing = 0b10,
    /// SEV-ES is required.
    SevEs = 0b100,
    /// Sending the guest to another platform is disallowed.
    NoSend = 0b1000,
    /// The guest must not be transmitted to another platform not in the domain.
    Domain = 0b10000,
    /// The guest must not be transmitted to another platform that is not SEV capable.
    Sev = 0b100000,
}

fn default_amd_sev_policy() -> u32 {
    AmdSevPolicy::NoDebug as u32
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrustedExecutionEnvironment {
    /// OVMF firmware to use.
    pub firmware: Option<ItemHash>,
    /// SEV Policy. The default value is 0x01 for SEV without debugging.
    #[serde(default = "default_amd_sev_policy")]
    pub policy: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceEnvironment {
    #[serde(default)]
    pub internet: bool,
    #[serde(default)]
    pub aleph_api: bool,
    /// Hypervisor to use. Default is Qemu.
    pub hypervisor: Option<Hypervisor>,
    /// Trusted Execution Environment properties. Defaults to no TEE.
    pub trusted_execution: Option<TrustedExecutionEnvironment>,
    // The following fields are kept for retro-compatibility.
    #[serde(default)]
    pub reproducible: bool,
    #[serde(default)]
    pub shared_cache: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeRequirements {
    /// Address of the node owner.
    pub owner: Option<Address>,
    /// Node address must match this regular expression.
    pub address_regex: Option<String>,
    /// Hash of the compute resource node that must be used.
    pub node_hash: Option<String>,
    /// Terms and conditions of this CRN.
    pub terms_and_conditions: Option<ItemHash>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostRequirements {
    /// Required CPU properties.
    pub cpu: Option<CpuProperties>,
    /// Required Compute Resource Node properties.
    pub node: Option<NodeRequirements>,
    /// GPUs needed to pass-through from the host.
    pub gpu: Option<Vec<GpuProperties>>,
}
