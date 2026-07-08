use crate::chain::Address;
use crate::item_hash::ItemHash;
use memsizes::MiB;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionTriggers {
    /// Route HTTP requests to the program.
    pub http: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    MiB::from(128)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
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

/// SEV-SNP guest policy (64-bit). Bit 17 is reserved and must be 1; 0x30000
/// also sets bit 16 (SMT allowed), the recommended default.
pub const SNP_POLICY_RESERVED_BIT_17: u64 = 1 << 17;
pub const DEFAULT_SNP_POLICY: u64 = 0x30000;
/// Maximum number of launch-measurement annotations per TEE config.
pub const MAX_MEASUREMENTS: usize = 16;

#[derive(thiserror::Error, Debug)]
pub enum TeeError {
    #[error(
        "SEV-SNP guest policy must have reserved bit 17 set (e.g. {DEFAULT_SNP_POLICY:#x}); \
         got {0:#x}. SEV policy bit semantics do not apply to SEV-SNP"
    )]
    PolicyReservedBitUnset(u64),
    #[error("{platform} digest must be {expected} lowercase hex characters, got {got}")]
    BadDigestLength {
        platform: &'static str,
        expected: usize,
        got: usize,
    },
    #[error("digest must be lowercase hex")]
    DigestNotLowercaseHex,
    #[error("firmware belongs to the SEV flow and must not be set in sev_snp mode; use runtime instead")]
    FirmwareInSnpMode,
    #[error("sev_snp mode requires {0}")]
    SnpModeRequires(&'static str),
    #[error("{0} is only valid in sev_snp mode")]
    SnpOnlyField(&'static str),
    #[error("at most {MAX_MEASUREMENTS} measurements are allowed, got {0}")]
    TooManyMeasurements(usize),
}

/// Raise an error unless the value is a plausible SEV-SNP guest policy.
/// The u64 type already excludes negative and >64-bit values.
pub fn validate_snp_policy(policy: u64) -> Result<(), TeeError> {
    if policy & SNP_POLICY_RESERVED_BIT_17 == 0 {
        return Err(TeeError::PolicyReservedBitUnset(policy));
    }
    Ok(())
}

/// TEE platforms with defined launch-measurement semantics.
///
/// Grows over protocol upgrades (e.g. "tdx" with MRTD digests). Unknown
/// platforms are schema-invalid: nothing unverifiable gets network blessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TeePlatform {
    #[serde(rename = "sev_snp")]
    SevSnp,
}

impl TeePlatform {
    fn as_str(self) -> &'static str {
        match self {
            TeePlatform::SevSnp => "sev_snp",
        }
    }

    /// Expected hex length of a launch digest for this platform.
    /// sev_snp: 48-byte SHA-384 launch digest.
    fn digest_hex_len(self) -> usize {
        match self {
            TeePlatform::SevSnp => 96,
        }
    }
}

/// Supervisor-opaque verification annotation, validated by the CCN.
///
/// Declares the launch digest a verifier should expect. Multiple entries
/// (one per vcpu_type) keep a message verifiable across a mixed CPU fleet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawLaunchMeasurement")]
pub struct LaunchMeasurement {
    /// TEE platform this digest applies to.
    pub platform: TeePlatform,
    /// Expected launch digest, lowercase hex; length is platform-defined.
    pub digest: String,
    /// QEMU CPU model this digest was computed for (e.g. "EPYC-v4").
    /// Required by direct-boot measurement recipes, absent for igvm bundles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcpu_type: Option<String>,
}

#[derive(Deserialize)]
struct RawLaunchMeasurement {
    platform: TeePlatform,
    digest: String,
    #[serde(default)]
    vcpu_type: Option<String>,
}

impl TryFrom<RawLaunchMeasurement> for LaunchMeasurement {
    type Error = TeeError;

    fn try_from(raw: RawLaunchMeasurement) -> Result<Self, Self::Error> {
        let expected = raw.platform.digest_hex_len();
        if raw.digest.len() != expected {
            return Err(TeeError::BadDigestLength {
                platform: raw.platform.as_str(),
                expected,
                got: raw.digest.len(),
            });
        }
        if !raw
            .digest
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(TeeError::DigestNotLowercaseHex);
        }
        Ok(Self {
            platform: raw.platform,
            digest: raw.digest,
            vcpu_type: raw.vcpu_type,
        })
    }
}

fn default_amd_sev_policy() -> u32 {
    AmdSevPolicy::NoDebug as u32
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrustedExecutionEnvironment {
    /// OVMF firmware to use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hypervisor: Option<Hypervisor>,
    /// Trusted Execution Environment properties. Defaults to no TEE.
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<Address>,
    /// Node address must match this regular expression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address_regex: Option<String>,
    /// Hash of the compute resource node that must be used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_hash: Option<String>,
    /// Terms and conditions of this CRN.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terms_and_conditions: Option<ItemHash>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostRequirements {
    /// Required CPU properties.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CpuProperties>,
    /// Required Compute Resource Node properties.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeRequirements>,
    /// GPUs needed to pass-through from the host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<Vec<GpuProperties>>,
}

#[cfg(test)]
mod test {
    use super::*;

    const SNP_DIGEST: &str =
        "abababababababababababababababababababababababababababababababababababababababababababababababab";

    #[test]
    fn test_launch_measurement_valid() {
        let m: LaunchMeasurement = serde_json::from_str(&format!(
            r#"{{"platform": "sev_snp", "digest": "{SNP_DIGEST}", "vcpu_type": "EPYC-v4"}}"#
        ))
        .unwrap();
        assert_eq!(m.platform, TeePlatform::SevSnp);
        assert_eq!(m.vcpu_type.as_deref(), Some("EPYC-v4"));
        // vcpu_type is optional: absent for igvm-recipe bundles
        let m: LaunchMeasurement = serde_json::from_str(&format!(
            r#"{{"platform": "sev_snp", "digest": "{SNP_DIGEST}"}}"#
        ))
        .unwrap();
        assert_eq!(m.vcpu_type, None);
    }

    #[test]
    fn test_launch_measurement_rejects_bad_digests() {
        // wrong length (sha256-sized), non-hex, uppercase hex
        for digest in ["cd".repeat(32), "zz".repeat(48), "AB".repeat(48)] {
            let json = format!(r#"{{"platform": "sev_snp", "digest": "{digest}"}}"#);
            assert!(
                serde_json::from_str::<LaunchMeasurement>(&json).is_err(),
                "digest {digest} should be rejected"
            );
        }
    }

    #[test]
    fn test_launch_measurement_rejects_unknown_platform() {
        let json = format!(r#"{{"platform": "tdx", "digest": "{SNP_DIGEST}"}}"#);
        assert!(serde_json::from_str::<LaunchMeasurement>(&json).is_err());
    }

    #[test]
    fn test_launch_measurement_roundtrip_omits_null_vcpu_type() {
        let m: LaunchMeasurement = serde_json::from_str(&format!(
            r#"{{"platform": "sev_snp", "digest": "{SNP_DIGEST}"}}"#
        ))
        .unwrap();
        let value = serde_json::to_value(&m).unwrap();
        let mut keys: Vec<&str> =
            value.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        keys.sort();
        // no vcpu_type key when None (serde_json orders keys alphabetically)
        assert_eq!(keys, vec!["digest", "platform"]);
    }

    #[test]
    fn test_validate_snp_policy() {
        validate_snp_policy(DEFAULT_SNP_POLICY).unwrap(); // 0x30000: bits 16+17
        validate_snp_policy(1 << 17).unwrap();
        validate_snp_policy((1 << 40) | (1 << 17)).unwrap(); // >u32 range is fine
        assert!(validate_snp_policy(0x1).is_err()); // the AMD SEV default lacks bit 17
        assert!(validate_snp_policy(0x10000).is_err()); // SMT bit alone, no bit 17
    }
}
