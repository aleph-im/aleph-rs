use crate::chain::Address;
use crate::item_hash::ItemHash;
use memsizes::MiB;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU16;

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
    #[error(
        "firmware belongs to the SEV flow and must not be set in {0} mode; use runtime instead"
    )]
    FirmwareInMeasuredMode(&'static str),
    #[error("{mode} mode requires {field}")]
    MeasuredModeRequires {
        mode: &'static str,
        field: &'static str,
    },
    #[error("{0} is only valid in the measured TEE modes (sev_snp, tdx)")]
    MeasuredOnlyField(&'static str),
    #[error("at most {MAX_MEASUREMENTS} measurements are allowed, got {0}")]
    TooManyMeasurements(usize),
    #[error("measurement platform {got} does not match the declared TEE {expected}")]
    MeasurementPlatformMismatch {
        expected: &'static str,
        got: &'static str,
    },
    #[error("platform {platform} was declared with {registers} registers")]
    RegistersPlatformMismatch {
        platform: &'static str,
        registers: &'static str,
    },
    #[error("tdx mode has no host-chosen launch policy; policy must be left at its default")]
    TdxPolicySet,
    #[error("V-PROGRAM supports only the sev_snp backend")]
    UnsupportedVProgramBackend,
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
/// Grows over protocol upgrades. Unknown platforms are schema-invalid:
/// nothing unverifiable gets network blessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TeePlatform {
    #[serde(rename = "sev_snp")]
    SevSnp,
    #[serde(rename = "tdx")]
    Tdx,
}

impl TeePlatform {
    /// The platform's wire string (its serde rename, e.g. "sev_snp"), for
    /// display sites that would otherwise round-trip through serde to get it.
    pub fn as_str(self) -> &'static str {
        match self {
            TeePlatform::SevSnp => "sev_snp",
            TeePlatform::Tdx => "tdx",
        }
    }
}

/// Every pinned register is a 48-byte SHA-384 value.
pub const REGISTER_HEX_LEN: usize = 96;

/// Deserialize a measurement register: exactly [`REGISTER_HEX_LEN`] lowercase
/// hex characters. Lowercase only, so two encodings of the same value can
/// never both validate.
///
/// A field-level hook rather than a newtype: the value is a plain `String`
/// everywhere it is used, and messages only ever arrive by deserialization, so
/// a wrapper type would buy nothing that this does not.
fn deserialize_register<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    let value = String::deserialize(deserializer)?;
    if value.len() != REGISTER_HEX_LEN {
        return Err(D::Error::custom(TeeError::BadDigestLength {
            platform: "register",
            expected: REGISTER_HEX_LEN,
            got: value.len(),
        }));
    }
    if !value
        .bytes()
        .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(D::Error::custom(TeeError::DigestNotLowercaseHex));
    }
    Ok(value)
}

/// The measurement registers SEV-SNP pins: one launch digest.
///
/// A TEE's launch identity is not always a single value. SEV-SNP has one
/// launch digest, while platforms such as Intel TDX spread it over several
/// hardware registers (MRTD plus RTMRs), which is why the wire shape is an
/// object rather than a scalar. Each platform gets a concrete struct rather
/// than a generic map: `deny_unknown_fields` plus required fields give the
/// closed key set natively, with no validator to keep in step, and no
/// unbounded map to parse before rejecting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SevSnpRegisters {
    #[serde(deserialize_with = "deserialize_register")]
    pub launch: String,
}

/// The measurement registers Intel TDX pins: firmware, boot chain, config.
///
/// The pinned set is `{mrtd, rtmr1, rtmr2, mrconfigid}`. `rtmr0` is
/// deliberately absent: TDVF extends the VMM-supplied memory layout and the
/// variable store into it, which are deployment parameters, not code
/// identity. `rtmr3` is absent because it carries the launch-TCB commitment,
/// which a verifier *derives* rather than compares against a declared
/// constant; keeping it out of the schema lets enforcement harden later
/// without a protocol change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TdxRegisters {
    #[serde(deserialize_with = "deserialize_register")]
    pub mrtd: String,
    #[serde(deserialize_with = "deserialize_register")]
    pub rtmr1: String,
    #[serde(deserialize_with = "deserialize_register")]
    pub rtmr2: String,
    #[serde(deserialize_with = "deserialize_register")]
    pub mrconfigid: String,
}

/// Per-platform measurement registers, discriminated on the sibling
/// `platform` field of [`LaunchMeasurement`].
///
/// The wire shape is untagged: the member key sets are disjoint and closed
/// (`deny_unknown_fields`), so parsing is unambiguous, and the
/// [`LaunchMeasurement`] constructor enforces that the parsed member matches
/// the declared platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MeasurementRegisters {
    SevSnp(SevSnpRegisters),
    Tdx(TdxRegisters),
}

impl MeasurementRegisters {
    /// The platform whose register set this is.
    pub fn platform(&self) -> TeePlatform {
        match self {
            MeasurementRegisters::SevSnp(_) => TeePlatform::SevSnp,
            MeasurementRegisters::Tdx(_) => TeePlatform::Tdx,
        }
    }

    /// The registers as (name, value) pairs in wire order, for display sites
    /// that render any platform's set without matching on it.
    pub fn entries(&self) -> Vec<(&'static str, &str)> {
        match self {
            MeasurementRegisters::SevSnp(r) => vec![("launch", r.launch.as_str())],
            MeasurementRegisters::Tdx(r) => vec![
                ("mrtd", r.mrtd.as_str()),
                ("rtmr1", r.rtmr1.as_str()),
                ("rtmr2", r.rtmr2.as_str()),
                ("mrconfigid", r.mrconfigid.as_str()),
            ],
        }
    }

    /// The SEV-SNP registers, if this is an SEV-SNP set.
    pub fn as_sev_snp(&self) -> Option<&SevSnpRegisters> {
        match self {
            MeasurementRegisters::SevSnp(r) => Some(r),
            MeasurementRegisters::Tdx(_) => None,
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
    /// TEE platform these registers apply to.
    pub platform: TeePlatform,
    /// Expected measurement registers; sev_snp declares `{"launch"}`, tdx
    /// declares `{"mrtd", "rtmr1", "rtmr2", "mrconfigid"}`.
    pub registers: MeasurementRegisters,
    /// QEMU CPU model these registers were computed for (e.g. "EPYC-v4").
    /// Required by direct-boot measurement recipes, absent for igvm bundles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcpu_type: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLaunchMeasurement {
    platform: TeePlatform,
    registers: MeasurementRegisters,
    #[serde(default)]
    vcpu_type: Option<String>,
}

impl TryFrom<RawLaunchMeasurement> for LaunchMeasurement {
    type Error = TeeError;

    fn try_from(raw: RawLaunchMeasurement) -> Result<Self, Self::Error> {
        if raw.registers.platform() != raw.platform {
            return Err(TeeError::RegistersPlatformMismatch {
                platform: raw.platform.as_str(),
                registers: raw.registers.platform().as_str(),
            });
        }
        Ok(Self {
            platform: raw.platform,
            registers: raw.registers,
            vcpu_type: raw.vcpu_type,
        })
    }
}

impl LaunchMeasurement {
    /// The SEV-SNP launch digest, i.e. the `launch` register; `None` for
    /// another platform's register set.
    pub fn snp_launch_digest(&self) -> Option<&str> {
        self.registers.as_sev_snp().map(|r| r.launch.as_str())
    }
}

fn default_amd_sev_policy() -> u64 {
    AmdSevPolicy::NoDebug as u64
}

/// TEE mode discriminator. Absent on legacy messages, which are SEV.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TeeMode {
    /// Explicit spelling of the legacy default: behaves exactly like leaving
    /// `mode` absent (see `check_mode_consistency`).
    #[serde(rename = "sev")]
    Sev,
    #[serde(rename = "sev_snp")]
    SevSnp,
    #[serde(rename = "tdx")]
    Tdx,
}

impl TeeMode {
    /// The mode's wire string (its serde rename, e.g. "sev_snp").
    pub fn as_str(self) -> &'static str {
        match self {
            TeeMode::Sev => "sev",
            TeeMode::SevSnp => "sev_snp",
            TeeMode::Tdx => "tdx",
        }
    }
}

/// Trusted Execution Environment properties.
///
/// Two families of modes coexist:
/// - mode None or Sev (legacy): AMD SEV/SEV-ES with the CRN-mediated
///   launch-secret flow; `firmware` references the confidential OVMF and
///   `policy` uses AMD SEV bit semantics (AmdSevPolicy).
/// - measured modes (SevSnp, Tdx): measured boot from a runtime bundle with
///   direct client-to-guest attestation; `measurements` carry the expected
///   registers. In sev_snp mode `policy` uses the SEV-SNP 64-bit
///   guest-policy semantics. In tdx mode there is no host-chosen launch
///   policy at all (TDATTRIBUTES and XFAM are set by the TDX module and
///   measured, not selected), so `policy` must be left at its default.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawTrustedExecutionEnvironment")]
pub struct TrustedExecutionEnvironment {
    /// OVMF firmware to use (SEV mode only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub firmware: Option<ItemHash>,
    /// TEE policy. SEV bit semantics in SEV mode (default 0x01, no debugging);
    /// SEV-SNP 64-bit guest policy in sev_snp mode.
    #[serde(default = "default_amd_sev_policy")]
    pub policy: u64,
    /// TEE mode; None means legacy SEV (kept for wire stability).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<TeeMode>,
    /// Measured runtime bundle store message (measured modes only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<ItemHash>,
    /// Expected measurement registers (measured modes only); CCN-validated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurements: Option<Vec<LaunchMeasurement>>,
    /// In-guest attestation port (measured modes only); None means the runtime
    /// bundle default (8443).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attestation_port: Option<NonZeroU16>,
}

impl TrustedExecutionEnvironment {
    pub fn is_snp(&self) -> bool {
        self.mode == Some(TeeMode::SevSnp)
    }

    /// Measured-boot modes: runtime bundle plus declared registers.
    pub fn is_measured(&self) -> bool {
        matches!(self.mode, Some(TeeMode::SevSnp) | Some(TeeMode::Tdx))
    }

    fn check_mode_consistency(&self) -> Result<(), TeeError> {
        match self.mode {
            Some(mode @ (TeeMode::SevSnp | TeeMode::Tdx)) => {
                let mode_str = mode.as_str();
                if self.firmware.is_some() {
                    return Err(TeeError::FirmwareInMeasuredMode(mode_str));
                }
                if self.runtime.is_none() {
                    return Err(TeeError::MeasuredModeRequires {
                        mode: mode_str,
                        field: "runtime",
                    });
                }
                let measurements = match self.measurements.as_deref() {
                    None | Some([]) => {
                        return Err(TeeError::MeasuredModeRequires {
                            mode: mode_str,
                            field: "measurements",
                        });
                    }
                    Some(m) if m.len() > MAX_MEASUREMENTS => {
                        return Err(TeeError::TooManyMeasurements(m.len()));
                    }
                    Some(m) => m,
                };
                // A measured TeeMode and its TeePlatform share a wire name
                // ("sev_snp", "tdx"); this comparison relies on keeping that
                // naming aligned when adding a platform.
                for measurement in measurements {
                    if measurement.platform.as_str() != mode_str {
                        return Err(TeeError::MeasurementPlatformMismatch {
                            expected: mode_str,
                            got: measurement.platform.as_str(),
                        });
                    }
                }
                match mode {
                    TeeMode::SevSnp => validate_snp_policy(self.policy)?,
                    // TDX has no host-chosen launch policy: TDATTRIBUTES and
                    // XFAM are set by the TDX module and measured, not
                    // selected. Reject any non-default value rather than
                    // inventing a meaning.
                    TeeMode::Tdx => {
                        if self.policy != default_amd_sev_policy() {
                            return Err(TeeError::TdxPolicySet);
                        }
                    }
                    TeeMode::Sev => unreachable!("matched as measured above"),
                }
            }
            None | Some(TeeMode::Sev) => {
                if self.runtime.is_some() {
                    return Err(TeeError::MeasuredOnlyField("runtime"));
                }
                if self.measurements.is_some() {
                    return Err(TeeError::MeasuredOnlyField("measurements"));
                }
                if self.attestation_port.is_some() {
                    return Err(TeeError::MeasuredOnlyField("attestation_port"));
                }
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct RawTrustedExecutionEnvironment {
    #[serde(default)]
    firmware: Option<ItemHash>,
    #[serde(default = "default_amd_sev_policy")]
    policy: u64,
    #[serde(default)]
    mode: Option<TeeMode>,
    #[serde(default)]
    runtime: Option<ItemHash>,
    #[serde(default)]
    measurements: Option<Vec<LaunchMeasurement>>,
    #[serde(default)]
    attestation_port: Option<NonZeroU16>,
}

impl TryFrom<RawTrustedExecutionEnvironment> for TrustedExecutionEnvironment {
    type Error = TeeError;

    fn try_from(raw: RawTrustedExecutionEnvironment) -> Result<Self, Self::Error> {
        let tee = TrustedExecutionEnvironment {
            firmware: raw.firmware,
            policy: raw.policy,
            mode: raw.mode,
            runtime: raw.runtime,
            measurements: raw.measurements,
            attestation_port: raw.attestation_port,
        };
        tee.check_mode_consistency()?;
        Ok(tee)
    }
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

    const SNP_DIGEST: &str = "abababababababababababababababababababababababababababababababababababababababababababababababab";

    const ITEM_HASH_HEX: &str = "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe";

    fn snp_tee_json(policy: &str) -> String {
        format!(
            r#"{{"mode": "sev_snp", "policy": {policy}, "runtime": "{ITEM_HASH_HEX}",
                 "measurements": [{{"platform": "sev_snp", "registers": {{"launch": "{SNP_DIGEST}"}}}}]}}"#
        )
    }

    #[test]
    fn test_trusted_execution_legacy_sev_unchanged() {
        let legacy = r#"{"policy": 1, "firmware": "e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317"}"#;
        let tee: TrustedExecutionEnvironment = serde_json::from_str(legacy).unwrap();
        assert_eq!(tee.mode, None); // None means legacy SEV
        assert!(!tee.is_snp());
        assert_eq!(tee.policy, 1);
        // serialization stability: no new keys appear on legacy content
        let value = serde_json::to_value(&tee).unwrap();
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["firmware", "policy"]);
    }

    #[test]
    fn test_trusted_execution_snp_valid() {
        let tee: TrustedExecutionEnvironment =
            serde_json::from_str(&snp_tee_json("196608")).unwrap();
        assert!(tee.is_snp());
        assert_eq!(tee.policy, 0x30000);
        assert!(tee.runtime.is_some());
        assert_eq!(tee.measurements.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_trusted_execution_snp_policy_above_u32() {
        // policy is u64: values beyond the old u32 field must parse
        let policy = (1u64 << 40) | (1 << 17);
        let tee: TrustedExecutionEnvironment =
            serde_json::from_str(&snp_tee_json(&policy.to_string())).unwrap();
        assert_eq!(tee.policy, policy);
    }

    #[test]
    fn test_trusted_execution_snp_rejects_invalid() {
        // SEV-style policy value (bit 17 unset)
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&snp_tee_json("1")).is_err());
        // negative policy is unrepresentable in u64
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&snp_tee_json("-1")).is_err());
        // missing runtime
        let json = format!(
            r#"{{"mode": "sev_snp", "policy": 196608,
                 "measurements": [{{"platform": "sev_snp", "registers": {{"launch": "{SNP_DIGEST}"}}}}]}}"#
        );
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err());
        // missing measurements
        let json =
            format!(r#"{{"mode": "sev_snp", "policy": 196608, "runtime": "{ITEM_HASH_HEX}"}}"#);
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err());
        // firmware forbidden in snp mode
        let json = format!(
            r#"{{"mode": "sev_snp", "policy": 196608, "runtime": "{ITEM_HASH_HEX}",
                 "firmware": "{ITEM_HASH_HEX}",
                 "measurements": [{{"platform": "sev_snp", "registers": {{"launch": "{SNP_DIGEST}"}}}}]}}"#
        );
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err());
    }

    #[test]
    fn test_trusted_execution_sev_forbids_snp_fields() {
        for extra in [
            format!(r#""runtime": "{ITEM_HASH_HEX}""#),
            format!(
                r#""measurements": [{{"platform": "sev_snp", "registers": {{"launch": "{SNP_DIGEST}"}}}}]"#
            ),
            r#""attestation_port": 8443"#.to_string(),
        ] {
            let json = format!(r#"{{"policy": 1, {extra}}}"#);
            assert!(
                serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err(),
                "{extra} should be rejected outside sev_snp mode"
            );
        }
    }

    #[test]
    fn test_trusted_execution_explicit_sev_mode_matches_legacy() {
        // "sev" spelled out behaves exactly like `mode` absent
        let json = r#"{"mode": "sev", "policy": 1, "firmware": "e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317"}"#;
        let tee: TrustedExecutionEnvironment = serde_json::from_str(json).unwrap();
        assert_eq!(tee.mode, Some(TeeMode::Sev));
        assert!(!tee.is_snp());
        assert!(tee.firmware.is_some());
        assert_eq!(tee.policy, 1);

        // policy keeps informational SEV bit semantics: an SNP-style value is
        // not rejected, exactly as with `mode` absent
        let json = r#"{"mode": "sev", "policy": 196608}"#;
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(json).is_ok());

        // SNP-only fields stay rejected
        let json = format!(r#"{{"mode": "sev", "policy": 1, "runtime": "{ITEM_HASH_HEX}"}}"#);
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err());
    }

    #[test]
    fn test_trusted_execution_attestation_port_bounds() {
        let json = format!(
            r#"{{"mode": "sev_snp", "policy": 196608, "runtime": "{ITEM_HASH_HEX}",
                 "measurements": [{{"platform": "sev_snp", "registers": {{"launch": "{SNP_DIGEST}"}}}}],
                 "attestation_port": 8443}}"#
        );
        // a valid port is accepted and round-trips
        let tee: TrustedExecutionEnvironment = serde_json::from_str(&json).unwrap();
        assert_eq!(tee.attestation_port.map(NonZeroU16::get), Some(8443));
        assert_eq!(
            serde_json::to_value(&tee).unwrap()["attestation_port"],
            8443
        );
        // 0 is unrepresentable in NonZeroU16
        let bad = json.replace("\"attestation_port\": 8443", "\"attestation_port\": 0");
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&bad).is_err());
        let bad = json.replace("\"attestation_port\": 8443", "\"attestation_port\": 65536");
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&bad).is_err());
    }

    #[test]
    fn test_launch_measurement_valid() {
        let m: LaunchMeasurement = serde_json::from_str(&format!(
            r#"{{"platform": "sev_snp", "registers": {{"launch": "{SNP_DIGEST}"}}, "vcpu_type": "EPYC-v4"}}"#
        ))
        .unwrap();
        assert_eq!(m.platform, TeePlatform::SevSnp);
        assert_eq!(m.snp_launch_digest(), Some(SNP_DIGEST));
        assert_eq!(m.vcpu_type.as_deref(), Some("EPYC-v4"));
        // vcpu_type is optional: absent for igvm-recipe bundles
        let m: LaunchMeasurement = serde_json::from_str(&format!(
            r#"{{"platform": "sev_snp", "registers": {{"launch": "{SNP_DIGEST}"}}}}"#
        ))
        .unwrap();
        assert_eq!(m.vcpu_type, None);
    }

    #[test]
    fn test_launch_measurement_rejects_bad_register_values() {
        // wrong length (sha256-sized), non-hex, uppercase hex
        for digest in ["cd".repeat(32), "zz".repeat(48), "AB".repeat(48)] {
            let json =
                format!(r#"{{"platform": "sev_snp", "registers": {{"launch": "{digest}"}}}}"#);
            assert!(
                serde_json::from_str::<LaunchMeasurement>(&json).is_err(),
                "register value {digest} should be rejected"
            );
        }
    }

    /// The register set is exactly `{"launch"}`: nothing more, nothing less.
    /// An unknown register is as schema-invalid as an unknown platform.
    #[test]
    fn test_launch_measurement_register_key_set_is_closed() {
        for registers in [
            // missing the required key
            "{}".to_string(),
            // an unknown key alongside the required one
            format!(r#"{{"launch": "{SNP_DIGEST}", "mrtd": "{SNP_DIGEST}"}}"#),
            // a register from another platform instead of the required one
            format!(r#"{{"mrtd": "{SNP_DIGEST}"}}"#),
        ] {
            let json = format!(r#"{{"platform": "sev_snp", "registers": {registers}}}"#);
            assert!(
                serde_json::from_str::<LaunchMeasurement>(&json).is_err(),
                "register set {registers} should be rejected"
            );
        }
    }

    #[test]
    fn test_launch_measurement_rejects_unknown_platform() {
        let json = format!(r#"{{"platform": "tdx", "registers": {{"mrtd": "{SNP_DIGEST}"}}}}"#);
        assert!(serde_json::from_str::<LaunchMeasurement>(&json).is_err());
    }

    /// The pre-register scalar shape must not silently deserialize into a
    /// measurement with an empty register map.
    #[test]
    fn test_launch_measurement_rejects_legacy_digest_shape() {
        let json = format!(r#"{{"platform": "sev_snp", "digest": "{SNP_DIGEST}"}}"#);
        assert!(serde_json::from_str::<LaunchMeasurement>(&json).is_err());
    }

    #[test]
    fn test_launch_measurement_roundtrip_omits_null_vcpu_type() {
        let m: LaunchMeasurement = serde_json::from_str(&format!(
            r#"{{"platform": "sev_snp", "registers": {{"launch": "{SNP_DIGEST}"}}}}"#
        ))
        .unwrap();
        let value = serde_json::to_value(&m).unwrap();
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        keys.sort();
        // no vcpu_type key when None (serde_json orders keys alphabetically)
        assert_eq!(keys, vec!["platform", "registers"]);
    }

    #[test]
    fn test_validate_snp_policy() {
        validate_snp_policy(DEFAULT_SNP_POLICY).unwrap(); // 0x30000: bits 16+17
        validate_snp_policy(1 << 17).unwrap();
        validate_snp_policy((1 << 40) | (1 << 17)).unwrap(); // >u32 range is fine
        assert!(validate_snp_policy(0x1).is_err()); // the AMD SEV default lacks bit 17
        assert!(validate_snp_policy(0x10000).is_err()); // SMT bit alone, no bit 17
    }

    fn tdx_registers_json() -> String {
        format!(
            r#"{{"mrtd": "{m}", "rtmr1": "{r1}", "rtmr2": "{r2}", "mrconfigid": "{c}"}}"#,
            m = "11".repeat(48),
            r1 = "22".repeat(48),
            r2 = "33".repeat(48),
            c = "44".repeat(48),
        )
    }

    fn tdx_tee_json() -> String {
        format!(
            r#"{{"mode": "tdx", "runtime": "{ITEM_HASH_HEX}",
                 "measurements": [{{"platform": "tdx", "registers": {}}}]}}"#,
            tdx_registers_json()
        )
    }

    #[test]
    fn test_launch_measurement_tdx_valid() {
        let json = format!(
            r#"{{"platform": "tdx", "registers": {}, "vcpu_type": "GraniteRapids"}}"#,
            tdx_registers_json()
        );
        let m: LaunchMeasurement = serde_json::from_str(&json).unwrap();
        assert_eq!(m.platform, TeePlatform::Tdx);
        let MeasurementRegisters::Tdx(registers) = &m.registers else {
            panic!("expected tdx registers");
        };
        assert_eq!(registers.mrtd, "11".repeat(48));
        assert_eq!(registers.mrconfigid, "44".repeat(48));
        assert_eq!(m.vcpu_type.as_deref(), Some("GraniteRapids"));
        // no launch register on a tdx set
        assert_eq!(m.snp_launch_digest(), None);
        // vcpu_type stays optional on tdx too
        let json = format!(
            r#"{{"platform": "tdx", "registers": {}}}"#,
            tdx_registers_json()
        );
        let m: LaunchMeasurement = serde_json::from_str(&json).unwrap();
        assert_eq!(m.vcpu_type, None);
    }

    #[test]
    fn test_launch_measurement_tdx_key_set_is_closed() {
        // the pinned set is exactly {mrtd, rtmr1, rtmr2, mrconfigid}: rtmr0
        // (deployment parameters) and rtmr3 (derived launch-TCB commitment)
        // are deliberately not pinnable
        for missing in ["mrtd", "rtmr1", "rtmr2", "mrconfigid"] {
            let registers = tdx_registers_json().replace(missing, &format!("x{missing}"));
            let json = format!(r#"{{"platform": "tdx", "registers": {registers}}}"#);
            assert!(
                serde_json::from_str::<LaunchMeasurement>(&json).is_err(),
                "renamed {missing} should be rejected"
            );
        }
        for forbidden in ["rtmr0", "rtmr3", "launch"] {
            let registers = tdx_registers_json().replace(
                "\"mrtd\"",
                &format!("\"{forbidden}\": \"{}\", \"mrtd\"", "55".repeat(48)),
            );
            let json = format!(r#"{{"platform": "tdx", "registers": {registers}}}"#);
            assert!(
                serde_json::from_str::<LaunchMeasurement>(&json).is_err(),
                "extra {forbidden} should be rejected"
            );
        }
    }

    #[test]
    fn test_launch_measurement_tdx_rejects_bad_register_values() {
        for bad in ["cd".repeat(32), "zz".repeat(48), "AB".repeat(48)] {
            let registers = tdx_registers_json().replace(&"22".repeat(48), &bad);
            let json = format!(r#"{{"platform": "tdx", "registers": {registers}}}"#);
            assert!(
                serde_json::from_str::<LaunchMeasurement>(&json).is_err(),
                "rtmr1 {bad} should be rejected"
            );
        }
    }

    #[test]
    fn test_launch_measurement_platform_must_match_registers() {
        // the register-set union is discriminated on `platform`
        let json = format!(
            r#"{{"platform": "sev_snp", "registers": {}}}"#,
            tdx_registers_json()
        );
        let err = serde_json::from_str::<LaunchMeasurement>(&json).unwrap_err();
        assert!(err.to_string().contains("was declared with"), "{err}");
        let json = format!(r#"{{"platform": "tdx", "registers": {{"launch": "{SNP_DIGEST}"}}}}"#);
        assert!(serde_json::from_str::<LaunchMeasurement>(&json).is_err());
    }

    #[test]
    fn test_launch_measurement_tdx_roundtrip() {
        let json = format!(
            r#"{{"platform": "tdx", "registers": {}}}"#,
            tdx_registers_json()
        );
        let m: LaunchMeasurement = serde_json::from_str(&json).unwrap();
        let value = serde_json::to_value(&m).unwrap();
        let mut keys: Vec<&str> = value["registers"]
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["mrconfigid", "mrtd", "rtmr1", "rtmr2"]);
        let back: LaunchMeasurement = serde_json::from_value(value).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn test_trusted_execution_tdx_valid() {
        let tee: TrustedExecutionEnvironment = serde_json::from_str(&tdx_tee_json()).unwrap();
        assert!(tee.is_measured());
        assert!(!tee.is_snp());
        assert_eq!(tee.mode, Some(TeeMode::Tdx));
        assert!(tee.runtime.is_some());
        assert_eq!(tee.measurements.as_ref().unwrap().len(), 1);
        // the pydantic policy default round-trips: a dump carries policy=1
        let value = serde_json::to_value(&tee).unwrap();
        assert_eq!(value["policy"], 1);
        let back: TrustedExecutionEnvironment = serde_json::from_value(value).unwrap();
        assert_eq!(back, tee);
    }

    #[test]
    fn test_trusted_execution_tdx_rejects_invalid() {
        // missing runtime
        let json = format!(
            r#"{{"mode": "tdx", "measurements": [{{"platform": "tdx", "registers": {}}}]}}"#,
            tdx_registers_json()
        );
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err());
        // missing measurements
        let json = format!(r#"{{"mode": "tdx", "runtime": "{ITEM_HASH_HEX}"}}"#);
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err());
        // firmware forbidden
        let json =
            tdx_tee_json().replacen("{", &format!(r#"{{"firmware": "{ITEM_HASH_HEX}", "#), 1);
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err());
    }

    #[test]
    fn test_trusted_execution_tdx_has_no_policy() {
        // tdx has no host-chosen launch policy: any non-default value is
        // rejected rather than given an invented meaning
        let json = tdx_tee_json().replacen("{", r#"{"policy": 196608, "#, 1);
        let err = serde_json::from_str::<TrustedExecutionEnvironment>(&json).unwrap_err();
        assert!(
            err.to_string().contains("no host-chosen launch policy"),
            "{err}"
        );
        // the explicit default is accepted (it appears in every dump)
        let json = tdx_tee_json().replacen("{", r#"{"policy": 1, "#, 1);
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_ok());
    }

    #[test]
    fn test_trusted_execution_measurement_platform_must_match_mode() {
        // an sev_snp measurement under tdx mode, and vice versa
        let json = format!(
            r#"{{"mode": "tdx", "runtime": "{ITEM_HASH_HEX}",
                 "measurements": [{{"platform": "sev_snp", "registers": {{"launch": "{SNP_DIGEST}"}}}}]}}"#
        );
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err());
        let json = format!(
            r#"{{"mode": "sev_snp", "policy": 196608, "runtime": "{ITEM_HASH_HEX}",
                 "measurements": [{{"platform": "tdx", "registers": {}}}]}}"#,
            tdx_registers_json()
        );
        assert!(serde_json::from_str::<TrustedExecutionEnvironment>(&json).is_err());
    }

    #[test]
    fn test_trusted_execution_tdx_attestation_port_allowed() {
        let json = tdx_tee_json().replacen("{", r#"{"attestation_port": 8443, "#, 1);
        let tee: TrustedExecutionEnvironment = serde_json::from_str(&json).unwrap();
        assert_eq!(tee.attestation_port.map(NonZeroU16::get), Some(8443));
    }
}
