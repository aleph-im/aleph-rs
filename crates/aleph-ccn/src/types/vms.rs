//! VM-related enums and newtypes.
//!
//! Mirrors `src/aleph/types/vms.py`.

use serde::{Deserialize, Serialize};

/// VM version string (Python `NewType("VmVersion", str)`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VmVersion(pub String);

impl VmVersion {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for VmVersion {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for VmVersion {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl std::fmt::Display for VmVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Type of VM workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VmType {
    Instance,
    Program,
}

/// CPU architecture targeted by a VM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CpuArchitecture {
    #[serde(rename = "x86_64")]
    X86_64,
    #[serde(rename = "arm64")]
    Arm64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vm_version_roundtrip() {
        let v = VmVersion::from("1.2.3");
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, "\"1.2.3\"");
        let back: VmVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(back, v);
        assert_eq!(v.as_str(), "1.2.3");
        assert_eq!(v.to_string(), "1.2.3");
    }

    #[test]
    fn vm_type_roundtrip() {
        assert_eq!(
            serde_json::to_string(&VmType::Instance).unwrap(),
            "\"instance\""
        );
        assert_eq!(
            serde_json::to_string(&VmType::Program).unwrap(),
            "\"program\""
        );
        let parsed: VmType = serde_json::from_str("\"instance\"").unwrap();
        assert_eq!(parsed, VmType::Instance);
    }

    #[test]
    fn cpu_architecture_roundtrip() {
        assert_eq!(
            serde_json::to_string(&CpuArchitecture::X86_64).unwrap(),
            "\"x86_64\""
        );
        assert_eq!(
            serde_json::to_string(&CpuArchitecture::Arm64).unwrap(),
            "\"arm64\""
        );
        let parsed: CpuArchitecture = serde_json::from_str("\"x86_64\"").unwrap();
        assert_eq!(parsed, CpuArchitecture::X86_64);
        let parsed: CpuArchitecture = serde_json::from_str("\"arm64\"").unwrap();
        assert_eq!(parsed, CpuArchitecture::Arm64);
    }
}
