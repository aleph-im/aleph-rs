//! SEV-ES launch attestation primitives. Pure data + pure crypto - no I/O.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct SEVInfo {
    pub api_major: u8,
    pub api_minor: u8,
    pub build_id: u8,
    pub policy: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SEVMeasurement {
    pub sev_info: SEVInfo,
    /// base64 of 48 bytes: 32-byte measure || 16-byte nonce
    pub launch_measure: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfidentialError {
    #[error("launch_measure is not valid base64: {0}")]
    InvalidLaunchMeasureBase64(base64::DecodeError),
    #[error(
        "launch_measure has unexpected length: expected 48 bytes (32 measure + 16 nonce), got {0}"
    )]
    InvalidLaunchMeasureLength(usize),
}

impl SEVMeasurement {
    pub fn split_launch_measure(&self) -> Result<([u8; 32], [u8; 16]), ConfidentialError> {
        use base64::Engine;
        let raw = base64::engine::general_purpose::STANDARD
            .decode(&self.launch_measure)
            .map_err(ConfidentialError::InvalidLaunchMeasureBase64)?;
        if raw.len() != 48 {
            return Err(ConfidentialError::InvalidLaunchMeasureLength(raw.len()));
        }
        let mut measure = [0u8; 32];
        let mut nonce = [0u8; 16];
        measure.copy_from_slice(&raw[0..32]);
        nonce.copy_from_slice(&raw[32..48]);
        Ok((measure, nonce))
    }
}

/// HMAC-SHA256(tik, 0x04 || api_major || api_minor || build_id ||
///             policy.to_le_bytes() || firmware_hash || nonce)
/// AMD SEV API specification section 6.5.2.
pub fn compute_expected_measure(
    info: &SEVInfo,
    tik: &[u8; 16],
    firmware_hash: &[u8; 32],
    nonce: &[u8; 16],
) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(tik).expect("HMAC accepts a 16-byte key");
    mac.update(&[0x04u8]);
    mac.update(&[info.api_major]);
    mac.update(&[info.api_minor]);
    mac.update(&[info.build_id]);
    mac.update(&info.policy.to_le_bytes());
    mac.update(firmware_hash);
    mac.update(nonce);
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn compute_expected_measure_matches_python_fixture() {
        let info = SEVInfo {
            api_major: 1,
            api_minor: 55,
            build_id: 24,
            policy: 1,
        };
        let tik: [u8; 16] = hex::decode("9e939311ce26b5119f5df07e1ba10177")
            .unwrap()
            .try_into()
            .unwrap();
        let firmware_hash: [u8; 32] =
            hex::decode("d06471f485c0a61aba5a431ec136b947be56907acf6ed96afb11788ae4525aeb")
                .unwrap()
                .try_into()
                .unwrap();
        let nonce: [u8; 16] = base64::engine::general_purpose::STANDARD
            .decode("URQNqJAqh/2ep4drjx/XvA==")
            .unwrap()
            .try_into()
            .unwrap();

        let measure = compute_expected_measure(&info, &tik, &firmware_hash, &nonce);

        let expected = base64::engine::general_purpose::STANDARD
            .decode("ls2jv10V3HVShVI/RHCo/a43WO0soLZf0huU9ZZstIw=")
            .unwrap();
        assert_eq!(measure.as_slice(), expected.as_slice());
    }

    #[test]
    fn split_launch_measure_happy_path() {
        use base64::Engine;
        let m = SEVMeasurement {
            sev_info: SEVInfo {
                api_major: 1,
                api_minor: 55,
                build_id: 24,
                policy: 1,
            },
            launch_measure: "ls2jv10V3HVShVI/RHCo/a43WO0soLZf0huU9ZZstIxRFA2okCqH/Z6nh2uPH9e8"
                .to_string(),
        };
        let (measure, nonce) = m.split_launch_measure().unwrap();
        let expected_measure = base64::engine::general_purpose::STANDARD
            .decode("ls2jv10V3HVShVI/RHCo/a43WO0soLZf0huU9ZZstIw=")
            .unwrap();
        let expected_nonce = base64::engine::general_purpose::STANDARD
            .decode("URQNqJAqh/2ep4drjx/XvA==")
            .unwrap();
        assert_eq!(measure.as_slice(), expected_measure.as_slice());
        assert_eq!(nonce.as_slice(), expected_nonce.as_slice());
    }

    #[test]
    fn split_launch_measure_rejects_short_input() {
        let m = SEVMeasurement {
            sev_info: SEVInfo {
                api_major: 1,
                api_minor: 55,
                build_id: 24,
                policy: 1,
            },
            launch_measure: "AAAA".to_string(),
        };
        assert!(matches!(
            m.split_launch_measure(),
            Err(ConfidentialError::InvalidLaunchMeasureLength(_))
        ));
    }

    #[test]
    fn split_launch_measure_rejects_invalid_base64() {
        let m = SEVMeasurement {
            sev_info: SEVInfo {
                api_major: 1,
                api_minor: 55,
                build_id: 24,
                policy: 1,
            },
            launch_measure: "!!!not base64!!!".to_string(),
        };
        assert!(matches!(
            m.split_launch_measure(),
            Err(ConfidentialError::InvalidLaunchMeasureBase64(_))
        ));
    }
}
