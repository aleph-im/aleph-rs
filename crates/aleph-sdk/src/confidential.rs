//! SEV-ES launch attestation primitives. Pure data + pure crypto - no I/O.

use aes::Aes128;
use ctr::cipher::{KeyIvInit, StreamCipher};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

/// Default SHA-256 of the OVMF firmware blob used for confidential VMs on
/// aleph.cloud. Matches Python's `DEFAULT_CONFIDENTIAL_FIRMWARE_HASH` in
/// `aleph-sdk-python/src/aleph/sdk/conf.py`. Pass `--firmware-hash` to
/// override or `--firmware-file` to recompute locally.
pub const DEFAULT_CONFIDENTIAL_FIRMWARE_HASH_HEX: &str =
    "89b76b0e64fe9015084fbffdf8ac98185bafc688bfe7a0b398585c392d03c7ee";

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

/// SEV-ES launch secret injection. AMD SEV API specification, LAUNCH_SECRET command.
///
/// Returns `(packet_header_b64, encrypted_secret_b64)`.
///
/// Plaintext secret-table layout (AMD spec):
///   table_header_guid (16 bytes, 1e74f542-71dd-4d66-963e-ef4287ff173b, UUID little-endian)
///   table_length      (4 bytes, little-endian, = total table length in bytes)
///   secret_guid       (16 bytes, 736869e5-84f0-4973-92ec-06879ce3da0b, UUID little-endian)
///   secret_length     (4 bytes, little-endian, = 16 + 4 + secret.len() + 1)
///   secret_bytes      (utf-8)
///   nul terminator    (1 byte)
///   zero padding to a 16-byte boundary
///
/// Encrypted with AES-128-CTR using `tek` as the key.
///
/// Packet header: flags (4 zero bytes) || iv (16) || HMAC-SHA256(tik, ...) (32).
/// HMAC input: 0x01 (LAUNCH_SECRET command id) || flags(4) || iv(16) ||
///             secret_table_size_le32 || encrypted_secret_table || vm_measure
/// per the SEV API spec.
pub fn build_secret_packet(
    tek: &[u8; 16],
    tik: &[u8; 16],
    vm_measure: &[u8; 32],
    secret: &str,
    iv: [u8; 16],
) -> (String, String) {
    use base64::Engine;

    const HEADER_GUID: [u8; 16] = uuid_le(
        0x1e74f542,
        0x71dd,
        0x4d66,
        0x96,
        0x3e,
        [0xef, 0x42, 0x87, 0xff, 0x17, 0x3b],
    );
    const SECRET_GUID: [u8; 16] = uuid_le(
        0x736869e5,
        0x84f0,
        0x4973,
        0x92,
        0xec,
        [0x06, 0x87, 0x9c, 0xe3, 0xda, 0x0b],
    );

    let secret_bytes = secret.as_bytes();
    let secret_entry_len: u32 = (16 + 4 + secret_bytes.len() + 1) as u32;
    let total_len_unrounded = 16 + 4 + secret_entry_len as usize;
    let total_len = (total_len_unrounded + 15) & !15;

    let mut table = vec![0u8; total_len];
    table[0..16].copy_from_slice(&HEADER_GUID);
    table[16..20].copy_from_slice(&(total_len as u32).to_le_bytes());
    table[20..36].copy_from_slice(&SECRET_GUID);
    table[36..40].copy_from_slice(&secret_entry_len.to_le_bytes());
    table[40..40 + secret_bytes.len()].copy_from_slice(secret_bytes);
    // trailing nul is already zero from vec![0u8; total_len]

    type Aes128Ctr = ctr::Ctr64BE<Aes128>;
    let mut cipher =
        Aes128Ctr::new_from_slices(tek, &iv).expect("AES-128-CTR accepts a 16-byte key and IV");
    let mut ciphertext = table.clone();
    cipher.apply_keystream(&mut ciphertext);

    let flags: [u8; 4] = [0, 0, 0, 0];
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(tik).expect("HMAC-SHA256 accepts a 16-byte key");
    mac.update(&[0x01u8]);
    mac.update(&flags);
    mac.update(&iv);
    mac.update(&(total_len as u32).to_le_bytes());
    mac.update(&ciphertext);
    mac.update(vm_measure);
    let header_hmac = mac.finalize().into_bytes();

    let mut header = Vec::with_capacity(4 + 16 + 32);
    header.extend_from_slice(&flags);
    header.extend_from_slice(&iv);
    header.extend_from_slice(&header_hmac);

    (
        base64::engine::general_purpose::STANDARD.encode(&header),
        base64::engine::general_purpose::STANDARD.encode(&ciphertext),
    )
}

const fn uuid_le(d1: u32, d2: u16, d3: u16, d4_hi: u8, d4_lo: u8, rest: [u8; 6]) -> [u8; 16] {
    let mut out = [0u8; 16];
    let d1 = d1.to_le_bytes();
    let d2 = d2.to_le_bytes();
    let d3 = d3.to_le_bytes();
    out[0] = d1[0];
    out[1] = d1[1];
    out[2] = d1[2];
    out[3] = d1[3];
    out[4] = d2[0];
    out[5] = d2[1];
    out[6] = d3[0];
    out[7] = d3[1];
    out[8] = d4_hi;
    out[9] = d4_lo;
    out[10] = rest[0];
    out[11] = rest[1];
    out[12] = rest[2];
    out[13] = rest[3];
    out[14] = rest[4];
    out[15] = rest[5];
    out
}

/// Stream-hashes the OVMF firmware blob at `path` and returns the lowercase
/// hex-encoded SHA-256 digest. Matches `aleph_sdk.utils.calculate_firmware_hash`
/// in Python; the digest is what AMD's SEV API feeds into the launch measurement.
pub fn calculate_firmware_hash(path: &Path) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)?;
    Ok(hex::encode(hasher.finalize()))
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

    #[test]
    fn build_secret_packet_table_layout() {
        use base64::Engine;
        let tek = [0u8; 16];
        let tik = [0u8; 16];
        let vm_measure = [0u8; 32];
        let secret = "topsecret";
        let iv = [0u8; 16];

        let (_hdr_b64, sec_b64) = build_secret_packet(&tek, &tik, &vm_measure, secret, iv);
        let ciphertext = base64::engine::general_purpose::STANDARD
            .decode(&sec_b64)
            .unwrap();

        // Expected length: header_guid(16) + header_len(4) + secret_guid(16) + secret_len(4) + secret(9) + zero(1) = 50, rounded up to 64.
        assert_eq!(ciphertext.len(), 64);
    }

    #[test]
    fn calculate_firmware_hash_known_input() {
        // SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("firmware.bin");
        std::fs::write(&path, b"abc").unwrap();
        let hex = calculate_firmware_hash(&path).unwrap();
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn build_secret_packet_locked_fixture() {
        use base64::Engine;
        let tek: [u8; 16] = hex::decode("000102030405060708090a0b0c0d0e0f")
            .unwrap()
            .try_into()
            .unwrap();
        let tik: [u8; 16] = hex::decode("0f0e0d0c0b0a09080706050403020100")
            .unwrap()
            .try_into()
            .unwrap();
        let vm_measure: [u8; 32] =
            hex::decode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
                .unwrap()
                .try_into()
                .unwrap();
        let secret = "hello";
        let iv = [0u8; 16];

        let (hdr_b64, sec_b64) = build_secret_packet(&tek, &tik, &vm_measure, secret, iv);
        let hdr = base64::engine::general_purpose::STANDARD
            .decode(&hdr_b64)
            .unwrap();
        let sec = base64::engine::general_purpose::STANDARD
            .decode(&sec_b64)
            .unwrap();
        // Packet header layout: flags(4 = "\x00\x00\x00\x00") || iv(16) || hmac(32) = 52 bytes
        assert_eq!(hdr.len(), 52);
        assert_eq!(&hdr[0..4], &[0, 0, 0, 0]);
        assert_eq!(&hdr[4..20], &iv);
        // Ciphertext length matches the table size for a 5-byte secret:
        // 16 + 4 + 16 + 4 + 5 + 1 = 46, rounded to 48.
        assert_eq!(sec.len(), 48);
    }
}
