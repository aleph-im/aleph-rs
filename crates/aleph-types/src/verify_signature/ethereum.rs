use super::SignatureVerificationError;
use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
use sha3::{Digest, Keccak256};

/// Recovers the Ethereum address that signed the given message bytes.
///
/// Applies EIP-191 personal message prefix, hashes with Keccak-256,
/// performs secp256k1 ECDSA recovery, and derives the address.
pub(super) fn recover_address(
    message: &[u8],
    signature_hex: &str,
) -> Result<String, SignatureVerificationError> {
    let sig_bytes = decode_signature(signature_hex)?;
    let (r_s, v) = sig_bytes.split_at(64);

    let recovery_id = normalize_v(v[0])?;
    let signature = Signature::from_slice(r_s)
        .map_err(|e| SignatureVerificationError::InvalidSignature(e.to_string()))?;

    let digest = eip191_hash(message);

    let verifying_key = VerifyingKey::recover_from_prehash(&digest, &signature, recovery_id)
        .map_err(|e| SignatureVerificationError::InvalidSignature(e.to_string()))?;

    Ok(public_key_to_address(&verifying_key))
}

/// Hex-decodes the signature, stripping an optional `0x` prefix.
fn decode_signature(hex_str: &str) -> Result<[u8; 65], SignatureVerificationError> {
    let hex_str = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = hex::decode(hex_str)
        .map_err(|e| SignatureVerificationError::InvalidSignature(e.to_string()))?;
    bytes.try_into().map_err(|v: Vec<u8>| {
        SignatureVerificationError::InvalidSignature(format!(
            "expected 65 signature bytes, got {}",
            v.len()
        ))
    })
}

/// Normalizes the `v` byte: only accepts 0, 1, 27, or 28.
fn normalize_v(v: u8) -> Result<RecoveryId, SignatureVerificationError> {
    let id = match v {
        0 | 1 => v,
        27 | 28 => v - 27,
        _ => {
            return Err(SignatureVerificationError::InvalidSignature(format!(
                "unexpected recovery id byte: {v}"
            )));
        }
    };
    RecoveryId::try_from(id)
        .map_err(|e| SignatureVerificationError::InvalidSignature(e.to_string()))
}

/// Applies the EIP-191 personal message prefix and hashes with Keccak-256.
pub(crate) fn eip191_hash(message: &[u8]) -> [u8; 32] {
    let prefix = format!("\x19Ethereum Signed Message:\n{}", message.len());
    let mut hasher = Keccak256::new();
    hasher.update(prefix.as_bytes());
    hasher.update(message);
    hasher.finalize().into()
}

/// Derives an EIP-55 checksummed Ethereum address from a secp256k1 public key.
/// Address = "0x" + last 20 bytes of keccak256(uncompressed_pubkey[1..]),
/// with mixed-case checksum per EIP-55.
pub(crate) fn public_key_to_address(key: &VerifyingKey) -> String {
    let uncompressed = key.to_encoded_point(false);
    let public_key_bytes = &uncompressed.as_bytes()[1..]; // skip 0x04 prefix
    let hash = Keccak256::digest(public_key_bytes);
    let address_bytes = &hash[12..]; // last 20 bytes
    eip55_checksum(address_bytes)
}

/// Encodes raw address bytes as an EIP-55 checksummed hex string.
fn eip55_checksum(address_bytes: &[u8]) -> String {
    let hex_addr = hex::encode(address_bytes);
    let hash = Keccak256::digest(hex_addr.as_bytes());

    let mut checksummed = String::with_capacity(42);
    checksummed.push_str("0x");
    for (i, c) in hex_addr.chars().enumerate() {
        if c.is_ascii_alphabetic() {
            // High nibble of the hash byte determines case
            let hash_byte = hash[i / 2];
            let nibble = if i % 2 == 0 {
                hash_byte >> 4
            } else {
                hash_byte & 0x0f
            };
            if nibble >= 8 {
                checksummed.push(c.to_ascii_uppercase());
            } else {
                checksummed.push(c);
            }
        } else {
            checksummed.push(c);
        }
    }
    checksummed
}
