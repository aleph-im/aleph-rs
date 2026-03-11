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

    // EIP-191: "\x19Ethereum Signed Message:\n{len}{message}"
    let prefix = format!("\x19Ethereum Signed Message:\n{}", message.len());
    let mut hasher = Keccak256::new();
    hasher.update(prefix.as_bytes());
    hasher.update(message);
    let digest = hasher.finalize();

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

/// Normalizes the `v` byte: values 27/28 are mapped to 0/1.
fn normalize_v(v: u8) -> Result<RecoveryId, SignatureVerificationError> {
    let id = if v >= 27 { v - 27 } else { v };
    RecoveryId::try_from(id)
        .map_err(|e| SignatureVerificationError::InvalidSignature(e.to_string()))
}

/// Derives an Ethereum address from a secp256k1 public key.
/// Address = "0x" + last 20 bytes of keccak256(uncompressed_pubkey[1..])
fn public_key_to_address(key: &VerifyingKey) -> String {
    let uncompressed = key.to_encoded_point(false);
    let public_key_bytes = &uncompressed.as_bytes()[1..]; // skip 0x04 prefix
    let hash = Keccak256::digest(public_key_bytes);
    let address_bytes = &hash[12..]; // last 20 bytes
    format!("0x{}", hex::encode(address_bytes))
}
