use super::SignatureVerificationError;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};

/// Verifies that `signature_b58` is a valid Ed25519 signature over `message`
/// produced by the public key `public_key_b58`.
///
/// Both `signature_b58` and `public_key_b58` are base58-encoded.
pub(super) fn verify(
    message: &[u8],
    signature_b58: &str,
    public_key_b58: &str,
) -> Result<(), SignatureVerificationError> {
    let pk_bytes = decode_b58::<32>(public_key_b58, "public key")?;
    let sig_bytes = decode_b58::<64>(signature_b58, "signature")?;

    let verifying_key = VerifyingKey::from_bytes(&pk_bytes)
        .map_err(|e| SignatureVerificationError::InvalidSignature(e.to_string()))?;
    let signature = Signature::from_bytes(&sig_bytes);

    verifying_key
        .verify(message, &signature)
        .map_err(|e| SignatureVerificationError::InvalidSignature(e.to_string()))
}

/// Base58-decodes a string into a fixed-size byte array.
fn decode_b58<const N: usize>(
    input: &str,
    label: &str,
) -> Result<[u8; N], SignatureVerificationError> {
    let bytes = bs58::decode(input)
        .into_vec()
        .map_err(|e| SignatureVerificationError::InvalidSignature(e.to_string()))?;
    bytes.try_into().map_err(|v: Vec<u8>| {
        SignatureVerificationError::InvalidSignature(format!(
            "expected {N} {label} bytes, got {}",
            v.len()
        ))
    })
}
