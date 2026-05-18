//! Mirrors `aleph/toolkit/ecdsa.py`.
//!
//! secp256k1 (k256) ECDSA helpers used by the auth-token middleware.
//! Wire formats are kept byte-for-byte compatible with the pyaleph reference,
//! which itself wraps `coincurve`:
//!
//!  * Private key: lowercase hex of the 32-byte scalar.
//!  * Public key: lowercase hex of the 33-byte SEC1 compressed encoding.
//!  * Signatures: DER-encoded (`30 ... `), base64-wrapped at the API surface,
//!    deterministic (RFC 6979) over the SHA-256 digest of the message bytes.
//!  * Auth token: `base64("{unix_timestamp}:{base64_signature}")` where the
//!    inner signature signs the UTF-8 timestamp string.
//!
//! All knobs (5 min default freshness, 30 s future-clock-skew tolerance)
//! match pyaleph exactly. See `tests/toolkit/test_ecdsa.py` for the spec.
//!
//! The verifier deliberately swallows every parse / cryptographic error
//! into a boolean `false` — same as the Python `except Exception: return
//! False`. The middleware translates that into a 401.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use k256::ecdsa::signature::{Signer as _, Verifier as _};
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};

/// Default max future-skew tolerance, in seconds. Matches pyaleph
/// (`timestamp - current_time > 30 -> reject`).
pub const FUTURE_SKEW_TOLERANCE_SECS: i64 = 30;

/// Generate a fresh secp256k1 key pair.
///
/// Returns `(private_key_hex, compressed_public_key_hex)`.
/// Matches `generate_key_pair()` in `aleph.toolkit.ecdsa`.
pub fn generate_key_pair() -> (String, String) {
    let signing = SigningKey::random(&mut k256::elliptic_curve::rand_core::OsRng);
    let priv_hex = hex::encode(signing.to_bytes());
    let pub_hex = hex::encode(signing.verifying_key().to_encoded_point(true).as_bytes());
    (priv_hex, pub_hex)
}

/// Build a key pair from an existing private-key hex.
///
/// Matches `generate_key_pair_from_private_key()`.
pub fn generate_key_pair_from_private_key(
    private_key_hex: &str,
) -> Result<(String, String), String> {
    let bytes = hex::decode(private_key_hex).map_err(|e| format!("invalid hex: {e}"))?;
    let signing =
        SigningKey::from_slice(&bytes).map_err(|e| format!("invalid private key: {e}"))?;
    let priv_hex = hex::encode(signing.to_bytes());
    let pub_hex = hex::encode(signing.verifying_key().to_encoded_point(true).as_bytes());
    Ok((priv_hex, pub_hex))
}

/// Sign `message` with `private_key_hex`, returning a lowercase-hex DER
/// signature. The k256 crate uses RFC 6979 deterministic nonces, matching
/// coincurve, so identical inputs produce identical outputs.
pub fn sign_message(private_key_hex: &str, message: &[u8]) -> Result<String, String> {
    let bytes = hex::decode(private_key_hex).map_err(|e| format!("invalid hex: {e}"))?;
    let signing =
        SigningKey::from_slice(&bytes).map_err(|e| format!("invalid private key: {e}"))?;
    let sig: Signature = signing.sign(message);
    // pyaleph stores the DER form on the wire; mirror that.
    Ok(hex::encode(sig.to_der().as_bytes()))
}

/// Sign `message` returning a base64-encoded DER signature, matching the
/// pyaleph `sign_message()` return shape.
pub fn sign_message_b64(private_key_hex: &str, message: &[u8]) -> Result<String, String> {
    let bytes = hex::decode(private_key_hex).map_err(|e| format!("invalid hex: {e}"))?;
    let signing =
        SigningKey::from_slice(&bytes).map_err(|e| format!("invalid private key: {e}"))?;
    let sig: Signature = signing.sign(message);
    Ok(BASE64.encode(sig.to_der().as_bytes()))
}

/// Verify a hex-encoded DER signature against `message` and a compressed
/// public-key hex. Returns `false` on any parse / cryptographic error.
pub fn verify_signature(public_key_hex: &str, signature_hex: &str, message: &[u8]) -> bool {
    let Ok(pub_bytes) = hex::decode(public_key_hex) else {
        return false;
    };
    let Ok(verifying) = VerifyingKey::from_sec1_bytes(&pub_bytes) else {
        return false;
    };
    let Ok(sig_bytes) = hex::decode(signature_hex) else {
        return false;
    };
    // Accept DER first (canonical pyaleph format), fall back to compact 64-byte.
    let sig = match Signature::from_der(&sig_bytes) {
        Ok(s) => s,
        Err(_) => match Signature::from_slice(&sig_bytes) {
            Ok(s) => s,
            Err(_) => return false,
        },
    };
    verifying.verify(message, &sig).is_ok()
}

/// Verify a base64-encoded DER signature against a UTF-8 message string.
/// Mirrors pyaleph's `verify_signature(message, signature_b64, public_key_hex)`.
pub fn verify_signature_b64(public_key_hex: &str, signature_b64: &str, message: &[u8]) -> bool {
    let Ok(sig_bytes) = BASE64.decode(signature_b64) else {
        return false;
    };
    verify_signature_bytes(public_key_hex, &sig_bytes, message)
}

/// Like [`verify_signature`] but with raw signature bytes.
fn verify_signature_bytes(public_key_hex: &str, sig_bytes: &[u8], message: &[u8]) -> bool {
    let Ok(pub_bytes) = hex::decode(public_key_hex) else {
        return false;
    };
    let Ok(verifying) = VerifyingKey::from_sec1_bytes(&pub_bytes) else {
        return false;
    };
    let sig = match Signature::from_der(sig_bytes) {
        Ok(s) => s,
        Err(_) => match Signature::from_slice(sig_bytes) {
            Ok(s) => s,
            Err(_) => return false,
        },
    };
    verifying.verify(message, &sig).is_ok()
}

/// Build an auth token signed with `private_key_hex` for the given UNIX
/// timestamp. Returns `base64("{timestamp}:{base64_signature}")`.
pub fn create_auth_token_at(private_key_hex: &str, timestamp: u64) -> Result<String, String> {
    let ts_str = timestamp.to_string();
    let sig_b64 = sign_message_b64(private_key_hex, ts_str.as_bytes())?;
    let inner = format!("{ts_str}:{sig_b64}");
    Ok(BASE64.encode(inner.as_bytes()))
}

/// Same as [`create_auth_token_at`] but uses the wall-clock now. Matches
/// pyaleph's `create_auth_token`.
pub fn create_auth_token(private_key_hex: &str) -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock before epoch: {e}"))?
        .as_secs();
    create_auth_token_at(private_key_hex, now)
}

/// Verify a base64-wrapped auth token against `public_key_hex`. Rejects
/// tokens older than `max_age_secs` and tokens more than 30 s in the future.
/// Returns `false` on any decoding / cryptographic error.
pub fn verify_auth_token(public_key_hex: &str, token_b64: &str, max_age_secs: u64) -> bool {
    verify_auth_token_at(public_key_hex, token_b64, max_age_secs, now_secs())
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Verify with an explicit `current_time` (seconds since epoch). Useful for
/// deterministic tests; this is what `verify_auth_token` delegates to.
pub fn verify_auth_token_at(
    public_key_hex: &str,
    token_b64: &str,
    max_age_secs: u64,
    current_time: i64,
) -> bool {
    let Ok(decoded) = BASE64.decode(token_b64) else {
        return false;
    };
    let Ok(token_data) = std::str::from_utf8(&decoded) else {
        return false;
    };
    let Some((timestamp_str, signature_b64)) = token_data.split_once(':') else {
        return false;
    };
    let Ok(timestamp) = timestamp_str.parse::<i64>() else {
        return false;
    };
    if current_time - timestamp > max_age_secs as i64
        || timestamp - current_time > FUTURE_SKEW_TOLERANCE_SECS
    {
        return false;
    }
    verify_signature_b64(public_key_hex, signature_b64, timestamp_str.as_bytes())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_key_pair_shape() {
        let (priv_hex, pub_hex) = generate_key_pair();
        assert_eq!(priv_hex.len(), 64);
        assert_eq!(pub_hex.len(), 66);
        assert!(pub_hex.starts_with("02") || pub_hex.starts_with("03"));
    }

    #[test]
    fn generate_from_known_private_key_matches_pyaleph_pubkey() {
        // From `tests/toolkit/test_ecdsa.py::test_token_roundtrip_with_known_values`.
        let priv_hex = "50b44756efbcb9266d974af8a8bcecb97d960fd8ddaadd31ecf2082c757fcaad";
        let expected_pub = "023d3b6f2e92e5d30b8d75291087051f6ef9425abbb626bebc3a5b358bce6007ee";
        let (out_priv, out_pub) = generate_key_pair_from_private_key(priv_hex).unwrap();
        assert_eq!(out_priv, priv_hex);
        assert_eq!(out_pub, expected_pub);
    }

    #[test]
    fn sign_and_verify_round_trip() {
        let (priv_hex, pub_hex) = generate_key_pair();
        let msg = b"test message";
        let sig_hex = sign_message(&priv_hex, msg).unwrap();
        assert!(verify_signature(&pub_hex, &sig_hex, msg));
        // Wrong message -> reject.
        assert!(!verify_signature(&pub_hex, &sig_hex, b"other message"));
        // Wrong key -> reject.
        let (_, other_pub) = generate_key_pair();
        assert!(!verify_signature(&other_pub, &sig_hex, msg));
    }

    #[test]
    fn verify_signature_garbage_inputs() {
        let (_, pub_hex) = generate_key_pair();
        // Bad hex signature.
        assert!(!verify_signature(&pub_hex, "zz-not-hex", b"message"));
        // Bad pubkey hex.
        assert!(!verify_signature("not-hex", "30", b"message"));
        // Truncated signature.
        assert!(!verify_signature(&pub_hex, "30", b"message"));
    }

    #[test]
    fn auth_token_round_trip_valid_time() {
        let (priv_hex, pub_hex) = generate_key_pair();
        let token = create_auth_token(&priv_hex).unwrap();
        assert!(verify_auth_token(&pub_hex, &token, 300));
    }

    #[test]
    fn auth_token_rejected_when_too_old() {
        let (priv_hex, pub_hex) = generate_key_pair();
        let now = now_secs();
        // Token built 600 s ago.
        let token = create_auth_token_at(&priv_hex, (now - 600) as u64).unwrap();
        // Default 5-min age window -> reject.
        assert!(!verify_auth_token(&pub_hex, &token, 300));
        // Wider window -> accept.
        assert!(verify_auth_token(&pub_hex, &token, 700));
    }

    #[test]
    fn auth_token_rejected_when_too_far_in_future() {
        let (priv_hex, pub_hex) = generate_key_pair();
        let now = now_secs();
        // 60 s in the future, beyond the 30 s skew window.
        let token = create_auth_token_at(&priv_hex, (now + 60) as u64).unwrap();
        assert!(!verify_auth_token(&pub_hex, &token, 300));
    }

    #[test]
    fn auth_token_accepts_future_within_skew() {
        let (priv_hex, pub_hex) = generate_key_pair();
        let now = now_secs();
        // 20 s ahead, within 30 s tolerance.
        let token = create_auth_token_at(&priv_hex, (now + 20) as u64).unwrap();
        assert!(verify_auth_token(&pub_hex, &token, 300));
    }

    #[test]
    fn auth_token_signature_tamper_rejected() {
        let (priv_hex, pub_hex) = generate_key_pair();
        let token = create_auth_token(&priv_hex).unwrap();
        // Flip one byte in the b64 token. Most flips will land inside the
        // signature payload; the verifier must reject.
        let mut bytes = token.into_bytes();
        let last = bytes.len() - 5;
        bytes[last] = if bytes[last] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(bytes).unwrap();
        assert!(!verify_auth_token(&pub_hex, &tampered, 300));
    }

    #[test]
    fn auth_token_wrong_public_key_rejected() {
        let (priv_hex, _) = generate_key_pair();
        let (_, other_pub) = generate_key_pair();
        let token = create_auth_token(&priv_hex).unwrap();
        assert!(!verify_auth_token(&other_pub, &token, 300));
    }

    #[test]
    fn auth_token_malformed_rejected() {
        let (_, pub_hex) = generate_key_pair();
        // Not base64.
        assert!(!verify_auth_token(&pub_hex, "invalid_base64!", 300));
        // Valid base64, no colon.
        let no_colon = BASE64.encode(b"no_colon_here");
        assert!(!verify_auth_token(&pub_hex, &no_colon, 300));
        // Valid base64, non-numeric timestamp.
        let bad_ts = BASE64.encode(b"not_a_number:abc");
        assert!(!verify_auth_token(&pub_hex, &bad_ts, 300));
    }

    #[test]
    fn pyaleph_known_keypair_token_round_trip() {
        // Identical to `test_token_roundtrip_with_known_values`.
        let priv_hex = "50b44756efbcb9266d974af8a8bcecb97d960fd8ddaadd31ecf2082c757fcaad";
        let pub_hex = "023d3b6f2e92e5d30b8d75291087051f6ef9425abbb626bebc3a5b358bce6007ee";

        // Cross-check: deterministic signature for a fixed message must match
        // what coincurve / pyaleph emits. The hex below was captured from the
        // reference Python implementation:
        //   PrivateKey.from_hex(priv_hex).sign(b"1700000000").hex()
        let expected_sig_hex = "3045022100aa129a3ac99aab0d41f78341614e4962eaf97e6a20d2c7d3859843a7d38a823f02201478cd92fa6d3e90f5d6bc345e206e02ca78eeb218581918bb815a8a87b372d8";
        let our_sig = sign_message(priv_hex, b"1700000000").unwrap();
        assert_eq!(our_sig, expected_sig_hex);
        assert!(verify_signature(pub_hex, expected_sig_hex, b"1700000000"));

        // Full create+verify round-trip with the known keys.
        let token = create_auth_token(priv_hex).unwrap();
        assert!(verify_auth_token(pub_hex, &token, 300));
    }
}
