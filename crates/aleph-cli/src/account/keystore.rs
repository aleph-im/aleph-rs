//! Ethereum keystore V3 (Web3 Secret Storage) encryption and decryption.
//!
//! Pure crypto + serde — no file I/O and no prompting. Write path always
//! uses scrypt with geth-strength parameters; read path also accepts
//! pbkdf2 (HMAC-SHA256) so keystores exported by other tools import.

use aes::cipher::{KeyIvInit, StreamCipher};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

type Aes128Ctr = ctr::Ctr128BE<aes::Aes128>;

// ---------------------------------------------------------------------------
// Scrypt fallback implementation
//
// The `scrypt` crate's `Params::new` enforces `log_n < r * 16` (i.e.
// N < 2^(128*r/8)).  Most keystores produced by real-world tools (the
// official Web3 Secret Storage test vector, Python `eth-keyfile` exports)
// use n=262144 with r=1, which violates that constraint (log_n=18 ≥ 16).
//
// `derive_key` therefore tries `scrypt::Params::new` first; if it succeeds
// (e.g. the geth-standard n=2^18, r=8, p=1 we use for writing), the
// battle-tested `scrypt` crate is used.  Only when the crate rejects the
// parameters does execution fall through to the internal `scrypt_kdf` below.
//
// The internal implementation hand-rolls the Salsa20/8 permutation (it does
// NOT use the `salsa20` crate), BlockMix, and ROMix so that the full
// parameter space accepted by real-world keystores can be handled.
// ---------------------------------------------------------------------------

// Pure Salsa20/8 on a 64-byte block (in-place), without going through the
// streaming cipher trait machinery.
fn salsa20_8_inplace(block: &mut [u8; 64]) {
    #[inline(always)]
    fn quarter_round(state: &mut [u32; 16], a: usize, b: usize, c: usize, d: usize) {
        state[b] ^= state[a].wrapping_add(state[d]).rotate_left(7);
        state[c] ^= state[b].wrapping_add(state[a]).rotate_left(9);
        state[d] ^= state[c].wrapping_add(state[b]).rotate_left(13);
        state[a] ^= state[d].wrapping_add(state[c]).rotate_left(18);
    }

    let mut x = [0u32; 16];
    for (word, chunk) in x.iter_mut().zip(block.chunks_exact(4)) {
        *word = u32::from_le_bytes(chunk.try_into().unwrap());
    }
    let mut z = x;

    for _ in 0..4 {
        // column rounds
        quarter_round(&mut z, 0, 4, 8, 12);
        quarter_round(&mut z, 5, 9, 13, 1);
        quarter_round(&mut z, 10, 14, 2, 6);
        quarter_round(&mut z, 15, 3, 7, 11);
        // row rounds
        quarter_round(&mut z, 0, 1, 2, 3);
        quarter_round(&mut z, 5, 6, 7, 4);
        quarter_round(&mut z, 10, 11, 8, 9);
        quarter_round(&mut z, 15, 12, 13, 14);
    }

    for (i, word) in z.iter_mut().enumerate() {
        *word = word.wrapping_add(x[i]);
    }

    for (chunk, word) in block.chunks_exact_mut(4).zip(z.iter()) {
        chunk.copy_from_slice(&word.to_le_bytes());
    }
}

/// BlockMix: operates on a slice of length 2*r*64 bytes (= 128*r).
fn scrypt_block_mix(input: &[u8], output: &mut [u8]) {
    debug_assert_eq!(input.len(), output.len());
    debug_assert_eq!(input.len() % 128, 0);
    let mut x = [0u8; 64];
    x.copy_from_slice(&input[input.len() - 64..]);

    let mut t = [0u8; 64];

    for (i, chunk) in input.chunks_exact(64).enumerate() {
        // t = x XOR chunk
        for j in 0..64 {
            t[j] = x[j] ^ chunk[j];
        }
        salsa20_8_inplace(&mut t);
        x.copy_from_slice(&t);

        // interleaved output: even blocks at the start, odd blocks at the
        // midpoint (matching the scrypt spec / romix crate behaviour).
        let pos = if i % 2 == 0 {
            (i / 2) * 64
        } else {
            (i / 2) * 64 + input.len() / 2
        };
        output[pos..pos + 64].copy_from_slice(&x);
    }
}

/// ROMix: the memory-hard loop.
fn scrypt_ro_mix(b: &mut [u8], n: usize) {
    use zeroize::Zeroize as _;

    let len = b.len(); // 128 * r
    let mut v = vec![0u8; n * len];
    let mut t = vec![0u8; len];
    let mut scratch = vec![0u8; len];

    // Fill V: V[i] = X, then X = BlockMix(X)
    for i in 0..n {
        v[i * len..(i + 1) * len].copy_from_slice(b);
        scrypt_block_mix(&v[i * len..(i + 1) * len], &mut t);
        b.copy_from_slice(&t);
    }

    // Mix phase
    for _ in 0..n {
        // integerify: the last 64-byte block's first word (LE u32), mod n
        let j = {
            let tail = &b[len - 64..len - 60];
            let word = u32::from_le_bytes(tail.try_into().unwrap());
            (word as usize) & (n - 1)
        };
        for k in 0..len {
            scratch[k] = b[k] ^ v[j * len + k];
        }
        scrypt_block_mix(&scratch, b);
    }

    v.zeroize();
    t.zeroize();
    scratch.zeroize();
}

/// scrypt KDF fallback — used only when `scrypt::Params::new` rejects the
/// parameter set (e.g. n=2^18, r=1 from the official Web3 test vectors).
/// For parameter sets the `scrypt` crate accepts, `derive_key` calls the
/// crate directly and never reaches this function.
fn scrypt_kdf(
    password: &[u8],
    salt: &[u8],
    log_n: u8,
    r: u32,
    p: u32,
    dk: &mut [u8],
) -> Result<(), KeystoreError> {
    use zeroize::Zeroize as _;

    let n: usize = 1usize
        .checked_shl(log_n as u32)
        .ok_or_else(|| KeystoreError::InvalidFormat("scrypt n overflows usize".into()))?;
    let r = r as usize;
    let p = p as usize;
    let block_len = 128 * r;

    // B = PBKDF2-HMAC-SHA256(password, salt, 1, p * 128 * r)
    let b_len = p.checked_mul(block_len)
        .ok_or_else(|| KeystoreError::InvalidFormat("scrypt p*128*r overflows".into()))?;
    let mut b = vec![0u8; b_len];
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password, salt, 1, &mut b);

    // Apply ROMix to each 128*r-byte block
    for chunk in b.chunks_exact_mut(block_len) {
        scrypt_ro_mix(chunk, n);
    }

    // DK = PBKDF2-HMAC-SHA256(password, B, 1, dklen)
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password, &b, 1, dk);

    b.zeroize();
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum KeystoreError {
    #[error("incorrect password")]
    IncorrectPassword,
    #[error("unsupported keystore version {0} (only version 3 is supported)")]
    UnsupportedVersion(u32),
    #[error("unsupported cipher '{0}' (only aes-128-ctr is supported)")]
    UnsupportedCipher(String),
    #[error("unsupported KDF '{0}' (only scrypt and pbkdf2 are supported)")]
    UnsupportedKdf(String),
    #[error("invalid keystore: {0}")]
    InvalidFormat(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeystoreV3 {
    pub version: u32,
    pub id: String,
    /// EVM address, lowercase hex without 0x prefix. Optional: some tools
    /// omit it; we always write it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// Some historical tools (e.g. old geth) capitalize this field.
    #[serde(alias = "Crypto")]
    pub crypto: CryptoSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CryptoSection {
    pub cipher: String,
    pub ciphertext: String,
    pub cipherparams: CipherParams,
    pub kdf: String,
    pub kdfparams: KdfParams,
    pub mac: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CipherParams {
    pub iv: String,
}

/// The two variants have disjoint required fields (`n`/`r`/`p` vs
/// `c`/`prf`), so untagged deserialization is unambiguous.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KdfParams {
    Scrypt {
        dklen: u32,
        n: u64,
        r: u32,
        p: u32,
        salt: String,
    },
    Pbkdf2 {
        dklen: u32,
        c: u32,
        prf: String,
        salt: String,
    },
}

/// Parse a keystore JSON string.
pub fn parse_keystore(json: &str) -> Result<KeystoreV3, KeystoreError> {
    serde_json::from_str(json).map_err(|e| KeystoreError::InvalidFormat(e.to_string()))
}

/// Detect whether `contents` is a V3 keystore.
///
/// Returns `Ok(Some(_))` for a parseable keystore, `Ok(None)` if the
/// contents are not keystore-shaped at all (e.g. a raw hex key — the
/// caller should fall back to other formats), and `Err(_)` if the file
/// is keystore-shaped but malformed (the caller should surface the error
/// rather than misinterpret the file as a hex key).
pub fn try_parse_v3(contents: &str) -> Result<Option<KeystoreV3>, KeystoreError> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(contents) else {
        return Ok(None);
    };
    let Some(obj) = value.as_object() else {
        return Ok(None);
    };
    if !obj.contains_key("crypto") && !obj.contains_key("Crypto") {
        return Ok(None);
    }
    parse_keystore(contents).map(Some)
}

/// Decode a hex private key (with or without 0x prefix) into 32 bytes.
pub fn decode_key_hex(key_hex: &str) -> Result<Zeroizing<[u8; 32]>, KeystoreError> {
    let bytes = Zeroizing::new(
        hex::decode(key_hex.trim().trim_start_matches("0x"))
            .map_err(|e| KeystoreError::InvalidFormat(format!("invalid hex private key: {e}")))?,
    );
    if bytes.len() != 32 {
        return Err(KeystoreError::InvalidFormat(format!(
            "private key must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut out = Zeroizing::new([0u8; 32]);
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn derive_key(crypto: &CryptoSection, password: &str) -> Result<Zeroizing<[u8; 32]>, KeystoreError> {
    let mut dk = Zeroizing::new([0u8; 32]);
    match (crypto.kdf.as_str(), &crypto.kdfparams) {
        ("scrypt", KdfParams::Scrypt { dklen, n, r, p, salt }) => {
            if *dklen != 32 {
                return Err(KeystoreError::InvalidFormat("dklen must be 32".into()));
            }
            let salt = hex::decode(salt)
                .map_err(|e| KeystoreError::InvalidFormat(format!("invalid salt hex: {e}")))?;
            if *n < 2 || !n.is_power_of_two() {
                return Err(KeystoreError::InvalidFormat(
                    "scrypt n must be a power of two >= 2".into(),
                ));
            }
            let log_n = n.trailing_zeros() as u8;
            // Prefer the battle-tested `scrypt` crate. Fall back to our own
            // implementation only when the crate rejects the parameters (e.g.
            // n=2^18, r=1 from the official Web3 Secret Storage test vectors,
            // where log_n=18 ≥ r*16=16).
            match scrypt::Params::new(log_n, *r, *p, 32) {
                Ok(params) => {
                    scrypt::scrypt(password.as_bytes(), &salt, &params, &mut dk[..])
                        .map_err(|_| KeystoreError::InvalidFormat("scrypt failed".into()))?;
                }
                Err(_) => {
                    scrypt_kdf(password.as_bytes(), &salt, log_n, *r, *p, &mut dk[..])?;
                }
            }
        }
        ("pbkdf2", KdfParams::Pbkdf2 { dklen, c, prf, salt }) => {
            if *dklen != 32 {
                return Err(KeystoreError::InvalidFormat("dklen must be 32".into()));
            }
            if prf != "hmac-sha256" {
                return Err(KeystoreError::UnsupportedKdf(format!("pbkdf2 with prf '{prf}'")));
            }
            let salt = hex::decode(salt)
                .map_err(|e| KeystoreError::InvalidFormat(format!("invalid salt hex: {e}")))?;
            pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password.as_bytes(), &salt, *c, &mut dk[..]);
        }
        ("scrypt", _) | ("pbkdf2", _) => {
            return Err(KeystoreError::InvalidFormat(
                "kdfparams do not match the declared kdf".into(),
            ));
        }
        (other, _) => return Err(KeystoreError::UnsupportedKdf(other.to_string())),
    }
    Ok(dk)
}

/// Verify the MAC and decrypt the private key.
///
/// The MAC is checked (in constant time) before any decryption output is
/// produced; a mismatch is reported as an incorrect password.
pub fn decrypt_key(ks: &KeystoreV3, password: &str) -> Result<Zeroizing<[u8; 32]>, KeystoreError> {
    if ks.version != 3 {
        return Err(KeystoreError::UnsupportedVersion(ks.version));
    }
    if ks.crypto.cipher != "aes-128-ctr" {
        return Err(KeystoreError::UnsupportedCipher(ks.crypto.cipher.clone()));
    }

    let ciphertext = hex::decode(&ks.crypto.ciphertext)
        .map_err(|e| KeystoreError::InvalidFormat(format!("invalid ciphertext hex: {e}")))?;
    if ciphertext.len() != 32 {
        return Err(KeystoreError::InvalidFormat(format!(
            "ciphertext must be 32 bytes, got {}",
            ciphertext.len()
        )));
    }
    let iv = hex::decode(&ks.crypto.cipherparams.iv)
        .map_err(|e| KeystoreError::InvalidFormat(format!("invalid IV hex: {e}")))?;
    let expected_mac = hex::decode(&ks.crypto.mac)
        .map_err(|e| KeystoreError::InvalidFormat(format!("invalid MAC hex: {e}")))?;

    let dk = derive_key(&ks.crypto, password)?;

    let mac = Keccak256::new()
        .chain_update(&dk[16..32])
        .chain_update(&ciphertext)
        .finalize();
    if mac.as_slice().ct_eq(&expected_mac).unwrap_u8() != 1 {
        return Err(KeystoreError::IncorrectPassword);
    }

    let mut key = Zeroizing::new([0u8; 32]);
    key.copy_from_slice(&ciphertext);
    let mut cipher = Aes128Ctr::new_from_slices(&dk[..16], &iv)
        .map_err(|_| KeystoreError::InvalidFormat("IV must be 16 bytes".into()))?;
    cipher.apply_keystream(&mut key[..]);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Official Web3 Secret Storage Definition test vectors.
    // Password: "testpassword"
    // Secret:   7a28b5ba57c53603b0b07b56bba752f7784bf506fa95edc395f5cf6c7514fe9d
    const TEST_SECRET: &str = "7a28b5ba57c53603b0b07b56bba752f7784bf506fa95edc395f5cf6c7514fe9d";

    const PBKDF2_VECTOR: &str = r#"{
        "crypto" : {
            "cipher" : "aes-128-ctr",
            "cipherparams" : { "iv" : "6087dab2f9fdbbfaddc31a909735c1e6" },
            "ciphertext" : "5318b4d5bcd28de64ee5559e671353e16f075ecae9f99c7a79a38af5f869aa46",
            "kdf" : "pbkdf2",
            "kdfparams" : {
                "c" : 262144,
                "dklen" : 32,
                "prf" : "hmac-sha256",
                "salt" : "ae3cd4e7013836a3df6bd7241b12db061dbe2c6785853cce422d148a624ce0bd"
            },
            "mac" : "517ead924a9d0dc3124507e3393d175ce3ff7c1e96529c6c555ce9e51205e9b2"
        },
        "id" : "3198bc9c-6672-5ab3-d995-4942343ae5b6",
        "version" : 3
    }"#;

    const SCRYPT_VECTOR: &str = r#"{
        "crypto" : {
            "cipher" : "aes-128-ctr",
            "cipherparams" : { "iv" : "83dbcc02d8ccb40e466191a123791e0e" },
            "ciphertext" : "d172bf743a674da9cdad04534d56926ef8358534d458fffccd4e6ad2fbde479c",
            "kdf" : "scrypt",
            "kdfparams" : {
                "dklen" : 32,
                "n" : 262144,
                "p" : 8,
                "r" : 1,
                "salt" : "ab0c7876052600dd703518d6fc3fe8984592145b591fc8fb5c6d43190334ba19"
            },
            "mac" : "2103ac29920d71da29f15d75b4a16dbe95cfd7ff8faea1056c33131d846e3097"
        },
        "id" : "3198bc9c-6672-5ab3-d995-4942343ae5b6",
        "version" : 3
    }"#;

    #[test]
    fn parse_v3_pbkdf2() {
        let ks = parse_keystore(PBKDF2_VECTOR).unwrap();
        assert_eq!(ks.version, 3);
        assert!(ks.address.is_none());
        assert_eq!(ks.crypto.kdf, "pbkdf2");
        assert!(matches!(ks.crypto.kdfparams, KdfParams::Pbkdf2 { .. }));
    }

    #[test]
    fn parse_v3_scrypt() {
        let ks = parse_keystore(SCRYPT_VECTOR).unwrap();
        assert_eq!(ks.crypto.kdf, "scrypt");
        assert!(matches!(
            ks.crypto.kdfparams,
            KdfParams::Scrypt { n: 262144, r: 1, p: 8, .. }
        ));
    }

    #[test]
    fn decrypt_official_pbkdf2_vector() {
        let ks = parse_keystore(PBKDF2_VECTOR).unwrap();
        let key = decrypt_key(&ks, "testpassword").unwrap();
        assert_eq!(hex::encode(&key[..]), TEST_SECRET);
    }

    #[test]
    fn decrypt_official_scrypt_vector() {
        let ks = parse_keystore(SCRYPT_VECTOR).unwrap();
        let key = decrypt_key(&ks, "testpassword").unwrap();
        assert_eq!(hex::encode(&key[..]), TEST_SECRET);
    }

    #[test]
    fn decrypt_wrong_password_rejected() {
        let ks = parse_keystore(PBKDF2_VECTOR).unwrap();
        let err = decrypt_key(&ks, "wrongpassword").unwrap_err();
        assert!(matches!(err, KeystoreError::IncorrectPassword));
    }

    #[test]
    fn decrypt_tampered_ciphertext_rejected() {
        let mut ks = parse_keystore(PBKDF2_VECTOR).unwrap();
        // Flip the first nibble of the ciphertext
        let mut ct = ks.crypto.ciphertext.clone();
        ct.replace_range(0..1, if &ct[0..1] == "5" { "6" } else { "5" });
        ks.crypto.ciphertext = ct;
        let err = decrypt_key(&ks, "testpassword").unwrap_err();
        assert!(matches!(err, KeystoreError::IncorrectPassword));
    }

    #[test]
    fn decrypt_unsupported_version_rejected() {
        let mut ks = parse_keystore(PBKDF2_VECTOR).unwrap();
        ks.version = 2;
        let err = decrypt_key(&ks, "testpassword").unwrap_err();
        assert!(matches!(err, KeystoreError::UnsupportedVersion(2)));
    }

    #[test]
    fn decrypt_unsupported_cipher_rejected() {
        let mut ks = parse_keystore(PBKDF2_VECTOR).unwrap();
        ks.crypto.cipher = "aes-256-gcm".to_string();
        let err = decrypt_key(&ks, "testpassword").unwrap_err();
        assert!(matches!(err, KeystoreError::UnsupportedCipher(_)));
    }

    #[test]
    fn try_parse_v3_detects_keystore() {
        assert!(try_parse_v3(PBKDF2_VECTOR).unwrap().is_some());
    }

    #[test]
    fn try_parse_v3_rejects_raw_hex() {
        assert!(try_parse_v3(TEST_SECRET).unwrap().is_none());
        assert!(try_parse_v3("0xdeadbeef").unwrap().is_none());
    }

    #[test]
    fn try_parse_v3_rejects_other_json() {
        assert!(try_parse_v3(r#"{"version": 3}"#).unwrap().is_none());
        assert!(try_parse_v3(r#"[1, 2, 3]"#).unwrap().is_none());
    }

    #[test]
    fn try_parse_v3_errors_on_malformed_keystore() {
        // Keystore-shaped (has "crypto") but missing required fields
        let res = try_parse_v3(r#"{"version": 3, "crypto": {"cipher": "aes-128-ctr"}}"#);
        assert!(res.is_err());
    }

    #[test]
    fn decode_key_hex_accepts_with_and_without_prefix() {
        let a = decode_key_hex(TEST_SECRET).unwrap();
        let b = decode_key_hex(&format!("0x{TEST_SECRET}")).unwrap();
        assert_eq!(&a[..], &b[..]);
    }

    #[test]
    fn decode_key_hex_rejects_wrong_length() {
        assert!(decode_key_hex("abcd").is_err());
    }

    #[test]
    fn fallback_scrypt_matches_crate_for_valid_params() {
        // (log_n, r, p) sets accepted by scrypt::Params::new — exercise r>1
        // and p>1 paths of our fallback implementation.
        for &(log_n, r, p) in &[(4u8, 8u32, 1u32), (8, 8, 1), (6, 4, 2), (4, 2, 4)] {
            let mut ours = [0u8; 32];
            scrypt_kdf(b"password", b"salt", log_n, r, p, &mut ours).unwrap();

            let params = scrypt::Params::new(log_n, r, p, 32).unwrap();
            let mut theirs = [0u8; 32];
            scrypt::scrypt(b"password", b"salt", &params, &mut theirs).unwrap();

            assert_eq!(ours, theirs, "mismatch at log_n={log_n} r={r} p={p}");
        }
    }
}
