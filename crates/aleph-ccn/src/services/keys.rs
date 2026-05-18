//! Node identity keypair generation. Mirrors `aleph/services/keys.py`.
//!
//! Generates a 2048-bit RSA keypair (the same shape used by py-libp2p) and
//! writes:
//! - `node-secret.pkcs8.der` — PKCS#8 DER, consumed by the Aleph.im p2p service.
//! - `node-pub.key`         — PEM-encoded public key.

use std::path::Path;

use rand::rngs::OsRng;
use rsa::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};
use rsa::{RsaPrivateKey, RsaPublicKey};

use crate::AlephError;
use crate::AlephResult;

/// Container for the generated key material. Equivalent to the libp2p
/// `KeyPair` struct on the Python side.
pub struct KeyPair {
    pub private_key: RsaPrivateKey,
    pub public_key: RsaPublicKey,
}

/// Generate a fresh 2048-bit RSA keypair. When `print_key` is true, the PKCS#8
/// PEM-encoded private key is printed to stdout for archiving — exactly what
/// the Python implementation does.
pub fn generate_keypair(print_key: bool) -> AlephResult<KeyPair> {
    let mut rng = OsRng;
    let private_key = RsaPrivateKey::new(&mut rng, 2048)
        .map_err(|e| AlephError::Internal(anyhow::anyhow!("rsa keygen: {e}")))?;
    let public_key = RsaPublicKey::from(&private_key);

    if print_key {
        let pem = private_key
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|e| AlephError::Internal(anyhow::anyhow!("pkcs8 pem: {e}")))?;
        println!("{}", pem.as_str());
    }

    Ok(KeyPair {
        private_key,
        public_key,
    })
}

/// Persist `key_pair` to `key_dir`. Creates the directory if needed; fails if
/// `key_dir` exists and is a non-directory. Mirrors `save_keys`.
pub fn save_keys(key_pair: &KeyPair, key_dir: &Path) -> AlephResult<()> {
    if key_dir.exists() {
        if !key_dir.is_dir() {
            return Err(AlephError::Storage(format!(
                "Key directory ({}) is not a directory",
                key_dir.display()
            )));
        }
    } else {
        std::fs::create_dir_all(key_dir)?;
    }
    let priv_path = key_dir.join("node-secret.pkcs8.der");
    let pub_path = key_dir.join("node-pub.key");

    let der = key_pair
        .private_key
        .to_pkcs8_der()
        .map_err(|e| AlephError::Internal(anyhow::anyhow!("pkcs8 der: {e}")))?;
    std::fs::write(&priv_path, der.as_bytes())?;

    let pem = key_pair
        .public_key
        .to_public_key_pem(LineEnding::LF)
        .map_err(|e| AlephError::Internal(anyhow::anyhow!("public pem: {e}")))?;
    std::fs::write(&pub_path, pem.as_bytes())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey};
    use rsa::traits::PublicKeyParts;
    use tempfile::tempdir;

    #[test]
    fn generate_keypair_produces_2048_bit_key() {
        let kp = generate_keypair(false).unwrap();
        // n.bits() returns the modulus bit-length.
        let bits = kp.private_key.n().bits();
        assert!(
            (2040..=2048).contains(&bits),
            "expected ~2048 bits, got {bits}"
        );
    }

    #[test]
    fn save_and_reload_roundtrip() {
        let kp = generate_keypair(false).unwrap();
        let dir = tempdir().unwrap();
        save_keys(&kp, dir.path()).unwrap();

        let priv_path = dir.path().join("node-secret.pkcs8.der");
        let pub_path = dir.path().join("node-pub.key");
        assert!(priv_path.exists());
        assert!(pub_path.exists());

        let der_bytes = std::fs::read(&priv_path).unwrap();
        let reloaded_priv = RsaPrivateKey::from_pkcs8_der(&der_bytes).unwrap();
        assert_eq!(reloaded_priv.n(), kp.private_key.n());
        assert_eq!(reloaded_priv.e(), kp.private_key.e());

        let pem_bytes = std::fs::read_to_string(&pub_path).unwrap();
        let reloaded_pub = RsaPublicKey::from_public_key_pem(&pem_bytes).unwrap();
        assert_eq!(reloaded_pub.n(), kp.public_key.n());
        assert_eq!(reloaded_pub.e(), kp.public_key.e());
    }

    #[test]
    fn save_fails_when_path_is_a_file() {
        let dir = tempdir().unwrap();
        let bad = dir.path().join("blocker");
        std::fs::write(&bad, b"already here").unwrap();
        let kp = generate_keypair(false).unwrap();
        let res = save_keys(&kp, &bad);
        assert!(res.is_err());
    }
}
