//! Internal helpers ported from `aleph/chains/nuls_aleph_sdk.py`.
//!
//! NULS legacy signature support. Only the bits used by `chains::nuls` are
//! ported here — encoding/parsing of varints, address derivation, message
//! template verification.

use ripemd::Ripemd160;
use sha2::{Digest, Sha256};

pub const MESSAGE_TEMPLATE_PREFIX: &[u8] = b"\x18NULS Signed Message:\n";

/// Custom base58 alphabet used by NULS (note ABCDEFGH order, not BTC's).
const B58_DIGITS: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// XOR of all bytes — used as a one-byte checksum on NULS addresses.
pub fn xor(body: &[u8]) -> u8 {
    body.iter().fold(0u8, |acc, b| acc ^ *b)
}

/// Encodes a NULS-style base58 string.
pub fn b58_encode(data: &[u8]) -> String {
    // Convert big-endian bytes to a bignum, then divide by 58.
    let mut zero_count = 0;
    for b in data {
        if *b == 0 {
            zero_count += 1;
        } else {
            break;
        }
    }

    let mut digits: Vec<u8> = Vec::with_capacity(data.len() * 2);
    let mut n = num_bigint::BigUint::from_bytes_be(data);
    let fifty_eight = num_bigint::BigUint::from(58u32);
    while n > num_bigint::BigUint::from(0u32) {
        let (q, r) = num_integer_div_rem(&n, &fifty_eight);
        digits.push(B58_DIGITS[r as usize]);
        n = q;
    }

    let mut out = String::with_capacity(zero_count + digits.len());
    for _ in 0..zero_count {
        out.push(B58_DIGITS[0] as char);
    }
    for b in digits.iter().rev() {
        out.push(*b as char);
    }
    out
}

fn num_integer_div_rem(
    n: &num_bigint::BigUint,
    d: &num_bigint::BigUint,
) -> (num_bigint::BigUint, u32) {
    let q = n / d;
    let r = n % d;
    // r < 58 fits in u32.
    let r_u64 = r.to_u64_digits().first().copied().unwrap_or(0);
    (q, r_u64 as u32)
}

/// Decodes a NULS-style base58 string.
pub fn b58_decode(s: &str) -> Option<Vec<u8>> {
    if s.is_empty() {
        return Some(Vec::new());
    }
    let mut n = num_bigint::BigUint::from(0u32);
    let fifty_eight = num_bigint::BigUint::from(58u32);
    for c in s.bytes() {
        let idx = B58_DIGITS.iter().position(|&d| d == c)?;
        n = n * &fifty_eight + num_bigint::BigUint::from(idx as u32);
    }
    let mut bytes = n.to_bytes_be();
    // Restore leading zeros.
    let pad = s.bytes().take_while(|&b| b == B58_DIGITS[0]).count();
    let mut out = vec![0u8; pad];
    out.append(&mut bytes);
    Some(out)
}

/// `b58_encode(addr || xor(addr))`.
pub fn address_from_hash(hash: &[u8]) -> String {
    let mut buf = Vec::with_capacity(hash.len() + 1);
    buf.extend_from_slice(hash);
    buf.push(xor(hash));
    b58_encode(&buf)
}

/// Inverse of `address_from_hash`.
pub fn hash_from_address(addr: &str) -> Option<Vec<u8>> {
    let raw = b58_decode(addr)?;
    if raw.is_empty() {
        return None;
    }
    Some(raw[..raw.len() - 1].to_vec())
}

/// Computes the NULS legacy address hash for a pubkey.
/// `chain_id` is a signed 16-bit little-endian.
pub fn public_key_to_hash(pub_key: &[u8], chain_id: i16, address_type: u8) -> Vec<u8> {
    let sha = Sha256::digest(pub_key);
    let ripe = Ripemd160::digest(sha);
    let mut out = Vec::with_capacity(2 + 1 + 20);
    out.extend_from_slice(&chain_id.to_le_bytes());
    out.push(address_type);
    out.extend_from_slice(&ripe);
    out
}

/// Encodes a length-prefixed varint in NULS's compact format.
pub fn varint_encode(value: u64) -> Vec<u8> {
    if value < 253 {
        vec![value as u8]
    } else if value <= 0xFFFF {
        let mut v = vec![253u8];
        v.extend_from_slice(&(value as u16).to_le_bytes());
        v
    } else if value <= 0xFFFF_FFFF {
        let mut v = vec![254u8];
        v.extend_from_slice(&(value as u32).to_le_bytes());
        v
    } else {
        let mut v = vec![255u8];
        v.extend_from_slice(&value.to_le_bytes());
        v
    }
}

/// Reads the [length-prefixed | bytes] format used by NULS.
pub fn read_by_length(buffer: &[u8], cursor: usize) -> Option<(usize, Vec<u8>)> {
    let (length, size) = varint_decode(buffer, cursor)?;
    let start = cursor + size;
    let end = start + length as usize;
    if end > buffer.len() {
        return None;
    }
    Some((size + length as usize, buffer[start..end].to_vec()))
}

fn varint_decode(buf: &[u8], cursor: usize) -> Option<(u64, usize)> {
    if cursor >= buf.len() {
        return None;
    }
    let first = buf[cursor];
    if first < 253 {
        Some((first as u64, 1))
    } else if first == 253 {
        if cursor + 3 > buf.len() {
            return None;
        }
        let mut tmp = [0u8; 2];
        tmp.copy_from_slice(&buf[cursor + 1..cursor + 3]);
        Some((u16::from_le_bytes(tmp) as u64, 3))
    } else if first == 254 {
        if cursor + 5 > buf.len() {
            return None;
        }
        let mut tmp = [0u8; 4];
        tmp.copy_from_slice(&buf[cursor + 1..cursor + 5]);
        Some((u32::from_le_bytes(tmp) as u64, 5))
    } else {
        if cursor + 9 > buf.len() {
            return None;
        }
        let mut tmp = [0u8; 8];
        tmp.copy_from_slice(&buf[cursor + 1..cursor + 9]);
        Some((u64::from_le_bytes(tmp), 9))
    }
}

/// NULS signature object: { pub_key, ecc_type, sig_ser }.
#[derive(Debug)]
pub struct NulsSignature {
    pub pub_key: Vec<u8>,
    pub ecc_type: u8,
    pub sig_ser: Vec<u8>,
}

impl NulsSignature {
    pub fn parse(raw: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let (size, pub_key) = read_by_length(raw, cursor)?;
        cursor += size;
        if cursor >= raw.len() {
            return None;
        }
        let ecc_type = raw[cursor];
        cursor += 1;
        let (_size, sig_ser) = read_by_length(raw, cursor)?;
        Some(Self {
            pub_key,
            ecc_type,
            sig_ser,
        })
    }

    /// Verifies the signature against the NULS message template.
    pub fn verify(&self, message: &[u8]) -> bool {
        use k256::ecdsa::{Signature, VerifyingKey, signature::Verifier as _};

        let mut body = varint_encode(message.len() as u64);
        body.extend_from_slice(message);

        let mut signed = Vec::with_capacity(MESSAGE_TEMPLATE_PREFIX.len() + body.len());
        signed.extend_from_slice(MESSAGE_TEMPLATE_PREFIX);
        signed.extend_from_slice(&body);

        // coincurve's PublicKey.verify uses SHA-256 by default.
        let digest = Sha256::digest(&signed);

        let key = match VerifyingKey::from_sec1_bytes(&self.pub_key) {
            Ok(k) => k,
            Err(_) => return false,
        };
        // NULS uses DER-encoded signatures, but our k256 prefers raw 64-byte.
        // Try DER first, fall back to raw.
        let sig = match Signature::from_der(&self.sig_ser) {
            Ok(s) => s,
            Err(_) => match Signature::from_slice(&self.sig_ser) {
                Ok(s) => s,
                Err(_) => return false,
            },
        };
        key.verify(&digest, &sig).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b58_roundtrip() {
        let data = b"hello-world";
        let encoded = b58_encode(data);
        let decoded = b58_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn varint_round_trip_small() {
        let enc = varint_encode(5);
        assert_eq!(enc, vec![5]);
        let (val, size) = varint_decode(&enc, 0).unwrap();
        assert_eq!(val, 5);
        assert_eq!(size, 1);
    }

    #[test]
    fn varint_round_trip_medium() {
        let enc = varint_encode(300);
        let (val, _) = varint_decode(&enc, 0).unwrap();
        assert_eq!(val, 300);
    }
}
