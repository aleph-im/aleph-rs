use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use thiserror::Error;
use crate::cid::Cid;

const HASH_LENGTH: usize = 32;

#[derive(Error, Debug)]
pub enum ItemHashError {
    #[error("Could not determine hash type: '{0}'")]
    UnknownHashType(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ItemHash {
    Native(AlephItemHash),
    Ipfs(Cid),
}

impl From<AlephItemHash> for ItemHash {
    fn from(value: AlephItemHash) -> Self {
        Self::Native(value)
    }
}

impl From<[u8; HASH_LENGTH]> for ItemHash {
    fn from(value: [u8; HASH_LENGTH]) -> Self {
        Self::Native(AlephItemHash::new(value))
    }
}

impl From<Cid> for ItemHash {
    fn from(value: Cid) -> Self {
        Self::Ipfs(value)
    }
}

impl TryFrom<&str> for ItemHash {
    type Error = ItemHashError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Ok(native_hash) = AlephItemHash::try_from(value) {
            return Ok(Self::Native(native_hash));
        }
        if let Ok(cid) = Cid::try_from(value) {
            return Ok(Self::Ipfs(cid));
        }

        Err(ItemHashError::UnknownHashType(value.to_string()))
    }
}

impl FromStr for ItemHash {
    type Err = ItemHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl Display for ItemHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ItemHash::Native(hash) => write!(f, "{}", hash),
            ItemHash::Ipfs(cid) => write!(f, "{}", cid),
        }
    }
}

/// Macro for creating ItemHash instances from hex string literals.
///
/// This macro simplifies creating ItemHash instances in tests and other code
/// by panicking on invalid input (similar to `vec!` or `format!`).
///
/// # Example
///
/// ```
/// use aleph_types::item_hash;
/// let hash = item_hash!("3c5b05761c8f94a7b8fe6d0d43e5fb91f9689c53c078a870e5e300c7da8a1878");
/// ```
#[macro_export]
macro_rules! item_hash {
    ($hash:expr) => {{ $crate::item_hash::ItemHash::try_from($hash).expect(concat!("Invalid ItemHash: ", $hash)) }};
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AlephItemHash {
    bytes: [u8; HASH_LENGTH],
}

impl AlephItemHash {
    pub fn new(bytes: [u8; HASH_LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let result = hasher.finalize();
        let mut hash_bytes = [0u8; HASH_LENGTH];
        hash_bytes.copy_from_slice(&result);
        Self { bytes: hash_bytes }
    }

    pub fn as_bytes(&self) -> &[u8; HASH_LENGTH] {
        &self.bytes
    }
}

#[derive(Error, Debug)]
pub enum AlephItemHashError {
    #[error("{0}: invalid hash length, expected 64 hex characters")]
    InvalidLength(String),
    #[error("invalid hex digit in hash string: {0}")]
    InvalidHexDigit(String),
}

impl TryFrom<&str> for AlephItemHash {
    type Error = AlephItemHashError;

    fn try_from(hex: &str) -> Result<Self, Self::Error> {
        if hex.len() != 2 * HASH_LENGTH {
            return Err(AlephItemHashError::InvalidLength(hex.to_string()));
        }
        let mut bytes = [0u8; HASH_LENGTH];
        for i in 0..HASH_LENGTH {
            bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                .map_err(|_| AlephItemHashError::InvalidHexDigit(hex.to_string()))?;
        }
        Ok(Self { bytes })
    }
}

impl FromStr for AlephItemHash {
    type Err = AlephItemHashError; // whatever TryFrom<String> returns

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        AlephItemHash::try_from(s)
    }
}

impl Display for AlephItemHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for byte in self.bytes.iter() {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl Serialize for AlephItemHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for AlephItemHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_aleph_item_hash() {
        let bytes = [0u8; HASH_LENGTH];
        let hash = AlephItemHash::new(bytes);
        assert_eq!(hash.as_bytes(), &bytes);
    }

    #[test]
    fn test_aleph_item_hash_from_bytes() {
        let input = b"test data";
        let hash = AlephItemHash::from_bytes(input);
        assert_eq!(hash.as_bytes().len(), HASH_LENGTH);
    }

    #[test]
    fn test_try_from_valid_hex() {
        let hex = "3c5b05761c8f94a7b8fe6d0d43e5fb91f9689c53c078a870e5e300c7da8a1878";
        let hash = ItemHash::try_from(hex).unwrap();
        assert_eq!(format!("{}", hash), hex);
    }

    #[test]
    fn test_try_from_invalid() {
        // Test invalid length
        assert!(ItemHash::try_from("000").is_err());
        // Test invalid hex digits
        assert!(
            ItemHash::try_from("00000000000000000000000000000000000000000000000000000000000000zz")
                .is_err()
        );
    }

    #[test]
    fn test_display() {
        let bytes = [0xab; HASH_LENGTH];
        let hash = ItemHash::from(bytes);
        assert_eq!(
            format!("{}", hash),
            "abababababababababababababababababababababababababababababababab"
        );
    }

    #[test]
    fn test_convert_back_to_string() {
        let item_hash_str = "3c5b05761c8f94a7b8fe6d0d43e5fb91f9689c53c078a870e5e300c7da8a1878";
        let item_hash =
            ItemHash::try_from(item_hash_str).expect("failed to decode a valid item hash");
        let converted_item_hash_str = item_hash.to_string();

        assert_eq!(item_hash_str, converted_item_hash_str);
    }

    #[test]
    fn test_serde() {
        let item_hash_str = "8eb3e437b5d626da009dc6202617dbdd183ed073b6cad37c64b039b8d5127e2f";
        let item_hash = ItemHash::try_from(item_hash_str).unwrap();

        let json_item_hash = format!("\"{item_hash_str}\"");

        let deserialized_item_hash: ItemHash = serde_json::from_str(&json_item_hash).unwrap();
        assert_eq!(item_hash, deserialized_item_hash);

        let serialized_item_hash = serde_json::to_string(&deserialized_item_hash).unwrap();
        assert_eq!(json_item_hash, serialized_item_hash);
    }
}
