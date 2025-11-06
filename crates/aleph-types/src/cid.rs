use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// Newtype for IPFS CIDv0 (base58-encoded, starts with "Qm", 46 characters).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CidV0(String);

/// Newtype for IPFS CIDv1 (multibase-encoded with various encodings).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CidV1(String);

/// Represents an IPFS Content Identifier (CID).
/// Supports both CIDv0 (base58-encoded SHA-256 multihash) and CIDv1 (multibase-encoded).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Cid {
    /// CIDv0: Always a base58-encoded multihash starting with "Qm"
    V0(CidV0),
    /// CIDv1: Multibase-encoded CID with various encodings (base32, base58btc, etc.)
    V1(CidV1),
}

#[derive(Error, Debug)]
pub enum CidError {
    #[error("invalid CID: empty string")]
    EmptyString,
    #[error("invalid CID format: unrecognized version or encoding")]
    InvalidFormat,
    #[error("invalid CIDv0: must start with 'Qm' and be 46 characters")]
    InvalidV0,
}

impl CidV0 {
    /// Creates a new CIDv0 from a string.
    /// CIDv0 must start with "Qm" and be exactly 46 characters long.
    pub fn new(cid: String) -> Result<Self, CidError> {
        if cid.starts_with("Qm") && cid.len() == 46 {
            Ok(CidV0(cid))
        } else {
            Err(CidError::InvalidV0)
        }
    }

    /// Returns the CIDv0 as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the CIDv0 and returns the inner string.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl TryFrom<String> for CidV0 {
    type Error = CidError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        CidV0::new(value)
    }
}

impl TryFrom<&str> for CidV0 {
    type Error = CidError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        CidV0::new(value.to_string())
    }
}

impl Display for CidV0 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl CidV1 {
    /// Creates a new CIDv1 from a string.
    /// CIDv1 typically starts with 'b' (base32) or 'z' (base58btc), but can have other multibase prefixes.
    pub fn new(cid: String) -> Self {
        CidV1(cid)
    }

    /// Returns the CIDv1 as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the CIDv1 and returns the inner string.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl From<String> for CidV1 {
    fn from(value: String) -> Self {
        CidV1(value)
    }
}

impl Display for CidV1 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Cid {
    /// Creates a new CIDv0 variant.
    pub fn v0(cid: CidV0) -> Self {
        Cid::V0(cid)
    }

    /// Creates a new CIDv1 variant.
    pub fn v1(cid: CidV1) -> Self {
        Cid::V1(cid)
    }

    /// Returns the CID as a string slice.
    pub fn as_str(&self) -> &str {
        match self {
            Cid::V0(cid) => cid.as_str(),
            Cid::V1(cid) => cid.as_str(),
        }
    }

    /// Checks if this is a CIDv0.
    pub fn is_v0(&self) -> bool {
        matches!(self, Cid::V0(_))
    }

    /// Checks if this is a CIDv1.
    pub fn is_v1(&self) -> bool {
        matches!(self, Cid::V1(_))
    }
}

impl TryFrom<String> for Cid {
    type Error = CidError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(CidError::EmptyString);
        }

        // CIDv0: starts with "Qm" and is 46 characters long
        if value.starts_with("Qm") && value.len() == 46 {
            return Ok(Cid::V0(CidV0(value)));
        }

        // CIDv1: multibase-encoded, typically starts with 'b' (base32) or 'z' (base58btc)
        // Common prefixes: b (base32), B (base32upper), z (base58btc), f (base16), F (base16upper),
        // m (base64), M (base64url), u (base64url), U (base64urlpad)
        if value.len() > 1 {
            let first_char = value.chars().next().unwrap();
            // Check for common multibase prefixes
            if matches!(
                first_char,
                'b' | 'B' | 'z' | 'f' | 'F' | 'm' | 'M' | 'u' | 'U'
            ) {
                return Ok(Cid::V1(CidV1(value)));
            }
        }

        Err(CidError::InvalidFormat)
    }
}

impl TryFrom<&str> for Cid {
    type Error = CidError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Cid::try_from(value.to_string())
    }
}

impl Display for Cid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<CidV0> for Cid {
    fn from(value: CidV0) -> Self {
        Cid::V0(value)
    }
}

impl From<CidV1> for Cid {
    fn from(value: CidV1) -> Self {
        Cid::V1(value)
    }
}

// Custom serialization for Cid
impl Serialize for Cid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

// Custom deserialization for Cid that detects the version
impl<'de> Deserialize<'de> for Cid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Cid::try_from(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cidv0_new() {
        let cid_str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8".to_string();
        let cid = CidV0::new(cid_str.clone()).unwrap();
        assert_eq!(cid.as_str(), cid_str);
    }

    #[test]
    fn test_cidv0_try_from() {
        let cid_str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";
        let cid = CidV0::try_from(cid_str).unwrap();
        assert_eq!(cid.as_str(), cid_str);
    }

    #[test]
    fn test_cidv0_invalid_length() {
        let cid_str = "QmYULJo".to_string();
        let result = CidV0::new(cid_str);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CidError::InvalidV0));
    }

    #[test]
    fn test_cidv0_invalid_prefix() {
        let cid_str = "XmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8".to_string();
        let result = CidV0::new(cid_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_cidv0_display() {
        let cid_str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";
        let cid = CidV0::try_from(cid_str).unwrap();
        assert_eq!(format!("{}", cid), cid_str);
    }

    #[test]
    fn test_cidv1_new() {
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi".to_string();
        let cid = CidV1::new(cid_str.clone());
        assert_eq!(cid.as_str(), cid_str);
    }

    #[test]
    fn test_cidv1_from_string() {
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi".to_string();
        let cid = CidV1::from(cid_str.clone());
        assert_eq!(cid.as_str(), cid_str);
    }

    #[test]
    fn test_cidv1_display() {
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let cid = CidV1::new(cid_str.to_string());
        assert_eq!(format!("{}", cid), cid_str);
    }

    #[test]
    fn test_cid_from_cidv0() {
        let cid_str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";
        let cidv0 = CidV0::try_from(cid_str).unwrap();
        let cid = Cid::from(cidv0);
        assert!(cid.is_v0());
        assert_eq!(cid.as_str(), cid_str);
    }

    #[test]
    fn test_cid_from_cidv1() {
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let cidv1 = CidV1::new(cid_str.to_string());
        let cid = Cid::from(cidv1);
        assert!(cid.is_v1());
        assert_eq!(cid.as_str(), cid_str);
    }

    #[test]
    fn test_cid_try_from_v0_string() {
        let cid_str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";
        let cid = Cid::try_from(cid_str).unwrap();
        assert!(cid.is_v0());
        assert_eq!(cid.as_str(), cid_str);
    }

    #[test]
    fn test_cid_try_from_v1_base32() {
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let cid = Cid::try_from(cid_str).unwrap();
        assert!(cid.is_v1());
        assert_eq!(cid.as_str(), cid_str);
    }

    #[test]
    fn test_cid_try_from_v1_base58btc() {
        let cid_str = "zdj7WWeQ43G6JJvLWQWZpyHuAMq6uYWRjkBXFad11vE2LHhQ7";
        let cid = Cid::try_from(cid_str).unwrap();
        assert!(cid.is_v1());
        assert_eq!(cid.as_str(), cid_str);
    }

    #[test]
    fn test_cid_empty_string() {
        let result = Cid::try_from("");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CidError::EmptyString));
    }

    #[test]
    fn test_cid_invalid_format() {
        let result = Cid::try_from("invalid_cid_format");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CidError::InvalidFormat));
    }

    #[test]
    fn test_cid_display() {
        let cid_str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";
        let cid = Cid::try_from(cid_str).unwrap();
        assert_eq!(format!("{}", cid), cid_str);
    }

    #[test]
    fn test_cidv0_serde() {
        let cid_str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";
        let cid = CidV0::try_from(cid_str).unwrap();

        let json = serde_json::to_string(&cid).unwrap();
        assert_eq!(json, format!("\"{}\"", cid_str));

        let deserialized: CidV0 = serde_json::from_str(&json).unwrap();
        assert_eq!(cid, deserialized);
    }

    #[test]
    fn test_cidv1_serde() {
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let cid = CidV1::new(cid_str.to_string());

        let json = serde_json::to_string(&cid).unwrap();
        assert_eq!(json, format!("\"{}\"", cid_str));

        let deserialized: CidV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(cid, deserialized);
    }

    #[test]
    fn test_cid_serde_v0() {
        let cid_str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8";
        let cid = Cid::try_from(cid_str).unwrap();

        let json = serde_json::to_string(&cid).unwrap();
        assert_eq!(json, format!("\"{}\"", cid_str));

        let deserialized: Cid = serde_json::from_str(&json).unwrap();
        assert_eq!(cid, deserialized);
        assert!(deserialized.is_v0());
    }

    #[test]
    fn test_cid_serde_v1() {
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let cid = Cid::try_from(cid_str).unwrap();

        let json = serde_json::to_string(&cid).unwrap();
        assert_eq!(json, format!("\"{}\"", cid_str));

        let deserialized: Cid = serde_json::from_str(&json).unwrap();
        assert_eq!(cid, deserialized);
        assert!(deserialized.is_v1());
    }

    #[test]
    fn test_cidv0_into_inner() {
        let cid_str = "QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8".to_string();
        let cid = CidV0::new(cid_str.clone()).unwrap();
        assert_eq!(cid.into_inner(), cid_str);
    }

    #[test]
    fn test_cidv1_into_inner() {
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi".to_string();
        let cid = CidV1::new(cid_str.clone());
        assert_eq!(cid.into_inner(), cid_str);
    }
}
