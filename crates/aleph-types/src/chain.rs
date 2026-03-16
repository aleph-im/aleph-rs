use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Chain {
    #[serde(rename = "ARB")]
    Arbitrum,
    #[serde(rename = "AURORA")]
    Aurora,
    #[serde(rename = "AVAX")]
    Avax,
    #[serde(rename = "BASE")]
    Base,
    #[serde(rename = "BLAST")]
    Blast,
    #[serde(rename = "BOB")]
    Bob,
    #[serde(rename = "BSC")]
    Bsc,
    #[serde(rename = "CSDK")]
    Csdk,
    #[serde(rename = "CYBER")]
    Cyber,
    #[serde(rename = "DOT")]
    Polkadot,
    #[serde(rename = "ES")]
    Eclipse,
    #[serde(rename = "ETH")]
    Ethereum,
    #[serde(rename = "ETHERLINK")]
    Etherlink,
    #[serde(rename = "FRAX")]
    Fraxtal,
    #[serde(rename = "HYPE")]
    Hype,
    #[serde(rename = "INK")]
    Ink,
    #[serde(rename = "LENS")]
    Lens,
    #[serde(rename = "LINEA")]
    Linea,
    #[serde(rename = "LISK")]
    Lisk,
    #[serde(rename = "METIS")]
    Metis,
    #[serde(rename = "MODE")]
    Mode,
    #[serde(rename = "NEO")]
    Neo,
    #[serde(rename = "NULS")]
    Nuls,
    #[serde(rename = "NULS2")]
    Nuls2,
    #[serde(rename = "OP")]
    Optimism,
    #[serde(rename = "POL")]
    Pol,
    #[serde(rename = "SOL")]
    Sol,
    #[serde(rename = "STT")]
    Somnia,
    #[serde(rename = "SONIC")]
    Sonic,
    #[serde(rename = "TEZOS")]
    Tezos,
    #[serde(rename = "UNICHAIN")]
    Unichain,
    #[serde(rename = "WLD")]
    Worldchain,
    #[serde(rename = "ZORA")]
    Zora,
}

impl std::fmt::Display for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Chain::Arbitrum => "ARB",
            Chain::Aurora => "AURORA",
            Chain::Avax => "AVAX",
            Chain::Base => "BASE",
            Chain::Blast => "BLAST",
            Chain::Bob => "BOB",
            Chain::Bsc => "BSC",
            Chain::Csdk => "CSDK",
            Chain::Cyber => "CYBER",
            Chain::Polkadot => "DOT",
            Chain::Eclipse => "ES",
            Chain::Ethereum => "ETH",
            Chain::Etherlink => "ETHERLINK",
            Chain::Fraxtal => "FRAX",
            Chain::Hype => "HYPE",
            Chain::Ink => "INK",
            Chain::Lens => "LENS",
            Chain::Linea => "LINEA",
            Chain::Lisk => "LISK",
            Chain::Metis => "METIS",
            Chain::Mode => "MODE",
            Chain::Neo => "NEO",
            Chain::Nuls => "NULS",
            Chain::Nuls2 => "NULS2",
            Chain::Optimism => "OP",
            Chain::Pol => "POL",
            Chain::Sol => "SOL",
            Chain::Somnia => "STT",
            Chain::Sonic => "SONIC",
            Chain::Tezos => "TEZOS",
            Chain::Unichain => "UNICHAIN",
            Chain::Worldchain => "WLD",
            Chain::Zora => "ZORA",
        };
        f.write_str(s)
    }
}

impl Chain {
    /// Returns true if this chain uses EVM-compatible signature verification
    /// (secp256k1 + EIP-191 personal sign).
    ///
    /// Uses an allow-list so that new chains added to the enum default to
    /// unsupported rather than silently attempting EVM verification.
    pub fn is_evm(&self) -> bool {
        matches!(
            self,
            Chain::Arbitrum
                | Chain::Aurora
                | Chain::Avax
                | Chain::Base
                | Chain::Blast
                | Chain::Bob
                | Chain::Bsc
                | Chain::Cyber
                | Chain::Ethereum
                | Chain::Etherlink
                | Chain::Fraxtal
                | Chain::Hype
                | Chain::Ink
                | Chain::Lens
                | Chain::Linea
                | Chain::Lisk
                | Chain::Metis
                | Chain::Mode
                | Chain::Optimism
                | Chain::Pol
                | Chain::Somnia
                | Chain::Sonic
                | Chain::Unichain
                | Chain::Worldchain
                | Chain::Zora
        )
    }

    /// Returns true if this chain uses SVM-compatible signature verification
    /// (Ed25519).
    ///
    /// Uses an allow-list so that new chains added to the enum default to
    /// unsupported rather than silently attempting Ed25519 verification.
    pub fn is_svm(&self) -> bool {
        matches!(self, Chain::Eclipse | Chain::Sol)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Address(String);

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Address {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for Address {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Macro for creating Address instances from string literals.
///
/// # Example
///
/// ```
/// use aleph_types::address;
/// let address = address!("0x238224C744F4b90b4494516e074D2676ECfC6803");
/// ```
#[macro_export]
macro_rules! address {
    ($address:expr) => {{ $crate::chain::Address::from($address.to_string()) }};
}

/// Cryptographic signature of a message.
///
/// Handles two formats:
/// - **Plain string** (EVM chains): `"0x636728db..."` — a hex-encoded ECDSA signature.
/// - **Structured object** (Solana): `{"signature": "5HH5Z...", "publicKey": "5SwCe..."}`
///   — a base58-encoded Ed25519 signature plus the signer's public key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Signature {
    /// The signature value (hex for EVM, base58 for Solana).
    value: String,
    /// The signer's public key, present for chains that include it
    /// alongside the signature (e.g., Solana).
    public_key: Option<String>,
}

impl Signature {
    /// Returns the signature value as a string.
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Returns the embedded public key, if present (e.g., Solana signatures).
    pub fn public_key(&self) -> Option<&str> {
        self.public_key.as_deref()
    }

    /// Creates a new Signature with an associated public key (e.g., for Solana).
    pub fn with_public_key(value: String, public_key: String) -> Self {
        Self {
            value,
            public_key: Some(public_key),
        }
    }
}

impl From<String> for Signature {
    fn from(value: String) -> Self {
        Self {
            value,
            public_key: None,
        }
    }
}

impl Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.public_key {
            None => serializer.serialize_str(&self.value),
            Some(pk) => {
                use serde::ser::SerializeStruct;
                let mut state = serializer.serialize_struct("Signature", 2)?;
                state.serialize_field("signature", &self.value)?;
                state.serialize_field("publicKey", pk)?;
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StructuredSig {
            signature: String,
            #[serde(rename = "publicKey")]
            public_key: String,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum SigFormat {
            Plain(String),
            Structured(StructuredSig),
        }

        match SigFormat::deserialize(deserializer)? {
            SigFormat::Plain(s) => Ok(Signature {
                value: s,
                public_key: None,
            }),
            SigFormat::Structured(s) => Ok(Signature {
                value: s.signature,
                public_key: Some(s.public_key),
            }),
        }
    }
}

/// Macro for creating Signature instances from string literals.
///
/// # Example
///
/// ```
/// use aleph_types::signature;
/// let signature = signature!("0x123456789");
/// ```
#[macro_export]
macro_rules! signature {
    ($signature:expr) => {{ $crate::chain::Signature::from($signature.to_string()) }};
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ensures Display output matches serde serialization for every Chain variant.
    /// If these drift, the verification buffer will be signed against a different
    /// string than what the protocol expects.
    #[test]
    fn test_chain_display_matches_serde() {
        let chains = [
            Chain::Arbitrum,
            Chain::Aurora,
            Chain::Avax,
            Chain::Base,
            Chain::Blast,
            Chain::Bob,
            Chain::Bsc,
            Chain::Csdk,
            Chain::Cyber,
            Chain::Polkadot,
            Chain::Eclipse,
            Chain::Ethereum,
            Chain::Etherlink,
            Chain::Fraxtal,
            Chain::Hype,
            Chain::Ink,
            Chain::Lens,
            Chain::Linea,
            Chain::Lisk,
            Chain::Metis,
            Chain::Mode,
            Chain::Neo,
            Chain::Nuls,
            Chain::Nuls2,
            Chain::Optimism,
            Chain::Pol,
            Chain::Sol,
            Chain::Somnia,
            Chain::Sonic,
            Chain::Tezos,
            Chain::Unichain,
            Chain::Worldchain,
            Chain::Zora,
        ];

        for chain in &chains {
            let display = chain.to_string();
            let serde = serde_json::to_string(chain).unwrap();
            let serde_unquoted = serde.trim_matches('"');
            assert_eq!(
                display, serde_unquoted,
                "Display and serde disagree for {chain:?}: Display={display}, serde={serde_unquoted}"
            );
        }
    }

    #[test]
    fn test_signature_with_public_key() {
        let sig = Signature::with_public_key(
            "5HH5Z".to_string(),
            "5SwCe".to_string(),
        );
        assert_eq!(sig.as_str(), "5HH5Z");
        assert_eq!(sig.public_key(), Some("5SwCe"));

        let json = serde_json::to_value(&sig).unwrap();
        assert_eq!(json["signature"], "5HH5Z");
        assert_eq!(json["publicKey"], "5SwCe");
    }
}
