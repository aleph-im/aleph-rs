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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Address(String);

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Signature(String);

impl From<String> for Signature {
    fn from(value: String) -> Self {
        Self(value)
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
