use std::fmt;

const HARDENED_BIT: u32 = 0x80000000;

/// BIP32 derivation path (e.g., m/44'/60'/0'/0/0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivationPath {
    components: Vec<u32>,
}

impl DerivationPath {
    /// Parse from string like "m/44'/60'/0'/0/0".
    pub fn parse(s: &str) -> Result<Self, DerivationPathError> {
        let s = s
            .strip_prefix("m/")
            .ok_or(DerivationPathError::MissingPrefix)?;
        if s.is_empty() {
            return Err(DerivationPathError::Empty);
        }

        let components: Result<Vec<u32>, _> = s
            .split('/')
            .map(|part| {
                let (num_str, hardened) = if let Some(stripped) = part.strip_suffix('\'') {
                    (stripped, true)
                } else {
                    (part, false)
                };
                let index: u32 = num_str
                    .parse()
                    .map_err(|_| DerivationPathError::InvalidComponent(part.to_string()))?;
                if index >= HARDENED_BIT {
                    return Err(DerivationPathError::InvalidComponent(part.to_string()));
                }
                Ok(if hardened {
                    index | HARDENED_BIT
                } else {
                    index
                })
            })
            .collect();

        Ok(Self {
            components: components?,
        })
    }

    /// Create a child path by appending an index.
    pub fn child(&self, index: u32, hardened: bool) -> Self {
        let mut components = self.components.clone();
        components.push(if hardened {
            index | HARDENED_BIT
        } else {
            index
        });
        Self { components }
    }

    /// Encode for APDU: 1-byte count + 4-byte big-endian per component.
    pub fn to_apdu_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + self.components.len() * 4);
        buf.push(self.components.len() as u8);
        for &c in &self.components {
            buf.extend_from_slice(&c.to_be_bytes());
        }
        buf
    }

    /// Default EVM base path: m/44'/60'/0'/0
    pub fn default_evm() -> Self {
        Self {
            components: vec![44 | HARDENED_BIT, 60 | HARDENED_BIT, HARDENED_BIT, 0],
        }
    }

    /// Default Solana base path: m/44'/501'
    pub fn default_sol() -> Self {
        Self {
            components: vec![44 | HARDENED_BIT, 501 | HARDENED_BIT],
        }
    }
}

impl fmt::Display for DerivationPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "m")?;
        for &c in &self.components {
            if c >= HARDENED_BIT {
                write!(f, "/{}'", c & !HARDENED_BIT)?;
            } else {
                write!(f, "/{c}")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DerivationPathError {
    #[error("derivation path must start with 'm/'")]
    MissingPrefix,
    #[error("derivation path must have at least one component")]
    Empty,
    #[error("invalid path component: {0}")]
    InvalidComponent(String),
}

use aleph_types::account::SignError;

#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error("Ledger device not found. Connect your Ledger and unlock it.")]
    DeviceNotFound,
    #[error("The {0} app is not open on your Ledger. Please open it and try again.")]
    WrongApp(String),
    #[error(
        "Could not connect to Ledger — another application may be using it. \
         Close Ledger Live and try again."
    )]
    DeviceBusy,
    #[error("Signing rejected on Ledger device.")]
    UserRejected,
    #[error("Ledger device is locked. Please unlock it and try again.")]
    DeviceLocked,
    #[error("Ledger communication error: {0}")]
    Communication(String),
    #[error("Invalid derivation path: {0}")]
    InvalidPath(#[from] DerivationPathError),
}

impl From<LedgerError> for SignError {
    fn from(e: LedgerError) -> Self {
        SignError::SigningFailed(e.to_string())
    }
}

/// Map a Ledger APDU status code to a LedgerError.
/// Returns None for success (0x9000).
fn apdu_status_to_error(status: u16, app_name: &str) -> Option<LedgerError> {
    match status {
        0x9000 => None,
        0x6804 | 0x5515 => Some(LedgerError::DeviceLocked),
        0x6D00 => Some(LedgerError::WrongApp(app_name.to_string())),
        0x6982 | 0x6985 | 0x6986 => Some(LedgerError::UserRejected),
        0x6A80 => Some(LedgerError::Communication(
            "invalid data sent to device".to_string(),
        )),
        code => Some(LedgerError::Communication(format!(
            "unexpected APDU status: 0x{code:04X}"
        ))),
    }
}

use aleph_types::chain::{Address, Signature};
use coins_ledger::Ledger;
use coins_ledger::common::{APDUCommand, APDUData};
use coins_ledger::transports::LedgerAsync;

/// Connect to the first available Ledger device.
pub async fn connect() -> Result<Ledger, LedgerError> {
    Ledger::init().await.map_err(|e| {
        let msg = e.to_string();
        if msg.contains("device not found") || msg.contains("No device") {
            LedgerError::DeviceNotFound
        } else if msg.contains("busy") || msg.contains("in use") || msg.contains("cannot open") {
            LedgerError::DeviceBusy
        } else {
            LedgerError::Communication(msg)
        }
    })
}

// Ethereum Ledger app APDU constants
const ETH_CLA: u8 = 0xE0;
const ETH_INS_GET_ADDRESS: u8 = 0x02;
const ETH_INS_SIGN_PERSONAL: u8 = 0x08;

/// Fetch an Ethereum address from the Ledger at the given derivation path.
async fn get_evm_address(ledger: &Ledger, path: &DerivationPath) -> Result<Address, LedgerError> {
    let data = path.to_apdu_bytes();

    let command = APDUCommand {
        cla: ETH_CLA,
        ins: ETH_INS_GET_ADDRESS,
        p1: 0x00,
        p2: 0x00,
        data: APDUData::new(&data),
        response_len: None,
    };

    let response = ledger
        .exchange(&command)
        .await
        .map_err(|e| LedgerError::Communication(e.to_string()))?;

    let status = response.retcode();
    if let Some(err) = apdu_status_to_error(status, "Ethereum") {
        return Err(err);
    }

    let response_data = response.data().ok_or_else(|| {
        LedgerError::Communication("no data in Ethereum app response".to_string())
    })?;

    // Response: pubkey_len (1) + pubkey (65) + addr_len (1) + addr_hex (40)
    if response_data.len() < 67 {
        return Err(LedgerError::Communication(
            "truncated response from Ethereum app".to_string(),
        ));
    }
    let pubkey_len = response_data[0] as usize;
    let addr_offset = 1 + pubkey_len;
    if response_data.len() < addr_offset + 1 {
        return Err(LedgerError::Communication(
            "truncated response from Ethereum app".to_string(),
        ));
    }
    let addr_len = response_data[addr_offset] as usize;
    let addr_start = addr_offset + 1;
    if response_data.len() < addr_start + addr_len {
        return Err(LedgerError::Communication(
            "truncated response from Ethereum app".to_string(),
        ));
    }

    let addr_hex = std::str::from_utf8(&response_data[addr_start..addr_start + addr_len])
        .map_err(|_| LedgerError::Communication("invalid UTF-8 in address".to_string()))?;

    Ok(Address::from(format!("0x{addr_hex}")))
}

/// Fetch multiple EVM addresses by iterating the last derivation path component.
pub async fn get_evm_addresses(
    ledger: &Ledger,
    base_path: &DerivationPath,
    count: usize,
) -> Result<Vec<(Address, DerivationPath)>, LedgerError> {
    let mut results = Vec::with_capacity(count);
    for i in 0..count {
        let path = base_path.child(i as u32, false);
        let address = get_evm_address(ledger, &path).await?;
        results.push((address, path));
    }
    Ok(results)
}

// Solana Ledger app APDU constants
const SOL_CLA: u8 = 0xE0;
const SOL_INS_GET_ADDRESS: u8 = 0x05;

/// Fetch a Solana address from the Ledger at the given derivation path.
async fn get_sol_address(ledger: &Ledger, path: &DerivationPath) -> Result<Address, LedgerError> {
    let data = path.to_apdu_bytes();

    let command = APDUCommand {
        cla: SOL_CLA,
        ins: SOL_INS_GET_ADDRESS,
        p1: 0x00,
        p2: 0x00,
        data: APDUData::new(&data),
        response_len: None,
    };

    let response = ledger
        .exchange(&command)
        .await
        .map_err(|e| LedgerError::Communication(e.to_string()))?;

    let status = response.retcode();
    if let Some(err) = apdu_status_to_error(status, "Solana") {
        return Err(err);
    }

    let response_data = response
        .data()
        .ok_or_else(|| LedgerError::Communication("no data in Solana app response".to_string()))?;

    if response_data.len() < 32 {
        return Err(LedgerError::Communication(
            "truncated response from Solana app".to_string(),
        ));
    }

    let address = bs58::encode(&response_data[..32]).into_string();
    Ok(Address::from(address))
}

/// Fetch multiple Solana addresses. Solana uses hardened child indices.
pub async fn get_sol_addresses(
    ledger: &Ledger,
    base_path: &DerivationPath,
    count: usize,
) -> Result<Vec<(Address, DerivationPath)>, LedgerError> {
    let mut results = Vec::with_capacity(count);
    for i in 0..count {
        let path = base_path.child(i as u32, true); // hardened for Ed25519
        let address = get_sol_address(ledger, &path).await?;
        results.push((address, path));
    }
    Ok(results)
}

/// Sign a message using the Ethereum Ledger app (EIP-191 personal_sign).
///
/// IMPORTANT: The Ledger performs EIP-191 hashing internally (prepends
/// "\x19Ethereum Signed Message:\n{len}" and Keccak-256 hashes). We send
/// the raw message, NOT a pre-hashed digest.
pub async fn sign_evm(
    ledger: &Ledger,
    path: &DerivationPath,
    message: &[u8],
) -> Result<Signature, LedgerError> {
    let path_bytes = path.to_apdu_bytes();
    let msg_len_bytes = (message.len() as u32).to_be_bytes();

    let mut first_chunk = Vec::new();
    first_chunk.extend_from_slice(&path_bytes);
    first_chunk.extend_from_slice(&msg_len_bytes);

    let first_msg_bytes = message.len().min(255 - first_chunk.len());
    first_chunk.extend_from_slice(&message[..first_msg_bytes]);

    let command = APDUCommand {
        cla: ETH_CLA,
        ins: ETH_INS_SIGN_PERSONAL,
        p1: 0x00,
        p2: 0x00,
        data: APDUData::new(&first_chunk),
        response_len: None,
    };

    let mut response = ledger
        .exchange(&command)
        .await
        .map_err(|e| LedgerError::Communication(e.to_string()))?;

    let mut offset = first_msg_bytes;
    while offset < message.len() {
        let status = response.retcode();
        if status != 0x9000
            && let Some(err) = apdu_status_to_error(status, "Ethereum")
        {
            return Err(err);
        }

        let end = (offset + 255).min(message.len());
        let chunk = &message[offset..end];

        let command = APDUCommand {
            cla: ETH_CLA,
            ins: ETH_INS_SIGN_PERSONAL,
            p1: 0x80,
            p2: 0x00,
            data: APDUData::new(chunk),
            response_len: None,
        };

        response = ledger
            .exchange(&command)
            .await
            .map_err(|e| LedgerError::Communication(e.to_string()))?;

        offset = end;
    }

    let status = response.retcode();
    if let Some(err) = apdu_status_to_error(status, "Ethereum") {
        return Err(err);
    }

    let data = response.data().ok_or_else(|| {
        LedgerError::Communication("no signature data from Ethereum app".to_string())
    })?;
    if data.len() < 65 {
        return Err(LedgerError::Communication(
            "truncated signature from Ethereum app".to_string(),
        ));
    }

    // Ledger returns v (1) || r (32) || s (32)
    // Aleph expects r (32) || s (32) || v (1)
    let v = data[0];
    let r = &data[1..33];
    let s = &data[33..65];

    let mut sig_bytes = [0u8; 65];
    sig_bytes[..32].copy_from_slice(r);
    sig_bytes[32..64].copy_from_slice(s);
    sig_bytes[64] = v;

    Ok(Signature::from(format!("0x{}", hex::encode(sig_bytes))))
}

use aleph_types::chain::Chain;

pub struct LedgerEvmAccount {
    address: Address,
    chain: Chain,
    derivation_path: DerivationPath,
}

impl LedgerEvmAccount {
    pub fn new(address: Address, chain: Chain, derivation_path: DerivationPath) -> Self {
        Self {
            address,
            chain,
            derivation_path,
        }
    }
}

impl aleph_types::account::Account for LedgerEvmAccount {
    fn chain(&self) -> Chain {
        self.chain.clone()
    }

    fn address(&self) -> &Address {
        &self.address
    }

    fn sign_raw(&self, buffer: &[u8]) -> Result<Signature, SignError> {
        tokio::runtime::Handle::current().block_on(async {
            let ledger = connect().await.map_err(SignError::from)?;
            sign_evm(&ledger, &self.derivation_path, buffer)
                .await
                .map_err(Into::into)
        })
    }
}

pub struct LedgerSolanaAccount {
    address: Address,
    chain: Chain,
    #[allow(dead_code)] // Will be used when Solana off-chain signing is supported
    derivation_path: DerivationPath,
}

impl LedgerSolanaAccount {
    pub fn new(address: Address, chain: Chain, derivation_path: DerivationPath) -> Self {
        Self {
            address,
            chain,
            derivation_path,
        }
    }
}

impl aleph_types::account::Account for LedgerSolanaAccount {
    fn chain(&self) -> Chain {
        self.chain.clone()
    }

    fn address(&self) -> &Address {
        &self.address
    }

    fn sign_raw(&self, _buffer: &[u8]) -> Result<Signature, SignError> {
        // The Solana Ledger app wraps messages in an off-chain message format
        // before signing, producing signatures over a different byte sequence
        // than what Aleph's verification expects (raw Ed25519 over the
        // verification buffer). Until the Aleph protocol supports Solana
        // off-chain message signatures, Ledger signing is EVM-only.
        Err(SignError::SigningFailed(
            "Solana Ledger signing is not yet supported. The Solana Ledger app's \
             off-chain message format is incompatible with Aleph's signature verification. \
             Use an EVM Ledger account or a local Solana key instead."
                .to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_evm_path() {
        let path = DerivationPath::parse("m/44'/60'/0'/0/0").unwrap();
        assert_eq!(path.components.len(), 5);
        assert_eq!(path.components[0], 44 | HARDENED_BIT);
        assert_eq!(path.components[1], 60 | HARDENED_BIT);
        assert_eq!(path.components[4], 0); // not hardened
    }

    #[test]
    fn parse_sol_path() {
        let path = DerivationPath::parse("m/44'/501'/0'").unwrap();
        assert_eq!(path.components.len(), 3);
        assert_eq!(path.components[2], HARDENED_BIT);
    }

    #[test]
    fn roundtrip_display() {
        let path = DerivationPath::parse("m/44'/60'/0'/0/0").unwrap();
        assert_eq!(path.to_string(), "m/44'/60'/0'/0/0");
    }

    #[test]
    fn child_appends() {
        let base = DerivationPath::default_evm();
        let child = base.child(3, false);
        assert_eq!(child.to_string(), "m/44'/60'/0'/0/3");
    }

    #[test]
    fn child_hardened() {
        let base = DerivationPath::default_sol();
        let child = base.child(2, true);
        assert_eq!(child.to_string(), "m/44'/501'/2'");
    }

    #[test]
    fn apdu_encoding() {
        let path = DerivationPath::parse("m/44'/60'/0'/0/0").unwrap();
        let bytes = path.to_apdu_bytes();
        assert_eq!(bytes[0], 5); // 5 components
        assert_eq!(bytes.len(), 1 + 5 * 4);
        // First component: 44 | 0x80000000 = 0x8000002C
        assert_eq!(&bytes[1..5], &[0x80, 0x00, 0x00, 0x2C]);
    }

    #[test]
    fn parse_rejects_missing_prefix() {
        assert!(DerivationPath::parse("44'/60'").is_err());
    }

    #[test]
    fn parse_rejects_empty() {
        assert!(DerivationPath::parse("m/").is_err());
    }

    #[test]
    fn parse_rejects_invalid_component() {
        assert!(DerivationPath::parse("m/44'/abc").is_err());
    }

    #[test]
    fn default_evm_path() {
        assert_eq!(DerivationPath::default_evm().to_string(), "m/44'/60'/0'/0");
    }

    #[test]
    fn default_sol_path() {
        assert_eq!(DerivationPath::default_sol().to_string(), "m/44'/501'");
    }

    #[test]
    fn apdu_success() {
        assert!(apdu_status_to_error(0x9000, "Ethereum").is_none());
    }

    #[test]
    fn apdu_user_rejected() {
        let err = apdu_status_to_error(0x6985, "Ethereum").unwrap();
        assert!(matches!(err, LedgerError::UserRejected));
    }

    #[test]
    fn apdu_wrong_app() {
        let err = apdu_status_to_error(0x6D00, "Ethereum").unwrap();
        assert!(matches!(err, LedgerError::WrongApp(name) if name == "Ethereum"));
    }

    #[test]
    fn apdu_device_locked() {
        let err = apdu_status_to_error(0x6804, "Solana").unwrap();
        assert!(matches!(err, LedgerError::DeviceLocked));
    }

    #[test]
    fn apdu_unknown_code() {
        let err = apdu_status_to_error(0x1234, "Ethereum").unwrap();
        assert!(matches!(err, LedgerError::Communication(_)));
    }

    #[test]
    fn ledger_error_converts_to_sign_error() {
        let err: SignError = LedgerError::UserRejected.into();
        assert!(err.to_string().contains("rejected"));
    }

    #[test]
    fn ledger_solana_signing_not_supported() {
        use aleph_types::account::Account;
        let account = LedgerSolanaAccount::new(
            Address::from("7Hg3test".to_string()),
            Chain::Sol,
            DerivationPath::default_sol(),
        );
        let err = account.sign_raw(b"test").unwrap_err();
        assert!(err.to_string().contains("not yet supported"));
        assert!(err.to_string().contains("off-chain"));
    }
}
