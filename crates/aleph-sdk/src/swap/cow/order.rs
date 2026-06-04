//! GPv2 order: the EIP-712 struct, domain, appData, and signing.

use alloy_primitives::{Address, B256, address, keccak256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolStruct, eip712_domain, sol};

use crate::swap::SwapError;

/// GPv2 settlement contract - the EIP-712 `verifyingContract`. Same on every
/// CoW chain.
pub const SETTLEMENT: Address = address!("9008D19f58AAbD9eD0D60971565AA8510560ab41");

/// Vault relayer - the ERC20 approval target for sell tokens. Same on every
/// CoW chain.
pub const VAULT_RELAYER: Address = address!("C92E8bdf79f0507f65a392b0ab4667716BFE0110");

/// Minimal appData document. CoW accepts the keccak256 of this JSON as the
/// order's appData hash. Kept byte-stable (no whitespace) so the hash is
/// reproducible.
///
/// The `"version":"1.3.0"` field is the **CoW appData document schema
/// version** (from <https://github.com/cowprotocol/app-data>), not the
/// aleph-cli or crate version. It must not be bumped on CLI releases.
pub const APP_DATA_JSON: &str = r#"{"appCode":"aleph-cli","version":"1.3.0","metadata":{}}"#;

sol! {
    /// The GPv2 order, exactly as the orderbook expects it for EIP-712.
    /// `kind`/`sellTokenBalance`/`buyTokenBalance` are `string` so alloy
    /// hashes them the way the protocol's typehash does.
    #[derive(Debug)]
    struct Order {
        address sellToken;
        address buyToken;
        address receiver;
        uint256 sellAmount;
        uint256 buyAmount;
        uint32 validTo;
        bytes32 appData;
        uint256 feeAmount;
        string kind;
        bool partiallyFillable;
        string sellTokenBalance;
        string buyTokenBalance;
    }
}

/// keccak256 of [`APP_DATA_JSON`] - the order's appData field.
pub fn app_data_hash() -> B256 {
    keccak256(APP_DATA_JSON.as_bytes())
}

/// Compute the EIP-712 signing digest for `order` on `chain_id`.
pub fn order_digest(order: &Order, chain_id: u64) -> B256 {
    let domain = eip712_domain! {
        name: "Gnosis Protocol",
        version: "v2",
        chain_id: chain_id,
        verifying_contract: SETTLEMENT,
    };
    order.eip712_signing_hash(&domain)
}

/// Sign the order digest with a secp256k1 signer, returning the 65-byte
/// `r||s||v` signature as `0x`-prefixed hex (v in {27,28}).
pub fn sign_order(
    order: &Order,
    chain_id: u64,
    signer: &PrivateKeySigner,
) -> Result<String, SwapError> {
    let digest = order_digest(order, chain_id);
    let sig = signer.sign_hash_sync(&digest).map_err(SwapError::Sign)?;
    Ok(sig.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;

    fn sample_order() -> Order {
        Order {
            sellToken: address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            buyToken: address!("27702a26126e0B3702af63Ee09aC4d1A084EF628"),
            receiver: address!("1111111111111111111111111111111111111111"),
            sellAmount: U256::from(50_000_000u64),
            buyAmount: U256::from(1_000_000_000_000_000_000u128),
            validTo: 2_000_000_000,
            appData: app_data_hash(),
            feeAmount: U256::ZERO,
            kind: "sell".to_string(),
            partiallyFillable: false,
            sellTokenBalance: "erc20".to_string(),
            buyTokenBalance: "erc20".to_string(),
        }
    }

    #[test]
    fn app_data_hash_is_stable() {
        // Pinned hash of APP_DATA_JSON. If the constant changes, update this
        // literal deliberately - do not re-derive it from the constant.
        assert_eq!(
            app_data_hash(),
            alloy_primitives::b256!(
                "c26dc5fb52bdd81b9218532e71a8919ada11e5da152fbd437aabe8236cbeec73"
            )
        );
    }

    #[test]
    fn digest_is_deterministic_and_chain_specific() {
        let order = sample_order();
        let d1 = order_digest(&order, 1);
        let d1_again = order_digest(&order, 1);
        let d_gnosis = order_digest(&order, 100);
        assert_eq!(d1, d1_again);
        assert_ne!(d1, d_gnosis, "domain separator must include chainId");
    }

    #[test]
    fn sign_produces_65_byte_hex() {
        // Deterministic anvil test key (publicly known; test-only).
        let signer: PrivateKeySigner =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                .parse()
                .unwrap();
        let sig = sign_order(&sample_order(), 1, &signer).unwrap();
        assert!(sig.starts_with("0x"));
        // 65 bytes => 130 hex chars + "0x".
        assert_eq!(sig.len(), 132);
        assert!(
            sig.ends_with("1b") || sig.ends_with("1c"),
            "v must be 27 or 28"
        );
    }

    #[test]
    fn order_type_hash_matches_gpv2() {
        // Canonical GPv2 order typehash from the CoW protocol spec:
        // keccak256("Order(address sellToken,address buyToken,address receiver,
        //   uint256 sellAmount,uint256 buyAmount,uint32 validTo,bytes32 appData,
        //   uint256 feeAmount,string kind,bool partiallyFillable,
        //   string sellTokenBalance,string buyTokenBalance)")
        let expected = alloy_primitives::b256!(
            "d5a25ba2e97094ad7d83dc28a6572da797d6b3e7fc6663bd93efb789fc17e489"
        );
        // eip712_type_hash is an instance method; use a sample order as receiver.
        assert_eq!(sample_order().eip712_type_hash(), expected);
    }
}
