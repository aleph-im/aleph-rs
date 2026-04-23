//! Integration tests for `buy_credits` against a spawned anvil node.
//!
//! These tests require the `anvil` binary from Foundry to be in `PATH`. They
//! are `#[ignore]`d by default so `cargo test` works on machines without
//! foundry installed; CI runs them with `--include-ignored`.

use aleph_sdk::credit::buy_credits;
use alloy_network::EthereumWallet;
use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolEvent, sol};

// Minimal ERC20 compiled from the Solidity below (solc 0.8.28, optimized
// 200 runs). Kept as a constant so these tests don't require solc at build
// time.
//
// ```solidity
// // SPDX-License-Identifier: MIT
// pragma solidity ^0.8.20;
//
// contract MockERC20 {
//     mapping(address => uint256) public balanceOf;
//     event Transfer(address indexed from, address indexed to, uint256 value);
//
//     constructor(address initialHolder, uint256 initialSupply) {
//         balanceOf[initialHolder] = initialSupply;
//         emit Transfer(address(0), initialHolder, initialSupply);
//     }
//
//     function transfer(address to, uint256 amount) external returns (bool) {
//         require(balanceOf[msg.sender] >= amount, "insufficient balance");
//         balanceOf[msg.sender] -= amount;
//         balanceOf[to] += amount;
//         emit Transfer(msg.sender, to, amount);
//         return true;
//     }
// }
// ```
sol! {
    #[sol(rpc, bytecode = "0x6080604052348015600e575f5ffd5b5060405161030f38038061030f833981016040819052602b91607b565b6001600160a01b0382165f81815260208181526040808320859055518481527fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef910160405180910390a3505060b0565b5f5f60408385031215608b575f5ffd5b82516001600160a01b038116811460a0575f5ffd5b6020939093015192949293505050565b610252806100bd5f395ff3fe608060405234801561000f575f5ffd5b5060043610610034575f3560e01c806370a0823114610038578063a9059cbb1461006a575b5f5ffd5b61005761004636600461019a565b5f6020819052908152604090205481565b6040519081526020015b60405180910390f35b61007d6100783660046101ba565b61008d565b6040519015158152602001610061565b335f908152602081905260408120548211156100e65760405162461bcd60e51b8152602060048201526014602482015273696e73756666696369656e742062616c616e636560601b604482015260640160405180910390fd5b335f90815260208190526040812080548492906101049084906101f6565b90915550506001600160a01b0383165f9081526020819052604081208054849290610130908490610209565b90915550506040518281526001600160a01b0384169033907fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef9060200160405180910390a35060015b92915050565b80356001600160a01b0381168114610195575f5ffd5b919050565b5f602082840312156101aa575f5ffd5b6101b38261017f565b9392505050565b5f5f604083850312156101cb575f5ffd5b6101d48361017f565b946020939093013593505050565b634e487b7160e01b5f52601160045260245ffd5b81810381811115610179576101796101e2565b80820180821115610179576101796101e256fea26469706673582212202c150b91e00ba6f3f190f4ee019988636c28d2acb75a5b4618f71998591f422f64736f6c634300081c0033")]
    contract MockERC20 {
        mapping(address => uint256) public balanceOf;
        event Transfer(address indexed from, address indexed to, uint256 value);
        constructor(address initialHolder, uint256 initialSupply);
        function transfer(address to, uint256 amount) external returns (bool);
    }
}

/// Arbitrary non-zero address standing in for the credit contract.
const CREDIT_CONTRACT: Address = address!("6b55F32Ea969910838defd03746Ced5E2AE8cB8B");

/// Spawn anvil + deploy a MockERC20 with `initial_supply` minted to the
/// signing account. Returns the configured provider, the sender's address,
/// and the deployed token address.
///
/// Held together in one helper so each test has a clean chain and a fresh
/// token — anvil is cheap, ~200ms per spawn.
async fn setup_test_env(
    initial_supply: U256,
) -> (
    impl Provider + Clone,
    Address,
    Address,
    alloy_node_bindings::AnvilInstance,
) {
    let anvil = Anvil::new().try_spawn().expect("failed to spawn anvil");
    let signer: PrivateKeySigner = (&anvil.keys()[0]).into();
    let sender = signer.address();

    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(anvil.endpoint_url());

    let token = MockERC20::deploy(provider.clone(), sender, initial_supply)
        .await
        .expect("deploy MockERC20");
    let token_address = *token.address();

    (provider, sender, token_address, anvil)
}

#[tokio::test]
#[ignore = "requires anvil in PATH (install via foundry)"]
async fn buy_credits_happy_path_transfers_tokens_and_emits_log() {
    let initial_supply = U256::from(1_000u64) * U256::from(10u64).pow(U256::from(18));
    let amount = U256::from(100u64) * U256::from(10u64).pow(U256::from(18));

    let (provider, sender, token_address, _anvil) = setup_test_env(initial_supply).await;

    let receipt = buy_credits(&provider, token_address, CREDIT_CONTRACT, amount)
        .await
        .expect("buy_credits should succeed");

    assert!(receipt.status(), "receipt should report success");
    assert_eq!(receipt.from, sender, "tx.from should be our signer");
    assert_eq!(
        receipt.to,
        Some(token_address),
        "tx.to should be the ERC20 (we call transfer on it)"
    );

    // On-chain balance moved.
    let token = MockERC20::new(token_address, provider.clone());
    let credit_balance = token
        .balanceOf(CREDIT_CONTRACT)
        .call()
        .await
        .expect("balanceOf should succeed");
    assert_eq!(
        credit_balance, amount,
        "credit contract should have received tokens"
    );
    let sender_balance = token
        .balanceOf(sender)
        .call()
        .await
        .expect("balanceOf should succeed");
    assert_eq!(sender_balance, initial_supply - amount);

    // The receipt must carry a Transfer(sender -> credit_contract, amount) log.
    // Skip the deploy-time mint log (address(0) -> sender) by matching on topics.
    let transfer_log = receipt
        .logs()
        .iter()
        .filter_map(|l| MockERC20::Transfer::decode_log(&l.inner).ok())
        .find(|decoded| decoded.from == sender && decoded.to == CREDIT_CONTRACT)
        .expect("receipt should contain a Transfer(sender, credit_contract, _) log");
    assert_eq!(transfer_log.value, amount);
}

#[tokio::test]
#[ignore = "requires anvil in PATH (install via foundry)"]
async fn buy_credits_errors_when_sender_has_insufficient_balance() {
    // Mint only 10 tokens, try to transfer 100.
    let initial_supply = U256::from(10u64) * U256::from(10u64).pow(U256::from(18));
    let amount = U256::from(100u64) * U256::from(10u64).pow(U256::from(18));

    let (provider, _sender, token_address, _anvil) = setup_test_env(initial_supply).await;

    // alloy's default tx filler runs eth_estimateGas first, which catches the
    // revert before broadcast — we assert only on `is_err`, not the specific
    // error string (which is produced by alloy, not our code).
    let result = buy_credits(&provider, token_address, CREDIT_CONTRACT, amount).await;
    assert!(
        result.is_err(),
        "buy_credits must fail when the transfer would revert; got {:?}",
        result.as_ref().map(|r| r.transaction_hash)
    );
}
