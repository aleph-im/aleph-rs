use aleph_sdk::builder::MessageBuilder;
use aleph_types::message::MessageType;

#[cfg(feature = "account-evm")]
use aleph_types::account::EvmAccount;
#[cfg(feature = "account-evm")]
use aleph_types::chain::Chain as EvmChain;

#[cfg(feature = "account-sol")]
use aleph_types::account::SolanaAccount;
#[cfg(feature = "account-sol")]
use aleph_types::chain::Chain as SolChain;

#[cfg(feature = "account-evm")]
const EVM_TEST_KEY: [u8; 32] = [
    0xac, 0x09, 0x74, 0xbe, 0xc3, 0x9a, 0x17, 0xe3, 0x6b, 0xa4, 0xa6, 0xb4, 0xd2, 0x38, 0xff, 0x94,
    0x4b, 0xac, 0xb4, 0x78, 0xcb, 0xed, 0x5e, 0xfb, 0xba, 0x0f, 0x2d, 0x1d, 0xb7, 0x44, 0xce, 0x06,
];

#[cfg(feature = "account-sol")]
const SOL_TEST_KEY: [u8; 32] = [
    0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c, 0xc4,
    0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
];

#[cfg(feature = "account-evm")]
#[test]
fn test_evm_end_to_end() {
    let account = EvmAccount::new(EvmChain::Ethereum, &EVM_TEST_KEY).unwrap();
    let content = serde_json::json!({"type": "test", "content": {"body": "Hello from Rust SDK"}});

    let pending = MessageBuilder::new(&account, MessageType::Post, content)
        .build()
        .unwrap();

    aleph_types::verify_signature::verify(
        &pending.chain,
        &pending.sender,
        &pending.signature,
        pending.message_type,
        &pending.item_hash,
    )
    .expect("EVM end-to-end: signature should verify");
}

#[cfg(feature = "account-sol")]
#[test]
fn test_solana_end_to_end() {
    let account = SolanaAccount::new(SolChain::Sol, &SOL_TEST_KEY).unwrap();
    let content = serde_json::json!({"type": "test", "content": {"body": "Hello from Rust SDK"}});

    let pending = MessageBuilder::new(&account, MessageType::Post, content)
        .build()
        .unwrap();

    aleph_types::verify_signature::verify(
        &pending.chain,
        &pending.sender,
        &pending.signature,
        pending.message_type,
        &pending.item_hash,
    )
    .expect("Solana end-to-end: signature should verify");
}

#[cfg(feature = "account-evm")]
#[test]
fn test_pending_message_serialization_inline() {
    let account = EvmAccount::new(EvmChain::Ethereum, &EVM_TEST_KEY).unwrap();
    let content = serde_json::json!({"type": "test", "content": {"body": "Hello"}});

    let pending = MessageBuilder::new(&account, MessageType::Post, content)
        .build()
        .unwrap();
    let json = serde_json::to_value(&pending).unwrap();

    assert!(json.get("sender").is_some());
    assert!(json.get("chain").is_some());
    assert!(json.get("signature").is_some());
    assert!(json.get("type").is_some());
    assert!(json.get("item_type").is_some());
    assert!(json.get("item_hash").is_some());
    assert!(json.get("time").is_some());
    assert_eq!(json["item_type"], "inline");
    assert!(json.get("item_content").is_some());
}

#[cfg(feature = "account-evm")]
#[test]
fn test_pending_message_storage_omits_content() {
    let account = EvmAccount::new(EvmChain::Ethereum, &EVM_TEST_KEY).unwrap();
    let content = serde_json::json!({"type": "test", "content": {"body": "Hello"}});

    let pending = MessageBuilder::new(&account, MessageType::Post, content)
        .allow_inlining(false)
        .build()
        .unwrap();
    let json = serde_json::to_value(&pending).unwrap();

    assert_eq!(json["item_type"], "storage");
    assert!(json.get("item_content").is_none());
    assert!(!pending.item_content.is_empty());
}
