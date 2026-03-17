use std::sync::LazyLock;

use aleph_types::account::Account;
use aleph_types::channel;
use aleph_types::channel::Channel;
use aleph_types::message::pending::PendingMessage;

use crate::aggregate_models::corechannel::NodeHash;
use crate::messages::{MessageBuildError, PostBuilder};

static FOUNDATION_CHANNEL: LazyLock<Channel> = LazyLock::new(|| channel!("FOUNDATION"));

fn build_operation<A: Account>(
    account: &A,
    content: serde_json::Value,
    reference: Option<NodeHash>,
) -> Result<PendingMessage, MessageBuildError> {
    let mut builder = PostBuilder::new(account, "corechan-operation", content)?
        .channel(FOUNDATION_CHANNEL.clone());
    if let Some(hash) = reference {
        builder = builder.reference(hash.to_string());
    }
    builder.build()
}

pub fn create_ccn<A: Account>(
    account: &A,
    name: &str,
    multiaddress: &str,
) -> Result<PendingMessage, MessageBuildError> {
    let content = serde_json::json!({
        "action": "create-node",
        "tags": ["create-node", "mainnet"],
        "details": {
            "name": name,
            "multiaddress": multiaddress,
        }
    });
    build_operation(account, content, None)
}

pub fn create_crn<A: Account>(
    account: &A,
    name: &str,
    address: &str,
) -> Result<PendingMessage, MessageBuildError> {
    let content = serde_json::json!({
        "action": "create-resource-node",
        "tags": ["create-resource-node", "mainnet"],
        "details": {
            "name": name,
            "address": address,
            "type": "compute",
        }
    });
    build_operation(account, content, None)
}

pub fn link_crn<A: Account>(
    account: &A,
    crn_hash: NodeHash,
) -> Result<PendingMessage, MessageBuildError> {
    let content = serde_json::json!({
        "action": "link",
        "tags": ["link", "mainnet"],
    });
    build_operation(account, content, Some(crn_hash))
}

pub fn unlink_crn<A: Account>(
    account: &A,
    crn_hash: NodeHash,
) -> Result<PendingMessage, MessageBuildError> {
    let content = serde_json::json!({
        "action": "unlink",
        "tags": ["unlink", "mainnet"],
    });
    build_operation(account, content, Some(crn_hash))
}

pub fn stake<A: Account>(
    account: &A,
    node_hash: NodeHash,
) -> Result<PendingMessage, MessageBuildError> {
    let content = serde_json::json!({
        "action": "stake-split",
        "tags": ["stake-split", "mainnet"],
    });
    build_operation(account, content, Some(node_hash))
}

pub fn unstake<A: Account>(
    account: &A,
    node_hash: NodeHash,
) -> Result<PendingMessage, MessageBuildError> {
    let content = serde_json::json!({
        "action": "unstake",
        "tags": ["unstake", "mainnet"],
    });
    build_operation(account, content, Some(node_hash))
}

pub fn drop_node<A: Account>(
    account: &A,
    node_hash: NodeHash,
) -> Result<PendingMessage, MessageBuildError> {
    let content = serde_json::json!({
        "action": "drop-node",
        "tags": ["drop-node", "mainnet"],
    });
    build_operation(account, content, Some(node_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::account::{Account, SignError};
    use aleph_types::chain::{Address, Chain, Signature};
    use aleph_types::message::MessageType;
    use std::str::FromStr;

    struct TestAccount {
        address: Address,
    }

    impl TestAccount {
        fn new() -> Self {
            Self {
                address: Address::from("0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".to_string()),
            }
        }
    }

    impl Account for TestAccount {
        fn chain(&self) -> Chain {
            Chain::Ethereum
        }
        fn address(&self) -> &Address {
            &self.address
        }
        fn sign_raw(&self, _buffer: &[u8]) -> Result<Signature, SignError> {
            Ok(Signature::from("0xDUMMY".to_string()))
        }
    }

    fn test_node_hash() -> NodeHash {
        NodeHash::from_str("a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77")
            .unwrap()
    }

    #[test]
    fn test_create_ccn() {
        let account = TestAccount::new();
        let msg = create_ccn(&account, "My CCN", "/ip4/1.2.3.4/tcp/4025/p2p/QmTest").unwrap();

        assert_eq!(msg.message_type, MessageType::Post);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["type"], "corechan-operation");
        assert_eq!(parsed["content"]["action"], "create-node");
        assert_eq!(
            parsed["content"]["tags"],
            serde_json::json!(["create-node", "mainnet"])
        );
        assert_eq!(parsed["content"]["details"]["name"], "My CCN");
        assert_eq!(
            parsed["content"]["details"]["multiaddress"],
            "/ip4/1.2.3.4/tcp/4025/p2p/QmTest"
        );
        assert!(parsed.get("ref").is_none());
    }

    #[test]
    fn test_create_crn() {
        let account = TestAccount::new();
        let msg = create_crn(&account, "My CRN", "https://crn.example.com/").unwrap();

        assert_eq!(msg.message_type, MessageType::Post);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["type"], "corechan-operation");
        assert_eq!(parsed["content"]["action"], "create-resource-node");
        assert_eq!(
            parsed["content"]["tags"],
            serde_json::json!(["create-resource-node", "mainnet"])
        );
        assert_eq!(parsed["content"]["details"]["name"], "My CRN");
        assert_eq!(
            parsed["content"]["details"]["address"],
            "https://crn.example.com/"
        );
        assert_eq!(parsed["content"]["details"]["type"], "compute");
        assert!(parsed.get("ref").is_none());
    }

    #[test]
    fn test_link_crn() {
        let account = TestAccount::new();
        let crn_hash = test_node_hash();
        let msg = link_crn(&account, crn_hash).unwrap();

        assert_eq!(msg.message_type, MessageType::Post);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["type"], "corechan-operation");
        assert_eq!(parsed["content"]["action"], "link");
        assert_eq!(
            parsed["content"]["tags"],
            serde_json::json!(["link", "mainnet"])
        );
        assert_eq!(
            parsed["ref"],
            "a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77"
        );
    }

    #[test]
    fn test_unlink_crn() {
        let account = TestAccount::new();
        let crn_hash = test_node_hash();
        let msg = unlink_crn(&account, crn_hash).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["content"]["action"], "unlink");
        assert_eq!(
            parsed["content"]["tags"],
            serde_json::json!(["unlink", "mainnet"])
        );
        assert_eq!(
            parsed["ref"],
            "a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77"
        );
    }

    #[test]
    fn test_stake() {
        let account = TestAccount::new();
        let node_hash = test_node_hash();
        let msg = stake(&account, node_hash).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["content"]["action"], "stake-split");
        assert_eq!(
            parsed["content"]["tags"],
            serde_json::json!(["stake-split", "mainnet"])
        );
        assert_eq!(
            parsed["ref"],
            "a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77"
        );
    }

    #[test]
    fn test_unstake() {
        let account = TestAccount::new();
        let node_hash = test_node_hash();
        let msg = unstake(&account, node_hash).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["content"]["action"], "unstake");
        assert_eq!(
            parsed["content"]["tags"],
            serde_json::json!(["unstake", "mainnet"])
        );
        assert_eq!(
            parsed["ref"],
            "a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77"
        );
    }

    #[test]
    fn test_drop_node() {
        let account = TestAccount::new();
        let node_hash = test_node_hash();
        let msg = drop_node(&account, node_hash).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["content"]["action"], "drop-node");
        assert_eq!(
            parsed["content"]["tags"],
            serde_json::json!(["drop-node", "mainnet"])
        );
        assert_eq!(
            parsed["ref"],
            "a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77"
        );
    }
}
