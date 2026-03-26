use std::sync::LazyLock;

use aleph_types::account::Account;
use aleph_types::channel;
use aleph_types::channel::Channel;
use aleph_types::message::pending::PendingMessage;
use serde::{Deserialize, Serialize};

use crate::aggregate_models::corechannel::NodeHash;
use crate::messages::{MessageBuildError, PostBuilder};

static FOUNDATION_CHANNEL: LazyLock<Channel> = LazyLock::new(|| channel!("FOUNDATION"));

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateNodeDetails {
    pub name: String,
    pub multiaddress: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateResourceNodeDetails {
    pub name: String,
    pub address: String,
    #[serde(rename = "type")]
    pub node_type: String,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AmendDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multiaddress: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub picture: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reward: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_reward: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manager: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorized: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locked: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registration_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terms_and_conditions: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
#[allow(clippy::large_enum_variant)]
pub enum CoreChannelAction {
    CreateNode { details: CreateNodeDetails },
    CreateResourceNode { details: CreateResourceNodeDetails },
    Link,
    Unlink,
    StakeSplit,
    DropNode,
    Unstake,
    Amend { details: AmendDetails },
}

#[derive(Debug, Serialize)]
struct CoreChannelContent {
    #[serde(flatten)]
    action: CoreChannelAction,
    tags: Vec<String>,
}

impl CoreChannelContent {
    fn new(action: CoreChannelAction, network: &str) -> Self {
        let action_tag = match &action {
            CoreChannelAction::CreateNode { .. } => "create-node",
            CoreChannelAction::CreateResourceNode { .. } => "create-resource-node",
            CoreChannelAction::Link => "link",
            CoreChannelAction::Unlink => "unlink",
            CoreChannelAction::StakeSplit => "stake-split",
            CoreChannelAction::DropNode => "drop-node",
            CoreChannelAction::Unstake => "unstake",
            CoreChannelAction::Amend { .. } => "amend",
        };
        Self {
            action,
            tags: vec![action_tag.to_string(), network.to_string()],
        }
    }
}

fn build_operation<A: Account>(
    account: &A,
    content: CoreChannelContent,
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
    network: &str,
) -> Result<PendingMessage, MessageBuildError> {
    let action = CoreChannelAction::CreateNode {
        details: CreateNodeDetails {
            name: name.to_string(),
            multiaddress: multiaddress.to_string(),
        },
    };
    build_operation(account, CoreChannelContent::new(action, network), None)
}

pub fn create_crn<A: Account>(
    account: &A,
    name: &str,
    address: &str,
    network: &str,
) -> Result<PendingMessage, MessageBuildError> {
    let action = CoreChannelAction::CreateResourceNode {
        details: CreateResourceNodeDetails {
            name: name.to_string(),
            address: address.to_string(),
            node_type: "compute".to_string(),
        },
    };
    build_operation(account, CoreChannelContent::new(action, network), None)
}

pub fn link_crn<A: Account>(
    account: &A,
    crn_hash: NodeHash,
    network: &str,
) -> Result<PendingMessage, MessageBuildError> {
    build_operation(
        account,
        CoreChannelContent::new(CoreChannelAction::Link, network),
        Some(crn_hash),
    )
}

pub fn unlink_crn<A: Account>(
    account: &A,
    crn_hash: NodeHash,
    network: &str,
) -> Result<PendingMessage, MessageBuildError> {
    build_operation(
        account,
        CoreChannelContent::new(CoreChannelAction::Unlink, network),
        Some(crn_hash),
    )
}

pub fn stake<A: Account>(
    account: &A,
    node_hash: NodeHash,
    network: &str,
) -> Result<PendingMessage, MessageBuildError> {
    build_operation(
        account,
        CoreChannelContent::new(CoreChannelAction::StakeSplit, network),
        Some(node_hash),
    )
}

pub fn unstake<A: Account>(
    account: &A,
    node_hash: NodeHash,
    network: &str,
) -> Result<PendingMessage, MessageBuildError> {
    build_operation(
        account,
        CoreChannelContent::new(CoreChannelAction::Unstake, network),
        Some(node_hash),
    )
}

pub fn drop_node<A: Account>(
    account: &A,
    node_hash: NodeHash,
    network: &str,
) -> Result<PendingMessage, MessageBuildError> {
    build_operation(
        account,
        CoreChannelContent::new(CoreChannelAction::DropNode, network),
        Some(node_hash),
    )
}

pub fn amend_node<A: Account>(
    account: &A,
    node_hash: NodeHash,
    details: AmendDetails,
    network: &str,
) -> Result<PendingMessage, MessageBuildError> {
    let action = CoreChannelAction::Amend { details };
    build_operation(account, CoreChannelContent::new(action, network), Some(node_hash))
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
        let msg =
            create_ccn(&account, "My CCN", "/ip4/1.2.3.4/tcp/4025/p2p/QmTest", "mainnet").unwrap();

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
        let msg = create_crn(&account, "My CRN", "https://crn.example.com/", "mainnet").unwrap();

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
        let msg = link_crn(&account, crn_hash, "mainnet").unwrap();

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
        let msg = unlink_crn(&account, crn_hash, "mainnet").unwrap();

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
        let msg = stake(&account, node_hash, "mainnet").unwrap();

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
        let msg = unstake(&account, node_hash, "mainnet").unwrap();

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
        let msg = drop_node(&account, node_hash, "mainnet").unwrap();

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

    #[test]
    fn test_amend_node() {
        let account = TestAccount::new();
        let node_hash = test_node_hash();
        let details = AmendDetails {
            name: Some("Updated Name".to_string()),
            reward: Some("0xNewRewardAddress".to_string()),
            ..Default::default()
        };
        let msg = amend_node(&account, node_hash, details, "mainnet").unwrap();

        assert_eq!(msg.message_type, MessageType::Post);

        let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
        assert_eq!(parsed["type"], "corechan-operation");
        assert_eq!(parsed["content"]["action"], "amend");
        assert_eq!(
            parsed["content"]["tags"],
            serde_json::json!(["amend", "mainnet"])
        );
        assert_eq!(parsed["content"]["details"]["name"], "Updated Name");
        assert_eq!(parsed["content"]["details"]["reward"], "0xNewRewardAddress");
        // Omitted fields must not appear (no nulls)
        assert!(parsed["content"]["details"].get("multiaddress").is_none());
        assert!(parsed["content"]["details"].get("locked").is_none());
        assert_eq!(
            parsed["ref"],
            "a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77"
        );
    }

    #[test]
    fn test_deserialize_create_node_action() {
        let json = r#"{"action":"create-node","details":{"name":"My CCN","multiaddress":"/ip4/1.2.3.4/tcp/4025"}}"#;
        let action: CoreChannelAction = serde_json::from_str(json).unwrap();
        match action {
            CoreChannelAction::CreateNode { details } => {
                assert_eq!(details.name, "My CCN");
                assert_eq!(details.multiaddress, "/ip4/1.2.3.4/tcp/4025");
            }
            _ => panic!("expected CreateNode"),
        }
    }

    #[test]
    fn test_deserialize_link_action() {
        let json = r#"{"action":"link"}"#;
        let action: CoreChannelAction = serde_json::from_str(json).unwrap();
        assert!(matches!(action, CoreChannelAction::Link));
    }
}
