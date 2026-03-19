use serde::{Deserialize, Serialize};

use crate::chain::{Address, Chain};
use crate::message::MessageType;

/// A single authorization entry granting a delegate address
/// permission to act on behalf of the owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authorization {
    pub address: Address,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<Chain>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub channels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub types: Vec<MessageType>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub post_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aggregate_keys: Vec<String>,
}

/// Content of the "security" aggregate.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityAggregateContent {
    #[serde(default)]
    pub authorizations: Vec<Authorization>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_authorization_round_trip() {
        let auth = Authorization {
            address: Address::from("0xabc123".to_string()),
            chain: None,
            channels: vec![],
            types: vec![],
            post_types: vec![],
            aggregate_keys: vec![],
        };
        let json = serde_json::to_string(&auth).unwrap();
        // Empty/None fields must be omitted
        assert_eq!(json, r#"{"address":"0xabc123"}"#);
        let deserialized: Authorization = serde_json::from_str(&json).unwrap();
        assert_eq!(auth, deserialized);
    }

    #[test]
    fn test_full_authorization_round_trip() {
        let auth = Authorization {
            address: Address::from("0xdelegate".to_string()),
            chain: Some(Chain::Ethereum),
            channels: vec!["my-channel".to_string()],
            types: vec![MessageType::Post, MessageType::Aggregate],
            post_types: vec!["blog".to_string()],
            aggregate_keys: vec!["profile".to_string()],
        };
        let json = serde_json::to_string(&auth).unwrap();
        let deserialized: Authorization = serde_json::from_str(&json).unwrap();
        assert_eq!(auth, deserialized);
    }

    #[test]
    fn test_python_sdk_wire_format_compatibility() {
        // JSON as produced by the Python SDK
        let python_json = r#"{
            "address": "0xdelegate",
            "chain": "ETH",
            "channels": ["aleph-test"],
            "types": ["POST", "AGGREGATE"],
            "post_types": ["blog"],
            "aggregate_keys": ["profile"]
        }"#;
        let auth: Authorization = serde_json::from_str(python_json).unwrap();
        assert_eq!(auth.address, Address::from("0xdelegate".to_string()));
        assert_eq!(auth.chain, Some(Chain::Ethereum));
        assert_eq!(auth.types, vec![MessageType::Post, MessageType::Aggregate]);
    }

    #[test]
    fn test_security_aggregate_content_round_trip() {
        let content = SecurityAggregateContent {
            authorizations: vec![Authorization {
                address: Address::from("0xabc".to_string()),
                chain: None,
                channels: vec![],
                types: vec![MessageType::Post],
                post_types: vec![],
                aggregate_keys: vec![],
            }],
        };
        let json = serde_json::to_string(&content).unwrap();
        let deserialized: SecurityAggregateContent = serde_json::from_str(&json).unwrap();
        assert_eq!(content, deserialized);
    }

    #[test]
    fn test_empty_security_aggregate_deserialization() {
        let json = r#"{"authorizations":[]}"#;
        let content: SecurityAggregateContent = serde_json::from_str(json).unwrap();
        assert!(content.authorizations.is_empty());
    }
}
