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

impl Authorization {
    /// Returns a merged Authorization if `self` and `other` can be safely
    /// combined into one entry without changing the set of allowed messages.
    /// Returns None if they should remain separate.
    ///
    /// The merge rule: same `address` and `chain`, and of the four list
    /// fields (`channels`, `types`, `post_types`, `aggregate_keys`) at most
    /// one differs as a set, and both sides of that field are non-empty.
    /// Empty list (= wildcard) is never merged with a restricted list,
    /// because set-union would either narrow the wildcard or broaden the
    /// restricted side.
    pub fn try_merge(&self, other: &Self) -> Option<Self> {
        if self.address != other.address || self.chain != other.chain {
            return None;
        }

        let channels_eq = set_eq(&self.channels, &other.channels);
        let types_eq = set_eq(&self.types, &other.types);
        let post_types_eq = set_eq(&self.post_types, &other.post_types);
        let aggregate_keys_eq = set_eq(&self.aggregate_keys, &other.aggregate_keys);

        let differing = (!channels_eq) as u8
            + (!types_eq) as u8
            + (!post_types_eq) as u8
            + (!aggregate_keys_eq) as u8;

        match differing {
            0 => Some(self.clone()),
            1 => {
                let mut merged = self.clone();
                if !channels_eq {
                    if self.channels.is_empty() || other.channels.is_empty() {
                        return None;
                    }
                    merged.channels = sorted_union(&self.channels, &other.channels);
                } else if !types_eq {
                    if self.types.is_empty() || other.types.is_empty() {
                        return None;
                    }
                    merged.types = sorted_union(&self.types, &other.types);
                } else if !post_types_eq {
                    if self.post_types.is_empty() || other.post_types.is_empty() {
                        return None;
                    }
                    merged.post_types = sorted_union(&self.post_types, &other.post_types);
                } else if !aggregate_keys_eq {
                    if self.aggregate_keys.is_empty() || other.aggregate_keys.is_empty() {
                        return None;
                    }
                    merged.aggregate_keys =
                        sorted_union(&self.aggregate_keys, &other.aggregate_keys);
                }
                Some(merged)
            }
            _ => None,
        }
    }
}

fn set_eq<T: Ord + Clone>(a: &[T], b: &[T]) -> bool {
    let mut a_sorted: Vec<T> = a.to_vec();
    a_sorted.sort();
    a_sorted.dedup();
    let mut b_sorted: Vec<T> = b.to_vec();
    b_sorted.sort();
    b_sorted.dedup();
    a_sorted == b_sorted
}

fn sorted_union<T: Ord + Clone>(a: &[T], b: &[T]) -> Vec<T> {
    let mut out: Vec<T> = a.iter().cloned().chain(b.iter().cloned()).collect();
    out.sort();
    out.dedup();
    out
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

    fn auth(
        address: &str,
        chain: Option<Chain>,
        channels: Vec<&str>,
        types: Vec<MessageType>,
        post_types: Vec<&str>,
        aggregate_keys: Vec<&str>,
    ) -> Authorization {
        Authorization {
            address: Address::from(address.to_string()),
            chain,
            channels: channels.into_iter().map(String::from).collect(),
            types,
            post_types: post_types.into_iter().map(String::from).collect(),
            aggregate_keys: aggregate_keys.into_iter().map(String::from).collect(),
        }
    }

    #[test]
    fn try_merge_identical_returns_clone() {
        let a = auth(
            "0xD",
            None,
            vec![],
            vec![MessageType::Post],
            vec![],
            vec!["k1"],
        );
        let merged = a.try_merge(&a).unwrap();
        assert_eq!(merged, a);
    }

    #[test]
    fn try_merge_set_equal_different_order() {
        let a = auth("0xD", None, vec![], vec![], vec![], vec!["A", "B"]);
        let b = auth("0xD", None, vec![], vec![], vec![], vec!["B", "A"]);
        let merged = a.try_merge(&b).expect("set-equal entries merge");
        assert_eq!(
            merged
                .aggregate_keys
                .iter()
                .collect::<std::collections::HashSet<_>>(),
            ["A".to_string(), "B".to_string()].iter().collect()
        );
    }

    #[test]
    fn try_merge_set_equal_with_duplicates() {
        let a = auth("0xD", None, vec![], vec![], vec![], vec!["A", "A", "B"]);
        let b = auth("0xD", None, vec![], vec![], vec![], vec!["A", "B"]);
        assert!(a.try_merge(&b).is_some());
    }

    #[test]
    fn try_merge_aggregate_keys_one_differs_unions() {
        let a = auth("0xD", None, vec![], vec![], vec![], vec!["A", "B"]);
        let b = auth("0xD", None, vec![], vec![], vec![], vec!["C"]);
        let merged = a.try_merge(&b).expect("aggregate_keys merge");
        let mut got = merged.aggregate_keys.clone();
        got.sort();
        assert_eq!(got, vec!["A".to_string(), "B".to_string(), "C".to_string()]);
        assert!(merged.channels.is_empty());
        assert!(merged.types.is_empty());
        assert!(merged.post_types.is_empty());
    }

    #[test]
    fn try_merge_post_types_one_differs_unions() {
        let a = auth("0xD", None, vec![], vec![], vec!["blog"], vec![]);
        let b = auth("0xD", None, vec![], vec![], vec!["comment"], vec![]);
        let merged = a.try_merge(&b).expect("post_types merge");
        let mut got = merged.post_types.clone();
        got.sort();
        assert_eq!(got, vec!["blog".to_string(), "comment".to_string()]);
    }

    #[test]
    fn try_merge_channels_one_differs_unions() {
        let a = auth("0xD", None, vec!["c1"], vec![], vec![], vec![]);
        let b = auth("0xD", None, vec!["c2"], vec![], vec![], vec![]);
        let merged = a.try_merge(&b).expect("channels merge");
        let mut got = merged.channels.clone();
        got.sort();
        assert_eq!(got, vec!["c1".to_string(), "c2".to_string()]);
    }

    #[test]
    fn try_merge_types_one_differs_unions() {
        let a = auth("0xD", None, vec![], vec![MessageType::Post], vec![], vec![]);
        let b = auth(
            "0xD",
            None,
            vec![],
            vec![MessageType::Aggregate],
            vec![],
            vec![],
        );
        let merged = a.try_merge(&b).expect("types merge");
        assert!(merged.types.contains(&MessageType::Post));
        assert!(merged.types.contains(&MessageType::Aggregate));
        assert_eq!(merged.types.len(), 2);
    }

    #[test]
    fn try_merge_one_side_empty_returns_none() {
        let a = auth("0xD", None, vec![], vec![], vec![], vec!["A"]);
        let b = auth("0xD", None, vec![], vec![], vec![], vec![]);
        assert!(
            a.try_merge(&b).is_none(),
            "restricted vs wildcard must not merge"
        );
        assert!(
            b.try_merge(&a).is_none(),
            "wildcard vs restricted must not merge"
        );
    }

    #[test]
    fn try_merge_two_fields_differ_returns_none() {
        let a = auth(
            "0xD",
            None,
            vec!["c1"],
            vec![MessageType::Post],
            vec![],
            vec![],
        );
        let b = auth(
            "0xD",
            None,
            vec!["c2"],
            vec![MessageType::Aggregate],
            vec![],
            vec![],
        );
        assert!(a.try_merge(&b).is_none());
    }

    #[test]
    fn try_merge_different_address_returns_none() {
        let a = auth("0xD", None, vec![], vec![], vec![], vec!["A"]);
        let b = auth("0xE", None, vec![], vec![], vec![], vec!["A"]);
        assert!(a.try_merge(&b).is_none());
    }

    #[test]
    fn try_merge_different_chain_some_vs_none_returns_none() {
        let a = auth(
            "0xD",
            Some(Chain::Ethereum),
            vec![],
            vec![],
            vec![],
            vec!["A"],
        );
        let b = auth("0xD", None, vec![], vec![], vec![], vec!["A"]);
        assert!(a.try_merge(&b).is_none());
    }

    #[test]
    fn try_merge_different_chain_some_vs_some_returns_none() {
        let a = auth(
            "0xD",
            Some(Chain::Ethereum),
            vec![],
            vec![],
            vec![],
            vec!["A"],
        );
        let b = auth("0xD", Some(Chain::Sol), vec![], vec![], vec![], vec!["B"]);
        assert!(a.try_merge(&b).is_none());
    }
}
