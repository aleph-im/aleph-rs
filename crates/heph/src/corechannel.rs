use std::collections::HashMap;

use aleph_sdk::corechannel::{
    AmendDetails, CoreChannelAction, CreateNodeDetails, CreateResourceNodeDetails,
};
use serde::Serialize;

use crate::db::Db;
use crate::db::aggregates::{update_aggregate, upsert_aggregate};

/// The address that owns the corechannel aggregate.
const CORECHANNEL_ADDRESS: &str = "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10";
const CORECHANNEL_KEY: &str = "corechannel";

/// Parsed corechan-operation fields extracted from a POST's item_content.
pub struct ParsedCorechanOp {
    pub action: CoreChannelAction,
    pub ref_: Option<String>,
}

/// Try to parse a corechan-operation from a POST message's item_content JSON.
/// Returns None if this is not a corechan-operation or parsing fails.
pub fn parse_corechan_operation(item_content: &str) -> Option<ParsedCorechanOp> {
    let val: serde_json::Value = serde_json::from_str(item_content).ok()?;
    if val.get("type")?.as_str()? != "corechan-operation" {
        return None;
    }
    let content = val.get("content")?;
    let action: CoreChannelAction = serde_json::from_value(content.clone()).ok()?;
    let ref_ = val
        .get("ref")
        .and_then(|r| r.as_str())
        .map(|s| s.to_string());
    Some(ParsedCorechanOp { action, ref_ })
}

/// Persist the current CoreChannelState to the aggregates table.
pub fn persist_aggregate(db: &Db, state: &CoreChannelState, time: f64) {
    let json = state.to_aggregate_json();
    if let Err(e) = db.with_conn(|conn| {
        let inserted = upsert_aggregate(
            conn,
            CORECHANNEL_ADDRESS,
            CORECHANNEL_KEY,
            &json,
            time,
            None,
        )?;
        if !inserted {
            update_aggregate(
                conn,
                CORECHANNEL_ADDRESS,
                CORECHANNEL_KEY,
                &json,
                time,
                None,
                false,
            )?;
        }
        Ok::<(), rusqlite::Error>(())
    }) {
        tracing::warn!("failed to persist corechannel aggregate: {e}");
    }
}

const SYNTHETIC_STAKE: u64 = 500_000;
const MAX_LINKED_CRNS: usize = 8;

#[derive(Debug, Serialize)]
pub struct CcnEntry {
    pub hash: String,
    pub name: String,
    pub time: f64,
    pub owner: String,
    pub score: f64,
    pub reward: String,
    pub multiaddress: String,
    pub manager: String,
    pub resource_nodes: Vec<String>,
    pub total_staked: u64,
    pub stakers: HashMap<String, u64>,
    pub locked: bool,
    pub authorized: Vec<String>,
    pub picture: String,
    pub banner: String,
    pub description: String,
    pub stream_reward: String,
    pub registration_url: String,
    pub terms_and_conditions: String,
}

#[derive(Debug, Serialize)]
pub struct CrnEntry {
    pub hash: String,
    pub name: String,
    pub time: f64,
    pub owner: String,
    pub score: f64,
    pub reward: String,
    pub address: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub manager: String,
    pub locked: bool,
    pub authorized: Vec<String>,
    pub picture: String,
    pub banner: String,
    pub description: String,
    pub stream_reward: String,
    pub registration_url: String,
    pub terms_and_conditions: String,
}

pub struct CoreChannelState {
    pub nodes: HashMap<String, CcnEntry>,
    pub resource_nodes: HashMap<String, CrnEntry>,
    pub address_nodes: HashMap<String, String>,
}

impl Default for CoreChannelState {
    fn default() -> Self {
        Self::new()
    }
}

impl CoreChannelState {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            resource_nodes: HashMap::new(),
            address_nodes: HashMap::new(),
        }
    }

    /// Apply a corechan-operation. Returns true if state changed.
    pub fn apply_operation(
        &mut self,
        action: CoreChannelAction,
        sender: &str,
        ref_: Option<&str>,
        item_hash: &str,
        time: f64,
    ) -> bool {
        match action {
            CoreChannelAction::CreateNode { details } => {
                self.handle_create_node(sender, item_hash, time, details)
            }
            CoreChannelAction::CreateResourceNode { details } => {
                self.handle_create_resource_node(sender, item_hash, time, details)
            }
            CoreChannelAction::Link => self.handle_link(sender, ref_),
            CoreChannelAction::Unlink => self.handle_unlink(sender, ref_),
            CoreChannelAction::DropNode => self.handle_drop_node(sender, ref_),
            CoreChannelAction::Amend { details } => self.handle_amend(sender, ref_, details),
            CoreChannelAction::StakeSplit | CoreChannelAction::Unstake => false,
        }
    }

    fn handle_create_node(
        &mut self,
        sender: &str,
        item_hash: &str,
        time: f64,
        details: CreateNodeDetails,
    ) -> bool {
        if self.address_nodes.contains_key(sender) {
            return false;
        }
        let mut stakers = HashMap::new();
        stakers.insert(sender.to_string(), SYNTHETIC_STAKE);
        let entry = CcnEntry {
            hash: item_hash.to_string(),
            name: details.name,
            time,
            owner: sender.to_string(),
            score: 1.0,
            reward: sender.to_string(),
            multiaddress: details.multiaddress,
            manager: sender.to_string(),
            resource_nodes: Vec::new(),
            total_staked: SYNTHETIC_STAKE,
            stakers,
            locked: false,
            authorized: Vec::new(),
            picture: String::new(),
            banner: String::new(),
            description: String::new(),
            stream_reward: String::new(),
            registration_url: String::new(),
            terms_and_conditions: String::new(),
        };
        self.nodes.insert(item_hash.to_string(), entry);
        self.address_nodes
            .insert(sender.to_string(), item_hash.to_string());
        true
    }

    fn handle_create_resource_node(
        &mut self,
        sender: &str,
        item_hash: &str,
        time: f64,
        details: CreateResourceNodeDetails,
    ) -> bool {
        let entry = CrnEntry {
            hash: item_hash.to_string(),
            name: details.name,
            time,
            owner: sender.to_string(),
            score: 1.0,
            reward: sender.to_string(),
            address: details.address,
            status: "waiting".to_string(),
            parent: None,
            manager: sender.to_string(),
            locked: false,
            authorized: Vec::new(),
            picture: String::new(),
            banner: String::new(),
            description: String::new(),
            stream_reward: String::new(),
            registration_url: String::new(),
            terms_and_conditions: String::new(),
        };
        self.resource_nodes.insert(item_hash.to_string(), entry);
        true
    }

    fn handle_link(&mut self, sender: &str, ref_: Option<&str>) -> bool {
        let crn_hash = match ref_ {
            Some(h) => h,
            None => return false,
        };
        let ccn_hash = match self.address_nodes.get(sender) {
            Some(h) => h.clone(),
            None => return false,
        };
        // CRN must exist and have no parent
        match self.resource_nodes.get(crn_hash) {
            Some(c) if c.parent.is_none() => {}
            _ => return false,
        }
        // CCN must not be at max
        match self.nodes.get(&ccn_hash) {
            Some(c) if c.resource_nodes.len() < MAX_LINKED_CRNS => {}
            _ => return false,
        }
        // Apply
        self.resource_nodes.get_mut(crn_hash).unwrap().parent = Some(ccn_hash.clone());
        self.resource_nodes.get_mut(crn_hash).unwrap().status = "linked".to_string();
        self.nodes
            .get_mut(&ccn_hash)
            .unwrap()
            .resource_nodes
            .push(crn_hash.to_string());
        true
    }

    fn handle_unlink(&mut self, sender: &str, ref_: Option<&str>) -> bool {
        let crn_hash = match ref_ {
            Some(h) => h,
            None => return false,
        };
        let crn = match self.resource_nodes.get(crn_hash) {
            Some(c) => c,
            None => return false,
        };
        let parent_hash = match &crn.parent {
            Some(h) => h.clone(),
            None => return false,
        };
        // Sender must be the parent CCN's owner
        match self.nodes.get(&parent_hash) {
            Some(c) if c.owner == sender => {}
            _ => return false,
        }
        // Apply
        self.resource_nodes.get_mut(crn_hash).unwrap().parent = None;
        self.resource_nodes.get_mut(crn_hash).unwrap().status = "waiting".to_string();
        self.nodes
            .get_mut(&parent_hash)
            .unwrap()
            .resource_nodes
            .retain(|h| h != crn_hash);
        true
    }

    fn handle_drop_node(&mut self, sender: &str, ref_: Option<&str>) -> bool {
        let ref_hash = match ref_ {
            Some(h) => h,
            None => return false,
        };

        // Try CCN drop: sender must own a CCN and ref must be that CCN
        if let Some(sender_ccn) = self.address_nodes.get(sender)
            && sender_ccn == ref_hash
            && self.nodes.contains_key(ref_hash)
        {
            let children: Vec<String> = self.nodes[ref_hash].resource_nodes.clone();
            for crn_hash in &children {
                if let Some(crn) = self.resource_nodes.get_mut(crn_hash) {
                    crn.parent = None;
                    crn.status = "waiting".to_string();
                }
            }
            self.nodes.remove(ref_hash);
            self.address_nodes.remove(sender);
            return true;
        }

        // Try CRN drop: ref must be a resource node owned by sender
        if let Some(crn) = self.resource_nodes.get(ref_hash)
            && crn.owner == sender
        {
            if let Some(parent_hash) = crn.parent.clone()
                && let Some(ccn) = self.nodes.get_mut(&parent_hash)
            {
                ccn.resource_nodes.retain(|h| h != ref_hash);
            }
            self.resource_nodes.remove(ref_hash);
            return true;
        }

        false
    }

    fn handle_amend(&mut self, sender: &str, ref_: Option<&str>, details: AmendDetails) -> bool {
        let ref_hash = match ref_ {
            Some(h) => h,
            None => return false,
        };

        // Try CCN amend: sender must be owner (and own a CCN) or manager
        if let Some(node) = self.nodes.get(ref_hash) {
            let is_owner =
                node.owner == sender && self.address_nodes.get(sender) == Some(&ref_hash.to_string());
            let is_manager = node.manager == sender;
            if !is_owner && !is_manager {
                return false;
            }
            let node = self.nodes.get_mut(ref_hash).unwrap();
            if let Some(v) = details.name {
                node.name = v;
            }
            if let Some(v) = details.multiaddress {
                node.multiaddress = v;
            }
            if let Some(v) = details.picture {
                node.picture = v;
            }
            if let Some(v) = details.banner {
                node.banner = v;
            }
            if let Some(v) = details.description {
                node.description = v;
            }
            if let Some(v) = details.reward {
                node.reward = v;
            }
            if let Some(v) = details.stream_reward {
                node.stream_reward = v;
            }
            if let Some(v) = details.manager {
                node.manager = v;
            }
            if let Some(v) = details.authorized {
                node.authorized = v;
            }
            if let Some(v) = details.locked {
                node.locked = v;
            }
            if let Some(v) = details.registration_url {
                node.registration_url = v;
            }
            if let Some(v) = details.terms_and_conditions {
                node.terms_and_conditions = v;
            }
            return true;
        }

        // Try CRN amend: sender must be owner or manager
        if let Some(node) = self.resource_nodes.get(ref_hash) {
            if node.owner != sender && node.manager != sender {
                return false;
            }
            let node = self.resource_nodes.get_mut(ref_hash).unwrap();
            if let Some(v) = details.name {
                node.name = v;
            }
            if let Some(v) = details.address {
                node.address = v;
            }
            if let Some(v) = details.picture {
                node.picture = v;
            }
            if let Some(v) = details.banner {
                node.banner = v;
            }
            if let Some(v) = details.description {
                node.description = v;
            }
            if let Some(v) = details.reward {
                node.reward = v;
            }
            if let Some(v) = details.stream_reward {
                node.stream_reward = v;
            }
            if let Some(v) = details.manager {
                node.manager = v;
            }
            if let Some(v) = details.authorized {
                node.authorized = v;
            }
            if let Some(v) = details.locked {
                node.locked = v;
            }
            if let Some(v) = details.registration_url {
                node.registration_url = v;
            }
            if let Some(v) = details.terms_and_conditions {
                node.terms_and_conditions = v;
            }
            return true;
        }

        false
    }

    /// Serialize the current state to JSON for the aggregates table.
    pub fn to_aggregate_json(&self) -> String {
        let nodes: Vec<&CcnEntry> = self.nodes.values().collect();
        let resource_nodes: Vec<&CrnEntry> = self.resource_nodes.values().collect();
        serde_json::json!({
            "nodes": nodes,
            "resource_nodes": resource_nodes,
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_ccn() {
        let mut state = CoreChannelState::new();
        let changed = state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "My CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "abc123hash",
            1000.0,
        );
        assert!(changed);
        assert_eq!(state.nodes.len(), 1);
        let node = &state.nodes["abc123hash"];
        assert_eq!(node.name, "My CCN");
        assert_eq!(node.owner, "0xOwner");
        assert_eq!(node.multiaddress, "/ip4/1.2.3.4/tcp/4025");
        assert_eq!(node.score, 1.0);
        assert_eq!(node.reward, "0xOwner");
        assert_eq!(node.manager, "0xOwner");
        assert_eq!(node.total_staked, 500_000);
        assert_eq!(node.stakers.len(), 1);
        assert_eq!(node.stakers["0xOwner"], 500_000);
        assert_eq!(node.resource_nodes.len(), 0);
        assert_eq!(state.address_nodes["0xOwner"], "abc123hash");
    }

    #[test]
    fn test_create_ccn_1_to_1_constraint() {
        let mut state = CoreChannelState::new();
        let changed1 = state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "First".to_string(),
                    multiaddress: "/ip4/1.1.1.1/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "hash1",
            1000.0,
        );
        assert!(changed1);

        let changed2 = state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "Second".to_string(),
                    multiaddress: "/ip4/2.2.2.2/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "hash2",
            1001.0,
        );
        assert!(!changed2);
        assert_eq!(state.nodes.len(), 1);
    }

    #[test]
    fn test_create_crn() {
        let mut state = CoreChannelState::new();
        let changed = state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "My CRN".to_string(),
                    address: "https://crn.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner",
            None,
            "crn_hash",
            1000.0,
        );
        assert!(changed);
        assert_eq!(state.resource_nodes.len(), 1);
        let crn = &state.resource_nodes["crn_hash"];
        assert_eq!(crn.name, "My CRN");
        assert_eq!(crn.owner, "0xCrnOwner");
        assert_eq!(crn.address, "https://crn.example.com");
        assert_eq!(crn.status, "waiting");
        assert!(crn.parent.is_none());
        assert_eq!(crn.score, 1.0);
    }

    #[test]
    fn test_link_crn_to_ccn() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xCcnOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "CRN".to_string(),
                    address: "https://crn.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner",
            None,
            "crn_hash",
            1001.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::Link,
            "0xCcnOwner",
            Some("crn_hash"),
            "link_msg_hash",
            1002.0,
        );
        assert!(changed);
        let crn = &state.resource_nodes["crn_hash"];
        assert_eq!(crn.status, "linked");
        assert_eq!(crn.parent, Some("ccn_hash".to_string()));
        let ccn = &state.nodes["ccn_hash"];
        assert!(ccn.resource_nodes.contains(&"crn_hash".to_string()));
    }

    #[test]
    fn test_link_fails_non_ccn_owner() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xCcnOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "CRN".to_string(),
                    address: "https://crn.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner",
            None,
            "crn_hash",
            1001.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::Link,
            "0xNobody",
            Some("crn_hash"),
            "link_msg_hash",
            1002.0,
        );
        assert!(!changed);
        assert_eq!(state.resource_nodes["crn_hash"].status, "waiting");
    }

    #[test]
    fn test_link_max_8_crns() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xCcnOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        for i in 0..8 {
            let crn_hash = format!("crn_{i}");
            state.apply_operation(
                CoreChannelAction::CreateResourceNode {
                    details: CreateResourceNodeDetails {
                        name: format!("CRN {i}"),
                        address: format!("https://crn{i}.example.com"),
                        node_type: "compute".to_string(),
                    },
                },
                &format!("0xCrnOwner{i}"),
                None,
                &crn_hash,
                1001.0 + i as f64,
            );
            let changed = state.apply_operation(
                CoreChannelAction::Link,
                "0xCcnOwner",
                Some(&crn_hash),
                &format!("link_{i}"),
                1100.0 + i as f64,
            );
            assert!(changed, "link {i} should succeed");
        }
        assert_eq!(state.nodes["ccn_hash"].resource_nodes.len(), 8);

        // 9th link should be ignored
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "CRN 9".to_string(),
                    address: "https://crn9.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner9",
            None,
            "crn_9",
            1200.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::Link,
            "0xCcnOwner",
            Some("crn_9"),
            "link_9",
            1201.0,
        );
        assert!(!changed);
    }

    #[test]
    fn test_drop_ccn() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "CRN".to_string(),
                    address: "https://crn.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner",
            None,
            "crn_hash",
            1001.0,
        );
        state.apply_operation(
            CoreChannelAction::Link,
            "0xOwner",
            Some("crn_hash"),
            "link_hash",
            1002.0,
        );
        assert_eq!(state.resource_nodes["crn_hash"].status, "linked");

        let changed = state.apply_operation(
            CoreChannelAction::DropNode,
            "0xOwner",
            Some("ccn_hash"),
            "drop_hash",
            1003.0,
        );
        assert!(changed);
        assert!(state.nodes.is_empty());
        assert!(!state.address_nodes.contains_key("0xOwner"));
        let crn = &state.resource_nodes["crn_hash"];
        assert_eq!(crn.status, "waiting");
        assert!(crn.parent.is_none());
    }

    #[test]
    fn test_drop_ccn_wrong_owner() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::DropNode,
            "0xOther",
            Some("ccn_hash"),
            "drop_hash",
            1001.0,
        );
        assert!(!changed);
        assert_eq!(state.nodes.len(), 1);
    }

    #[test]
    fn test_drop_crn() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xCcnOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "CRN".to_string(),
                    address: "https://crn.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner",
            None,
            "crn_hash",
            1001.0,
        );
        state.apply_operation(
            CoreChannelAction::Link,
            "0xCcnOwner",
            Some("crn_hash"),
            "link_hash",
            1002.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::DropNode,
            "0xCrnOwner",
            Some("crn_hash"),
            "drop_hash",
            1003.0,
        );
        assert!(changed);
        assert!(state.resource_nodes.is_empty());
        assert!(state.nodes["ccn_hash"].resource_nodes.is_empty());
    }

    #[test]
    fn test_drop_crn_wrong_owner() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "CRN".to_string(),
                    address: "https://crn.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner",
            None,
            "crn_hash",
            1000.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::DropNode,
            "0xOther",
            Some("crn_hash"),
            "drop_hash",
            1001.0,
        );
        assert!(!changed);
        assert_eq!(state.resource_nodes.len(), 1);
    }

    #[test]
    fn test_stake_is_noop() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::StakeSplit,
            "0xStaker",
            Some("ccn_hash"),
            "stake_hash",
            1001.0,
        );
        assert!(!changed);
        assert_eq!(state.nodes["ccn_hash"].stakers.len(), 1);
    }

    #[test]
    fn test_to_aggregate_json() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "CRN".to_string(),
                    address: "https://crn.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner",
            None,
            "crn_hash",
            1001.0,
        );
        let json = state.to_aggregate_json();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed["nodes"].is_array());
        assert_eq!(parsed["nodes"].as_array().unwrap().len(), 1);
        assert!(parsed["resource_nodes"].is_array());
        assert_eq!(parsed["resource_nodes"].as_array().unwrap().len(), 1);
        let crn = &parsed["resource_nodes"][0];
        assert_eq!(crn["status"], "waiting");
        assert!(crn["parent"].is_null() || crn.get("parent").is_none());
    }

    #[test]
    fn test_full_flow_with_db_persistence() {
        use crate::db::Db;
        use crate::db::aggregates::get_aggregate;

        let db = Db::open_in_memory().unwrap();
        let mut state = CoreChannelState::new();

        // Create CCN
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "Test CCN".to_string(),
                    multiaddress: "/ip4/10.0.0.1/tcp/4025".to_string(),
                },
            },
            "0xCcnOwner",
            None,
            "ccn_hash_001",
            1000.0,
        );
        persist_aggregate(&db, &state, 1000.0);

        // Verify it's in the DB
        let agg = db
            .with_conn(|conn| get_aggregate(conn, CORECHANNEL_ADDRESS, CORECHANNEL_KEY))
            .unwrap()
            .expect("aggregate should exist");
        let parsed: serde_json::Value = serde_json::from_str(&agg.content).unwrap();
        assert_eq!(parsed["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(parsed["nodes"][0]["name"], "Test CCN");
        assert_eq!(parsed["nodes"][0]["total_staked"], 500_000);
        assert_eq!(parsed["resource_nodes"].as_array().unwrap().len(), 0);

        // Create and link a CRN
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "Test CRN".to_string(),
                    address: "https://crn.test.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner",
            None,
            "crn_hash_001",
            1001.0,
        );
        state.apply_operation(
            CoreChannelAction::Link,
            "0xCcnOwner",
            Some("crn_hash_001"),
            "link_hash",
            1002.0,
        );
        persist_aggregate(&db, &state, 1002.0);

        // Verify updated state
        let agg = db
            .with_conn(|conn| get_aggregate(conn, CORECHANNEL_ADDRESS, CORECHANNEL_KEY))
            .unwrap()
            .expect("aggregate should exist");
        let parsed: serde_json::Value = serde_json::from_str(&agg.content).unwrap();
        assert_eq!(parsed["resource_nodes"].as_array().unwrap().len(), 1);
        assert_eq!(parsed["resource_nodes"][0]["status"], "linked");
        assert_eq!(parsed["resource_nodes"][0]["parent"], "ccn_hash_001");

        // Drop CCN — CRN should be unlinked
        state.apply_operation(
            CoreChannelAction::DropNode,
            "0xCcnOwner",
            Some("ccn_hash_001"),
            "drop_hash",
            1003.0,
        );
        persist_aggregate(&db, &state, 1003.0);

        let agg = db
            .with_conn(|conn| get_aggregate(conn, CORECHANNEL_ADDRESS, CORECHANNEL_KEY))
            .unwrap()
            .expect("aggregate should exist");
        let parsed: serde_json::Value = serde_json::from_str(&agg.content).unwrap();
        assert_eq!(parsed["nodes"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["resource_nodes"].as_array().unwrap().len(), 1);
        assert_eq!(parsed["resource_nodes"][0]["status"], "waiting");
    }

    #[test]
    fn test_amend_ccn() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::Amend {
                details: AmendDetails {
                    name: Some("New Name".to_string()),
                    reward: Some("0xReward".to_string()),
                    ..Default::default()
                },
            },
            "0xOwner",
            Some("ccn_hash"),
            "amend_hash",
            1001.0,
        );
        assert!(changed);
        let node = &state.nodes["ccn_hash"];
        assert_eq!(node.name, "New Name");
        assert_eq!(node.reward, "0xReward");
        // Unchanged fields stay the same
        assert_eq!(node.multiaddress, "/ip4/1.2.3.4/tcp/4025");
        assert_eq!(node.manager, "0xOwner");
    }

    #[test]
    fn test_amend_ccn_by_manager() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        // Set a manager
        state.nodes.get_mut("ccn_hash").unwrap().manager = "0xManager".to_string();

        let changed = state.apply_operation(
            CoreChannelAction::Amend {
                details: AmendDetails {
                    name: Some("Manager Updated".to_string()),
                    ..Default::default()
                },
            },
            "0xManager",
            Some("ccn_hash"),
            "amend_hash",
            1001.0,
        );
        assert!(changed);
        assert_eq!(state.nodes["ccn_hash"].name, "Manager Updated");
    }

    #[test]
    fn test_amend_ccn_unauthorized() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::Amend {
                details: AmendDetails {
                    name: Some("Hacked".to_string()),
                    ..Default::default()
                },
            },
            "0xNobody",
            Some("ccn_hash"),
            "amend_hash",
            1001.0,
        );
        assert!(!changed);
        assert_eq!(state.nodes["ccn_hash"].name, "CCN");
    }

    #[test]
    fn test_amend_crn() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "CRN".to_string(),
                    address: "https://crn.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xOwner",
            None,
            "crn_hash",
            1000.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::Amend {
                details: AmendDetails {
                    address: Some("https://new.example.com".to_string()),
                    description: Some("Updated CRN".to_string()),
                    ..Default::default()
                },
            },
            "0xOwner",
            Some("crn_hash"),
            "amend_hash",
            1001.0,
        );
        assert!(changed);
        let crn = &state.resource_nodes["crn_hash"];
        assert_eq!(crn.address, "https://new.example.com");
        assert_eq!(crn.description, "Updated CRN");
        assert_eq!(crn.name, "CRN"); // unchanged
    }

    #[test]
    fn test_unlink_crn() {
        let mut state = CoreChannelState::new();
        state.apply_operation(
            CoreChannelAction::CreateNode {
                details: CreateNodeDetails {
                    name: "CCN".to_string(),
                    multiaddress: "/ip4/1.2.3.4/tcp/4025".to_string(),
                },
            },
            "0xCcnOwner",
            None,
            "ccn_hash",
            1000.0,
        );
        state.apply_operation(
            CoreChannelAction::CreateResourceNode {
                details: CreateResourceNodeDetails {
                    name: "CRN".to_string(),
                    address: "https://crn.example.com".to_string(),
                    node_type: "compute".to_string(),
                },
            },
            "0xCrnOwner",
            None,
            "crn_hash",
            1001.0,
        );
        state.apply_operation(
            CoreChannelAction::Link,
            "0xCcnOwner",
            Some("crn_hash"),
            "link_hash",
            1002.0,
        );
        let changed = state.apply_operation(
            CoreChannelAction::Unlink,
            "0xCcnOwner",
            Some("crn_hash"),
            "unlink_hash",
            1003.0,
        );
        assert!(changed);
        let crn = &state.resource_nodes["crn_hash"];
        assert_eq!(crn.status, "waiting");
        assert!(crn.parent.is_none());
        assert!(state.nodes["ccn_hash"].resource_nodes.is_empty());
    }
}
