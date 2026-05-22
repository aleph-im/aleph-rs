//! Models for the `port-forwarding` aggregate. The aggregate is per-sender and
//! maps a VM/program/IPFS-website item hash to the set of ports the sender
//! wants exposed by the CRN. The CRN reads this aggregate to decide what to
//! forward; the host-side mapped port is discovered separately via the CRN's
//! `/v2/about/executions/list` endpoint.

use aleph_types::item_hash::ItemHash;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub const PORT_FORWARDING_AGGREGATE_KEY: &str = "port-forwarding";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PortFlags {
    pub tcp: bool,
    pub udp: bool,
}

/// Per-VM ports configuration. The wire shape is `{"ports": {"80": {...}, ...}}`,
/// not the bare map, so this is a named-field struct rather than a
/// `#[serde(transparent)]` newtype.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Ports {
    pub ports: BTreeMap<u16, PortFlags>,
}

/// The full `port-forwarding` aggregate. `None` for a hash means the entry was
/// soft-deleted (set to null).
pub type PortForwardingAggregate = HashMap<ItemHash, Option<Ports>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_simple_entry() {
        let raw = serde_json::json!({
            "1111111111111111111111111111111111111111111111111111111111111111": {
                "ports": {
                    "80":   { "tcp": true,  "udp": false },
                    "443":  { "tcp": true,  "udp": false },
                    "5353": { "tcp": false, "udp": true  }
                }
            }
        });
        let agg: PortForwardingAggregate = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(agg.len(), 1);
        let (hash, entry) = agg.iter().next().unwrap();
        let ports = entry.as_ref().unwrap();
        assert_eq!(ports.ports.len(), 3);
        assert_eq!(
            ports.ports.get(&80).unwrap(),
            &PortFlags {
                tcp: true,
                udp: false
            }
        );
        let again = serde_json::to_value(&agg).unwrap();
        assert_eq!(again, raw);
        let _ = hash;
    }

    #[test]
    fn parses_null_entry_as_none() {
        let raw = serde_json::json!({
            "1111111111111111111111111111111111111111111111111111111111111111": null
        });
        let agg: PortForwardingAggregate = serde_json::from_value(raw).unwrap();
        let (_, entry) = agg.iter().next().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn empty_aggregate_parses() {
        let raw = serde_json::json!({});
        let agg: PortForwardingAggregate = serde_json::from_value(raw).unwrap();
        assert!(agg.is_empty());
    }
}
