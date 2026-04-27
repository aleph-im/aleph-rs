//! Models for the `domains` aggregate. Domains attach human-readable names
//! to websites (`type=ipfs`), programs, or instances.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const DOMAINS_AGGREGATE_KEY: &str = "domains";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DomainTargetType {
    Ipfs,
    Program,
    Instance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct DomainOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catch_all_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DomainEntry {
    #[serde(rename = "type")]
    pub kind: DomainTargetType,
    /// Legacy duplicate of `type`, mirrored for dashboard compatibility.
    #[serde(rename = "programType")]
    pub program_type: DomainTargetType,
    /// `volume_id` of the website (or program/instance message hash).
    pub message_id: String,
    pub updated_at: f64,
    #[serde(default, skip_serializing_if = "is_empty_options")]
    pub options: DomainOptions,
}

fn is_empty_options(o: &DomainOptions) -> bool {
    o.catch_all_path.is_none()
}

pub type DomainsAggregate = BTreeMap<String, Option<DomainEntry>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dashboard_shape() {
        let raw = serde_json::json!({
            "site.example.com": {
                "type": "ipfs",
                "programType": "ipfs",
                "message_id": "vol_abc",
                "updated_at": 1714000000.0,
                "options": { "catch_all_path": "/404.html" }
            },
            "removed.example.com": null
        });
        let agg: DomainsAggregate = serde_json::from_value(raw).unwrap();
        let entry = agg.get("site.example.com").unwrap().as_ref().unwrap();
        assert_eq!(entry.kind, DomainTargetType::Ipfs);
        assert_eq!(entry.program_type, DomainTargetType::Ipfs);
        assert_eq!(entry.message_id, "vol_abc");
        assert_eq!(entry.options.catch_all_path.as_deref(), Some("/404.html"));
        assert!(agg.get("removed.example.com").unwrap().is_none());
    }

    #[test]
    fn omits_empty_options_on_serialize() {
        let entry = DomainEntry {
            kind: DomainTargetType::Program,
            program_type: DomainTargetType::Program,
            message_id: "msg1".into(),
            updated_at: 0.0,
            options: DomainOptions::default(),
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert!(json.get("options").is_none());
    }
}
