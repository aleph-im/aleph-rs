//! Models for the `websites` aggregate, used by the dashboard's static-site
//! hosting feature. See docs/superpowers/specs/2026-04-27-frontend-pages-design.md.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const WEBSITE_CHANNEL: &str = "ALEPH-CLOUDSOLUTIONS";
pub const WEBSITES_AGGREGATE_KEY: &str = "websites";
pub const DEFAULT_IPFS_CATCH_ALL_PATH: &str = "/404.html";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebsiteMetadata {
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub framework: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct WebsitePayment {
    #[serde(default)]
    pub chain: String,
    #[serde(rename = "type", default)]
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebsiteEntry {
    pub metadata: WebsiteMetadata,
    #[serde(default)]
    pub payment: WebsitePayment,
    pub version: u64,
    pub volume_id: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub history: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ens: Vec<String>,
    #[serde(with = "super::serde_helpers::epoch_secs_lenient")]
    pub created_at: f64,
    #[serde(with = "super::serde_helpers::epoch_secs_lenient")]
    pub updated_at: f64,
}

/// The full `websites` aggregate. `None` = soft-deleted entry.
pub type WebsitesAggregate = BTreeMap<String, Option<WebsiteEntry>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dashboard_shape() {
        let raw = serde_json::json!({
            "my-site": {
                "metadata": { "name": "my-site", "tags": [], "framework": "nextjs" },
                "payment": { "chain": "ETH", "type": "hold" },
                "version": 2,
                "volume_id": "abc123",
                "history": { "1": "old123" },
                "ens": [],
                "created_at": 1714000000.0,
                "updated_at": 1714001000.0
            },
            "deleted-site": null
        });
        let agg: WebsitesAggregate = serde_json::from_value(raw).unwrap();
        let entry = agg.get("my-site").unwrap().as_ref().unwrap();
        assert_eq!(entry.version, 2);
        assert_eq!(entry.metadata.framework, "nextjs");
        assert_eq!(entry.history.get("1").unwrap(), "old123");
        assert!(agg.get("deleted-site").unwrap().is_none());
    }

    #[test]
    fn parses_dashboard_iso_timestamps() {
        // The frontend dashboard writes the epoch fields as RFC-3339 strings
        // instead of the f64 epoch seconds the spec calls for; accept both.
        let raw = serde_json::json!({
            "my-site": {
                "metadata": { "name": "my-site", "tags": [], "framework": "nextjs" },
                "payment": { "chain": "ETH", "type": "hold" },
                "version": 1,
                "volume_id": "abc123",
                "history": {},
                "ens": [],
                "created_at": "2026-01-18T22:00:00Z",
                "updated_at": "2026-01-18T22:30:40.691Z"
            }
        });
        let agg: WebsitesAggregate = serde_json::from_value(raw).unwrap();
        let entry = agg.get("my-site").unwrap().as_ref().unwrap();
        assert!(
            (entry.created_at - 1768773600.0).abs() < 1e-3,
            "got {}",
            entry.created_at
        );
        assert!(
            (entry.updated_at - 1768775440.691).abs() < 1e-3,
            "got {}",
            entry.updated_at
        );
    }

    #[test]
    fn serializes_omits_empty_optionals() {
        let entry = WebsiteEntry {
            metadata: WebsiteMetadata {
                name: "x".into(),
                tags: vec![],
                framework: "none".into(),
            },
            payment: WebsitePayment {
                chain: "ETH".into(),
                kind: "hold".into(),
            },
            version: 1,
            volume_id: "vol1".into(),
            history: BTreeMap::new(),
            ens: vec![],
            created_at: 0.0,
            updated_at: 0.0,
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert!(json.get("history").is_none());
        assert!(json.get("ens").is_none());
    }
}
