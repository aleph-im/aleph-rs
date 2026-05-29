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
    /// Any keys the dashboard (or a future writer) attached that this model
    /// does not name explicitly, e.g. `confidential: true` on instance entries.
    /// Captured so round-trip writes don't silently drop them.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DomainEntry {
    #[serde(rename = "type")]
    pub kind: DomainTargetType,
    /// Legacy duplicate of `type`, mirrored for dashboard compatibility.
    /// Many real-world entries on the CCN omit it (e.g. older dashboard
    /// versions), so it's optional on the read path; writers in this crate
    /// always populate it.
    #[serde(
        rename = "programType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub program_type: Option<DomainTargetType>,
    /// `volume_id` of the website (or program/instance message hash).
    pub message_id: String,
    /// Update timestamp set by the writer. Optional because many entries
    /// stored on the CCN predate the dashboard adding it; downstream
    /// consumers (e.g. the DNS resolver) already treat missing as "not
    /// stale" rather than an error.
    #[serde(
        default,
        with = "super::serde_helpers::option_epoch_secs_lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<f64>,
    /// `null` and `{}` mean the same thing ("no options"); the custom
    /// deserializer collapses both into `DomainOptions::default()`.
    #[serde(
        default,
        deserialize_with = "super::serde_helpers::default_on_null",
        skip_serializing_if = "is_empty_options"
    )]
    pub options: DomainOptions,
    /// Top-level keys the dashboard attaches that this model does not name
    /// (e.g. `"spa": "1"`). Captured so round-trip writes preserve them.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

fn is_empty_options(o: &DomainOptions) -> bool {
    o.catch_all_path.is_none() && o.extra.is_empty()
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
        assert_eq!(entry.program_type, Some(DomainTargetType::Ipfs));
        assert_eq!(entry.message_id, "vol_abc");
        assert_eq!(entry.updated_at, Some(1714000000.0));
        assert_eq!(entry.options.catch_all_path.as_deref(), Some("/404.html"));
        assert!(agg.get("removed.example.com").unwrap().is_none());
    }

    #[test]
    fn parses_dashboard_iso_timestamps() {
        // The frontend dashboard writes `updated_at` as an RFC-3339 string
        // instead of the f64 epoch seconds the spec calls for; accept both.
        let raw = serde_json::json!({
            "site.example.com": {
                "type": "ipfs",
                "programType": "ipfs",
                "message_id": "vol_abc",
                "updated_at": "2026-01-18T22:30:40.691Z",
                "options": {}
            }
        });
        let agg: DomainsAggregate = serde_json::from_value(raw).unwrap();
        let entry = agg.get("site.example.com").unwrap().as_ref().unwrap();
        let ts = entry.updated_at.expect("updated_at parsed from ISO string");
        assert!((ts - 1768775440.691).abs() < 1e-3, "got {ts}");
    }

    #[test]
    fn accepts_missing_updated_at() {
        // Real CCN data: many entries have no `updated_at` at all.
        let raw = serde_json::json!({
            "cms.aleph.im": {
                "type": "ipfs",
                "options": null,
                "message_id": "abc",
                "programType": "ipfs"
            }
        });
        let agg: DomainsAggregate = serde_json::from_value(raw).unwrap();
        let entry = agg.get("cms.aleph.im").unwrap().as_ref().unwrap();
        assert_eq!(entry.updated_at, None);
    }

    #[test]
    fn accepts_missing_program_type() {
        // Real CCN data: some entries lack `programType`.
        let raw = serde_json::json!({
            "docs.aleph.im": {
                "type": "ipfs",
                "options": { "catch_all_path": "/404.html" },
                "message_id": "abc",
                "updated_at": "2024-09-10T13:49:00.824Z"
            }
        });
        let agg: DomainsAggregate = serde_json::from_value(raw).unwrap();
        let entry = agg.get("docs.aleph.im").unwrap().as_ref().unwrap();
        assert_eq!(entry.program_type, None);
        assert_eq!(entry.kind, DomainTargetType::Ipfs);
    }

    #[test]
    fn accepts_null_options() {
        // Real CCN data: `options: null` is equivalent to `{}`.
        let raw = serde_json::json!({
            "n8n.aleph.cloud": {
                "type": "instance",
                "options": null,
                "message_id": "abc",
                "programType": "instance"
            }
        });
        let agg: DomainsAggregate = serde_json::from_value(raw).unwrap();
        let entry = agg.get("n8n.aleph.cloud").unwrap().as_ref().unwrap();
        assert_eq!(entry.options, DomainOptions::default());
    }

    #[test]
    fn preserves_unknown_top_level_fields_on_roundtrip() {
        // `cms.twentysix.cloud` in the wild carries a top-level `"spa": "1"`.
        // We want to keep it on round-trip so write paths don't silently drop it.
        let raw = serde_json::json!({
            "type": "ipfs",
            "spa": "1",
            "message_id": "abc",
            "programType": "ipfs"
        });
        let entry: DomainEntry = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(
            entry.extra.get("spa"),
            Some(&serde_json::Value::String("1".into()))
        );
        let out = serde_json::to_value(&entry).unwrap();
        assert_eq!(out.get("spa"), Some(&serde_json::Value::String("1".into())));
    }

    #[test]
    fn preserves_unknown_options_fields_on_roundtrip() {
        // `cvmtest.aleph.im` has `options: { "confidential": true }`. The CLI
        // never reads it but must not drop it on re-write.
        let raw = serde_json::json!({
            "type": "instance",
            "options": { "confidential": true },
            "message_id": "abc",
            "programType": "instance"
        });
        let entry: DomainEntry = serde_json::from_value(raw).unwrap();
        assert_eq!(
            entry.options.extra.get("confidential"),
            Some(&serde_json::Value::Bool(true))
        );
        let out = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            out.get("options").and_then(|o| o.get("confidential")),
            Some(&serde_json::Value::Bool(true))
        );
    }

    #[test]
    fn omits_empty_options_on_serialize() {
        let entry = DomainEntry {
            kind: DomainTargetType::Program,
            program_type: Some(DomainTargetType::Program),
            message_id: "msg1".into(),
            updated_at: Some(0.0),
            options: DomainOptions::default(),
            extra: Default::default(),
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert!(json.get("options").is_none());
    }

    #[test]
    fn parses_mixed_real_world_aggregate_shape() {
        // Regression for the bug surfaced by `aleph website list --address
        // 0x28152dDF5cd213F341c8104d5361bBe41e95b301`: the production CCN
        // returns a single aggregate that mixes spec-shaped entries, entries
        // missing updated_at, entries missing programType, entries with
        // `options: null`, entries with unknown options fields, entries with
        // unknown top-level fields, and explicit null entries.
        let raw = serde_json::json!({
            "spec-shaped.example.com": {
                "type": "ipfs",
                "programType": "ipfs",
                "message_id": "a",
                "updated_at": 1714000000.0,
                "options": { "catch_all_path": "/404.html" }
            },
            "no-updated-at.example.com": {
                "type": "ipfs",
                "options": null,
                "message_id": "b",
                "programType": "ipfs"
            },
            "no-program-type.example.com": {
                "type": "ipfs",
                "options": { "catch_all_path": "/404.html" },
                "message_id": "c",
                "updated_at": "2024-09-10T13:49:00.824Z"
            },
            "extras-top.example.com": {
                "type": "ipfs",
                "spa": "1",
                "message_id": "d",
                "programType": "ipfs"
            },
            "extras-options.example.com": {
                "type": "instance",
                "options": { "confidential": true },
                "message_id": "e",
                "programType": "instance"
            },
            "null-entry.example.com": null
        });
        let agg: DomainsAggregate =
            serde_json::from_value(raw).expect("real-world aggregate must deserialize");
        assert_eq!(agg.len(), 6);
        assert!(agg.get("null-entry.example.com").unwrap().is_none());
        assert_eq!(
            agg.get("no-updated-at.example.com")
                .unwrap()
                .as_ref()
                .unwrap()
                .updated_at,
            None
        );
        assert_eq!(
            agg.get("no-program-type.example.com")
                .unwrap()
                .as_ref()
                .unwrap()
                .program_type,
            None
        );
    }

    #[test]
    fn omits_none_updated_at_and_program_type_on_serialize() {
        let entry = DomainEntry {
            kind: DomainTargetType::Ipfs,
            program_type: None,
            message_id: "msg1".into(),
            updated_at: None,
            options: DomainOptions::default(),
            extra: Default::default(),
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert!(json.get("updated_at").is_none());
        assert!(json.get("programType").is_none());
    }
}
