//! Client and types for the CRN list aggregator
//! (`https://crns-list.aleph.sh/crns.json`).
//!
//! This is not an aleph aggregate — it is an HTTP endpoint scraping every
//! active CRN's status and bundling scoring results.

use serde::Deserialize;
use std::collections::HashMap;

/// Top-level response from `crns.json`.
#[derive(Debug, Clone, Deserialize)]
pub struct CrnListResponse {
    pub crns: Vec<CrnListEntry>,
}

/// A single CRN entry. Optional fields match the aggregator's permissive schema;
/// unknown fields go into `extra` for forward-compatibility.
#[derive(Debug, Clone, Deserialize)]
pub struct CrnListEntry {
    pub hash: String,
    pub name: String,
    pub address: String,
    #[serde(default)]
    pub score: Option<f64>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub payment_receiver_address: Option<String>,
    #[serde(default)]
    pub gpu_support: bool,
    #[serde(default)]
    pub confidential_support: bool,
    #[serde(default)]
    pub qemu_support: bool,
    #[serde(default)]
    pub ipv6_check: Option<HashMap<String, bool>>,
    #[serde(default)]
    pub system_usage: Option<SystemUsage>,
    #[serde(default)]
    pub compatible_available_gpus: Option<Vec<Gpu>>,
    #[serde(default)]
    pub terms_and_conditions: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SystemUsage {
    pub cpu: CpuInfo,
    pub mem: MemoryInfo,
    pub disk: DiskInfo,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CpuInfo {
    pub count: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryInfo {
    #[serde(rename = "available_kB")]
    pub available_kb: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiskInfo {
    #[serde(rename = "available_kB")]
    pub available_kb: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Gpu {
    pub vendor: String,
    pub model: String,
    pub device_name: String,
    pub device_class: String,
    pub pci_host: String,
    pub device_id: String,
    #[serde(default)]
    pub compatible: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/crns_list.json");

    #[test]
    fn parses_fixture() {
        let parsed: CrnListResponse = serde_json::from_str(FIXTURE).unwrap();
        assert_eq!(parsed.crns.len(), 5);

        let alpha = &parsed.crns[0];
        assert_eq!(alpha.name, "alpha");
        assert_eq!(alpha.score, Some(0.95));
        assert!(alpha.qemu_support);
        assert!(!alpha.gpu_support);
        let usage = alpha.system_usage.as_ref().unwrap();
        assert_eq!(usage.cpu.count, 16);
        assert_eq!(usage.mem.available_kb, 33_554_432);
        assert_eq!(usage.disk.available_kb, 524_288_000);

        let delta = &parsed.crns[3];
        assert!(delta.gpu_support);
        let gpus = delta.compatible_available_gpus.as_ref().unwrap();
        assert_eq!(gpus.len(), 1);
        assert_eq!(gpus[0].model, "H100");

        let epsilon = &parsed.crns[4];
        assert!(epsilon.system_usage.is_none());
    }
}
