//! Client and types for the CRN list aggregator
//! (`https://crns-list.aleph.sh/crns.json`).
//!
//! This is not an aleph aggregate — it is an HTTP endpoint scraping every
//! active CRN's status and bundling scoring results.

use serde::{Deserialize, Deserializer};
use std::collections::HashMap;

/// Deserialize a bool field that may be `null` in JSON (treat `null` as `false`).
fn deserialize_bool_or_null<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    Ok(Option::<bool>::deserialize(d)?.unwrap_or(false))
}

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
    #[serde(default, deserialize_with = "deserialize_bool_or_null")]
    pub gpu_support: bool,
    #[serde(default, deserialize_with = "deserialize_bool_or_null")]
    pub confidential_support: bool,
    #[serde(default, deserialize_with = "deserialize_bool_or_null")]
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

/// The aggregator's `cpu` payload also includes `load_average`, `core_frequencies`,
/// etc. — we deliberately model only the fields consumers of this SDK read today.
/// Unknown fields are silently ignored by serde (no data loss; they stay in the JSON
/// and can be materialized later by adding them here).
#[derive(Debug, Clone, Deserialize)]
pub struct CpuInfo {
    pub count: u32,
}

/// The aggregator reports `total_kB` and `available_kB`. Only `available_kB` is
/// read by downstream consumers; the total is ignored to keep the type minimal.
#[derive(Debug, Clone, Deserialize)]
pub struct MemoryInfo {
    #[serde(rename = "available_kB")]
    pub available_kb: u64,
}

/// See `MemoryInfo` — same minimalism convention applies to disk metrics.
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
    #[serde(default, deserialize_with = "deserialize_bool_or_null")]
    pub compatible: bool,
}

/// Filter options for the CRN list. All fields default to "no constraint".
#[derive(Debug, Clone, Default)]
pub struct CrnFilter {
    /// If true, require every value in `ipv6_check` to be `true`.
    pub ipv6: bool,
    /// Require the CRN to report at least this many vCPUs.
    pub min_vcpus: Option<u32>,
    /// Require at least this much available memory (MiB).
    pub min_memory_mib: Option<u64>,
    /// Require at least this much available disk (MiB).
    pub min_disk_mib: Option<u64>,
    /// Require `confidential_support == true`.
    pub confidential: bool,
    /// Require `gpu_support == true` with at least one compatible available GPU.
    pub gpu: bool,
}

impl CrnListResponse {
    /// Return references to entries matching the filter.
    pub fn filter(&self, f: &CrnFilter) -> Vec<&CrnListEntry> {
        self.crns.iter().filter(|e| matches_filter(e, f)).collect()
    }
}

fn matches_filter(entry: &CrnListEntry, f: &CrnFilter) -> bool {
    if f.ipv6 {
        let ok = entry
            .ipv6_check
            .as_ref()
            .is_some_and(|m| m.values().all(|v| *v));
        if !ok {
            return false;
        }
    }
    if f.confidential && !entry.confidential_support {
        return false;
    }
    if f.gpu {
        let has_gpu = entry.gpu_support
            && entry
                .compatible_available_gpus
                .as_ref()
                .is_some_and(|g| !g.is_empty());
        if !has_gpu {
            return false;
        }
    }
    let needs_usage =
        f.min_vcpus.is_some() || f.min_memory_mib.is_some() || f.min_disk_mib.is_some();
    if needs_usage {
        let Some(u) = entry.system_usage.as_ref() else {
            return false;
        };
        if let Some(v) = f.min_vcpus
            && u.cpu.count < v
        {
            return false;
        }
        if let Some(m) = f.min_memory_mib
            && u.mem.available_kb / 1024 < m
        {
            return false;
        }
        if let Some(d) = f.min_disk_mib
            && u.disk.available_kb / 1024 < d
        {
            return false;
        }
    }
    true
}

/// Default URL for the CRN list aggregator. Override with the
/// `ALEPH_CRN_LIST_URL` env var or by passing a custom `url` to `fetch_crns_list`.
pub const DEFAULT_CRN_LIST_URL: &str = "https://crns-list.aleph.sh/crns.json";

#[derive(Debug, thiserror::Error)]
pub enum CrnListError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Failed to parse CRN list response: {0}")]
    Parse(#[from] serde_json::Error),
}

/// Fetch and parse the CRN list from `url`.
///
/// If `only_active` is true, inactive CRNs are filtered out by the server
/// (adds `?filter_inactive=true`).
pub async fn fetch_crns_list(
    http: &reqwest::Client,
    url: &url::Url,
    only_active: bool,
) -> Result<CrnListResponse, CrnListError> {
    let mut url = url.clone();
    url.query_pairs_mut().append_pair(
        "filter_inactive",
        if only_active { "true" } else { "false" },
    );
    let bytes = http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let parsed = serde_json::from_slice(&bytes)?;
    Ok(parsed)
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

    #[test]
    fn filter_ipv6_drops_beta() {
        let r: CrnListResponse = serde_json::from_str(FIXTURE).unwrap();
        let f = CrnFilter {
            ipv6: true,
            ..Default::default()
        };
        let names: Vec<&str> = r.filter(&f).iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            ["alpha", "gamma-low-ram", "delta-gpu", "epsilon-nousage"]
        );
    }

    #[test]
    fn filter_min_memory_drops_gamma() {
        let r: CrnListResponse = serde_json::from_str(FIXTURE).unwrap();
        let f = CrnFilter {
            min_memory_mib: Some(4096),
            ..Default::default()
        };
        let names: Vec<&str> = r.filter(&f).iter().map(|c| c.name.as_str()).collect();
        // epsilon has no system_usage so it's also dropped by resource filter
        assert_eq!(names, ["alpha", "beta-noipv6", "delta-gpu"]);
    }

    #[test]
    fn filter_gpu_keeps_only_delta() {
        let r: CrnListResponse = serde_json::from_str(FIXTURE).unwrap();
        let f = CrnFilter {
            gpu: true,
            ..Default::default()
        };
        let names: Vec<&str> = r.filter(&f).iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["delta-gpu"]);
    }

    #[test]
    fn filter_confidential_keeps_only_gamma() {
        let r: CrnListResponse = serde_json::from_str(FIXTURE).unwrap();
        let f = CrnFilter {
            confidential: true,
            ..Default::default()
        };
        let names: Vec<&str> = r.filter(&f).iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["gamma-low-ram"]);
    }

    #[test]
    fn filter_combined_ipv6_and_vcpus() {
        let r: CrnListResponse = serde_json::from_str(FIXTURE).unwrap();
        let f = CrnFilter {
            ipv6: true,
            min_vcpus: Some(8),
            ..Default::default()
        };
        let names: Vec<&str> = r.filter(&f).iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["alpha", "delta-gpu"]);
    }

    #[tokio::test]
    #[ignore = "hits the live CRN list aggregator"]
    async fn fetch_live_crns_list() {
        let http = reqwest::Client::new();
        let url = DEFAULT_CRN_LIST_URL.parse().unwrap();
        let list = fetch_crns_list(&http, &url, true).await.unwrap();
        assert!(!list.crns.is_empty(), "aggregator returned no CRNs");
    }
}
