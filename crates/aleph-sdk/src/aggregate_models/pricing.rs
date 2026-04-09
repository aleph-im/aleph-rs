//! Models for the pricing aggregate, i.e. the aggregate that describes the pricing tiers and
//! compute unit specifications for various entities on the network.

use aleph_types::address;
use aleph_types::chain::Address;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::LazyLock;

pub static PRICING_ADDRESS: LazyLock<Address> =
    LazyLock::new(|| address!("0xFba561a84A537fCaa567bb7A2257e7142701ae2A"));

/// Top-level wrapper. `get_aggregate` deserializes the `data` field which contains
/// `{"pricing": {...}}`.
#[derive(Debug, Clone, Deserialize)]
pub struct PricingAggregate {
    pub pricing: PricingData,
}

/// Full pricing data keyed by entity.
/// The JSON has keys like "instance", "program", "storage", etc. — unknown fields are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct PricingData {
    pub instance: PricingPerEntity,
    pub instance_confidential: PricingPerEntity,
    pub instance_gpu_standard: PricingPerEntity,
    pub instance_gpu_premium: PricingPerEntity,
}

/// A GPU model available on the network, with its pricing tier info.
#[derive(Debug, Clone)]
pub struct GpuModel {
    pub name: String,
    pub vram_mib: Option<u64>,
    pub compute_units: u32,
    pub tier: String,
}

impl PricingData {
    /// List all GPU models from standard and premium tiers.
    pub fn available_gpu_models(&self) -> Vec<GpuModel> {
        let mut models = Vec::new();
        for tier in &self.instance_gpu_standard.tiers {
            if let Some(model) = &tier.model {
                models.push(GpuModel {
                    name: model.clone(),
                    vram_mib: tier.vram,
                    compute_units: tier.compute_units,
                    tier: "standard".to_string(),
                });
            }
        }
        for tier in &self.instance_gpu_premium.tiers {
            if let Some(model) = &tier.model {
                models.push(GpuModel {
                    name: model.clone(),
                    vram_mib: tier.vram,
                    compute_units: tier.compute_units,
                    tier: "premium".to_string(),
                });
            }
        }
        models
    }

    /// Select the pricing entity based on instance type.
    pub fn for_instance(&self, confidential: bool, gpu_model: Option<&str>) -> &PricingPerEntity {
        if confidential {
            return &self.instance_confidential;
        }
        if let Some(model) = gpu_model {
            let is_premium = self
                .instance_gpu_premium
                .tiers
                .iter()
                .any(|t| t.model.as_deref() == Some(model));
            if is_premium {
                return &self.instance_gpu_premium;
            }
            return &self.instance_gpu_standard;
        }
        &self.instance
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PricingPerEntity {
    pub compute_unit: ComputeUnitSpec,
    pub tiers: Vec<Tier>,
    pub price: HashMap<String, Price>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ComputeUnitSpec {
    pub vcpus: u32,
    pub memory_mib: u64,
    pub disk_mib: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Tier {
    pub id: String,
    pub compute_units: u32,
    /// GPU model name (only for GPU tiers).
    #[serde(default)]
    pub model: Option<String>,
    /// GPU VRAM in MiB (only for GPU tiers).
    #[serde(default)]
    pub vram: Option<u64>,
}

/// Price with per-payment-type values as decimal strings.
/// `credit` is always present. `payg` and `holding` may be absent.
#[derive(Debug, Clone, Deserialize)]
pub struct Price {
    #[serde(default)]
    pub payg: Option<String>,
    #[serde(default)]
    pub holding: Option<String>,
    pub credit: String,
}

/// A tier with its resolved hardware specifications.
#[derive(Debug, Clone)]
pub struct ResolvedTier {
    pub id: String,
    pub compute_units: u32,
    pub vcpus: u32,
    pub memory_mib: u64,
    pub disk_mib: u64,
}

impl PricingPerEntity {
    /// Generate a doctl-style slug for a tier (e.g. "4vcpu-8gb").
    pub fn tier_slug(&self, tier: &Tier) -> String {
        let vcpus = tier.compute_units * self.compute_unit.vcpus;
        let memory_gib = (tier.compute_units as u64 * self.compute_unit.memory_mib) / 1024;
        format!("{vcpus}vcpu-{memory_gib}gb")
    }

    /// Find a tier matching a slug. Returns resolved specs.
    pub fn find_tier_by_slug(&self, slug: &str) -> Option<ResolvedTier> {
        self.tiers.iter().find_map(|tier| {
            if self.tier_slug(tier) == slug {
                Some(ResolvedTier {
                    id: tier.id.clone(),
                    compute_units: tier.compute_units,
                    vcpus: tier.compute_units * self.compute_unit.vcpus,
                    memory_mib: tier.compute_units as u64 * self.compute_unit.memory_mib,
                    disk_mib: tier.compute_units as u64 * self.compute_unit.disk_mib,
                })
            } else {
                None
            }
        })
    }

    /// List all available slugs.
    pub fn available_slugs(&self) -> Vec<String> {
        self.tiers.iter().map(|tier| self.tier_slug(tier)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pricing() -> PricingPerEntity {
        PricingPerEntity {
            compute_unit: ComputeUnitSpec {
                vcpus: 1,
                memory_mib: 2048,
                disk_mib: 20480,
            },
            tiers: vec![
                Tier {
                    id: "tier-1".into(),
                    compute_units: 1,
                    model: None,
                    vram: None,
                },
                Tier {
                    id: "tier-2".into(),
                    compute_units: 2,
                    model: None,
                    vram: None,
                },
                Tier {
                    id: "tier-3".into(),
                    compute_units: 4,
                    model: None,
                    vram: None,
                },
                Tier {
                    id: "tier-4".into(),
                    compute_units: 6,
                    model: None,
                    vram: None,
                },
                Tier {
                    id: "tier-5".into(),
                    compute_units: 8,
                    model: None,
                    vram: None,
                },
                Tier {
                    id: "tier-6".into(),
                    compute_units: 12,
                    model: None,
                    vram: None,
                },
            ],
            price: HashMap::from([(
                "compute_unit".to_string(),
                Price {
                    payg: Some("0.055".to_string()),
                    holding: Some("1000".to_string()),
                    credit: "14250".to_string(),
                },
            )]),
        }
    }

    #[test]
    fn tier_slug_generation() {
        let pricing = test_pricing();

        assert_eq!(pricing.tier_slug(&pricing.tiers[0]), "1vcpu-2gb");
        assert_eq!(pricing.tier_slug(&pricing.tiers[2]), "4vcpu-8gb");
        assert_eq!(pricing.tier_slug(&pricing.tiers[5]), "12vcpu-24gb");
    }

    #[test]
    fn find_tier_by_slug_found() {
        let pricing = test_pricing();
        let resolved = pricing.find_tier_by_slug("4vcpu-8gb").unwrap();

        assert_eq!(resolved.id, "tier-3");
        assert_eq!(resolved.compute_units, 4);
        assert_eq!(resolved.vcpus, 4);
        assert_eq!(resolved.memory_mib, 8192);
        assert_eq!(resolved.disk_mib, 81920);
    }

    #[test]
    fn find_tier_by_slug_not_found() {
        let pricing = test_pricing();
        assert!(pricing.find_tier_by_slug("3vcpu-6gb").is_none());
    }

    #[test]
    fn available_slugs_lists_all() {
        let pricing = test_pricing();
        let slugs = pricing.available_slugs();

        assert_eq!(
            slugs,
            vec![
                "1vcpu-2gb",
                "2vcpu-4gb",
                "4vcpu-8gb",
                "6vcpu-12gb",
                "8vcpu-16gb",
                "12vcpu-24gb",
            ]
        );
    }

    #[test]
    fn deserialize_pricing_aggregate() {
        let json = r#"{
            "pricing": {
                "instance": {
                    "compute_unit": {
                        "vcpus": 1,
                        "memory_mib": 2048,
                        "disk_mib": 20480
                    },
                    "tiers": [
                        {"id": "tier-1", "compute_units": 1},
                        {"id": "tier-2", "compute_units": 2}
                    ],
                    "price": {
                        "compute_unit": {
                            "payg": "0.055",
                            "holding": "1000",
                            "credit": "14250"
                        }
                    }
                },
                "instance_confidential": {
                    "compute_unit": {"vcpus": 1, "memory_mib": 2048, "disk_mib": 20480},
                    "tiers": [{"id": "tier-1", "compute_units": 1}],
                    "price": {"compute_unit": {"credit": "28500"}}
                },
                "instance_gpu_standard": {
                    "compute_unit": {"vcpus": 1, "memory_mib": 6144, "disk_mib": 61440},
                    "tiers": [{"id": "tier-1", "compute_units": 3, "model": "RTX 4000 ADA", "vram": 20480}],
                    "price": {"compute_unit": {"credit": "0.28"}}
                },
                "instance_gpu_premium": {
                    "compute_unit": {"vcpus": 1, "memory_mib": 6144, "disk_mib": 61440},
                    "tiers": [{"id": "tier-1", "compute_units": 16, "model": "A100", "vram": 81920}],
                    "price": {"compute_unit": {"credit": "0.56"}}
                },
                "program": {
                    "compute_unit": {"vcpus": 1, "memory_mib": 128, "disk_mib": 0},
                    "tiers": [],
                    "price": {}
                },
                "storage": {
                    "compute_unit": {"vcpus": 0, "memory_mib": 0, "disk_mib": 1},
                    "tiers": [],
                    "price": {}
                }
            }
        }"#;

        let agg: PricingAggregate = serde_json::from_str(json).unwrap();
        assert_eq!(agg.pricing.instance.compute_unit.vcpus, 1);
        assert_eq!(agg.pricing.instance.compute_unit.memory_mib, 2048);
        assert_eq!(agg.pricing.instance.tiers.len(), 2);
        assert_eq!(agg.pricing.instance.tiers[0].id, "tier-1");

        let price = agg.pricing.instance.price.get("compute_unit").unwrap();
        assert_eq!(price.payg.as_deref(), Some("0.055"));
        assert_eq!(price.holding.as_deref(), Some("1000"));
        assert_eq!(price.credit, "14250");
    }
}
