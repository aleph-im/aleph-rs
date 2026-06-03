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

impl GpuModel {
    /// Lowercase, hyphen-separated slug for display and matching (e.g. "rtx-4000-ada").
    pub fn slug(&self) -> String {
        self.name.to_lowercase().replace(' ', "-")
    }
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
    ///
    /// Returns the standard instance pricing if the GPU model is not found in either GPU tier.
    pub fn for_instance(&self, confidential: bool, gpu_model: Option<&str>) -> &PricingPerEntity {
        if confidential {
            return &self.instance_confidential;
        }
        if let Some(model) = gpu_model {
            if self
                .instance_gpu_premium
                .tiers
                .iter()
                .any(|t| t.model.as_deref() == Some(model))
            {
                return &self.instance_gpu_premium;
            }
            if self
                .instance_gpu_standard
                .tiers
                .iter()
                .any(|t| t.model.as_deref() == Some(model))
            {
                return &self.instance_gpu_standard;
            }
        }
        &self.instance
    }

    /// Return the GPU namespace ("standard" or "premium") whose tiers expose a size matching
    /// `slug`, or `None` if the slug is not a GPU size. Used to hint that a non-GPU `--size`
    /// error is actually a GPU size living in another pricing namespace.
    pub fn gpu_namespace_for_slug(&self, slug: &str) -> Option<&'static str> {
        if self.instance_gpu_standard.find_tier_by_slug(slug).is_some() {
            return Some("standard");
        }
        if self.instance_gpu_premium.find_tier_by_slug(slug).is_some() {
            return Some("premium");
        }
        None
    }

    /// User-facing error message for a non-GPU `--size` value that does not match
    /// any regular instance tier. Lists the available regular sizes and, when the
    /// slug actually belongs to a GPU namespace, appends a hint to add `--gpu`.
    /// Shared by every command that resolves a regular `--size` so the wording and
    /// the cross-namespace hint stay consistent.
    pub fn invalid_instance_size_message(&self, slug: &str) -> String {
        let available = self.instance.available_slugs().join(", ");
        let mut msg = format!("invalid size '{slug}'. Available sizes: {available}");
        if self.gpu_namespace_for_slug(slug).is_some() {
            msg.push_str(&format!(
                " Note: '{slug}' is a GPU size; add --gpu <model> \
                 (see `aleph instance price --list-gpus`)."
            ));
        }
        msg
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
    /// Generate a size slug for a given number of compute units (e.g. "4vcpu-8gb").
    pub fn slug_for_compute_units(&self, compute_units: u32) -> String {
        let vcpus = compute_units * self.compute_unit.vcpus;
        let memory_gib = (compute_units as u64 * self.compute_unit.memory_mib) / 1024;
        format!("{vcpus}vcpu-{memory_gib}gb")
    }

    /// Generate a slug for a tier (e.g. "4vcpu-8gb").
    pub fn tier_slug(&self, tier: &Tier) -> String {
        self.slug_for_compute_units(tier.compute_units)
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

    /// Inverse of `slug_for_compute_units`: parse a size slug into a compute-unit
    /// count for this entity's compute-unit definition. Returns `None` when the
    /// slug is not a clean whole multiple of this entity's compute unit (wrong
    /// format, vcpus not divisible by the per-CU vcpus, or memory that does not
    /// match the resulting CU count).
    ///
    /// Unlike `find_tier_by_slug`, this does not require a matching tier to
    /// exist: GPU entities list one tier per model (its minimum), but a GPU can
    /// be sized at any whole multiple of its compute unit at or above that
    /// minimum, so the size is derived arithmetically rather than enumerated.
    pub fn compute_units_for_slug(&self, slug: &str) -> Option<u32> {
        let (vcpu_part, _) = slug.split_once("vcpu-")?;
        let vcpus: u32 = vcpu_part.parse().ok()?;
        let per_cu = self.compute_unit.vcpus;
        if per_cu == 0 || vcpus == 0 || !vcpus.is_multiple_of(per_cu) {
            return None;
        }
        let compute_units = vcpus / per_cu;
        // Round-trip to validate the memory portion matches this CU definition.
        (self.slug_for_compute_units(compute_units) == slug).then_some(compute_units)
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

    /// A GPU-style compute unit: 1 vCPU + 6 GiB RAM per CU. The model tier's
    /// `compute_units` is only a minimum; sizes scale by whole CU multiples.
    fn gpu_pricing() -> PricingPerEntity {
        PricingPerEntity {
            compute_unit: ComputeUnitSpec {
                vcpus: 1,
                memory_mib: 6144,
                disk_mib: 61440,
            },
            tiers: vec![Tier {
                id: "tier-1".into(),
                compute_units: 3,
                model: Some("RTX 4000 ADA".into()),
                vram: Some(20480),
            }],
            price: HashMap::new(),
        }
    }

    fn gpu_pricing_tier(memory_mib: u64, model: &str, compute_units: u32) -> PricingPerEntity {
        PricingPerEntity {
            compute_unit: ComputeUnitSpec {
                vcpus: 1,
                memory_mib,
                disk_mib: 61440,
            },
            tiers: vec![Tier {
                id: "gpu-tier".into(),
                compute_units,
                model: Some(model.into()),
                vram: Some(20480),
            }],
            price: HashMap::from([(
                "compute_unit".to_string(),
                Price {
                    payg: None,
                    holding: None,
                    credit: "0.28".to_string(),
                },
            )]),
        }
    }

    #[test]
    fn compute_units_for_slug_accepts_any_clean_multiple() {
        let gpu = gpu_pricing();
        // The model minimum and arbitrary larger multiples both resolve, even
        // though only the 3-CU tier exists.
        assert_eq!(gpu.compute_units_for_slug("3vcpu-18gb"), Some(3));
        assert_eq!(gpu.compute_units_for_slug("4vcpu-24gb"), Some(4));
        assert_eq!(gpu.compute_units_for_slug("5vcpu-30gb"), Some(5));
    }

    #[test]
    fn compute_units_for_slug_rejects_mismatched_or_malformed() {
        let gpu = gpu_pricing();
        // Memory does not match the CU definition (4 CU would be 24gb, not 8gb).
        assert_eq!(gpu.compute_units_for_slug("4vcpu-8gb"), None);
        assert_eq!(gpu.compute_units_for_slug("0vcpu-0gb"), None);
        assert_eq!(gpu.compute_units_for_slug("garbage"), None);
        assert_eq!(gpu.compute_units_for_slug(""), None);
    }

    #[test]
    fn gpu_namespace_for_slug_classifies_slugs() {
        // memory_mib 6144 => 6 GiB per CU; CU 4 => 4vcpu-24gb (standard), CU 16 => 16vcpu-96gb (premium)
        let data = PricingData {
            instance: test_pricing(),
            instance_confidential: test_pricing(),
            instance_gpu_standard: gpu_pricing_tier(6144, "RTX 4000 ADA", 4),
            instance_gpu_premium: gpu_pricing_tier(6144, "A100", 16),
        };

        assert_eq!(data.gpu_namespace_for_slug("4vcpu-24gb"), Some("standard"));
        assert_eq!(data.gpu_namespace_for_slug("16vcpu-96gb"), Some("premium"));
        // A regular (non-GPU) size is not a GPU slug.
        assert_eq!(data.gpu_namespace_for_slug("4vcpu-8gb"), None);
        assert_eq!(data.gpu_namespace_for_slug("does-not-exist"), None);
    }

    #[test]
    fn invalid_instance_size_message_hints_at_gpu_namespace() {
        let data = PricingData {
            instance: test_pricing(),
            instance_confidential: test_pricing(),
            instance_gpu_standard: gpu_pricing_tier(6144, "RTX 4000 ADA", 4),
            instance_gpu_premium: gpu_pricing_tier(6144, "A100", 16),
        };

        // A GPU size gets the cross-namespace hint appended.
        let gpu_msg = data.invalid_instance_size_message("4vcpu-24gb");
        assert!(gpu_msg.contains("Available sizes:"), "{gpu_msg}");
        assert!(gpu_msg.contains("is a GPU size; add --gpu"), "{gpu_msg}");

        // A truly unknown size lists the available sizes without a GPU hint.
        let plain_msg = data.invalid_instance_size_message("does-not-exist");
        assert!(plain_msg.contains("Available sizes:"), "{plain_msg}");
        assert!(!plain_msg.contains("GPU size"), "{plain_msg}");
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
