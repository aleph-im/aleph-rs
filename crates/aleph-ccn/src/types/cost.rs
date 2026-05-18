//! Cost computation domain types.
//!
//! Mirrors `src/aleph/types/cost.py`.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Type of product whose pricing is being described.
///
/// Mirrors `aleph.toolkit.constants.ProductPriceType` (which lives outside
/// `aleph.types` in Python but is part of the cost API surface).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductPriceType {
    Storage,
    Web3Hosting,
    Program,
    ProgramPersistent,
    Instance,
    InstanceGpuPremium,
    InstanceConfidential,
    InstanceGpuStandard,
}

impl ProductPriceType {
    /// The wire/JSON key (same as Python `.value`).
    pub fn as_value_str(&self) -> &'static str {
        match self {
            ProductPriceType::Storage => "storage",
            ProductPriceType::Web3Hosting => "web3_hosting",
            ProductPriceType::Program => "program",
            ProductPriceType::ProgramPersistent => "program_persistent",
            ProductPriceType::Instance => "instance",
            ProductPriceType::InstanceGpuPremium => "instance_gpu_premium",
            ProductPriceType::InstanceConfidential => "instance_confidential",
            ProductPriceType::InstanceGpuStandard => "instance_gpu_standard",
        }
    }
}

/// Pricing components (`holding`, `payg`, `credit`) — each is a Decimal,
/// defaulting to zero when missing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductPriceOptions {
    pub holding: Decimal,
    pub payg: Decimal,
    pub credit: Decimal,
}

impl ProductPriceOptions {
    pub fn new(
        holding: Option<&str>,
        payg: Option<&str>,
        credit: Option<&str>,
    ) -> Result<Self, rust_decimal::Error> {
        let parse = |opt: Option<&str>| -> Result<Decimal, rust_decimal::Error> {
            match opt {
                None => Ok(Decimal::ZERO),
                Some(s) if s.is_empty() => Ok(Decimal::ZERO),
                Some(s) => Decimal::from_str(s).or_else(|_| {
                    // rust_decimal::from_str rejects exponent notation like "1e-18";
                    // fall back to the scientific parser to match Python's
                    // `Decimal(str)` permissiveness.
                    Decimal::from_scientific(s)
                }),
            }
        };
        Ok(Self {
            holding: parse(holding)?,
            payg: parse(payg)?,
            credit: parse(credit)?,
        })
    }
}

/// Compute-unit shape (vcpus / disk / memory).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductComputeUnit {
    pub vcpus: u32,
    pub disk_mib: u64,
    pub memory_mib: u64,
}

impl ProductComputeUnit {
    pub fn new(vcpus: u32, disk_mib: u64, memory_mib: u64) -> Self {
        Self {
            vcpus,
            disk_mib,
            memory_mib,
        }
    }
}

/// Full per-product price information (storage + optional compute unit).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductPrice {
    pub storage: ProductPriceOptions,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub compute_unit: Option<ProductPriceOptions>,
}

impl ProductPrice {
    pub fn new(storage: ProductPriceOptions, compute_unit: Option<ProductPriceOptions>) -> Self {
        Self {
            storage,
            compute_unit,
        }
    }
}

/// Pricing tier definition for tiered (e.g., GPU) products.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductTier {
    pub id: String,
    pub compute_units: u32,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub vram: Option<u64>,
}

/// Top-level pricing record for a product type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductPricing {
    #[serde(rename = "type")]
    pub price_type: ProductPriceType,
    pub price: ProductPrice,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub compute_unit: Option<ProductComputeUnit>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tiers: Option<Vec<ProductTier>>,
}

/// Errors returned by [`ProductPricing::from_aggregate`].
#[derive(Debug, thiserror::Error)]
pub enum ProductPricingError {
    #[error("missing key in aggregate: {0}")]
    MissingKey(String),
    #[error("invalid value for {field}: {message}")]
    InvalidValue { field: String, message: String },
}

impl ProductPricing {
    pub fn new(
        price_type: ProductPriceType,
        price: ProductPrice,
        compute_unit: Option<ProductComputeUnit>,
        tiers: Option<Vec<ProductTier>>,
    ) -> Self {
        Self {
            price_type,
            price,
            compute_unit,
            tiers,
        }
    }

    /// Build a `ProductPricing` from the aggregate content JSON.
    ///
    /// Mirrors Python `ProductPricing.from_aggregate(price_type, aggregate)`,
    /// where `aggregate` is either an `AggregateDb` whose `.content` is a dict,
    /// or a plain dict. In both cases the call reduces to looking up
    /// `content[price_type.value]`. Here we accept the already-extracted
    /// outer dict (`Value`); call sites do the AggregateDb→dict step.
    pub fn from_aggregate(
        price_type: ProductPriceType,
        aggregate: &Value,
    ) -> Result<Self, ProductPricingError> {
        let content = aggregate.get(price_type.as_value_str()).ok_or_else(|| {
            ProductPricingError::MissingKey(price_type.as_value_str().to_string())
        })?;

        let price = content
            .get("price")
            .ok_or_else(|| ProductPricingError::MissingKey("price".to_string()))?;

        let storage_obj = price
            .get("storage")
            .ok_or_else(|| ProductPricingError::MissingKey("price.storage".to_string()))?;

        let storage = ProductPriceOptions::new(
            json_str(storage_obj.get("holding")),
            json_str(storage_obj.get("payg")),
            json_str(storage_obj.get("credit")),
        )
        .map_err(|e| ProductPricingError::InvalidValue {
            field: "price.storage".to_string(),
            message: e.to_string(),
        })?;

        let compute_unit_section = content.get("compute_unit");
        let tiers_section = content.get("tiers");

        // The Python code computes `compute_unit_options` from
        // `price["compute_unit"]` when `content["compute_unit"]` is truthy.
        let price_compute_unit_options = if compute_unit_section.is_some() {
            let price_cu = price
                .get("compute_unit")
                .ok_or_else(|| ProductPricingError::MissingKey("price.compute_unit".to_string()))?;
            Some(
                ProductPriceOptions::new(
                    json_str(price_cu.get("holding")),
                    json_str(price_cu.get("payg")),
                    json_str(price_cu.get("credit")),
                )
                .map_err(|e| ProductPricingError::InvalidValue {
                    field: "price.compute_unit".to_string(),
                    message: e.to_string(),
                })?,
            )
        } else {
            None
        };

        // Python ties the surfaced `compute_unit` to the presence of `tiers`,
        // not `compute_unit`. This matches the original behaviour exactly.
        let compute_unit = if tiers_section.is_some() {
            let cu = compute_unit_section
                .ok_or_else(|| ProductPricingError::MissingKey("compute_unit".to_string()))?;
            Some(ProductComputeUnit {
                vcpus: u32_from(cu.get("vcpus"), "compute_unit.vcpus")?,
                disk_mib: u64_from(cu.get("disk_mib"), "compute_unit.disk_mib")?,
                memory_mib: u64_from(cu.get("memory_mib"), "compute_unit.memory_mib")?,
            })
        } else {
            None
        };

        let tiers = match tiers_section {
            None => Some(Vec::<ProductTier>::new()),
            Some(Value::Null) => Some(Vec::new()),
            Some(Value::Array(arr)) => Some(
                arr.iter()
                    .map(|tier| -> Result<ProductTier, ProductPricingError> {
                        Ok(ProductTier {
                            id: tier
                                .get("id")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ProductPricingError::MissingKey("tier.id".to_string())
                                })?
                                .to_string(),
                            compute_units: u32_from(
                                tier.get("compute_units"),
                                "tier.compute_units",
                            )?,
                            model: tier
                                .get("model")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            vram: tier.get("vram").and_then(|v| v.as_u64()),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some(_) => {
                return Err(ProductPricingError::InvalidValue {
                    field: "tiers".to_string(),
                    message: "expected array".to_string(),
                });
            }
        };

        Ok(ProductPricing {
            price_type,
            price: ProductPrice {
                storage,
                compute_unit: price_compute_unit_options,
            },
            compute_unit,
            tiers,
        })
    }
}

fn json_str(v: Option<&Value>) -> Option<&str> {
    match v {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Number(n)) => {
            // Numeric prices appear sometimes in aggregate JSON: serialize through
            // its string form so Decimal::from_str can ingest it.
            Some(num_to_static(n))
        }
        _ => None,
    }
}

/// Coerce a `serde_json::Number` into a `'static str` via leaking.
///
/// We avoid this in practice because production payloads use strings. Numeric
/// values are exceedingly rare and small in volume, so the leak is acceptable
/// in the same way that one-time configuration parsing leaks are. If this
/// becomes hot, replace with an owned String + temporary buffer parsing.
fn num_to_static(n: &serde_json::Number) -> &'static str {
    Box::leak(n.to_string().into_boxed_str())
}

fn u32_from(v: Option<&Value>, field: &str) -> Result<u32, ProductPricingError> {
    v.and_then(|x| x.as_u64())
        .and_then(|n| u32::try_from(n).ok())
        .ok_or_else(|| ProductPricingError::InvalidValue {
            field: field.to_string(),
            message: "expected unsigned integer fitting in u32".to_string(),
        })
}

fn u64_from(v: Option<&Value>, field: &str) -> Result<u64, ProductPricingError> {
    v.and_then(|x| x.as_u64())
        .ok_or_else(|| ProductPricingError::InvalidValue {
            field: field.to_string(),
            message: "expected unsigned integer".to_string(),
        })
}

/// Types of cost items contributing to a message's total.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CostType {
    #[serde(rename = "EXECUTION")]
    Execution,
    #[serde(rename = "EXECUTION_VOLUME_PERSISTENT")]
    ExecutionVolumePersistent,
    #[serde(rename = "EXECUTION_VOLUME_INMUTABLE")]
    ExecutionVolumeInmutable,
    #[serde(rename = "EXECUTION_VOLUME_DISCOUNT")]
    ExecutionVolumeDiscount,
    #[serde(rename = "EXECUTION_INSTANCE_VOLUME_ROOTFS")]
    ExecutionInstanceVolumeRootfs,
    #[serde(rename = "EXECUTION_PROGRAM_VOLUME_CODE")]
    ExecutionProgramVolumeCode,
    #[serde(rename = "EXECUTION_PROGRAM_VOLUME_RUNTIME")]
    ExecutionProgramVolumeRuntime,
    #[serde(rename = "EXECUTION_PROGRAM_VOLUME_DATA")]
    ExecutionProgramVolumeData,
    #[serde(rename = "STORAGE")]
    Storage,
}

impl CostType {
    pub fn as_value_str(&self) -> &'static str {
        match self {
            CostType::Execution => "EXECUTION",
            CostType::ExecutionVolumePersistent => "EXECUTION_VOLUME_PERSISTENT",
            CostType::ExecutionVolumeInmutable => "EXECUTION_VOLUME_INMUTABLE",
            CostType::ExecutionVolumeDiscount => "EXECUTION_VOLUME_DISCOUNT",
            CostType::ExecutionInstanceVolumeRootfs => "EXECUTION_INSTANCE_VOLUME_ROOTFS",
            CostType::ExecutionProgramVolumeCode => "EXECUTION_PROGRAM_VOLUME_CODE",
            CostType::ExecutionProgramVolumeRuntime => "EXECUTION_PROGRAM_VOLUME_RUNTIME",
            CostType::ExecutionProgramVolumeData => "EXECUTION_PROGRAM_VOLUME_DATA",
            CostType::Storage => "STORAGE",
        }
    }
}

/// Base record describing one cost item. Mirrors Python `VolumeCost`.
///
/// Both `SizedVolume` and `RefVolume` are model-as-flat-structs because
/// Python uses inheritance for code reuse, but the data model is just
/// `cost_type` + `name` + a few optionals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeCost {
    pub cost_type: CostType,
    pub name: String,
}

impl VolumeCost {
    pub fn new(cost_type: CostType, name: Option<String>) -> Self {
        let name = name.unwrap_or_else(|| cost_type.as_value_str().to_string());
        Self { cost_type, name }
    }
}

/// A cost item attached to a sized volume. Mirrors Python `SizedVolume`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SizedVolume {
    pub cost_type: CostType,
    pub name: String,
    pub size_mib: Decimal,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub r#ref: Option<String>,
}

impl SizedVolume {
    pub fn new(
        cost_type: CostType,
        size_mib: Decimal,
        r#ref: Option<String>,
        name: Option<String>,
    ) -> Self {
        let name = name.unwrap_or_else(|| cost_type.as_value_str().to_string());
        Self {
            cost_type,
            name,
            size_mib,
            r#ref,
        }
    }
}

/// A cost item attached to a referenced (parent) volume. Mirrors Python `RefVolume`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefVolume {
    pub cost_type: CostType,
    pub name: String,
    pub r#ref: String,
    pub use_latest: bool,
}

impl RefVolume {
    pub fn new(cost_type: CostType, r#ref: String, use_latest: bool, name: Option<String>) -> Self {
        let name = name.unwrap_or_else(|| cost_type.as_value_str().to_string());
        Self {
            cost_type,
            name,
            r#ref,
            use_latest,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn product_price_type_roundtrip() {
        for (variant, expected) in [
            (ProductPriceType::Storage, "\"storage\""),
            (ProductPriceType::Web3Hosting, "\"web3_hosting\""),
            (ProductPriceType::Program, "\"program\""),
            (
                ProductPriceType::ProgramPersistent,
                "\"program_persistent\"",
            ),
            (ProductPriceType::Instance, "\"instance\""),
            (
                ProductPriceType::InstanceGpuPremium,
                "\"instance_gpu_premium\"",
            ),
            (
                ProductPriceType::InstanceConfidential,
                "\"instance_confidential\"",
            ),
            (
                ProductPriceType::InstanceGpuStandard,
                "\"instance_gpu_standard\"",
            ),
        ] {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            let parsed: ProductPriceType = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn cost_type_roundtrip() {
        let cases = [
            (CostType::Execution, "\"EXECUTION\""),
            (
                CostType::ExecutionVolumePersistent,
                "\"EXECUTION_VOLUME_PERSISTENT\"",
            ),
            (
                CostType::ExecutionVolumeInmutable,
                "\"EXECUTION_VOLUME_INMUTABLE\"",
            ),
            (CostType::Storage, "\"STORAGE\""),
        ];
        for (variant, expected) in cases {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            let parsed: CostType = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn product_price_options_defaults_zero() {
        let p = ProductPriceOptions::new(None, None, None).unwrap();
        assert_eq!(p.holding, Decimal::ZERO);
        assert_eq!(p.payg, Decimal::ZERO);
        assert_eq!(p.credit, Decimal::ZERO);

        let p = ProductPriceOptions::new(Some("0.000001"), Some(""), Some("1.5")).unwrap();
        assert_eq!(p.holding, Decimal::from_str("0.000001").unwrap());
        assert_eq!(p.payg, Decimal::ZERO);
        assert_eq!(p.credit, Decimal::from_str("1.5").unwrap());
    }

    #[test]
    fn sized_volume_default_name() {
        let v = SizedVolume::new(CostType::Storage, Decimal::from(100), None, None);
        assert_eq!(v.name, "STORAGE");

        let v = SizedVolume::new(
            CostType::Execution,
            Decimal::from(1),
            Some("ref-hash".into()),
            Some("custom".into()),
        );
        assert_eq!(v.name, "custom");
        assert_eq!(v.r#ref.as_deref(), Some("ref-hash"));
    }

    #[test]
    fn ref_volume_default_name() {
        let v = RefVolume::new(
            CostType::ExecutionInstanceVolumeRootfs,
            "abc".into(),
            true,
            None,
        );
        assert_eq!(v.name, "EXECUTION_INSTANCE_VOLUME_ROOTFS");
        assert!(v.use_latest);
    }

    #[test]
    fn from_aggregate_storage_only() {
        let agg = json!({
            "storage": {
                "price": {
                    "storage": {
                        "holding": "0.333333333333333"
                    }
                }
            }
        });
        let p = ProductPricing::from_aggregate(ProductPriceType::Storage, &agg).unwrap();
        assert_eq!(p.price_type, ProductPriceType::Storage);
        assert_eq!(
            p.price.storage.holding,
            Decimal::from_str("0.333333333333333").unwrap()
        );
        assert_eq!(p.price.storage.payg, Decimal::ZERO);
        assert!(p.price.compute_unit.is_none());
        assert!(p.compute_unit.is_none());
        assert_eq!(p.tiers.as_ref().unwrap().len(), 0);
    }

    #[test]
    fn from_aggregate_with_tiers() {
        let agg = json!({
            "instance_gpu_standard": {
                "price": {
                    "storage": {"holding": "0", "payg": "0.0001"},
                    "compute_unit": {"holding": "200", "payg": "0.0002"}
                },
                "compute_unit": {
                    "vcpus": 4,
                    "disk_mib": 81920,
                    "memory_mib": 16384
                },
                "tiers": [
                    {"id": "tier-1", "compute_units": 1, "model": "rtx-3090", "vram": 24576},
                    {"id": "tier-2", "compute_units": 2}
                ]
            }
        });
        let p =
            ProductPricing::from_aggregate(ProductPriceType::InstanceGpuStandard, &agg).unwrap();
        assert_eq!(p.price.storage.payg, Decimal::from_str("0.0001").unwrap());
        let cu = p.price.compute_unit.as_ref().unwrap();
        assert_eq!(cu.holding, Decimal::from(200));
        let cu_struct = p.compute_unit.unwrap();
        assert_eq!(cu_struct.vcpus, 4);
        assert_eq!(cu_struct.disk_mib, 81920);
        assert_eq!(cu_struct.memory_mib, 16384);
        let tiers = p.tiers.as_ref().unwrap();
        assert_eq!(tiers.len(), 2);
        assert_eq!(tiers[0].id, "tier-1");
        assert_eq!(tiers[0].model.as_deref(), Some("rtx-3090"));
        assert_eq!(tiers[0].vram, Some(24576));
        assert!(tiers[1].model.is_none());
    }
}
