//! Constants used throughout the node.
//!
//! Port of `src/aleph/toolkit/constants.py`.
//!
//! The `DEFAULT_PRICE_AGGREGATE` and `DEFAULT_SETTINGS_AGGREGATE` dictionaries
//! are kept as `serde_json::Value` for portability — they are direct
//! translations of the Python literals.

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// --- Byte sizes ------------------------------------------------------------

pub const KIB: u64 = 1024;
pub const MIB: u64 = 1024 * 1024;
pub const GIB: u64 = 1024 * 1024 * 1024;

// Lowercase aliases mirroring the Python identifiers (`KiB`, `MiB`, `GiB`).
#[allow(non_upper_case_globals)]
pub const KiB: u64 = KIB;
#[allow(non_upper_case_globals)]
pub const MiB: u64 = MIB;
#[allow(non_upper_case_globals)]
pub const GiB: u64 = GIB;

// --- Time in seconds -------------------------------------------------------

pub const MINUTE: u64 = 60;
pub const HOUR: u64 = 60 * MINUTE;
pub const DAY: u64 = 24 * HOUR;

// --- Product price type ----------------------------------------------------

/// Mirrors Python's `ProductPriceType(str, Enum)`. Serialised as the lowercase
/// snake_case string identical to its Python value.
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
    /// Wire-format string identical to the Python enum value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Storage => "storage",
            Self::Web3Hosting => "web3_hosting",
            Self::Program => "program",
            Self::ProgramPersistent => "program_persistent",
            Self::Instance => "instance",
            Self::InstanceGpuPremium => "instance_gpu_premium",
            Self::InstanceConfidential => "instance_confidential",
            Self::InstanceGpuStandard => "instance_gpu_standard",
        }
    }
}

// --- Pricing aggregate -----------------------------------------------------

pub const PRICE_AGGREGATE_OWNER: &str = "0xFba561a84A537fCaa567bb7A2257e7142701ae2A";
pub const PRICE_AGGREGATE_KEY: &str = "pricing";
pub const PRICE_PRECISION: u32 = 18;

/// Default pricing aggregate. Mirrors `DEFAULT_PRICE_AGGREGATE` exactly.
pub static DEFAULT_PRICE_AGGREGATE: Lazy<Value> = Lazy::new(|| {
    json!({
        "program": {
            "price": {
                "storage": {
                    "payg": "0.000000977",
                    "holding": "0.05",
                    "credit": "0.977"
                },
                "compute_unit": {
                    "payg": "0.011",
                    "holding": "200",
                    "credit": "11000"
                }
            },
            "tiers": [
                {"id": "tier-1", "compute_units": 1},
                {"id": "tier-2", "compute_units": 2},
                {"id": "tier-3", "compute_units": 4},
                {"id": "tier-4", "compute_units": 6},
                {"id": "tier-5", "compute_units": 8},
                {"id": "tier-6", "compute_units": 12}
            ],
            "compute_unit": {
                "vcpus": 1,
                "disk_mib": 2048,
                "memory_mib": 2048
            }
        },
        "storage": {
            "price": {
                "storage": {
                    "holding": "0.333333333",
                    "credit": "0.17967489030626108"
                }
            }
        },
        "instance": {
            "price": {
                "storage": {
                    "payg": "0.000000977",
                    "holding": "0.05",
                    "credit": "0.17967489030626108"
                },
                "compute_unit": {
                    "payg": "0.055",
                    "holding": "1000",
                    "credit": "14250"
                }
            },
            "tiers": [
                {"id": "tier-1", "compute_units": 1},
                {"id": "tier-2", "compute_units": 2},
                {"id": "tier-3", "compute_units": 4},
                {"id": "tier-4", "compute_units": 6},
                {"id": "tier-5", "compute_units": 8},
                {"id": "tier-6", "compute_units": 12}
            ],
            "compute_unit": {
                "vcpus": 1,
                "disk_mib": 20480,
                "memory_mib": 2048
            }
        },
        "web3_hosting": {
            "price": {
                "fixed": 50,
                "storage": {
                    "holding": "0.333333333",
                    "credit": "0.17967489030626108"
                }
            }
        },
        "program_persistent": {
            "price": {
                "storage": {
                    "payg": "0.000000977",
                    "holding": "0.05",
                    "credit": "0.977"
                },
                "compute_unit": {
                    "payg": "0.055",
                    "holding": "1000",
                    "credit": "55000"
                }
            },
            "tiers": [
                {"id": "tier-1", "compute_units": 1},
                {"id": "tier-2", "compute_units": 2},
                {"id": "tier-3", "compute_units": 4},
                {"id": "tier-4", "compute_units": 6},
                {"id": "tier-5", "compute_units": 8},
                {"id": "tier-6", "compute_units": 12}
            ],
            "compute_unit": {
                "vcpus": 1,
                "disk_mib": 20480,
                "memory_mib": 2048
            }
        },
        "instance_gpu_premium": {
            "price": {
                "storage": {
                    "payg": "0.000000977",
                    "credit": "0.17967489030626108"
                },
                "compute_unit": {
                    "payg": "0.56",
                    "holding": "560",
                    "credit": "86250"
                }
            },
            "tiers": [
                {"id": "tier-1", "vram": 81920, "model": "A100", "compute_units": 16},
                {"id": "tier-2", "vram": 81920, "model": "H100", "compute_units": 24}
            ],
            "compute_unit": {
                "vcpus": 1,
                "disk_mib": 61440,
                "memory_mib": 6144
            }
        },
        "instance_confidential": {
            "price": {
                "storage": {
                    "payg": "0.000000977",
                    "holding": "0.05",
                    "credit": "0.17967489030626108"
                },
                "compute_unit": {
                    "payg": "0.11",
                    "holding": "2000",
                    "credit": "28500"
                }
            },
            "tiers": [
                {"id": "tier-1", "compute_units": 1},
                {"id": "tier-2", "compute_units": 2},
                {"id": "tier-3", "compute_units": 4},
                {"id": "tier-4", "compute_units": 6},
                {"id": "tier-5", "compute_units": 8},
                {"id": "tier-6", "compute_units": 12}
            ],
            "compute_unit": {
                "vcpus": 1,
                "disk_mib": 20480,
                "memory_mib": 2048
            }
        },
        "instance_gpu_standard": {
            "price": {
                "storage": {
                    "payg": "0.000000977",
                    "credit": "0.17967489030626108"
                },
                "compute_unit": {
                    "payg": "0.28",
                    "holding": "280",
                    "credit": "43125"
                }
            },
            "tiers": [
                {"id": "tier-1", "vram": 20480, "model": "RTX 4000 ADA", "compute_units": 3},
                {"id": "tier-2", "vram": 24576, "model": "RTX 3090", "compute_units": 4},
                {"id": "tier-3", "vram": 24576, "model": "RTX 4090", "compute_units": 6},
                {"id": "tier-4", "vram": 49152, "model": "L40S", "compute_units": 12}
            ],
            "compute_unit": {
                "vcpus": 1,
                "disk_mib": 61440,
                "memory_mib": 6144
            }
        }
    })
});

// --- Settings aggregate ----------------------------------------------------

pub const SETTINGS_AGGREGATE_OWNER: &str = "0xFba561a84A537fCaa567bb7A2257e7142701ae2A";
pub const SETTINGS_AGGREGATE_KEY: &str = "settings";

pub static DEFAULT_SETTINGS_AGGREGATE: Lazy<Value> = Lazy::new(|| {
    json!({
        "compatible_gpus": [
            {"name": "AD102GL [L40S]", "model": "L40S", "vendor": "NVIDIA", "device_id": "10de:26b9"},
            {"name": "GB202 [GeForce RTX 5090]", "model": "RTX 5090", "vendor": "NVIDIA", "device_id": "10de:2685"},
            {"name": "GB202 [GeForce RTX 5090 D]", "model": "RTX 5090", "vendor": "NVIDIA", "device_id": "10de:2687"},
            {"name": "AD102 [GeForce RTX 4090]", "model": "RTX 4090", "vendor": "NVIDIA", "device_id": "10de:2684"},
            {"name": "AD102 [GeForce RTX 4090 D]", "model": "RTX 4090", "vendor": "NVIDIA", "device_id": "10de:2685"},
            {"name": "GA102 [GeForce RTX 3090]", "model": "RTX 3090", "vendor": "NVIDIA", "device_id": "10de:2204"},
            {"name": "GA102 [GeForce RTX 3090 Ti]", "model": "RTX 3090", "vendor": "NVIDIA", "device_id": "10de:2203"},
            {"name": "AD104GL [RTX 4000 SFF Ada Generation]", "model": "RTX 4000 ADA", "vendor": "NVIDIA", "device_id": "10de:27b0"},
            {"name": "AD104GL [RTX 4000 Ada Generation]", "model": "RTX 4000 ADA", "vendor": "NVIDIA", "device_id": "10de:27b2"},
            {"name": "GH100 [H100]", "model": "H100", "vendor": "NVIDIA", "device_id": "10de:2336"},
            {"name": "GH100 [H100 NVSwitch]", "model": "H100", "vendor": "NVIDIA", "device_id": "10de:22a3"},
            {"name": "GH100 [H100 CNX]", "model": "H100", "vendor": "NVIDIA", "device_id": "10de:2313"},
            {"name": "GH100 [H100 SXM5 80GB]", "model": "H100", "vendor": "NVIDIA", "device_id": "10de:2330"},
            {"name": "GH100 [H100 PCIe]", "model": "H100", "vendor": "NVIDIA", "device_id": "10de:2331"},
            {"name": "GA100", "model": "A100", "vendor": "NVIDIA", "device_id": "10de:2080"},
            {"name": "GA100", "model": "A100", "vendor": "NVIDIA", "device_id": "10de:2081"},
            {"name": "GA100 [A100 SXM4 80GB]", "model": "A100", "vendor": "NVIDIA", "device_id": "10de:20b2"},
            {"name": "GA100 [A100 PCIe 80GB]", "model": "A100", "vendor": "NVIDIA", "device_id": "10de:20b5"},
            {"name": "GA100 [A100X]", "model": "A100", "vendor": "NVIDIA", "device_id": "10de:20b8"}
        ],
        "community_wallet_address": "0x5aBd3258C5492fD378EBC2e0017416E199e5Da56",
        "community_wallet_timestamp": 1_739_301_770i64
    })
});

// --- Cutoff timestamps -----------------------------------------------------

pub const STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT: i64 = 22_196_000;
pub const STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP: i64 = 1_743_775_079;

/// Cutoff for hold and stream payment type messages. After this timestamp,
/// new messages with hold/stream payment types are rejected. (2026-03-11 UTC)
pub const HOLD_AND_STREAM_CUTOFF_TIMESTAMP: i64 = 1_773_187_200;

/// Cutoff for STORE messages requiring credit-only payment. (2027-01-01 UTC)
pub const CREDIT_ONLY_CUTOFF_TIMESTAMP: i64 = 1_798_761_600;

/// Credit precision change cutoff: 1 USD = 1,000,000 credits (previously 100).
/// (2026-02-02 UTC)
pub const CREDIT_PRECISION_CUTOFF_TIMESTAMP: i64 = 1_769_990_400;
pub const CREDIT_PRECISION_MULTIPLIER: i64 = 10_000;

// --- File size + cost limits ----------------------------------------------

pub const DEFAULT_MAX_FILE_SIZE: u64 = 100 * MIB;
pub const DEFAULT_MAX_UPLOAD_FILE_SIZE: u64 = GIB;
pub const DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE: u64 = 25 * MIB;
/// Minimum MiB cost for pure STORE messages.
pub const MIN_STORE_COST_MIB: u64 = 25;
/// Minimum cost per hour in credits for instances and volumes.
pub const MIN_CREDIT_COST_PER_HOUR: u64 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_size_constants() {
        assert_eq!(KIB, 1024);
        assert_eq!(MIB, 1024 * 1024);
        assert_eq!(GIB, 1024u64.pow(3));
    }

    #[test]
    fn test_time_constants() {
        assert_eq!(MINUTE, 60);
        assert_eq!(HOUR, 3600);
        assert_eq!(DAY, 86400);
    }

    #[test]
    fn test_price_aggregate_has_all_product_types() {
        let agg = &*DEFAULT_PRICE_AGGREGATE;
        for v in [
            "program",
            "storage",
            "instance",
            "web3_hosting",
            "program_persistent",
            "instance_gpu_premium",
            "instance_confidential",
            "instance_gpu_standard",
        ] {
            assert!(agg.get(v).is_some(), "missing key {v}");
        }
    }

    #[test]
    fn test_product_price_type_serde() {
        assert_eq!(ProductPriceType::Storage.as_str(), "storage");
        assert_eq!(ProductPriceType::Web3Hosting.as_str(), "web3_hosting");
        let s = serde_json::to_string(&ProductPriceType::InstanceGpuPremium).unwrap();
        assert_eq!(s, "\"instance_gpu_premium\"");
        let v: ProductPriceType = serde_json::from_str("\"program_persistent\"").unwrap();
        assert_eq!(v, ProductPriceType::ProgramPersistent);
    }

    #[test]
    fn test_settings_aggregate_loads() {
        let agg = &*DEFAULT_SETTINGS_AGGREGATE;
        assert_eq!(
            agg["community_wallet_address"],
            "0x5aBd3258C5492fD378EBC2e0017416E199e5Da56"
        );
        let gpus = agg["compatible_gpus"].as_array().unwrap();
        assert_eq!(gpus.len(), 19);
    }

    #[test]
    fn test_cutoffs_are_consistent_with_python() {
        assert_eq!(STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT, 22_196_000);
        assert_eq!(HOLD_AND_STREAM_CUTOFF_TIMESTAMP, 1_773_187_200);
        assert_eq!(CREDIT_ONLY_CUTOFF_TIMESTAMP, 1_798_761_600);
        assert_eq!(CREDIT_PRECISION_CUTOFF_TIMESTAMP, 1_769_990_400);
        assert_eq!(CREDIT_PRECISION_MULTIPLIER, 10_000);
    }
}
