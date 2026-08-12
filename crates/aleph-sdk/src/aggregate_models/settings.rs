//! Models for the `settings` aggregate published by the aleph foundation at
//! [`SETTINGS_ADDRESS`]. Besides network-wide settings (community wallet, CRN
//! version, allocation signers, ...) it lists the GPU models compatible with
//! the network in `compatible_gpus`. The CLI uses that list to resolve a
//! user-provided `--gpu <model_id>` to the concrete PCI device properties that
//! go into an instance message, so no GPU data has to be hardcoded in the
//! client.

use crate::attest::{AmdProduct, TcbFloor};
use aleph_types::address;
use aleph_types::chain::Address;
use serde::Deserialize;
use std::sync::LazyLock;

/// Address that publishes the `settings` aggregate. Same foundation address as
/// the pricing and vm-images aggregates.
pub static SETTINGS_ADDRESS: LazyLock<Address> =
    LazyLock::new(|| address!("0xFba561a84A537fCaa567bb7A2257e7142701ae2A"));

/// Aggregate key for the settings data.
pub const SETTINGS_KEY: &str = "settings";

/// Top-level wrapper. `get_aggregate` deserializes the `data` field which
/// contains `{"settings": {...}}`.
#[derive(Debug, Clone, Deserialize)]
pub struct SettingsAggregate {
    pub settings: SettingsData,
}

/// Network-wide settings. Only the fields the SDK consumes are modeled; unknown
/// keys are ignored for forward-compatibility.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SettingsData {
    #[serde(default)]
    pub compatible_gpus: Vec<CompatibleGpu>,
    #[serde(default)]
    pub snp_min_tcb: SnpMinTcb,
}

/// Per-generation minimum SEV-SNP TCB from `settings.snp_min_tcb`. Absent
/// generations resolve to `None`; the client falls back to its builtin
/// baseline.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SnpMinTcb {
    #[serde(default)]
    pub milan: Option<TcbFloorDto>,
    #[serde(default)]
    pub genoa: Option<TcbFloorDto>,
    #[serde(default)]
    pub turin: Option<TcbFloorDto>,
}

/// Wire form of a floor in the aggregate.
#[derive(Debug, Clone, Deserialize)]
pub struct TcbFloorDto {
    #[serde(default)]
    pub fmc: Option<u8>,
    pub bootloader: u8,
    pub tee: u8,
    pub snp: u8,
    pub microcode: u8,
}

impl From<&TcbFloorDto> for TcbFloor {
    fn from(d: &TcbFloorDto) -> Self {
        TcbFloor {
            fmc: d.fmc,
            bootloader: d.bootloader,
            tee: d.tee,
            snp: d.snp,
            microcode: d.microcode,
        }
    }
}

impl SnpMinTcb {
    pub fn floor_for(&self, product: AmdProduct) -> Option<TcbFloor> {
        let dto = match product {
            AmdProduct::Milan => self.milan.as_ref(),
            AmdProduct::Genoa => self.genoa.as_ref(),
            AmdProduct::Turin => self.turin.as_ref(),
        };
        dto.map(TcbFloor::from)
    }
}

/// One GPU device variant compatible with the network, from
/// `settings.compatible_gpus`. Several entries can share the same model: one per
/// PCI device variant (e.g. "RTX 3090" and "RTX 3090 Ti" both have model
/// `"RTX 3090"` but distinct `device_id`s).
#[derive(Debug, Clone, Deserialize)]
pub struct CompatibleGpu {
    /// Stable machine identifier for the model and the value users pass to
    /// `--gpu` (e.g. `"rtx3090"`). Optional: aggregates published before this
    /// field was introduced omit it, in which case [`CompatibleGpu::model_id`]
    /// derives it from the model name.
    #[serde(default, rename = "model_id")]
    pub explicit_model_id: Option<String>,
    /// Human-readable model name (e.g. `"RTX 3090"`).
    pub model: String,
    /// Full device name (e.g. `"GA102 [GeForce RTX 3090]"`).
    pub name: String,
    /// GPU vendor (e.g. `"NVIDIA"`).
    pub vendor: String,
    /// PCI device class: `"0300"` (VGA compatible controller) or `"0302"`
    /// (3D controller). Optional: aggregates published before this field was
    /// introduced omit it.
    #[serde(default)]
    pub device_class: Option<String>,
    /// PCI `vendor:device` id (e.g. `"10de:2204"`).
    pub device_id: String,
}

impl CompatibleGpu {
    /// Canonical model id: the aggregate's explicit `model_id` when present,
    /// otherwise derived from the model name so the CLI keeps working before the
    /// aggregate carries explicit ids.
    pub fn model_id(&self) -> String {
        self.explicit_model_id
            .clone()
            .unwrap_or_else(|| derive_model_id(&self.model))
    }
}

impl SettingsData {
    /// All compatible GPU variants for a model id (matched against the canonical
    /// id, so the derived fallback resolves too), in aggregate order. The first
    /// element is the representative variant used when no specific node GPU is
    /// known (e.g. automatic placement).
    pub fn gpu_variants_for_model_id(&self, model_id: &str) -> Vec<&CompatibleGpu> {
        self.compatible_gpus
            .iter()
            .filter(|g| g.model_id() == model_id)
            .collect()
    }

    /// The canonical model id for a GPU model name, taken from a matching
    /// `compatible_gpus` entry (so an explicit `model_id` wins) and falling back
    /// to the derived form when the model is absent from the aggregate.
    pub fn model_id_for_name(&self, model_name: &str) -> String {
        self.compatible_gpus
            .iter()
            .find(|g| g.model == model_name)
            .map(CompatibleGpu::model_id)
            .unwrap_or_else(|| derive_model_id(model_name))
    }
}

/// Derive a model id from a GPU model name: lowercase, hyphen-joining the
/// whitespace-separated words, except that the leading prefix is joined directly
/// to an immediately-following number (so `"RTX 3090"` -> `"rtx3090"` and
/// `"H100"` -> `"h100"`, while `"RTX 4000 ADA"` -> `"rtx4000-ada"`,
/// `"RTX A5000"` -> `"rtx-a5000"`, `"RTX PRO 6000"` -> `"rtx-pro-6000"`). Used as
/// a fallback when the settings aggregate carries no explicit `model_id` for a
/// model, so the CLI keeps resolving `--gpu` before the aggregate is updated.
pub fn derive_model_id(model_name: &str) -> String {
    let words: Vec<String> = model_name
        .split_whitespace()
        .map(str::to_lowercase)
        .collect();
    let mut id = String::new();
    for (i, word) in words.iter().enumerate() {
        // No separator between the leading prefix (e.g. "rtx", "h") and a number
        // right after it; a hyphen everywhere else.
        let glue_to_number = i == 1 && word.starts_with(|c: char| c.is_ascii_digit());
        if i != 0 && !glue_to_number {
            id.push('-');
        }
        id.push_str(word);
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gpu(model_id: Option<&str>, model: &str, device_id: &str) -> CompatibleGpu {
        CompatibleGpu {
            explicit_model_id: model_id.map(str::to_string),
            model: model.into(),
            name: format!("{model} device"),
            vendor: "NVIDIA".into(),
            device_class: Some("0300".into()),
            device_id: device_id.into(),
        }
    }

    #[test]
    fn derive_model_id_glues_prefix_number_and_hyphenates_rest() {
        assert_eq!(derive_model_id("RTX 3090"), "rtx3090");
        assert_eq!(derive_model_id("H100"), "h100");
        assert_eq!(derive_model_id("L40S"), "l40s");
        assert_eq!(derive_model_id("RTX 4000 ADA"), "rtx4000-ada");
        assert_eq!(derive_model_id("RTX A5000"), "rtx-a5000");
        assert_eq!(derive_model_id("RTX PRO 6000"), "rtx-pro-6000");
        assert_eq!(
            derive_model_id("RTX PRO 6000 Blackwell Max-Q"),
            "rtx-pro-6000-blackwell-max-q"
        );
    }

    #[test]
    fn model_id_prefers_explicit_then_derives() {
        // The explicit id wins, e.g. a curated short form that drops "Blackwell".
        assert_eq!(
            gpu(
                Some("rtx-pro-6000-max-q"),
                "RTX PRO 6000 Blackwell Max-Q",
                "10de:2bb4"
            )
            .model_id(),
            "rtx-pro-6000-max-q"
        );
        assert_eq!(gpu(None, "RTX 3090", "10de:2204").model_id(), "rtx3090");
    }

    #[test]
    fn variants_for_model_id_preserve_order_and_match_derived() {
        let data = SettingsData {
            compatible_gpus: vec![
                gpu(None, "RTX 3090", "10de:2204"),
                gpu(None, "RTX 3090", "10de:2203"),
                gpu(None, "L40S", "10de:26b9"),
            ],
            snp_min_tcb: SnpMinTcb::default(),
        };
        let variants = data.gpu_variants_for_model_id("rtx3090");
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0].device_id, "10de:2204");
        assert_eq!(variants[1].device_id, "10de:2203");
    }

    #[test]
    fn model_id_for_name_uses_explicit_when_present() {
        let data = SettingsData {
            compatible_gpus: vec![gpu(
                Some("rtx-pro-6000-max-q"),
                "RTX PRO 6000 Blackwell Max-Q",
                "10de:2bb4",
            )],
            snp_min_tcb: SnpMinTcb::default(),
        };
        assert_eq!(
            data.model_id_for_name("RTX PRO 6000 Blackwell Max-Q"),
            "rtx-pro-6000-max-q"
        );
        // Absent model falls back to derivation.
        assert_eq!(data.model_id_for_name("RTX 3090"), "rtx3090");
    }

    #[test]
    fn parses_snp_min_tcb_and_resolves_per_generation() {
        let json = r#"{
      "settings": {
        "compatible_gpus": [],
        "snp_min_tcb": {
          "genoa": { "bootloader": 9, "tee": 0, "snp": 21, "microcode": 84 },
          "turin": { "fmc": 3, "bootloader": 4, "tee": 0, "snp": 8, "microcode": 12 }
        }
      }
    }"#;
        let agg: SettingsAggregate = serde_json::from_str(json).unwrap();
        let genoa = agg
            .settings
            .snp_min_tcb
            .floor_for(crate::attest::AmdProduct::Genoa)
            .unwrap();
        assert_eq!(
            genoa,
            crate::attest::TcbFloor {
                fmc: None,
                bootloader: 9,
                tee: 0,
                snp: 21,
                microcode: 84
            }
        );
        let turin = agg
            .settings
            .snp_min_tcb
            .floor_for(crate::attest::AmdProduct::Turin)
            .unwrap();
        assert_eq!(turin.fmc, Some(3));
        // Milan absent from this aggregate -> None (caller falls back to baseline).
        assert!(
            agg.settings
                .snp_min_tcb
                .floor_for(crate::attest::AmdProduct::Milan)
                .is_none()
        );
    }

    #[test]
    fn settings_without_snp_min_tcb_still_deserializes() {
        let json = r#"{ "settings": { "compatible_gpus": [] } }"#;
        let agg: SettingsAggregate = serde_json::from_str(json).unwrap();
        assert!(
            agg.settings
                .snp_min_tcb
                .floor_for(crate::attest::AmdProduct::Genoa)
                .is_none()
        );
    }
}
