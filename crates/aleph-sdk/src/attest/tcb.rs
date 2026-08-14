//! Minimum-TCB ("floor") enforcement for SEV-SNP attestation. See
//! aleph-vm docs/plans/2026-08-12-snp-tcb-floor-design.md. The floor is
//! compared componentwise against every TCB view in the report; the host is
//! adversarial, so this client-side check is the enforcement point.

use std::str::FromStr;

use super::verify::AmdProduct;
use sev::firmware::host::TcbVersion;

/// A single SEV-SNP TCB component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Component {
    Fmc,
    Bootloader,
    Tee,
    Snp,
    Microcode,
}

/// One component of a report's TCB that fell below the floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deficiency {
    pub component: Component,
    pub required: u8,
    pub actual: u8,
}

/// A minimum SEV-SNP TCB. `fmc` is Turin-only: `None` means "not part of this
/// generation's TCB" and is not compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcbFloor {
    pub fmc: Option<u8>,
    pub bootloader: u8,
    pub tee: u8,
    pub snp: u8,
    pub microcode: u8,
}

impl TcbFloor {
    /// A floor that accepts any report. For tests and behavior-preserving
    /// call-site updates only; never used in the attested-call path.
    pub const UNRESTRICTED: TcbFloor = TcbFloor {
        fmc: None,
        bootloader: 0,
        tee: 0,
        snp: 0,
        microcode: 0,
    };

    /// Componentwise `tcb.component >= floor.component`. Returns *every*
    /// deficient component so the error names all of them. `fmc` is checked
    /// only when this floor sets it (a report's missing `fmc` counts as 0).
    pub fn satisfied_by(&self, tcb: &TcbVersion) -> Result<(), Vec<Deficiency>> {
        let mut out = Vec::new();
        if let Some(required) = self.fmc {
            let actual = tcb.fmc.unwrap_or(0);
            if actual < required {
                out.push(Deficiency {
                    component: Component::Fmc,
                    required,
                    actual,
                });
            }
        }
        for (component, required, actual) in [
            (Component::Bootloader, self.bootloader, tcb.bootloader),
            (Component::Tee, self.tee, tcb.tee),
            (Component::Snp, self.snp, tcb.snp),
            (Component::Microcode, self.microcode, tcb.microcode),
        ] {
            if actual < required {
                out.push(Deficiency {
                    component,
                    required,
                    actual,
                });
            }
        }
        if out.is_empty() { Ok(()) } else { Err(out) }
    }

    /// Componentwise max of two full floors (folds baseline with aggregate).
    /// `Option<u8>: Ord` gives `None < Some`, so a set `fmc` always wins.
    pub fn raise_to(&self, other: &TcbFloor) -> TcbFloor {
        TcbFloor {
            fmc: self.fmc.max(other.fmc),
            bootloader: self.bootloader.max(other.bootloader),
            tee: self.tee.max(other.tee),
            snp: self.snp.max(other.snp),
            microcode: self.microcode.max(other.microcode),
        }
    }
}

/// Conservative, release-time known-good floor per AMD generation. A "floor of
/// floors": the settings aggregate can only raise it. Bump each SDK release as
/// new CVEs land. NOTE: confirm these against AMD's current published TCB
/// before a release; the values below are the design-doc baselines.
///
/// This is the floor for the product's *classic* server parts. Some products
/// cover more than one silicon family whose microcode SPL sequences are not
/// comparable; use [`builtin_baseline_policy`] to get every family's floor.
pub fn builtin_baseline(product: AmdProduct) -> TcbFloor {
    match product {
        AmdProduct::Milan => TcbFloor {
            fmc: None,
            bootloader: 4,
            tee: 0,
            snp: 22,
            microcode: 213,
        },
        AmdProduct::Genoa => TcbFloor {
            fmc: None,
            bootloader: 9,
            tee: 0,
            snp: 21,
            microcode: 84,
        },
        AmdProduct::Turin => TcbFloor {
            fmc: Some(3),
            bootloader: 4,
            tee: 0,
            snp: 8,
            microcode: 12,
        },
    }
}

/// CPUID family (19h = Zen 3/Zen 4 server) and the model range of the Zen4c
/// parts (Bergamo EPYC 97x4, Siena EPYC 8004: models A0h-AFh). Both attest
/// under the *Genoa* KDS product (same ARK/ASK, same VCEK product name), but
/// their x86 microcode is a different patch line than classic Genoa (models
/// 10h-1Fh), so the two families' microcode SPLs are not comparable: current
/// classic-Genoa microcode sits in the 8x range while current Zen4c microcode
/// sits in the 2x range. A single product-keyed microcode floor therefore
/// cannot be right for both.
const ZEN4_FAMILY: u8 = 0x19;
const ZEN4C_MODEL_RANGE: std::ops::RangeInclusive<u8> = 0xA0..=0xAF;

/// The minimum-TCB policy for one AMD product: the floor for its classic
/// parts plus, where the product spans a second silicon family with its own
/// microcode line, that family's floor. The floor to enforce is selected per
/// report via [`TcbFloorPolicy::for_model`], from the report's own CPUID
/// family/model fields (present and signed since report version 3,
/// SNP firmware 1.55).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcbFloorPolicy {
    /// Floor for the product's classic parts, and the fallback whenever the
    /// report does not identify the silicon (pre-v3 reports). The fallback is
    /// deliberately the strict classic floor: the SNP SVN floors already
    /// imply firmware recent enough to emit v3 reports, so a report without
    /// CPUID fields comes from firmware that fails the floor anyway.
    pub default: TcbFloor,
    /// Floor for the product's Zen4c family (Bergamo/Siena), when the
    /// product has one. `None` means every part of this product shares
    /// `default`.
    pub zen4c: Option<TcbFloor>,
}

impl TcbFloorPolicy {
    /// A policy that accepts any report. For tests and behavior-preserving
    /// call-site updates only; never used in the attested-call path.
    pub const UNRESTRICTED: TcbFloorPolicy = TcbFloorPolicy {
        default: TcbFloor::UNRESTRICTED,
        zen4c: None,
    };

    /// A policy applying one floor to every silicon family of the product.
    pub fn uniform(floor: TcbFloor) -> TcbFloorPolicy {
        TcbFloorPolicy {
            default: floor,
            zen4c: None,
        }
    }

    /// Select the floor to enforce for a report, from the report's CPUID
    /// family/model fields (`None` on pre-v3 reports).
    pub fn for_model(&self, cpuid_family: Option<u8>, cpuid_model: Option<u8>) -> &TcbFloor {
        if let (Some(zen4c), Some(ZEN4_FAMILY), Some(model)) =
            (self.zen4c.as_ref(), cpuid_family, cpuid_model)
            && ZEN4C_MODEL_RANGE.contains(&model)
        {
            return zen4c;
        }
        &self.default
    }

    /// Raise every family's floor by `other` (folds the network floor in).
    pub fn raise_to(&self, other: &TcbFloor) -> TcbFloorPolicy {
        TcbFloorPolicy {
            default: self.default.raise_to(other),
            zen4c: self.zen4c.map(|f| f.raise_to(other)),
        }
    }
}

/// [`builtin_baseline`] for every silicon family of the product. Genoa is the
/// only product spanning two microcode lines: classic Genoa (models 10h-1Fh)
/// and the Zen4c parts Bergamo/Siena (models A0h-AFh), which share the SP5
/// PSP firmware line (bootloader/tee/snp floors identical) but follow their
/// own x86 microcode sequence. NOTE: like the classic baselines, confirm the
/// Zen4c microcode value against AMD's current published patch level before a
/// release (current Zen4c microcode 0x0AA0021C -> SPL 0x1C = 28).
pub fn builtin_baseline_policy(product: AmdProduct) -> TcbFloorPolicy {
    let default = builtin_baseline(product);
    match product {
        AmdProduct::Genoa => TcbFloorPolicy {
            default,
            zen4c: Some(TcbFloor {
                microcode: 28,
                ..default
            }),
        },
        AmdProduct::Milan | AmdProduct::Turin => TcbFloorPolicy {
            default,
            zen4c: None,
        },
    }
}

/// A partial `--min-tcb` patch: only the named components. Unnamed components
/// keep the network floor value when applied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TcbFloorOverride {
    fmc: Option<u8>,
    bootloader: Option<u8>,
    tee: Option<u8>,
    snp: Option<u8>,
    microcode: Option<u8>,
}

impl TcbFloorOverride {
    pub fn is_empty(&self) -> bool {
        self.fmc.is_none()
            && self.bootloader.is_none()
            && self.tee.is_none()
            && self.snp.is_none()
            && self.microcode.is_none()
    }

    /// Apply this patch onto `network`. Returns the effective floor (named
    /// components set to the patch value, others kept from `network`) and the
    /// list of components the patch set strictly below `network`.
    pub fn apply_to(&self, network: &TcbFloor) -> (TcbFloor, Vec<Component>) {
        let mut eff = *network;
        let mut lowered = Vec::new();
        let mut set = |component: Component, patch: Option<u8>, field: &mut u8, net: u8| {
            if let Some(v) = patch {
                if v < net {
                    lowered.push(component);
                }
                *field = v;
            }
        };
        set(
            Component::Bootloader,
            self.bootloader,
            &mut eff.bootloader,
            network.bootloader,
        );
        set(Component::Tee, self.tee, &mut eff.tee, network.tee);
        set(Component::Snp, self.snp, &mut eff.snp, network.snp);
        set(
            Component::Microcode,
            self.microcode,
            &mut eff.microcode,
            network.microcode,
        );
        if let Some(v) = self.fmc {
            let net = network.fmc.unwrap_or(0);
            if v < net {
                lowered.push(Component::Fmc);
            }
            eff.fmc = Some(v);
        }
        (eff, lowered)
    }
}

impl FromStr for TcbFloorOverride {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut out = TcbFloorOverride::default();
        for pair in s.split(',') {
            let (name, value) = pair
                .split_once('=')
                .ok_or_else(|| format!("expected component=value, got {pair:?}"))?;
            let value: u8 = value
                .trim()
                .parse()
                .map_err(|_| format!("component value must be 0-255, got {value:?}"))?;
            match name.trim() {
                "fmc" => out.fmc = Some(value),
                "bootloader" => out.bootloader = Some(value),
                "tee" => out.tee = Some(value),
                "snp" => out.snp = Some(value),
                "microcode" => out.microcode = Some(value),
                other => {
                    return Err(format!(
                        "unknown TCB component {other:?} (expected fmc, bootloader, tee, snp, microcode)"
                    ));
                }
            }
        }
        if out.is_empty() {
            return Err("--min-tcb needs at least one component=value pair".to_string());
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sev::firmware::host::TcbVersion;

    fn tcb(bootloader: u8, tee: u8, snp: u8, microcode: u8) -> TcbVersion {
        TcbVersion {
            fmc: None,
            bootloader,
            tee,
            snp,
            microcode,
        }
    }

    #[test]
    fn satisfied_when_every_component_meets_or_exceeds_floor() {
        let floor = TcbFloor {
            fmc: None,
            bootloader: 4,
            tee: 0,
            snp: 21,
            microcode: 84,
        };
        assert!(floor.satisfied_by(&tcb(4, 0, 21, 84)).is_ok()); // exact
        assert!(floor.satisfied_by(&tcb(9, 1, 30, 90)).is_ok()); // above
    }

    #[test]
    fn reports_every_deficient_component_not_just_the_first() {
        let floor = TcbFloor {
            fmc: None,
            bootloader: 4,
            tee: 0,
            snp: 21,
            microcode: 84,
        };
        let defs = floor.satisfied_by(&tcb(3, 0, 20, 84)).unwrap_err();
        assert_eq!(defs.len(), 2);
        assert!(
            defs.iter()
                .any(|d| d.component == Component::Bootloader && d.required == 4 && d.actual == 3)
        );
        assert!(
            defs.iter()
                .any(|d| d.component == Component::Snp && d.required == 21 && d.actual == 20)
        );
    }

    #[test]
    fn fmc_is_compared_only_when_the_floor_sets_it() {
        let mut turin = tcb(4, 0, 8, 12);
        turin.fmc = Some(2);
        // Floor requires fmc >= 3; report has fmc 2 -> deficient.
        let with_fmc = TcbFloor {
            fmc: Some(3),
            bootloader: 4,
            tee: 0,
            snp: 8,
            microcode: 12,
        };
        let defs = with_fmc.satisfied_by(&turin).unwrap_err();
        assert!(defs.iter().any(|d| d.component == Component::Fmc));
        // Same report, floor without fmc -> fmc ignored, passes.
        let no_fmc = TcbFloor {
            fmc: None,
            bootloader: 4,
            tee: 0,
            snp: 8,
            microcode: 12,
        };
        assert!(no_fmc.satisfied_by(&turin).is_ok());
    }

    #[test]
    fn raise_to_is_componentwise_max_and_prefers_some_fmc() {
        let a = TcbFloor {
            fmc: None,
            bootloader: 4,
            tee: 0,
            snp: 21,
            microcode: 90,
        };
        let b = TcbFloor {
            fmc: Some(3),
            bootloader: 9,
            tee: 0,
            snp: 8,
            microcode: 84,
        };
        let m = a.raise_to(&b);
        assert_eq!(
            m,
            TcbFloor {
                fmc: Some(3),
                bootloader: 9,
                tee: 0,
                snp: 21,
                microcode: 90
            }
        );
    }

    #[test]
    fn unrestricted_accepts_any_report() {
        assert!(
            TcbFloor::UNRESTRICTED
                .satisfied_by(&tcb(0, 0, 0, 0))
                .is_ok()
        );
    }

    #[test]
    fn builtin_baseline_is_nonzero_per_generation() {
        assert!(builtin_baseline(AmdProduct::Genoa).snp > 0);
        assert!(builtin_baseline(AmdProduct::Turin).fmc.is_some());
    }

    #[test]
    fn genoa_policy_selects_the_zen4c_floor_for_bergamo_siena_models() {
        let policy = builtin_baseline_policy(AmdProduct::Genoa);
        // Bergamo/Siena: family 19h, models A0h-AFh.
        for model in [0xA0, 0xA7, 0xAF] {
            let floor = policy.for_model(Some(0x19), Some(model));
            assert_eq!(floor, policy.zen4c.as_ref().unwrap());
        }
        // Classic Genoa models keep the classic floor.
        for model in [0x10, 0x11, 0x1F] {
            assert_eq!(policy.for_model(Some(0x19), Some(model)), &policy.default);
        }
        // Zen4c microcode line sits far below classic Genoa's; the PSP-line
        // components are shared.
        let zen4c = policy.zen4c.unwrap();
        assert!(zen4c.microcode < policy.default.microcode);
        assert_eq!(zen4c.bootloader, policy.default.bootloader);
        assert_eq!(zen4c.snp, policy.default.snp);
    }

    #[test]
    fn policy_falls_back_to_the_default_floor_without_cpuid_fields() {
        // Pre-v3 reports carry no CPUID family/model: the strict classic
        // floor applies (conservative fallback).
        let policy = builtin_baseline_policy(AmdProduct::Genoa);
        assert_eq!(policy.for_model(None, None), &policy.default);
        assert_eq!(policy.for_model(Some(0x19), None), &policy.default);
        assert_eq!(policy.for_model(None, Some(0xA1)), &policy.default);
        // A Zen4c-range model on a DIFFERENT family is not Zen4c.
        assert_eq!(policy.for_model(Some(0x1A), Some(0xA1)), &policy.default);
    }

    #[test]
    fn products_without_a_zen4c_family_always_use_the_default_floor() {
        for product in [AmdProduct::Milan, AmdProduct::Turin] {
            let policy = builtin_baseline_policy(product);
            assert!(policy.zen4c.is_none());
            assert_eq!(policy.for_model(Some(0x19), Some(0xA1)), &policy.default);
            assert_eq!(policy.default, builtin_baseline(product));
        }
    }

    #[test]
    fn policy_raise_to_raises_every_family_floor() {
        let policy = builtin_baseline_policy(AmdProduct::Genoa);
        let network = TcbFloor {
            fmc: None,
            bootloader: 10,
            tee: 1,
            snp: 25,
            microcode: 0,
        };
        let raised = policy.raise_to(&network);
        assert_eq!(raised.default.bootloader, 10);
        assert_eq!(raised.default.snp, 25);
        // The classic microcode floor is untouched (network 0 < 84)...
        assert_eq!(raised.default.microcode, policy.default.microcode);
        // ...and the zen4c floor got the same componentwise raise.
        let zen4c = raised.zen4c.unwrap();
        assert_eq!(zen4c.bootloader, 10);
        assert_eq!(zen4c.snp, 25);
        assert_eq!(zen4c.microcode, policy.zen4c.unwrap().microcode);
    }

    #[test]
    fn unrestricted_policy_accepts_any_report_for_any_model() {
        let policy = TcbFloorPolicy::UNRESTRICTED;
        assert!(
            policy
                .for_model(Some(0x19), Some(0xA1))
                .satisfied_by(&tcb(0, 0, 0, 0))
                .is_ok()
        );
    }

    #[test]
    fn override_parses_component_value_pairs() {
        let o: TcbFloorOverride = "snp=9,microcode=15".parse().unwrap();
        let net = TcbFloor {
            fmc: None,
            bootloader: 4,
            tee: 0,
            snp: 21,
            microcode: 84,
        };
        let (eff, lowered) = o.apply_to(&net);
        // named components replace; unnamed keep the network value
        assert_eq!(
            eff,
            TcbFloor {
                fmc: None,
                bootloader: 4,
                tee: 0,
                snp: 9,
                microcode: 15
            }
        );
        // both were lowered below the network floor
        assert_eq!(lowered.len(), 2);
        assert!(lowered.contains(&Component::Snp));
        assert!(lowered.contains(&Component::Microcode));
    }

    #[test]
    fn override_raising_reports_no_lowered_components() {
        let o: TcbFloorOverride = "snp=30".parse().unwrap();
        let net = TcbFloor {
            fmc: None,
            bootloader: 4,
            tee: 0,
            snp: 21,
            microcode: 84,
        };
        let (eff, lowered) = o.apply_to(&net);
        assert_eq!(eff.snp, 30);
        assert!(lowered.is_empty());
    }

    #[test]
    fn override_mixing_raise_and_lower_flags_only_the_lowered() {
        let o: TcbFloorOverride = "snp=30,microcode=15".parse().unwrap();
        let net = TcbFloor {
            fmc: None,
            bootloader: 4,
            tee: 0,
            snp: 21,
            microcode: 84,
        };
        let (eff, lowered) = o.apply_to(&net);
        assert_eq!(
            eff,
            TcbFloor {
                fmc: None,
                bootloader: 4,
                tee: 0,
                snp: 30,
                microcode: 15
            }
        );
        assert_eq!(lowered, vec![Component::Microcode]);
    }

    #[test]
    fn override_rejects_unknown_component_and_non_numeric_value() {
        assert!("bogus=1".parse::<TcbFloorOverride>().is_err());
        assert!("snp=notanum".parse::<TcbFloorOverride>().is_err());
        assert!("snp".parse::<TcbFloorOverride>().is_err());
    }

    #[test]
    fn empty_override_string_is_rejected() {
        assert!("".parse::<TcbFloorOverride>().is_err());
    }
}
