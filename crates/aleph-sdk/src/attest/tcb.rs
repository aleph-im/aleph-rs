//! Minimum-TCB ("floor") enforcement for SEV-SNP attestation. See
//! docs/plans/2026-08-12-snp-tcb-floor-design.md. The floor is compared
//! componentwise against every TCB view in the report; the host is
//! adversarial, so this client-side check is the enforcement point.

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

#[cfg(test)]
mod tests {
    use super::*;
    use sev::firmware::host::TcbVersion;

    fn tcb(bootloader: u8, tee: u8, snp: u8, microcode: u8) -> TcbVersion {
        let mut v = TcbVersion::default();
        v.fmc = None;
        v.bootloader = bootloader;
        v.tee = tee;
        v.snp = snp;
        v.microcode = microcode;
        v
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
}
