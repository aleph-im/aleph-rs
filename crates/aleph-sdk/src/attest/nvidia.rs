//! Minimum NVIDIA driver ("floor") enforcement for confidential-GPU
//! V-Programs. The floor is checked against the runtime manifest's
//! `gpu.driver_version` before a `vprogram call` talks to a GPU workload;
//! an outdated driver can leak plaintext across the PCIe link even inside
//! an otherwise-verified SEV-SNP guest.

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use super::AttestError;

/// A driver version, e.g. `595.71.05`. Always normalized to 3 components so
/// `595.71 == 595.71.0` compares equal and orders correctly.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DriverVersion(Vec<u32>);

impl FromStr for DriverVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 2 && parts.len() != 3 {
            return Err(format!(
                "driver version must be 2 or 3 dot-separated numeric components, got {s:?}"
            ));
        }
        let mut components = parts
            .iter()
            .map(|p| {
                p.parse::<u32>()
                    .map_err(|_| format!("driver version component must be numeric, got {s:?}"))
            })
            .collect::<Result<Vec<u32>, String>>()?;
        if components.len() == 2 {
            components.push(0);
        }
        Ok(DriverVersion(components))
    }
}

impl fmt::Display for DriverVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.0[0], self.0[1], self.0[2])
    }
}

/// A minimum NVIDIA driver policy: the lowest accepted driver version, and
/// which GPU architectures are accepted at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NvidiaFloor {
    pub min_driver: DriverVersion,
    pub accepted_archs: Vec<String>,
}

impl NvidiaFloor {
    /// Conservative, release-time known-good floor: driver 580.0.0, both
    /// architectures accepted. A "floor of floors": the settings aggregate
    /// can only raise it.
    pub fn builtin_baseline() -> NvidiaFloor {
        NvidiaFloor {
            min_driver: DriverVersion(vec![580, 0, 0]),
            accepted_archs: vec!["hopper".to_string(), "blackwell".to_string()],
        }
    }

    /// Fold `other` into this floor: the higher driver version wins, and
    /// the accepted architectures narrow to the intersection when `other`
    /// names any (an empty `other` list leaves this floor's architectures
    /// untouched). Never lowers either constraint.
    pub fn raise_to(&self, other: &NvidiaFloor) -> NvidiaFloor {
        let min_driver = match self.min_driver.cmp(&other.min_driver) {
            Ordering::Less => other.min_driver.clone(),
            _ => self.min_driver.clone(),
        };
        let accepted_archs = if other.accepted_archs.is_empty() {
            self.accepted_archs.clone()
        } else {
            self.accepted_archs
                .iter()
                .filter(|a| other.accepted_archs.contains(a))
                .cloned()
                .collect()
        };
        NvidiaFloor {
            min_driver,
            accepted_archs,
        }
    }

    /// Check a runtime manifest's `gpu.driver_version` and the message's GPU
    /// `arch` against this floor.
    pub fn check(&self, driver_version: &str, arch: &str) -> Result<(), AttestError> {
        if !self.accepted_archs.iter().any(|a| a == arch) {
            return Err(AttestError::GpuArchNotAccepted(arch.to_string()));
        }
        let got: DriverVersion = driver_version
            .parse()
            .map_err(|_| AttestError::GpuDriverUnparsable(driver_version.to_string()))?;
        if got < self.min_driver {
            return Err(AttestError::GpuDriverBelowFloor {
                floor: self.min_driver.to_string(),
                got: driver_version.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_version_parses_two_and_three_components() {
        assert_eq!(
            "580.0.0".parse::<DriverVersion>().unwrap(),
            "580.0".parse::<DriverVersion>().unwrap()
        );
        assert_eq!(
            "595.71.05".parse::<DriverVersion>().unwrap().to_string(),
            "595.71.5"
        );
    }

    #[test]
    fn driver_version_rejects_bad_input() {
        assert!("580".parse::<DriverVersion>().is_err());
        assert!("580.0.0.0".parse::<DriverVersion>().is_err());
        assert!("a.b".parse::<DriverVersion>().is_err());
        assert!("".parse::<DriverVersion>().is_err());
    }

    #[test]
    fn driver_version_orders_numerically_not_lexically() {
        assert!(
            "580.9".parse::<DriverVersion>().unwrap() < "580.10".parse::<DriverVersion>().unwrap()
        );
        assert!(
            "580.71".parse::<DriverVersion>().unwrap() < "595.0".parse::<DriverVersion>().unwrap()
        );
    }

    #[test]
    fn builtin_baseline_is_580_and_both_archs() {
        let floor = NvidiaFloor::builtin_baseline();
        assert_eq!(floor.min_driver.to_string(), "580.0.0");
        assert_eq!(floor.accepted_archs, vec!["hopper", "blackwell"]);
    }

    #[test]
    fn raise_to_takes_the_higher_driver_and_never_lowers() {
        let a = NvidiaFloor {
            min_driver: "595.0".parse().unwrap(),
            accepted_archs: vec!["hopper".into(), "blackwell".into()],
        };
        let lower = NvidiaFloor {
            min_driver: "580.0".parse().unwrap(),
            accepted_archs: vec![],
        };
        let raised = a.raise_to(&lower);
        assert_eq!(
            raised.min_driver, a.min_driver,
            "a lower other must not win"
        );

        let higher = NvidiaFloor {
            min_driver: "600.0".parse().unwrap(),
            accepted_archs: vec![],
        };
        let raised = a.raise_to(&higher);
        assert_eq!(raised.min_driver, higher.min_driver);
    }

    #[test]
    fn raise_to_intersects_archs_only_when_other_names_any() {
        let a = NvidiaFloor {
            min_driver: "580.0".parse().unwrap(),
            accepted_archs: vec!["hopper".into(), "blackwell".into()],
        };
        let empty_other = NvidiaFloor {
            min_driver: "580.0".parse().unwrap(),
            accepted_archs: vec![],
        };
        assert_eq!(a.raise_to(&empty_other).accepted_archs, a.accepted_archs);

        let narrowing = NvidiaFloor {
            min_driver: "580.0".parse().unwrap(),
            accepted_archs: vec!["hopper".into()],
        };
        assert_eq!(a.raise_to(&narrowing).accepted_archs, vec!["hopper"]);
    }

    fn floor() -> NvidiaFloor {
        NvidiaFloor {
            min_driver: "595.0".parse().unwrap(),
            accepted_archs: vec!["hopper".into(), "blackwell".into()],
        }
    }

    #[test]
    fn check_accepts_a_driver_at_or_above_the_floor() {
        assert!(floor().check("595.0.0", "hopper").is_ok());
        assert!(floor().check("600.71.05", "blackwell").is_ok());
    }

    #[test]
    fn check_rejects_a_driver_below_the_floor_naming_floor_and_value() {
        let err = floor().check("580.0.0", "hopper").unwrap_err();
        match err {
            AttestError::GpuDriverBelowFloor { floor, got } => {
                assert_eq!(floor, "595.0.0");
                assert_eq!(got, "580.0.0");
            }
            other => panic!("expected GpuDriverBelowFloor, got: {other:?}"),
        }
    }

    #[test]
    fn check_rejects_an_arch_the_floor_does_not_accept() {
        let narrow = NvidiaFloor {
            min_driver: "580.0".parse().unwrap(),
            accepted_archs: vec!["hopper".into()],
        };
        let err = narrow.check("600.0.0", "blackwell").unwrap_err();
        assert!(matches!(err, AttestError::GpuArchNotAccepted(a) if a == "blackwell"));
    }

    #[test]
    fn check_rejects_an_unparsable_driver_version() {
        let err = floor().check("not-a-version", "hopper").unwrap_err();
        assert!(matches!(err, AttestError::GpuDriverUnparsable(v) if v == "not-a-version"));
    }
}
