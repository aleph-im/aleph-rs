use serde::{Deserialize, Serialize};

/// How to round when a conversion isn’t an exact integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rounding {
    Floor,   // toward zero
    Ceil,    // away from zero
    Nearest, // ties to even
}

/// Error for failed conversions (overflow, rounding disallowed, etc.)
#[derive(thiserror::Error, Debug)]
pub enum MemConvError {
    #[error("conversion overflowed u64 range")]
    Overflow,
    #[error("zero not allowed for this type")]
    ZeroNotAllowed, // for NonZero variants if you add them
}

/// Core trait all memory-size newtypes implement.
/// The *semantic value* is “count of this unit”, stored as an integer.
/// Each type specifies its bytes-per-unit as a `u64` constant.
pub trait MemorySize: Sized + Copy {
    /// Count of units (e.g., “5 MiB” => 5).
    fn units(self) -> u64;

    /// Convert value to `f64` (for convenience).
    fn as_f64(self) -> f64 {
        self.units() as f64
    }

    /// Construct from a count of units (no validation besides overflow domain).
    fn from_units(units: u64) -> Self;

    /// Exact bytes per 1 unit of this type (e.g., MiB = 1_048_576).
    const BYTES_PER_UNIT: u64;

    /// Convert to raw bytes as a `Bytes` newtype (checked multiply).
    fn to_bytes(self) -> Result<Bytes, MemConvError> {
        Bytes::from_units_checked(
            self.units()
                .checked_mul(Self::BYTES_PER_UNIT)
                .ok_or(MemConvError::Overflow)?,
        )
    }

    /// Convert to another memory unit with **rounding**.
    ///
    /// This performs: `self.bytes() / T::BYTES_PER_UNIT`, rounded as requested.
    fn to_rounded<T: MemorySize>(self, mode: Rounding) -> Result<T, MemConvError> {
        let b = self.to_bytes()?.units();
        let d = T::BYTES_PER_UNIT;

        let (q, r) = (b / d, b % d);
        let add = match mode {
            Rounding::Floor => 0,
            Rounding::Ceil => (r > 0) as u64,
            Rounding::Nearest => {
                // ties-to-even on q
                let twice_r = r.checked_mul(2).ok_or(MemConvError::Overflow)?;
                if twice_r > d || (twice_r == d && q % 2 == 1) {
                    1
                } else {
                    0
                }
            }
        };

        let units = q.checked_add(add).ok_or(MemConvError::Overflow)?;
        Ok(T::from_units(units))
    }

    /// Convert to another unit **only if exact** (no remainder).
    fn to_exact<T: MemorySize>(self) -> Option<T> {
        let b = self.to_bytes().ok()?.units();
        if b % T::BYTES_PER_UNIT == 0 {
            Some(T::from_units(b / T::BYTES_PER_UNIT))
        } else {
            None
        }
    }

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.units().checked_add(rhs.units()).map(Self::from_units)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.units().checked_sub(rhs.units()).map(Self::from_units)
    }

    fn saturating_add(self, rhs: Self) -> Self {
        Self::from_units(self.units().saturating_add(rhs.units()))
    }

    fn saturating_sub(self, rhs: Self) -> Self {
        Self::from_units(self.units().saturating_sub(rhs.units()))
    }
}

/// Canonical base type: raw bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Bytes(u64);

impl MemorySize for Bytes {
    #[inline]
    fn units(self) -> u64 {
        self.0
    }
    #[inline]
    fn from_units(units: u64) -> Self {
        Self(units)
    }
    const BYTES_PER_UNIT: u64 = 1;
}

impl From<u64> for Bytes {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Bytes {
    /// Additional constructor that can catch domain rules if you ever add them.
    fn from_units_checked(units: u64) -> Result<Self, MemConvError> {
        Ok(Self(units))
    }
}

/// Helper macro to declare a new memory unit and implement `MemorySize` + basic From/TryFrom.
macro_rules! mem_unit {
    ($name:ident, $bytes_per_unit:expr) => {
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(u64);
        impl MemorySize for $name {
            #[inline]
            fn units(self) -> u64 {
                self.0
            }
            #[inline]
            fn from_units(units: u64) -> Self {
                Self(units)
            }
            const BYTES_PER_UNIT: u64 = $bytes_per_unit;
        }
        // Convenience conversions to/from Bytes with exactness guarantees where appropriate.
        impl From<$name> for Bytes {
            fn from(v: $name) -> Bytes {
                v.to_bytes().expect("u64 overflow in to_bytes")
            }
        }
        impl TryFrom<Bytes> for $name {
            type Error = MemConvError;
            fn try_from(b: Bytes) -> Result<Self, Self::Error> {
                if b.units() % Self::BYTES_PER_UNIT == 0 {
                    Ok(Self(b.units() / Self::BYTES_PER_UNIT))
                } else {
                    Err(MemConvError::Overflow) // “not exactly divisible”; reuse Overflow for brevity
                }
            }
        }
        impl From<u64> for $name {
            fn from(v: u64) -> Self {
                Self(v)
            }
        }
    };
}

// Binary IEC units
mem_unit!(KiB, 1024u64);
mem_unit!(MiB, 1024u64 * 1024);
mem_unit!(GiB, 1024u64 * 1024 * 1024);

// Decimal SI units
mem_unit!(KB, 1000u64);
mem_unit!(MB, 1000u64 * 1000);
mem_unit!(GB, 1000u64 * 1000 * 1000);

/// Convert Gigabytes to Mebibytes (the unit used for VM volumes).
/// ounds up to ensure that data of a given size will fit in the space allocated.
pub const fn gigabyte_to_mebibyte(gb: u64) -> u64 {
    let mebibyte = (1 << 20) as f64;
    let gigabyte = 1_000_000_000f64;

    let result = gb as f64 * gigabyte / mebibyte;
    result.ceil() as u64
}

/// -------------
/// Usage sample:
/// -------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_bytes() {
        let m = MiB::from_units(5);
        let b = m.to_bytes().unwrap();
        assert_eq!(b.units(), 5 * MiB::BYTES_PER_UNIT);
        assert_eq!(m, b.to_exact::<MiB>().unwrap());
    }

    #[test]
    fn to_exact_and_rounded() {
        let g = GiB::from_units(2); // 2 GiB
        assert_eq!(g.to_exact::<MiB>().unwrap().units(), 2048);

        let two_gib_in_mb_floor = g.to_rounded::<MB>(Rounding::Floor).unwrap();
        let two_gib_in_mb_ceil = g.to_rounded::<MB>(Rounding::Ceil).unwrap();
        assert!(two_gib_in_mb_ceil.units() >= two_gib_in_mb_floor.units());
    }

    #[test]
    fn overflow_guard() {
        // A very large value that would overflow when multiplied by BYTES_PER_UNIT
        let big = GiB::from_units(u64::MAX / GiB::BYTES_PER_UNIT + 1);
        assert!(matches!(big.to_bytes(), Err(MemConvError::Overflow)));
    }

    #[test]
    fn test_gigabyte_to_mebibyte() {
        let mib = gigabyte_to_mebibyte(20);
        assert_eq!(mib, 19074);
    }

    #[test]
    fn test_bytes_checked_add() {
        let size = Bytes::from_units(100);

        // Test adding zero
        assert_eq!(
            size.checked_add(Bytes::from_units(0)),
            Some(Bytes::from_units(100))
        );

        // Test adding non-zero
        assert_eq!(
            size.checked_add(Bytes::from_units(50)),
            Some(Bytes::from_units(150))
        );

        // Test overflow
        let max_size = Bytes::from_units(u64::MAX);
        assert_eq!(max_size.checked_add(Bytes::from_units(1)), None);
    }

    #[test]
    fn test_bytes_checked_sub() {
        let size = Bytes::from_units(100);

        // Test subtracting zero
        assert_eq!(
            size.checked_sub(Bytes::from_units(0)),
            Some(Bytes::from_units(100))
        );

        // Test subtracting non-zero
        assert_eq!(
            size.checked_sub(Bytes::from_units(50)),
            Some(Bytes::from_units(50))
        );

        // Test underflow
        assert_eq!(size.checked_sub(Bytes::from_units(150)), None);
    }

    #[test]
    fn test_bytes_saturating_add() {
        let size = Bytes::from_units(100);

        // Test normal addition
        assert_eq!(
            size.saturating_add(Bytes::from_units(50)),
            Bytes::from_units(150)
        );

        // Test overflow
        let max_size = Bytes::from_units(u64::MAX);
        assert_eq!(
            max_size.saturating_add(Bytes::from_units(1)),
            Bytes::from_units(u64::MAX)
        );
    }

    #[test]
    fn test_bytes_saturating_sub() {
        let size = Bytes::from_units(100);

        // Test normal subtraction
        assert_eq!(
            size.saturating_sub(Bytes::from_units(50)),
            Bytes::from_units(50)
        );

        // Test underflow
        assert_eq!(
            size.saturating_sub(Bytes::from_units(150)),
            Bytes::from_units(0)
        );
    }
}
