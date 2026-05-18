//! Cost-related helpers and cutoff checks.
//!
//! Port of `src/aleph/toolkit/costs.py`.
//!
//! The Python module operates on `MessageDb` instances directly. The
//! corresponding Rust DB model is not yet ported (see
//! `types/message_processing_result.rs`), so the public surface here accepts
//! the minimal fields each function actually uses (confirmation height and
//! message time). Once `MessageDb` lands, thin wrappers can adapt these
//! helpers without changing the underlying behaviour.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;

use super::constants::{
    CREDIT_ONLY_CUTOFF_TIMESTAMP, HOLD_AND_STREAM_CUTOFF_TIMESTAMP, PRICE_PRECISION,
    STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT, STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP,
};
use super::timestamp::timestamp_to_datetime;

/// A value usable as input to [`format_cost`] — either a `Decimal` or a string
/// containing a decimal literal. Mirrors Python's `Decimal | str` annotation.
pub enum CostInput<'a> {
    Decimal(Decimal),
    Str(&'a str),
}

impl<'a> From<Decimal> for CostInput<'a> {
    fn from(value: Decimal) -> Self {
        Self::Decimal(value)
    }
}

impl<'a> From<&'a str> for CostInput<'a> {
    fn from(value: &'a str) -> Self {
        Self::Str(value)
    }
}

impl<'a> From<&'a String> for CostInput<'a> {
    fn from(value: &'a String) -> Self {
        Self::Str(value.as_str())
    }
}

fn to_decimal(input: CostInput<'_>) -> Decimal {
    match input {
        CostInput::Decimal(d) => d,
        CostInput::Str(s) => Decimal::from_str(s)
            .or_else(|_| Decimal::from_str_exact(s))
            .unwrap_or_else(|e| {
                // Don't take down the worker on malformed input; mirror Python
                // by surfacing a zero cost and warning loudly.
                tracing::warn!("to_decimal: failed to parse cost string {s:?}: {e}");
                Decimal::ZERO
            }),
    }
}

/// Quantize a cost value to `p` digits of fractional precision, rounding
/// toward negative infinity (floor) — matching
/// `Decimal(v).quantize(Decimal(1)/Decimal(10**p), ROUND_FLOOR)`.
///
/// `p` defaults to [`PRICE_PRECISION`] in Python; callers can pass `None`.
pub fn format_cost<'a, I: Into<CostInput<'a>>>(v: I, p: Option<u32>) -> Decimal {
    let p = p.unwrap_or(PRICE_PRECISION);
    let d = to_decimal(v.into());
    d.round_dp_with_strategy(p, RoundingStrategy::ToNegativeInfinity)
}

/// Same as [`format_cost`] but returns a fixed-precision string,
/// always rendering exactly `p` fractional digits — matching
/// `"{:.{p}f}".format(n, p=p)`.
pub fn format_cost_str<'a, I: Into<CostInput<'a>>>(v: I, p: Option<u32>) -> String {
    let p = p.unwrap_or(PRICE_PRECISION);
    let n = format_cost(v, Some(p));
    format!("{:.*}", p as usize, n)
}

/// Inputs required by [`are_store_and_program_free`]. Mirrors the relevant
/// fields of `MessageDb`.
#[derive(Debug, Clone, Copy)]
pub struct StoreAndProgramFreeInput {
    /// Confirmation height, if any (Python: `confirmations[0].height`).
    pub confirmation_height: Option<i64>,
    /// Message time (Python: `message.time`).
    pub time: DateTime<Utc>,
}

/// True when STORE and PROGRAM message types are still free for the given
/// confirmation height/time. Mirrors `are_store_and_program_free`.
pub fn are_store_and_program_free(input: &StoreAndProgramFreeInput) -> bool {
    if let Some(height) = input.confirmation_height {
        height < STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT
    } else {
        input.time < timestamp_to_datetime(STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP as f64)
    }
}

/// True when a message requires credit-only payment based on its `time`.
/// Mirrors `is_credit_only_required`.
pub fn is_credit_only_required(time: DateTime<Utc>) -> bool {
    time >= timestamp_to_datetime(CREDIT_ONLY_CUTOFF_TIMESTAMP as f64)
}

/// True when hold and stream payment types are deprecated for a message with
/// the given `time`. Mirrors `is_hold_and_stream_deprecated`.
pub fn is_hold_and_stream_deprecated(time: DateTime<Utc>) -> bool {
    time >= timestamp_to_datetime(HOLD_AND_STREAM_CUTOFF_TIMESTAMP as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_cost_floors() {
        let v = format_cost("0.123456789012345678901", None);
        // 18 fractional digits, floored.
        assert_eq!(v.to_string(), "0.123456789012345678");
    }

    #[test]
    fn test_format_cost_with_precision() {
        let v = format_cost("1.99999", Some(2));
        assert_eq!(v.to_string(), "1.99");
    }

    #[test]
    fn test_format_cost_str_pads_zeros() {
        let s = format_cost_str("1", Some(4));
        assert_eq!(s, "1.0000");
        let s = format_cost_str("1.5", Some(2));
        assert_eq!(s, "1.50");
    }

    #[test]
    fn test_format_cost_str_default_precision() {
        let s = format_cost_str("0.5", None);
        assert!(s.starts_with("0.5"));
        assert_eq!(s.len(), 2 + PRICE_PRECISION as usize); // "0." + 18 digits
    }

    #[test]
    fn test_are_store_and_program_free_by_height() {
        // Below cutoff height => free.
        let input = StoreAndProgramFreeInput {
            confirmation_height: Some(STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT - 1),
            time: timestamp_to_datetime(STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP as f64 + 1.0),
        };
        assert!(are_store_and_program_free(&input));

        // At or above cutoff height => not free.
        let input = StoreAndProgramFreeInput {
            confirmation_height: Some(STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT),
            time: timestamp_to_datetime(0.0),
        };
        assert!(!are_store_and_program_free(&input));
    }

    #[test]
    fn test_are_store_and_program_free_by_time() {
        let input = StoreAndProgramFreeInput {
            confirmation_height: None,
            time: timestamp_to_datetime(STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP as f64 - 1.0),
        };
        assert!(are_store_and_program_free(&input));
        let input = StoreAndProgramFreeInput {
            confirmation_height: None,
            time: timestamp_to_datetime(STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP as f64),
        };
        assert!(!are_store_and_program_free(&input));
    }

    #[test]
    fn test_credit_only_required() {
        assert!(!is_credit_only_required(timestamp_to_datetime(
            CREDIT_ONLY_CUTOFF_TIMESTAMP as f64 - 1.0
        )));
        assert!(is_credit_only_required(timestamp_to_datetime(
            CREDIT_ONLY_CUTOFF_TIMESTAMP as f64
        )));
    }

    #[test]
    fn test_hold_and_stream_deprecated() {
        assert!(!is_hold_and_stream_deprecated(timestamp_to_datetime(
            HOLD_AND_STREAM_CUTOFF_TIMESTAMP as f64 - 1.0
        )));
        assert!(is_hold_and_stream_deprecated(timestamp_to_datetime(
            HOLD_AND_STREAM_CUTOFF_TIMESTAMP as f64
        )));
    }
}
