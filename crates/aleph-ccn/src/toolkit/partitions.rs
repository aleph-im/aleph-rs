//! Helpers for monthly RANGE-partitioned tables on a TIMESTAMPTZ column.
//!
//! Port of `src/aleph/toolkit/partitions.py`.
//!
//! Used by:
//! * the migration that creates `crn_metrics` / `ccn_metrics` (in raw SQL)
//! * the `metrics_partition` cron job that maintains them (create next
//!   month, drop past-cutoff)
//!
//! Keeping the naming and bounds logic in one place means migration-time
//! partitions and cron-created partitions follow identical conventions.

use chrono::{DateTime, Datelike, TimeZone, Utc};

/// First instant of `d`'s month, UTC. Mirrors Python `month_floor`.
pub fn month_floor(d: DateTime<Utc>) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(d.year(), d.month(), 1, 0, 0, 0)
        .single()
        .expect("first-of-month is always valid")
}

/// Shift `d` by N calendar months (positive or negative). Snaps to the
/// first day of the resulting month. Mirrors Python `add_months`.
pub fn add_months(d: DateTime<Utc>, months: i32) -> DateTime<Utc> {
    // Python: total = d.month - 1 + months; year = d.year + total // 12;
    // month = total % 12 + 1. Use Euclidean division so negative offsets
    // wrap the same way Python's floor-division does.
    let total = (d.month() as i32) - 1 + months;
    let year = d.year() + total.div_euclid(12);
    let month = total.rem_euclid(12) + 1;
    Utc.with_ymd_and_hms(year, month as u32, 1, 0, 0, 0)
        .single()
        .expect("first-of-month is always valid")
}

/// List of `[lower, upper)` month-aligned ranges from `start` to
/// `end_exclusive`. Both arguments should already be at month boundaries.
/// Mirrors Python `monthly_bounds`.
pub fn monthly_bounds(
    start: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    let mut bounds = Vec::new();
    let mut cursor = start;
    while cursor < end_exclusive {
        let upper = add_months(cursor, 1);
        bounds.push((cursor, upper));
        cursor = upper;
    }
    bounds
}

/// Naming convention: `<table>_YYYY_MM`, e.g. `crn_metrics_2026_05`.
/// Mirrors Python `partition_name`.
pub fn partition_name(table: &str, lower: DateTime<Utc>) -> String {
    format!("{table}_{:04}_{:02}", lower.year(), lower.month())
}

/// TIMESTAMPTZ literal suitable for inlining into DDL. Mirrors Python
/// `ts_literal` (`%Y-%m-%d %H:%M:%S%z`).
pub fn ts_literal(d: DateTime<Utc>) -> String {
    d.format("%Y-%m-%d %H:%M:%S%z").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ymd(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, 12, 34, 56).single().unwrap()
    }

    #[test]
    fn month_floor_snaps_to_first_instant() {
        let expected = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).single().unwrap();
        assert_eq!(month_floor(ymd(2026, 5, 18)), expected);
    }

    #[test]
    fn add_months_forward_and_back() {
        let base = month_floor(ymd(2026, 5, 18));
        assert_eq!(add_months(base, 1), month_floor(ymd(2026, 6, 1)));
        assert_eq!(add_months(base, -5), month_floor(ymd(2025, 12, 1)));
        assert_eq!(add_months(base, 8), month_floor(ymd(2027, 1, 1)));
    }

    #[test]
    fn monthly_bounds_covers_range() {
        let start = month_floor(ymd(2026, 1, 1));
        let end = month_floor(ymd(2026, 4, 1));
        let bounds = monthly_bounds(start, end);
        assert_eq!(bounds.len(), 3);
        assert_eq!(bounds[0].0, month_floor(ymd(2026, 1, 1)));
        assert_eq!(bounds[2].1, month_floor(ymd(2026, 4, 1)));
    }

    #[test]
    fn partition_name_format() {
        assert_eq!(
            partition_name("crn_metrics", month_floor(ymd(2026, 5, 1))),
            "crn_metrics_2026_05"
        );
    }

    #[test]
    fn ts_literal_includes_offset() {
        let s = ts_literal(month_floor(ymd(2026, 5, 1)));
        assert!(s.starts_with("2026-05-01 00:00:00"));
        assert!(s.ends_with("+0000"));
    }
}
