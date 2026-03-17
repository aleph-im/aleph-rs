/// Credit-based cost calculation for Heph (spec section 8).
///
/// Heph uses credit-only payments with pre-seeded balances.
/// POST/AGGREGATE/FORGET are free. STORE/PROGRAM/INSTANCE cost credits.
use crate::db::Db;
use crate::handlers::{ProcessingError, ProcessingResult};

// ---------------------------------------------------------------------------
// Pricing constants
// ---------------------------------------------------------------------------

/// Storage cost: credits per MiB per hour.
const STORAGE_CREDIT: f64 = 0.0033;

/// Compute cost: credits per CU per hour.
const COMPUTE_CREDIT: f64 = 0.011;

/// Minimum billable size for STORE messages in MiB.
const MIN_STORE_COST_MIB: u64 = 25;

/// Minimum cost per hour for VMs (1 credit/hour).
const MIN_CREDIT_COST_PER_HOUR: f64 = 1.0;

/// Seconds in a day — the node requires 1 day of coverage in the credit balance.
const SECONDS_PER_DAY: f64 = 86_400.0;

/// Pre-seeded balance for addresses not yet in the DB (spec: seed on startup).
pub const INITIAL_CREDIT_BALANCE: i64 = 1_000_000_000;

// ---------------------------------------------------------------------------
// Public result type (kept simple; not currently needed externally)
// ---------------------------------------------------------------------------

/// Result of a cost calculation.
#[allow(dead_code)]
pub struct CostResult {
    pub per_second: f64,
    pub cost_type: &'static str,
}

// ---------------------------------------------------------------------------
// Cost calculation functions
// ---------------------------------------------------------------------------

/// Calculate the per-second cost for a STORE message.
///
/// `size_bytes` — actual file size in bytes. Minimum billable size is 25 MiB.
pub fn calculate_store_cost(size_bytes: u64) -> f64 {
    let size_mib = size_mib_from_bytes(size_bytes).max(MIN_STORE_COST_MIB);
    (size_mib as f64) * STORAGE_CREDIT / 3600.0
}

/// Calculate the total per-second cost for a PROGRAM or INSTANCE message.
///
/// * `vcpus`           — number of virtual CPUs
/// * `memory_mib`      — allocated memory in MiB
/// * `total_volume_mib`— total size of all volumes in MiB (persistent + rootfs, etc.)
pub fn calculate_vm_cost(vcpus: u32, memory_mib: u32, total_volume_mib: u64) -> f64 {
    let compute_units = compute_units(vcpus, memory_mib);

    // Execution cost per second.
    let execution_per_second = (compute_units as f64) * COMPUTE_CREDIT / 3600.0;

    // Storage cost per second (with free disk per CU deduction).
    let free_mib = (compute_units as u64).saturating_mul(2048);
    let billable_storage_mib = total_volume_mib.saturating_sub(free_mib);
    let storage_per_second = (billable_storage_mib as f64) * STORAGE_CREDIT / 3600.0;

    let total_per_second = execution_per_second + storage_per_second;

    // Apply minimum: 1 credit/hour.
    let min_per_second = MIN_CREDIT_COST_PER_HOUR / 3600.0;
    total_per_second.max(min_per_second)
}

/// Check that `address` has enough credits to cover `new_per_second_cost` on top of
/// all its existing resources, for one full day.
///
/// Formula:
/// ```text
/// required_balance = (existing_total_per_second + new_per_second_cost) * 86400
/// if credit_balance < required_balance: reject
/// ```
pub fn check_credit_balance(
    db: &Db,
    address: &str,
    new_per_second_cost: f64,
) -> ProcessingResult<()> {
    // 1. Get current credit balance.
    //    Auto-seed with INITIAL_CREDIT_BALANCE if the address has no record yet
    //    (spec: addresses are pre-seeded on startup; in tests/dev there's no startup phase).
    let balance: i64 = db
        .with_conn(|conn| -> rusqlite::Result<i64> {
            let existing = crate::db::balances::get_credit_balance(conn, address)?;
            if let Some(b) = existing {
                return Ok(b);
            }
            // Auto-seed and return initial balance.
            crate::db::balances::set_credit_balance(conn, address, INITIAL_CREDIT_BALANCE)?;
            Ok(INITIAL_CREDIT_BALANCE)
        })
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // 2. Get sum of all existing per-second costs for this address.
    let existing_total: f64 = db
        .with_conn(|conn| crate::db::costs::get_total_cost_for_address(conn, address))
        .map_err(|e| ProcessingError::InternalError(e.to_string()))?;

    // 3. Apply formula.
    let required = (existing_total + new_per_second_cost) * SECONDS_PER_DAY;

    if (balance as f64) < required {
        return Err(ProcessingError::CreditInsufficient(format!(
            "credit balance insufficient: have {balance} credits, need {required:.2} \
             (existing_cost={existing_total:.6}/s + new={new_per_second_cost:.6}/s * {SECONDS_PER_DAY}s)"
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Convert bytes to MiB, rounding up.
fn size_mib_from_bytes(bytes: u64) -> u64 {
    bytes.div_ceil(1024 * 1024)
}

/// Compute units = max(vcpus, ceil(memory_mib / 2048)).
fn compute_units(vcpus: u32, memory_mib: u32) -> u32 {
    let cu_from_mem = memory_mib.div_ceil(2048);
    vcpus.max(cu_from_mem)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Test 5: Store cost calculation
    // -----------------------------------------------------------------------

    #[test]
    fn test_store_cost_100_mib() {
        // 100 MiB file: 100 * 0.0033 / 3600
        let expected = 100.0 * STORAGE_CREDIT / 3600.0;
        let result = calculate_store_cost(100 * 1024 * 1024);
        assert!(
            (result - expected).abs() < 1e-12,
            "got {result}, expected {expected}"
        );
    }

    #[test]
    fn test_store_cost_minimum_applied() {
        // 1-byte file → billed as 25 MiB minimum
        let result = calculate_store_cost(1);
        let expected = 25.0 * STORAGE_CREDIT / 3600.0;
        assert!(
            (result - expected).abs() < 1e-12,
            "got {result}, expected {expected}"
        );
    }

    #[test]
    fn test_store_cost_zero_bytes() {
        // 0-byte file → billed as 25 MiB minimum
        let result = calculate_store_cost(0);
        let expected = 25.0 * STORAGE_CREDIT / 3600.0;
        assert!((result - expected).abs() < 1e-12);
    }

    #[test]
    fn test_store_cost_exactly_25_mib() {
        let result = calculate_store_cost(25 * 1024 * 1024);
        let expected = 25.0 * STORAGE_CREDIT / 3600.0;
        assert!((result - expected).abs() < 1e-12);
    }

    // -----------------------------------------------------------------------
    // Test 6: VM cost calculation
    // -----------------------------------------------------------------------

    #[test]
    fn test_vm_cost_2vcpus_4096mib() {
        // compute_units = max(2, ceil(4096/2048)) = max(2, 2) = 2
        // execution = 2 * 0.011 / 3600 ≈ 6.111e-6/s
        // free_mib = 2 * 2048 = 4096; storage = max(0, 0 - 4096) = 0
        // raw total ≈ 6.111e-6/s
        // minimum = 1.0 / 3600 ≈ 2.778e-4/s  → minimum applies
        let result = calculate_vm_cost(2, 4096, 0);
        let min_per_second = MIN_CREDIT_COST_PER_HOUR / 3600.0;
        let exec_per_second = 2.0 * COMPUTE_CREDIT / 3600.0;
        let expected = exec_per_second.max(min_per_second);
        assert!(
            (result - expected).abs() < 1e-12,
            "got {result}, expected {expected}"
        );
        // Sanity: the minimum is larger than raw execution cost.
        assert!(
            min_per_second > exec_per_second,
            "minimum ({min_per_second}) should exceed raw execution cost ({exec_per_second})"
        );
    }

    #[test]
    fn test_vm_cost_1vcpu_128mib_minimum() {
        // compute_units = max(1, ceil(128/2048)) = max(1, 1) = 1
        // execution = 1 * 0.011 / 3600 ≈ 3.056e-6/s
        // min = 1.0 / 3600 ≈ 2.778e-4/s  → execution wins over minimum
        let result = calculate_vm_cost(1, 128, 0);
        let exec_cost = 1.0 * COMPUTE_CREDIT / 3600.0;
        let min_cost = MIN_CREDIT_COST_PER_HOUR / 3600.0;
        let expected = exec_cost.max(min_cost);
        assert!(
            (result - expected).abs() < 1e-12,
            "got {result}, expected {expected}"
        );
    }

    #[test]
    fn test_vm_cost_with_storage_exceeding_free_disk() {
        // 1 CU → 2048 MiB free disk
        // total_volume_mib = 5000 → billable = 5000 - 2048 = 2952 MiB
        let result = calculate_vm_cost(1, 128, 5000);
        let exec = 1.0 * COMPUTE_CREDIT / 3600.0;
        let storage = 2952.0 * STORAGE_CREDIT / 3600.0;
        let expected = (exec + storage).max(MIN_CREDIT_COST_PER_HOUR / 3600.0);
        assert!(
            (result - expected).abs() < 1e-12,
            "got {result}, expected {expected}"
        );
    }

    // -----------------------------------------------------------------------
    // Compute-units helper
    // -----------------------------------------------------------------------

    #[test]
    fn test_compute_units_memory_dominated() {
        // 1 vCPU, 8192 MiB → ceil(8192/2048) = 4 CUs
        assert_eq!(compute_units(1, 8192), 4);
    }

    #[test]
    fn test_compute_units_vcpu_dominated() {
        // 8 vCPUs, 2048 MiB → ceil(2048/2048) = 1, but vcpus=8 wins
        assert_eq!(compute_units(8, 2048), 8);
    }

    // -----------------------------------------------------------------------
    // check_credit_balance integration tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_preseed_balance_stored() {
        use crate::db::Db;
        use crate::db::balances::{get_credit_balance, set_credit_balance};

        let db = Db::open_in_memory().unwrap();
        let addr = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1";

        db.with_conn(|conn| set_credit_balance(conn, addr, 1_000_000_000))
            .unwrap();

        let balance = db.with_conn(|conn| get_credit_balance(conn, addr)).unwrap();
        assert_eq!(balance, Some(1_000_000_000));
    }

    #[test]
    fn test_credit_balance_passes_with_sufficient_credits() {
        use crate::db::Db;
        use crate::db::balances::set_credit_balance;

        let db = Db::open_in_memory().unwrap();
        let addr = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2";

        // Pre-seed with 1 billion credits.
        db.with_conn(|conn| set_credit_balance(conn, addr, 1_000_000_000))
            .unwrap();

        // A small per-second cost should pass easily.
        let per_second = calculate_store_cost(100 * 1024 * 1024);
        let result = check_credit_balance(&db, addr, per_second);
        assert!(result.is_ok(), "expected Ok but got {:?}", result);
    }

    #[test]
    fn test_credit_balance_fails_with_insufficient_credits() {
        use crate::db::Db;
        use crate::db::balances::set_credit_balance;

        let db = Db::open_in_memory().unwrap();
        let addr = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3";

        // Set a very small balance (1 credit).
        db.with_conn(|conn| set_credit_balance(conn, addr, 1))
            .unwrap();

        // A large file will require far more than 1 credit.
        let per_second = calculate_store_cost(1024 * 1024 * 1024); // 1 GiB
        let result = check_credit_balance(&db, addr, per_second);
        assert!(result.is_err(), "expected CreditInsufficient error");
        let err = result.unwrap_err();
        assert_eq!(
            err.error_code(),
            6,
            "expected code 6 (CreditInsufficient), got {:?}",
            err
        );
    }

    #[test]
    fn test_post_message_is_free_no_balance_needed() {
        // POST is free — no check_credit_balance call is made.
        // This test just verifies the check passes with zero balance for a zero-cost message.
        use crate::db::Db;

        let db = Db::open_in_memory().unwrap();
        let addr = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa4";
        // No balance seeded — zero cost message passes.
        let result = check_credit_balance(&db, addr, 0.0);
        assert!(result.is_ok());
    }
}
