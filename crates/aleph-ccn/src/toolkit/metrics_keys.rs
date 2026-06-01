//! Shared Redis key constants for node metrics. Mirrors
//! `aleph/toolkit/metrics_keys.py`.
//!
//! Kept in the toolkit (a leaf module that imports nothing from the web or
//! handler layers) so both the writer (`handlers/content/store.rs`) and the
//! reader (`web/controllers/metrics.rs`) use the same strings.

use aleph_types::message::item_type::ItemType;

// STORE file-fetch metrics, split by item type because the fetch source
// differs: `storage` files are pulled from CCN HTTP APIs, `ipfs` files come
// through the IPFS (Kubo) daemon. Tracking them separately tells a disk-bound
// regression (both slow) from a network/bitswap one (only ipfs slow). The mean
// fetch time is duration_ms_sum / (total - failed) per type.
pub const STORE_FETCH_IPFS_TOTAL_KEY: &str = "pyaleph_store_fetch_ipfs_total";
pub const STORE_FETCH_IPFS_FAILED_KEY: &str = "pyaleph_store_fetch_ipfs_failed_total";
pub const STORE_FETCH_IPFS_DURATION_MS_SUM_KEY: &str = "pyaleph_store_fetch_ipfs_duration_ms_sum";
pub const STORE_FETCH_STORAGE_TOTAL_KEY: &str = "pyaleph_store_fetch_storage_total";
pub const STORE_FETCH_STORAGE_FAILED_KEY: &str = "pyaleph_store_fetch_storage_failed_total";
pub const STORE_FETCH_STORAGE_DURATION_MS_SUM_KEY: &str =
    "pyaleph_store_fetch_storage_duration_ms_sum";

/// Return the `(total, failed, duration_ms_sum)` Redis keys for an item type.
/// Mirrors `store_fetch_keys` in pyaleph.
pub fn store_fetch_keys(item_type: ItemType) -> (&'static str, &'static str, &'static str) {
    if item_type == ItemType::Ipfs {
        (
            STORE_FETCH_IPFS_TOTAL_KEY,
            STORE_FETCH_IPFS_FAILED_KEY,
            STORE_FETCH_IPFS_DURATION_MS_SUM_KEY,
        )
    } else {
        (
            STORE_FETCH_STORAGE_TOTAL_KEY,
            STORE_FETCH_STORAGE_FAILED_KEY,
            STORE_FETCH_STORAGE_DURATION_MS_SUM_KEY,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_fetch_keys_differentiate_by_type() {
        let ipfs_keys = store_fetch_keys(ItemType::Ipfs);
        let storage_keys = store_fetch_keys(ItemType::Storage);

        assert_eq!(
            ipfs_keys,
            (
                STORE_FETCH_IPFS_TOTAL_KEY,
                STORE_FETCH_IPFS_FAILED_KEY,
                STORE_FETCH_IPFS_DURATION_MS_SUM_KEY,
            )
        );
        assert_eq!(
            storage_keys,
            (
                STORE_FETCH_STORAGE_TOTAL_KEY,
                STORE_FETCH_STORAGE_FAILED_KEY,
                STORE_FETCH_STORAGE_DURATION_MS_SUM_KEY,
            )
        );
        // The two key triples must be fully disjoint.
        let ipfs_set = [ipfs_keys.0, ipfs_keys.1, ipfs_keys.2];
        let storage_set = [storage_keys.0, storage_keys.1, storage_keys.2];
        assert!(ipfs_set.iter().all(|k| !storage_set.contains(k)));
    }
}
