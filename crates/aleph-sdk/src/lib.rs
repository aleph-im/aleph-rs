pub mod aggregate_models;
pub mod authorization;
pub mod builder;
pub mod caching_aggregate_client;
pub mod client;
pub mod confidential;
pub mod corechannel;
#[cfg(feature = "credits")]
pub mod credit;
pub mod credit_transfer;
pub mod crn;
pub mod crns_list;
pub mod ipfs;
pub mod messages;
pub mod progress;
pub mod scheduler;
pub mod ssh;
#[cfg(feature = "swap")]
pub mod swap;
pub mod verify;
pub mod ws;

// CID computation (hashing, UnixFS folder DAGs, CARv1 framing) lives in the
// dependency-light `aleph-cid` crate so it can be reused from FFI bindings.
// Re-exported at the historical paths for API compatibility. The
// `ItemHash`-aware hashing/verification wrapper stays here in `verify`.
pub use aleph_cid::{car, folder_hash};
