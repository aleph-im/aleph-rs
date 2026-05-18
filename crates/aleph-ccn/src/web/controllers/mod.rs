//! HTTP controllers. Mirrors `aleph/web/controllers/`.
//!
//! Individual controllers are wired into `routes::router` as they land.

pub mod accounts;
pub mod aggregates;
pub mod auth;
pub mod authorizations;
pub mod channels;
pub mod error;
pub mod info;
pub mod ipfs;
pub mod main;
pub mod messages;
pub mod metrics;
pub mod p2p;
pub mod posts;
pub mod prices;
pub mod programs;
pub mod routes;
pub mod storage;
pub mod utils;
pub mod version;
