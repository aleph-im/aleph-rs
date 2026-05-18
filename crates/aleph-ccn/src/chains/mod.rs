//! Chain integrations. Mirrors `aleph/chains/`.
//!
//! Layout matches the Python tree: one module per supported chain plus the
//! shared `abc`/`common`/`signature_verifier` helpers and the on-chain
//! orchestration in `chain_data_service` + `connector` + `indexer_reader`.

pub mod abc;
pub mod avalanche;
pub mod bsc;
pub mod chain_data_service;
pub mod common;
pub mod connector;
pub mod cosmos;
pub mod ethereum;
pub mod evm;
pub mod indexer_reader;
pub mod nuls;
pub mod nuls2;
pub mod nuls_aleph_sdk;
pub mod signature_verifier;
pub mod solana;
pub mod substrate;
pub mod tezos;
