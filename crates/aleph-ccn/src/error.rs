//! Shared error types for the CCN.
//!
//! Mirrors `aleph/exceptions.py` and `aleph/toolkit/exceptions.py`.

use thiserror::Error;

pub type AlephResult<T> = Result<T, AlephError>;

#[derive(Debug, Error)]
pub enum AlephError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("database error: {0}")]
    Db(#[from] tokio_postgres::Error),

    #[error("pool error: {0}")]
    Pool(String),

    #[error("migration error: {0}")]
    Migrate(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("hex error: {0}")]
    Hex(#[from] hex::FromHexError),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("invalid message: {0}")]
    InvalidMessage(String),

    #[error("invalid signature")]
    InvalidSignature,

    #[error("message rejected: {code:?} - {reason}")]
    Rejected { code: i32, reason: String },

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid item hash: {0}")]
    InvalidItemHash(String),

    #[error("content too large: {actual} > {limit}")]
    ContentTooLarge { actual: u64, limit: u64 },

    #[error("storage: {0}")]
    Storage(String),

    #[error("p2p: {0}")]
    P2p(String),

    #[error("ipfs: {0}")]
    Ipfs(String),

    #[error("chain: {0}")]
    Chain(String),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}
