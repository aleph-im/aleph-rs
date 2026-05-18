//! Trait roles for chain integrations. Mirrors `aleph/chains/abc.py`.
//!
//! In pyaleph, `Verifier`, `ChainReader` and `ChainWriter` are abstract base
//! classes parameterised on `BasePendingMessage`. Because the schemas slice
//! is still in flight (the concrete `BasePendingMessage` struct on this side
//! is an enum that doesn't expose every field we need), we use a narrow
//! `PendingMessageView` trait so chain code compiles independently.
//!
//! When the schemas agent finishes the typed `BasePendingMessage`, all that
//! is required is an `impl PendingMessageView for BasePendingMessage` and the
//! chain code lights up unchanged.

use async_trait::async_trait;

use aleph_types::chain::Chain;
use aleph_types::message::MessageType;

use crate::AlephResult;
use crate::config::Settings;

/// Borrowed view of the fields a Verifier needs to look at on a pending
/// message. Mirrors `aleph.schemas.pending_messages.BasePendingMessage`.
///
/// The view exposes raw strings rather than typed wrappers because non-EVM
/// chains (Tezos, Cosmos, Substrate, NULS) use address formats that are not
/// expressible via `aleph_types::chain::Address`'s parser.
pub trait PendingMessageView: Send + Sync {
    fn chain(&self) -> Chain;
    fn sender(&self) -> &str;
    fn message_type(&self) -> MessageType;
    fn item_hash(&self) -> &str;
    /// The raw signature payload. Some chains (Solana/Tezos/Cosmos/Substrate)
    /// pack the signature into a JSON object — the verifier handles that.
    fn signature(&self) -> Option<&str>;
    /// Message time in seconds since the Unix epoch. Required by Tezos
    /// Micheline-style signatures.
    fn time_seconds(&self) -> f64;
}

#[async_trait]
pub trait Verifier: Send + Sync {
    /// Returns `Ok(true)` if the message signature matches the sender,
    /// `Ok(false)` if it does not (or is malformed in a way we explicitly
    /// recognise), and `Err(...)` for unrecoverable verification errors.
    async fn verify_signature(&self, message: &dyn PendingMessageView) -> AlephResult<bool>;
}

#[async_trait]
pub trait ChainReader: Send + Sync {
    async fn fetcher(&self, cfg: &Settings) -> AlephResult<()>;
}

#[async_trait]
pub trait ChainWriter: ChainReader {
    async fn packer(&self, cfg: &Settings) -> AlephResult<()>;
}

/// Minimal in-memory implementation of `PendingMessageView`, useful for tests.
#[derive(Debug, Clone)]
pub struct SimplePendingMessage {
    pub chain: Chain,
    pub sender: String,
    pub message_type: MessageType,
    pub item_hash: String,
    pub signature: Option<String>,
    pub time_seconds: f64,
}

impl PendingMessageView for SimplePendingMessage {
    fn chain(&self) -> Chain {
        self.chain.clone()
    }
    fn sender(&self) -> &str {
        &self.sender
    }
    fn message_type(&self) -> MessageType {
        self.message_type
    }
    fn item_hash(&self) -> &str {
        &self.item_hash
    }
    fn signature(&self) -> Option<&str> {
        self.signature.as_deref()
    }
    fn time_seconds(&self) -> f64 {
        self.time_seconds
    }
}
