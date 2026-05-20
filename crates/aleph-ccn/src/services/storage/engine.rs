//! Abstract storage backend. Mirrors `aleph/services/storage/engine.py`.
//!
//! Backends are accessed through trait objects so the rest of the codebase can
//! swap filesystem, IPFS or remote backends without recompilation.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;

use crate::{AlephError, AlephResult};

#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn read(&self, filename: &str) -> AlephResult<Option<Bytes>>;

    async fn read_iterator(
        &self,
        filename: &str,
        chunk_size: usize,
    ) -> AlephResult<Option<BoxStream<'static, std::io::Result<Bytes>>>>;

    async fn write(&self, filename: &str, content: &[u8]) -> AlephResult<()>;

    async fn write_file(&self, filename: &str, path: &Path) -> AlephResult<()> {
        let content = tokio::fs::read(path).await.map_err(AlephError::Io)?;
        self.write(filename, &content).await
    }

    async fn delete(&self, filename: &str) -> AlephResult<()>;

    async fn exists(&self, filename: &str) -> AlephResult<bool>;
}
