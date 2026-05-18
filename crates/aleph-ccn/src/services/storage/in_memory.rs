//! In-memory storage engine, intended for tests. Mirrors
//! `tests/helpers/in_memory_storage_engine.py`.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;

use super::engine::StorageEngine;
use crate::AlephResult;

#[derive(Default, Debug)]
pub struct InMemoryStorageEngine {
    files: Mutex<HashMap<String, Bytes>>,
}

impl InMemoryStorageEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_files(files: HashMap<String, Bytes>) -> Self {
        Self {
            files: Mutex::new(files),
        }
    }
}

#[async_trait]
impl StorageEngine for InMemoryStorageEngine {
    async fn read(&self, filename: &str) -> AlephResult<Option<Bytes>> {
        Ok(self.files.lock().unwrap().get(filename).cloned())
    }

    async fn read_iterator(
        &self,
        filename: &str,
        chunk_size: usize,
    ) -> AlephResult<Option<BoxStream<'static, std::io::Result<Bytes>>>> {
        let Some(content) = self.read(filename).await? else {
            return Ok(None);
        };
        let chunk_size = chunk_size.max(1);
        let chunks: Vec<Bytes> = content
            .chunks(chunk_size)
            .map(|c| Bytes::copy_from_slice(c))
            .collect();
        let stream = futures_util::stream::iter(chunks.into_iter().map(Ok));
        Ok(Some(Box::pin(stream)))
    }

    async fn write(&self, filename: &str, content: &[u8]) -> AlephResult<()> {
        self.files
            .lock()
            .unwrap()
            .insert(filename.to_string(), Bytes::copy_from_slice(content));
        Ok(())
    }

    async fn delete(&self, filename: &str) -> AlephResult<()> {
        self.files.lock().unwrap().remove(filename);
        Ok(())
    }

    async fn exists(&self, filename: &str) -> AlephResult<bool> {
        Ok(self.files.lock().unwrap().contains_key(filename))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt as _;

    #[tokio::test]
    async fn empty_returns_none() {
        let e = InMemoryStorageEngine::new();
        assert!(e.read("missing").await.unwrap().is_none());
        assert!(!e.exists("missing").await.unwrap());
    }

    #[tokio::test]
    async fn roundtrip() {
        let e = InMemoryStorageEngine::new();
        e.write("a", b"hello").await.unwrap();
        assert_eq!(e.read("a").await.unwrap().unwrap().as_ref(), b"hello");
        assert!(e.exists("a").await.unwrap());
        e.delete("a").await.unwrap();
        assert!(!e.exists("a").await.unwrap());
    }

    #[tokio::test]
    async fn chunked() {
        let e = InMemoryStorageEngine::new();
        e.write("blob", &vec![3u8; 2500]).await.unwrap();
        let s = e.read_iterator("blob", 1024).await.unwrap().unwrap();
        let total: usize = s
            .fold(0usize, |acc, c| async move { acc + c.unwrap().len() })
            .await;
        assert_eq!(total, 2500);
    }
}
