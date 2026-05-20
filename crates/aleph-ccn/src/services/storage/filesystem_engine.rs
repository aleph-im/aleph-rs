//! Filesystem-backed storage. Mirrors `aleph/services/storage/fileystem_engine.py`
//! (sic; the Python file name is misspelled).

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};

use super::engine::StorageEngine;
use crate::{AlephError, AlephResult};

pub struct FileSystemStorageEngine {
    pub folder: PathBuf,
}

impl FileSystemStorageEngine {
    pub fn new<P: AsRef<Path>>(folder: P) -> AlephResult<Self> {
        let folder = folder.as_ref().to_path_buf();
        if folder.exists() && !folder.is_dir() {
            return Err(AlephError::Storage(format!(
                "'{}' exists and is not a directory.",
                folder.display()
            )));
        }
        std::fs::create_dir_all(&folder).map_err(AlephError::Io)?;
        Ok(Self { folder })
    }

    fn resolve(&self, filename: &str) -> PathBuf {
        self.folder.join(filename)
    }
}

#[async_trait]
impl StorageEngine for FileSystemStorageEngine {
    async fn read(&self, filename: &str) -> AlephResult<Option<Bytes>> {
        let path = self.resolve(filename);
        match fs::read(&path).await {
            Ok(content) => Ok(Some(Bytes::from(content))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(AlephError::Io(e)),
        }
    }

    async fn read_iterator(
        &self,
        filename: &str,
        chunk_size: usize,
    ) -> AlephResult<Option<BoxStream<'static, std::io::Result<Bytes>>>> {
        let path = self.resolve(filename);
        let file = match fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(AlephError::Io(e)),
        };
        let stream = async_stream(file, chunk_size);
        Ok(Some(Box::pin(stream)))
    }

    async fn write(&self, filename: &str, content: &[u8]) -> AlephResult<()> {
        let path = self.resolve(filename);
        fs::write(&path, content).await.map_err(AlephError::Io)
    }

    async fn write_file(&self, filename: &str, source: &Path) -> AlephResult<()> {
        let path = self.resolve(filename);
        fs::copy(source, path).await.map_err(AlephError::Io)?;
        Ok(())
    }

    async fn delete(&self, filename: &str) -> AlephResult<()> {
        let path = self.resolve(filename);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(AlephError::Io(e)),
        }
    }

    async fn exists(&self, filename: &str) -> AlephResult<bool> {
        let path = self.resolve(filename);
        Ok(fs::try_exists(&path).await.map_err(AlephError::Io)?)
    }
}

fn async_stream(
    file: tokio::fs::File,
    chunk_size: usize,
) -> impl tokio_stream::Stream<Item = std::io::Result<Bytes>> + Send + 'static {
    let reader = BufReader::new(file);
    let chunk_size = chunk_size.max(1);
    futures_util::stream::unfold(
        (reader, chunk_size),
        |(mut reader, chunk_size)| async move {
            let mut buf = vec![0u8; chunk_size];
            match reader.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    buf.truncate(n);
                    Some((Ok(Bytes::from(buf)), (reader, chunk_size)))
                }
                Err(e) => Some((Err(e), (reader, chunk_size))),
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt as _;
    use tempfile::tempdir;

    #[tokio::test]
    async fn roundtrip() {
        let dir = tempdir().unwrap();
        let engine = FileSystemStorageEngine::new(dir.path()).unwrap();
        assert!(!engine.exists("foo").await.unwrap());
        engine.write("foo", b"hello world").await.unwrap();
        assert!(engine.exists("foo").await.unwrap());
        let body = engine.read("foo").await.unwrap().unwrap();
        assert_eq!(body.as_ref(), b"hello world");
        engine.delete("foo").await.unwrap();
        assert!(!engine.exists("foo").await.unwrap());
    }

    #[tokio::test]
    async fn chunked_read() {
        let dir = tempdir().unwrap();
        let engine = FileSystemStorageEngine::new(dir.path()).unwrap();
        engine.write("blob", &vec![7u8; 3000]).await.unwrap();
        let stream = engine.read_iterator("blob", 1024).await.unwrap().unwrap();
        let total: usize = stream
            .fold(0usize, |acc, c| async move { acc + c.unwrap().len() })
            .await;
        assert_eq!(total, 3000);
    }

    #[tokio::test]
    async fn delete_missing_is_noop() {
        let dir = tempdir().unwrap();
        let engine = FileSystemStorageEngine::new(dir.path()).unwrap();
        engine.delete("absent").await.unwrap();
    }
}
