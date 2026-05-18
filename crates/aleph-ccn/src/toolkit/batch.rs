//! Async iterator batching.
//!
//! Port of `src/aleph/toolkit/batch.py`.
//!
//! Python defines `async_batch(async_iterable, n)` which yields lists of
//! up to `n` items. We mirror it as a function over [`futures_util::Stream`]
//! that returns a stream of `Vec<T>` using `unfold`.

use futures_util::stream::{self, Stream, StreamExt};

/// Group an async stream into batches of `n` items.
///
/// The final batch may contain fewer than `n` items. Mirrors `async_batch`.
///
/// # Panics
/// Panics if `n == 0`.
pub fn async_batch<S, T>(stream: S, n: usize) -> impl Stream<Item = Vec<T>>
where
    S: Stream<Item = T> + Unpin,
{
    assert!(n > 0, "batch size must be > 0");
    stream::unfold((stream, n), |(mut s, n)| async move {
        let mut batch: Vec<T> = Vec::with_capacity(n);
        while let Some(item) = s.next().await {
            batch.push(item);
            if batch.len() == n {
                return Some((batch, (s, n)));
            }
        }
        if batch.is_empty() {
            None
        } else {
            Some((batch, (s, n)))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn test_batch_even_split() {
        let items = futures_util::stream::iter(0..6);
        let batches: Vec<Vec<i32>> = async_batch(items, 2).collect().await;
        assert_eq!(batches, vec![vec![0, 1], vec![2, 3], vec![4, 5]]);
    }

    #[tokio::test]
    async fn test_batch_with_remainder() {
        let items = futures_util::stream::iter(0..5);
        let batches: Vec<Vec<i32>> = async_batch(items, 2).collect().await;
        assert_eq!(batches, vec![vec![0, 1], vec![2, 3], vec![4]]);
    }

    #[tokio::test]
    async fn test_batch_empty_stream() {
        let items = futures_util::stream::iter(std::iter::empty::<i32>());
        let batches: Vec<Vec<i32>> = async_batch(items, 3).collect().await;
        assert!(batches.is_empty());
    }
}
