//! Byte-progress reporting for streamed uploads.
//!
//! UI-free: the combinator counts bytes and invokes a caller-supplied callback;
//! the caller decides how (or whether) to render. Shared by the CLI's file
//! upload and instance-backup restore paths so both throttle and tick
//! identically.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};

/// Minimum gap between progress ticks. A final tick is always emitted on the
/// last chunk regardless of this interval.
const TICK_INTERVAL: Duration = Duration::from_millis(500);

/// Wrap an upload byte stream so it reports cumulative progress to `on_tick`
/// roughly every [`TICK_INTERVAL`], plus a guaranteed final tick once `total`
/// bytes have been seen (so a slow last chunk doesn't leave the rendered
/// percentage stuck just below 100%).
///
/// `on_tick(sent, total)` receives the cumulative bytes sent, capped at
/// `total`, and the total expected size. Every chunk is passed through
/// unchanged; errors do not advance the counter or tick.
pub fn report_upload_progress<S>(
    stream: S,
    total: u64,
    mut on_tick: impl FnMut(u64, u64) + Send + 'static,
) -> impl Stream<Item = S::Item> + Send
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
{
    let mut sent: u64 = 0;
    let mut last_report = Instant::now();
    stream.map(move |chunk| {
        if let Ok(bytes) = &chunk {
            sent = sent.saturating_add(bytes.len() as u64);
            if last_report.elapsed() >= TICK_INTERVAL || sent >= total {
                on_tick(sent.min(total), total);
                last_report = Instant::now();
            }
        }
        chunk
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Drive `report_upload_progress` over `chunks` (each becomes one `Ok`
    /// stream item), returning the bytes that came out the far end and the
    /// list of `(sent, total)` ticks recorded.
    async fn run(chunks: &[&[u8]], total: u64) -> (Vec<u8>, Vec<(u64, u64)>) {
        let ticks = Arc::new(Mutex::new(Vec::new()));
        let recorder = {
            let ticks = Arc::clone(&ticks);
            move |sent, total| ticks.lock().unwrap().push((sent, total))
        };
        let input: Vec<std::io::Result<Bytes>> = chunks
            .iter()
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        let stream = report_upload_progress(futures_util::stream::iter(input), total, recorder);
        let collected: Vec<Bytes> = stream.map(|r| r.unwrap()).collect().await;
        let bytes = collected.into_iter().flatten().collect();
        let ticks = Arc::try_unwrap(ticks).unwrap().into_inner().unwrap();
        (bytes, ticks)
    }

    #[tokio::test]
    async fn passes_chunks_through_unchanged() {
        let (bytes, _) = run(&[b"hello ", b"world"], 11).await;
        assert_eq!(bytes, b"hello world");
    }

    #[tokio::test]
    async fn emits_final_tick_at_total_even_when_under_interval() {
        // Chunks arrive instantly, so the 500ms interval never elapses; only
        // the final-chunk rule should fire a tick, and it must report 100%.
        let (_, ticks) = run(&[b"aaaa", b"bbbb", b"cc"], 10).await;
        assert_eq!(ticks.last(), Some(&(10, 10)));
    }

    #[tokio::test]
    async fn ticks_never_exceed_total_and_are_monotonic() {
        let (_, ticks) = run(&[b"aaaa", b"bbbb", b"cc"], 10).await;
        let mut prev = 0;
        for (sent, total) in &ticks {
            assert_eq!(*total, 10);
            assert!(*sent <= 10, "tick {sent} exceeded total");
            assert!(*sent >= prev, "tick {sent} went backwards from {prev}");
            prev = *sent;
        }
    }

    #[tokio::test]
    async fn empty_stream_produces_no_ticks() {
        let (bytes, ticks) = run(&[], 0).await;
        assert!(bytes.is_empty());
        assert!(ticks.is_empty());
    }
}
