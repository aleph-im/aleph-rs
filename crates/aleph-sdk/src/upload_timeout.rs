//! Timeout policy for streamed uploads.
//!
//! The upload endpoints stream a request body whose duration is dominated by
//! the client's connection speed and the server's synchronous IPFS pinning.
//! A single wall-clock deadline over the whole request (reqwest's `.timeout()`)
//! cuts large uploads on slow-but-healthy links, because it does not reset as
//! bytes flow.
//!
//! Instead the SDK manages the upload deadline itself via [`UploadTimeout`].
//! The [`Idle`](UploadTimeout::Idle) policy aborts only when no bytes have moved
//! for a while, so a slow upload that keeps making progress is never cut.
//!
//! ## Why an external watchdog
//!
//! Idle detection cannot be a per-poll timeout on the body stream. When the
//! socket back-pressures on a slow connection, hyper stops polling the body
//! stream, so a timeout wrapping `poll_next` would never fire during a stall.
//! The reliable signal is the *chunk-consumed* event: hyper pulls the next
//! chunk only once the socket has drained the previous one. [`track_activity`]
//! bumps a shared counter on each consumed chunk, and [`run_upload`] runs a
//! watchdog that observes that counter.
//!
//! "Activity" therefore means "bytes accepted by the HTTP client for
//! transmission". With a streamed body reqwest applies back-pressure, so this
//! closely tracks socket drain; a genuine stall stops activity and the watchdog
//! fires.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::time::{Duration, Instant, sleep};

/// Chunk size used when turning an in-memory buffer into an activity-tracked
/// upload stream. Small enough that the activity counter advances smoothly as
/// the socket drains, large enough to keep per-chunk overhead negligible.
const BYTES_STREAM_CHUNK: usize = 64 * 1024;

/// Policy governing how long an upload may run before the SDK aborts it.
///
/// Applies only to the upload endpoints (the retry-less `upload_client`); other
/// requests use [`TimeoutConfig::request_timeout`](crate::client::TimeoutConfig).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadTimeout {
    /// No SDK-managed deadline; only the connection timeout applies. A stalled
    /// upload can hang until the OS tears the connection down.
    None,
    /// Abort after this much wall-clock time, regardless of progress. Close to
    /// the pre-policy behavior, but note the scope: like all policies here it
    /// bounds the request send and response headers, not the reading of the
    /// (tiny) response body afterwards. Does not suit large uploads on slow links.
    Total(Duration),
    /// Abort only after no bytes have been sent for this long. A slow upload
    /// that keeps making progress runs to completion. The window starts when the
    /// upload begins, so it also bounds a stalled connection setup before the
    /// first byte flows; this is only noticeable with a very short duration.
    Idle(Duration),
}

impl Default for UploadTimeout {
    /// Idle timeout of 120s: forgiving of slow links, quick to give up on a
    /// genuinely dead connection.
    fn default() -> Self {
        UploadTimeout::Idle(Duration::from_secs(120))
    }
}

impl std::str::FromStr for UploadTimeout {
    type Err = String;

    /// Parse an idle-timeout override, as used by the CLI's
    /// `ALEPH_UPLOAD_TIMEOUT` escape hatch. Accepts a whole number of seconds
    /// for the idle window, or `none`/`off`/`0` to disable the SDK-managed
    /// deadline entirely. Surrounding whitespace is trimmed and the keywords are
    /// case-insensitive.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("none") || s.eq_ignore_ascii_case("off") {
            return Ok(UploadTimeout::None);
        }
        let secs: u64 = s
            .parse()
            .map_err(|_| format!("expected a number of seconds or `none`, got {s:?}"))?;
        // A zero-second idle window would abort instantly; read it as "disable".
        if secs == 0 {
            Ok(UploadTimeout::None)
        } else {
            Ok(UploadTimeout::Idle(Duration::from_secs(secs)))
        }
    }
}

/// Which deadline tripped, for building a user-facing message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutFired {
    Total(Duration),
    Idle(Duration),
}

impl fmt::Display for TimeoutFired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `{:?}` on a Duration renders human-readable units ("60s", "200ms")
        // without truncating sub-second timeouts to "0s".
        match self {
            TimeoutFired::Total(d) => {
                write!(f, "exceeded total timeout of {d:?}")
            }
            TimeoutFired::Idle(d) => {
                write!(f, "no data sent for {d:?}")
            }
        }
    }
}

/// Shared counter of bytes handed to the HTTP client, bumped by
/// [`track_activity`] and sampled by the idle watchdog in [`run_upload`].
#[derive(Clone, Default)]
pub struct UploadActivity(Arc<AtomicU64>);

impl UploadActivity {
    pub fn new() -> Self {
        Self::default()
    }

    fn bump(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Wrap an upload byte stream so every chunk that passes through bumps
/// `activity` by its length. Chunks are forwarded unchanged; errored items do
/// not advance the counter.
pub fn track_activity<S>(stream: S, activity: UploadActivity) -> impl Stream<Item = S::Item> + Send
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
{
    stream.map(move |chunk| {
        if let Ok(bytes) = &chunk {
            activity.bump(bytes.len() as u64);
        }
        chunk
    })
}

/// Turn an in-memory buffer into a chunked byte stream suitable for a streamed
/// multipart part. Combined with [`track_activity`] this gives buffered uploads
/// (small metadata bodies, CAR archives) the same incremental activity signal
/// as file streams, so the idle policy applies to them too.
pub fn bytes_stream(data: Vec<u8>) -> impl Stream<Item = std::io::Result<Bytes>> + Send {
    let bytes = Bytes::from(data);
    let len = bytes.len();
    let offsets = (0..len).step_by(BYTES_STREAM_CHUNK);
    futures_util::stream::iter(offsets.map(move |start| {
        let end = (start + BYTES_STREAM_CHUNK).min(len);
        Ok(bytes.slice(start..end))
    }))
}

/// Watchdog future that resolves once `activity` has not advanced for `idle`.
///
/// The clock starts now, before the first byte is sent, so a stalled connection
/// setup counts as idle too. That deliberately bounds a pre-first-byte hang; it
/// is only noticeable when `idle` is very short relative to connection setup.
async fn watch_idle(activity: UploadActivity, idle: Duration) {
    // Poll finely enough to bound detection latency to roughly `idle + tick`.
    let tick = (idle / 4).max(Duration::from_millis(50));
    let mut last_value = activity.load();
    let mut last_change = Instant::now();
    loop {
        sleep(tick).await;
        let current = activity.load();
        if current != last_value {
            last_value = current;
            last_change = Instant::now();
        } else if last_change.elapsed() >= idle {
            return;
        }
    }
}

/// Run the upload future `fut` under `policy`.
///
/// Returns `Ok(fut output)` if the upload finished within the policy, or
/// `Err(TimeoutFired)` if a deadline tripped first (in which case `fut` is
/// dropped, cancelling the in-flight request). `activity` is only consulted for
/// [`UploadTimeout::Idle`]; callers must feed it via [`track_activity`] for the
/// idle policy to see progress, otherwise `Idle(d)` degrades to a total `d`.
///
/// The policy bounds exactly what `fut` covers. Callers pass the `send()` future
/// (request body plus response headers), so reading the response body afterwards
/// is not bounded here; for uploads that body is a tiny JSON object.
pub async fn run_upload<F, T>(
    policy: UploadTimeout,
    activity: UploadActivity,
    fut: F,
) -> Result<T, TimeoutFired>
where
    F: std::future::Future<Output = T>,
{
    match policy {
        UploadTimeout::None => Ok(fut.await),
        UploadTimeout::Total(d) => tokio::time::timeout(d, fut)
            .await
            .map_err(|_| TimeoutFired::Total(d)),
        UploadTimeout::Idle(d) => {
            tokio::pin!(fut);
            tokio::select! {
                out = &mut fut => Ok(out),
                () = watch_idle(activity, d) => Err(TimeoutFired::Idle(d)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_fired_messages() {
        assert_eq!(
            TimeoutFired::Total(Duration::from_secs(120)).to_string(),
            "exceeded total timeout of 120s"
        );
        assert_eq!(
            TimeoutFired::Idle(Duration::from_secs(60)).to_string(),
            "no data sent for 60s"
        );
        // Sub-second timeouts must not truncate to "0s".
        assert_eq!(
            TimeoutFired::Total(Duration::from_millis(200)).to_string(),
            "exceeded total timeout of 200ms"
        );
    }

    #[test]
    fn default_is_idle_120s() {
        assert_eq!(
            UploadTimeout::default(),
            UploadTimeout::Idle(Duration::from_secs(120))
        );
    }

    #[test]
    fn parses_seconds_as_idle() {
        assert_eq!(
            "300".parse::<UploadTimeout>().unwrap(),
            UploadTimeout::Idle(Duration::from_secs(300))
        );
        // Trimmed and tolerant of surrounding whitespace.
        assert_eq!(
            "  90 ".parse::<UploadTimeout>().unwrap(),
            UploadTimeout::Idle(Duration::from_secs(90))
        );
    }

    #[test]
    fn parses_disable_keywords() {
        for s in ["none", "OFF", "0", " none "] {
            assert_eq!(s.parse::<UploadTimeout>().unwrap(), UploadTimeout::None);
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!("soon".parse::<UploadTimeout>().is_err());
        assert!("-5".parse::<UploadTimeout>().is_err());
        assert!("".parse::<UploadTimeout>().is_err());
    }

    #[tokio::test]
    async fn bytes_stream_preserves_content_in_chunks() {
        let data: Vec<u8> = (0..(BYTES_STREAM_CHUNK * 2 + 7) as u32)
            .map(|i| i as u8)
            .collect();
        let collected: Vec<u8> = bytes_stream(data.clone())
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(collected, data);
    }

    #[tokio::test]
    async fn track_activity_counts_bytes() {
        let activity = UploadActivity::new();
        let stream = track_activity(bytes_stream(vec![0u8; 5000]), activity.clone());
        let _: Vec<_> = stream.collect().await;
        assert_eq!(activity.load(), 5000);
    }

    #[tokio::test(start_paused = true)]
    async fn none_never_times_out() {
        let activity = UploadActivity::new();
        let fut = async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            42
        };
        let out = run_upload(UploadTimeout::None, activity, fut).await;
        assert_eq!(out, Ok(42));
    }

    #[tokio::test(start_paused = true)]
    async fn total_fires_at_deadline() {
        let activity = UploadActivity::new();
        let fut = async {
            tokio::time::sleep(Duration::from_secs(300)).await;
            1
        };
        let out: Result<i32, _> = run_upload(
            UploadTimeout::Total(Duration::from_secs(120)),
            activity,
            fut,
        )
        .await;
        assert_eq!(out, Err(TimeoutFired::Total(Duration::from_secs(120))));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_fires_when_no_activity() {
        let activity = UploadActivity::new();
        // Upload that never makes progress and never finishes.
        let fut = async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            1
        };
        let out: Result<i32, _> =
            run_upload(UploadTimeout::Idle(Duration::from_secs(60)), activity, fut).await;
        assert_eq!(out, Err(TimeoutFired::Idle(Duration::from_secs(60))));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_resets_on_activity_and_completes() {
        let activity = UploadActivity::new();
        let bumper = activity.clone();
        // A "slow but alive" upload: bumps activity every 30s for 5 minutes,
        // then finishes. Each gap is under the 60s idle window, so it survives.
        let fut = async move {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_secs(30)).await;
                bumper.bump(1024);
            }
            "done"
        };
        let out = run_upload(UploadTimeout::Idle(Duration::from_secs(60)), activity, fut).await;
        assert_eq!(out, Ok("done"));
    }
}
