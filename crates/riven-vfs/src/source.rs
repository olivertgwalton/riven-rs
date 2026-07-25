//! One byte source per backend, behind a single trait.
//!
//! Before this, usenet and HTTP origins each had their own reader, cache and
//! prefetch machinery (`UsenetSession` vs `MediaStream` + `chunks` + `detect`
//! + `RangeCache`). They differ in exactly one respect — how a byte range is
//! fetched — so that is all this trait exposes. Everything above it
//! (buffering, read-ahead, sequential detection) is shared, in [`crate::prefetch`].

use async_trait::async_trait;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use riven_core::local_source::LocalByteSource;

/// Fetches byte ranges of one open file.
///
/// `#[async_trait]` because handles store these as `Arc<dyn ByteSource>`.
#[async_trait]
pub trait ByteSource: Send + Sync {
    /// Fetch the inclusive range `[start, end]`.
    ///
    /// A short read is allowed — origins cap their own windows — but callers
    /// must never forward one mid-file to the kernel (see [`crate::prefetch`]).
    async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes>;

    /// Total file size in bytes.
    fn size(&self) -> u64;

    /// Tell the origin where the player is, for origins that own their own
    /// read-ahead. No-op by default.
    async fn report_position(&self, _position: u64) {}
}

/// Usenet-backed file. Read-ahead lives in `riven-usenet`, which adapts depth
/// and parallelism from the reported position, so this only forwards.
pub struct UsenetSource {
    inner: Arc<dyn LocalByteSource>,
    info_hash: Arc<str>,
    file_index: usize,
    size: u64,
    /// Active-streams registry key, powering the dashboard's "now playing".
    /// Registered for the life of the handle and released on drop, so the
    /// entry cannot outlive playback even if a read fails.
    stream_key: String,
}

impl UsenetSource {
    pub fn new(
        inner: Arc<dyn LocalByteSource>,
        info_hash: Arc<str>,
        file_index: usize,
        size: u64,
        filename: &str,
    ) -> Self {
        let stream_key = format!("{info_hash}:{file_index}");
        inner.stream_register(&stream_key, &info_hash, filename, size);
        Self {
            inner,
            info_hash,
            file_index,
            size,
            stream_key,
        }
    }
}

impl Drop for UsenetSource {
    fn drop(&mut self) {
        self.inner.stream_unregister(&self.stream_key);
    }
}

#[async_trait]
impl ByteSource for UsenetSource {
    async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
        self.inner
            .read_range(&self.info_hash, self.file_index, start, end)
            .await
            .map_err(io::Error::other)
    }

    fn size(&self) -> u64 {
        self.size
    }

    async fn report_position(&self, _position: u64) {
        // Read-ahead lives in `crate::prefetch` now, so the origin is only
        // told the stream is alive — it no longer runs a second, competing
        // read-ahead of its own.
        self.inner.stream_touch(&self.stream_key);
    }
}

/// Mints a fresh URL for a handle whose link has expired. Blocking (it hits
/// the debrid API and persists the result), so it is called off-runtime.
pub type LinkRefresher = Arc<dyn Fn() -> Option<Arc<str>> + Send + Sync>;

/// HTTP origin (debrid). Serves ranges with a plain `Range:` request; the
/// shared prefetcher above supplies the sequencing and buffering that
/// `MediaStream`'s bespoke chunk/cache layer used to.
pub struct HttpSource {
    client: reqwest::Client,
    /// Swappable: debrid links expire mid-playback and are re-minted in place.
    url: parking_lot::Mutex<Arc<str>>,
    size: u64,
    refresh: Option<LinkRefresher>,
}

impl HttpSource {
    pub fn new(
        client: reqwest::Client,
        url: Arc<str>,
        size: u64,
        refresh: Option<LinkRefresher>,
    ) -> Self {
        Self {
            client,
            url: parking_lot::Mutex::new(url),
            size,
            refresh,
        }
    }

    fn url(&self) -> Arc<str> {
        Arc::clone(&self.url.lock())
    }

    async fn get(&self, url: &str, start: u64, end: u64) -> Result<Bytes, String> {
        let response = self
            .client
            .get(url)
            .header(reqwest::header::RANGE, format!("bytes={start}-{end}"))
            .send()
            .await
            .map_err(|error| error.to_string())?;

        let status = response.status();
        if !status.is_success() {
            return Err(format!("origin returned {status}"));
        }
        response
            .bytes()
            .await
            .map_err(|error| error.to_string())
    }
}

#[async_trait]
impl ByteSource for HttpSource {
    async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
        let url = self.url();
        let first = match self.get(&url, start, end).await {
            Ok(data) => return Ok(data),
            Err(error) => error,
        };

        // A debrid link can die mid-playback (expiry, or the entry re-downloaded
        // underneath us). Re-mint it once and retry rather than failing the
        // read: the player would otherwise see EIO and stop.
        let Some(refresh) = self.refresh.clone() else {
            return Err(io::Error::other(first));
        };
        let fresh = tokio::task::spawn_blocking(move || refresh())
            .await
            .ok()
            .flatten();
        let Some(fresh) = fresh else {
            return Err(io::Error::other(first));
        };

        // Another read may have refreshed first; last writer wins and both
        // converge on a live URL.
        *self.url.lock() = Arc::clone(&fresh);
        tracing::warn!(target: "streaming", error = %first, "stream link failed; retrying on a fresh link");

        self.get(&fresh, start, end).await.map_err(io::Error::other)
    }

    fn size(&self) -> u64 {
        self.size
    }
}
