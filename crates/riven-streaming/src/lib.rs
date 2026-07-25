//! Shared origin layer for every streaming consumer.
//!
//! The API and FUSE adapters intentionally differ in how they deliver bytes,
//! but they use this crate for target classification, source construction,
//! link renewal, range correctness, and stream lifetime tracking.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use reqwest::header::{CONTENT_RANGE, RANGE};
use riven_core::local_source::LocalByteSource;
use riven_core::stream_link::{LinkRequest, request_stream_url};
use tokio::sync::{Mutex, mpsc};

/// The backend-specific identity needed to open a playable entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamTarget {
    Usenet {
        info_hash: String,
        file_index: usize,
    },
    Http,
}

/// Classify an entry consistently for every consumer.
///
/// Explicit Usenet columns take precedence. Legacy rows fall back to their
/// `usenet://` marker; everything else is an HTTP/debrid target.
pub fn classify_stream_target(
    usenet_info_hash: Option<&str>,
    usenet_file_index: Option<i64>,
    stream_url: Option<&str>,
    download_url: Option<&str>,
) -> StreamTarget {
    if let (Some(info_hash), Some(file_index)) = (usenet_info_hash, usenet_file_index)
        && !info_hash.is_empty()
        && let Ok(file_index) = usize::try_from(file_index)
    {
        return StreamTarget::Usenet {
            info_hash: info_hash.to_owned(),
            file_index,
        };
    }

    if let Some((info_hash, file_index)) = stream_url
        .or(download_url)
        .and_then(riven_core::local_source::parse_usenet_url)
    {
        return StreamTarget::Usenet {
            info_hash,
            file_index,
        };
    }

    StreamTarget::Http
}

/// Inputs required to mint and persist a debrid stream URL.
#[derive(Clone, Debug)]
pub struct LinkSpec {
    pub entry_id: i64,
    pub download_url: Option<String>,
    pub provider: Option<String>,
}

/// One coalesced, persistent link resolver shared by API and FUSE.
pub struct StreamLinkResolver {
    request_tx: mpsc::Sender<LinkRequest>,
    locks: DashMap<i64, Arc<Mutex<()>>>,
}

impl StreamLinkResolver {
    pub fn new(request_tx: mpsc::Sender<LinkRequest>) -> Self {
        Self {
            request_tx,
            locks: DashMap::new(),
        }
    }

    /// Resolve a URL, coalescing simultaneous requests for the same entry.
    ///
    /// `current_url` is the URL the caller already knows is stale. After
    /// taking the lock the database is rechecked, so a peer's newly persisted
    /// URL wins without another provider request.
    pub async fn resolve(&self, spec: &LinkSpec, current_url: Option<&str>) -> Option<Arc<str>> {
        let lock = self
            .locks
            .entry(spec.entry_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let guard = lock.lock().await;

        if let Ok(Some(entry)) = riven_db::repo::get_media_entry_by_id(spec.entry_id).await
            && let Some(fresh) = entry.stream_url
            && Some(fresh.as_str()) != current_url
        {
            drop(guard);
            self.remove_unused_lock(spec.entry_id);
            return Some(Arc::from(fresh));
        }

        let url = request_stream_url(
            spec.download_url.as_deref(),
            spec.provider.as_deref(),
            Some(spec.entry_id),
            current_url,
            &self.request_tx,
        )
        .await;

        if let Some(url) = url.as_deref()
            && let Err(error) = riven_db::repo::update_stream_url(spec.entry_id, url).await
        {
            tracing::warn!(
                entry_id = spec.entry_id,
                %error,
                "failed to persist refreshed stream url"
            );
        }

        drop(guard);
        self.remove_unused_lock(spec.entry_id);
        url.map(Arc::from)
    }

    pub fn resolve_blocking(
        &self,
        spec: &LinkSpec,
        current_url: Option<&str>,
        runtime: &tokio::runtime::Handle,
    ) -> Option<Arc<str>> {
        runtime.block_on(self.resolve(spec, current_url))
    }

    fn remove_unused_lock(&self, entry_id: i64) {
        self.locks
            .remove_if(&entry_id, |_, lock| Arc::strong_count(lock) <= 2);
    }
}

/// Fetches ranges for one open file. Buffering belongs to the consumer.
#[async_trait]
pub trait ByteSource: Send + Sync {
    async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes>;
    fn size(&self) -> u64;
    fn report_position(&self, _position: u64) {}
}

/// Constructs origin sources and owns their shared link resolver.
pub struct SourceFactory {
    client: reqwest::Client,
    local_source: Option<Arc<dyn LocalByteSource>>,
    links: Arc<StreamLinkResolver>,
}

impl SourceFactory {
    pub fn new(
        client: reqwest::Client,
        request_tx: mpsc::Sender<LinkRequest>,
        local_source: Option<Arc<dyn LocalByteSource>>,
    ) -> Self {
        Self {
            client,
            local_source,
            links: Arc::new(StreamLinkResolver::new(request_tx)),
        }
    }

    pub async fn resolve_link(
        &self,
        spec: &LinkSpec,
        current_url: Option<&str>,
    ) -> Option<Arc<str>> {
        self.links.resolve(spec, current_url).await
    }

    pub fn http_client(&self) -> &reqwest::Client {
        &self.client
    }

    pub fn resolve_link_blocking(
        &self,
        spec: &LinkSpec,
        current_url: Option<&str>,
        runtime: &tokio::runtime::Handle,
    ) -> Option<Arc<str>> {
        self.links.resolve_blocking(spec, current_url, runtime)
    }

    pub fn open_usenet(
        &self,
        info_hash: &str,
        file_index: usize,
        size: u64,
        filename: &str,
    ) -> Option<Arc<dyn ByteSource>> {
        self.local_source.as_ref().map(|source| {
            Arc::new(UsenetSource::new(
                Arc::clone(source),
                Arc::from(info_hash),
                file_index,
                size,
                filename,
            )) as Arc<dyn ByteSource>
        })
    }

    pub fn open_http(
        self: &Arc<Self>,
        url: Arc<str>,
        size: u64,
        link: LinkSpec,
        runtime: tokio::runtime::Handle,
    ) -> Arc<dyn ByteSource> {
        Arc::new(HttpSource::new(
            self.client.clone(),
            url,
            size,
            Arc::clone(self),
            link,
            runtime,
        ))
    }
}

pub struct UsenetSource {
    inner: Arc<dyn LocalByteSource>,
    info_hash: Arc<str>,
    file_index: usize,
    size: u64,
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
        static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);
        let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed);
        let stream_key = format!("{info_hash}:{file_index}:{stream_id}");
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

    fn report_position(&self, _position: u64) {
        self.inner.stream_touch(&self.stream_key);
    }
}

struct HttpSource {
    client: reqwest::Client,
    url: parking_lot::Mutex<Arc<str>>,
    size: u64,
    factory: Arc<SourceFactory>,
    link: LinkSpec,
    runtime: tokio::runtime::Handle,
}

impl HttpSource {
    fn new(
        client: reqwest::Client,
        url: Arc<str>,
        size: u64,
        factory: Arc<SourceFactory>,
        link: LinkSpec,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            client,
            url: parking_lot::Mutex::new(url),
            size,
            factory,
            link,
            runtime,
        }
    }

    async fn get(&self, url: &str, start: u64, end: u64) -> Result<Bytes, String> {
        let response = self
            .client
            .get(url)
            .header(RANGE, format!("bytes={start}-{end}"))
            .send()
            .await
            .map_err(|error| error.to_string())?;

        let status = response.status();
        let content_range = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let response_range = validate_http_range_response(
            status.as_u16(),
            content_range.as_deref(),
            start,
            end,
            self.size,
        )?;
        let bytes = response.bytes().await.map_err(|error| error.to_string())?;
        let expected_len = response_range.1 - response_range.0 + 1;
        if bytes.len() as u64 != expected_len {
            return Err(format!(
                "origin body length {} does not match Content-Range length {expected_len}",
                bytes.len()
            ));
        }
        Ok(bytes)
    }
}

#[async_trait]
impl ByteSource for HttpSource {
    async fn read_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
        let url = Arc::clone(&self.url.lock());
        let first = match self.get(&url, start, end).await {
            Ok(data) => return Ok(data),
            Err(error) => error,
        };

        let factory = Arc::clone(&self.factory);
        let link = self.link.clone();
        let runtime = self.runtime.clone();
        let stale = Arc::clone(&url);
        let fresh = tokio::task::spawn_blocking(move || {
            factory.resolve_link_blocking(&link, Some(&stale), &runtime)
        })
        .await
        .ok()
        .flatten();
        let Some(fresh) = fresh else {
            return Err(io::Error::other(first));
        };

        *self.url.lock() = Arc::clone(&fresh);
        tracing::warn!(
            target: "streaming",
            error = %first,
            "stream link failed; retrying on a fresh link"
        );
        self.get(&fresh, start, end).await.map_err(io::Error::other)
    }

    fn size(&self) -> u64 {
        self.size
    }
}

/// Validate that an origin honoured the requested byte range.
///
/// A shorter range is valid because some CDNs cap response windows; the next
/// exact-read iteration continues at the returned end.
pub fn validate_http_range_response(
    status: u16,
    content_range: Option<&str>,
    requested_start: u64,
    requested_end: u64,
    file_size: u64,
) -> Result<(u64, u64), String> {
    if status != 206 {
        return Err(format!(
            "origin ignored bytes={requested_start}-{requested_end}: returned HTTP {status}"
        ));
    }
    let value =
        content_range.ok_or_else(|| "HTTP 206 response omitted Content-Range".to_string())?;
    let value = value
        .strip_prefix("bytes ")
        .ok_or_else(|| format!("invalid Content-Range {value:?}"))?;
    let (range, total) = value
        .split_once('/')
        .ok_or_else(|| format!("invalid Content-Range {value:?}"))?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| format!("invalid Content-Range {value:?}"))?;
    let start = start
        .parse::<u64>()
        .map_err(|error| format!("invalid Content-Range start {start:?}: {error}"))?;
    let end = end
        .parse::<u64>()
        .map_err(|error| format!("invalid Content-Range end {end:?}: {error}"))?;
    let total = total
        .parse::<u64>()
        .map_err(|error| format!("invalid Content-Range size {total:?}: {error}"))?;

    if start != requested_start || end < start || end > requested_end || total != file_size {
        return Err(format!(
            "Content-Range bytes {start}-{end}/{total} does not satisfy \
             bytes={requested_start}-{requested_end} for size {file_size}"
        ));
    }
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    struct LifecycleSource {
        registered: AtomicUsize,
        touched: AtomicUsize,
        unregistered: AtomicUsize,
    }

    #[async_trait]
    impl LocalByteSource for LifecycleSource {
        async fn read_range(
            &self,
            _info_hash: &str,
            _file_index: usize,
            _start: u64,
            _end_inclusive: u64,
        ) -> anyhow::Result<Bytes> {
            Ok(Bytes::new())
        }

        fn stream_register(&self, _key: &str, _info_hash: &str, _filename: &str, _file_size: u64) {
            self.registered.fetch_add(1, Ordering::Relaxed);
        }

        fn stream_touch(&self, _key: &str) {
            self.touched.fetch_add(1, Ordering::Relaxed);
        }

        fn stream_unregister(&self, _key: &str) {
            self.unregistered.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn classifies_explicit_and_legacy_usenet_targets() {
        assert_eq!(
            classify_stream_target(Some("nzb-a"), Some(3), None, None),
            StreamTarget::Usenet {
                info_hash: "nzb-a".to_string(),
                file_index: 3,
            }
        );
        assert_eq!(
            classify_stream_target(None, None, Some("usenet://nzb-b/7?x=1"), None),
            StreamTarget::Usenet {
                info_hash: "nzb-b".to_string(),
                file_index: 7,
            }
        );
        assert_eq!(
            classify_stream_target(None, None, Some("https://cdn/file.mkv"), None),
            StreamTarget::Http
        );
    }

    #[test]
    fn rejects_invalid_explicit_index_without_turning_it_into_zero() {
        assert_eq!(
            classify_stream_target(Some("nzb-a"), Some(-1), None, None),
            StreamTarget::Http
        );
    }

    #[test]
    fn usenet_source_tracks_its_complete_lifecycle() {
        let lifecycle = Arc::new(LifecycleSource {
            registered: AtomicUsize::new(0),
            touched: AtomicUsize::new(0),
            unregistered: AtomicUsize::new(0),
        });
        let local: Arc<dyn LocalByteSource> = lifecycle.clone();
        let source = UsenetSource::new(local, Arc::from("nzb-a"), 0, 100, "movie.mkv");

        assert_eq!(lifecycle.registered.load(Ordering::Relaxed), 1);
        source.report_position(0);
        assert_eq!(lifecycle.touched.load(Ordering::Relaxed), 1);
        drop(source);
        assert_eq!(lifecycle.unregistered.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn validates_partial_and_capped_ranges() {
        assert_eq!(
            validate_http_range_response(206, Some("bytes 10-19/100"), 10, 19, 100),
            Ok((10, 19))
        );
        assert_eq!(
            validate_http_range_response(206, Some("bytes 10-14/100"), 10, 19, 100),
            Ok((10, 14))
        );
    }

    #[test]
    fn rejects_ignored_or_mismatched_ranges() {
        assert!(validate_http_range_response(200, None, 10, 19, 100).is_err());
        assert!(validate_http_range_response(206, None, 10, 19, 100).is_err());
        assert!(validate_http_range_response(206, Some("bytes 0-9/100"), 10, 19, 100).is_err());
        assert!(validate_http_range_response(206, Some("bytes 10-19/200"), 10, 19, 100).is_err());
    }
}
