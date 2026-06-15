//! In-process byte source for VFS streaming.
//!
//! The FUSE layer (`riven-vfs`) is origin-agnostic: for debrid entries it
//! fetches byte ranges over HTTP from a remote CDN. For usenet entries it
//! calls the streamer **in process** through `LocalByteSource` — no loopback
//! HTTP, no duplicate read-ahead. The trait lives in `riven-core` so
//! `riven-vfs` depends only on the abstraction, not on `riven-usenet`.

use bytes::Bytes;

/// A read-by-range byte source addressed by `(info_hash, file_index)`,
/// implemented in-process by the usenet streamer.
#[async_trait::async_trait]
pub trait LocalByteSource: Send + Sync {
    /// Read the inclusive byte range `[start, end]` of the file. Returns the
    /// decoded bytes (which may be slightly shorter than requested at the
    /// tail of a segment — callers must tolerate a short read, as they
    /// already do for HTTP origins that cap their window).
    async fn read_range(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> anyhow::Result<Bytes>;

    /// Warm the segment cache for the inclusive range `[start, end]` ahead of
    /// the live read position. Fire-and-forget from the caller's side: the
    /// implementation bounds its own concurrency and deduplicates against
    /// in-flight and already-cached segments, so calling it on every read with
    /// an overlapping look-ahead window is cheap. This is how sequential
    /// playback builds a read-ahead lead — independent of the per-handle read
    /// serialization — so a slow segment is fetched well before the player
    /// reaches it rather than stalling the read.
    async fn prefetch(&self, info_hash: &str, file_index: usize, start: u64, end_inclusive: u64);

    /// Active-stream registry hooks, driving the dashboard's "now playing"
    /// view. The VFS calls these as it serves a usenet handle. `key`
    /// uniquely identifies an open handle (e.g. `"{info_hash}:{file_index}"`).
    fn stream_register(&self, key: &str, info_hash: &str, filename: &str, file_size: u64);
    fn stream_touch(&self, key: &str);
    fn stream_unregister(&self, key: &str);
}

/// Parse a `usenet://{info_hash}/{file_index}` stream marker into
/// `(info_hash, file_index)`. Returns `None` for anything else (e.g. a debrid
/// CDN link). This is only the fallback for rows whose explicit
/// `usenet_info_hash`/`usenet_file_index` columns aren't populated; entries
/// are normally identified by those columns directly.
pub fn parse_usenet_url(url: &str) -> Option<(String, usize)> {
    let rest = url.strip_prefix("usenet://")?;
    let rest = rest.split(['?', '#']).next().unwrap_or(rest);
    let mut parts = rest.split('/');
    let info_hash = parts.next()?;
    let file_index = parts.next()?.parse::<usize>().ok()?;
    if info_hash.is_empty() || parts.next().is_some() {
        return None;
    }
    Some((info_hash.to_string(), file_index))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_usenet_url() {
        assert_eq!(
            parse_usenet_url("usenet://nzb-abc123/0"),
            Some(("nzb-abc123".to_string(), 0))
        );
        assert_eq!(
            parse_usenet_url("usenet://nzb-deadbeef/3?x=1"),
            Some(("nzb-deadbeef".to_string(), 3))
        );
    }

    #[test]
    fn rejects_non_usenet_url() {
        assert_eq!(
            parse_usenet_url("https://debrid.example/dl/token/file.mkv"),
            None
        );
        assert_eq!(parse_usenet_url("usenet://onlyhash"), None);
        assert_eq!(parse_usenet_url("usenet://hash/0/extra"), None);
        assert_eq!(parse_usenet_url("usenet://hash/notanumber"), None);
    }
}
