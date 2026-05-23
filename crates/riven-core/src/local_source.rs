//! In-process byte source for VFS streaming.
//!
//! The FUSE layer (`riven-vfs`) is origin-agnostic: it fetches byte ranges
//! over HTTP from a stream URL. For debrid that URL is a remote CDN. For
//! usenet it used to be the loopback `/usenet/{info_hash}/{file_index}`
//! route on riven's own HTTP server — so a Plex read went FUSE → HTTP →
//! the usenet streamer, two streaming layers with their own buffering on
//! each side of a localhost socket.
//!
//! `LocalByteSource` lets the usenet streamer be called **in process**
//! instead. `riven-vfs` recognises a usenet stream URL, looks up the
//! injected source, and reads ranges directly — no loopback HTTP, no
//! duplicate read-ahead. The trait lives in `riven-core` so `riven-vfs`
//! depends only on the abstraction, not on `riven-usenet`.

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

    /// Open a contiguous byte stream from `start` to EOF, eagerly
    /// pipelined. Used for sequential playback so a single slow segment is
    /// absorbed by the read-ahead buffer rather than stalling the reader.
    async fn open_stream(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
    ) -> anyhow::Result<futures::stream::BoxStream<'static, std::io::Result<Bytes>>>;
}

/// Parse a usenet stream URL of the shape `…/usenet/{info_hash}/{file_index}`
/// into its components. Returns `None` for any other URL (e.g. a debrid CDN
/// link), which routes the read through the normal HTTP path.
pub fn parse_usenet_url(url: &str) -> Option<(String, usize)> {
    let rest = url.split("/usenet/").nth(1)?;
    // Strip any query string / fragment.
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
            parse_usenet_url("https://riven.example/usenet/nzb-abc123/0"),
            Some(("nzb-abc123".to_string(), 0))
        );
        assert_eq!(
            parse_usenet_url("http://127.0.0.1:8080/usenet/nzb-deadbeef/3?x=1"),
            Some(("nzb-deadbeef".to_string(), 3))
        );
    }

    #[test]
    fn rejects_non_usenet_url() {
        assert_eq!(parse_usenet_url("https://debrid.example/dl/token/file.mkv"), None);
        assert_eq!(parse_usenet_url("https://x/usenet/onlyhash"), None);
        assert_eq!(parse_usenet_url("https://x/usenet/hash/0/extra"), None);
        assert_eq!(parse_usenet_url("https://x/usenet/hash/notanumber"), None);
    }
}
