//! In-process byte source for VFS streaming.
//!
//! The FUSE layer (`riven-vfs`) is origin-agnostic: for debrid entries it
//! fetches byte ranges over HTTP from a remote CDN. For usenet entries it
//! calls the streamer **in process** through `LocalByteSource` — no loopback
//! HTTP, no duplicate read-ahead. The trait lives in `riven-core` so
//! `riven-vfs` depends only on the abstraction, not on `riven-usenet`.

use std::sync::Arc;

use bytes::Bytes;

/// How an origin is physically chunked, so read-ahead fetches whole units of
/// the origin's own instead of straddling them.
///
/// Presence is the signal: a layout means the origin fetches articles, and
/// read-ahead sizes its cushion in bytes and divides by `chunk_size`. Absence
/// means a plain ranged HTTP origin, chunked into 8 MiB by the reader.
#[derive(Debug, Clone)]
pub struct SourceLayout {
    /// Natural fetch unit — one usenet article's decoded size.
    pub chunk_size: u64,
}

/// A source of open files, implemented in-process by the usenet streamer.
///
/// The unit here is deliberately the **open file**, not `(info_hash,
/// file_index)` per read. An origin generally has to resolve something before
/// it can serve a byte — for usenet, the file's segment map — and resolving it
/// per read means either paying for it every time or keeping a cache with an
/// eviction policy that is a guess about which files will be read again.
///
/// There is nothing to guess. The kernel says exactly which files are being
/// read, by holding them open, and it says when it is done via `release`. So
/// the origin resolves once per handle and holds it for that handle's life:
/// no budget to size, no eviction to tune, no lock on the read path, and
/// memory that scales with concurrent streams rather than with a number
/// someone picked.
pub trait LocalByteSource: Send + Sync {
    /// Begin serving one file. Returns immediately — `fuser`'s `open` is
    /// synchronous — and the returned handle resolves what it needs on first
    /// use, keeping it until dropped.
    fn open_file(&self, info_hash: &str, file_index: usize) -> Arc<dyn LocalOpenFile>;

    /// Active-stream registry hooks, driving the dashboard's "now playing"
    /// view. The VFS calls these as it serves a usenet handle. `key`
    /// uniquely identifies an open handle (e.g. `"{info_hash}:{file_index}"`).
    fn stream_register(&self, key: &str, info_hash: &str, filename: &str, file_size: u64);
    fn stream_touch(&self, key: &str);
    fn stream_unregister(&self, key: &str);
}

/// One open file, for as long as the handle above it lives.
#[async_trait::async_trait]
pub trait LocalOpenFile: Send + Sync {
    /// Chunking of this file, when the origin has a natural one.
    async fn layout(&self) -> Option<SourceLayout> {
        None
    }

    /// Read the inclusive byte range `[start, end]`. Returns the decoded bytes
    /// (which may be slightly shorter than requested at the tail of a segment —
    /// callers must tolerate a short read, as they already do for HTTP origins
    /// that cap their window).
    async fn read_range(&self, start: u64, end_inclusive: u64) -> anyhow::Result<Bytes>;
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
