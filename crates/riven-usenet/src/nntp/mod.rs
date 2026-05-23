//! Tiny async NNTP client.
//!
//! Implements just enough of RFC 3977 to log in (AUTHINFO USER/PASS) and
//! fetch article bodies (BODY <message-id>). Article bodies are
//! `.`-terminated and dot-stuffed; we undo that here so callers receive a
//! clean payload to hand to the yEnc decoder.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;

mod connection;
mod pool;
mod priority_semaphore;

pub use connection::NntpConnection;
pub use pool::NntpPool;
pub use priority_semaphore::Priority;

#[derive(Clone)]
pub struct NntpServerConfig {
    pub host: String,
    pub port: u16,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub use_tls: bool,
    pub max_connections: u32,
    pub timeout: Duration,
}

// Manual Debug: redact `pass` so accidental `tracing::debug!(?cfg)` or
// panic backtraces don't print credentials.
impl std::fmt::Debug for NntpServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NntpServerConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("pass", &self.pass.as_deref().map(|_| "<redacted>"))
            .field("use_tls", &self.use_tls)
            .field("max_connections", &self.max_connections)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NntpError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("tls error: {0}")]
    Tls(String),
    #[error("auth failed: {0}")]
    AuthFailed(String),
    #[error("server error: {0}")]
    ServerError(String),
    #[error("article not found: {0}")]
    ArticleNotFound(String),
    #[error("protocol error: {0}")]
    Protocol(&'static str),
    #[error("timed out")]
    Timeout,
}

pub(crate) enum NntpStream {
    Plain(BufReader<TcpStream>),
    // Boxed: rustls' TlsStream is ~1KB on the stack and dwarfs the Plain variant.
    Tls(Box<BufReader<tokio_rustls::client::TlsStream<TcpStream>>>),
}

impl NntpStream {
    /// Read a single line, failing with `TimedOut` if no data arrives within
    /// `timeout`. The deadline is per call, so it acts as an inactivity timeout
    /// (matching nntppool's `SetReadDeadline`): a slow-but-progressing transfer
    /// keeps resetting it, while a half-dead socket trips it.
    pub(crate) async fn read_line(
        &mut self,
        buf: &mut String,
        timeout: Duration,
    ) -> io::Result<usize> {
        tokio::time::timeout(timeout, async {
            match self {
                NntpStream::Plain(s) => s.read_line(buf).await,
                NntpStream::Tls(s) => s.read_line(buf).await,
            }
        })
        .await
        .map_err(|_e| io::Error::new(io::ErrorKind::TimedOut, "nntp read timed out"))?
    }

    /// Read a `.`-terminated multi-line response into the caller-provided
    /// `out`. `timeout` applies as an inactivity deadline shared across
    /// every read in this call: a single `tokio::time::Sleep` is pinned
    /// once outside the loop and `reset()` each time a read returns data,
    /// rather than wrapping each read in a fresh `tokio::time::timeout()`
    /// future. Profile showed ~0.5 % of CPU in `Timeout::poll`'s memset.
    ///
    /// `out` is cleared on entry; the caller threads it from the encoded
    /// buffer pool (a `crate::bufpool::PooledBuf`) so the next BODY fetch
    /// reuses its hot pages.
    ///
    /// Reads bulk chunks and scans for the `\r\n.\r\n` terminator with
    /// `memmem` (replaces a previous per-line `read_until(b'\n', ...)`
    /// loop — a ~700 KB body has ~5,500 lines, so the line loop did
    /// ~5,500 memchr scans + extend_from_slice copies per article).
    /// Dot-stuffing is undone in a single pass at the end; the common
    /// case (no stuffed lines) skips that work entirely.
    pub(crate) async fn read_until_dot(
        &mut self,
        out: &mut Vec<u8>,
        timeout: Duration,
    ) -> io::Result<()> {
        // RFC 3977: multi-line response is terminated by `<CRLF>.<CRLF>`.
        // Lines beginning with `.` are dot-stuffed (sender doubles the leading
        // `.`). Strictly searching for `\r\n.\r\n` is unambiguous because
        // a dot-stuffed line would be `\r\n..<more>`.
        const TERMINATOR: &[u8] = b"\r\n.\r\n";

        out.clear();
        let mut scanned: usize = 0;

        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);

        let term_end = loop {
            // Search only what we haven't seen yet, plus a 4-byte overlap so a
            // terminator straddling the previous read boundary isn't missed.
            let scan_from = scanned.saturating_sub(TERMINATOR.len() - 1);
            if out.len() >= TERMINATOR.len()
                && let Some(rel) = memchr::memmem::find(&out[scan_from..], TERMINATOR)
            {
                break scan_from + rel + TERMINATOR.len();
            }
            // Special case: the response is the terminator itself (empty body).
            if out.len() >= 3 && &out[..3] == b".\r\n" {
                break 3;
            }
            scanned = out.len();

            let read_fut = async {
                match self {
                    NntpStream::Plain(s) => fill_into(s, out).await,
                    NntpStream::Tls(s) => fill_into(s, out).await,
                }
            };
            tokio::pin!(read_fut);
            tokio::select! {
                biased;
                r = &mut read_fut => r?,
                _ = &mut sleep => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "nntp read timed out",
                    ));
                }
            };
            // Extend the inactivity deadline: data made it through, so the
            // socket is still alive even if the next read stalls briefly.
            sleep.as_mut().reset(tokio::time::Instant::now() + timeout);
        };

        // Trim everything from the terminator's leading `\r\n` (inclusive of
        // the trailing CRLF on the body's last line, which the caller's yEnc
        // decoder expects) onward.
        out.truncate(term_end - 3);
        undot_stuff(out);
        Ok(())
    }

    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            NntpStream::Plain(s) => s.get_mut().write_all(buf).await,
            NntpStream::Tls(s) => s.get_mut().write_all(buf).await,
        }
    }

    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        match self {
            NntpStream::Plain(s) => s.get_mut().flush().await,
            NntpStream::Tls(s) => s.get_mut().flush().await,
        }
    }
}

/// Read more bytes from `reader` into the tail of `buf`, growing the
/// buffer's capacity if needed. Returns the number of bytes appended.
/// Wraps `AsyncRead::read` rather than `read_until` so we can pull large
/// chunks (typically a full BufReader fill, 512 KB at a time) instead of
/// one line per call.
async fn fill_into<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> io::Result<usize> {
    const READ_CHUNK: usize = 64 * 1024;
    let n = buf.len();
    buf.resize(n + READ_CHUNK, 0);
    let got = reader.read(&mut buf[n..]).await?;
    buf.truncate(n + got);
    if got == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "EOF before terminating dot",
        ));
    }
    Ok(got)
}

/// Un-dot-stuff an NNTP article body in place. RFC 3977 §3.1.1: a line
/// beginning with `.` is sent with the leading `.` doubled. We remove the
/// extra dot. Most yEnc articles have ~20 stuffed lines per 700 KB
/// segment (a yEnc-encoded byte happens to be `.`/0x2E ≈ 1/256 lines);
/// the common case still pays one memmem scan + a tight rewrite. The
/// no-stuffing case (cleaner NZB postings) skips the rewrite entirely.
fn undot_stuff(buf: &mut Vec<u8>) {
    let has_leading = buf.starts_with(b"..");
    let has_interior = memchr::memmem::find(buf, b"\r\n..").is_some();
    if !has_leading && !has_interior {
        return;
    }

    // In-place compact: read from `r`, write to `w` (always `w <= r`).
    // Skips the second `.` of every `..` that opens a line.
    let len = buf.len();
    let mut r = 0;
    let mut w = 0;
    let mut prev_was_cr = false;
    let mut at_line_start = true;
    while r < len {
        let b = buf[r];
        if at_line_start && b == b'.' && r + 1 < len && buf[r + 1] == b'.' {
            buf[w] = b'.';
            w += 1;
            r += 2;
            at_line_start = false;
            prev_was_cr = false;
            continue;
        }
        buf[w] = b;
        w += 1;
        r += 1;
        at_line_start = prev_was_cr && b == b'\n';
        prev_was_cr = b == b'\r';
    }
    buf.truncate(w);
}

pub(crate) fn build_tls_connector() -> Result<TlsConnector, NntpError> {
    static CONFIG: parking_lot::Mutex<Option<Arc<ClientConfig>>> = parking_lot::Mutex::new(None);
    let mut guard = CONFIG.lock();
    if guard.is_none() {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let cfg = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        *guard = Some(Arc::new(cfg));
    }
    let cfg = guard.as_ref().unwrap().clone();
    Ok(TlsConnector::from(cfg))
}

/// One configured NNTP provider with its own bounded connection pool.
/// `priority` orders providers: lower number = tried first. `is_backup`
/// flags block accounts / fill providers — only consulted after every
/// primary returned `ArticleNotFound`.
#[derive(Debug, Clone)]
pub struct NntpProvider {
    pub config: NntpServerConfig,
    pub priority: i32,
    pub is_backup: bool,
}

/// Multi-provider configuration handed to `UsenetStreamer::shared`.
#[derive(Debug, Clone)]
pub struct NntpConfig {
    /// One or more NNTP providers ordered by intent. A single primary is the
    /// common case. Order doesn't matter for ingest; the pool sorts internally
    /// by `(is_backup, priority)`.
    pub providers: Vec<NntpProvider>,
}

impl NntpConfig {
    pub fn primary(&self) -> Option<&NntpServerConfig> {
        self.providers.first().map(|p| &p.config)
    }

    /// Total primary (non-backup) connection budget summed across providers —
    /// the real ceiling the pool enforces via its `PrioritizedSemaphore`.
    pub fn total_max_connections(&self) -> usize {
        self.providers
            .iter()
            .filter(|p| !p.is_backup)
            .map(|p| p.config.max_connections.max(1) as usize)
            .sum::<usize>()
            .max(1)
    }
}

/// Default number of concurrent download/ingest workers. Deliberately small —
/// not scaled to fill the connection pool. On usenet, total throughput is
/// bounded by your line, so many concurrent ingests don't drain a backlog
/// faster; they just split the pipe into slow trickles and starve
/// playback/scanning of bandwidth (segment fetches collapsed from ~100 ms to
/// ~23 s when ~16 ingests saturated the line). altmount keeps imports at ~2
/// workers for exactly this reason and leaves the rest of the connections —
/// and the bandwidth — for streaming. Overridable via the `maxdownloadworkers`
/// setting for installs that want to trade streaming responsiveness for faster
/// backlog drain.
pub const DEFAULT_DOWNLOAD_WORKERS: usize = 4;

/// Initialize rustls's default crypto provider exactly once. Safe to call
/// multiple times. Idempotent. Must run before any TLS handshake.
pub(crate) fn init_crypto() {
    drop(rustls::crypto::ring::default_provider().install_default());
}

/// Pool for encoded NNTP article bodies (input to the yEnc decoder).
/// Encoded bodies run slightly larger than decoded ones (yEnc adds ~3 %
/// plus CRLFs), so the same 2 MB cap covers them; 64 retained matches the
/// decoded-side pool. The buffer recycles when `yenc::decode`'s
/// `spawn_blocking` closure drops the `PooledBuf` it was handed.
static ENCODED_BUF_POOL: crate::bufpool::BufPool = crate::bufpool::BufPool::new(64, 2 * 1024 * 1024);

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn undot_stuff_noop_when_no_stuffing() {
        let mut buf = b"=ybegin line=128\r\nfoo\r\nbar\r\n".to_vec();
        let before = buf.clone();
        undot_stuff(&mut buf);
        assert_eq!(buf, before);
    }

    #[test]
    fn undot_stuff_interior_lines() {
        let mut buf = b"foo\r\n..stuffed\r\nbar\r\n".to_vec();
        undot_stuff(&mut buf);
        assert_eq!(buf, b"foo\r\n.stuffed\r\nbar\r\n");
    }

    #[test]
    fn undot_stuff_multiple_interior() {
        let mut buf = b"a\r\n..one\r\nb\r\n..two\r\nc\r\n".to_vec();
        undot_stuff(&mut buf);
        assert_eq!(buf, b"a\r\n.one\r\nb\r\n.two\r\nc\r\n");
    }

    #[test]
    fn undot_stuff_leading_dot() {
        // Body begins with a stuffed line (rare but legal): `..foo\r\n` → `.foo\r\n`.
        let mut buf = b"..start\r\nbody\r\n".to_vec();
        undot_stuff(&mut buf);
        assert_eq!(buf, b".start\r\nbody\r\n");
    }

    #[test]
    fn undot_stuff_does_not_touch_mid_line_double_dot() {
        // `foo..bar` (no preceding CRLF) is content, not a stuffed line.
        let mut buf = b"foo..bar\r\n".to_vec();
        let before = buf.clone();
        undot_stuff(&mut buf);
        assert_eq!(buf, before);
    }

    // (Pool recycling/oversized behaviour is now covered by
    // `crate::bufpool::tests` — the encoded body pool is just a `BufPool`
    // instance.)

    /// Helper that drives `read_until_dot`'s scanning logic against an
    /// in-memory reader so we can exercise the terminator + un-stuff paths
    /// without a real NNTP socket. We bypass the `NntpStream` enum (which
    /// is fixed to TcpStream / TlsStream) by inlining the same loop.
    async fn read_until_dot_in_memory(input: &[u8]) -> io::Result<Vec<u8>> {
        const TERMINATOR: &[u8] = b"\r\n.\r\n";
        let mut reader = Cursor::new(input.to_vec());
        let mut buf: Vec<u8> = Vec::with_capacity(64);
        let mut scanned: usize = 0;

        let term_end = loop {
            let scan_from = scanned.saturating_sub(TERMINATOR.len() - 1);
            if buf.len() >= TERMINATOR.len()
                && let Some(rel) = memchr::memmem::find(&buf[scan_from..], TERMINATOR)
            {
                break scan_from + rel + TERMINATOR.len();
            }
            if buf.len() >= 3 && &buf[..3] == b".\r\n" {
                break 3;
            }
            scanned = buf.len();
            fill_into(&mut reader, &mut buf).await?;
        };

        buf.truncate(term_end - 3);
        undot_stuff(&mut buf);
        Ok(buf)
    }

    #[tokio::test]
    async fn read_until_dot_basic_body() {
        let body = read_until_dot_in_memory(b"=ybegin\r\ndata\r\n=yend\r\n.\r\n")
            .await
            .unwrap();
        assert_eq!(body, b"=ybegin\r\ndata\r\n=yend\r\n");
    }

    #[tokio::test]
    async fn read_until_dot_empty_body() {
        let body = read_until_dot_in_memory(b".\r\n").await.unwrap();
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn read_until_dot_unstuffs() {
        let body = read_until_dot_in_memory(b"a\r\n..b\r\nc\r\n.\r\n")
            .await
            .unwrap();
        assert_eq!(body, b"a\r\n.b\r\nc\r\n");
    }

    #[tokio::test]
    async fn read_until_dot_terminator_at_buffer_boundary() {
        // Force the terminator to straddle a read boundary by sourcing
        // bytes from a reader that returns short chunks. `Cursor` returns
        // the full slice in one read, so we wrap in a chunked reader.
        struct ChunkedReader {
            data: Vec<u8>,
            pos: usize,
            chunk: usize,
        }
        impl tokio::io::AsyncRead for ChunkedReader {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<io::Result<()>> {
                let remaining = self.data.len() - self.pos;
                let n = remaining.min(self.chunk).min(buf.remaining());
                if n == 0 {
                    return std::task::Poll::Ready(Ok(()));
                }
                let start = self.pos;
                buf.put_slice(&self.data[start..start + n]);
                self.pos += n;
                std::task::Poll::Ready(Ok(()))
            }
        }

        let mut reader = ChunkedReader {
            data: b"abc\r\n.\r\n".to_vec(),
            pos: 0,
            chunk: 1, // one byte per read; terminator straddles every boundary
        };
        let mut buf: Vec<u8> = Vec::new();
        let mut scanned: usize = 0;
        const TERMINATOR: &[u8] = b"\r\n.\r\n";
        let term_end = loop {
            let scan_from = scanned.saturating_sub(TERMINATOR.len() - 1);
            if buf.len() >= TERMINATOR.len()
                && let Some(rel) = memchr::memmem::find(&buf[scan_from..], TERMINATOR)
            {
                break scan_from + rel + TERMINATOR.len();
            }
            if buf.len() >= 3 && &buf[..3] == b".\r\n" {
                break 3;
            }
            scanned = buf.len();
            fill_into(&mut reader, &mut buf).await.unwrap();
        };
        buf.truncate(term_end - 3);
        undot_stuff(&mut buf);
        assert_eq!(buf, b"abc\r\n");
    }
}
