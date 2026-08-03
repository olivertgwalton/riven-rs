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

mod client;
mod pool;

pub use client::{NntpClient, Traffic};
pub use pool::{ClientPool, DEFAULT_ARTICLE_TIMEOUT, Lease, ProviderHealth, ProviderTraffic};

#[derive(Clone)]
pub struct NntpServerConfig {
    pub host: String,
    pub port: u16,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub use_tls: bool,
    pub max_connections: u32,
    /// Floor on how long one article may take against this provider before the
    /// fetch is abandoned and the next provider tried. The effective budget
    /// scales up from here with the provider's own measured latency — see
    /// [`ClientPool::article_budget`].
    pub article_timeout: Duration,
}

impl std::fmt::Debug for NntpServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NntpServerConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("pass", &self.pass.as_deref().map(|_| "<redacted>"))
            .field("use_tls", &self.use_tls)
            .field("max_connections", &self.max_connections)
            .field("article_timeout", &self.article_timeout)
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
    /// `480` — the provider wants us to authenticate again, which a
    /// reconnect satisfies.
    #[error("authentication required: {0}")]
    AuthRequired(String),
    #[error("provider connection limit reached: {0}")]
    TooManyConnections(String),
    #[error("protocol error: {0}")]
    Protocol(&'static str),
    #[error("timed out")]
    Timeout,
}

/// One buffer, above the record layer, and a small one.
///
/// A buffer *below* rustls was tried and measured: the theory was that rustls
/// reads ciphertext a TLS record at a time (≤16 KiB) and so pays a `recv` per
/// record. It does not. Under a real 4K stream the socket already has far more
/// than a record waiting, so `read()` returns a large chunk either way —
/// profiling the two builds against the same title at the same position put
/// `recv_with_flags` at *exactly* 170 samples both times. The inner buffer
/// removed no syscall and added a memcpy, showing up as ~3.5 % of CPU in
/// `poll_fill_buf` frames that were not there before.
///
/// What is worth keeping is the size. This only has to serve status lines and
/// hand [`fill_into`]-sized (64 KiB) chunks up, so 512 KiB bought nothing —
/// and it is per connection, of which a provider allows 100.
pub(crate) enum NntpTransport {
    Plain(BufReader<TcpStream>),
    Tls(Box<BufReader<tokio_rustls::client::TlsStream<TcpStream>>>),
}

/// One NNTP connection's read/write half, plus the bytes a bulk read pulled in
/// past the end of the reply it was reading.
///
/// That carry is what makes pipelining possible. [`read_until_dot`] reads in
/// 64 KiB chunks, so the chunk that completes one reply routinely contains the
/// start of the next one; with a single command outstanding there is never a
/// next one and the excess was simply truncated away. Under pipelining that
/// excess is the following reply's status line, and dropping it desynchronises
/// the connection — the reader then waits for a status line the server already
/// sent, until the budget expires.
///
/// [`read_until_dot`]: NntpStream::read_until_dot
pub(crate) struct NntpStream {
    inner: NntpTransport,
    carry: Vec<u8>,
}

impl NntpStream {
    pub(crate) fn new(inner: NntpTransport) -> Self {
        Self {
            inner,
            carry: Vec::new(),
        }
    }

    /// Read a single line, failing with `TimedOut` if no data arrives within
    /// `timeout`. The deadline is per call, so it acts as an inactivity timeout
    /// (matching nntppool's `SetReadDeadline`): a slow-but-progressing transfer
    /// keeps resetting it, while a half-dead socket trips it.
    ///
    /// Carried-over bytes are consumed before the socket is touched, so a
    /// status line already pulled in by the previous body read is seen here
    /// rather than waited for.
    pub(crate) async fn read_line(
        &mut self,
        buf: &mut String,
        timeout: Duration,
    ) -> io::Result<usize> {
        if !self.carry.is_empty() {
            // Status lines are ASCII, and every byte here is one this
            // connection already read, so a lossy decode cannot invent one.
            if let Some(end) = memchr::memchr(b'\n', &self.carry) {
                let line: Vec<u8> = self.carry.drain(..=end).collect();
                buf.push_str(&String::from_utf8_lossy(&line));
                return Ok(line.len());
            }
            // A partial line: hand over what there is and let the socket
            // supply the rest of it.
            let partial = std::mem::take(&mut self.carry);
            buf.push_str(&String::from_utf8_lossy(&partial));
            let rest = self.read_line_from_socket(buf, timeout).await?;
            return Ok(partial.len() + rest);
        }
        self.read_line_from_socket(buf, timeout).await
    }

    async fn read_line_from_socket(
        &mut self,
        buf: &mut String,
        timeout: Duration,
    ) -> io::Result<usize> {
        tokio::time::timeout(timeout, async {
            match &mut self.inner {
                NntpTransport::Plain(s) => s.read_line(buf).await,
                NntpTransport::Tls(s) => s.read_line(buf).await,
            }
        })
        .await
        .map_err(|_e| io::Error::new(io::ErrorKind::TimedOut, "nntp read timed out"))?
    }

    /// Read a `.`-terminated multi-line response into the caller-provided
    /// `out`. `timeout` is an **absolute** deadline for the whole response,
    /// not an inactivity timer: a connection that keeps trickling a few bytes
    /// resets an inactivity timer forever, and a body that never ends is the
    /// one case a streaming reader cannot afford to wait out. This matches
    /// streamnzb, which sets one `SetDeadline` when the body starts.
    ///
    /// A single `tokio::time::Sleep` is pinned once outside the loop rather
    /// than wrapping each read in a fresh `tokio::time::timeout()` future —
    /// profile showed ~0.5 % of CPU in `Timeout::poll`'s memset.
    ///
    /// `out` is cleared on entry and reused across the articles of one
    /// pipelined batch, so a batch pays one allocation rather than one per
    /// article.
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
        const TERMINATOR: &[u8] = b"\r\n.\r\n";

        out.clear();
        // Whatever the previous reply's last read over-pulled is the front of
        // this one, and may already contain the whole body.
        out.append(&mut self.carry);
        let mut scanned: usize = 0;

        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);

        let term_end = loop {
            let scan_from = scanned.saturating_sub(TERMINATOR.len() - 1);
            if out.len() >= TERMINATOR.len()
                && let Some(rel) = memchr::memmem::find(&out[scan_from..], TERMINATOR)
            {
                break scan_from + rel + TERMINATOR.len();
            }
            if out.len() >= 3 && &out[..3] == b".\r\n" {
                break 3;
            }
            scanned = out.len();

            let read_fut = async {
                match &mut self.inner {
                    NntpTransport::Plain(s) => fill_into(s, out).await,
                    NntpTransport::Tls(s) => fill_into(s, out).await,
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
        };

        // Bytes past the terminator belong to the next reply, not this body.
        // Captured before the truncate that would otherwise discard them.
        if term_end < out.len() {
            self.carry.extend_from_slice(&out[term_end..]);
        }
        out.truncate(term_end - 3);
        undot_stuff(out);
        Ok(())
    }

    pub(crate) async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match &mut self.inner {
            NntpTransport::Plain(s) => s.get_mut().write_all(buf).await,
            NntpTransport::Tls(s) => s.get_mut().write_all(buf).await,
        }
    }

    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            NntpTransport::Plain(s) => s.get_mut().flush().await,
            NntpTransport::Tls(s) => s.get_mut().flush().await,
        }
    }
}

/// Read more bytes from `reader` into the tail of `buf`, growing the
/// buffer's capacity if needed. Returns the number of bytes appended.
/// Uses `read_buf` so the read lands in `buf`'s **uninitialized** spare
/// capacity (advancing the length by the amount read) — no `resize`-zero of
/// the chunk before it's overwritten. A ~700 KB body would otherwise memset
/// ~700 KB of scratch across its ~11 reads. Pulls large chunks (a full
/// BufReader fill) rather than one line per call.
async fn fill_into<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> io::Result<usize> {
    const READ_CHUNK: usize = 64 * 1024;
    buf.reserve(READ_CHUNK);
    let got = reader.read_buf(buf).await?;
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
    drop(rustls::crypto::aws_lc_rs::default_provider().install_default());
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::io::Cursor;

    /// Decoded payload every article served by [`spawn_fake_nntp_server`]
    /// carries.
    pub(crate) const FAKE_SEGMENT_PAYLOAD: &[u8] = b"riven fake usenet segment payload";

    /// Loopback listener speaking enough NNTP for pool tests: `200` greeting,
    /// `111` to DATE, `223` to STAT, and a yEnc article to BODY.
    pub(crate) async fn spawn_fake_nntp_server()
    -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
        use tokio::net::TcpListener;

        let article = crate::yenc::tests::encode_single(FAKE_SEGMENT_PAYLOAD, "fake.bin");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let article = article.clone();
                tokio::spawn(async move {
                    let (read_half, mut write_half) = socket.into_split();
                    if write_half
                        .write_all(b"200 fake nntp ready\r\n")
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let mut lines = TokioBufReader::new(read_half).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let reply: Vec<u8> = if line.starts_with("DATE") {
                            b"111 20260101000000\r\n".to_vec()
                        } else if line.starts_with("STAT") {
                            b"223 0 <exists>\r\n".to_vec()
                        } else if line.starts_with("BODY") {
                            let mut out = b"222 0 <exists>\r\n".to_vec();
                            out.extend_from_slice(&article);
                            out.extend_from_slice(b"\r\n.\r\n");
                            out
                        } else if line.starts_with("QUIT") {
                            drop(write_half.write_all(b"205 bye\r\n").await);
                            return;
                        } else {
                            b"500 what\r\n".to_vec()
                        };
                        if write_half.write_all(&reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (addr, handle)
    }

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
        let mut buf = b"..start\r\nbody\r\n".to_vec();
        undot_stuff(&mut buf);
        assert_eq!(buf, b".start\r\nbody\r\n");
    }

    #[test]
    fn undot_stuff_does_not_touch_mid_line_double_dot() {
        let mut buf = b"foo..bar\r\n".to_vec();
        let before = buf.clone();
        undot_stuff(&mut buf);
        assert_eq!(buf, before);
    }

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
            chunk: 1,
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
