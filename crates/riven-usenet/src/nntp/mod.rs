//! Tiny async NNTP client.
//!
//! Implements just enough of RFC 3977 to log in (AUTHINFO USER/PASS) and
//! fetch article bodies (BODY <message-id>). Article bodies are
//! `.`-terminated and dot-stuffed; we undo that here so callers receive a
//! clean payload to hand to the yEnc decoder.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;

mod connection;
mod pool;

pub use connection::NntpConnection;
pub use pool::NntpPool;

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
    pub(crate) async fn read_line(&mut self, buf: &mut String) -> io::Result<usize> {
        match self {
            NntpStream::Plain(s) => s.read_line(buf).await,
            NntpStream::Tls(s) => s.read_line(buf).await,
        }
    }

    pub(crate) async fn read_until_dot(&mut self) -> io::Result<Vec<u8>> {
        // RFC 3977: multi-line response terminated by a line containing only ".".
        // Lines beginning with "." are dot-stuffed (sender prepends an extra ".").
        let mut out = Vec::with_capacity(1 << 16);
        let mut line = Vec::with_capacity(1024);
        loop {
            line.clear();
            let n = match self {
                NntpStream::Plain(s) => s.read_until(b'\n', &mut line).await?,
                NntpStream::Tls(s) => s.read_until(b'\n', &mut line).await?,
            };
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF before terminating dot",
                ));
            }
            let end = if line.ends_with(b"\r\n") {
                line.len() - 2
            } else if line.ends_with(b"\n") {
                line.len() - 1
            } else {
                line.len()
            };
            let content = &line[..end];
            if content == b"." {
                break;
            }
            // Un-stuff: a literal `.` at line start is sent as `..`.
            let payload = if content.first() == Some(&b'.') {
                &content[1..]
            } else {
                content
            };
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n");
        }
        Ok(out)
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

/// Multi-provider configuration handed to `UsenetStreamer::new`.
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

/// Initialize rustls's default crypto provider exactly once. Safe to call
/// multiple times. Idempotent. Must run before any TLS handshake.
pub(crate) fn init_crypto() {
    drop(rustls::crypto::ring::default_provider().install_default());
}
