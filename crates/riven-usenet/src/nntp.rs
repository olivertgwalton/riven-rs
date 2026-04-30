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
use tokio::sync::Semaphore;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::pki_types::ServerName;

#[derive(Debug, Clone)]
pub struct NntpServerConfig {
    pub host: String,
    pub port: u16,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub use_tls: bool,
    pub max_connections: u32,
    pub timeout: Duration,
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

/// One open + authenticated NNTP connection.
pub struct NntpConnection {
    stream: NntpStream,
}

enum NntpStream {
    Plain(BufReader<TcpStream>),
    // Boxed: rustls' TlsStream is ~1KB on the stack and dwarfs the Plain variant.
    Tls(Box<BufReader<tokio_rustls::client::TlsStream<TcpStream>>>),
}

impl NntpStream {
    async fn read_line(&mut self, buf: &mut String) -> io::Result<usize> {
        match self {
            NntpStream::Plain(s) => s.read_line(buf).await,
            NntpStream::Tls(s) => s.read_line(buf).await,
        }
    }

    async fn read_until_dot(&mut self) -> io::Result<Vec<u8>> {
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
            // Strip trailing CRLF.
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

    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            NntpStream::Plain(s) => s.get_mut().write_all(buf).await,
            NntpStream::Tls(s) => s.get_mut().write_all(buf).await,
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        match self {
            NntpStream::Plain(s) => s.get_mut().flush().await,
            NntpStream::Tls(s) => s.get_mut().flush().await,
        }
    }
}

impl NntpConnection {
    pub async fn connect(cfg: &NntpServerConfig) -> Result<Self, NntpError> {
        let connect_fut = async {
            let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;
            tcp.set_nodelay(true).ok();
            let stream = if cfg.use_tls {
                let connector = build_tls_connector()?;
                let server_name = ServerName::try_from(cfg.host.clone())
                    .map_err(|e| NntpError::Tls(e.to_string()))?;
                let tls = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| NntpError::Tls(e.to_string()))?;
                NntpStream::Tls(Box::new(BufReader::new(tls)))
            } else {
                NntpStream::Plain(BufReader::new(tcp))
            };
            Ok::<NntpStream, NntpError>(stream)
        };

        let stream = tokio::time::timeout(cfg.timeout, connect_fut)
            .await
            .map_err(|_| NntpError::Timeout)??;

        let mut conn = NntpConnection { stream };
        let greeting = conn.read_status().await?;
        if !(greeting.starts_with("200") || greeting.starts_with("201")) {
            return Err(NntpError::ServerError(greeting));
        }

        if let (Some(user), Some(pass)) = (cfg.user.as_deref(), cfg.pass.as_deref()) {
            conn.send(&format!("AUTHINFO USER {user}\r\n")).await?;
            let r = conn.read_status().await?;
            if r.starts_with("381") {
                conn.send(&format!("AUTHINFO PASS {pass}\r\n")).await?;
                let r2 = conn.read_status().await?;
                if !r2.starts_with("281") {
                    return Err(NntpError::AuthFailed(r2));
                }
            } else if !r.starts_with("281") {
                return Err(NntpError::AuthFailed(r));
            }
        }

        Ok(conn)
    }

    async fn send(&mut self, line: &str) -> Result<(), NntpError> {
        self.stream.write_all(line.as_bytes()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_status(&mut self) -> Result<String, NntpError> {
        let mut s = String::new();
        let n = self.stream.read_line(&mut s).await?;
        if n == 0 {
            return Err(NntpError::Protocol("EOF reading status"));
        }
        Ok(s.trim_end_matches(['\r', '\n']).to_string())
    }

    /// Fetch the body of an article by message-id. Returns the raw, un-stuffed
    /// body (CRLF line endings preserved) ready for the yEnc decoder.
    pub async fn fetch_body(&mut self, message_id: &str) -> Result<Vec<u8>, NntpError> {
        // Some servers reject angle-less message-ids, so always wrap.
        let id_wrapped = if message_id.starts_with('<') {
            message_id.to_string()
        } else {
            format!("<{message_id}>")
        };
        self.send(&format!("BODY {id_wrapped}\r\n")).await?;
        let status = self.read_status().await?;
        if status.starts_with("430") || status.starts_with("423") {
            return Err(NntpError::ArticleNotFound(status));
        }
        if !status.starts_with("222") {
            return Err(NntpError::ServerError(status));
        }
        Ok(self.stream.read_until_dot().await?)
    }

    pub async fn quit(&mut self) {
        let _ = self.send("QUIT\r\n").await;
    }
}

fn build_tls_connector() -> Result<TlsConnector, NntpError> {
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

/// Connection pool. Hands out fresh connections lazily, capped by a semaphore.
/// Connections are not currently kept alive between checkouts — for streaming
/// workloads where each range request fetches multiple segments in parallel
/// the dominant cost is BODY round-trips, not connect+AUTHINFO. Future work:
/// reuse connections via a parking lot.
pub struct NntpPool {
    cfg: NntpServerConfig,
    permits: Arc<Semaphore>,
}

impl NntpPool {
    pub fn new(cfg: NntpServerConfig) -> Self {
        let permits = Arc::new(Semaphore::new(cfg.max_connections.max(1) as usize));
        Self { cfg, permits }
    }

    pub fn config(&self) -> &NntpServerConfig {
        &self.cfg
    }

    pub async fn fetch_body(&self, message_id: &str) -> Result<Vec<u8>, NntpError> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| NntpError::Protocol("pool closed"))?;
        let mut conn = NntpConnection::connect(&self.cfg).await?;
        let result = conn.fetch_body(message_id).await;
        conn.quit().await;
        result
    }
}

/// Initialize rustls's default crypto provider exactly once. Safe to call
/// multiple times. Idempotent. Must run before any TLS handshake.
pub fn init_crypto() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
