use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;

use crate::bufpool::PooledBuf;

use super::{ENCODED_BUF_POOL, NntpError, NntpServerConfig, NntpStream, build_tls_connector};

/// Resolve `host` (serve-stale, hickory) and pair each IP with `port`. DNS
/// caching, the resolver, and the serve-stale fallback all live in
/// [`riven_core::dns`], shared with the HTTP client so both paths resolve
/// identically.
async fn resolve_cached(host: &str, port: u16) -> Result<Vec<SocketAddr>, NntpError> {
    Ok(riven_core::dns::resolve_cached(host)
        .await?
        .into_iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect())
}

/// Populate the DNS cache for a host before a burst of concurrent dials, so a
/// cold cache doesn't let every prewarm dial hit the resolver at once.
pub(crate) async fn warm_dns(host: &str, _port: u16) {
    riven_core::dns::warm(host).await;
}

/// Drop a host's cached address so the next dial re-resolves — used when a
/// cached address fails to connect (e.g. the provider rotated IPs).
fn invalidate_dns(host: &str, _port: u16) {
    riven_core::dns::invalidate(host);
}

/// Connect to the first reachable address, returning the last error if none work.
async fn connect_first(addrs: &[SocketAddr]) -> Result<TcpStream, NntpError> {
    let mut last_err: Option<std::io::Error> = None;
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_err = Some(error),
        }
    }
    Err(last_err
        .unwrap_or_else(|| std::io::Error::other("no addresses to connect"))
        .into())
}

/// One open + authenticated NNTP connection.
pub struct NntpConnection {
    stream: NntpStream,
    /// Per-read deadline. Without this a half-dead socket (provider idle-drop,
    /// throttle, network blip) would block `read_status`/`read_until_dot`
    /// forever — the connection stays checked out so the idle reaper never
    /// touches it, wedging the worker permanently. On timeout we surface
    /// `NntpError::Timeout`, which the pool treats as a transient failure:
    /// the dead connection is dropped and the fetch retries on a fresh one.
    read_timeout: Duration,
}

impl NntpConnection {
    pub async fn connect(cfg: &NntpServerConfig) -> Result<Self, NntpError> {
        // Sized to absorb a typical ~720 KB segment body in roughly one
        // fill rather than thousands of 8 KB syscalls.
        const NNTP_READ_BUF: usize = 512 * 1024;

        let connect_fut = async {
            // Dial a cached/resolved IP (TLS still verifies against `cfg.host`
            // below, so the SNI/cert path is unaffected). On connect failure,
            // drop the cached address so the next attempt re-resolves.
            let addrs = resolve_cached(&cfg.host, cfg.port).await?;
            let tcp = match connect_first(&addrs).await {
                Ok(tcp) => tcp,
                Err(error) => {
                    invalidate_dns(&cfg.host, cfg.port);
                    return Err(error);
                }
            };
            drop(tcp.set_nodelay(true));
            let stream = if cfg.use_tls {
                let connector = build_tls_connector()?;
                let server_name = ServerName::try_from(cfg.host.clone())
                    .map_err(|e| NntpError::Tls(e.to_string()))?;
                let tls = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| NntpError::Tls(e.to_string()))?;
                NntpStream::Tls(Box::new(BufReader::with_capacity(NNTP_READ_BUF, tls)))
            } else {
                NntpStream::Plain(BufReader::with_capacity(NNTP_READ_BUF, tcp))
            };
            Ok::<NntpStream, NntpError>(stream)
        };

        let stream = tokio::time::timeout(cfg.timeout, connect_fut)
            .await
            .map_err(|_e| NntpError::Timeout)??;

        let mut conn = NntpConnection {
            stream,
            read_timeout: cfg.timeout,
        };
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
        let n = self.stream.read_line(&mut s, self.read_timeout).await?;
        if n == 0 {
            return Err(NntpError::Protocol("EOF reading status"));
        }
        Ok(s.trim_end_matches(['\r', '\n']).to_string())
    }

    /// Fetch the body of an article by message-id. Returns the raw, un-stuffed
    /// body (CRLF line endings preserved) ready for the yEnc decoder. The
    /// buffer is checked out from the encoded-body pool — the caller
    /// (typically `do_fetch_with_retry` consuming the `PooledBuf` inside its
    /// `spawn_blocking` decode closure) returns the allocation to the pool
    /// when the `PooledBuf` drops.
    pub(crate) async fn fetch_body(&mut self, message_id: &str) -> Result<PooledBuf, NntpError> {
        // Some servers reject angle-less message-ids, so always wrap in <>.
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
        // Pull a recycled buffer from the pool; ~1 MB matches the typical
        // ~720 KB encoded segment with headroom for yEnc escape overhead.
        let mut buf = PooledBuf::take(&ENCODED_BUF_POOL, 1 << 20);
        self.stream
            .read_until_dot(buf.as_mut_vec(), self.read_timeout)
            .await?;
        Ok(buf)
    }

    /// RFC 3977 `DATE` — used as a cheap liveness ping before reusing
    /// a stale-but-not-expired pooled connection.
    pub async fn date(&mut self) -> Result<(), NntpError> {
        self.send("DATE\r\n").await?;
        let status = self.read_status().await?;
        if status.starts_with("111") {
            return Ok(());
        }
        Err(NntpError::ServerError(status))
    }

    /// `STAT <message-id>`. Returns `Ok(true)` if the article exists on the
    /// server (RFC 3977 `223`), `Ok(false)` for `423`/`430` (no such
    /// article), and propagates other server errors.
    pub async fn stat(&mut self, message_id: &str) -> Result<bool, NntpError> {
        let id_wrapped = if message_id.starts_with('<') {
            message_id.to_string()
        } else {
            format!("<{message_id}>")
        };
        self.send(&format!("STAT {id_wrapped}\r\n")).await?;
        let status = self.read_status().await?;
        if status.starts_with("223") {
            return Ok(true);
        }
        if status.starts_with("430") || status.starts_with("423") {
            return Ok(false);
        }
        Err(NntpError::ServerError(status))
    }

    pub async fn quit(&mut self) {
        drop(self.send("QUIT\r\n").await);
    }
}
