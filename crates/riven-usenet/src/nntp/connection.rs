use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;

use super::{NntpError, NntpServerConfig, NntpStream, build_tls_connector};

/// One open + authenticated NNTP connection.
pub struct NntpConnection {
    stream: NntpStream,
}

impl NntpConnection {
    pub async fn connect(cfg: &NntpServerConfig) -> Result<Self, NntpError> {
        // Sized to absorb a typical ~720 KB segment body in roughly one
        // fill rather than thousands of 8 KB syscalls.
        const NNTP_READ_BUF: usize = 512 * 1024;

        let connect_fut = async {
            let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;
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
        Ok(self.stream.read_until_dot().await?)
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
