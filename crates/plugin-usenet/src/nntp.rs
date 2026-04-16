use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use url::Url;

const NNTP_TIMEOUT_SECS: u64 = 15;

trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

#[derive(Clone, Debug)]
struct NntpServer {
    host: String,
    port: u16,
    username: Option<String>,
    password: Option<String>,
    tls: bool,
}

impl NntpServer {
    fn parse(value: &str) -> anyhow::Result<Self> {
        let url = Url::parse(value)?;
        let tls = match url.scheme() {
            "nntps" | "snews" => true,
            "nntp" | "news" => false,
            scheme => bail!("unsupported NNTP scheme {scheme}"),
        };

        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .context("missing NNTP host")?
            .to_string();
        let port = url.port().unwrap_or(if tls { 563 } else { 119 });
        let username = (!url.username().is_empty()).then(|| url.username().to_string());
        let password = url.password().map(ToOwned::to_owned);

        Ok(Self {
            host,
            port,
            username,
            password,
            tls,
        })
    }
}

struct NntpConnection {
    reader: BufReader<Box<dyn AsyncReadWrite>>,
}

impl NntpConnection {
    async fn connect(server: &NntpServer) -> anyhow::Result<Self> {
        let stream = timeout(
            Duration::from_secs(NNTP_TIMEOUT_SECS),
            TcpStream::connect((server.host.as_str(), server.port)),
        )
        .await
        .context("NNTP connect timed out")??;

        let stream: Box<dyn AsyncReadWrite> = if server.tls {
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let connector = TlsConnector::from(std::sync::Arc::new(config));
            let server_name = ServerName::try_from(server.host.clone())?;
            Box::new(
                timeout(
                    Duration::from_secs(NNTP_TIMEOUT_SECS),
                    connector.connect(server_name, stream),
                )
                .await
                .context("NNTP TLS handshake timed out")??,
            )
        } else {
            Box::new(stream)
        };

        let mut conn = Self {
            reader: BufReader::new(stream),
        };
        let greeting = conn.read_line().await?;
        if !greeting.starts_with("200 ") && !greeting.starts_with("201 ") {
            bail!("unexpected NNTP greeting: {greeting}");
        }

        if let Some(username) = &server.username {
            conn.command(&format!("AUTHINFO USER {username}")).await?;
            let password = server
                .password
                .as_deref()
                .context("NNTP password required after AUTHINFO USER")?;
            let response = conn.command(&format!("AUTHINFO PASS {password}")).await?;
            if !response.starts_with("281 ") {
                bail!("NNTP authentication failed: {response}");
            }
        }

        Ok(conn)
    }

    async fn command(&mut self, command: &str) -> anyhow::Result<String> {
        timeout(Duration::from_secs(NNTP_TIMEOUT_SECS), async {
            self.reader.get_mut().write_all(command.as_bytes()).await?;
            self.reader.get_mut().write_all(b"\r\n").await?;
            self.reader.get_mut().flush().await?;
            self.read_line().await
        })
        .await
        .context("NNTP command timed out")?
    }

    async fn read_line(&mut self) -> anyhow::Result<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        Ok(line.trim_end_matches(['\r', '\n']).to_string())
    }
}

pub(crate) async fn check_nntp_availability(
    servers: &[String],
    message_ids: &[String],
) -> anyhow::Result<()> {
    if message_ids.is_empty() {
        return Ok(());
    }
    if servers.is_empty() {
        bail!("no NNTP servers configured for availability check");
    }

    let mut errors = Vec::new();
    for server in servers {
        match NntpServer::parse(server) {
            Ok(parsed) => match check_server_availability(parsed, message_ids).await {
                Ok(()) => return Ok(()),
                Err(error) => errors.push(format!("{server}: {error}")),
            },
            Err(error) => errors.push(format!("{server}: {error}")),
        }
    }

    bail!(
        "sampled articles unavailable on configured NNTP servers: {}",
        errors.join("; ")
    )
}

async fn check_server_availability(
    server: NntpServer,
    message_ids: &[String],
) -> anyhow::Result<()> {
    let mut conn = NntpConnection::connect(&server).await?;
    for message_id in message_ids {
        let response = conn
            .command(&format!("STAT {}", format_message_id(message_id)))
            .await?;
        if response.starts_with("430 ") {
            bail!("article missing: {message_id}");
        }
        if !response.starts_with("223 ") {
            bail!("unexpected STAT response for {message_id}: {response}");
        }
    }
    let _ = conn.command("QUIT").await;
    Ok(())
}

fn format_message_id(message_id: &str) -> String {
    let trimmed = message_id.trim();
    if trimmed.starts_with('<') && trimmed.ends_with('>') {
        trimmed.to_string()
    } else {
        format!("<{trimmed}>")
    }
}
