//! One NNTP client == one TCP/TLS connection.
//!
//! Commands are issued synchronously against the socket; the owning
//! [`ClientPool`](super::pool::ClientPool) guarantees a client is only ever
//! held by one caller at a time.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;

use super::{NntpError, NntpServerConfig, NntpStream, NntpTransport, build_tls_connector};

/// Dial + greeting + authentication budget.
const DIAL_TIMEOUT: Duration = Duration::from_secs(30);
/// Deadline for an ordinary command's status line.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
/// STAT is a single-line request/reply on a warm socket; anything slower is a
/// stall, and validation sweeps thousands of them.
const STAT_TIMEOUT: Duration = Duration::from_secs(2);
/// Initial attempt plus two retries, per command.
const COMMAND_ATTEMPTS: usize = 3;
/// Capacity an encoded-body buffer starts at. Comfortably above the ~720 KB
/// article most posters cut, so the common case never reallocates mid-body;
/// a larger post grows the buffer once and the allocator keeps the pages.
const BODY_BUF_CAPACITY: usize = 1 << 20;

/// Per-provider byte/article counters, shared by every client of that provider.
#[derive(Debug, Default)]
pub struct Traffic {
    pub bytes_read: AtomicU64,
    pub articles_read: AtomicU64,
}

/// True if a greeting/status line is the provider reporting that the account
/// is at its connection limit rather than a generic failure. NNTP has no
/// standard code for this; `502` and `400` are the de-facto convention.
fn is_too_many_connections(status: &str) -> bool {
    status.starts_with("502")
        || status.starts_with("400")
        || status.to_ascii_lowercase().contains("too many connections")
}

async fn resolve(host: &str, port: u16) -> Result<Vec<SocketAddr>, NntpError> {
    Ok(riven_core::dns::resolve_cached(host)
        .await?
        .into_iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect())
}

/// Rotating start index so successive dials spread over every address a host
/// resolves to. The resolver returns a stable cached order, so a plain
/// first-success loop funnels a whole account into `addrs[0]` and caps
/// aggregate throughput at that one endpoint.
static DIAL_ROTATION: AtomicUsize = AtomicUsize::new(0);

async fn connect_first(addrs: &[SocketAddr]) -> Result<TcpStream, NntpError> {
    if addrs.is_empty() {
        return Err(std::io::Error::other("no addresses to connect").into());
    }
    let start = DIAL_ROTATION.fetch_add(1, Ordering::Relaxed);
    let mut last_err: Option<std::io::Error> = None;
    for offset in 0..addrs.len() {
        let addr = addrs[start.wrapping_add(offset) % addrs.len()];
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_err = Some(error),
        }
    }
    Err(last_err
        .unwrap_or_else(|| std::io::Error::other("no addresses to connect"))
        .into())
}

pub struct NntpClient {
    stream: NntpStream,
    config: Arc<NntpServerConfig>,
    traffic: Arc<Traffic>,
    last_used: Instant,
    /// A command whose response was not fully drained leaves unread bytes on
    /// the wire; the next user of the socket would read them as its own reply.
    /// Such a client is closed rather than returned to the idle queue.
    poisoned: bool,
}

impl NntpClient {
    pub async fn connect(
        config: Arc<NntpServerConfig>,
        traffic: Arc<Traffic>,
    ) -> Result<Self, NntpError> {
        let stream = tokio::time::timeout(DIAL_TIMEOUT, dial(&config))
            .await
            .map_err(|_elapsed| NntpError::Timeout)??;
        let mut client = Self {
            stream,
            config,
            traffic,
            last_used: Instant::now(),
            poisoned: false,
        };
        tokio::time::timeout(DIAL_TIMEOUT, client.handshake())
            .await
            .map_err(|_elapsed| NntpError::Timeout)??;
        Ok(client)
    }

    async fn handshake(&mut self) -> Result<(), NntpError> {
        let greeting = self.read_status(COMMAND_TIMEOUT).await?;
        // 201 is a read-only server, which is all this crate ever needs.
        if !(greeting.starts_with("200") || greeting.starts_with("201")) {
            if is_too_many_connections(&greeting) {
                return Err(NntpError::TooManyConnections(greeting));
            }
            return Err(NntpError::ServerError(greeting));
        }
        self.authenticate().await
    }

    async fn authenticate(&mut self) -> Result<(), NntpError> {
        let (Some(user), Some(pass)) = (
            self.config.user.clone().filter(|u| !u.is_empty()),
            self.config.pass.clone(),
        ) else {
            return Ok(());
        };
        self.send(&format!("AUTHINFO USER {user}\r\n")).await?;
        let status = self.read_status(COMMAND_TIMEOUT).await?;
        if status.starts_with("381") {
            self.send(&format!("AUTHINFO PASS {pass}\r\n")).await?;
            let status = self.read_status(COMMAND_TIMEOUT).await?;
            if !status.starts_with("281") {
                return Err(NntpError::AuthFailed(status));
            }
        } else if !status.starts_with("281") {
            return Err(NntpError::AuthFailed(status));
        }
        Ok(())
    }

    /// Reopen and reauthenticate the socket in place, discarding whatever
    /// state the old one was in.
    async fn reconnect(&mut self) -> Result<(), NntpError> {
        let stream = tokio::time::timeout(DIAL_TIMEOUT, dial(&self.config))
            .await
            .map_err(|_elapsed| NntpError::Timeout)??;
        self.stream = stream;
        self.poisoned = false;
        tokio::time::timeout(DIAL_TIMEOUT, self.handshake())
            .await
            .map_err(|_elapsed| NntpError::Timeout)?
    }

    /// Fetch an article body, yEnc-encoded and un-dot-stuffed.
    ///
    /// `budget` bounds the whole call — every attempt, every reconnect, and the
    /// body read itself — rather than each read separately. That distinction is
    /// the point: the body read used to carry a flat 300 s deadline and the
    /// retry loop could spend it three times over, so one article could hold a
    /// playback read for a quarter of an hour. Bounding the call instead lets
    /// [`SegmentPool`](crate::pool::SegmentPool) treat an exhausted budget as
    /// "this provider is not answering" and move to the next one, which is a
    /// recovery a longer wait on the same socket can never be.
    ///
    /// Note this is not a hedge: no second request is issued while the first is
    /// still running, so a stalled provider costs the budget once, not a
    /// duplicate of every fetch.
    pub(crate) async fn body(
        &mut self,
        message_id: &str,
        budget: Duration,
    ) -> Result<Vec<u8>, NntpError> {
        let deadline = Instant::now() + budget;
        let mut last_err = None;
        for attempt in 0..COMMAND_ATTEMPTS {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(last_err.unwrap_or(NntpError::Timeout));
            }
            match self.body_once(message_id, remaining).await {
                Ok(buf) => return Ok(buf),
                Err(error) => {
                    if !should_reconnect(&error) || attempt + 1 == COMMAND_ATTEMPTS {
                        return Err(error);
                    }
                    tracing::debug!(
                        host = %self.config.host,
                        message_id,
                        attempt,
                        %error,
                        "nntp BODY failed; reconnecting"
                    );
                    last_err = Some(error);
                    // A reconnect that would outlive the budget is not worth
                    // dialing: the caller has a healthier provider to try.
                    if deadline.saturating_duration_since(Instant::now()).is_zero() {
                        return Err(last_err.unwrap_or(NntpError::Timeout));
                    }
                    self.reconnect().await?;
                }
            }
        }
        Err(last_err.unwrap_or(NntpError::Protocol("retry exhausted without error")))
    }

    async fn body_once(
        &mut self,
        message_id: &str,
        budget: Duration,
    ) -> Result<Vec<u8>, NntpError> {
        // A future can be cancelled at any await below. Poison before writing
        // so a socket with an unread status or body is never pooled.
        self.poisoned = true;
        let deadline = Instant::now() + budget;
        self.send(&format!("BODY {}\r\n", wrap_id(message_id)))
            .await?;
        let status = self
            .read_status(budget.min(COMMAND_TIMEOUT))
            .await
            .map_err(stalled_as_timeout)?;
        if let Err(error) = classify_article_status(&status, "222") {
            // Non-body responses end at the status line, so the wire is clean.
            self.poisoned = false;
            return Err(error);
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // The status arrived but there is no budget left to drain the body.
            // Leaving `poisoned` set closes the socket rather than pooling one
            // with an unread body on it.
            return Err(NntpError::Timeout);
        }
        let mut buf = Vec::with_capacity(BODY_BUF_CAPACITY);
        self.stream
            .read_until_dot(&mut buf, remaining)
            .await
            .map_err(stalled_as_timeout)?;
        self.poisoned = false;

        self.last_used = Instant::now();
        self.traffic
            .bytes_read
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        self.traffic.articles_read.fetch_add(1, Ordering::Relaxed);
        Ok(buf)
    }

    /// `BODY` a batch of message-ids over this one connection, writing every
    /// command before reading any reply — [`stat_many`](Self::stat_many)'s
    /// shape, applied to article bodies. A batch of `n` costs one round trip
    /// instead of `n`, and one connection instead of `n`.
    ///
    /// The connection, not the round trip, is usually the reason to reach for
    /// this. Bodies are large, so a batch on one connection is bounded by that
    /// connection's share of provider bandwidth where `n` parallel fetches are
    /// not — this is a win for a scan that would otherwise hold `n` slots the
    /// streaming path wants, and a loss for anything latency-critical. Playback
    /// deliberately does not use it.
    ///
    /// Results are positional. A `430` is a status line and nothing else, so
    /// the wire stays in sync and the rest of the batch is still readable —
    /// hence the per-article `Err`. Anything else leaves unread bodies on the
    /// socket with no way to resynchronise, so the connection is poisoned and
    /// the whole batch fails.
    ///
    /// `budget` bounds the entire batch, the way [`body`](Self::body)'s bounds
    /// one article. Unlike `body` there is no retry: a reconnect would lose the
    /// replies still queued behind the failure.
    pub async fn body_many(
        &mut self,
        message_ids: &[String],
        budget: Duration,
    ) -> Result<Vec<Result<Vec<u8>, NntpError>>, NntpError> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let deadline = Instant::now() + budget;

        let mut pipeline = String::new();
        for id in message_ids {
            pipeline.push_str("BODY ");
            pipeline.push_str(&wrap_id(id));
            pipeline.push_str("\r\n");
        }
        self.poisoned = true;
        self.send(&pipeline).await?;

        let mut out = Vec::with_capacity(message_ids.len());
        for _ in message_ids {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(NntpError::Timeout);
            }
            let status = self
                .read_status(remaining.min(COMMAND_TIMEOUT))
                .await
                .map_err(stalled_as_timeout)?;
            match classify_article_status(&status, "222") {
                Ok(()) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(NntpError::Timeout);
                    }
                    let mut buf = Vec::with_capacity(BODY_BUF_CAPACITY);
                    self.stream
                        .read_until_dot(&mut buf, remaining)
                        .await
                        .map_err(stalled_as_timeout)?;
                    self.traffic
                        .bytes_read
                        .fetch_add(buf.len() as u64, Ordering::Relaxed);
                    self.traffic.articles_read.fetch_add(1, Ordering::Relaxed);
                    out.push(Ok(buf));
                }
                Err(error @ NntpError::ArticleNotFound(_)) => out.push(Err(error)),
                Err(error) => return Err(error),
            }
        }

        self.poisoned = false;
        self.last_used = Instant::now();
        Ok(out)
    }

    /// `STAT <message-id>`: `Ok(true)` when the article exists, `Ok(false)`
    /// for `423`/`430`.
    pub async fn stat(&mut self, message_id: &str) -> Result<bool, NntpError> {
        let mut last_err = None;
        for attempt in 0..COMMAND_ATTEMPTS {
            match self.stat_once(message_id).await {
                Ok(exists) => return Ok(exists),
                Err(error) => {
                    if !should_reconnect(&error) || attempt + 1 == COMMAND_ATTEMPTS {
                        return Err(error);
                    }
                    last_err = Some(error);
                    self.reconnect().await?;
                }
            }
        }
        Err(last_err.unwrap_or(NntpError::Protocol("retry exhausted without error")))
    }

    async fn stat_once(&mut self, message_id: &str) -> Result<bool, NntpError> {
        self.poisoned = true;
        self.send(&format!("STAT {}\r\n", wrap_id(message_id)))
            .await?;
        let status = self.read_status(STAT_TIMEOUT).await?;
        self.poisoned = false;
        self.last_used = Instant::now();
        match classify_article_status(&status, "223") {
            Ok(()) => Ok(true),
            Err(NntpError::ArticleNotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// STAT a batch of message-ids over this one connection, writing every
    /// command before reading any reply. Replies come back in order, so a
    /// batch of `n` costs one round trip instead of `n` — which is the whole
    /// point of validating a sample without borrowing a connection per
    /// article.
    ///
    /// Results are positional. A transport failure part-way through leaves
    /// unread replies on the wire, so the connection is poisoned and the
    /// caller gets the error rather than a truncated batch.
    pub async fn stat_many(&mut self, message_ids: &[String]) -> Result<Vec<bool>, NntpError> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipeline = String::new();
        for id in message_ids {
            pipeline.push_str("STAT ");
            pipeline.push_str(&wrap_id(id));
            pipeline.push_str("\r\n");
        }
        self.poisoned = true;
        self.send(&pipeline).await?;
        let mut out = Vec::with_capacity(message_ids.len());
        for _ in message_ids {
            let status = self.read_status(COMMAND_TIMEOUT).await?;
            match classify_article_status(&status, "223") {
                Ok(()) => out.push(true),
                Err(NntpError::ArticleNotFound(_)) => out.push(false),
                Err(error) => return Err(error),
            }
        }
        self.poisoned = false;
        self.last_used = Instant::now();
        Ok(out)
    }

    pub async fn quit(&mut self) {
        drop(self.send("QUIT\r\n").await);
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn last_used(&self) -> Instant {
        self.last_used
    }

    pub fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    async fn send(&mut self, line: &str) -> Result<(), NntpError> {
        self.stream.write_all(line.as_bytes()).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_status(&mut self, timeout: Duration) -> Result<String, NntpError> {
        let mut line = String::new();
        let read = self.stream.read_line(&mut line, timeout).await?;
        if read == 0 {
            return Err(NntpError::Protocol("EOF reading status"));
        }
        Ok(line.trim_end_matches(['\r', '\n']).to_string())
    }
}

async fn dial(config: &NntpServerConfig) -> Result<NntpStream, NntpError> {
    /// Read buffer per connection. `fill_into` asks for 64 KiB at a time, so a
    /// larger one batches nothing it was not already batching — see
    /// [`NntpTransport`] for what was measured. Was 512 KiB, times the 100
    /// connections a provider allows.
    const READ_BUF: usize = 64 * 1024;

    let addrs = resolve(&config.host, config.port).await?;
    let tcp = match connect_first(&addrs).await {
        Ok(tcp) => tcp,
        Err(error) => {
            // A cached address that no longer accepts connections means the
            // provider rotated IPs; force the next dial to re-resolve.
            riven_core::dns::invalidate(&config.host);
            return Err(error);
        }
    };
    drop(tcp.set_nodelay(true));
    if !config.use_tls {
        return Ok(NntpStream::new(NntpTransport::Plain(
            BufReader::with_capacity(READ_BUF, tcp),
        )));
    }
    let connector = build_tls_connector()?;
    let server_name =
        ServerName::try_from(config.host.clone()).map_err(|e| NntpError::Tls(e.to_string()))?;
    let tls = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| NntpError::Tls(e.to_string()))?;
    Ok(NntpStream::new(NntpTransport::Tls(Box::new(
        BufReader::with_capacity(READ_BUF, tls),
    ))))
}

fn wrap_id(message_id: &str) -> String {
    if message_id.starts_with('<') {
        message_id.to_string()
    } else {
        format!("<{message_id}>")
    }
}

/// Map an article command's status line onto an error, given the code that
/// means success for that command.
fn classify_article_status(status: &str, ok_code: &str) -> Result<(), NntpError> {
    if status.starts_with(ok_code) {
        return Ok(());
    }
    if status.starts_with("430") || status.starts_with("423") {
        return Err(NntpError::ArticleNotFound(status.to_string()));
    }
    if status.starts_with("480") {
        return Err(NntpError::AuthRequired(status.to_string()));
    }
    if is_too_many_connections(status) {
        return Err(NntpError::TooManyConnections(status.to_string()));
    }
    Err(NntpError::ServerError(status.to_string()))
}

/// Surface a read that ran out of time as [`NntpError::Timeout`] rather than a
/// generic io error, so the pool can tell "this provider stopped answering"
/// apart from "the socket broke" and demote on the former.
fn stalled_as_timeout(error: impl Into<NntpError>) -> NntpError {
    match error.into() {
        NntpError::Io(io) if io.kind() == std::io::ErrorKind::TimedOut => NntpError::Timeout,
        other => other,
    }
}

/// Whether a failed command is worth retrying on a fresh socket. A missing
/// article is a fact about the provider's spool, not the connection, and
/// reconnecting for it just wastes a dial.
fn should_reconnect(error: &NntpError) -> bool {
    matches!(
        error,
        NntpError::AuthRequired(_)
            | NntpError::Io(_)
            | NntpError::Tls(_)
            | NntpError::Timeout
            | NntpError::Protocol(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_bare_message_ids_only() {
        assert_eq!(wrap_id("abc@host"), "<abc@host>");
        assert_eq!(wrap_id("<abc@host>"), "<abc@host>");
    }

    #[test]
    fn article_status_classification() {
        assert!(classify_article_status("222 0 <x>", "222").is_ok());
        assert!(matches!(
            classify_article_status("430 no such article", "222"),
            Err(NntpError::ArticleNotFound(_))
        ));
        assert!(matches!(
            classify_article_status("423 no such article number", "223"),
            Err(NntpError::ArticleNotFound(_))
        ));
        assert!(matches!(
            classify_article_status("480 auth required", "222"),
            Err(NntpError::AuthRequired(_))
        ));
        assert!(matches!(
            classify_article_status("502 too many connections", "222"),
            Err(NntpError::TooManyConnections(_))
        ));
    }

    #[test]
    fn missing_articles_never_trigger_reconnect() {
        assert!(!should_reconnect(&NntpError::ArticleNotFound("430".into())));
        assert!(!should_reconnect(&NntpError::ServerError("500".into())));
        assert!(!should_reconnect(&NntpError::TooManyConnections(
            "502".into()
        )));
        assert!(should_reconnect(&NntpError::AuthRequired("480".into())));
        assert!(should_reconnect(&NntpError::Timeout));
    }
}
