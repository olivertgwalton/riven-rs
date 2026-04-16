use std::time::Duration;

use anyhow::{Context, bail};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, Method, StatusCode,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    },
    response::{IntoResponse, Response},
};
use percent_encoding::percent_decode_str;
use redis::AsyncCommands;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use url::Url;

use super::ApiState;

const NNTP_TIMEOUT_SECS: u64 = 20;

trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UsenetPayload {
    nzb_url: Option<String>,
    nzb_urls: Vec<String>,
    servers: Vec<String>,
    filename: String,
}

#[derive(Clone, Debug)]
struct NzbFile {
    name: String,
    size: u64,
    segments: Vec<NzbSegment>,
}

#[derive(Clone, Debug)]
struct NzbSegment {
    bytes: u64,
    message_id: String,
}

#[derive(Clone, Copy, Debug)]
struct ByteRange {
    start: u64,
    end: u64,
}

#[derive(Debug)]
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
                .context("missing NNTP password")?;
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

    async fn decoded_body(&mut self, message_id: &str) -> anyhow::Result<Vec<u8>> {
        let response = self
            .command(&format!("BODY {}", format_message_id(message_id)))
            .await?;
        if !response.starts_with("222 ") {
            bail!("unexpected BODY response for {message_id}: {response}");
        }

        let mut decoder = YencDecoder::default();
        loop {
            let line = self.read_line().await?;
            if line == "." {
                break;
            }
            let line = line.strip_prefix("..").unwrap_or(&line);
            decoder.push_line(line.as_bytes());
        }
        Ok(decoder.finish())
    }
}

pub(super) async fn usenet_stream_handler(
    State(state): State<ApiState>,
    Path((hash, filename)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    match serve_usenet_stream(state, hash, filename, method, headers).await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(error = %error, "usenet stream failed");
            (StatusCode::BAD_GATEWAY, error.to_string()).into_response()
        }
    }
}

async fn serve_usenet_stream(
    state: ApiState,
    hash: String,
    filename: String,
    method: Method,
    headers: HeaderMap,
) -> anyhow::Result<Response> {
    let payload = load_payload(&state, &hash)
        .await
        .context("Usenet stream expired")?;
    let nzb = fetch_nzb(&state, &payload).await?;
    let files = parse_nzb_files(&nzb);
    let decoded_filename = percent_decode(&filename);
    let file = files
        .iter()
        .find(|file| file.name == decoded_filename || file.name == payload.filename)
        .or_else(|| files.iter().max_by_key(|file| file.size))
        .context("NZB does not contain a directly streamable media file")?;

    let range = parse_range(headers.get(RANGE), file.size)?;
    let content_length = range
        .map(|range| range.end - range.start + 1)
        .unwrap_or(file.size);
    let body = if method == Method::HEAD {
        Vec::new()
    } else {
        fetch_file_range(&payload.servers, file, range).await?
    };

    let mut response = Response::builder().status(if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    });
    let headers_out = response.headers_mut().expect("headers are available");
    headers_out.insert(ACCEPT_RANGES, "bytes".parse()?);
    headers_out.insert(CONTENT_TYPE, content_type(&file.name).parse()?);
    headers_out.insert(CONTENT_LENGTH, content_length.to_string().parse()?);
    if let Some(range) = range {
        headers_out.insert(
            CONTENT_RANGE,
            format!("bytes {}-{}/{}", range.start, range.end, file.size).parse()?,
        );
    }

    Ok(response.body(Body::from(body))?)
}

async fn load_payload(state: &ApiState, hash: &str) -> Option<UsenetPayload> {
    let mut conn = state.job_queue.redis.clone();
    let value: Option<String> = conn
        .get(format!("plugin:usenet:payload:{hash}"))
        .await
        .ok()
        .flatten();
    value.and_then(|value| serde_json::from_str(&value).ok())
}

async fn fetch_nzb(state: &ApiState, payload: &UsenetPayload) -> anyhow::Result<String> {
    let urls = if payload.nzb_urls.is_empty() {
        payload.nzb_url.iter().cloned().collect::<Vec<_>>()
    } else {
        payload.nzb_urls.clone()
    };

    let mut errors = Vec::new();
    for url in urls {
        match state.stream_client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return Ok(response.text().await?),
            Ok(response) => errors.push(format!("{url}: HTTP {}", response.status())),
            Err(error) => errors.push(format!("{url}: {error}")),
        }
    }
    bail!("failed to fetch NZB: {}", errors.join("; "))
}

async fn fetch_file_range(
    servers: &[String],
    file: &NzbFile,
    range: Option<ByteRange>,
) -> anyhow::Result<Vec<u8>> {
    let range = range.unwrap_or(ByteRange {
        start: 0,
        end: file.size.saturating_sub(1),
    });

    let mut errors = Vec::new();
    for server in servers {
        match NntpServer::parse(server) {
            Ok(server) => match fetch_file_range_from_server(&server, file, range).await {
                Ok(bytes) => return Ok(bytes),
                Err(error) => errors.push(error.to_string()),
            },
            Err(error) => errors.push(error.to_string()),
        }
    }

    bail!(
        "failed to fetch Usenet articles from configured NNTP servers: {}",
        errors.join("; ")
    )
}

async fn fetch_file_range_from_server(
    server: &NntpServer,
    file: &NzbFile,
    range: ByteRange,
) -> anyhow::Result<Vec<u8>> {
    let mut conn = NntpConnection::connect(server).await?;
    let mut output = Vec::with_capacity((range.end - range.start + 1) as usize);
    let mut cursor = 0u64;

    for segment in &file.segments {
        let segment_start = cursor;
        let segment_end = cursor + segment.bytes.saturating_sub(1);
        cursor += segment.bytes;

        if segment_end < range.start {
            continue;
        }
        if segment_start > range.end {
            break;
        }

        let decoded = conn.decoded_body(&segment.message_id).await?;
        let take_start = range.start.saturating_sub(segment_start) as usize;
        let take_end = (range.end.min(segment_end) - segment_start + 1) as usize;
        output.extend_from_slice(
            &decoded[take_start.min(decoded.len())..take_end.min(decoded.len())],
        );
    }

    let _ = conn.command("QUIT").await;
    Ok(output)
}

fn parse_nzb_files(xml: &str) -> Vec<NzbFile> {
    let mut files = Vec::new();
    let mut cursor = 0usize;
    let lower = xml.to_ascii_lowercase();

    while let Some(relative_start) = lower[cursor..].find("<file") {
        let start = cursor + relative_start;
        let Some(tag_end_relative) = lower[start..].find('>') else {
            break;
        };
        let tag_end = start + tag_end_relative;
        let file_tag = &xml[start..=tag_end];
        let Some(end_relative) = lower[tag_end..].find("</file>") else {
            break;
        };
        let end = tag_end + end_relative;
        let body = &xml[tag_end + 1..end];
        let name = attr_value(file_tag, "subject")
            .as_deref()
            .and_then(filename_from_subject)
            .unwrap_or_else(|| "usenet-file".to_string());
        let segments = parse_segments(body);
        let size = segments.iter().map(|segment| segment.bytes).sum();
        if is_media_file(&name) && !segments.is_empty() {
            files.push(NzbFile {
                name,
                size,
                segments,
            });
        }
        cursor = end + "</file>".len();
    }

    files
}

fn parse_segments(body: &str) -> Vec<NzbSegment> {
    let mut segments = Vec::new();
    let mut cursor = 0usize;
    let lower = body.to_ascii_lowercase();

    while let Some(relative_start) = lower[cursor..].find("<segment") {
        let start = cursor + relative_start;
        let Some(tag_end_relative) = lower[start..].find('>') else {
            break;
        };
        let tag_end = start + tag_end_relative;
        let Some(end_relative) = lower[tag_end..].find("</segment>") else {
            break;
        };
        let end = tag_end + end_relative;
        if let Some(bytes) =
            attr_value(&body[start..=tag_end], "bytes").and_then(|value| value.parse::<u64>().ok())
        {
            let message_id = decode_xml_entities(body[tag_end + 1..end].trim());
            if !message_id.is_empty() {
                segments.push(NzbSegment { bytes, message_id });
            }
        }
        cursor = end + "</segment>".len();
    }

    segments
}

fn parse_range(
    value: Option<&axum::http::HeaderValue>,
    file_size: u64,
) -> anyhow::Result<Option<ByteRange>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let raw = value.to_str()?.trim();
    let Some(spec) = raw.strip_prefix("bytes=") else {
        bail!("invalid Range header");
    };
    if spec.contains(',') {
        bail!("multiple byte ranges are not supported");
    }
    let (start, end) = spec.split_once('-').context("invalid Range header")?;
    let range = match (start.is_empty(), end.is_empty()) {
        (false, false) => ByteRange {
            start: start.parse()?,
            end: end.parse()?,
        },
        (false, true) => ByteRange {
            start: start.parse()?,
            end: file_size.saturating_sub(1),
        },
        (true, false) => {
            let suffix = end.parse::<u64>()?;
            ByteRange {
                start: file_size.saturating_sub(suffix),
                end: file_size.saturating_sub(1),
            }
        }
        (true, true) => bail!("invalid Range header"),
    };
    if range.start > range.end || range.start >= file_size {
        bail!("range is not satisfiable");
    }
    Ok(Some(range))
}

#[derive(Default)]
struct YencDecoder {
    bytes: Vec<u8>,
}

impl YencDecoder {
    fn push_line(&mut self, line: &[u8]) {
        if line.starts_with(b"=ybegin") || line.starts_with(b"=ypart") || line.starts_with(b"=yend")
        {
            return;
        }
        let mut index = 0usize;
        while index < line.len() {
            let mut byte = line[index];
            if byte == b'=' && index + 1 < line.len() {
                index += 1;
                byte = line[index].wrapping_sub(64);
            }
            self.bytes.push(byte.wrapping_sub(42));
            index += 1;
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

fn attr_value(tag: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=");
    let start = tag.find(&needle)? + needle.len();
    let quote = tag[start..].chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value_start = start + quote.len_utf8();
    let value_end = tag[value_start..].find(quote)? + value_start;
    Some(decode_xml_entities(&tag[value_start..value_end]))
}

fn filename_from_subject(subject: &str) -> Option<String> {
    for separator in ['"', '\''] {
        for part in subject.split(separator) {
            let cleaned = clean_filename(part);
            if is_media_file(&cleaned) {
                return Some(cleaned);
            }
        }
    }
    subject
        .split_whitespace()
        .map(clean_filename)
        .find(|part| is_media_file(part))
}

fn clean_filename(value: &str) -> String {
    decode_xml_entities(value)
        .trim()
        .trim_matches(['"', '\'', '(', ')', '[', ']'])
        .trim()
        .to_string()
}

fn decode_xml_entities(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn format_message_id(message_id: &str) -> String {
    let trimmed = message_id.trim();
    if trimmed.starts_with('<') && trimmed.ends_with('>') {
        trimmed.to_string()
    } else {
        format!("<{trimmed}>")
    }
}

fn percent_decode(value: &str) -> String {
    percent_decode_str(value).decode_utf8_lossy().into_owned()
}

fn is_media_file(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    ["mkv", "mp4", "avi", "mov", "m4v", "webm", "ts", "m2ts"]
        .iter()
        .any(|ext| lower.ends_with(&format!(".{ext}")))
}

fn content_type(filename: &str) -> &'static str {
    let lower = filename.to_ascii_lowercase();
    if lower.ends_with(".mp4") || lower.ends_with(".m4v") {
        "video/mp4"
    } else if lower.ends_with(".webm") {
        "video/webm"
    } else if lower.ends_with(".ts") || lower.ends_with(".m2ts") {
        "video/mp2t"
    } else {
        "video/x-matroska"
    }
}
