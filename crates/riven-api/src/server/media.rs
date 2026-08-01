use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
        header::{
            ACCEPT_RANGES, CACHE_CONTROL, CONNECTION, CONTENT_DISPOSITION, CONTENT_LENGTH,
            CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE, VARY,
        },
    },
    response::{IntoResponse, Response},
};
use riven_core::stream_link::request_stream_url;
use riven_usenet::UsenetStreamer;
use riven_vfs::prefetch::{FileKey, Prefetcher};
use riven_vfs::source::{ByteSource, UsenetSource};

use super::ApiState;
use super::auth::{authorize_request, check_stremio_token, has_valid_api_key};

const MEDIA_RESPONSE_HEADERS: [HeaderName; 7] = [
    ACCEPT_RANGES,
    CACHE_CONTROL,
    CONTENT_DISPOSITION,
    CONTENT_LENGTH,
    CONTENT_RANGE,
    CONTENT_TYPE,
    ETAG,
];

const MEDIA_OPTIONAL_HEADERS: [HeaderName; 1] = [LAST_MODIFIED];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestedRange {
    start: Option<u64>,
    end: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeHeaderError {
    Invalid,
    MultipleRangesUnsupported,
    Unsatisfiable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpstreamRangeError {
    MissingPartialContent,
}

fn copy_response_headers(from: &reqwest::header::HeaderMap, to: &mut HeaderMap) {
    for name in MEDIA_RESPONSE_HEADERS
        .iter()
        .chain(MEDIA_OPTIONAL_HEADERS.iter())
    {
        if let Some(value) = from.get(name)
            && let Ok(cloned) = HeaderValue::from_bytes(value.as_bytes())
        {
            to.insert(name.clone(), cloned);
        }
    }
}

fn is_expired_stream_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::UNAUTHORIZED
            | reqwest::StatusCode::FORBIDDEN
            | reqwest::StatusCode::NOT_FOUND
            | reqwest::StatusCode::GONE
    )
}

fn parse_requested_range(
    range_header: Option<&HeaderValue>,
    file_size: u64,
) -> Result<Option<RequestedRange>, RangeHeaderError> {
    let Some(range_header) = range_header else {
        return Ok(None);
    };

    let raw = range_header
        .to_str()
        .map_err(|_e| RangeHeaderError::Invalid)?
        .trim();
    let Some(spec) = raw.strip_prefix("bytes=") else {
        return Err(RangeHeaderError::Invalid);
    };

    if spec.contains(',') {
        return Err(RangeHeaderError::MultipleRangesUnsupported);
    }

    let (start, end) = spec.split_once('-').ok_or(RangeHeaderError::Invalid)?;
    if start.is_empty() && end.is_empty() {
        return Err(RangeHeaderError::Invalid);
    }

    let requested = RequestedRange {
        start: (!start.is_empty())
            .then(|| start.parse::<u64>().map_err(|_e| RangeHeaderError::Invalid))
            .transpose()?,
        end: (!end.is_empty())
            .then(|| end.parse::<u64>().map_err(|_e| RangeHeaderError::Invalid))
            .transpose()?,
    };

    match (requested.start, requested.end) {
        (Some(start), Some(end)) if start > end => Err(RangeHeaderError::Unsatisfiable),
        (Some(start), _) if start >= file_size => Err(RangeHeaderError::Unsatisfiable),
        (None, Some(0)) => Err(RangeHeaderError::Unsatisfiable),
        (None, Some(_)) | (Some(_), _) => Ok(Some(requested)),
        (None, None) => Err(RangeHeaderError::Invalid),
    }
}

fn range_error_response(error: RangeHeaderError, file_size: u64) -> Response {
    let status = match error {
        RangeHeaderError::Invalid => StatusCode::BAD_REQUEST,
        RangeHeaderError::MultipleRangesUnsupported => StatusCode::NOT_IMPLEMENTED,
        RangeHeaderError::Unsatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
    };

    let mut response = Response::builder().status(status);
    if matches!(error, RangeHeaderError::Unsatisfiable)
        && let Some(headers) = response.headers_mut()
    {
        let content_range = format!("bytes */{file_size}");
        if let Ok(value) = HeaderValue::from_str(&content_range) {
            headers.insert(CONTENT_RANGE, value);
        }
        headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    }

    response
        .body(Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn is_seek_request(range: Option<RequestedRange>) -> bool {
    match range {
        Some(RequestedRange { start: Some(0), .. }) | None => false,
        Some(_) => true,
    }
}

fn validate_upstream_range_response(
    requested_range: Option<RequestedRange>,
    upstream_status: reqwest::StatusCode,
) -> Result<(), UpstreamRangeError> {
    if requested_range.is_some()
        && upstream_status != reqwest::StatusCode::PARTIAL_CONTENT
        && upstream_status != reqwest::StatusCode::RANGE_NOT_SATISFIABLE
    {
        return Err(UpstreamRangeError::MissingPartialContent);
    }

    Ok(())
}

fn build_media_request(
    client: &reqwest::Client,
    method: Method,
    stream_url: &str,
    request_headers: &HeaderMap,
) -> reqwest::RequestBuilder {
    let mut request = client
        .request(method, stream_url)
        .header(reqwest::header::ACCEPT_ENCODING, "identity")
        .header(CONNECTION, "keep-alive");

    if let Some(value) = request_headers.get(RANGE) {
        request = request.header(RANGE, value.clone());
    }
    if let Some(value) = request_headers.get(IF_RANGE) {
        request = request.header(IF_RANGE, value.clone());
    }

    request
}

async fn resolve_media_stream_url(
    state: &ApiState,
    entry: &riven_db::entities::FileSystemEntry,
) -> Option<String> {
    let url = request_stream_url(
        entry.download_url.as_deref(),
        entry.provider.as_deref(),
        Some(entry.id),
        entry.stream_url.as_deref(),
        &state.link_request_tx,
    )
    .await?;
    if let Err(error) = riven_db::repo::update_stream_url(entry.id, &url).await {
        tracing::warn!(
            entry_id = entry.id,
            error = %error,
            "failed to persist refreshed stream url"
        );
    }
    Some(url)
}

async fn prewarm_playback_target(
    state: ApiState,
    entry: riven_db::entities::FileSystemEntry,
    reason: &'static str,
) {
    if entry.download_url.is_none() {
        return;
    }
    if entry.stream_url.is_some() {
        tracing::debug!(
            entry_id = entry.id,
            reason,
            "skipping prewarm, stream url already present"
        );
        return;
    }

    let started = Instant::now();
    match resolve_media_stream_url(&state, &entry).await {
        Some(_) => tracing::debug!(
            entry_id = entry.id,
            reason,
            elapsed_ms = started.elapsed().as_millis(),
            "prewarmed media stream url"
        ),
        None => tracing::warn!(
            entry_id = entry.id,
            reason,
            "failed to prewarm media stream url"
        ),
    }
}

fn maybe_spawn_next_prewarm(
    state: &ApiState,
    entry_id: i64,
    method: &Method,
    requested_range: Option<RequestedRange>,
) {
    if method != Method::GET || is_seek_request(requested_range) {
        return;
    }

    let state = state.clone();
    tokio::spawn(async move {
        match riven_db::repo::get_next_playback_entry(entry_id).await {
            Ok(Some(next_entry)) => {
                prewarm_playback_target(state, next_entry, "next_playback").await
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(entry_id, error = %error, "failed to look up next playback target")
            }
        }
    });
}

async fn fetch_media_response(
    state: &ApiState,
    method: Method,
    stream_url: &str,
    request_headers: &HeaderMap,
) -> Result<reqwest::Response> {
    Ok(
        build_media_request(&state.stream_client, method, stream_url, request_headers)
            .send()
            .await?,
    )
}

async fn load_media_entry(entry_id: i64) -> Result<Option<riven_db::entities::FileSystemEntry>> {
    riven_db::repo::get_media_entry_by_id(entry_id).await
}

/// Query string for the media bridge. `?download=1` (any value) flips the
/// response to a forced browser download (`Content-Disposition: attachment`)
/// instead of inline playback.
///
/// `api_key` and `token` exist because playback clients can't set headers.
/// Stremio in particular only ever gets a URL, so the credential has to be in
/// it — `token` carries the derived Stremio addon token (see
/// `auth::stremio_addon_token`) so the real API key never leaves Riven.
#[derive(Debug, Default, serde::Deserialize)]
pub(super) struct MediaQuery {
    download: Option<String>,
    api_key: Option<String>,
    token: Option<String>,
}

/// Resolve the in-process usenet target `(info_hash, file_index)` for an entry,
/// preferring the explicit columns and falling back to parsing a `usenet://`
/// stream URL (mirrors the VFS open path).
fn usenet_target(entry: &riven_db::entities::FileSystemEntry) -> Option<(String, usize)> {
    if let (Some(info_hash), Some(idx)) =
        (entry.usenet_info_hash.as_deref(), entry.usenet_file_index)
    {
        return Some((info_hash.to_string(), usize::try_from(idx).unwrap_or(0)));
    }
    let candidate = entry
        .stream_url
        .as_deref()
        .or(entry.download_url.as_deref())?;
    parse_usenet_url(candidate)
}

fn parse_usenet_url(url: &str) -> Option<(String, usize)> {
    let rest = url.strip_prefix("usenet://")?;
    let (hash, idx) = rest.split_once('/')?;
    if hash.is_empty() {
        return None;
    }
    Some((hash.to_string(), idx.parse().ok()?))
}

/// Build a `Content-Disposition: attachment` header that names the saved file
/// after the entry's original filename (sanitised to header-safe ASCII).
/// Mark a media response as belonging to the caller who authenticated for it.
///
/// Whether these bytes may be served is now a function of the `Cookie` /
/// `Authorization` header, but the URL — `/media/{id}` — is a small integer and
/// entirely guessable. Without this, a shared cache in front of riven (nginx,
/// Varnish, Cloudflare — routine for a publicly exposed instance) may store a
/// `200` produced for a signed-in user under that bare URL and replay it to a
/// caller who presented no credential at all.
///
/// It matters most on the debrid path, where `copy_response_headers` forwards
/// the CDN's own `Cache-Control` verbatim. Those upstream URLs are signed and
/// unguessable, so upstream marks them cacheable — and riven would otherwise
/// re-emit that `public` on a URL that is neither. This has to overwrite, not
/// fill in a default.
///
/// Both headers, because they fail independently: an operator who has set
/// `proxy_ignore_headers Cache-Control` still gets the `Vary` protection, and a
/// cache that mishandles `Vary` still sees `no-store`.
pub(super) fn stamp_private_cache_headers(headers: &mut HeaderMap) {
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    headers.insert(
        VARY,
        HeaderValue::from_static("Cookie, Authorization, Range"),
    );
}

fn attachment_disposition(entry: &riven_db::entities::FileSystemEntry) -> Option<HeaderValue> {
    let name = entry
        .original_filename
        .clone()
        .or_else(|| {
            entry
                .path
                .rsplit('/')
                .next()
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("download-{}", entry.id));
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii() && !c.is_control() && c != '"' && c != '\\' {
                c
            } else {
                '_'
            }
        })
        .collect();
    HeaderValue::from_str(&format!("attachment; filename=\"{sanitized}\"")).ok()
}

/// Resolve a requested byte range against a known file size into a concrete,
/// satisfiable `(start, end_inclusive, is_partial)` triple.
fn resolve_concrete_range(range: Option<RequestedRange>, file_size: u64) -> (u64, u64, bool) {
    match range {
        Some(r) => {
            let (start, end) = match (r.start, r.end) {
                (Some(start), Some(end)) => (start, end.min(file_size - 1)),
                (Some(start), None) => (start, file_size - 1),
                (None, Some(suffix)) => (file_size.saturating_sub(suffix), file_size - 1),
                (None, None) => (0, file_size - 1),
            };
            (start, end, true)
        }
        None => (0, file_size - 1, false),
    }
}

/// Stream a usenet-backed entry directly from the in-process streamer. Usenet
/// entries have no HTTP origin, so the debrid proxy path cannot serve them;
/// instead the range is read through the same read-ahead window the FUSE mount
/// uses, so both paths share one buffering policy and one unit cache.
async fn serve_usenet_media(
    entry: &riven_db::entities::FileSystemEntry,
    info_hash: String,
    file_index: usize,
    method: Method,
    headers: &HeaderMap,
    want_download: bool,
) -> Response {
    let file_size = u64::try_from(entry.file_size).unwrap_or(0);
    if file_size == 0 {
        return StatusCode::NOT_FOUND.into_response();
    }

    let requested_range = match parse_requested_range(headers.get(RANGE), file_size) {
        Ok(range) => range,
        Err(error) => return range_error_response(error, file_size),
    };
    let (start, end_inclusive, is_partial) = resolve_concrete_range(requested_range, file_size);

    let Some(streamer) = UsenetStreamer::existing_shared() else {
        tracing::warn!(
            entry_id = entry.id,
            "usenet streamer unavailable for media download"
        );
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    let content_length = end_inclusive - start + 1;
    let status = if is_partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    let mut builder = Response::builder().status(status);
    if let Some(out) = builder.headers_mut() {
        out.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        out.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        if let Ok(value) = HeaderValue::from_str(&content_length.to_string()) {
            out.insert(CONTENT_LENGTH, value);
        }
        if is_partial
            && let Ok(value) =
                HeaderValue::from_str(&format!("bytes {start}-{end_inclusive}/{file_size}"))
        {
            out.insert(CONTENT_RANGE, value);
        }
        if want_download && let Some(disposition) = attachment_disposition(entry) {
            out.insert(CONTENT_DISPOSITION, disposition);
        }
        stamp_private_cache_headers(out);
    }

    tracing::debug!(
        entry_id = entry.id,
        method = %method,
        range = ?requested_range,
        want_download,
        "usenet media bridge serving"
    );

    if method == Method::HEAD {
        return builder
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    // Read through the same [`Prefetcher`] the FUSE mount uses, rather than
    // walking articles here.
    //
    // This path used to call `LocalByteSource::read_range` itself from a
    // `stream::unfold`, which is strictly sequential: every 8 MiB chunk started
    // with nothing on the wire, blocked on the slowest article it spanned, then
    // left the wire idle through the concatenation and the whole client write
    // before the next chunk began. It measured 3.2 MB/s against 40 MB/s from a
    // client that simply keeps its connections busy, and ~64 MB/s from raw NNTP
    // at the same connection count. The read-ahead window is what keeps the wire
    // busy across chunk boundaries, and there is no reason to have a second,
    // worse copy of it here.
    let filename = entry.path.rsplit('/').next().unwrap_or(&entry.path);
    let source: Arc<dyn ByteSource> = Arc::new(UsenetSource::new(
        Arc::new(streamer),
        Arc::from(info_hash.as_str()),
        file_index,
        file_size,
        filename,
    ));
    // Dropped with the response body, which aborts whatever the window still
    // has in flight — so a cancelled download stops costing connections.
    let reader = Arc::new(Prefetcher::new(
        source,
        FileKey::bridge(entry.id),
        &tokio::runtime::Handle::current(),
    ));

    // Deliberately far smaller than the old 8 MiB. A request spanning more units
    // than the window holds is served directly by the prefetcher, bypassing the
    // very read-ahead this change exists to use — and at a 716 KiB article, 8 MiB
    // spans twelve units against a non-archive window of eight.
    const CHUNK: usize = 1024 * 1024;
    // The entry path rides along with the stream state purely so a mid-stream
    // read failure names the title instead of only its info_hash.
    let entry_path: Arc<str> = Arc::from(entry.path.as_str());
    let body_stream = futures::stream::unfold(
        (reader, entry_path, start, end_inclusive),
        move |(reader, entry_path, pos, end)| async move {
            if pos > end {
                return None;
            }
            let len = CHUNK.min((end - pos + 1) as usize);
            match reader.read(pos, len).await {
                Ok(bytes) if !bytes.is_empty() => {
                    let next = pos + bytes.len() as u64;
                    Some((
                        Ok::<bytes::Bytes, std::io::Error>(bytes),
                        (reader, entry_path, next, end),
                    ))
                }
                Ok(_) => None,
                Err(error) => {
                    tracing::warn!(
                        file = %entry_path,
                        file_index,
                        pos,
                        error = %error,
                        "usenet download read failed"
                    );
                    Some((
                        Err(std::io::Error::other("usenet read failed")),
                        (reader, entry_path, end + 1, end),
                    ))
                }
            }
        },
    );

    builder
        .body(Body::from_stream(body_stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Accept a signed-in session, the usual header credential, or either URL-borne
/// credential a player can carry: the API key itself, or the derived Stremio
/// addon token.
///
/// The session is what the *browser* has. A download is an `<a download>`, which
/// is a plain top-level navigation — it carries cookies and nothing else, since
/// no header can be attached to it. Checking only the API key here meant every
/// download button wrote a 12-byte file reading `Unauthorized` to disk. The
/// alternative — putting `?api_key=` in the href — would publish an admin-level
/// credential into the DOM, browser history and the download manager, so the
/// session is both the working answer and the safer one.
///
/// Any role suffices: the ladder governs mutations, and a user who can see an
/// item in the library can already stream it.
///
/// Order is deliberate, and is about cost rather than trust — the two URL-borne
/// credentials are constant-time string comparisons, while a session costs two
/// database lookups. This route serves *range* requests, so a player streaming a
/// file hits it continuously; putting the session first would put a pair of
/// queries under every one of those. The cheap checks answer for every player,
/// and the session is only reached by the browser, which is the one caller that
/// has nothing else to present.
async fn media_credential_ok(state: &ApiState, headers: &HeaderMap, query: &MediaQuery) -> bool {
    if query
        .token
        .as_deref()
        .is_some_and(|token| check_stremio_token(state, token))
    {
        return true;
    }

    let encoded = query
        .api_key
        .as_deref()
        .map(|api_key| format!("api_key={}", urlencoding_encode(api_key)));
    if has_valid_api_key(state, headers, encoded.as_deref()) {
        return true;
    }

    // Last, and only for a caller that presented neither: the session cookie.
    // `authorize_request` verifies it against riven's own store — expiry and
    // bans included — so this is a lookup, not a claim. Reaching it also means
    // an anonymous request gets logged as a rejection exactly once, rather than
    // every legitimate Stremio range request logging a spurious warning on its
    // way to the token check.
    authorize_request(state, headers, encoded.as_deref())
        .await
        .is_ok()
}

/// `has_valid_api_key` parses its `query` argument as a form-encoded string, so a
/// value lifted out of an already-decoded `MediaQuery` has to be re-encoded
/// before being handed back to it.
fn urlencoding_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

pub(super) async fn media_bridge_handler(
    State(state): State<ApiState>,
    Path(entry_id): Path<i64>,
    Query(query): Query<MediaQuery>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let request_started = Instant::now();

    if !media_credential_ok(&state, &headers, &query).await {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let entry = match load_media_entry(entry_id).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            tracing::error!(entry_id, error = %error, "failed to load media entry");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let want_download = query.download.is_some();

    if let Some((info_hash, file_index)) = usenet_target(&entry) {
        return serve_usenet_media(
            &entry,
            info_hash,
            file_index,
            method,
            &headers,
            want_download,
        )
        .await;
    }

    if entry.download_url.is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }

    let requested_range = match parse_requested_range(
        headers.get(RANGE),
        u64::try_from(entry.file_size).unwrap_or(0),
    ) {
        Ok(range) => range,
        Err(error) => {
            return range_error_response(error, u64::try_from(entry.file_size).unwrap_or(0));
        }
    };

    let mut refreshed_stream_url = false;
    let mut stream_url = entry.stream_url.clone();
    if stream_url.is_none() {
        refreshed_stream_url = true;
        stream_url = resolve_media_stream_url(&state, &entry).await;
    }

    let Some(initial_stream_url) = stream_url.take() else {
        return StatusCode::BAD_GATEWAY.into_response();
    };

    let mut upstream =
        match fetch_media_response(&state, method.clone(), &initial_stream_url, &headers).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(entry_id, error = %error, "initial media request failed");
                let Some(refreshed) = resolve_media_stream_url(&state, &entry).await else {
                    return StatusCode::BAD_GATEWAY.into_response();
                };
                refreshed_stream_url = true;

                match fetch_media_response(&state, method.clone(), &refreshed, &headers).await {
                    Ok(response) => response,
                    Err(error) => {
                        tracing::error!(entry_id, error = %error, "refreshed media request failed");
                        return StatusCode::BAD_GATEWAY.into_response();
                    }
                }
            }
        };

    let mut upstream_range_error =
        validate_upstream_range_response(requested_range, upstream.status()).err();

    if is_expired_stream_status(upstream.status()) || upstream_range_error.is_some() {
        let Some(refreshed) = resolve_media_stream_url(&state, &entry).await else {
            return StatusCode::BAD_GATEWAY.into_response();
        };
        refreshed_stream_url = true;

        match fetch_media_response(&state, method.clone(), &refreshed, &headers).await {
            Ok(response) => {
                upstream = response;
                upstream_range_error =
                    validate_upstream_range_response(requested_range, upstream.status()).err();
            }
            Err(error) => {
                tracing::error!(
                    entry_id,
                    error = %error,
                    "media request failed after refreshing stream url"
                );
                return StatusCode::BAD_GATEWAY.into_response();
            }
        }
    }

    if upstream_range_error.is_some() {
        tracing::warn!(
            entry_id,
            status = %upstream.status(),
            range = ?requested_range,
            "upstream ignored byte range request"
        );
        return StatusCode::BAD_GATEWAY.into_response();
    }

    let mut response = Response::builder().status(upstream.status());
    if let Some(headers_out) = response.headers_mut() {
        copy_response_headers(upstream.headers(), headers_out);
        headers_out
            .entry(ACCEPT_RANGES)
            .or_insert(HeaderValue::from_static("bytes"));
        if want_download && let Some(disposition) = attachment_disposition(&entry) {
            headers_out.insert(CONTENT_DISPOSITION, disposition);
        }
        // After the copy, so the CDN's own `Cache-Control` is replaced rather
        // than preserved.
        stamp_private_cache_headers(headers_out);
    }

    tracing::debug!(
        entry_id,
        method = %method,
        status = %upstream.status(),
        range = ?requested_range,
        seek = is_seek_request(requested_range),
        refreshed_stream_url,
        ttfb_ms = request_started.elapsed().as_millis(),
        content_length = ?upstream.headers().get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()),
        "media bridge served"
    );

    maybe_spawn_next_prewarm(&state, entry_id, &method, requested_range);

    if method == Method::HEAD {
        return response
            .body(Body::empty())
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    response
        .body(Body::from_stream(upstream.bytes_stream()))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The debrid path copies the CDN's headers first and stamps afterwards. A
    /// signed upstream URL is legitimately cacheable, so upstream really does
    /// send `public` — and `/media/{id}` is not signed, so re-emitting it would
    /// let a shared cache replay one user's bytes to an anonymous caller.
    /// Filling in a default instead of overwriting would reintroduce exactly
    /// that.
    #[test]
    fn private_cache_headers_overwrite_an_upstream_public_directive() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=3600"),
        );
        headers.insert(VARY, HeaderValue::from_static("Accept-Encoding"));

        stamp_private_cache_headers(&mut headers);

        assert_eq!(headers.get(CACHE_CONTROL).unwrap(), "private, no-store");
        assert_eq!(headers.get(VARY).unwrap(), "Cookie, Authorization, Range");
        // `insert` replaces rather than appends — a lingering `public` in a
        // second value would be honoured by some caches.
        assert_eq!(headers.get_all(CACHE_CONTROL).iter().count(), 1);
        assert_eq!(headers.get_all(VARY).iter().count(), 1);
    }

    #[test]
    fn parses_open_ended_byte_range() {
        let range = HeaderValue::from_static("bytes=1024-");
        assert_eq!(
            parse_requested_range(Some(&range), 10_000),
            Ok(Some(RequestedRange {
                start: Some(1024),
                end: None,
            }))
        );
    }

    #[test]
    fn parses_suffix_byte_range() {
        let range = HeaderValue::from_static("bytes=-4096");
        assert_eq!(
            parse_requested_range(Some(&range), 10_000),
            Ok(Some(RequestedRange {
                start: None,
                end: Some(4096),
            }))
        );
    }

    #[test]
    fn rejects_multiple_ranges() {
        let range = HeaderValue::from_static("bytes=0-1,5-6");
        assert_eq!(
            parse_requested_range(Some(&range), 10_000),
            Err(RangeHeaderError::MultipleRangesUnsupported)
        );
    }

    #[test]
    fn rejects_unsatisfiable_ranges() {
        let range = HeaderValue::from_static("bytes=999-1000");
        assert_eq!(
            parse_requested_range(Some(&range), 100),
            Err(RangeHeaderError::Unsatisfiable)
        );
    }

    #[test]
    fn unsatisfiable_range_response_sets_content_range() {
        let response = range_error_response(RangeHeaderError::Unsatisfiable, 1234);
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers().get(CONTENT_RANGE),
            Some(&HeaderValue::from_static("bytes */1234"))
        );
        assert_eq!(
            response.headers().get(ACCEPT_RANGES),
            Some(&HeaderValue::from_static("bytes"))
        );
    }

    #[test]
    fn detects_seek_requests() {
        assert!(!is_seek_request(None));
        assert!(!is_seek_request(Some(RequestedRange {
            start: Some(0),
            end: None,
        })));
        assert!(is_seek_request(Some(RequestedRange {
            start: Some(2048),
            end: None,
        })));
        assert!(is_seek_request(Some(RequestedRange {
            start: None,
            end: Some(2048),
        })));
    }

    #[test]
    fn parses_usenet_stream_url() {
        assert_eq!(
            parse_usenet_url("usenet://abc123/4"),
            Some(("abc123".to_string(), 4))
        );
        assert_eq!(parse_usenet_url("usenet://abc123/"), None);
        assert_eq!(parse_usenet_url("usenet:///4"), None);
        assert_eq!(parse_usenet_url("https://cdn.example/file.mkv"), None);
    }

    #[test]
    fn resolves_full_range_when_unspecified() {
        assert_eq!(resolve_concrete_range(None, 1000), (0, 999, false));
    }

    #[test]
    fn resolves_open_ended_and_suffix_ranges() {
        assert_eq!(
            resolve_concrete_range(
                Some(RequestedRange {
                    start: Some(100),
                    end: None
                }),
                1000
            ),
            (100, 999, true)
        );
        assert_eq!(
            resolve_concrete_range(
                Some(RequestedRange {
                    start: None,
                    end: Some(50)
                }),
                1000
            ),
            (950, 999, true)
        );
        assert_eq!(
            resolve_concrete_range(
                Some(RequestedRange {
                    start: Some(10),
                    end: Some(5000)
                }),
                1000
            ),
            (10, 999, true)
        );
    }

    #[test]
    fn rejects_range_requests_when_upstream_returns_full_content() {
        assert_eq!(
            validate_upstream_range_response(
                Some(RequestedRange {
                    start: Some(0),
                    end: Some(1023),
                }),
                reqwest::StatusCode::OK
            ),
            Err(UpstreamRangeError::MissingPartialContent)
        );
    }
}
