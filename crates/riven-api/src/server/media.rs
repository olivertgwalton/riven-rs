use std::time::Instant;

use anyhow::Result;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
        header::{
            ACCEPT_RANGES, CACHE_CONTROL, CONNECTION, CONTENT_DISPOSITION, CONTENT_LENGTH,
            CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE,
        },
    },
    response::{IntoResponse, Response},
};
use riven_core::stream_link::request_stream_url;

use super::ApiState;
use super::auth::check_api_key;

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
        &state.link_request_tx,
    )
    .await?;
    if let Err(error) = riven_db::repo::update_stream_url(&state.db_pool, entry.id, &url).await {
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
        match riven_db::repo::get_next_playback_entry(&state.db_pool, entry_id).await {
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

async fn load_media_entry(
    state: &ApiState,
    entry_id: i64,
) -> Result<Option<riven_db::entities::FileSystemEntry>> {
    riven_db::repo::get_media_entry_by_id(&state.db_pool, entry_id).await
}

pub(super) async fn media_bridge_handler(
    State(state): State<ApiState>,
    Path(entry_id): Path<i64>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let request_started = Instant::now();

    if !check_api_key(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let entry = match load_media_entry(&state, entry_id).await {
        Ok(Some(entry)) => entry,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => {
            tracing::error!(entry_id, error = %error, "failed to load media entry");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if entry.download_url.is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }

    let requested_range = match parse_requested_range(headers.get(RANGE), u64::try_from(entry.file_size).unwrap_or(0)) {
        Ok(range) => range,
        Err(error) => return range_error_response(error, u64::try_from(entry.file_size).unwrap_or(0)),
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
    }

    tracing::info!(
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
