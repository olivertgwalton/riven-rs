//! `/usenet/{info_hash}/{file_index}` HTTP route.
//!
//! Streams bytes from a previously-ingested NZB. Supports byte-range requests
//! so video players can seek, though the seek is *approximate* — see the
//! comment in `riven_usenet::streamer`. For HEAD requests we just answer
//! with the metadata size; no NNTP traffic is generated.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    },
    response::{IntoResponse, Response},
};

use super::ApiState;

/// Parse a single-range `Range: bytes=START-END` header. Returns `None` for
/// no header / unparseable header, `Some(Err(()))` for an unsatisfiable range.
fn parse_range(header: Option<&HeaderValue>, total: u64) -> Option<Result<(u64, u64), ()>> {
    let raw = header?.to_str().ok()?;
    let spec = raw.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (start_s, end_s) = spec.split_once('-')?;
    let (start, end) = match (start_s.is_empty(), end_s.is_empty()) {
        (false, false) => {
            let s: u64 = start_s.parse().ok()?;
            let e: u64 = end_s.parse().ok()?;
            (s, e.min(total - 1))
        }
        (false, true) => {
            let s: u64 = start_s.parse().ok()?;
            (s, total - 1)
        }
        (true, false) => {
            // Suffix range: last N bytes.
            let n: u64 = end_s.parse().ok()?;
            if n == 0 || n > total {
                return Some(Err(()));
            }
            (total - n, total - 1)
        }
        (true, true) => return None,
    };
    if start > end || start >= total {
        return Some(Err(()));
    }
    Some(Ok((start, end)))
}

fn guess_content_type(filename: &str) -> &'static str {
    let lower = filename.to_ascii_lowercase();
    if lower.ends_with(".mkv") {
        "video/x-matroska"
    } else if lower.ends_with(".mp4") || lower.ends_with(".m4v") {
        "video/mp4"
    } else if lower.ends_with(".webm") {
        "video/webm"
    } else if lower.ends_with(".avi") {
        "video/x-msvideo"
    } else if lower.ends_with(".mov") {
        "video/quicktime"
    } else {
        "application/octet-stream"
    }
}

pub async fn usenet_stream_handler(
    State(state): State<ApiState>,
    Path((info_hash, file_index)): Path<(String, usize)>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Some(streamer) = state.usenet_streamer.as_ref() else {
        return (StatusCode::NOT_FOUND, "usenet streaming not enabled").into_response();
    };

    let meta = match streamer.load_meta(&info_hash).await {
        Ok(m) => m,
        Err(e) => {
            tracing::debug!(info_hash, error = %e, "usenet meta lookup failed");
            return (StatusCode::NOT_FOUND, "unknown info_hash").into_response();
        }
    };
    let Some(file) = meta.files.get(file_index) else {
        return (StatusCode::NOT_FOUND, "file index out of range").into_response();
    };
    let total = file.total_size;
    let content_type = guess_content_type(&file.filename);

    let range = parse_range(headers.get(RANGE), total);
    let (start, end, partial) = match range {
        Some(Ok((s, e))) => (s, e, true),
        Some(Err(())) => {
            let mut resp = (StatusCode::RANGE_NOT_SATISFIABLE, "").into_response();
            if let Ok(v) = HeaderValue::from_str(&format!("bytes */{}", total)) {
                resp.headers_mut().insert(CONTENT_RANGE, v);
            }
            return resp;
        }
        None => (0, total - 1, false),
    };

    let mut header_map = HeaderMap::new();
    header_map.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    header_map.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    let len = end - start + 1;
    if let Ok(v) = HeaderValue::from_str(&len.to_string()) {
        header_map.insert(CONTENT_LENGTH, v);
    }
    if partial
        && let Ok(v) = HeaderValue::from_str(&format!("bytes {start}-{end}/{total}"))
    {
        header_map.insert(CONTENT_RANGE, v);
    }

    if method == Method::HEAD {
        let status = if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        let mut resp = (status, "").into_response();
        *resp.headers_mut() = header_map;
        return resp;
    }

    // Synchronously fetch the requested range. For very large ranges the
    // player will issue a fresh request after consuming each chunk, so we
    // intentionally don't try to chunk a single response across multiple
    // segments — keeps the code simple and the memory bounded by what the
    // client asked for.
    match streamer.read_range(&info_hash, file_index, start, end).await {
        Ok(bytes) => {
            let status = if partial {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            };
            let mut resp = Response::new(Body::from(bytes));
            *resp.status_mut() = status;
            *resp.headers_mut() = header_map;
            resp
        }
        Err(e) => {
            tracing::warn!(info_hash, file_index, error = %e, "usenet read_range failed");
            (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response()
        }
    }
}
