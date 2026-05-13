//! `/usenet/{info_hash}/{file_index}` HTTP route.
//!
//! Streams bytes from a previously-ingested NZB. Supports byte-range requests
//! so video players can seek, though the seek is *approximate* — see the
//! comment in `riven_usenet::streamer`. For HEAD requests we just answer
//! with the metadata size; no NNTP traffic is generated.

use std::net::SocketAddr;

use axum::{
    body::Body,
    extract::{ConnectInfo, Path, State},
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    },
    response::{IntoResponse, Response},
};

use super::ApiState;
use super::auth::check_api_key;

/// Auth gate for `/usenet/...` requests. Passes when either:
///   - the request carries a valid `x-api-key` / `Authorization` header
///     (the standard `check_api_key` path), OR
///   - the request originates from loopback (127.0.0.1 / ::1).
///
/// The loopback exemption exists because `riven-vfs` runs in-process and
/// fetches stream URLs over HTTP without attaching auth headers. Loopback
/// is unreachable from outside the host, so this doesn't weaken external
/// auth — `RIVEN_SETTING__API_KEY` still gates everything from outside.
fn check_usenet_auth(state: &ApiState, headers: &HeaderMap, peer: SocketAddr) -> bool {
    if check_api_key(state, headers) {
        return true;
    }
    peer.ip().is_loopback()
}

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
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    if !check_usenet_auth(&state, &headers, peer) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

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

    // Cap any single response at the same lookahead window the VFS uses
    // for its sequential reader (32 MB). Mirrors debrid CDN behaviour:
    // an open-ended Range request gets a bounded 206 Partial Content,
    // and the client re-requests for the next window when needed. If we
    // honoured `bytes=0-` literally for a 30 GB MKV the body stream
    // would try to deliver the entire file in one HTTP response, which:
    //   1. saturates the segment cache eagerly (LRU thrash),
    //   2. queues tens of thousands of NNTP fetches up front,
    //   3. trips reqwest's total-request timeout long before bytes flow.
    const MAX_RESPONSE_WINDOW: u64 = 32 * 1024 * 1024;
    let range = parse_range(headers.get(RANGE), total);
    let (start, end, partial) = match range {
        Some(Ok((s, e))) => {
            let capped_end = e.min(s.saturating_add(MAX_RESPONSE_WINDOW - 1));
            (s, capped_end, true)
        }
        Some(Err(())) => {
            let mut resp = (StatusCode::RANGE_NOT_SATISFIABLE, "").into_response();
            if let Ok(v) = HeaderValue::from_str(&format!("bytes */{}", total)) {
                resp.headers_mut().insert(CONTENT_RANGE, v);
            }
            return resp;
        }
        None => {
            let end = MAX_RESPONSE_WINDOW.saturating_sub(1).min(total - 1);
            (0, end, end + 1 < total)
        }
    };

    let mut header_map = HeaderMap::new();
    header_map.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    header_map.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    let len = end - start + 1;
    if let Ok(v) = HeaderValue::from_str(&len.to_string()) {
        header_map.insert(CONTENT_LENGTH, v);
    }
    if partial && let Ok(v) = HeaderValue::from_str(&format!("bytes {start}-{end}/{total}")) {
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

    const BUFFER_THRESHOLD: u64 = 1024 * 1024;
    const STREAM_CHUNK_BYTES: u64 = 256 * 1024;
    let status = if partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    if len <= BUFFER_THRESHOLD {
        return match streamer
            .read_range(&info_hash, file_index, start, end)
            .await
        {
            Ok(bytes) => {
                let mut resp = Response::new(Body::from(bytes));
                *resp.status_mut() = status;
                *resp.headers_mut() = header_map;
                resp
            }
            Err(e) => {
                tracing::warn!(info_hash, file_index, error = %e, "usenet read_range failed");
                (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response()
            }
        };
    }

    // Eager prefetch + cancellation. The prefetch task warms the segment
    // cache for the requested range; the body stream's own reads hit the
    // cache instead of NNTP. Because `fetch_decoded_cached` now
    // deduplicates concurrent fetches via an in-flight promise map,
    // the body and the prefetch coordinate transparently — if both want
    // segment N at the same time, only one NNTP round-trip happens.
    // The prefetch is also internally bounded to 4 concurrent fetches
    // so it doesn't starve the NNTP connection pool.
    //
    // The cancellation token aborts the prefetch task when the body
    // stream is dropped (client disconnect, completion) — no orphaned
    // bandwidth.
    let cancel = tokio_util::sync::CancellationToken::new();
    {
        let streamer = streamer.clone();
        let info_hash = info_hash.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = streamer.prefetch_range(&info_hash, file_index, start, end) => {}
                _ = cancel.cancelled() => {
                    tracing::debug!(info_hash, "usenet prefetch cancelled");
                }
            }
        });
    }

    let client_ua = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("Unknown")
        .to_string();
    let stream_key = format!("{info_hash}:{file_index}");
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    riven_usenet::active_streams().register(
        stream_key.clone(),
        riven_usenet::state::ActiveStream {
            info_hash: info_hash.clone(),
            filename: file.filename.clone(),
            file_size: total,
            started_at: now_secs,
            last_active: now_secs,
            client: client_ua,
        },
    );

    // Guard that lives inside the unfold's state. When the body stream
    // is dropped (client disconnect, body completion, axum tearing
    // things down), the guard's Drop runs: cancels the prefetch token
    // and unregisters the active-streams entry. Without this both would
    // leak past the stream's lifetime.
    struct StreamGuard {
        cancel: tokio_util::sync::CancellationToken,
        key: String,
    }
    impl Drop for StreamGuard {
        fn drop(&mut self) {
            self.cancel.cancel();
            riven_usenet::active_streams().unregister(&self.key);
        }
    }

    struct UnfoldState {
        pos: u64,
        _guard: StreamGuard,
    }

    let initial = UnfoldState {
        pos: start,
        _guard: StreamGuard {
            cancel: cancel.clone(),
            key: stream_key.clone(),
        },
    };

    let info_hash_owned = info_hash.clone();
    let streamer = streamer.clone();
    tracing::info!(
        info_hash,
        file_index,
        start,
        end,
        len,
        "usenet body stream starting"
    );
    let stream = futures::stream::unfold(initial, move |mut state| {
        let info_hash = info_hash_owned.clone();
        let streamer = streamer.clone();
        let key = state._guard.key.clone();
        async move {
            if state.pos > end {
                return None;
            }
            let chunk_end = state.pos.saturating_add(STREAM_CHUNK_BYTES - 1).min(end);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            riven_usenet::active_streams().touch(&key, now);

            let chunk_started = std::time::Instant::now();
            tracing::debug!(pos = state.pos, chunk_end, "usenet body chunk starting");
            match streamer
                .read_range(&info_hash, file_index, state.pos, chunk_end)
                .await
            {
                Ok(bytes) => {
                    let elapsed_ms = chunk_started.elapsed().as_millis();
                    let returned = bytes.len();
                    let requested = (chunk_end - state.pos + 1) as usize;
                    if returned != requested {
                        tracing::warn!(
                            pos = state.pos,
                            chunk_end,
                            returned,
                            requested,
                            elapsed_ms,
                            "usenet body chunk SHORT — emitting fewer bytes than requested"
                        );
                    } else {
                        tracing::debug!(
                            pos = state.pos,
                            returned,
                            elapsed_ms,
                            "usenet body chunk ok"
                        );
                    }
                    state.pos = chunk_end.saturating_add(1);
                    let item: Result<axum::body::Bytes, std::io::Error> =
                        Ok(axum::body::Bytes::from(bytes));
                    Some((item, state))
                }
                Err(e) => {
                    tracing::warn!(
                        info_hash, file_index, pos = state.pos, error = %e,
                        "usenet stream chunk failed"
                    );
                    let item: Result<axum::body::Bytes, std::io::Error> =
                        Err(std::io::Error::other(format!("upstream: {e}")));
                    state.pos = end.saturating_add(1);
                    Some((item, state))
                }
            }
        }
    });
    let body = Body::from_stream(stream);
    let mut resp = Response::new(body);
    *resp.status_mut() = status;
    *resp.headers_mut() = header_map;
    resp
}
