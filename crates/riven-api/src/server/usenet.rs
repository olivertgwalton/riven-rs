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

    // Fire-and-forget head+tail precache the first time this file is
    // touched in the process. Players (Plex/Jellyfin/ffprobe) almost
    // always read a few MB at the start and seek near the end for the
    // MKV cue index before sequential playback — precaching both
    // windows means those probes hit the segment cache instead of
    // spending NNTP round-trips. `PrecachedFiles::claim` makes this a
    // no-op after the first call per (info_hash, file_index).
    {
        let streamer = streamer.clone();
        let info_hash = info_hash.clone();
        tokio::spawn(async move {
            streamer.precache_head_tail(&info_hash, file_index).await;
        });
    }

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

    // Headers shared by HEAD, buffered, and streamed responses.
    //
    // We compute `Content-Length` from the size of the body we'll actually
    // emit, not from the requested range, because for `Direct` sources the
    // requested range is in *encoded* byte space and the served bytes are
    // *decoded* — even after `rescale_direct_to_decoded` at ingest, per-
    // segment yEnc variance produces sub-percent drift, and the legacy
    // pre-rescale meta entries are still ~3% off. Setting Content-Length to
    // the requested range and then emitting fewer bytes caused hyper to
    // close the connection with a framing error, which Plex/ffmpeg saw as
    // a corrupt response and retried indefinitely.
    //
    // For HEAD and small (≤BUFFER_THRESHOLD) responses we know the exact
    // body length up front and set Content-Length accordingly. For large
    // streamed responses we omit Content-Length entirely — hyper falls
    // back to chunked transfer-encoding, which has no length contract for
    // the body to violate.
    let mut header_map = HeaderMap::new();
    header_map.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    header_map.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    let len = end - start + 1;
    if partial && let Ok(v) = HeaderValue::from_str(&format!("bytes {start}-{end}/{total}")) {
        header_map.insert(CONTENT_RANGE, v);
    }

    if method == Method::HEAD {
        let status = if partial {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        if let Ok(v) = HeaderValue::from_str(&len.to_string()) {
            header_map.insert(CONTENT_LENGTH, v);
        }
        let mut resp = (status, "").into_response();
        *resp.headers_mut() = header_map;
        return resp;
    }

    const BUFFER_THRESHOLD: u64 = 1024 * 1024;
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
                let mut response_headers = header_map;
                if let Ok(v) = HeaderValue::from_str(&bytes.len().to_string()) {
                    response_headers.insert(CONTENT_LENGTH, v);
                }
                let mut resp = Response::new(Body::from(bytes));
                *resp.status_mut() = status;
                *resp.headers_mut() = response_headers;
                resp
            }
            Err(e) => {
                tracing::warn!(info_hash, file_index, error = %e, "usenet read_range failed");
                (StatusCode::BAD_GATEWAY, format!("upstream error: {e}")).into_response()
            }
        };
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

    tracing::info!(
        info_hash,
        file_index,
        start,
        end,
        len,
        "usenet body stream starting"
    );

    // Bounded, self-pipelining body stream. `byte_stream` keeps a small
    // number of segment fetches in flight ahead of the emit cursor and
    // yields decoded slices in order — no separate race-to-EOF prefetch
    // task (which used to evict the player's own read position from the
    // cache and cause the video-freezes-while-audio-continues stutter).
    // `buffered` backpressure bounds the read-ahead to the lookahead
    // window, so it can't run away from playback.
    //
    // `BodyStream` wraps it to (a) map decode errors to `io::Error` for
    // hyper, (b) unregister the active-streams entry when the body is
    // dropped (client disconnect / completion), and (c) refresh the
    // active-streams heartbeat every Nth frame without a per-frame clock
    // read.
    let inner = streamer.byte_stream(meta.clone(), file_index, start, end);

    struct BodyStream {
        inner: futures::stream::BoxStream<'static, Result<axum::body::Bytes, riven_usenet::StreamerError>>,
        key: String,
        frames: u32,
    }
    impl Drop for BodyStream {
        fn drop(&mut self) {
            riven_usenet::active_streams().unregister(&self.key);
        }
    }
    impl futures::Stream for BodyStream {
        type Item = Result<axum::body::Bytes, std::io::Error>;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            const TOUCH_EVERY_N_FRAMES: u32 = 16;
            let this = self.get_mut();
            match this.inner.as_mut().poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(bytes))) => {
                    this.frames = this.frames.wrapping_add(1);
                    if this.frames.is_multiple_of(TOUCH_EVERY_N_FRAMES) {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        riven_usenet::active_streams().touch(&this.key, now);
                    }
                    std::task::Poll::Ready(Some(Ok(bytes)))
                }
                std::task::Poll::Ready(Some(Err(e))) => std::task::Poll::Ready(Some(Err(
                    std::io::Error::other(format!("upstream: {e}")),
                ))),
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        }
    }

    let body = Body::from_stream(BodyStream {
        inner,
        key: stream_key,
        frames: 0,
    });
    let mut resp = Response::new(body);
    *resp.status_mut() = status;
    *resp.headers_mut() = header_map;
    resp
}
