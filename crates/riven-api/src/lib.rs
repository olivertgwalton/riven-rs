pub mod schema;

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use async_graphql::http::{
    GraphiQLSource, create_multipart_mixed_stream, is_accept_multipart_mixed,
};
use async_graphql_axum::{
    GraphQLBatchRequest, GraphQLRequest, GraphQLResponse, rejection::GraphQLRejection,
};
use axum::{
    Router,
    body::Body,
    extract::{FromRequest, Path, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode,
        header::{
            ACCEPT_RANGES, CACHE_CONTROL, CONNECTION, CONTENT_DISPOSITION, CONTENT_LENGTH,
            CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE,
        },
    },
    response::{
        Html, IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures::StreamExt;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

use apalis_board_api::framework::{ApiBuilder, RegisterRoute};
use apalis_board_api::ui::ServeUI;

// ── Board asset middleware ──
// The board WASM SPA requests its hashed assets at root-level absolute paths
// (e.g. /apalis-board-web-<hash>.js). This middleware intercepts any such
// request before it reaches the frontend fallback, without hardcoding filenames.

async fn board_assets_middleware(
    uri: axum::http::Uri,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = uri.path();
    if path.contains('.')
        && let Some(file) = ServeUI::get_file(path)
    {
        let bytes = file.contents().to_vec();
        let content_type = ServeUI::content_type(path);
        let mut builder = axum::http::Response::builder()
            .status(200)
            .header("content-type", content_type);
        if let Some(cc) = ServeUI::cache_control(path) {
            builder = builder.header("cache-control", cc);
        }
        return builder
            .body(axum::body::Body::from(bytes))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }
    next.run(req).await
}
use plugin_logs::LogControl;
use riven_core::plugin::PluginRegistry;
use riven_core::stream_link::{LinkRequest, request_stream_url};
use riven_queue::JobQueue;

use crate::schema::{AppSchema, build_schema};

#[derive(Clone)]
pub struct ApiState {
    pub schema: AppSchema,
    pub db_pool: sqlx::PgPool,
    pub job_queue: Arc<JobQueue>,
    pub api_key: Option<String>,
    pub log_tx: broadcast::Sender<String>,
    pub notification_tx: broadcast::Sender<String>,
    pub stream_client: reqwest::Client,
    pub link_request_tx: tokio::sync::mpsc::Sender<LinkRequest>,
    pub runtime: tokio::runtime::Handle,
}

// ── Auth helper ──

fn check_api_key(state: &ApiState, headers: &HeaderMap) -> bool {
    let Some(ref expected) = state.api_key else {
        return true;
    };
    if expected.is_empty() {
        return true;
    }
    let provided = headers
        .get("x-api-key")
        .or_else(|| headers.get("authorization"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_start_matches("Bearer ").trim());
    provided == Some(expected.as_str())
}

// ── GraphQL ──

async fn graphql_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    req: Request<Body>,
) -> Response {
    if !check_api_key(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let accepts_multipart = headers
        .get("accept")
        .and_then(|value| value.to_str().ok())
        .map(is_accept_multipart_mixed)
        .unwrap_or_default();

    if accepts_multipart {
        let req = match GraphQLRequest::<GraphQLRejection>::from_request(req, &()).await {
            Ok(req) => req,
            Err(error) => return error.into_response(),
        };
        let stream = state.schema.execute_stream(req.into_inner());
        let body = Body::from_stream(
            create_multipart_mixed_stream(stream, std::time::Duration::from_secs(30))
                .map(Ok::<_, std::io::Error>),
        );

        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "multipart/mixed; boundary=graphql")
            .body(body)
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    let req = match GraphQLBatchRequest::<GraphQLRejection>::from_request(req, &()).await {
        Ok(req) => req,
        Err(error) => return error.into_response(),
    };

    let gql_resp: GraphQLResponse = state.schema.execute_batch(req.into_inner()).await.into();
    gql_resp.into_response()
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

// ── Webhooks ──

async fn seerr_webhook(State(state): State<ApiState>) -> impl IntoResponse {
    tracing::info!("seerr webhook received, triggering content service");
    state.job_queue.push_content_service().await;
    StatusCode::OK
}

/// Convert a broadcast receiver into an SSE stream, labelling each event with `event_name`.
/// Silently skips lagged messages and terminates when the channel closes.
fn broadcast_to_sse(
    rx: broadcast::Receiver<String>,
    event_name: &'static str,
) -> impl futures::Stream<Item = Result<Event, Infallible>> {
    futures::stream::unfold(rx, move |mut rx| async move {
        let data = loop {
            match rx.recv().await {
                Ok(line) => break line,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        };
        Some((Ok(Event::default().event(event_name).data(data)), rx))
    })
}

// ── SSE: live logs ──

async fn logs_stream_handler(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if !check_api_key(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let ping = futures::stream::once(async {
        Ok::<Event, Infallible>(Event::default().event("connected").data("ok"))
    });
    let rest = broadcast_to_sse(state.log_tx.subscribe(), "log");
    Sse::new(ping.chain(rest))
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(3)))
        .into_response()
}

// ── SSE: real-time notifications ──

async fn notifications_stream_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Response {
    if !check_api_key(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let ping = futures::stream::once(async {
        Ok::<Event, Infallible>(Event::default().event("connected").data("ok"))
    });
    let rest = broadcast_to_sse(state.notification_tx.subscribe(), "notification");

    Sse::new(ping.chain(rest))
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(3)))
        .into_response()
}

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
        .map_err(|_| RangeHeaderError::Invalid)?
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
            .then(|| start.parse::<u64>().map_err(|_| RangeHeaderError::Invalid))
            .transpose()?,
        end: (!end.is_empty())
            .then(|| end.parse::<u64>().map_err(|_| RangeHeaderError::Invalid))
            .transpose()?,
    };

    match (requested.start, requested.end) {
        (Some(start), Some(end)) if start > end => Err(RangeHeaderError::Unsatisfiable),
        (Some(start), _) if start >= file_size => Err(RangeHeaderError::Unsatisfiable),
        (None, Some(0)) => Err(RangeHeaderError::Unsatisfiable),
        (None, Some(_)) => Ok(Some(requested)),
        (Some(_), _) => Ok(Some(requested)),
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
        &state.runtime,
    )?;
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

async fn media_bridge_handler(
    State(state): State<ApiState>,
    Path(entry_id): Path<i64>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let request_started = Instant::now();

    if !check_api_key(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let entry = match riven_db::repo::get_media_entry_by_id(&state.db_pool, entry_id).await {
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

    let requested_range = match parse_requested_range(headers.get(RANGE), entry.file_size as u64) {
        Ok(range) => range,
        Err(error) => return range_error_response(error, entry.file_size as u64),
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

// ── Server bootstrap ──

pub async fn start_server(
    port: u16,
    db_pool: sqlx::PgPool,
    registry: Arc<PluginRegistry>,
    job_queue: Arc<JobQueue>,
    api_key: Option<String>,
    log_directory: String,
    log_tx: broadcast::Sender<String>,
    notification_tx: broadcast::Sender<String>,
    downloader_config: Arc<tokio::sync::RwLock<riven_core::downloader::DownloaderConfig>>,
    log_control: Arc<LogControl>,
    stream_client: reqwest::Client,
    link_request_tx: tokio::sync::mpsc::Sender<LinkRequest>,
) -> Result<()> {
    let schema = build_schema(
        db_pool.clone(),
        registry,
        job_queue.clone(),
        log_directory,
        downloader_config,
        log_control,
    );

    let board_api = ApiBuilder::new(Router::new())
        .register(job_queue.index_storage.clone())
        .register(job_queue.index_plugin_storage.clone())
        .register(job_queue.scrape_storage.clone())
        .register(job_queue.scrape_plugin_storage.clone())
        .register(job_queue.parse_storage.clone())
        .register(job_queue.download_storage.clone())
        .register(job_queue.content_storage.clone())
        .build();
    let board_ui = Router::new().fallback_service(ServeUI::new());

    let static_dir =
        std::env::var("RIVEN_STATIC_DIR").unwrap_or_else(|_| "./frontend/dist".to_string());
    let serve_frontend =
        ServeDir::new(&static_dir).fallback(ServeFile::new(format!("{static_dir}/index.html")));

    let state = ApiState {
        schema,
        db_pool,
        job_queue,
        api_key,
        log_tx,
        notification_tx,
        stream_client,
        link_request_tx,
        runtime: tokio::runtime::Handle::current(),
    };

    let app = Router::new()
        .nest("/api/v1", board_api.with_state(()))
        .route("/graphql", get(graphiql).post(graphql_handler))
        .route(
            "/media/{entry_id}",
            get(media_bridge_handler).head(media_bridge_handler),
        )
        .route("/webhook/seerr", post(seerr_webhook))
        .route("/logs/stream", get(logs_stream_handler))
        .route("/notifications/stream", get(notifications_stream_handler))
        .nest("/board", board_ui.with_state(()))
        .fallback_service(serve_frontend)
        .layer(axum::middleware::from_fn(board_assets_middleware))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(port = port, "GraphQL server listening");

    axum::serve(listener, app).await?;

    Ok(())
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
