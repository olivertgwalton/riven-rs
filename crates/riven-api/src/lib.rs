pub mod schema;

use std::convert::Infallible;
use std::sync::Arc;

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
    extract::FromRequest,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
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
use riven_queue::JobQueue;

use crate::schema::{AppSchema, build_schema};

#[derive(Clone)]
pub struct ApiState {
    pub schema: AppSchema,
    pub job_queue: Arc<JobQueue>,
    pub api_key: Option<String>,
    pub log_tx: broadcast::Sender<String>,
    pub notification_tx: broadcast::Sender<String>,
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

    Sse::new(broadcast_to_sse(
        state.notification_tx.subscribe(),
        "notification",
    ))
    .keep_alive(KeepAlive::default())
    .into_response()
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
) -> Result<()> {
    let schema = build_schema(
        db_pool,
        registry,
        job_queue.clone(),
        log_directory,
        downloader_config,
        log_control,
    );

    let board_api = ApiBuilder::new(Router::new())
        .register(job_queue.index_storage.clone())
        .register(job_queue.scrape_storage.clone())
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
        job_queue,
        api_key,
        log_tx,
        notification_tx,
    };

    let app = Router::new()
        .nest("/api/v1", board_api.with_state(()))
        .route("/graphql", get(graphiql).post(graphql_handler))
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
