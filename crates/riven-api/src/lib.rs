pub mod schema;

use std::convert::Infallible;
use std::sync::Arc;

use anyhow::Result;
use async_graphql::http::GraphiQLSource;
use futures::StreamExt;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response, sse::{Event, KeepAlive, Sse}},
    routing::{get, post},
    Router,
};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

use apalis_board_api::framework::{ApiBuilder, RegisterRoute};
use apalis_board_api::ui::ServeUI;

// ── Board asset handler ──

async fn serve_board_asset(uri: axum::http::Uri) -> Response {
    let path = uri.path().to_owned();
    match ServeUI::get_file(&path) {
        Some(file) => {
            let bytes: Vec<u8> = file.contents().to_vec();
            let content_type = ServeUI::content_type(&path);
            let mut builder = axum::http::Response::builder()
                .status(200)
                .header("content-type", content_type);
            if let Some(cc) = ServeUI::cache_control(&path) {
                builder = builder.header("cache-control", cc);
            }
            builder
                .body(axum::body::Body::from(bytes))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
use riven_core::plugin::PluginRegistry;
use riven_queue::JobQueue;

use crate::schema::{build_schema, AppSchema};

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
    req: GraphQLRequest,
) -> Response {
    if !check_api_key(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let gql_resp: GraphQLResponse = state.schema.execute(req.into_inner()).await.into();
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

// ── SSE: live logs ──

async fn logs_stream_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Response {
    if !check_api_key(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let rx = state.log_tx.subscribe();
    let ping = futures::stream::once(async {
        Ok::<Event, Infallible>(Event::default().event("connected").data("ok"))
    });
    let rest = futures::stream::unfold(rx, |mut rx| async move {
        let data = loop {
            match rx.recv().await {
                Ok(line) => break line,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        };
        Some((Ok::<Event, Infallible>(Event::default().event("log").data(data)), rx))
    });

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

    let rx = state.notification_tx.subscribe();
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        let data = loop {
            match rx.recv().await {
                Ok(line) => break line,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        };
        Some((Ok::<Event, Infallible>(Event::default().event("notification").data(data)), rx))
    });

    Sse::new(stream)
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
) -> Result<()> {
    let schema = build_schema(
        db_pool,
        registry,
        job_queue.clone(),
        log_directory,
        downloader_config,
    );

    let board_api = ApiBuilder::new(Router::new())
        .register(job_queue.index_storage.clone())
        .register(job_queue.scrape_storage.clone())
        .register(job_queue.download_storage.clone())
        .register(job_queue.content_storage.clone())
        .build();
    // The board SPA (WASM) is served under /board but makes API calls to
    // absolute paths /api/v1/... — so board_api must live at root /api/v1.
    let board_ui = Router::new().fallback_service(ServeUI::new());

    let static_dir = std::env::var("RIVEN_STATIC_DIR").unwrap_or_else(|_| "./frontend/dist".to_string());
    let serve_frontend = ServeDir::new(&static_dir)
        .fallback(ServeFile::new(format!("{static_dir}/index.html")));

    let state = ApiState {
        schema,
        job_queue,
        api_key,
        log_tx,
        notification_tx,
    };

    // The board SPA references assets at root-level absolute paths.
    // Register them explicitly so they're reachable when the board is nested at /board.
    let app = Router::new()
        .route("/apalis-board-web-4f06dae1128a9b0a.js", get(serve_board_asset))
        .route("/apalis-board-web-4f06dae1128a9b0a_bg.wasm", get(serve_board_asset))
        .route("/input-341faa0de831bcfb.css", get(serve_board_asset))
        .nest("/api/v1", board_api.with_state(()))
        .route("/graphql", get(graphiql).post(graphql_handler))
        .route("/webhook/seerr", post(seerr_webhook))
        .route("/logs/stream", get(logs_stream_handler))
        .route("/notifications/stream", get(notifications_stream_handler))
        .nest("/board", board_ui.with_state(()))
        .fallback_service(serve_frontend)
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(port = port, "GraphQL server listening");

    axum::serve(listener, app).await?;

    Ok(())
}
