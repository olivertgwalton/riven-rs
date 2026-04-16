mod auth;
mod board;
mod graphql;
mod media;
mod usenet;
mod webhooks;

use std::sync::Arc;

use anyhow::Result;
use axum::{Router, routing::get, routing::post};
use plugin_logs::LogControl;
use riven_core::plugin::PluginRegistry;
use riven_core::stream_link::LinkRequest;
use riven_queue::JobQueue;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

use apalis_board_api::framework::{ApiBuilder, RegisterRoute};
use apalis_board_api::ui::ServeUI;

use crate::schema::{build_schema, start_event_controller};

pub use state::ApiState;

mod state {
    use std::sync::Arc;

    use riven_core::stream_link::LinkRequest;
    use riven_queue::JobQueue;
    use tokio::sync::broadcast;

    use crate::schema::AppSchema;

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
}

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
        log_tx.clone(),
    );

    start_event_controller(job_queue.clone());

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
        .route(
            "/graphql",
            get(graphql::graphql_get_handler).post(graphql::graphql_handler),
        )
        .route(
            "/media/{entry_id}",
            get(media::media_bridge_handler).head(media::media_bridge_handler),
        )
        .route(
            "/stream/usenet/{hash}/{filename}",
            get(usenet::usenet_stream_handler).head(usenet::usenet_stream_handler),
        )
        .route("/webhook/seerr", post(webhooks::seerr_webhook))
        .nest("/board", board_ui.with_state(()))
        .fallback_service(serve_frontend)
        .layer(axum::middleware::from_fn(board::board_assets_middleware))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(port = port, "GraphQL server listening");

    axum::serve(listener, app).await?;

    Ok(())
}
