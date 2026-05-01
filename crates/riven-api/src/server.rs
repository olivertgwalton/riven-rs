mod auth;
mod board;
mod graphql;
mod media;
mod usenet;
mod webhooks;

use std::sync::Arc;

use anyhow::Result;
use axum::{Router, routing::get, routing::post};
use riven_core::logging::LogControl;
use riven_core::http::HttpClient;
use riven_core::plugin::PluginRegistry;
use riven_core::stream_link::LinkRequest;
use riven_queue::JobQueue;
use tokio::sync::broadcast;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};

use apalis_board_api::framework::{ApiBuilder, RegisterRoute};
use apalis_board_api::ui::ServeUI;

use crate::schema::{build_schema, start_event_controller};
use crate::vfs_mount::VfsMountManager;

pub use state::ApiState;

pub struct StartServerConfig {
    pub port: u16,
    pub db_pool: sqlx::PgPool,
    pub registry: Arc<PluginRegistry>,
    pub job_queue: Arc<JobQueue>,
    pub http_client: HttpClient,
    pub api_key: Option<String>,
    pub frontend_auth_signing_secret: Option<String>,
    pub log_directory: String,
    pub log_tx: broadcast::Sender<String>,
    pub notification_tx: broadcast::Sender<String>,
    pub downloader_config: Arc<tokio::sync::RwLock<riven_core::downloader::DownloaderConfig>>,
    pub log_control: Arc<LogControl>,
    pub stream_client: reqwest::Client,
    pub link_request_tx: tokio::sync::mpsc::Sender<LinkRequest>,
    pub cors_allowed_origins: Vec<String>,
    pub vfs_mount_manager: Arc<VfsMountManager>,
    pub usenet_streamer: Option<riven_usenet::UsenetStreamer>,
    pub cancel: tokio_util::sync::CancellationToken,
}

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
        pub frontend_auth_signing_secret: Option<String>,
        pub log_tx: broadcast::Sender<String>,
        pub notification_tx: broadcast::Sender<String>,
        pub stream_client: reqwest::Client,
        pub link_request_tx: tokio::sync::mpsc::Sender<LinkRequest>,
        pub runtime: tokio::runtime::Handle,
        pub usenet_streamer: Option<riven_usenet::UsenetStreamer>,
    }
}

pub async fn start_server(config: StartServerConfig) -> Result<()> {
    let StartServerConfig {
        port,
        db_pool,
        registry,
        job_queue,
        http_client,
        api_key,
        frontend_auth_signing_secret,
        log_directory,
        log_tx,
        notification_tx,
        downloader_config,
        log_control,
        stream_client,
        link_request_tx,
        cors_allowed_origins,
        vfs_mount_manager,
        usenet_streamer,
        cancel,
    } = config;

    // Refuse to start with the unsafe combination of no API key and no CORS
    // allowlist: a malicious cross-origin page in a victim's browser could
    // otherwise drive every endpoint as the trusted_api_key admin.
    let api_key_empty = api_key.as_deref().is_none_or(str::is_empty);
    if api_key_empty && cors_allowed_origins.is_empty() {
        anyhow::bail!(
            "refusing to start: both RIVEN_SETTING__API_KEY and \
             RIVEN_SETTING__CORS_ALLOWED_ORIGINS are unset. Set at least one \
             (an API key gates auth; an origins list constrains CORS)."
        );
    }

    let schema = build_schema(
        db_pool.clone(),
        registry,
        job_queue.clone(),
        http_client,
        log_directory,
        downloader_config,
        log_control,
        log_tx.clone(),
        vfs_mount_manager,
    );

    start_event_controller(job_queue.clone());

    let mut board_builder = ApiBuilder::new(Router::new())
        .register(job_queue.index_storage.clone())
        .register(job_queue.scrape_storage.clone())
        .register(job_queue.parse_storage.clone())
        .register(job_queue.download_storage.clone())
        .register(job_queue.rank_streams_storage.clone())
        .register(job_queue.process_media_item_storage.clone());
    for storage in job_queue.plugin_hook_storages.values() {
        board_builder = board_builder.register(storage.clone());
    }
    let board_api = board_builder.build();
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
        frontend_auth_signing_secret,
        log_tx,
        notification_tx,
        stream_client,
        link_request_tx,
        runtime: tokio::runtime::Handle::current(),
        usenet_streamer,
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
            "/usenet/{info_hash}/{file_index}",
            get(usenet::usenet_stream_handler).head(usenet::usenet_stream_handler),
        )
        .route("/webhook/seerr", post(webhooks::seerr_webhook))
        .nest("/board", board_ui.with_state(()))
        .fallback_service(serve_frontend)
        .layer(axum::middleware::from_fn(board::board_assets_middleware))
        .layer(build_cors_layer(cors_allowed_origins))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(port = port, "GraphQL server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;

    Ok(())
}

fn build_cors_layer(allowed: Vec<String>) -> CorsLayer {
    if allowed.is_empty() {
        // Reachable only when api_key is set (start_server bails on the
        // empty-key + empty-origins combo). Permissive CORS is acceptable
        // here because every privileged endpoint gates on the API key.
        tracing::warn!(
            "CORS is permissive — set RIVEN_SETTING__CORS_ALLOWED_ORIGINS to \
             constrain cross-origin browser access"
        );
        return CorsLayer::permissive();
    }
    let origins: Vec<axum::http::HeaderValue> =
        allowed.iter().filter_map(|o| o.parse().ok()).collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_headers(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
}
