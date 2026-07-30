mod auth;
mod authn;
mod board;
mod first_user;
mod graphql;
mod legacy_password;
mod media;
mod plex;
mod stremio;

use std::sync::Arc;

use anyhow::Result;
use axum::http::{
    HeaderName, Method,
    header::{ACCEPT_RANGES, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
};
use axum::{Router, routing::get};
use better_auth::integrations::axum::AxumIntegration;
use riven_core::http::HttpClient;
use riven_core::logging::LogControl;
use riven_core::plugin::PluginRegistry;
use riven_core::stream_link::LinkRequest;
use riven_queue::JobQueue;
use riven_queue::main_orchestrator::start_event_controller;
use tokio::sync::broadcast;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};

use apalis_board_api::framework::{ApiBuilder, RegisterRoute};
use apalis_board_api::ui::ServeUI;

use crate::schema::build_schema;
use crate::vfs_mount::VfsMountManager;

pub use state::ApiState;

pub struct StartServerConfig {
    pub host: String,
    pub port: u16,
    pub registry: Arc<PluginRegistry>,
    pub job_queue: Arc<JobQueue>,
    pub http_client: HttpClient,
    pub api_key: Option<String>,
    pub log_directory: String,
    pub log_tx: broadcast::Sender<String>,
    pub notification_tx: broadcast::Sender<String>,
    pub downloader_config: Arc<tokio::sync::RwLock<riven_core::downloader::DownloaderConfig>>,
    pub log_control: Arc<LogControl>,
    pub stream_client: reqwest::Client,
    pub link_request_tx: tokio::sync::mpsc::Sender<LinkRequest>,
    pub cors_allowed_origins: Vec<String>,
    pub vfs_mount_manager: Arc<VfsMountManager>,
    pub cancel: tokio_util::sync::CancellationToken,
    /// Signing key for `better-auth` sessions. Must be at least 32 bytes;
    /// rotating it invalidates every session.
    pub auth_secret: String,
    /// Public origin the browser reaches riven at. `better-auth` uses it for
    /// cookie scope and trusted redirect targets, so a wrong value is a login
    /// loop rather than a loud failure.
    pub public_url: String,
}

mod state {
    use std::sync::Arc;

    use axum::extract::FromRef;
    use riven_core::stream_link::LinkRequest;
    use riven_queue::JobQueue;
    use tokio::sync::broadcast;

    use crate::schema::AppSchema;
    use crate::server::authn::RivenAuth;

    #[derive(Clone)]
    pub struct ApiState {
        pub schema: AppSchema,
        pub job_queue: Arc<JobQueue>,
        pub api_key: Option<String>,
        pub log_tx: broadcast::Sender<String>,
        pub notification_tx: broadcast::Sender<String>,
        pub stream_client: reqwest::Client,
        pub link_request_tx: tokio::sync::mpsc::Sender<LinkRequest>,
        pub runtime: tokio::runtime::Handle,
        pub auth: Arc<RivenAuth>,
    }

    /// Lets `better-auth`'s router and its `CurrentSession` / `OptionalSession`
    /// extractors pull the auth handle out of riven's own state.
    impl FromRef<ApiState> for Arc<RivenAuth> {
        fn from_ref(state: &ApiState) -> Self {
            state.auth.clone()
        }
    }
}

pub async fn start_server(config: StartServerConfig) -> Result<()> {
    let StartServerConfig {
        host,
        port,
        registry,
        job_queue,
        http_client,
        api_key,
        log_directory,
        log_tx,
        notification_tx,
        downloader_config,
        log_control,
        stream_client,
        link_request_tx,
        cors_allowed_origins,
        vfs_mount_manager,
        cancel,
        auth_secret,
        public_url,
    } = config;

    // Built before the router so a bad secret fails startup loudly rather than
    // at the first login attempt.
    let auth = authn::build(&auth_secret, &public_url, cors_allowed_origins.clone()).await?;

    let schema = build_schema(
        registry,
        job_queue.clone(),
        http_client,
        log_directory,
        downloader_config,
        log_control,
        log_tx.clone(),
        vfs_mount_manager,
        crate::schema::StremioAddonToken(riven_core::stremio::addon_token(
            api_key.as_deref().unwrap_or_default(),
        )),
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
        std::env::var("RIVEN_STATIC_DIR").unwrap_or_else(|_| "./frontend/build".to_string());
    let serve_frontend =
        ServeDir::new(&static_dir).fallback(ServeFile::new(format!("{static_dir}/index.html")));

    let state = ApiState {
        schema,
        job_queue,
        api_key,
        log_tx,
        notification_tx,
        stream_client,
        link_request_tx,
        runtime: tokio::runtime::Handle::current(),
        auth: auth.clone(),
    };

    // Routes reached by third-party players. Stremio and whatever player it
    // hands a stream URL to are origins we can't enumerate, and the instance
    // allowlist emits no `access-control-allow-origin` for anything outside it —
    // which fails the fetch before a body is read. These authenticate by token
    // in the URL rather than by cookie, so a wildcard origin grants no ambient
    // authority and they carry their own permissive CORS. The allowlist stays in
    // force for /graphql, /board and the frontend.
    let player_routes = Router::new()
        .route(
            "/media/{entry_id}",
            get(media::media_bridge_handler).head(media::media_bridge_handler),
        )
        // Alias of the above, so every path Stremio touches lives under
        // `/stremio` and one proxy rule covers manifest, streams and bytes.
        .route(
            "/stremio/media/{entry_id}",
            get(media::media_bridge_handler).head(media::media_bridge_handler),
        )
        .route(
            "/stremio/{token}/manifest.json",
            get(stremio::manifest_handler),
        )
        .route(
            "/stremio/{token}/stream/{kind}/{id}",
            get(stremio::stream_handler),
        )
        .layer(build_player_cors_layer());

    let routes = Router::new()
        .route(
            "/graphql",
            get(graphql::graphql_get_handler).post(graphql::graphql_handler),
        )
        // Hands out the addon token, so it stays on the instance allowlist —
        // only the frontend should be able to read it.
        .route("/stremio/manifest-url", get(stremio::manifest_url_handler))
        // Plex sign-in. Not under better-auth's router because Plex is a
        // PIN-and-poll flow, not OAuth2 — see `plex.rs`.
        .route("/auth/plex/start", axum::routing::post(plex::start))
        .route("/auth/plex/poll/{pin_id}", get(plex::poll))
        // Whether better-auth's own `/auth/sign-up/email` will accept a caller.
        // See `first_user.rs`: it does exactly once, for the first account.
        .route("/auth/first-user", get(first_user::availability))
        // better-auth's own endpoints: sign-in/out, sessions, password, 2FA,
        // passkeys, API keys, admin.
        //
        // `axum_router_with_state` yields *unprefixed* routes (`/sign-in/email`,
        // `/get-session`, …) — `AuthConfig::base_path` governs cookie scope and
        // generated URLs, not where the router mounts. So this must be nested,
        // not merged: merging put every endpoint at the root, where `/auth/*`
        // then fell through to the SPA fallback and answered POSTs with 405.
        // The prefix here and `base_path` in `authn::build` must stay in step.
        .nest("/auth", auth.clone().axum_router_with_state::<ApiState>())
        .nest("/api/v1", board_api.with_state(()))
        .nest("/board", board_ui.with_state(()))
        .fallback_service(serve_frontend)
        .layer(build_cors_layer(cors_allowed_origins));

    let app = player_routes
        .merge(routes)
        .layer(axum::middleware::from_fn(board::board_assets_middleware))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    tracing::info!(host = %host, port = port, "GraphQL server listening");

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;

    Ok(())
}

/// CORS for the token-authenticated player routes. Origin is wildcarded because
/// the set of clients is open-ended (Stremio, VLC-in-a-webview, Infuse), and
/// `Range` has to be allowed or seeking fails. `Content-Range`/`Accept-Ranges`
/// are exposed so a scripted player can read them back. Credentials are
/// deliberately not allowed — the token in the URL is the only credential, so
/// there is no cookie for a wildcard origin to leak.
fn build_player_cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
        .allow_headers(tower_http::cors::Any)
        .expose_headers([ACCEPT_RANGES, CONTENT_RANGE, CONTENT_LENGTH, CONTENT_TYPE])
}

/// CORS for the instance's own surface: GraphQL, `/auth`, the board, the bundle.
///
/// Credentials are allowed, because the session — and the passkey challenge that
/// briefly sits beside it — are cookies, and a cross-origin `fetch` with
/// `credentials: "include"` is rejected outright without
/// `access-control-allow-credentials: true`. That is also why the header and
/// method lists are spelled out: the Fetch spec forbids pairing `*` with
/// credentials, and `tower-http` panics rather than emitting a header a browser
/// would refuse.
///
/// With no allowlist there is no safe answer — `*` and credentials cannot
/// coexist — so the permissive fallback stays credential-less. Same-origin still
/// works there (riven serves the bundle itself), but a separate dev server at
/// another port will not get a session until an origin is configured.
fn build_cors_layer(allowed: Vec<String>) -> CorsLayer {
    if allowed.is_empty() {
        tracing::warn!(
            "CORS is permissive and cookie-less — set RIVEN_SETTING__CORS_ALLOWED_ORIGINS \
             to allow a cross-origin frontend to hold a session"
        );
        return CorsLayer::permissive();
    }
    let origins: Vec<axum::http::HeaderValue> =
        allowed.iter().filter_map(|o| o.parse().ok()).collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_credentials(true)
        .allow_headers([
            CONTENT_TYPE,
            AUTHORIZATION,
            HeaderName::from_static("x-api-key"),
        ])
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
}
