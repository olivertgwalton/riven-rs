mod artwork;
mod auth;
mod authn;
mod board;
mod graphql;
mod legacy_password;
mod media;
mod nzb_upload;
mod oidc;
mod plex;
mod stremio;

use std::sync::Arc;

use anyhow::Result;
use axum::extract::DefaultBodyLimit;
use axum::http::{
    HeaderName, HeaderValue, Method,
    header::{ACCEPT_RANGES, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
};
use axum::{
    Router,
    routing::{get, post},
};
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
    pub redis_conn: redis::aio::ConnectionManager,
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
    /// Public origin the browser reaches riven at. Cookie security, the
    /// passkey relying party and OAuth redirect URIs derive from it, so a
    /// wrong value is a login loop rather than a loud failure.
    pub public_url: String,
    /// OIDC sign-in providers (PocketID, Authelia, Keycloak, ...). Endpoints
    /// are resolved via discovery in `authn::build` — see `oidc::resolve_providers`.
    pub oidc_providers: Vec<riven_core::settings::OidcProviderSettings>,
}

mod state {
    use std::sync::Arc;

    use riven_core::stream_link::LinkRequest;
    use riven_queue::JobQueue;
    use tokio::sync::broadcast;

    use crate::schema::AppSchema;
    use crate::server::authn::AuthService;

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
        pub auth: Arc<AuthService>,
        /// Held here as well as in the schema so the artwork proxy can dispatch
        /// to media-server plugins without going through GraphQL.
        pub registry: Arc<riven_core::plugin::PluginRegistry>,
        /// The port this server itself is bound to, so the NZB-upload handler
        /// can hand back a loopback URL (`http://127.0.0.1:{gql_port}/...`)
        /// without threading the instance's public URL through just for this.
        pub gql_port: u16,
    }
}

pub async fn start_server(config: StartServerConfig) -> Result<()> {
    let StartServerConfig {
        host,
        port,
        registry,
        job_queue,
        redis_conn,
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
        public_url,
        oidc_providers,
    } = config;

    let auth = authn::build(&public_url, &oidc_providers).await?;

    let schema = build_schema(
        registry.clone(),
        job_queue.clone(),
        redis_conn,
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
        registry,
        gql_port: port,
    };

    let board_guard =
        axum::middleware::from_fn_with_state(state.clone(), board::require_board_admin);

    // Routes reached by third-party players. Stremio and whatever player it
    // hands a stream URL to are origins we can't enumerate, and the instance
    // allowlist emits no `access-control-allow-origin` for anything outside it —
    // which fails the fetch before a body is read. So these carry their own
    // permissive CORS; the allowlist stays in force for /graphql, /board and the
    // frontend.
    //
    // `/media` also accepts the session cookie, because the frontend's download
    // button is an `<a download>` — a top-level navigation, which can carry no
    // header. The wildcard origin still grants no ambient authority over it:
    // `build_player_cors_layer` sends no `access-control-allow-credentials`, so
    // a cross-origin `fetch` with `credentials: "include"` is refused outright
    // and can never read the bytes, and the session cookie is `SameSite=Lax`, so
    // a cross-site subresource (`<video src>`) never carries it either. What Lax
    // does allow — a top-level GET the user clicks through to — delivers the
    // file to that user's own disk, which is the feature.
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
        // Proxies media-server artwork so the browser never receives the Plex
        // token / Emby API key that fetching it requires. See `artwork.rs`.
        .route("/artwork/{server}", get(artwork::artwork_handler))
        // Manual Scrape's "upload an NZB file" entry point. The action lives
        // at a deliberately distinct path from where the staged file is
        // served back out (`/internal/nzb-uploads/{file}`, matching
        // `riven_core::nzb::NZB_UPLOAD_ROUTE_PREFIX`) rather than sharing it,
        // so this exact-match route and the `nest_service` below can never
        // ambiguously overlap in the router. The upload action itself is
        // capped to a small body via a route-scoped `DefaultBodyLimit`;
        // serving the staged file back out (fetched by `plugin-usenet`'s own
        // HTTP client, never the browser) carries no such limit since it only
        // ever reads what this route already wrote. See `nzb_upload.rs`.
        .route(
            "/internal/nzb-upload",
            post(nzb_upload::upload_handler)
                .route_layer(DefaultBodyLimit::max(nzb_upload::MAX_UPLOAD_BYTES)),
        )
        .nest_service(
            "/internal/nzb-uploads",
            ServeDir::new(riven_core::nzb::NZB_UPLOAD_DIR),
        )
        // Everything auth: sign-in/out, sign-up, sessions, password, Plex,
        // passkeys, OIDC, admin — see `authn::router`. It carries its own
        // rate-limit middleware, which needs the state to resolve a caller.
        .nest("/auth", authn::router())
        // The board crate authenticates nothing itself, and `push_task` on
        // `/api/v1` enqueues whatever it is handed. Both halves are admin-only.
        .nest("/api/v1", board_api.with_state(()).layer(board_guard.clone()))
        .nest("/board", board_ui.with_state(()).layer(board_guard))
        .fallback_service(serve_frontend)
        .layer(build_cors_layer(cors_allowed_origins));

    let mut app = player_routes
        .merge(routes)
        .layer(axum::middleware::from_fn(board::board_assets_middleware));

    // Applied above the asset middleware so the board's own bundle is covered
    // too, and outside both routers so a 404 carries them as well.
    for header in SECURITY_HEADERS {
        app = app.layer(security_header_layer(header));
    }

    let app = app.with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    tracing::info!(host = %host, port = port, "GraphQL server listening");

    // `with_connect_info` rather than the plain service so the auth rate
    // limiter can key on the peer address — the only client identity that
    // cannot be forged by a header.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move { cancel.cancelled().await })
    .await?;

    Ok(())
}

/// Response headers applied to everything riven serves.
///
/// `frame-ancestors 'none'` is the load-bearing one: without it the admin UI is
/// framable, and a mutation like `removeItems` is one clickjacked button away.
/// It is spelled as CSP rather than `X-Frame-Options` because the latter is
/// obsolete, plus `X-Frame-Options` for the handful of engines that never
/// implemented `frame-ancestors`.
///
/// `strict-origin-when-cross-origin` keeps query strings out of the `Referer`,
/// which matters for any route that carries a credential in the URL — the
/// Stremio addon token and the `?api_key=` fallback both do.
///
/// The CSP is otherwise deliberately loose. The SPA is Vite-built with hashed
/// assets and no inline scripts, but the board UI it shares an origin with loads
/// WebAssembly, and metadata posters come from TMDB/TVDB/Plex — a tight
/// `default-src` would have to enumerate all of that and would break on the
/// first new artwork host. `frame-ancestors` and `nosniff` are the parts that
/// buy real protection here; there is no `{@html}`, `innerHTML` or `eval`
/// anywhere in the frontend for a script-src to defend.
///
/// Spelled as a list because `SetResponseHeaderLayer` carries one header each;
/// [`start_server`] folds them onto the router.
const SECURITY_HEADERS: [(&str, &str); 4] = [
    ("content-security-policy", "frame-ancestors 'none'"),
    // For the handful of engines that never implemented `frame-ancestors`.
    ("x-frame-options", "DENY"),
    ("x-content-type-options", "nosniff"),
    ("referrer-policy", "strict-origin-when-cross-origin"),
];

fn security_header_layer(
    (name, value): (&'static str, &'static str),
) -> tower_http::set_header::SetResponseHeaderLayer<HeaderValue> {
    tower_http::set_header::SetResponseHeaderLayer::overriding(
        HeaderName::from_static(name),
        HeaderValue::from_static(value),
    )
}

/// CORS for the player routes. Origin is wildcarded because the set of clients
/// is open-ended (Stremio, VLC-in-a-webview, Infuse), and `Range` has to be
/// allowed or seeking fails. `Content-Range`/`Accept-Ranges` are exposed so a
/// scripted player can read them back.
///
/// **Credentials must stay disallowed, and this is now load-bearing.** `/media`
/// accepts the session cookie (see `media::media_credential_ok`), so a wildcard
/// origin here is what stops cross-origin script from reading a signed-in user's
/// media: the Fetch spec makes a `credentials: "include"` request against
/// `access-control-allow-origin: *` a network error, so the response is never
/// readable. `allow_origin(Any)` emits a literal `*` — do not change it to
/// `mirror_request()`, which echoes the caller's origin and, paired with
/// credentials, would hand any site on the internet a read of the library.
///
/// The cookie itself is the second half: better-auth defaults it to
/// `SameSite=Lax`, so a cross-site subresource (`<video src>`) never carries it
/// at all. What Lax permits is a top-level navigation the user clicks, which
/// delivers the file to that user's own disk — the download button.
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

    // `*` cannot be paired with credentials — the Fetch spec forbids it and
    // `tower-http` panics rather than emitting a header browsers would refuse.
    // Treat it as the operator asking for the open configuration, which is the
    // permissive layer below, not as one entry in a list.
    if allowed.iter().any(|origin| origin.trim() == "*") {
        tracing::warn!(
            "RIVEN_SETTING__CORS_ALLOWED_ORIGINS contains `*` — falling back to \
             permissive, cookie-less CORS. A wildcard origin cannot carry credentials, \
             so list the exact origins instead if a cross-origin frontend needs a session"
        );
        return CorsLayer::permissive();
    }

    let mut origins: Vec<axum::http::HeaderValue> = Vec::with_capacity(allowed.len());
    for origin in &allowed {
        match origin.parse() {
            Ok(value) => origins.push(value),
            // Previously `filter_map(…ok())`, which dropped typos in silence. A
            // single bad entry is worth naming; an all-bad list is worse than
            // no list, because `AllowOrigin::list([])` matches nothing and every
            // cross-origin request fails with no explanation.
            Err(_) => tracing::error!(
                %origin,
                "ignoring unparseable entry in RIVEN_SETTING__CORS_ALLOWED_ORIGINS"
            ),
        }
    }

    if origins.is_empty() {
        tracing::error!(
            "every entry in RIVEN_SETTING__CORS_ALLOWED_ORIGINS was unparseable — \
             falling back to permissive, cookie-less CORS rather than an allowlist \
             that would reject every cross-origin request in silence"
        );
        return CorsLayer::permissive();
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// `stamp_private_cache_headers` sets `Vary` on the handler's response, and
    /// the CORS layer then merges its own headers over the top. Every other CORS
    /// header overwrites — `response_headers.extend(headers.drain())` — but
    /// `Vary` is deliberately special-cased to `append`, so a handler's value is
    /// never lost. That is load-bearing: `Vary: Cookie` is half of what stops a
    /// shared cache replaying one user's media to an anonymous caller, and it
    /// would disappear in silence if the layer ever overwrote it.
    ///
    /// The layer contributes nothing of its own here, which is correct rather
    /// than surprising: `update_vary_header` only lists a header if the response
    /// actually varies by it, and this layer answers with constants —
    /// `allow_origin(Any)` and `allow_headers(Any)` are both `Const` wildcards
    /// and the method list is fixed, so all three `varies_with_*` predicates are
    /// false. Hence the exact-value assertion; a stray `origin` appearing here
    /// would mean someone swapped a constant for a mirrored request value, which
    /// is precisely the change the credentials test above guards against.
    #[tokio::test]
    async fn the_cors_layer_preserves_the_handlers_vary_and_cache_control() {
        let app = Router::new()
            .route(
                "/media/{id}",
                get(|| async {
                    let mut response = axum::response::Response::new(Body::from("bytes"));
                    media::stamp_private_cache_headers(response.headers_mut());
                    response
                }),
            )
            .layer(build_player_cors_layer());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/media/1")
                    .header("origin", "https://player.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let headers = response.headers();
        let vary = headers
            .get_all("vary")
            .iter()
            .map(|value| value.to_str().unwrap().to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(", ");

        assert!(
            vary.contains("cookie"),
            "the CORS layer dropped the handler's `Vary`, leaving media cacheable \
             by a key that ignores the credential: {vary:?}"
        );
        assert_eq!(vary, "cookie, authorization, range");
        assert_eq!(headers.get("cache-control").unwrap(), "private, no-store");
    }

    /// The wildcard origin on the player routes is what stops cross-origin
    /// script from reading a signed-in user's media, now that `/media` accepts
    /// the session cookie — but it only holds while credentials stay
    /// disallowed. The Fetch spec makes a `credentials: "include"` request
    /// against `access-control-allow-origin: *` a network error; pair the two
    /// and every site on the internet can read the library. Asserted rather
    /// than trusted to a comment, because the failure is silent.
    #[tokio::test]
    async fn player_cors_wildcards_the_origin_and_never_allows_credentials() {
        let app = Router::new()
            .route("/media/{id}", get(|| async { "bytes" }))
            .layer(build_player_cors_layer());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/media/1")
                    .header("origin", "https://evil.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let headers = response.headers();
        assert_eq!(
            headers.get("access-control-allow-origin").unwrap(),
            "*",
            "a mirrored origin here would be readable by the caller's script"
        );
        assert!(
            headers.get("access-control-allow-credentials").is_none(),
            "credentials must never be allowed alongside the wildcard origin"
        );
    }

    /// The cookie-less fallback applies to `/graphql` and `/auth` when no
    /// origins are configured, which is the default. Same invariant, different
    /// layer: permissive must not quietly start allowing credentials.
    #[tokio::test]
    async fn the_permissive_fallback_is_also_credential_less() {
        let app = Router::new()
            .route("/graphql", get(|| async { "{}" }))
            .layer(build_cors_layer(Vec::new()));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/graphql")
                    .header("origin", "https://evil.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(
            response
                .headers()
                .get("access-control-allow-credentials")
                .is_none()
        );
    }
}
