use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use sea_orm::ConnectionTrait;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use riven_core::events::RivenEvent;
use riven_core::reindex::ReindexConfig;
use riven_queue::{DownloaderConfig, JobQueue};

mod runtime;
mod setup;
mod usenet;

use usenet::setting_u64;

/// The streaming path allocates and frees a ~700 KB buffer per article, on the
/// order of a hundred a second under 4K playback. musl's allocator serves those
/// through `mmap` and returns them with `madvise(MADV_DONTNEED)`, so every
/// re-allocation re-faults its pages in — ~6 % of CPU on the streaming profile,
/// none of it doing work. mimalloc keeps the pages on a thread-local free list
/// instead.
///
/// Set here rather than in a library crate because a `#[global_allocator]` is a
/// property of the final binary: a library that declared one would force it on
/// every consumer, including the test harnesses.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

const USER_AGENT: &str = concat!("riven-rs/", env!("CARGO_PKG_VERSION"));

/// Client for outbound API calls — metadata providers, scrapers, notification
/// targets.
///
/// Kept separate from [`build_stream_client`] because the two workloads want
/// opposite settings, and one client cannot hold both. These are many small
/// JSON requests to a handful of repeatedly-polled hosts, so HTTP/2 is left
/// enabled (multiplexing and header compression both pay off) and a total
/// request deadline is meaningful.
fn build_api_client() -> Result<reqwest::Client> {
    use riven_core::config::api;

    Ok(reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .dns_resolver(riven_core::dns::CachedDnsResolver)
        .connect_timeout(Duration::from_secs(api::CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(api::REQUEST_TIMEOUT_SECS))
        .pool_idle_timeout(Duration::from_secs(api::POOL_IDLE_TIMEOUT_SECS))
        .pool_max_idle_per_host(16)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .connection_verbose(false)
        .build()?)
}

/// Client for VFS range reads against debrid origins.
///
/// The principal difference from [`build_api_client`] is its timeout policy:
///
/// `read_timeout`, not `timeout`. Reqwest's `timeout` is a total deadline
///   covering the body; on a multi-megabyte ranged read that caps throughput
///   instead of detecting a fault, and it trips hardest exactly when archive
///   read-ahead has several fetches sharing the link. `read_timeout` resets on
///   every successful read, so it catches a genuinely stalled connection and
///   leaves a slow-but-progressing one alone.
fn build_stream_client() -> Result<reqwest::Client> {
    use riven_core::config::vfs;

    Ok(reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .dns_resolver(riven_core::dns::CachedDnsResolver)
        .connect_timeout(Duration::from_secs(vfs::CONNECT_TIMEOUT_SECS))
        .read_timeout(Duration::from_secs(vfs::ACTIVITY_TIMEOUT_SECS))
        .pool_idle_timeout(Duration::from_secs(vfs::ACTIVITY_TIMEOUT_SECS))
        .pool_max_idle_per_host(32)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .connection_verbose(false)
        .build()?)
}

/// Reject a misconfigured instance before anything is opened or written.
///
/// Both values come from the environment only — `apply_general_db_override`
/// does not touch them — so this runs on the first line, ahead of migrations
/// and ahead of the optional startup wipe.
///
/// Refusing to boot is what riven-ts did: its `hooks.server.ts` threw when the
/// API key was absent, so there was never an unconfigured instance to be lenient
/// about. The Rust port made both optional, and the two failure modes were
/// quiet: a missing key disabled the only credential machine callers have, and a
/// missing secret left the process running with a dead GraphQL server, because
/// `authn::build`'s error surfaced inside a spawned task that only logged it.
fn validate_auth_settings(settings: &riven_core::settings::RivenSettings) -> Result<()> {
    anyhow::ensure!(
        !settings.api_key.trim().is_empty(),
        "RIVEN_SETTING__API_KEY is required. It authenticates machine callers \
         (Overseerr/Jellyseerr webhooks) and derives the Stremio addon token. \
         Generate one with `openssl rand -hex 32`."
    );
    Ok(())
}

/// Whether `host` is a wildcard bind address rather than somewhere a browser
/// can go. Parsed rather than string-matched so `::`, `[::]` and the expanded
/// `0:0:0:0:0:0:0:0` are all caught alongside `0.0.0.0`.
fn is_unspecified_host(host: &str) -> bool {
    host.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_unspecified())
}

/// The host component of a URL, without scheme, port or path.
fn url_host(url: &str) -> Option<&str> {
    let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    // An IPv6 literal keeps its brackets, so only split a port off after them.
    let host = match authority.rsplit_once(']') {
        Some((bracketed, _)) => &authority[..bracketed.len() + 1],
        None => authority.split(':').next().unwrap_or(authority),
    };
    (!host.is_empty()).then_some(host)
}

/// The public origin browsers reach riven at.
///
/// Explicit configuration wins, then `ORIGIN` (the same value the CORS allowlist
/// falls back to), then the bind address.
///
/// The bind address is the interesting case: `gql_host` defaults to `0.0.0.0`,
/// which is meaningful to `bind()` and meaningless to a browser. Used verbatim
/// it became better-auth's `base_url`, which sets the cookie scope and — because
/// the passkey relying-party ID is that URL's host — produced an RP ID of
/// `0.0.0.0`, which every browser rejects. Passkeys then failed at registration
/// with nothing in the logs pointing here. A wildcard bind is normal, so it is
/// resolved to loopback rather than refused; a wildcard someone typed into
/// `PUBLIC_URL` or `ORIGIN` is a mistake, so it fails the boot.
fn resolve_public_url(
    configured: &str,
    origin_env: Option<String>,
    gql_host: &str,
    gql_port: u16,
) -> Result<String> {
    for (value, source) in [
        (configured.trim(), "RIVEN_SETTING__PUBLIC_URL"),
        (origin_env.as_deref().map(str::trim).unwrap_or(""), "ORIGIN"),
    ] {
        if value.is_empty() {
            continue;
        }
        anyhow::ensure!(
            !url_host(value).is_some_and(is_unspecified_host),
            "{source} is set to `{value}`, whose host is a wildcard bind address. \
             It must be the origin a browser reaches riven at — passkeys are bound \
             to this hostname and will not register against a wildcard."
        );
        return Ok(value.to_string());
    }

    if is_unspecified_host(gql_host) {
        let fallback = format!("http://localhost:{gql_port}");
        tracing::info!(
            %gql_host,
            public_url = %fallback,
            "no public URL configured; assuming loopback because the bind host is a wildcard"
        );
        return Ok(fallback);
    }
    Ok(format!("http://{gql_host}:{gql_port}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut settings = riven_core::settings::RivenSettings::load()?;
    validate_auth_settings(&settings)?;
    // Validation accepted the trimmed form, so make that the stored form.
    // Otherwise a key with stray whitespace passes the check and then fails
    // every comparison: `api_key_matches` trims the incoming header, and the
    // Stremio addon token is an HMAC keyed on the untrimmed value.
    settings.api_key = settings.api_key.trim().to_string();
    // `connect` opens the SeaORM connection and publishes it as the process-wide
    // global that the migrated repo functions read via `riven_db::orm()`. It must
    // run before any repo call. The returned handle is only needed locally for
    // `run_migrations` and the optional startup wipe.
    let db = riven_db::connect(&settings.database_url).await?;
    riven_db::run_migrations(&db).await?;

    if let Ok(Some(general_settings)) = riven_db::repo::get_setting("general").await {
        settings.apply_general_db_override(&general_settings);
    }

    let log_settings = riven_core::logging::LogSettings::from(&settings);
    let (log_tx, _) = broadcast::channel::<String>(1024);
    let observability =
        riven_core::logging::init_logging(&log_settings, &settings.log_directory, log_tx.clone())?;
    let log_control = observability.log_control.clone();
    tracing::info!("riven starting up");

    if settings.unsafe_wipe_database_on_startup {
        tracing::warn!("unsafe_wipe_database_on_startup is enabled — wiping database");
        riven_db::orm()
            .execute_unprepared("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .await?;
        riven_db::run_migrations(&db).await?;
    }

    let redis_conn = riven_queue::connect_managed(settings.redis_url.as_str()).await?;
    tracing::info!("redis connection established");

    let http_client = riven_core::http::HttpClient::new(build_api_client()?);
    let stream_http_client = build_stream_client()?;

    let registry = setup::register_plugins(
        http_client.clone(),
        redis_conn,
        settings.filesystem.mount_path.clone(),
        &settings,
    )
    .await;

    let mut usenet_download_workers: Option<usize> = None;
    let usenet_settings_json = registry.get_plugin_settings_json("usenet").await;
    let usenet_streamer: Option<riven_usenet::UsenetStreamer> = match usenet_settings_json
        .as_ref()
        .and_then(plugin_usenet::nntp_config_from_json_value)
    {
        Some(cfg) => {
            let primary = cfg.primary();
            tracing::info!(
                providers = cfg.providers.len(),
                host = primary.map(|c| c.host.as_str()).unwrap_or("?"),
                port = primary.map(|c| c.port).unwrap_or(0),
                tls = primary.map(|c| c.use_tls).unwrap_or(true),
                "usenet streaming enabled"
            );
            let configured = setting_u64(&usenet_settings_json, "maxdownloadworkers")
                .map(|n| n as usize)
                .filter(|&n| n > 0);
            usenet_download_workers =
                Some(configured.unwrap_or(riven_usenet::DEFAULT_DOWNLOAD_WORKERS));
            Some(riven_usenet::UsenetStreamer::shared(cfg, db.clone()))
        }
        None => {
            tracing::info!("usenet streaming disabled (plugin not configured)");
            None
        }
    };

    let (notification_tx, _) = broadcast::channel::<String>(512);

    let job_queue = Arc::new(
        JobQueue::new(
            &settings.redis_url,
            registry.clone(),
            notification_tx.clone(),
            DownloaderConfig::from(&settings),
            ReindexConfig::from(&settings),
            settings.filesystem.clone(),
            settings.retry_interval_secs,
            settings.maximum_scrape_attempts,
        )
        .await?,
    );
    {
        let mut redis = job_queue.redis.clone();
        let queues = job_queue.queue_names();
        riven_queue::prune_queue_history(&mut redis, &queues).await;
    }

    // Reconcile stored library-profile membership against the current filesystem
    // settings once at boot. Membership is otherwise only written at download
    // time and on settings changes, so a profile added while a save failed — or
    // any drift from the active filter rules — would leave its library view
    // empty until the next edit. Only diffs are written, so this is a no-op in
    // steady state.
    match riven_queue::reconcile_library_profiles(&settings.filesystem).await {
        Ok(0) => {}
        Ok(updated) => {
            tracing::info!(updated, "reconciled library-profile membership at startup")
        }
        Err(error) => tracing::error!(%error, "failed to reconcile library-profile membership"),
    }

    let (link_tx, mut link_rx) = tokio::sync::mpsc::channel(64);

    let vfs_mount_path = settings.filesystem.mount_path.clone();
    let usenet_local_source: Option<Arc<dyn riven_core::local_source::LocalByteSource>> =
        usenet_streamer
            .clone()
            .map(|s| Arc::new(s) as Arc<dyn riven_core::local_source::LocalByteSource>);
    let vfs_mount_manager = Arc::new(riven_api::vfs_mount::VfsMountManager::new(
        &vfs_mount_path,
        job_queue.vfs_layout.clone(),
        job_queue.filesystem_settings_revision.clone(),
        stream_http_client.clone(),
        link_tx.clone(),
        usenet_local_source,
    )?);

    usenet::spawn_background_tasks(
        usenet_streamer.clone(),
        usenet_settings_json.clone(),
        job_queue.clone(),
        registry.clone(),
    );

    tokio::spawn({
        let link_registry = registry.clone();
        async move {
            while let Some(req) = link_rx.recv().await {
                let event = RivenEvent::MediaItemStreamLinkRequested {
                    magnet: req.download_url,
                    info_hash: String::new(),
                    provider: req.provider,
                };
                let results = link_registry.dispatch(&event).await;

                let mut link = None;
                for (_, result) in results {
                    if let Ok(riven_core::events::HookResponse::StreamLink(sl)) = result {
                        link = Some(sl.link);
                        break;
                    }
                }

                drop(req.response_tx.send(link));
            }
        }
    });

    let cancel = CancellationToken::new();

    let gql_host = settings.gql_host.clone();
    let gql_port = settings.gql_port;
    // Auth configuration was validated before the database was opened; see
    // `validate_auth_settings`.
    let public_url = resolve_public_url(
        &settings.public_url,
        std::env::var("ORIGIN").ok(),
        &gql_host,
        gql_port,
    )?;

    let gql_handle = tokio::spawn({
        let jq = job_queue.clone();
        let reg = registry.clone();
        let api_key = (!settings.api_key.is_empty()).then(|| settings.api_key.clone());
        let log_dir = settings.log_directory.clone();
        let mut cors_allowed_origins: Vec<String> = settings
            .cors_allowed_origins
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if cors_allowed_origins.is_empty()
            && let Ok(origin) = std::env::var("ORIGIN")
            && !origin.trim().is_empty()
        {
            tracing::info!(origin, "CORS allowlist falling back to ORIGIN");
            cors_allowed_origins.push(origin);
        }
        // Cookie scope and trusted redirect targets are derived from this, so
        // prefer explicit config, then the same ORIGIN the CORS allowlist uses,
        // then the bind address for local runs.
        let public_url = public_url.clone();
        let oidc_providers = settings.oidc_providers.clone();
        let log_tx = log_tx.clone();
        let notif_tx = notification_tx.clone();
        let log_control = log_control.clone();
        let vfs_mount_manager = vfs_mount_manager.clone();
        let cancel = cancel.clone();
        async move {
            if let Err(e) = riven_api::start_server(riven_api::StartServerConfig {
                host: gql_host,
                port: gql_port,
                registry: reg,
                job_queue: jq.clone(),
                http_client: http_client.clone(),
                api_key,
                log_directory: log_dir,
                log_tx,
                notification_tx: notif_tx,
                downloader_config: jq.downloader_config.clone(),
                log_control,
                stream_client: stream_http_client.clone(),
                link_request_tx: link_tx.clone(),
                cors_allowed_origins,
                vfs_mount_manager,
                cancel,
                public_url,
                oidc_providers,
            })
            .await
            {
                tracing::error!(error = %e, "GraphQL server error");
            }
        }
    });

    job_queue.notify(RivenEvent::CoreStarted).await;

    let runtime_tasks = runtime::start(job_queue.clone(), cancel.clone(), usenet_download_workers);

    tracing::info!(gql_port, vfs = vfs_mount_path, "riven is running");

    runtime::wait_for_shutdown().await?;
    tracing::info!("shutdown signal received; draining");

    job_queue.notify(RivenEvent::CoreShutdown).await;
    cancel.cancel();

    runtime_tasks.drain(gql_handle).await;

    vfs_mount_manager.unmount().await;
    observability.shutdown();

    tracing::info!("riven shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{resolve_public_url, url_host, validate_auth_settings};
    use riven_core::settings::RivenSettings;

    fn settings(api_key: &str) -> RivenSettings {
        RivenSettings {
            api_key: api_key.to_string(),
            ..RivenSettings::default()
        }
    }

    /// The bug this guards: `0.0.0.0` reaching auth as `base_url` makes
    /// the passkey relying-party ID `0.0.0.0`, which browsers reject.
    #[test]
    fn a_wildcard_bind_host_resolves_to_loopback() {
        assert_eq!(
            resolve_public_url("", None, "0.0.0.0", 8080).unwrap(),
            "http://localhost:8080"
        );
        assert_eq!(
            resolve_public_url("", None, "::", 8080).unwrap(),
            "http://localhost:8080"
        );
        // A real bind host is already an origin and is left alone.
        assert_eq!(
            resolve_public_url("", None, "127.0.0.1", 3000).unwrap(),
            "http://127.0.0.1:3000"
        );
    }

    #[test]
    fn explicit_configuration_wins_over_the_bind_address() {
        assert_eq!(
            resolve_public_url("https://riven.example.com", None, "0.0.0.0", 8080).unwrap(),
            "https://riven.example.com"
        );
        assert_eq!(
            resolve_public_url(
                "",
                Some("https://from-origin.example".to_string()),
                "0.0.0.0",
                8080
            )
            .unwrap(),
            "https://from-origin.example"
        );
    }

    /// A wildcard someone typed is a mistake, not a default to paper over.
    #[test]
    fn an_explicitly_configured_wildcard_is_refused() {
        for (configured, origin) in [
            ("http://0.0.0.0:8080", None),
            ("https://[::]", None),
            ("", Some("http://0.0.0.0:8080".to_string())),
        ] {
            let error = resolve_public_url(configured, origin, "127.0.0.1", 8080)
                .expect_err("a wildcard host must not boot")
                .to_string();
            assert!(error.contains("wildcard bind address"), "{error}");
        }
    }

    #[test]
    fn url_hosts_are_extracted_without_scheme_port_or_path() {
        assert_eq!(
            url_host("https://riven.example.com/x?y"),
            Some("riven.example.com")
        );
        assert_eq!(url_host("http://0.0.0.0:8080"), Some("0.0.0.0"));
        assert_eq!(url_host("https://[::1]:8443/a"), Some("[::1]"));
        assert_eq!(url_host("riven.example.com"), Some("riven.example.com"));
    }

    #[test]
    fn a_complete_configuration_is_accepted() {
        assert!(validate_auth_settings(&settings("a-key")).is_ok());
    }

    /// The default settings carry an empty API key, so a bare `docker run`
    /// with no env must fail here rather than come up unauthenticated.
    #[test]
    fn the_defaults_do_not_boot() {
        assert!(validate_auth_settings(&RivenSettings::default()).is_err());
    }

    #[test]
    fn a_missing_or_blank_api_key_is_rejected() {
        for key in ["", "   "] {
            let error = validate_auth_settings(&settings(key))
                .expect_err("a blank api key must not boot")
                .to_string();
            assert!(error.contains("RIVEN_SETTING__API_KEY"), "{error}");
        }
    }
}
