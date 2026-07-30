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
    anyhow::ensure!(
        settings.auth_secret.len() >= 32,
        "RIVEN_SETTING__AUTH_SECRET must be at least 32 characters (got {}). \
         It signs session tokens, so rotating it signs everyone out. \
         Generate one with `openssl rand -hex 32`.",
        settings.auth_secret.len()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut settings = riven_core::settings::RivenSettings::load()?;
    validate_auth_settings(&settings)?;
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
        // better-auth signs sessions with this.
        let auth_secret = settings.auth_secret.clone();
        // Cookie scope and trusted redirect targets are derived from this, so
        // prefer explicit config, then the same ORIGIN the CORS allowlist uses,
        // then the bind address for local runs.
        let public_url = if !settings.public_url.is_empty() {
            settings.public_url.clone()
        } else {
            std::env::var("ORIGIN")
                .ok()
                .filter(|origin| !origin.trim().is_empty())
                .unwrap_or_else(|| format!("http://{gql_host}:{gql_port}"))
        };
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
                auth_secret,
                public_url,
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
    use super::validate_auth_settings;
    use riven_core::settings::RivenSettings;

    fn settings(api_key: &str, auth_secret: &str) -> RivenSettings {
        RivenSettings {
            api_key: api_key.to_string(),
            auth_secret: auth_secret.to_string(),
            ..RivenSettings::default()
        }
    }

    const GOOD_SECRET: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn a_complete_configuration_is_accepted() {
        assert!(validate_auth_settings(&settings("a-key", GOOD_SECRET)).is_ok());
    }

    /// The default settings carry empty strings for both, so a bare `docker run`
    /// with no env must fail here rather than come up unauthenticated.
    #[test]
    fn the_defaults_do_not_boot() {
        assert!(validate_auth_settings(&RivenSettings::default()).is_err());
    }

    #[test]
    fn a_missing_or_blank_api_key_is_rejected() {
        for key in ["", "   "] {
            let error = validate_auth_settings(&settings(key, GOOD_SECRET))
                .expect_err("a blank api key must not boot")
                .to_string();
            assert!(error.contains("RIVEN_SETTING__API_KEY"), "{error}");
        }
    }

    /// Matches `authn::build`'s own floor, so the failure lands at startup
    /// rather than inside the spawned GraphQL task where it only got logged.
    #[test]
    fn a_short_auth_secret_is_rejected() {
        for secret in ["", "too-short"] {
            let error = validate_auth_settings(&settings("a-key", secret))
                .expect_err("a short secret must not boot")
                .to_string();
            assert!(error.contains("at least 32 characters"), "{error}");
        }
        assert!(validate_auth_settings(&settings("a-key", &"x".repeat(32))).is_ok());
    }
}
