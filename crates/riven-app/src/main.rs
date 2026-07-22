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

fn build_http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .dns_resolver(riven_core::dns::CachedDnsResolver)
        .connect_timeout(Duration::from_secs(
            riven_core::config::vfs::CONNECT_TIMEOUT_SECS,
        ))
        .timeout(Duration::from_secs(
            riven_core::config::vfs::ACTIVITY_TIMEOUT_SECS,
        ))
        .pool_idle_timeout(Duration::from_secs(
            riven_core::config::vfs::ACTIVITY_TIMEOUT_SECS,
        ))
        .pool_max_idle_per_host(32)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http1_only()
        .connection_verbose(false)
        .build()?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut settings = riven_core::settings::RivenSettings::load()?;
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

    let reqwest_client = build_http_client()?;
    let http_client = riven_core::http::HttpClient::new(reqwest_client.clone());
    let stream_http_client = reqwest_client;

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
        settings.vfs_cache_max_size_mb,
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
    if settings.api_key.is_empty() {
        tracing::warn!("RIVEN_SETTING__API_KEY is empty — GraphQL API auth is DISABLED (dev only)");
    }
    if settings.frontend_auth_signing_secret.is_empty() {
        tracing::warn!(
            "RIVEN_SETTING__FRONTEND_AUTH_SIGNING_SECRET is empty — frontend RBAC is DISABLED"
        );
    }
    let gql_handle = tokio::spawn({
        let jq = job_queue.clone();
        let reg = registry.clone();
        let api_key = (!settings.api_key.is_empty()).then(|| settings.api_key.clone());
        let frontend_auth_signing_secret = (!settings.frontend_auth_signing_secret.is_empty())
            .then(|| settings.frontend_auth_signing_secret.clone());
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
                frontend_auth_signing_secret,
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
