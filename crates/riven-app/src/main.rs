use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::signal;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use riven_core::events::RivenEvent;
use riven_core::reindex::ReindexConfig;
use riven_queue::worker::Scheduler;
use riven_queue::{DownloaderConfig, JobQueue};

// Force plugin crate linking so inventory collects registrations.
include!(concat!(env!("OUT_DIR"), "/plugin_crates.rs"));

mod setup;

fn build_http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(
            riven_core::config::vfs::CONNECT_TIMEOUT_SECS,
        ))
        .timeout(Duration::from_secs(
            riven_core::config::vfs::ACTIVITY_TIMEOUT_SECS,
        ))
        .pool_idle_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(16)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http1_only()
        .connection_verbose(false)
        .build()?)
}

fn build_streaming_http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(
            riven_core::config::vfs::CONNECT_TIMEOUT_SECS,
        ))
        .timeout(Duration::from_secs(
            riven_core::config::vfs::ACTIVITY_TIMEOUT_SECS,
        ))
        .pool_idle_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(16)
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .http1_only()
        .connection_verbose(false)
        .build()?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut settings = riven_core::settings::RivenSettings::load()?;
    let db_pool = riven_db::connect(&settings.database_url).await?;
    riven_db::run_migrations(&db_pool).await?;

    let log_settings = riven_core::logging::load_log_settings(&db_pool, &settings).await?;
    let (log_tx, _) = broadcast::channel::<String>(1024);
    let observability =
        riven_core::logging::init_logging(&log_settings, &settings.log_directory, log_tx.clone())?;
    let log_control = observability.log_control.clone();
    tracing::info!("riven starting up");

    if settings.unsafe_wipe_database_on_startup {
        tracing::warn!("unsafe_wipe_database_on_startup is enabled — wiping database");
        sqlx::query("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .execute(&db_pool)
            .await?;
        riven_db::run_migrations(&db_pool).await?;
    }

    if let Ok(Some(general_settings)) = riven_db::repo::get_setting(&db_pool, "general").await {
        settings.apply_general_db_override(&general_settings);
    }

    let redis_client = redis::Client::open(settings.redis_url.as_str())?;
    let redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    tracing::info!("redis connection established");

    let http_client = riven_core::http::HttpClient::new(build_http_client()?);
    let stream_http_client = build_streaming_http_client()?;

    let redis_conn_for_streamer = redis_conn.clone();
    let registry = setup::register_plugins(
        http_client.clone(),
        db_pool.clone(),
        redis_conn,
        settings.effective_vfs_mount_path().to_string(),
    )
    .await;

    // If the `usenet` plugin is configured with NNTP credentials, build a
    // streamer that the /usenet/ HTTP route can serve from. Failure to build
    // is non-fatal — Usenet streaming is just disabled.
    let usenet_streamer: Option<riven_usenet::UsenetStreamer> = match registry
        .get_plugin_settings_json("usenet")
        .await
        .as_ref()
        .and_then(plugin_usenet::nntp_config_from_json_value)
    {
        Some(cfg) => {
            tracing::info!(
                host = %cfg.server.host,
                port = cfg.server.port,
                tls = cfg.server.use_tls,
                "usenet streaming enabled"
            );
            Some(riven_usenet::UsenetStreamer::new(cfg, redis_conn_for_streamer))
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
            db_pool.clone(),
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

    let (link_tx, mut link_rx) = tokio::sync::mpsc::channel(64);

    let vfs_mount_path = settings.effective_vfs_mount_path().to_string();
    let vfs_mount_manager = Arc::new(riven_api::vfs_mount::VfsMountManager::new(
        &vfs_mount_path,
        job_queue.vfs_layout.clone(),
        job_queue.filesystem_settings_revision.clone(),
        db_pool.clone(),
        stream_http_client.clone(),
        link_tx.clone(),
        settings.vfs_cache_max_size_mb,
    )?);

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
                let link = results.into_iter().find_map(|(_, r)| {
                    r.ok().and_then(|resp| match resp {
                        riven_core::events::HookResponse::StreamLink(sl) => Some(sl.link),
                        _ => None,
                    })
                });
                let _ = req.response_tx.send(link);
            }
        }
    });

    let cancel = CancellationToken::new();

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
        let pool = db_pool.clone();
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
        let usenet_streamer = usenet_streamer.clone();
        let cancel = cancel.clone();
        async move {
            if let Err(e) = riven_api::start_server(riven_api::StartServerConfig {
                port: gql_port,
                db_pool: pool,
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
                usenet_streamer,
                cancel,
            })
            .await
            {
                tracing::error!(error = %e, "GraphQL server error");
            }
        }
    });

    job_queue.notify(RivenEvent::CoreStarted).await;

    let monitor_task = tokio::spawn({
        let jq = job_queue.clone();
        let cancel = cancel.clone();
        async move {
            let mut redis_conn = jq.redis.clone();
            let queues = jq.queue_names();
            while !cancel.is_cancelled() {
                riven_queue::clear_worker_registrations(&mut redis_conn, &queues).await;
                riven_queue::purge_orphaned_active_jobs(&mut redis_conn, &queues).await;
                let signal = {
                    let cancel = cancel.clone();
                    async move {
                        cancel.cancelled().await;
                        Ok::<_, std::io::Error>(())
                    }
                };
                let result = tokio::spawn(
                    riven_queue::start_workers(jq.clone()).run_with_signal(signal),
                )
                .await;
                if cancel.is_cancelled() {
                    break;
                }
                match result {
                    Ok(Ok(())) => tracing::warn!("apalis monitor exited, restarting"),
                    Ok(Err(e)) => {
                        tracing::error!(error = %e, "apalis monitor error, restarting in 5s")
                    }
                    Err(e) if e.is_panic() => {
                        tracing::error!("apalis monitor panicked, restarting in 5s")
                    }
                    Err(e) => {
                        tracing::error!(error = ?e, "apalis monitor task failed, restarting in 5s")
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    _ = cancel.cancelled() => break,
                }
            }
        }
    });

    let scheduler_task = tokio::spawn({
        let jq = job_queue.clone();
        let cancel = cancel.clone();
        async move {
            while !cancel.is_cancelled() {
                let scheduler = Scheduler::new(jq.clone(), cancel.clone());
                let result = tokio::spawn(scheduler.run()).await;
                if cancel.is_cancelled() {
                    break;
                }
                match result {
                    Ok(_) => tracing::warn!("scheduler exited unexpectedly, restarting"),
                    Err(e) if e.is_panic() => {
                        tracing::error!("scheduler panicked, restarting in 5s")
                    }
                    Err(e) => {
                        tracing::error!(error = ?e, "scheduler task failed, restarting in 5s")
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                    _ = cancel.cancelled() => break,
                }
            }
        }
    });

    tracing::info!(gql_port, vfs = vfs_mount_path, "riven is running");

    // Handle SIGINT and SIGTERM so the FUSE mount is properly unmounted on shutdown.
    #[cfg(unix)]
    {
        use signal::unix::{SignalKind, signal as unix_signal};
        let mut sigterm = unix_signal(SignalKind::terminate())?;
        tokio::select! {
            _ = signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    signal::ctrl_c().await?;
    tracing::info!("shutdown signal received; draining");

    job_queue.notify(RivenEvent::CoreShutdown).await;
    cancel.cancel();

    let drain = async {
        let _ = tokio::join!(gql_handle, monitor_task, scheduler_task);
    };
    if tokio::time::timeout(Duration::from_secs(30), drain).await.is_err() {
        tracing::warn!("drain timed out after 30s; proceeding to unmount");
    }

    vfs_mount_manager.unmount().await;
    observability.shutdown();

    tracing::info!("riven shutdown complete");
    Ok(())
}
