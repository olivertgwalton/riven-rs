use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::signal;
use tokio::sync::broadcast;

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

    let log_settings = plugin_logs::load_log_settings(&db_pool).await?;
    let (log_tx, _) = broadcast::channel::<String>(1024);
    let log_control =
        plugin_logs::init_logging(&log_settings, &settings.log_directory, log_tx.clone())?;
    tracing::info!("riven starting up");

    if settings.unsafe_refresh_database_on_startup {
        tracing::warn!("unsafe_refresh_database_on_startup is enabled — wiping database");
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

    let registry = setup::register_plugins(http_client.clone(), db_pool.clone(), redis_conn).await;
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
        )
        .await?,
    );
    {
        let mut redis = job_queue.redis.clone();
        riven_queue::prune_queue_history(&mut redis).await;
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
        async move {
            if let Err(e) = riven_api::start_server(
                gql_port,
                pool,
                reg,
                jq.clone(),
                http_client.clone(),
                api_key,
                frontend_auth_signing_secret,
                log_dir,
                log_tx,
                notif_tx,
                jq.downloader_config.clone(),
                log_control,
                stream_http_client.clone(),
                link_tx.clone(),
                cors_allowed_origins,
                vfs_mount_manager,
            )
            .await
            {
                tracing::error!(error = %e, "GraphQL server error");
            }
        }
    });

    job_queue.notify(RivenEvent::CoreStarted).await;

    let monitor_task = tokio::spawn({
        let jq = job_queue.clone();
        async move {
            let mut redis_conn = jq.redis.clone();
            loop {
                riven_queue::clear_worker_registrations(&mut redis_conn).await;
                riven_queue::purge_orphaned_active_jobs(&mut redis_conn).await;
                let result = tokio::spawn(riven_queue::start_workers(jq.clone()).run()).await;
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
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    });

    let scheduler_task = tokio::spawn({
        let db = db_pool.clone();
        let jq = job_queue.clone();
        async move {
            loop {
                let result = tokio::spawn(Scheduler::new(db.clone(), jq.clone()).run()).await;
                match result {
                    Ok(_) => tracing::warn!("scheduler exited unexpectedly, restarting"),
                    Err(e) if e.is_panic() => {
                        tracing::error!("scheduler panicked, restarting in 5s")
                    }
                    Err(e) => {
                        tracing::error!(error = ?e, "scheduler task failed, restarting in 5s")
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
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
    tracing::info!("shutdown signal received");

    job_queue.notify(RivenEvent::CoreShutdown).await;
    gql_handle.abort();
    monitor_task.abort();
    scheduler_task.abort();

    vfs_mount_manager.unmount().await;

    tracing::info!("riven shutdown complete");
    Ok(())
}
