use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::signal;
use tokio::sync::broadcast;

use riven_core::events::RivenEvent;
use riven_queue::worker::Scheduler;
use riven_queue::{DownloaderConfig, JobQueue};

// Force plugin crate linking so inventory collects registrations.
extern crate plugin_comet;
extern crate plugin_calendar;
extern crate plugin_dashboard;
extern crate plugin_emby_jellyfin;
extern crate plugin_listrr;
extern crate plugin_logs;
extern crate plugin_mdblist;
extern crate plugin_notifications;
extern crate plugin_plex;
extern crate plugin_seerr;
extern crate plugin_stremthru;
extern crate plugin_tmdb;
extern crate plugin_torrentio;
extern crate plugin_trakt;
extern crate plugin_tvdb;

mod logging;
mod setup;

#[tokio::main]
async fn main() -> Result<()> {
    // ── Load settings ──
    let mut settings = riven_core::settings::RivenSettings::load()?;

    // ── Create live-log broadcast channel ──
    let (log_tx, _) = broadcast::channel::<String>(1024);

    // ── Initialize logging (console + file + broadcast) ──
    logging::init_logging(&settings, log_tx.clone());

    tracing::info!("riven starting up");

    // ── Connect to database ──
    let db_pool = riven_db::connect(&settings.database_url).await?;

    if settings.unsafe_refresh_database_on_startup {
        tracing::warn!("unsafe_refresh_database_on_startup is enabled — wiping database");
        sqlx::query("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .execute(&db_pool)
            .await?;
    }

    riven_db::run_migrations(&db_pool).await?;

    // ── Load general settings overrides from DB ──
    if let Ok(Some(general)) = riven_db::repo::get_setting(&db_pool, "general").await {
        if let Some(v) = general.get("dubbed_anime_only").and_then(|v| v.as_bool()) {
            settings.dubbed_anime_only = v;
        }
        if let Some(v) = general.get("retry_interval_secs").and_then(|v| v.as_u64()) {
            settings.retry_interval_secs = v;
        }
        if let Some(v) = general.get("minimum_average_bitrate_movies").and_then(|v| v.as_u64()) {
            settings.minimum_average_bitrate_movies = Some(v as u32);
        }
        if let Some(v) = general.get("minimum_average_bitrate_episodes").and_then(|v| v.as_u64()) {
            settings.minimum_average_bitrate_episodes = Some(v as u32);
        }
        if let Some(v) = general.get("maximum_average_bitrate_movies").and_then(|v| v.as_u64()) {
            settings.maximum_average_bitrate_movies = Some(v as u32);
        }
        if let Some(v) = general.get("maximum_average_bitrate_episodes").and_then(|v| v.as_u64()) {
            settings.maximum_average_bitrate_episodes = Some(v as u32);
        }
    }

    // ── Connect to Redis ──
    let redis_client = redis::Client::open(settings.redis_url.as_str())?;
    let redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    tracing::info!("redis connection established");

    // ── Register plugins ──
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("Mozilla/5.0 (compatible; riven/1.0)")
        .build()?;

    let registry = setup::register_plugins(http_client.clone(), db_pool.clone(), redis_conn).await;

    // ── Create notification broadcast channel ──
    let (notification_tx, _) = broadcast::channel::<String>(512);

    // ── Build DownloaderConfig ──
    let downloader_config = DownloaderConfig {
        minimum_average_bitrate_movies: settings.minimum_average_bitrate_movies,
        minimum_average_bitrate_episodes: settings.minimum_average_bitrate_episodes,
        maximum_average_bitrate_movies: settings.maximum_average_bitrate_movies,
        maximum_average_bitrate_episodes: settings.maximum_average_bitrate_episodes,
    };

    // ── Create JobQueue ──
    let job_queue = Arc::new(
        JobQueue::new(
            &settings.redis_url,
            registry.clone(),
            notification_tx.clone(),
            db_pool.clone(),
            downloader_config,
        )
        .await?,
    );

    // ── Start apalis workers ──
    let monitor_jq = job_queue.clone();

    // ── Start scheduler (self-restarting) ──
    let scheduler_db   = db_pool.clone();
    let scheduler_jq   = job_queue.clone();

    // ── Start VFS ──
    let (link_tx, mut link_rx) = tokio::sync::mpsc::channel(64);

    let vfs_handle = if !settings.vfs_mount_path.is_empty() {
        let vfs_session = riven_vfs::mount(
            &settings.vfs_mount_path,
            db_pool.clone(),
            http_client.clone(),
            link_tx,
            settings.vfs_debug_logging,
            settings.vfs_cache_max_size_mb,
        )?;
        Some(vfs_session)
    } else {
        tracing::info!("VFS mount path not configured, skipping VFS");
        None
    };

    // ── Handle VFS link requests via plugin system ──
    let link_registry = registry.clone();
    tokio::spawn(async move {
        while let Some(req) = link_rx.recv().await {
            let event = RivenEvent::MediaItemStreamLinkRequested {
                magnet: req.download_url,
                info_hash: String::new(),
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
    });

    // ── Start GraphQL server ──
    let gql_port = settings.gql_port;
    let gql_pool = db_pool.clone();
    let gql_job_queue = job_queue.clone();
    let gql_log_tx = log_tx.clone();
    let gql_notification_tx = notification_tx.clone();
    let gql_handle = tokio::spawn(async move {
        let api_key = (!settings.api_key.is_empty()).then(|| settings.api_key.clone());
        if let Err(e) = riven_api::start_server(
            gql_port,
            gql_pool,
            registry.clone(),
            gql_job_queue.clone(),
            api_key,
            settings.log_directory.clone(),
            gql_log_tx,
            gql_notification_tx,
            gql_job_queue.downloader_config.clone(),
        )
        .await
        {
            tracing::error!(error = %e, "GraphQL server error");
        }
    });

    // ── Publish started event ──
    job_queue.notify(RivenEvent::CoreStarted).await;

    // ── Run everything ──
    let monitor_task = tokio::spawn(async move {
        let mut redis_conn = monitor_jq.redis.clone();
        loop {
            riven_queue::clear_worker_registrations(&mut redis_conn).await;
            let result = tokio::spawn(riven_queue::start_workers(monitor_jq.clone()).run()).await;
            match result {
                Ok(Ok(())) => tracing::warn!("apalis monitor exited, restarting"),
                Ok(Err(e)) => tracing::error!(error = %e, "apalis monitor error, restarting in 5s"),
                Err(e) if e.is_panic() => tracing::error!("apalis monitor panicked, restarting in 5s"),
                Err(e) => tracing::error!(error = ?e, "apalis monitor task failed, restarting in 5s"),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
    let scheduler_task = tokio::spawn(async move {
        loop {
            let result = tokio::spawn(
                Scheduler::new(scheduler_db.clone(), scheduler_jq.clone()).run()
            ).await;
            match result {
                Ok(_) => tracing::warn!("scheduler exited unexpectedly, restarting"),
                Err(e) if e.is_panic() => tracing::error!("scheduler panicked, restarting in 5s"),
                Err(e) => tracing::error!(error = ?e, "scheduler task failed, restarting in 5s"),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    tracing::info!(
        gql_port = gql_port,
        vfs = settings.vfs_mount_path,
        "riven is running"
    );

    // ── Wait for shutdown signal ──
    // Handle both SIGINT (Ctrl+C) and SIGTERM (docker stop / systemd).
    // Without SIGTERM handling the process is SIGKILL'd before drop(vfs) runs,
    // leaving a stale FUSE mount that causes media servers to wipe their library.
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

    // Publish shutdown event
    job_queue.notify(RivenEvent::CoreShutdown).await;

    // Graceful shutdown
    gql_handle.abort();
    monitor_task.abort();
    scheduler_task.abort();

    if let Some(vfs) = vfs_handle {
        tracing::info!("unmounting VFS");
        drop(vfs);
    }

    tracing::info!("riven shutdown complete");
    Ok(())
}
