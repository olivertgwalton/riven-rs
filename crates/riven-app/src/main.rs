use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use sea_orm::ConnectionTrait;
use tokio::signal;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use riven_core::events::RivenEvent;
use riven_core::reindex::ReindexConfig;
use riven_queue::worker::Scheduler;
use riven_queue::{DownloaderConfig, JobQueue};

mod plugins;
mod setup;

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

/// Plugin settings are stored as strings; accept a string or a bare number.
fn setting_u64(json: &Option<serde_json::Value>, key: &str) -> Option<u64> {
    let v = json.as_ref()?.get(key)?;
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

/// Truthy plugin-settings flag: a JSON bool or a "1"/"true"/"yes"/"on" string.
fn setting_flag(json: &Option<serde_json::Value>, key: &str) -> bool {
    json.as_ref().and_then(|j| j.get(key)).is_some_and(|v| {
        v.as_bool().unwrap_or_else(|| {
            matches!(
                v.as_str().map(|s| s.trim().to_ascii_lowercase()).as_deref(),
                Some("1" | "true" | "yes" | "on")
            )
        })
    })
}

/// Parse an env var, treating unset/unparseable as `None`.
fn env_parsed<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok().and_then(|s| s.trim().parse().ok())
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
        settings.effective_vfs_mount_path().to_string(),
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

    let (link_tx, mut link_rx) = tokio::sync::mpsc::channel(64);

    let vfs_mount_path = settings.effective_vfs_mount_path().to_string();
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

    if let Some(streamer) = usenet_streamer.clone() {
        let repair_queue = job_queue.clone();
        let sample_percent = setting_u64(&usenet_settings_json, "availabilitysamplepercent")
            .map(|n| n as usize)
            .filter(|&n| (1..=100).contains(&n))
            .unwrap_or(riven_usenet::DEFAULT_AVAILABILITY_SAMPLE_PERCENT);
        let interval_secs = env_parsed::<u64>("RIVEN_USENET_HEALTH_SCAN_INTERVAL_SECS")
            .filter(|&n| n > 0)
            .unwrap_or(300);
        let batch = env_parsed::<i64>("RIVEN_USENET_HEALTH_SCAN_BATCH")
            .filter(|&n| n > 0)
            .unwrap_or(5);
        let auto_repair_forced = std::env::var("RIVEN_USENET_AUTO_REPAIR")
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        let repair_base_secs = env_parsed::<u64>("RIVEN_USENET_REPAIR_BASE_INTERVAL_SECS")
            .filter(|&n| n > 0)
            .unwrap_or(3600);
        let repair_max_cooldown_secs = env_parsed::<u64>("RIVEN_USENET_REPAIR_MAX_COOLDOWN_SECS")
            .filter(|&n| n > 0)
            .unwrap_or(86_400);
        let scanner_registry = registry.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;

                match riven_db::repo::prune_orphaned_usenet_health().await {
                    Ok(n) if n > 0 => {
                        tracing::debug!(removed = n, "usenet health: pruned orphaned rows")
                    }
                    Err(error) => tracing::debug!(%error, "usenet health: prune failed"),
                    _ => {}
                }

                let usenet_cfg = scanner_registry.get_plugin_settings_json("usenet").await;
                let auto_repair = auto_repair_forced || setting_flag(&usenet_cfg, "autorepair");
                let repair_max_retries = setting_u64(&usenet_cfg, "repairmaxretries")
                    .filter(|&n| n > 0)
                    .map(|n| n as i32)
                    .unwrap_or(3);
                let check_all_segments = setting_flag(&usenet_cfg, "checkallsegments");
                let effective_sample_percent = if check_all_segments {
                    100
                } else {
                    sample_percent
                };

                let due = match riven_db::repo::usenet_files_due_for_check(batch).await {
                    Ok(due) => due,
                    Err(error) => {
                        tracing::debug!(%error, "usenet health: due-for-check query failed");
                        continue;
                    }
                };
                for file in due {
                    let file_index = usize::try_from(file.file_index).unwrap_or(0);
                    let (status, total, sampled, missing, errors) = match streamer
                        .scan_availability(&file.info_hash, file_index, effective_sample_percent)
                        .await
                    {
                        Ok(scan) => (
                            scan.status(),
                            scan.total_segments as i32,
                            scan.sampled_segments as i32,
                            scan.missing_segments as i32,
                            scan.error_segments as i32,
                        ),
                        Err(riven_usenet::StreamerError::NotIngested(_)) => {
                            ("not_ingested", 0, 0, 0, 0)
                        }
                        Err(error) => {
                            tracing::debug!(info_hash = %file.info_hash, %error, "usenet health: scan failed");
                            ("unknown", 0, 0, 0, 0)
                        }
                    };
                    if let Err(error) = riven_db::repo::upsert_usenet_file_health(
                        riven_db::repo::UsenetHealthUpdate {
                            info_hash: &file.info_hash,
                            file_index: file.file_index,
                            media_item_id: file.media_item_id,
                            status,
                            total_segments: total,
                            sampled_segments: sampled,
                            missing_segments: missing,
                            error_segments: errors,
                        },
                    )
                    .await
                    {
                        tracing::debug!(%error, "usenet health: upsert failed");
                    }

                    if !auto_repair {
                        continue;
                    }
                    match status {
                        "healthy" => {
                            if let Err(error) = riven_db::repo::clear_usenet_repair_state(
                                &file.info_hash,
                                file.file_index,
                            )
                            .await
                            {
                                tracing::debug!(%error, "usenet auto-repair: clear state failed");
                            }
                        }
                        "unhealthy" | "not_ingested" => {
                            let Some(media_item_id) = file.media_item_id else {
                                continue;
                            };
                            match riven_db::repo::usenet_repair_due(
                                &file.info_hash,
                                file.file_index,
                                repair_max_retries,
                            )
                            .await
                            {
                                Ok(Some(attempts)) => {
                                    let shift = u32::try_from(attempts.clamp(0, 16)).unwrap_or(0);
                                    let backoff = repair_base_secs
                                        .saturating_mul(1u64 << shift)
                                        .min(repair_max_cooldown_secs)
                                        as i64;
                                    tracing::info!(
                                        info_hash = %file.info_hash,
                                        attempt = attempts + 1,
                                        max = repair_max_retries,
                                        status,
                                        "usenet auto-repair: re-grabbing"
                                    );
                                    if let Err(error) =
                                        repair_queue.regrab_media_item(media_item_id).await
                                    {
                                        tracing::warn!(%error, "usenet auto-repair: regrab failed");
                                    }
                                    if let Err(error) =
                                        riven_db::repo::record_usenet_repair_attempt(
                                            &file.info_hash,
                                            file.file_index,
                                            backoff,
                                        )
                                        .await
                                    {
                                        tracing::debug!(%error, "usenet auto-repair: record attempt failed");
                                    }
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    tracing::debug!(%error, "usenet auto-repair: due check failed")
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    if usenet_streamer.is_some()
        && let Some(mut dead_rx) = riven_usenet::state::take_dead_segment_receiver()
    {
        let repair_queue = job_queue.clone();
        let reg = registry.clone();
        tokio::spawn(async move {
            while let Some(ev) = dead_rx.recv().await {
                let enabled = setting_flag(
                    &reg.get_plugin_settings_json("usenet").await,
                    "blacklistonreadfailure",
                );
                if !enabled {
                    continue;
                }
                // `entry_type` is a Postgres enum; compare against its text form
                // so the literal binds cleanly, and read back only the bigint id.
                let media_item_id: Option<i64> = match riven_db::orm()
                    .query_one(sea_orm::Statement::from_sql_and_values(
                        sea_orm::DbBackend::Postgres,
                        "SELECT media_item_id FROM filesystem_entries \
                         WHERE usenet_info_hash = $1 AND usenet_file_index = $2 \
                           AND entry_type::text = 'media' LIMIT 1",
                        [ev.info_hash.clone().into(), (ev.file_index as i32).into()],
                    ))
                    .await
                {
                    Ok(Some(row)) => row.try_get::<Option<i64>>("", "media_item_id").unwrap_or(None),
                    Ok(None) => None,
                    Err(error) => {
                        tracing::debug!(%error, "read-time repair: media entry lookup failed");
                        None
                    }
                };
                let Some(media_item_id) = media_item_id else {
                    tracing::debug!(
                        info_hash = %ev.info_hash,
                        file_index = ev.file_index,
                        "read-time repair: no media entry for dead stream; skipping"
                    );
                    continue;
                };
                tracing::warn!(
                    info_hash = %ev.info_hash,
                    file_index = ev.file_index,
                    media_item_id,
                    detail = %ev.detail,
                    "read-time repair: dead segment hit during playback; blacklisting release and re-grabbing"
                );
                if let Err(error) = repair_queue.regrab_media_item(media_item_id).await {
                    tracing::warn!(%error, media_item_id, "read-time repair: regrab failed");
                }
            }
        });
    }

    if let Some(streamer) = usenet_streamer.clone() {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last: std::collections::HashMap<String, (u64, u64)> =
                std::collections::HashMap::new();
            loop {
                tick.tick().await;
                for t in streamer.pool().traffic_snapshot() {
                    let (last_bytes, last_articles) = last.get(&t.host).copied().unwrap_or((0, 0));
                    let bytes_delta = t.bytes_downloaded.saturating_sub(last_bytes);
                    let articles_delta = t.articles_downloaded.saturating_sub(last_articles);
                    if (bytes_delta > 0 || articles_delta > 0)
                        && let Err(error) = riven_db::repo::add_provider_traffic(
                            &t.host,
                            bytes_delta as i64,
                            articles_delta as i64,
                        )
                        .await
                    {
                        tracing::debug!(%error, host = %t.host, "usenet traffic flush failed");
                        continue;
                    }
                    last.insert(t.host, (t.bytes_downloaded, t.articles_downloaded));
                }
            }
        });
    }

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

    let monitor_task = tokio::spawn({
        let jq = job_queue.clone();
        let cancel = cancel.clone();
        async move {
            let mut redis_conn = jq.redis.clone();
            let queues = jq.queue_names();
            const MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(60);
            const RESTART_BACKOFF: Duration = Duration::from_secs(5);
            while !cancel.is_cancelled() {
                let maintenance = async {
                    riven_queue::clear_worker_registrations(&mut redis_conn, &queues).await;
                    riven_queue::purge_orphaned_worker_sets(&mut redis_conn, &queues).await;
                    riven_queue::purge_orphaned_active_jobs(&mut redis_conn, &queues).await;
                    riven_queue::purge_stale_dedup_keys(&mut redis_conn).await;
                };
                if tokio::time::timeout(MAINTENANCE_TIMEOUT, maintenance)
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        "pre-start Redis maintenance timed out; starting workers anyway"
                    );
                }

                let monitor_handle = tokio::spawn({
                    let jq = jq.clone();
                    async move {
                        riven_queue::start_workers(jq, usenet_download_workers)
                            .run()
                            .await
                    }
                });
                tokio::pin!(monitor_handle);
                let result = tokio::select! {
                    res = &mut monitor_handle => res,
                    _ = cancel.cancelled() => {
                        monitor_handle.abort();
                        break;
                    }
                };
                match result {
                    Ok(Ok(())) => tracing::warn!("apalis monitor exited, restarting"),
                    Ok(Err(e)) => tracing::error!(error = %e, "apalis monitor error, restarting"),
                    Err(e) if e.is_panic() => {
                        tracing::error!("apalis monitor panicked, restarting")
                    }
                    Err(e) => tracing::error!(error = ?e, "apalis monitor task failed, restarting"),
                }
                tokio::select! {
                    _ = tokio::time::sleep(RESTART_BACKOFF) => {}
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
        let (gql_res, monitor_res, scheduler_res) =
            tokio::join!(gql_handle, monitor_task, scheduler_task);
        if let Err(e) = gql_res {
            tracing::error!(error = ?e, "gql task ended with error during drain");
        }
        if let Err(e) = monitor_res {
            tracing::error!(error = ?e, "monitor task ended with error during drain");
        }
        if let Err(e) = scheduler_res {
            tracing::error!(error = ?e, "scheduler task ended with error during drain");
        }
    };
    if tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .is_err()
    {
        tracing::warn!("drain timed out after 30s; proceeding to unmount");
    }

    vfs_mount_manager.unmount().await;
    observability.shutdown();

    tracing::info!("riven shutdown complete");
    Ok(())
}
