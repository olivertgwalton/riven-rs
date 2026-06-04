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

const USER_AGENT: &str = concat!("riven-rs/", env!("CARGO_PKG_VERSION"));

/// Single shared HTTP client for every outbound request in the process —
/// plugins, the VFS streaming path, and ad-hoc fetches all share one
/// connection pool, TLS session cache, and DNS cache.
///
/// Mirrors the riven-ts design (single global `undici.Agent` set via
/// `setGlobalDispatcher`). Splitting plugins from streaming as we previously
/// did gave two independent pools against the same debrid hosts: every
/// playback URL refresh paid a cold TCP+TLS dial in one pool while the
/// other had warm connections sitting idle.
///
/// Keep-alive matches the TS `keepAliveMaxTimeout` (60 s) so the bursty
/// Plex scan pattern — "probe file, wait ~1 s, probe next file" — keeps
/// reusing connections instead of redialing for every file. Per-host idle
/// capacity is bumped to 32 to absorb scans of large libraries fanning out
/// across many files in parallel.
fn build_http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .user_agent(USER_AGENT)
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
    let db_pool = riven_db::connect(&settings.database_url).await?;
    riven_db::run_migrations(&db_pool).await?;

    if let Ok(Some(general_settings)) = riven_db::repo::get_setting(&db_pool, "general").await {
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
        sqlx::query("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .execute(&db_pool)
            .await?;
        riven_db::run_migrations(&db_pool).await?;
    }

    // Shared with every plugin (dedup guards, dashboard, …). Built with the
    // same bounded-timeout connection as the job-queue connections so no Redis
    // command can hang indefinitely after a connection blip.
    let redis_conn = riven_queue::connect_managed(settings.redis_url.as_str()).await?;
    tracing::info!("redis connection established");

    let reqwest_client = build_http_client()?;
    let http_client = riven_core::http::HttpClient::new(reqwest_client.clone());
    let stream_http_client = reqwest_client;

    let registry = setup::register_plugins(
        http_client.clone(),
        db_pool.clone(),
        redis_conn,
        settings.effective_vfs_mount_path().to_string(),
        &settings,
    )
    .await;

    // If the `usenet` plugin is configured with NNTP credentials, build a
    // streamer the VFS reads through in-process (as a `LocalByteSource`).
    // Failure to build is non-fatal — Usenet streaming is just disabled.
    // Concurrent usenet download workers. Kept small (default 4) rather than
    // scaled to the pool: on usenet, total throughput is line-bound, so many
    // concurrent ingests don't drain a backlog faster — they split the pipe
    // and starve playback/scanning of bandwidth. Leaving most connections idle
    // keeps streaming fast (altmount keeps imports at ~2 workers for this
    // reason). Overridable via the `maxdownloadworkers` usenet setting.
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
            // Settings are stored as strings; accept a string or bare number.
            let configured = usenet_settings_json
                .as_ref()
                .and_then(|v| v.get("maxdownloadworkers"))
                .and_then(|v| {
                    v.as_u64()
                        .map(|n| n as usize)
                        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<usize>().ok()))
                })
                .filter(|&n| n > 0);
            usenet_download_workers =
                Some(configured.unwrap_or(riven_usenet::DEFAULT_DOWNLOAD_WORKERS));
            // `shared` (not `new`) so playback, ingest, and the health-check
            // task all use the same `NntpPool` — the user's configured
            // `max_connections` is then the true ceiling against the
            // provider rather than being multiplied by the number of
            // construction sites.
            Some(riven_usenet::UsenetStreamer::shared(cfg, db_pool.clone()))
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
    // Usenet-backed VFS reads go in-process through the streamer (the old
    // loopback HTTP route has been removed).
    let usenet_local_source: Option<Arc<dyn riven_core::local_source::LocalByteSource>> =
        usenet_streamer
            .clone()
            .map(|s| Arc::new(s) as Arc<dyn riven_core::local_source::LocalByteSource>);
    let vfs_mount_manager = Arc::new(riven_api::vfs_mount::VfsMountManager::new(
        &vfs_mount_path,
        job_queue.vfs_layout.clone(),
        job_queue.filesystem_settings_revision.clone(),
        db_pool.clone(),
        stream_http_client.clone(),
        link_tx.clone(),
        settings.vfs_cache_max_size_mb,
        usenet_local_source,
    )?);

    // Background usenet availability scanner: periodically STAT-samples each
    // usenet-backed file's segments and records per-title health, powering the
    // dashboard's "Usenet Health" view. Low-priority STATs in small batches so
    // it never competes with live playback. Tunable via
    // `RIVEN_USENET_HEALTH_SCAN_INTERVAL_SECS` / `_BATCH`.
    if let Some(streamer) = usenet_streamer.clone() {
        let db = db_pool.clone();
        let repair_queue = job_queue.clone();
        let sample_percent = usenet_settings_json
            .as_ref()
            .and_then(|v| v.get("availabilitysamplepercent"))
            .and_then(|v| {
                v.as_u64()
                    .map(|n| n as usize)
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<usize>().ok()))
            })
            .filter(|&n| (1..=100).contains(&n))
            .unwrap_or(riven_usenet::DEFAULT_AVAILABILITY_SAMPLE_PERCENT);
        let interval_secs = std::env::var("RIVEN_USENET_HEALTH_SCAN_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(300);
        let batch = std::env::var("RIVEN_USENET_HEALTH_SCAN_BATCH")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(5);
        // altmount-style auto-repair. Enabled via the usenet plugin's
        // "Auto-Repair" UI toggle (read per-tick below so it takes effect
        // without a restart); `RIVEN_USENET_AUTO_REPAIR` is a force-on override
        // for headless setups. Backoff timing is env-tunable (sensible defaults).
        let auto_repair_forced = std::env::var("RIVEN_USENET_AUTO_REPAIR")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        let repair_base_secs = std::env::var("RIVEN_USENET_REPAIR_BASE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(3600);
        let repair_max_cooldown_secs = std::env::var("RIVEN_USENET_REPAIR_MAX_COOLDOWN_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(86_400);
        let scanner_registry = registry.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;

                // Drop health rows for usenet files that no longer exist (e.g.
                // a re-grab moved the title onto a different/non-usenet release),
                // so stale "not ingested"/"missing data" rows don't linger.
                match riven_db::repo::prune_orphaned_usenet_health(&db).await {
                    Ok(n) if n > 0 => {
                        tracing::debug!(removed = n, "usenet health: pruned orphaned rows")
                    }
                    Err(error) => tracing::debug!(%error, "usenet health: prune failed"),
                    _ => {}
                }

                // Read the auto-repair toggle + retry cap from settings each tick
                // so the UI toggle is live. Env force-on wins when set.
                let usenet_cfg = scanner_registry.get_plugin_settings_json("usenet").await;
                let auto_repair = auto_repair_forced
                    || usenet_cfg
                        .as_ref()
                        .and_then(|v| v.get("autorepair"))
                        .map(|v| {
                            v.as_bool().unwrap_or_else(|| {
                                matches!(
                                    v.as_str().map(|s| s.trim().to_ascii_lowercase()).as_deref(),
                                    Some("1" | "true" | "yes" | "on")
                                )
                            })
                        })
                        .unwrap_or(false);
                let repair_max_retries = usenet_cfg
                    .as_ref()
                    .and_then(|v| v.get("repairmaxretries"))
                    .and_then(|v| {
                        v.as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
                    })
                    .filter(|&n| n > 0)
                    .map(|n| n as i32)
                    .unwrap_or(3);

                let due = match riven_db::repo::usenet_files_due_for_check(&db, batch).await {
                    Ok(due) => due,
                    Err(error) => {
                        tracing::debug!(%error, "usenet health: due-for-check query failed");
                        continue;
                    }
                };
                for file in due {
                    let file_index = usize::try_from(file.file_index).unwrap_or(0);
                    let (status, total, sampled, missing, errors) = match streamer
                        .scan_availability(&file.info_hash, file_index, sample_percent)
                        .await
                    {
                        Ok(scan) => (
                            scan.status(),
                            scan.total_segments as i32,
                            scan.sampled_segments as i32,
                            scan.missing_segments as i32,
                            scan.error_segments as i32,
                        ),
                        // No segment map for this release — it was never
                        // ingested (or the meta is gone), so it isn't
                        // streamable. Distinct from "couldn't reach provider".
                        Err(riven_usenet::StreamerError::NotIngested(_)) => {
                            ("not_ingested", 0, 0, 0, 0)
                        }
                        Err(error) => {
                            tracing::debug!(info_hash = %file.info_hash, %error, "usenet health: scan failed");
                            ("unknown", 0, 0, 0, 0)
                        }
                    };
                    if let Err(error) = riven_db::repo::upsert_usenet_file_health(
                        &db,
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
                        // Healthy again → clear repair bookkeeping (resolve).
                        "healthy" => {
                            if let Err(error) = riven_db::repo::clear_usenet_repair_state(
                                &db,
                                &file.info_hash,
                                file.file_index,
                            )
                            .await
                            {
                                tracing::debug!(%error, "usenet auto-repair: clear state failed");
                            }
                        }
                        // Confirmed broken / never ingested → auto re-grab if due
                        // and under the retry cap (exponential backoff between).
                        "unhealthy" | "not_ingested" => {
                            let Some(media_item_id) = file.media_item_id else {
                                continue;
                            };
                            match riven_db::repo::usenet_repair_due(
                                &db,
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
                                    if let Err(error) = riven_db::repo::record_usenet_repair_attempt(
                                        &db,
                                        &file.info_hash,
                                        file.file_index,
                                        backoff,
                                    )
                                    .await
                                    {
                                        tracing::debug!(%error, "usenet auto-repair: record attempt failed");
                                    }
                                }
                                // Not due / retries exhausted.
                                Ok(None) => {}
                                Err(error) => {
                                    tracing::debug!(%error, "usenet auto-repair: due check failed")
                                }
                            }
                        }
                        // "unknown"/unverified is transient — don't auto-repair.
                        _ => {}
                    }
                }
            }
        });
    }

    // Persist per-provider download traffic. The pool counts encoded wire
    // bytes + article bodies per provider in memory; this flusher writes the
    // deltas to the DB every minute so lifetime totals survive restarts and
    // daily buckets accumulate for the usage-trend chart.
    if let Some(streamer) = usenet_streamer.clone() {
        let db = db_pool.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last: std::collections::HashMap<String, (u64, u64)> =
                std::collections::HashMap::new();
            loop {
                tick.tick().await;
                for t in streamer.pool().traffic_snapshot() {
                    let (last_bytes, last_articles) =
                        last.get(&t.host).copied().unwrap_or((0, 0));
                    let bytes_delta = t.bytes_downloaded.saturating_sub(last_bytes);
                    let articles_delta = t.articles_downloaded.saturating_sub(last_articles);
                    if (bytes_delta > 0 || articles_delta > 0)
                        && let Err(error) = riven_db::repo::add_provider_traffic(
                            &db,
                            &t.host,
                            bytes_delta as i64,
                            articles_delta as i64,
                        )
                        .await
                    {
                        tracing::debug!(%error, host = %t.host, "usenet traffic flush failed");
                        // Don't advance the baseline — retry the delta next tick.
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
            // Matches riven-ts open.ts: refresh the stream URL on demand and
            // hand back whatever the plugin returns. On failure we send None,
            // which surfaces as ENOENT to the VFS caller. We intentionally do
            // NOT blacklist the stream, delete the entry, or regress item
            // state — playback errors are transient and shouldn't tear down
            // a Completed item (a single dead URL on one episode of a
            // season pack would otherwise orphan that episode with no
            // fallback stream of its own).
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
        let cancel = cancel.clone();
        async move {
            if let Err(e) = riven_api::start_server(riven_api::StartServerConfig {
                host: gql_host,
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
            // Hard ceiling on the pre-start maintenance phase. The Redis
            // connection already has a per-command `response_timeout`, but a
            // blip can stall each of many per-queue commands; without an outer
            // bound the restart loop could still sit for minutes before
            // spawning workers. If maintenance overruns we proceed to
            // `start_workers` anyway and let the next iteration retry — a
            // wedged restart loop (workers never come back until the process
            // is restarted) is far worse than skipping one orphan purge.
            const MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(60);
            // Backoff between restart attempts so a connection that is fully
            // down (every `start_workers().run()` returning immediately) cannot
            // turn into a hot loop hammering Redis.
            const RESTART_BACKOFF: Duration = Duration::from_secs(5);
            while !cancel.is_cancelled() {
                // Unconditionally clears worker heartbeats so re-registration is
                // not rejected by `register_worker.lua`'s "still active within
                // threshold" check after a previous run died.
                let maintenance = async {
                    riven_queue::clear_worker_registrations(&mut redis_conn, &queues).await;
                    riven_queue::purge_orphaned_worker_sets(&mut redis_conn, &queues).await;
                    riven_queue::purge_orphaned_active_jobs(&mut redis_conn, &queues).await;
                };
                if tokio::time::timeout(MAINTENANCE_TIMEOUT, maintenance)
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        "pre-start Redis maintenance timed out; starting workers anyway"
                    );
                }

                // `Monitor::run()` resolves as soon as all workers complete (e.g.
                // a Redis blip on the shared `apalis_conn` errors every backend's
                // poll stream in the same tick). `run_with_signal()` would have
                // hung here forever because it `join!`s workers with an external
                // signal that only fires on shutdown.
                let monitor_handle = tokio::spawn({
                    let jq = jq.clone();
                    async move { riven_queue::start_workers(jq, usenet_download_workers).run().await }
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
                    Err(e) if e.is_panic() => tracing::error!("apalis monitor panicked, restarting"),
                    Err(e) => tracing::error!(error = ?e, "apalis monitor task failed, restarting"),
                }
                // Brief pause before re-registering. Guards against a hot
                // restart loop when Redis is down hard and every attempt fails
                // immediately; harmless in the normal (rare) restart case.
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
    if tokio::time::timeout(Duration::from_secs(30), drain).await.is_err() {
        tracing::warn!("drain timed out after 30s; proceeding to unmount");
    }

    vfs_mount_manager.unmount().await;
    observability.shutdown();

    tracing::info!("riven shutdown complete");
    Ok(())
}
