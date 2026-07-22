use std::sync::Arc;

use riven_core::plugin::PluginRegistry;
use riven_queue::JobQueue;
use sea_orm::ConnectionTrait;

/// Plugin settings are stored as strings; accept a string or a bare number.
pub(crate) fn setting_u64(json: &Option<serde_json::Value>, key: &str) -> Option<u64> {
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

pub(crate) fn spawn_background_tasks(
    usenet_streamer: Option<riven_usenet::UsenetStreamer>,
    usenet_settings_json: Option<serde_json::Value>,
    job_queue: Arc<JobQueue>,
    registry: Arc<PluginRegistry>,
) {
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
                    Ok(Some(row)) => row
                        .try_get::<Option<i64>>("", "media_item_id")
                        .unwrap_or(None),
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
}
