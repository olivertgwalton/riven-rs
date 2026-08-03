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

/// A settings flag whose default is on: only an explicit false turns it off.
///
/// The JSON equivalent of the `settings.get_or("key", "true") != "false"` idiom
/// the plugins use at their own read sites (see `plugin-stremthru`'s
/// `scrapenabled` and `checkdebridcache`). Schema defaults are not written into
/// stored settings, so a default-on flag has to restate its default wherever it
/// is read; [`setting_flag`] would read "absent" as off and silently disable it
/// for every install that has never touched the setting.
fn setting_flag_on_by_default(json: &Option<serde_json::Value>, key: &str) -> bool {
    let Some(value) = json.as_ref().and_then(|j| j.get(key)) else {
        return true;
    };
    if let Some(flag) = value.as_bool() {
        return flag;
    }
    !matches!(
        value
            .as_str()
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref(),
        Some("0" | "false" | "no" | "off")
    )
}

/// Truthy env flag, in the same spelling the settings flags accept.
fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Parse an env var, treating unset/unparseable as `None`.
fn env_parsed<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok().and_then(|s| s.trim().parse().ok())
}

/// Converge stored NZB metadata onto the current format, a few releases at a
/// time.
///
/// Releases posted as one RAR archive containing several media files used to
/// store a full copy of every volume's segment list per file — 46 % of the
/// stored volume data on the library this was measured against, and 12.5× on
/// its worst row. Reading collapses those copies; this rewrites them so the
/// saving reaches disk.
///
/// Incremental and idempotent by design. It rewrites every row it touches, and
/// doing that to a whole library at boot competes with playback for exactly no
/// benefit — a release nobody watches can wait. Largest first, so an instance
/// restarted before it finishes has still dealt with the rows that matter.
///
/// The old format stays readable regardless: this is how a deployment stops
/// *carrying* it, not a licence to delete the reader. Anyone downgrading needs
/// it, and other people's databases are full of rows this has never seen.
fn spawn_meta_compaction(streamer: riven_usenet::UsenetStreamer) {
    /// Releases per tick. Small: each is a read, a re-serialise and a write of
    /// a document that can run to tens of MB.
    const BATCH: u32 = 8;
    /// Long enough that this is background work rather than a second workload.
    const INTERVAL_SECS: u64 = 300;
    /// Let the instance finish starting before touching the database.
    const INITIAL_DELAY_SECS: u64 = 120;

    if env_flag("RIVEN_USENET_DISABLE_META_COMPACTION") {
        tracing::debug!("usenet meta compaction disabled by environment");
        return;
    }

    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(INITIAL_DELAY_SECS)).await;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(INTERVAL_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut total = 0usize;
        let mut reclaimed = 0i64;
        loop {
            tick.tick().await;
            match streamer.compact_outdated_meta(BATCH).await {
                Ok((0, _)) => {
                    if total > 0 {
                        tracing::info!(
                            releases = total,
                            reclaimed_mib = reclaimed / (1024 * 1024),
                            "usenet meta compaction complete"
                        );
                    }
                    return;
                }
                Ok((n, bytes)) => {
                    total += n;
                    reclaimed += bytes;
                    tracing::debug!(
                        releases = n,
                        total,
                        reclaimed_mib = reclaimed / (1024 * 1024),
                        "usenet meta compaction progress"
                    );
                }
                Err(error) => {
                    tracing::warn!(%error, "usenet meta compaction tick failed");
                }
            }
        }
    });
}

pub(crate) fn spawn_background_tasks(
    usenet_streamer: Option<riven_usenet::UsenetStreamer>,
    usenet_settings_json: Option<serde_json::Value>,
    job_queue: Arc<JobQueue>,
    registry: Arc<PluginRegistry>,
) {
    if let Some(streamer) = usenet_streamer.clone() {
        let repair_queue = job_queue.clone();
        // The read path has no plugin context, so this rides on a process-wide
        // flag. Set here at startup and again on each scan tick below, so a
        // change in the UI takes effect without a restart.
        riven_usenet::set_degraded_playback(setting_flag_on_by_default(
            &usenet_settings_json,
            "degradedplayback",
        ));
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
        let auto_repair_forced = env_flag("RIVEN_USENET_AUTO_REPAIR");
        let repair_base_secs = env_parsed::<u64>("RIVEN_USENET_REPAIR_BASE_INTERVAL_SECS")
            .filter(|&n| n > 0)
            .unwrap_or(3600);
        let repair_max_cooldown_secs = env_parsed::<u64>("RIVEN_USENET_REPAIR_MAX_COOLDOWN_SECS")
            .filter(|&n| n > 0)
            .unwrap_or(86_400);
        spawn_meta_compaction(streamer.clone());

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
                riven_usenet::set_degraded_playback(setting_flag_on_by_default(
                    &usenet_cfg,
                    "degradedplayback",
                ));
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
                            tracing::debug!(
                                info_hash = %file.info_hash,
                                file = %file.path,
                                %error,
                                "usenet health: scan failed"
                            );
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
                        tracing::debug!(%error, file = %file.path, "usenet health: upsert failed");
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
                                tracing::debug!(
                                    %error,
                                    file = %file.path,
                                    "usenet auto-repair: clear state failed"
                                );
                            }
                        }
                        "unhealthy" | "not_ingested" => {
                            let Some(media_item_id) = file.media_item_id else {
                                continue;
                            };
                            // Repairing blacklists the release and re-grabs it,
                            // which replaces the file a viewer is reading from.
                            // The health row above is already written, so the
                            // title still shows as unhealthy; only the swap
                            // waits. Checked before `usenet_repair_due` so a
                            // deferral does not burn a repair attempt or
                            // advance the backoff — the next tick retries.
                            if riven_usenet::active_streams().is_streaming(&file.info_hash) {
                                tracing::debug!(
                                    info_hash = %file.info_hash,
                                    file = %file.path,
                                    status,
                                    "usenet auto-repair: release is being streamed; deferring"
                                );
                                continue;
                            }
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
                                        file = %file.path,
                                        attempt = attempts + 1,
                                        max = repair_max_retries,
                                        status,
                                        "usenet auto-repair: re-grabbing"
                                    );
                                    if let Err(error) =
                                        repair_queue.regrab_media_item(media_item_id).await
                                    {
                                        tracing::warn!(
                                            %error,
                                            file = %file.path,
                                            "usenet auto-repair: regrab failed"
                                        );
                                    }
                                    if let Err(error) =
                                        riven_db::repo::record_usenet_repair_attempt(
                                            &file.info_hash,
                                            file.file_index,
                                            backoff,
                                        )
                                        .await
                                    {
                                        tracing::debug!(
                                            %error,
                                            file = %file.path,
                                            "usenet auto-repair: record attempt failed"
                                        );
                                    }
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    tracing::debug!(
                                        %error,
                                        file = %file.path,
                                        "usenet auto-repair: due check failed"
                                    )
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
                    .query_one_raw(sea_orm::Statement::from_sql_and_values(
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
                        file = %ev.filename,
                        file_index = ev.file_index,
                        "read-time repair: no media entry for dead stream; skipping"
                    );
                    continue;
                };
                tracing::warn!(
                    info_hash = %ev.info_hash,
                    file = %ev.filename,
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
                for t in streamer.pool().traffic() {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn json(raw: &str) -> Option<serde_json::Value> {
        Some(serde_json::from_str(raw).unwrap())
    }

    /// The regression this reader exists for: `setting_flag` reads an absent
    /// key as off, which would disable a default-on setting for every install
    /// that has never opened it. Schema defaults are not written into stored
    /// settings, so the default has to live here.
    #[test]
    fn a_default_on_flag_stays_on_until_explicitly_turned_off() {
        assert!(setting_flag_on_by_default(&None, "degradedplayback"));
        assert!(setting_flag_on_by_default(
            &json(r#"{}"#),
            "degradedplayback"
        ));
        assert!(setting_flag_on_by_default(
            &json(r#"{"other": false}"#),
            "degradedplayback"
        ));
        assert!(!setting_flag(&json(r#"{}"#), "degradedplayback"));
    }

    #[test]
    fn a_default_on_flag_accepts_both_json_and_string_spellings() {
        for raw in [r#"{"k": false}"#, r#"{"k": "false"}"#, r#"{"k": "OFF"}"#] {
            assert!(!setting_flag_on_by_default(&json(raw), "k"), "{raw}");
        }
        for raw in [r#"{"k": true}"#, r#"{"k": "true"}"#, r#"{"k": "on"}"#] {
            assert!(setting_flag_on_by_default(&json(raw), "k"), "{raw}");
        }
    }

    #[test]
    fn an_ordinary_flag_stays_off_until_explicitly_turned_on() {
        assert!(!setting_flag(&None, "autorepair"));
        assert!(!setting_flag(
            &json(r#"{"autorepair": false}"#),
            "autorepair"
        ));
        assert!(setting_flag(
            &json(r#"{"autorepair": "yes"}"#),
            "autorepair"
        ));
    }
}
