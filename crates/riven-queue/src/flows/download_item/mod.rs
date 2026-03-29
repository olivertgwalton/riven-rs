mod helpers;
mod persist;

use std::collections::HashMap;
use std::time::Instant;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::entities::Stream;
use riven_db::repo;
use riven_rank::{ParsedData, RankSettings};

use crate::{DownloadJob, JobQueue};
use helpers::load_item_or_err;
use persist::{persist_episode, persist_movie, persist_season};
use super::load_active_profiles;

/// Run the download item flow.
pub async fn run(id: i64, _job: &DownloadJob, queue: &JobQueue) {
    let start_time = Instant::now();
    tracing::debug!(id, "running download flow");

    let item = match load_item_or_err(id, queue, "media item not found for download").await {
        Some(item) => item,
        None => return,
    };

    // Load enabled profiles (empty = single-version mode).
    let version_profiles: Vec<(String, RankSettings)> =
        load_active_profiles(&queue.db_pool).await;
    let multi_version = !version_profiles.is_empty();

    // Early bail.
    if item.state == MediaItemState::Completed {
        if !multi_version {
            tracing::debug!(id, "item already completed, skipping duplicate download");
            return;
        }
        let done = fetch_done_profiles(queue, id, item.item_type).await;
        if version_profiles.iter().all(|(name, _)| done.contains(name)) {
            tracing::debug!(id, "all configured profile versions present, skipping");
            return;
        }
    }

    // Fetch ALL non-blacklisted streams for the batch cache-check.
    let all_streams = match repo::get_non_blacklisted_streams(&queue.db_pool, id).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(id, error = %e, "failed to fetch streams for download");
            return;
        }
    };

    if all_streams.is_empty() {
        tracing::debug!(id, "no streams available for download");
        queue
            .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                id,
                title: item.title.clone(),
                item_type: item.item_type,
            })
            .await;
        queue.fan_out_download(id).await;
        return;
    }

    // Batch cache-check all ranked streams.
    let hashes: Vec<String> = all_streams.iter().map(|s| s.info_hash.clone()).collect();
    let cache_event = RivenEvent::MediaItemDownloadCacheCheckRequested { hashes };
    let cache_results = queue.registry.dispatch(&cache_event).await;

    let mut cached_info: HashMap<String, u64> = HashMap::new();
    for (_, result) in cache_results {
        if let Ok(HookResponse::CacheCheck(results)) = result {
            for r in results {
                if matches!(
                    r.status,
                    riven_core::types::TorrentStatus::Cached
                        | riven_core::types::TorrentStatus::Downloaded
                ) {
                    let size_sum = r.files.iter().map(|f| f.size).sum();
                    cached_info.insert(r.hash.to_lowercase(), size_sum);
                }
            }
        }
    }

    let config = queue.downloader_config.read().await;
    let max_size_bytes: Option<u64> = match item.item_type {
        MediaItemType::Movie => config
            .maximum_average_bitrate_movies
            .zip(item.runtime)
            .map(|(mbps, rt)| riven_core::downloader::DownloaderConfig::threshold_bytes(mbps, rt)),
        MediaItemType::Episode => config
            .maximum_average_bitrate_episodes
            .zip(item.runtime)
            .map(|(mbps, rt)| riven_core::downloader::DownloaderConfig::threshold_bytes(mbps, rt)),
        _ => None,
    };
    let min_size_bytes: Option<u64> = match item.item_type {
        MediaItemType::Movie => config
            .minimum_average_bitrate_movies
            .zip(item.runtime)
            .map(|(mbps, rt)| riven_core::downloader::DownloaderConfig::threshold_bytes(mbps, rt)),
        MediaItemType::Episode => config
            .minimum_average_bitrate_episodes
            .zip(item.runtime)
            .map(|(mbps, rt)| riven_core::downloader::DownloaderConfig::threshold_bytes(mbps, rt)),
        _ => None,
    };
    drop(config);

    if multi_version {
        run_multi_version(
            id, &item, queue, start_time,
            &version_profiles, &all_streams, &cached_info,
            max_size_bytes, min_size_bytes,
        )
        .await;
    } else {
        run_single_version(
            id, &item, queue, start_time,
            &all_streams, &cached_info,
            max_size_bytes, min_size_bytes,
        )
        .await;
    }
}

// ── Multi-version flow ────────────────────────────────────────────────────────

async fn run_multi_version(
    id: i64,
    item: &riven_db::entities::MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    version_profiles: &[(String, RankSettings)],
    all_streams: &[Stream],
    cached_info: &HashMap<String, u64>,
    max_size_bytes: Option<u64>,
    min_size_bytes: Option<u64>,
) {
    let downloaded_profiles = fetch_done_profiles(queue, id, item.item_type).await;

    let mut any_success = false;

    for (profile_name, profile_settings) in version_profiles {
        if downloaded_profiles.contains(profile_name) {
            tracing::debug!(id, profile = profile_name, "profile version already present, skipping");
            continue;
        }

        let path_tag: Option<&str> = Some(profile_name.as_str());

        // Pick the best cached stream for this profile from the already-fetched pool.
        let stream = match pick_best_for_profile(
            all_streams, profile_settings, cached_info, max_size_bytes, min_size_bytes,
        ) {
            Some(s) => s,
            None => {
                tracing::debug!(id, profile = profile_name, "no cached stream found for profile");
                continue;
            }
        };

        let success = attempt_download(
            id, item, queue, stream, path_tag, Some(profile_name.as_str()), start_time,
        )
        .await;

        if success {
            any_success = true;
            tracing::debug!(id, profile = profile_name, "profile version downloaded");
        }
    }

    if !any_success {
        queue.fan_out_download(id).await;
        return;
    }

    if let Err(e) = repo::refresh_state_cascade(&queue.db_pool, item).await {
        tracing::error!(error = %e, "failed to refresh state after multi-version download");
    }

    // Re-queue if any profiles are still missing.
    let done = fetch_done_profiles(queue, id, item.item_type).await;
    if !version_profiles.iter().all(|(name, _)| done.contains(name)) {
        tracing::debug!(id, "some profile versions still missing — re-queuing");
        queue.fan_out_download(id).await;
    }

    let duration = start_time.elapsed();
    queue
        .notify(RivenEvent::MediaItemDownloadSuccess {
            id,
            title: item.title.clone(),
            full_title: item.full_title.clone(),
            item_type: item.item_type,
            year: item.year,
            imdb_id: item.imdb_id.clone(),
            tmdb_id: item.tmdb_id.clone(),
            poster_path: item.poster_path.clone(),
            plugin_name: String::new(),
            provider: None,
            duration_seconds: duration.as_secs_f64(),
        })
        .await;
    tracing::debug!(id, duration_secs = duration.as_secs_f64(), "multi-version download flow completed");
}

/// Select the best stream from `candidates` that is cached, passes bitrate
/// checks, and passes the profile's full fetch pipeline (resolution, quality,
/// HDR, audio, codec, etc.). Streams are then scored using the profile's rank
/// settings — the same pipeline used during scraping — so each profile picks
/// a genuinely distinct quality tier.
fn pick_best_for_profile<'a>(
    candidates: &'a [Stream],
    profile: &RankSettings,
    cached_info: &HashMap<String, u64>,
    max_size: Option<u64>,
    min_size: Option<u64>,
) -> Option<&'a Stream> {
    let model = riven_rank::RankingModel::default();

    let mut scored: Vec<(&Stream, i64)> = candidates
        .iter()
        .filter_map(|s| {
            // Must be cached and within bitrate limits.
            let size = cached_info.get(&s.info_hash.to_lowercase())?;
            if max_size.is_some_and(|max| *size > max) {
                return None;
            }
            if min_size.is_some_and(|min| *size < min) {
                return None;
            }

            // Deserialise stored parsed data so we can run the profile checks.
            let parsed: ParsedData = s
                .parsed_data
                .as_ref()
                .and_then(|v| serde_json::from_value(v.clone()).ok())?;

            // Run the same fetch checks the scraper uses (resolution, quality,
            // HDR, audio, codec, extras). Streams that fail are not eligible.
            let (fetch, _) = riven_rank::rank::check_fetch(&parsed, profile);
            if !fetch {
                return None;
            }

            // Score the stream using this profile's rank settings.
            let (score, _) = riven_rank::rank::scores::get_rank(&parsed, profile, &model);
            Some((s, score))
        })
        .collect();

    // Highest profile-specific score wins; break ties with resolution rank.
    scored.sort_by(|(a, sa), (b, sb)| {
        sb.cmp(sa).then_with(|| {
            let ra = profile.resolution_ranks.rank_for(stream_resolution(a));
            let rb = profile.resolution_ranks.rank_for(stream_resolution(b));
            rb.cmp(&ra)
        })
    });

    scored.into_iter().next().map(|(s, _)| s)
}

fn stream_resolution(s: &Stream) -> &str {
    s.parsed_data
        .as_ref()
        .and_then(|pd| pd.get("resolution"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
}

/// Fetch downloaded profile names, using the season-aware query for Season items
/// (where filesystem entries are stored on episode IDs, not the season itself).
async fn fetch_done_profiles(queue: &JobQueue, id: i64, item_type: MediaItemType) -> Vec<String> {
    let result = if item_type == MediaItemType::Season {
        repo::get_downloaded_profile_names_for_season(&queue.db_pool, id).await
    } else {
        repo::get_downloaded_profile_names(&queue.db_pool, id).await
    };
    result.unwrap_or_default()
}

// ── Single-version flow ───────────────────────────────────────────────────────

async fn run_single_version(
    id: i64,
    item: &riven_db::entities::MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    streams: &[Stream],
    cached_info: &HashMap<String, u64>,
    max_size_bytes: Option<u64>,
    min_size_bytes: Option<u64>,
) {
    let valid_streams: Vec<&Stream> = streams
        .iter()
        .filter(|s| {
            let Some(size) = cached_info.get(&s.info_hash.to_lowercase()) else {
                return false;
            };
            tracing::debug!(id, info_hash = %s.info_hash, size, "stream is cached");
            if max_size_bytes.is_some_and(|max| *size > max) {
                tracing::debug!(id, info_hash = %s.info_hash, size, "stream filtered: exceeds max bitrate");
                return false;
            }
            if min_size_bytes.is_some_and(|min| *size < min) {
                tracing::debug!(id, info_hash = %s.info_hash, size, "stream filtered: below min bitrate");
                return false;
            }
            true
        })
        .collect();

    if valid_streams.is_empty() {
        tracing::debug!(id, "no cached+valid streams found in this pass");
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "no cached stream found within bitrate limits".into(),
            })
            .await;
        queue.fan_out_download(id).await;
        return;
    }

    for stream in valid_streams {
        let success = attempt_download(id, item, queue, stream, None, None, start_time).await;
        if !success {
            continue; // stream already blacklisted by attempt_download; try next
        }

        if let Err(e) = repo::refresh_state_cascade(&queue.db_pool, item).await {
            tracing::error!(error = %e, "failed to refresh state after download");
        }

        let duration = start_time.elapsed();
        queue
            .notify(RivenEvent::MediaItemDownloadSuccess {
                id,
                title: item.title.clone(),
                full_title: item.full_title.clone(),
                item_type: item.item_type,
                year: item.year,
                imdb_id: item.imdb_id.clone(),
                tmdb_id: item.tmdb_id.clone(),
                poster_path: item.poster_path.clone(),
                plugin_name: String::new(),
                provider: None,
                duration_seconds: duration.as_secs_f64(),
            })
            .await;
        tracing::debug!(id, duration_secs = duration.as_secs_f64(), "download flow completed");
        return;
    }

    queue.fan_out_download(id).await;
}

// ── Shared download attempt ───────────────────────────────────────────────────

/// Dispatch a download request for `stream`, then call the appropriate persist
/// function.  Returns `true` on success, `false` if the stream was rejected
/// (already blacklisted by the persist layer).
///
/// `path_tag`    – embedded in the VFS filename when `Some` (multi-version).
/// `profile_name` – stored on the filesystem entry for version tracking.
async fn attempt_download(
    id: i64,
    item: &riven_db::entities::MediaItem,
    queue: &JobQueue,
    stream: &Stream,
    path_tag: Option<&str>,
    profile_name: Option<&str>,
    start_time: Instant,
) -> bool {
    let info_hash = &stream.info_hash;
    let stream_id = Some(stream.id);
    let resolution = stream_resolution(stream).to_owned();
    let resolution_ref: Option<&str> = Some(resolution.as_str());

    let magnet = format!("magnet:?xt=urn:btih:{info_hash}");
    let event = RivenEvent::MediaItemDownloadRequested {
        id,
        info_hash: info_hash.clone(),
        magnet,
    };

    let results = queue.registry.dispatch(&event).await;
    let mut download_result: Option<Box<DownloadResult>> = None;

    for (plugin_name, result) in results {
        match result {
            Ok(HookResponse::Download(dl)) => {
                tracing::debug!(plugin = plugin_name, files = dl.files.len(), "download responded");
                download_result = Some(dl);
                break;
            }
            Ok(HookResponse::DownloadStreamUnavailable) => {
                tracing::debug!(plugin = plugin_name, info_hash, "stream unexpectedly not cached");
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(plugin = plugin_name, error = %e, "download hook failed (transient)");
                return false;
            }
        }
    }

    let Some(dl) = download_result else {
        return false;
    };
    let dl = *dl;

    match item.item_type {
        MediaItemType::Movie => {
            persist_movie(item, &dl, info_hash, queue, stream_id, resolution_ref, path_tag, profile_name).await
        }
        MediaItemType::Episode => {
            persist_episode(item, &dl, info_hash, queue, stream_id, resolution_ref, path_tag, profile_name).await
        }
        MediaItemType::Season => {
            persist_season(item, dl, info_hash, queue, start_time, stream_id, path_tag, profile_name).await;
            true // persist_season handles its own state updates
        }
        _ => {
            tracing::warn!(id, item_type = ?item.item_type, "unexpected item type in download flow");
            false
        }
    }
}
