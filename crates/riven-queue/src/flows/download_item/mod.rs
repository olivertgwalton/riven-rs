mod helpers;
mod persist;

use std::time::Instant;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::repo;

use crate::{DownloadJob, JobQueue};
use helpers::load_item_or_err;
use persist::{persist_episode, persist_movie, persist_season};

/// Run the download item flow.
///
/// Mirrors riven-ts `findValidTorrentProcessor`:
/// - Iterates ALL non-blacklisted streams in ranked order (one job = one full pass).
/// - `DownloadStreamUnavailable` (not cached in debrid) → skip without blacklisting.
/// - Genuine failures (wrong files, bitrate) are blacklisted inside `persist_*`.
/// - If all streams fail → fan_out_download (which re-scrapes seasons/shows).
pub async fn run(id: i64, _job: &DownloadJob, queue: &JobQueue) {
    let start_time = Instant::now();
    tracing::debug!(id, "running download flow");

    let item = match load_item_or_err(id, queue, "media item not found for download").await {
        Some(item) => item,
        None => return,
    };

    if item.state == MediaItemState::Completed {
        tracing::debug!(id, "item already completed, skipping duplicate download");
        return;
    }

    // Fetch all non-blacklisted streams in ranked order — mirrors riven-ts iterating
    // `rankedStreams` and skipping `failedInfoHashes`.
    let streams = match repo::get_non_blacklisted_streams(&queue.db_pool, id).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(id, error = %e, "failed to fetch streams for download");
            return;
        }
    };

    if streams.is_empty() {
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

    // Batch cache-check ALL ranked streams first. This provides the file sizes
    // for all available streams, allowing us to perform bitrate filtering for
    // all candidates in one single pass before attempting any downloads.
    let hashes: Vec<String> = streams.iter().map(|s| s.info_hash.clone()).collect();
    let cache_event = RivenEvent::MediaItemDownloadCacheCheckRequested { hashes };
    let cache_results = queue.registry.dispatch(&cache_event).await;

    let mut cached_info: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
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

    // Filter streams to only those that are confirmed cached and pass bitrate checks.
    let valid_streams: Vec<_> = streams
        .into_iter()
        .filter(|s| {
            if let Some(size) = cached_info.get(&s.info_hash.to_lowercase()) {
                tracing::debug!(id, info_hash = %s.info_hash, size, "stream is cached");
                if max_size_bytes.is_some_and(|max| *size > max) {
                    tracing::debug!(
                        id,
                        info_hash = s.info_hash,
                        size,
                        "stream filtered: cached size exceeds max bitrate"
                    );
                    return false;
                }
                if min_size_bytes.is_some_and(|min| *size < min) {
                    tracing::debug!(
                        info_hash = s.info_hash,
                        size,
                        "stream filtered: cached size below min bitrate"
                    );
                    return false;
                }
                true
            } else {
                false
            }
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
        let info_hash = &stream.info_hash;
        let magnet = format!("magnet:?xt=urn:btih:{}", info_hash);

        let event = RivenEvent::MediaItemDownloadRequested {
            id,
            info_hash: info_hash.clone(),
            magnet: magnet.clone(),
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
                    // This shouldn't really happen since we just cache-checked, but handles race conditions.
                    tracing::debug!(plugin = plugin_name, info_hash, "stream unexpectedly not cached");
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(plugin = plugin_name, error = %e, "download hook failed (transient)");
                    return;
                }
            }
        }

        if download_result.is_none() {
            continue;
        }

        let dl = *download_result.unwrap();

        let success = match item.item_type {
            MediaItemType::Movie => persist_movie(&item, &dl, info_hash, queue).await,
            MediaItemType::Episode => persist_episode(&item, &dl, info_hash, queue).await,
            MediaItemType::Season => {
                persist_season(&item, dl, info_hash, queue, start_time).await;
                return; // persist_season handles state updates internally
            }
            _ => {
                tracing::warn!(id, item_type = ?item.item_type, "unexpected item type in download flow");
                return;
            }
        };

        if !success {
            // persist_* already blacklisted the stream and emitted PartialSuccess.
            // Try the next ranked stream.
            continue;
        }

        // Success — update state and notify.
        if let Err(e) =
            repo::update_media_item_state(&queue.db_pool, id, MediaItemState::Completed).await
        {
            tracing::error!(error = %e, "failed to update state after download");
        }

        if item.item_type == MediaItemType::Episode {
            if let Err(e) = repo::cascade_state_update(&queue.db_pool, &item).await {
                tracing::error!(error = %e, "failed to cascade state update");
            }
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
                plugin_name: dl.plugin_name,
                provider: dl.provider,
                duration_seconds: duration.as_secs_f64(),
            })
            .await;

        tracing::debug!(id, duration_secs = duration.as_secs_f64(), "download flow completed");
        return;
    }

    // This part is only reached if all "valid" streams failed during persistent (e.g. wrong files)
    queue.fan_out_download(id).await;
}
