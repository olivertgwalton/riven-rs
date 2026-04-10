mod candidates;
mod execute;

use std::collections::HashMap;
use std::time::Instant;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;
use riven_rank::RankSettings;

use crate::context::{DownloadHierarchyContext, load_download_hierarchy_context};
use crate::flows::download_item::helpers::load_item_or_err;
use crate::flows::load_active_profiles;
use crate::orchestrator::LibraryOrchestrator;
use crate::{DownloadJob, JobQueue};

use self::candidates::{
    CachedCandidate, build_cached_candidates, find_preferred_candidate, pick_best_for_profile,
};
use self::execute::{DownloadAttemptOutcome, attempt_download};

pub async fn run(id: i64, job: &DownloadJob, queue: &JobQueue) {
    let start_time = Instant::now();
    tracing::debug!(id, "running download flow");

    let item = match load_item_or_err(id, queue, "media item not found for download").await {
        Some(item) => item,
        None => return,
    };

    if !matches!(
        item.state,
        MediaItemState::Scraped | MediaItemState::Ongoing | MediaItemState::PartiallyCompleted
    ) {
        tracing::debug!(id, state = ?item.state, "download requested for non-processable item");
        queue
            .notify(RivenEvent::MediaItemDownloadErrorIncorrectState { id })
            .await;
        return;
    }

    let hierarchy = match item.item_type {
        MediaItemType::Episode | MediaItemType::Season => {
            Some(load_download_hierarchy_context(&queue.db_pool, &item).await)
        }
        _ => None,
    };

    let version_profiles: Vec<(String, RankSettings)> = load_active_profiles(&queue.db_pool).await;
    let multi_version = !version_profiles.is_empty();

    let ranks = queue.resolution_ranks.read().await.clone();
    let all_streams = match repo::get_non_blacklisted_streams(&queue.db_pool, id, &ranks).await {
        Ok(streams) => streams,
        Err(error) => {
            tracing::error!(id, error = %error, "failed to fetch streams for download");
            return;
        }
    };

    if all_streams.is_empty() {
        tracing::debug!(id, "no streams available for download");
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "no streams available for download".into(),
            })
            .await;
        LibraryOrchestrator::new(queue)
            .fan_out_download_failure(id)
            .await;
        return;
    }

    let cached_info = collect_cached_info(queue, &all_streams).await;
    let (max_size_bytes, min_size_bytes) = load_bitrate_limits(queue, &item).await;
    let candidates = build_cached_candidates(
        id,
        &item,
        hierarchy.as_ref(),
        &all_streams,
        &cached_info,
        max_size_bytes,
        min_size_bytes,
    );

    if let Some(preferred_info_hash) = job.preferred_info_hash.as_ref() {
        let _ = run_preferred_stream(
            id,
            &item,
            queue,
            start_time,
            preferred_info_hash,
            &candidates,
            hierarchy.as_ref(),
        )
        .await;
    } else if multi_version {
        let _ = run_multi_version(
            id,
            &item,
            queue,
            start_time,
            &version_profiles,
            &candidates,
            hierarchy.as_ref(),
        )
        .await;
    } else {
        let _ = run_single_version(
            id,
            &item,
            queue,
            start_time,
            &candidates,
            hierarchy.as_ref(),
        )
        .await;
    }
}

async fn collect_cached_info(
    queue: &JobQueue,
    streams: &[Stream],
) -> HashMap<String, Vec<CacheCheckFile>> {
    let query_map: HashMap<String, String> = streams
        .iter()
        .map(|stream| (stream.info_hash.clone(), stream.magnet.clone()))
        .collect();
    let queries = query_map
        .into_iter()
        .map(|(hash, magnet)| CacheCheckQuery { hash, magnet })
        .collect();
    let cache_event = RivenEvent::MediaItemDownloadCacheCheckRequested { queries };
    let cache_results = queue.registry.dispatch(&cache_event).await;

    let mut cached_info: HashMap<String, Vec<CacheCheckFile>> = HashMap::new();
    for (_, result) in cache_results {
        if let Ok(HookResponse::CacheCheck(results)) = result {
            for result in results {
                if matches!(
                    result.status,
                    TorrentStatus::Cached | TorrentStatus::Downloaded
                ) {
                    cached_info.insert(result.hash.to_lowercase(), result.files);
                }
            }
        }
    }
    cached_info
}

async fn load_bitrate_limits(queue: &JobQueue, item: &MediaItem) -> (Option<u64>, Option<u64>) {
    let config = queue.downloader_config.read().await;
    let bitrate_threshold = |movies: Option<u32>, episodes: Option<u32>| {
        let mbps = match item.item_type {
            MediaItemType::Movie => movies,
            MediaItemType::Episode => episodes,
            _ => None,
        };
        mbps.zip(item.runtime)
            .map(|(m, rt)| riven_core::downloader::DownloaderConfig::threshold_bytes(m, rt))
    };

    let max_size_bytes = bitrate_threshold(
        config.maximum_average_bitrate_movies,
        config.maximum_average_bitrate_episodes,
    );
    let min_size_bytes = bitrate_threshold(
        config.minimum_average_bitrate_movies,
        config.minimum_average_bitrate_episodes,
    );
    (max_size_bytes, min_size_bytes)
}

async fn run_preferred_stream(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    preferred_info_hash: &str,
    candidates: &[CachedCandidate<'_>],
    hierarchy: Option<&DownloadHierarchyContext>,
) -> bool {
    let Some(candidate) = find_preferred_candidate(candidates, preferred_info_hash) else {
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "selected stream is not cached, valid, or linked to this item".into(),
            })
            .await;
        return false;
    };

    match attempt_download(
        id,
        item,
        queue,
        candidate.stream,
        None,
        None,
        start_time,
        hierarchy,
    )
    .await
    {
        DownloadAttemptOutcome::Failed => return false,
        DownloadAttemptOutcome::TerminalHandled => return true,
        DownloadAttemptOutcome::Succeeded => {}
    }

    finalize_download_success(id, item, queue, start_time, None).await;
    true
}

async fn run_multi_version(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    version_profiles: &[(String, RankSettings)],
    candidates: &[CachedCandidate<'_>],
    hierarchy: Option<&DownloadHierarchyContext>,
) -> bool {
    let downloaded_profiles = fetch_done_profiles(queue, id, item.item_type).await;
    let mut any_success = false;

    for (profile_name, profile_settings) in version_profiles {
        if downloaded_profiles.contains(profile_name) {
            tracing::debug!(
                id,
                profile = profile_name,
                "profile version already present, skipping"
            );
            continue;
        }

        let Some(stream) = pick_best_for_profile(candidates, item, profile_settings) else {
            tracing::debug!(
                id,
                profile = profile_name,
                "no cached stream found for profile"
            );
            continue;
        };

        match attempt_download(
            id,
            item,
            queue,
            stream,
            Some(profile_name.as_str()),
            Some(profile_name.as_str()),
            start_time,
            hierarchy,
        )
        .await
        {
            DownloadAttemptOutcome::Failed => {}
            DownloadAttemptOutcome::TerminalHandled => return true,
            DownloadAttemptOutcome::Succeeded => {
                any_success = true;
                tracing::debug!(id, profile = profile_name, "profile version downloaded");
            }
        }
    }

    if !any_success {
        tracing::debug!(id, title = %item.title, "no valid torrent found after trying cached candidates");
        LibraryOrchestrator::new(queue)
            .fan_out_download_failure(id)
            .await;
        return false;
    }

    let done = fetch_done_profiles(queue, id, item.item_type).await;
    if !version_profiles.iter().all(|(name, _)| done.contains(name)) {
        tracing::debug!(id, "some profile versions still missing — re-queuing");
        LibraryOrchestrator::new(queue)
            .fan_out_download_failure(id)
            .await;
    }

    finalize_download_success(id, item, queue, start_time, None).await;
    true
}

async fn run_single_version(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    candidates: &[CachedCandidate<'_>],
    hierarchy: Option<&DownloadHierarchyContext>,
) -> bool {
    if candidates.is_empty() {
        tracing::debug!(id, "no cached+valid streams found in this pass");
        LibraryOrchestrator::new(queue)
            .fan_out_download_failure(id)
            .await;
        return false;
    }

    for candidate in candidates {
        match attempt_download(
            id,
            item,
            queue,
            candidate.stream,
            None,
            None,
            start_time,
            hierarchy,
        )
        .await
        {
            DownloadAttemptOutcome::Failed => continue,
            DownloadAttemptOutcome::TerminalHandled => return true,
            DownloadAttemptOutcome::Succeeded => {
                finalize_download_success(id, item, queue, start_time, None).await;
                return true;
            }
        }
    }

    tracing::debug!(id, title = %item.title, "no valid torrent found after trying cached candidates");
    LibraryOrchestrator::new(queue)
        .fan_out_download_failure(id)
        .await;
    false
}

async fn finalize_download_success(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    provider: Option<String>,
) {
    if let Err(error) = repo::refresh_state_cascade(&queue.db_pool, item).await {
        tracing::error!(error = %error, "failed to refresh state after download");
    }
    LibraryOrchestrator::new(queue)
        .sync_item_request_state(item)
        .await;

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
            provider,
            duration_seconds: duration.as_secs_f64(),
        })
        .await;
    tracing::debug!(
        id,
        duration_secs = duration.as_secs_f64(),
        "download flow completed"
    );
}

async fn fetch_done_profiles(queue: &JobQueue, id: i64, item_type: MediaItemType) -> Vec<String> {
    let result = if item_type == MediaItemType::Season {
        repo::get_downloaded_profile_names_for_season(&queue.db_pool, id).await
    } else {
        repo::get_downloaded_profile_names(&queue.db_pool, id).await
    };
    result.unwrap_or_default()
}
