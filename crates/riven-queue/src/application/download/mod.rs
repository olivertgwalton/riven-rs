mod candidates;
mod execute;

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::types::*;
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;
use riven_rank::{QualityProfile, RankSettings};
use serde::Deserialize;

use crate::context::{DownloadHierarchyContext, load_download_hierarchy_context};
use crate::flows::download_item::helpers::load_item_or_err;
use crate::flows::download_item::persist::{finalize_download_success, persist_supplied_download};
use crate::flows::load_active_profiles;
use crate::{DownloadJob, JobQueue, RankStreamsJob};

use self::candidates::rank_streams_for_profile;
use self::execute::{DownloadAttemptOutcome, attempt_download};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualDownloadFileInput {
    pub name: String,
    pub size: u64,
    #[serde(default)]
    pub link: Option<String>,
    #[serde(default)]
    pub matched_media_item_id: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualDownloadTorrentInput {
    pub info_hash: String,
    pub files: Vec<ManualDownloadFileInput>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub torrent_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualDownloadErrorKind {
    IncorrectState,
    DownloadError,
}

#[derive(Debug, Clone)]
pub struct ManualDownloadError {
    pub kind: ManualDownloadErrorKind,
    pub item: Option<MediaItem>,
    pub message: String,
}

impl ManualDownloadError {
    fn incorrect_state(item: MediaItem) -> Self {
        Self {
            kind: ManualDownloadErrorKind::IncorrectState,
            item: Some(item),
            message: "media item is not in a downloadable state".to_string(),
        }
    }

    fn download_error(item: Option<MediaItem>, message: impl Into<String>) -> Self {
        Self {
            kind: ManualDownloadErrorKind::DownloadError,
            item,
            message: message.into(),
        }
    }
}

pub async fn persist_manual_download(
    id: i64,
    torrent: ManualDownloadTorrentInput,
    processed_by: &str,
    queue: &JobQueue,
) -> Result<MediaItem, ManualDownloadError> {
    let Some(item) = load_item_or_err(id, queue, "media item not found for download").await else {
        return Err(ManualDownloadError::download_error(
            None,
            "media item not found for download",
        ));
    };

    if !matches!(
        item.state,
        MediaItemState::Scraped | MediaItemState::Ongoing | MediaItemState::PartiallyCompleted
    ) {
        queue
            .notify(RivenEvent::MediaItemDownloadErrorIncorrectState { id })
            .await;
        return Err(ManualDownloadError::incorrect_state(item));
    }

    let streams = repo::get_streams_for_item(&queue.db_pool, id)
        .await
        .map_err(|error| {
            ManualDownloadError::download_error(Some(item.clone()), error.to_string())
        })?;
    let Some(stream) = streams
        .into_iter()
        .find(|stream| stream.info_hash.eq_ignore_ascii_case(&torrent.info_hash))
    else {
        return Err(ManualDownloadError::download_error(
            Some(item),
            format!("no linked stream found for info hash {}", torrent.info_hash),
        ));
    };

    if let Err(error) = repo::set_active_stream(&queue.db_pool, id, stream.id).await {
        return Err(ManualDownloadError::download_error(
            Some(item),
            error.to_string(),
        ));
    }

    let start_time = Instant::now();
    let download = DownloadResult {
        info_hash: torrent.info_hash.clone(),
        files: torrent
            .files
            .iter()
            .map(|file| DownloadFile {
                filename: file.name.clone(),
                file_size: file.size,
                download_url: file.link.clone(),
                stream_url: file
                    .matched_media_item_id
                    .map(|episode_id| format!("matched:{episode_id}")),
            })
            .collect(),
        provider: torrent.provider.clone(),
        plugin_name: processed_by.to_string(),
    };

    match persist_supplied_download(&item, &stream, download, queue, start_time).await {
        Ok(()) => repo::get_media_item(&queue.db_pool, id)
            .await
            .map_err(|error| {
                ManualDownloadError::download_error(Some(item.clone()), error.to_string())
            })?
            .ok_or_else(|| {
                ManualDownloadError::download_error(
                    Some(item),
                    "media item disappeared after download persist",
                )
            }),
        Err(error) => Err(ManualDownloadError::download_error(
            Some(item),
            error.to_string(),
        )),
    }
}

/// Step 1 of the download flow: validate the item is in a processable state
/// and hand off to `download::run`. Cache-check used to live here (bulk
/// upfront across every hash + every provider in parallel) but moved into
/// `download::run`'s per-iteration loop so an early hit on the first provider
/// short-circuits slower providers.
pub async fn run_rank_streams(id: i64, job: &RankStreamsJob, queue: &JobQueue) {
    tracing::debug!(id, "running rank-streams step");

    let Some(item) = load_item_silently(queue, id, "rank-streams").await else {
        return;
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

    let preferred = job.preferred_info_hash.clone();
    let magnet_for_preferred = if let Some(hash) = preferred.as_ref() {
        let ranks = queue.resolution_ranks.read().await.clone();
        repo::get_non_blacklisted_streams(&queue.db_pool, id, &ranks)
            .await
            .ok()
            .and_then(|streams| {
                streams
                    .into_iter()
                    .find(|s| s.info_hash.eq_ignore_ascii_case(hash))
                    .map(|s| s.magnet)
            })
    } else {
        None
    };

    // Preferred path carries info_hash/magnet; the normal path's placeholders
    // are fine since DownloadJob only consults `preferred_info_hash` for
    // selection.
    let download_job = DownloadJob {
        id,
        info_hash: preferred.clone().unwrap_or_default(),
        magnet: magnet_for_preferred.unwrap_or_default(),
        preferred_info_hash: preferred,
    };
    // Clear any stale `download` dedup key before hand-off. The rank-streams
    // dedup guard makes us the sole caller for this id, so a lingering key
    // can only come from an earlier crash/restart (30-min safety TTL).
    // Without this clear, `push_deduped` silently no-ops and the item stays
    // at Scraped.
    queue.release_dedup("download", id).await;
    queue.push_download(download_job).await;
}

/// find-valid-torrent + download-item fused. Reloads the item (matches
/// `findOneOrFail` semantics at the step boundary), iterates ranked streams
/// per profile, and per stream walks `(plugin, provider)` combinations with
/// per-iteration cache-check + early exit on the first hit.
pub async fn run(id: i64, job: &DownloadJob, queue: &JobQueue) {
    let start_time = Instant::now();
    tracing::debug!(id, "running download (find-valid-torrent + persist) step");

    let Some(item) = load_item_silently(queue, id, "download").await else {
        return;
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

    let mut active_profiles: Vec<(String, RankSettings)> =
        load_active_profiles(&queue.db_pool).await;
    if active_profiles.is_empty() {
        active_profiles.push((
            "ultra_hd".to_string(),
            QualityProfile::UltraHd.base_settings().prepare(),
        ));
    }

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
        return;
    }

    let is_manual = job.preferred_info_hash.is_some();
    let (max_size_bytes, min_size_bytes) = if is_manual {
        (None, None)
    } else {
        load_bitrate_limits(queue, &item).await
    };

    // Cache memo keyed on (plugin, provider) so each cache check fires at
    // most once per item flow.
    let plugin_providers = build_plugin_provider_iterations(queue).await;
    let mut cache = CacheMemo::new(
        all_streams.iter().map(|s| s.info_hash.clone()).collect(),
    );

    if let Some(preferred_info_hash) = job.preferred_info_hash.as_ref() {
        let _ = run_preferred_stream(
            id,
            &item,
            queue,
            start_time,
            preferred_info_hash,
            &all_streams,
            &plugin_providers,
            &mut cache,
            hierarchy.as_ref(),
        )
        .await;
    } else {
        let _ = run_downloads(
            id,
            &item,
            queue,
            start_time,
            &active_profiles,
            &all_streams,
            &plugin_providers,
            &mut cache,
            hierarchy.as_ref(),
            max_size_bytes,
            min_size_bytes,
        )
        .await;
    }
}

/// Load the media item; return `None` (without emitting a user-visible event)
/// when it's gone
async fn load_item_silently(queue: &JobQueue, id: i64, phase: &str) -> Option<MediaItem> {
    match repo::get_media_item(&queue.db_pool, id).await {
        Ok(Some(item)) => Some(item),
        Ok(None) => {
            tracing::debug!(
                id,
                phase,
                "media item disappeared before phase ran; skipping"
            );
            None
        }
        Err(error) => {
            tracing::error!(id, phase, %error, "failed to load media item");
            None
        }
    }
}

/// Memoizes cache-check results per `(plugin, provider)` so the same provider
/// is never queried twice per item flow. The download loop calls
/// `lookup` once per `(stream, plugin, provider)`; we lazily fetch the full
/// hash list the first time and reuse the response for every subsequent
/// stream that asks for the same provider.
struct CacheMemo {
    hashes: Vec<String>,
    /// `(plugin, provider)` → `hash → cached files`. An empty inner Vec is
    /// stored so we know we already asked and the hash isn't cached.
    results: HashMap<(String, Option<String>), HashMap<String, Vec<CacheCheckFile>>>,
}

impl CacheMemo {
    fn new(hashes: Vec<String>) -> Self {
        Self {
            hashes,
            results: HashMap::new(),
        }
    }

    /// Look up `info_hash` in the `(plugin, provider)` cache, fetching from
    /// the plugin if we haven't asked yet. Returns the cached files only when
    /// the hash is positively cached on this provider.
    async fn lookup(
        &mut self,
        queue: &JobQueue,
        plugin_name: &str,
        provider: Option<&str>,
        info_hash: &str,
        attempt_unknown: bool,
    ) -> Option<Vec<CacheCheckFile>> {
        let key = (plugin_name.to_string(), provider.map(str::to_string));
        if !self.results.contains_key(&key) {
            let map = fetch_provider_cache(queue, plugin_name, provider, &self.hashes, attempt_unknown).await;
            self.results.insert(key.clone(), map);
        }
        let map = self.results.get(&key)?;
        let files = map.get(&info_hash.to_lowercase())?;
        if files.is_empty() {
            None
        } else {
            Some(files.clone())
        }
    }
}

/// Issue one provider-scoped `MediaItemDownloadCacheCheckRequested` to a
/// single plugin and reduce the response to `hash → files`. Hashes whose
/// status is `Unknown` are kept only when `attempt_unknown` is on (matches
/// the historical bulk-check behaviour).
async fn fetch_provider_cache(
    queue: &JobQueue,
    plugin_name: &str,
    provider: Option<&str>,
    hashes: &[String],
    attempt_unknown: bool,
) -> HashMap<String, Vec<CacheCheckFile>> {
    let event = RivenEvent::MediaItemDownloadCacheCheckRequested {
        hashes: hashes.to_vec(),
        provider: provider.map(str::to_string),
    };
    let response = queue
        .registry
        .dispatch_to_plugin(plugin_name, &event)
        .await;
    let mut out: HashMap<String, Vec<CacheCheckFile>> = HashMap::new();
    match response {
        Some(Ok(HookResponse::CacheCheck(results))) => {
            for result in results {
                let keep = matches!(
                    result.status,
                    TorrentStatus::Cached | TorrentStatus::Downloaded
                ) || (attempt_unknown && result.status == TorrentStatus::Unknown);
                if keep {
                    out.insert(result.hash.to_lowercase(), result.files);
                }
            }
        }
        Some(Ok(_)) | None => {}
        Some(Err(error)) => {
            tracing::warn!(
                plugin = plugin_name,
                provider,
                error = %error,
                "cache check failed"
            );
        }
    }
    out
}

/// Build the flat `(plugin, provider)` iteration order used by the download
/// loop. Plugins that subscribe to `MediaItemDownloadRequested` are the
/// downloader candidates; for each one we ask `MediaItemDownloadProviderListRequested`
/// for its providers, falling back to a single `None` provider when the
/// plugin doesn't break itself out per-store.
async fn build_plugin_provider_iterations(queue: &JobQueue) -> Vec<(String, Option<String>)> {
    let plugins = queue
        .registry
        .subscriber_names(EventType::MediaItemDownloadRequested)
        .await;
    let mut out: Vec<(String, Option<String>)> = Vec::new();
    for plugin in plugins {
        let response = queue
            .registry
            .dispatch_to_plugin(&plugin, &RivenEvent::MediaItemDownloadProviderListRequested)
            .await;
        let providers: Vec<Option<String>> = match response {
            Some(Ok(HookResponse::ProviderList(list))) if !list.is_empty() => {
                list.into_iter().map(|p| Some(p.store)).collect()
            }
            _ => vec![None],
        };
        for provider in providers {
            out.push((plugin.clone(), provider));
        }
    }
    out
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
    streams: &[Stream],
    plugin_providers: &[(String, Option<String>)],
    cache: &mut CacheMemo,
    hierarchy: Option<&DownloadHierarchyContext>,
) -> bool {
    let Some(stream) = streams
        .iter()
        .find(|s| s.info_hash.eq_ignore_ascii_case(preferred_info_hash))
    else {
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "selected stream is not linked to this item".into(),
            })
            .await;
        return false;
    };

    let attempt_unknown = queue.downloader_config.read().await.attempt_unknown_downloads;

    // The user explicitly picked this stream from the scrape UI, so we trust
    // its earlier "cached" verdict and let attempt_download fall back to a
    // direct add when no provider currently reports it cached.
    for (plugin, provider) in plugin_providers {
        let cached_files = cache
            .lookup(queue, plugin, provider.as_deref(), &stream.info_hash, attempt_unknown)
            .await;
        let stores = stores_for_attempt(provider, cached_files);

        match attempt_download(
            id, item, queue, stream, stores, None, None, start_time, hierarchy, true,
        )
        .await
        {
            DownloadAttemptOutcome::Failed => continue,
            DownloadAttemptOutcome::TerminalHandled => return true,
            DownloadAttemptOutcome::Succeeded => {
                finalize_download_success(id, item, queue, start_time, None, None).await;
                return true;
            }
        }
    }

    queue
        .notify(RivenEvent::MediaItemDownloadError {
            id,
            title: item.title.clone(),
            error: "selected stream could not be downloaded from any provider".into(),
        })
        .await;
    false
}

async fn run_downloads(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    profiles: &[(String, RankSettings)],
    streams: &[Stream],
    plugin_providers: &[(String, Option<String>)],
    cache: &mut CacheMemo,
    hierarchy: Option<&DownloadHierarchyContext>,
    max_size_bytes: Option<u64>,
    min_size_bytes: Option<u64>,
) -> bool {
    let mut done_profiles: HashSet<String> = fetch_done_profiles(queue, id, item.item_type)
        .await
        .into_iter()
        .collect();
    let mut any_success = false;
    // (info_hash, plugin, provider) — we only skip exact retries; the same
    // hash can still be tried on a different provider that might have it.
    let mut attempted: HashSet<(String, String, Option<String>)> = HashSet::new();
    let attempt_unknown = queue.downloader_config.read().await.attempt_unknown_downloads;

    for (profile_name, profile_settings) in profiles {
        if done_profiles.contains(profile_name.as_str()) {
            tracing::debug!(
                id,
                profile = profile_name,
                "profile already downloaded, skipping"
            );
            continue;
        }

        let ranked = rank_streams_for_profile(streams, item, profile_settings);
        if ranked.is_empty() {
            tracing::debug!(
                id,
                profile = profile_name,
                "no stream found for profile"
            );
            continue;
        }

        let mut profile_done = false;
        'streams: for stream in ranked {
            if profile_done {
                break;
            }
            if queue.is_cancelled(id).await {
                tracing::debug!(id, "item cancelled; aborting stream loop");
                return any_success;
            }

            // Pre-debrid bitrate filter using the size already known for this
            // stream (from the torznab scrape or recorded by a previous failed
            // attempt via `update_stream_file_size`). Avoids the multi-second
            // debrid round-trip + cache-check for streams that cannot pass.
            if let Some(size) = stream.file_size_bytes
                && size >= 0
                && !passes_size_bounds(size as u64, max_size_bytes, min_size_bytes)
            {
                tracing::debug!(
                    id,
                    info_hash = %stream.info_hash,
                    file_size = size,
                    "stream pre-filter: known size violates bitrate threshold; skipping"
                );
                continue;
            }

            for (plugin, provider) in plugin_providers {
                let key = (
                    stream.info_hash.clone(),
                    plugin.clone(),
                    provider.clone(),
                );
                if attempted.contains(&key) {
                    continue;
                }

                let cached_files = cache
                    .lookup(queue, plugin, provider.as_deref(), &stream.info_hash, attempt_unknown)
                    .await;

                let is_cached = cached_files.is_some();
                if !is_cached && !attempt_unknown {
                    continue;
                }

                if let Some(files) = &cached_files
                    && !passes_bitrate_filter(files, max_size_bytes, min_size_bytes)
                {
                    continue;
                }

                let stores = stores_for_attempt(provider, cached_files);

                match attempt_download(
                    id,
                    item,
                    queue,
                    stream,
                    stores,
                    Some(profile_name.as_str()),
                    Some(profile_name.as_str()),
                    start_time,
                    hierarchy,
                    false,
                )
                .await
                {
                    DownloadAttemptOutcome::Failed => {
                        attempted.insert(key);
                    }
                    DownloadAttemptOutcome::TerminalHandled => return true,
                    DownloadAttemptOutcome::Succeeded => {
                        done_profiles.insert(profile_name.clone());
                        any_success = true;
                        profile_done = true;
                        continue 'streams;
                    }
                }
            }
        }
    }

    if !any_success {
        tracing::debug!(id, title = %item.title, "no valid torrent found after trying cached candidates");
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: "no valid torrent found after trying cached candidates".into(),
            })
            .await;
        return false;
    }

    if !profiles
        .iter()
        .all(|(name, _)| done_profiles.contains(name.as_str()))
    {
        tracing::debug!(id, "some profiles still missing a download; re-queuing");
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
    }

    finalize_download_success(id, item, queue, start_time, None, None).await;
    true
}

fn stores_for_attempt(
    provider: &Option<String>,
    cached_files: Option<Vec<CacheCheckFile>>,
) -> Vec<CachedStoreEntry> {
    cached_files
        .map(|files| {
            vec![CachedStoreEntry {
                store: provider.clone().unwrap_or_default(),
                files,
            }]
        })
        .unwrap_or_default()
}

fn passes_bitrate_filter(
    files: &[CacheCheckFile],
    max_size_bytes: Option<u64>,
    min_size_bytes: Option<u64>,
) -> bool {
    if max_size_bytes.is_none() && min_size_bytes.is_none() {
        return true;
    }
    let total: u64 = files.iter().filter_map(|f| f.size).sum();
    passes_size_bounds(total, max_size_bytes, min_size_bytes)
}

fn passes_size_bounds(size: u64, max_size_bytes: Option<u64>, min_size_bytes: Option<u64>) -> bool {
    if max_size_bytes.is_some_and(|m| size > m) {
        return false;
    }
    if min_size_bytes.is_some_and(|m| size < m) {
        return false;
    }
    true
}

async fn fetch_done_profiles(queue: &JobQueue, id: i64, item_type: MediaItemType) -> Vec<String> {
    let result = if item_type == MediaItemType::Season {
        repo::get_downloaded_profile_names_for_season(&queue.db_pool, id).await
    } else {
        repo::get_downloaded_profile_names(&queue.db_pool, id).await
    };
    result.unwrap_or_default()
}
