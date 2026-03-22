use std::time::Instant;

use riven_core::events::{HookResponse, RivenEvent};
use riven_core::plugin::PluginRegistry;
use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;
use crate::DownloaderConfig;
use tokio::sync::mpsc;

/// Log a bitrate failure, blacklist the stream, and send a PartialSuccess event.
async fn handle_bitrate_failure(
    id: i64,
    info_hash: &str,
    file_size: u64,
    runtime: Option<i32>,
    context: &str,
    db_pool: &sqlx::PgPool,
    event_tx: &mpsc::Sender<RivenEvent>,
) {
    tracing::warn!(
        id,
        file_size,
        runtime = ?runtime,
        info_hash = %info_hash,
        "{context} failed bitrate check — blacklisting stream"
    );
    if !info_hash.is_empty() {
        let _ = repo::blacklist_stream_by_hash(db_pool, id, info_hash).await;
    }
    let _ = event_tx
        .send(RivenEvent::MediaItemDownloadPartialSuccess { id })
        .await;
}

/// Load a media item by id, or send a `MediaItemDownloadError` event and return `None`.
async fn load_item_or_err(
    id: i64,
    db_pool: &sqlx::PgPool,
    event_tx: &mpsc::Sender<RivenEvent>,
    error_msg: &str,
) -> Option<MediaItem> {
    match repo::get_media_item(db_pool, id).await {
        Ok(Some(item)) => Some(item),
        Ok(None) => {
            tracing::error!(id, "{error_msg}");
            let _ = event_tx
                .send(RivenEvent::MediaItemDownloadError {
                    id,
                    title: String::new(),
                    error: error_msg.into(),
                })
                .await;
            None
        }
        Err(e) => {
            let _ = event_tx
                .send(RivenEvent::MediaItemDownloadError {
                    id,
                    title: String::new(),
                    error: e.to_string(),
                })
                .await;
            None
        }
    }
}

/// Returns true if `filename` appears to contain the given season/episode.
/// Handles S01E01, s1e1, 1x01 patterns and absolute episode numbers.
fn file_matches_episode(
    filename: &str,
    season: i32,
    episode: i32,
    absolute: Option<i32>,
) -> bool {
    let fl = filename.to_lowercase();

    // Standard SnnEnn patterns (with and without zero-padding)
    if fl.contains(&format!("s{:02}e{:02}", season, episode))
        || fl.contains(&format!("s{}e{}", season, episode))
    {
        return true;
    }
    // NxNN format (e.g. 1x01)
    if fl.contains(&format!("{}x{:02}", season, episode))
        || fl.contains(&format!("{}x{}", season, episode))
    {
        return true;
    }
    // Absolute episode number (common in anime)
    if let Some(abs) = absolute {
        if fl.contains(&format!("e{:03}", abs)) || fl.contains(&format!("e{:02}", abs)) {
            return true;
        }
    }
    false
}

/// Run the download item flow.
/// Dispatches to download plugins, persists results, updates state.
pub async fn run(
    id: i64,
    event: &RivenEvent,
    registry: &PluginRegistry,
    db_pool: &sqlx::PgPool,
    event_tx: &mpsc::Sender<RivenEvent>,
    downloader_config: &DownloaderConfig,
) {
    let start_time = Instant::now();
    tracing::info!(id, "running download flow");

    // Extract the info_hash from the triggering event so we can blacklist it
    // if the torrent turns out not to contain the required episode.
    let info_hash = match event {
        RivenEvent::MediaItemDownloadRequested { info_hash, .. } => info_hash.clone(),
        _ => String::new(),
    };

    let item = match load_item_or_err(id, db_pool, event_tx, "media item not found for download").await {
        Some(item) => item,
        None => return,
    };

    // Mark as Ongoing while the download is in progress so the scheduler
    // doesn't re-scrape the item and the scrape guard skips it.
    if let Err(e) = repo::update_media_item_state(db_pool, id, MediaItemState::Ongoing).await {
        tracing::warn!(error = %e, id, "failed to set state to ongoing");
    }

    let results = registry.dispatch(event).await;

    let mut download_result: Option<DownloadResult> = None;

    for (plugin_name, result) in results {
        match result {
            Ok(HookResponse::Download(dl)) => {
                tracing::info!(plugin = plugin_name, files = dl.files.len(), "download responded");
                download_result = Some(dl);
                break; // Use first successful downloader
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(plugin = plugin_name, error = %e, "download hook failed");
            }
        }
    }

    let dl = match download_result {
        Some(dl) => dl,
        None => {
            // Revert to Scraped so the retry scheduler can pick this up again.
            let _ = repo::update_media_item_state(db_pool, id, MediaItemState::Scraped).await;
            let _ = event_tx
                .send(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: "no download plugin responded".into(),
                })
                .await;
            return;
        }
    };

    // Persist download results as filesystem entries.
    // Movies: pick only the largest video file.
    // Episodes: match torrent files to the episode's S##E## number and build
    //           the correct VFS path the FUSE layer expects.
    let video_extensions = ["mkv", "mp4", "avi", "mov", "wmv", "m4v", "webm"];

    match item.item_type {
        MediaItemType::Movie => {
            let video_files: Vec<&DownloadFile> = dl
                .files
                .iter()
                .filter(|f| {
                    f.filename
                        .rsplit('.')
                        .next()
                        .map(|ext| video_extensions.contains(&ext.to_lowercase().as_str()))
                        .unwrap_or(false)
                })
                .collect();

            let file = if let Some(largest) = video_files.iter().max_by_key(|f| f.file_size) {
                largest
            } else if let Some(largest) = dl.files.iter().max_by_key(|f| f.file_size) {
                largest
            } else {
                let _ = event_tx
                    .send(RivenEvent::MediaItemDownloadError {
                        id,
                        title: item.title.clone(),
                    error: "torrent has no files".into(),
                    })
                    .await;
                return;
            };

            // Bitrate gate: reject streams whose file size implies too-low quality.
            if !downloader_config.movie_passes(file.file_size, item.runtime) {
                handle_bitrate_failure(id, &info_hash, file.file_size, item.runtime, "movie", db_pool, event_tx).await;
                return;
            }

            let ext = file.filename.rsplit('.').next().unwrap_or("mkv");
            let vfs_name = format!("{}.{ext}", item.pretty_name());
            let path = format!("/movies/{}/{vfs_name}", item.pretty_name());

            if let Err(e) = repo::create_media_entry(
                db_pool,
                id,
                &path,
                file.file_size as i64,
                &file.filename,
                file.download_url.as_deref(),
                file.stream_url.as_deref(),
                &dl.plugin_name,
                dl.provider.as_deref(),
            )
            .await
            {
                tracing::error!(error = %e, "failed to create media entry");
                let _ = event_tx
                    .send(RivenEvent::MediaItemDownloadError {
                        id,
                        title: item.title.clone(),
                    error: e.to_string(),
                    })
                    .await;
                return;
            }
        }

        MediaItemType::Episode => {
            // Gap 2: torrent-to-episode matching.
            // Look up the parent season and grandparent show to build the correct
            // VFS path and to obtain season_number for filename matching.
            let season_id = match item.parent_id {
                Some(sid) => sid,
                None => {
                    tracing::error!(id, "episode has no parent_id");
                    let _ = event_tx
                        .send(RivenEvent::MediaItemDownloadError {
                            id,
                            title: item.title.clone(),
                    error: "episode has no parent season".into(),
                        })
                        .await;
                    return;
                }
            };

            let season = match load_item_or_err(season_id, db_pool, event_tx, "could not load parent season").await {
                Some(s) => s,
                None => return,
            };

            let show_id = match season.parent_id {
                Some(sid) => sid,
                None => {
                    let _ = event_tx
                        .send(RivenEvent::MediaItemDownloadError {
                            id,
                            title: item.title.clone(),
                    error: "season has no parent show".into(),
                        })
                        .await;
                    return;
                }
            };

            let show: MediaItem = match load_item_or_err(show_id, db_pool, event_tx, "could not load parent show").await {
                Some(s) => s,
                None => return,
            };

            let season_number = season.season_number.unwrap_or(1);
            let episode_number = item.episode_number.unwrap_or(1);

            // Find a torrent file matching this episode.
            let matched: Vec<&DownloadFile> = dl
                .files
                .iter()
                .filter(|f| {
                    file_matches_episode(&f.filename, season_number, episode_number, item.absolute_number)
                })
                .collect();

            if matched.is_empty() {
                // Gap 3: partial success — the torrent doesn't contain this episode.
                // Blacklist this stream for the episode so the next attempt tries
                // a different torrent, then signal the event bus to re-queue.
                tracing::warn!(
                    id,
                    season = season_number,
                    episode = episode_number,
                    info_hash = %info_hash,
                    "no torrent file matched episode — blacklisting stream"
                );
                if !info_hash.is_empty() {
                    let _ = repo::blacklist_stream_by_hash(db_pool, id, &info_hash).await;
                }
                let _ = event_tx
                    .send(RivenEvent::MediaItemDownloadPartialSuccess { id })
                    .await;
                return;
            }

            // Pick the largest matching video file (handles multi-episode packs).
            let file = matched
                .iter()
                .max_by_key(|f| f.file_size)
                .copied()
                .unwrap();

            // Bitrate gate for episodes.
            if !downloader_config.episode_passes(file.file_size, item.runtime) {
                handle_bitrate_failure(id, &info_hash, file.file_size, item.runtime, "episode", db_pool, event_tx).await;
                return;
            }

            // Build the VFS path exactly as the FUSE readdir generates it.
            let show_name = show.pretty_name();
            let vfs_filename = format!(
                "{} - s{:02}e{:02}.mkv",
                show_name, season_number, episode_number
            );
            let path = format!(
                "/shows/{show_name}/Season {season_number:02}/{vfs_filename}"
            );

            if let Err(e) = repo::create_media_entry(
                db_pool,
                id,
                &path,
                file.file_size as i64,
                &file.filename,
                file.download_url.as_deref(),
                file.stream_url.as_deref(),
                &dl.plugin_name,
                dl.provider.as_deref(),
            )
            .await
            {
                tracing::error!(error = %e, "failed to create media entry");
                let _ = event_tx
                    .send(RivenEvent::MediaItemDownloadError {
                        id,
                        title: item.title.clone(),
                    error: e.to_string(),
                    })
                    .await;
                return;
            }
        }

        MediaItemType::Season => {
            // Season download: the torrent is a season pack.
            // Map files to each episode using filename matching, create a MediaEntry
            // per matched episode, and update each episode's state to Completed.
            // Mirrors riven-ts persist-download-results.ts Season branch.
            let show_id = match item.parent_id {
                Some(sid) => sid,
                None => {
                    tracing::error!(id, "season has no parent show");
                    let _ = event_tx
                        .send(RivenEvent::MediaItemDownloadError {
                            id,
                            title: item.title.clone(),
                    error: "season has no parent show".into(),
                        })
                        .await;
                    return;
                }
            };

            let show: MediaItem = match load_item_or_err(show_id, db_pool, event_tx, "could not load parent show").await {
                Some(s) => s,
                None => return,
            };

            let episodes = match repo::list_episodes(db_pool, id).await {
                Ok(eps) => eps,
                Err(e) => {
                    let _ = event_tx
                        .send(RivenEvent::MediaItemDownloadError {
                            id,
                            title: item.title.clone(),
                    error: e.to_string(),
                        })
                        .await;
                    return;
                }
            };

            let season_number = item.season_number.unwrap_or(1);
            let show_name = show.pretty_name();
            let mut any_matched = false;

            for ep in &episodes {
                let episode_number = ep.episode_number.unwrap_or(1);

                let matched: Vec<&DownloadFile> = dl
                    .files
                    .iter()
                    .filter(|f| file_matches_episode(&f.filename, season_number, episode_number, ep.absolute_number))
                    .collect();

                if matched.is_empty() {
                    continue;
                }

                let file = matched.iter().max_by_key(|f| f.file_size).copied().unwrap();

                // Bitrate gate
                if !downloader_config.episode_passes(file.file_size, ep.runtime) {
                    tracing::warn!(
                        id = ep.id,
                        file_size = file.file_size,
                        runtime = ?ep.runtime,
                        info_hash = %info_hash,
                        "episode failed bitrate check in season download — skipping"
                    );
                    continue;
                }

                let vfs_filename = format!(
                    "{} - s{:02}e{:02}.mkv",
                    show_name, season_number, episode_number
                );
                let path = format!("/shows/{show_name}/Season {season_number:02}/{vfs_filename}");

                match repo::create_media_entry(
                    db_pool,
                    ep.id,
                    &path,
                    file.file_size as i64,
                    &file.filename,
                    file.download_url.as_deref(),
                    file.stream_url.as_deref(),
                    &dl.plugin_name,
                    dl.provider.as_deref(),
                )
                .await
                {
                    Ok(_) => {
                        if let Err(e) = repo::update_media_item_state(db_pool, ep.id, MediaItemState::Completed).await {
                            tracing::error!(error = %e, ep_id = ep.id, "failed to update episode state");
                        }
                        any_matched = true;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, ep_id = ep.id, "failed to create media entry for episode");
                    }
                }
            }

            if !any_matched {
                tracing::warn!(id, season = season_number, info_hash = %info_hash, "no episodes matched in season download — blacklisting stream");
                if !info_hash.is_empty() {
                    let _ = repo::blacklist_stream_by_hash(db_pool, id, &info_hash).await;
                }
                let _ = event_tx
                    .send(RivenEvent::MediaItemDownloadPartialSuccess { id })
                    .await;
                return;
            }

            // Compute and update season state from episodes, then cascade to show.
            if let Ok(season_state) = repo::compute_state(db_pool, &item).await {
                let _ = repo::update_media_item_state(db_pool, id, season_state).await;
            }
            let _ = repo::cascade_state_update(db_pool, &item).await;

            let duration = start_time.elapsed();
            let _ = event_tx
                .send(RivenEvent::MediaItemDownloadSuccess {
                    id,
                    title: item.title.clone(),
                    full_title: item.full_title.clone(),
                    item_type: item.item_type,
                    year: item.year,
                    imdb_id: item.imdb_id.clone(),
                    tmdb_id: item.tmdb_id.clone(),
                    tvdb_id: item.tvdb_id.clone(),
                    poster_path: item.poster_path.clone(),
                    plugin_name: dl.plugin_name,
                    provider: dl.provider,
                    duration_seconds: duration.as_secs_f64(),
                })
                .await;
            tracing::info!(id, duration_secs = duration.as_secs_f64(), "season download flow completed");
            return;
        }

        _ => {
            tracing::warn!(id, item_type = ?item.item_type, "unexpected item type in download flow");
            return;
        }
    }

    // Update state (for Movie and Episode only; Season handles its own state above)
    let new_state = MediaItemState::Completed;
    if let Err(e) = repo::update_media_item_state(db_pool, id, new_state).await {
        tracing::error!(error = %e, "failed to update state after download");
    }

    // Cascade state update for episodes
    if let Err(e) = repo::cascade_state_update(db_pool, &item).await {
        tracing::error!(error = %e, "failed to cascade state update");
    }

    let duration = start_time.elapsed();
    let _ = event_tx
        .send(RivenEvent::MediaItemDownloadSuccess {
            id,
            title: item.title.clone(),
            full_title: item.full_title.clone(),
            item_type: item.item_type,
            year: item.year,
            imdb_id: item.imdb_id.clone(),
            tmdb_id: item.tmdb_id.clone(),
            tvdb_id: item.tvdb_id.clone(),
            poster_path: item.poster_path.clone(),
            plugin_name: dl.plugin_name,
            provider: dl.provider,
            duration_seconds: duration.as_secs_f64(),
        })
        .await;

    tracing::info!(id, duration_secs = duration.as_secs_f64(), "download flow completed");
}
