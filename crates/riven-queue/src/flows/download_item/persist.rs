use std::time::Instant;

use riven_core::events::RivenEvent;
use riven_core::types::*;
use riven_db::entities::MediaItem;
use riven_db::repo;

use crate::JobQueue;
use super::helpers::{handle_bitrate_failure, load_item_or_err};


/// Persist a movie download result. Returns `true` on success.
pub async fn persist_movie(
    item: &MediaItem,
    dl: &DownloadResult,
    info_hash: &str,
    queue: &JobQueue,
) -> bool {
    let id = item.id;

    tracing::debug!(id, info_hash, files = dl.files.len(), "persisting movie");
    for f in &dl.files {
        tracing::debug!(id, filename = %f.filename, size = f.file_size, "torrent file");
    }

    let mut video_files: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
        .files
        .iter()
        .map(|f| (f, super::helpers::parse_file_path(&f.filename)))
        .filter(|(_, parsed)| parsed.media_type() == "movie")
        .collect();

    // Sort by size descending
    video_files.sort_by_key(|(f, _)| std::cmp::Reverse(f.file_size));

    let file = if let Some(first) = video_files.first() {
        first.0
    } else if let Some(largest) = dl.files.iter().max_by_key(|f| f.file_size) {
        tracing::warn!(id, info_hash, "no movie file found in torrent; falling back to largest file");
        largest
    } else {
        tracing::warn!(id, info_hash = %info_hash, "torrent has no files — blacklisting stream");
        if !info_hash.is_empty() {
            let _ = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await;
        }
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        queue.fan_out_download(id).await;
        return false;
    };

    let config = queue.downloader_config.read().await;
    if !config.movie_passes(file.file_size, item.runtime) {
        handle_bitrate_failure(id, info_hash, file.file_size, item.runtime, "movie", queue).await;
        return false;
    }
    drop(config);

    let ext = file.filename.rsplit('.').next().unwrap_or("mkv");
    let vfs_name = format!("{}.{ext}", item.pretty_name());
    let path = format!("/movies/{}/{vfs_name}", item.pretty_name());

    if let Err(e) = repo::create_media_entry(
        &queue.db_pool,
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
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: e.to_string(),
            })
            .await;
        return false;
    }

    true
}

/// Persist an episode download result. Returns `true` on success.
pub async fn persist_episode(
    item: &MediaItem,
    dl: &DownloadResult,
    info_hash: &str,
    queue: &JobQueue,
) -> bool {
    let id = item.id;

    let season_id = match item.parent_id {
        Some(sid) => sid,
        None => {
            tracing::error!(id, "episode has no parent_id");
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: "episode has no parent season".into(),
                })
                .await;
            return false;
        }
    };

    let season =
        match load_item_or_err(season_id, queue, "could not load parent season").await {
            Some(s) => s,
            None => return false,
        };

    let show_id = match season.parent_id {
        Some(sid) => sid,
        None => {
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: "season has no parent show".into(),
                })
                .await;
            return false;
        }
    };

    let show: MediaItem =
        match load_item_or_err(show_id, queue, "could not load parent show").await {
            Some(s) => s,
            None => return false,
        };

    let season_number = season.season_number.unwrap_or(1);
    let episode_number = item.episode_number.unwrap_or(1);

    tracing::debug!(id, info_hash, files = dl.files.len(), "persisting episode");
    for f in &dl.files {
        tracing::debug!(id, filename = %f.filename, size = f.file_size, "torrent file");
    }

    let matched: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
        .files
        .iter()
        .map(|f| (f, super::helpers::parse_file_path(&f.filename)))
        .filter(|(_, parsed)| {
            parsed.seasons.contains(&season_number)
                && (parsed.episodes.contains(&episode_number)
                    || item.absolute_number.map_or(false, |abs| parsed.episodes.contains(&abs)))
        })
        .collect();

    if matched.is_empty() {
        tracing::info!(
            id,
            season = season_number,
            episode = episode_number,
            info_hash = %info_hash,
            "no torrent file matched episode — blacklisting stream"
        );
        if !info_hash.is_empty() {
            let _ = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await;
        }
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        queue.fan_out_download(id).await;
        return false;
    }

    let file = matched.iter().max_by_key(|(f, _)| f.file_size).unwrap().0;

    let config = queue.downloader_config.read().await;
    if !config.episode_passes(file.file_size, item.runtime) {
        drop(config);
        handle_bitrate_failure(id, info_hash, file.file_size, item.runtime, "episode", queue)
            .await;
        return false;
    }
    drop(config);

    let show_name = show.pretty_name();
    let vfs_filename = format!(
        "{} - s{:02}e{:02}.mkv",
        show_name, season_number, episode_number
    );
    let path = format!("/shows/{show_name}/Season {season_number:02}/{vfs_filename}");

    if let Err(e) = repo::create_media_entry(
        &queue.db_pool,
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
        queue
            .notify(RivenEvent::MediaItemDownloadError {
                id,
                title: item.title.clone(),
                error: e.to_string(),
            })
            .await;
        return false;
    }

    true
}

/// Persist a season (pack) download result.
/// On success this function also sends the `MediaItemDownloadSuccess` event and
/// updates state, so the caller should return immediately after.
pub async fn persist_season(
    item: &MediaItem,
    dl: DownloadResult,
    info_hash: &str,
    queue: &JobQueue,
    start_time: Instant,
) -> bool {
    let id = item.id;

    let show_id = match item.parent_id {
        Some(sid) => sid,
        None => {
            tracing::error!(id, "season has no parent show");
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: "season has no parent show".into(),
                })
                .await;
            return false;
        }
    };

    let show: MediaItem =
        match load_item_or_err(show_id, queue, "could not load parent show").await {
            Some(s) => s,
            None => return false,
        };

    let episodes = match repo::list_episodes(&queue.db_pool, id).await {
        Ok(eps) => eps,
        Err(e) => {
            queue
                .notify(RivenEvent::MediaItemDownloadError {
                    id,
                    title: item.title.clone(),
                    error: e.to_string(),
                })
                .await;
            return false;
        }
    };

    let season_number = item.season_number.unwrap_or(1);
    let show_name = show.pretty_name();
    let mut any_matched = false;

    let config = queue.downloader_config.read().await;

    for ep in &episodes {
        let episode_number = ep.episode_number.unwrap_or(1);

        let matched: Vec<(&DownloadFile, riven_rank::ParsedData)> = dl
            .files
            .iter()
            .map(|f| (f, super::helpers::parse_file_path(&f.filename)))
            .filter(|(_, parsed)| {
                parsed.seasons.contains(&season_number)
                    && (parsed.episodes.contains(&episode_number)
                        || ep.absolute_number.map_or(false, |abs| parsed.episodes.contains(&abs)))
            })
            .collect();

        if matched.is_empty() {
            continue;
        }

        let file = matched.iter().max_by_key(|(f, _)| f.file_size).unwrap().0;

        if !config.episode_passes(file.file_size, ep.runtime) {
            tracing::info!(
                id = ep.id,
                file_size = file.file_size,
                runtime = ?ep.runtime,
                info_hash = %info_hash,
                "episode failed bitrate check in season download — blacklisting stream for episode"
            );
            if !info_hash.is_empty() {
                let _ =
                    repo::blacklist_stream_by_hash(&queue.db_pool, ep.id, info_hash).await;
                let _ =
                    repo::update_stream_file_size(&queue.db_pool, info_hash, file.file_size).await;
            }
            continue;
        }

        let vfs_filename = format!(
            "{} - s{:02}e{:02}.mkv",
            show_name, season_number, episode_number
        );
        let path = format!("/shows/{show_name}/Season {season_number:02}/{vfs_filename}");

        match repo::create_media_entry(
            &queue.db_pool,
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
                if let Err(e) =
                    repo::update_media_item_state(&queue.db_pool, ep.id, MediaItemState::Completed)
                        .await
                {
                    tracing::error!(error = %e, ep_id = ep.id, "failed to update episode state");
                }
                any_matched = true;
            }
            Err(e) => {
                tracing::error!(error = %e, ep_id = ep.id, "failed to create media entry for episode");
            }
        }
    }

    drop(config);

    if !any_matched {
        tracing::info!(
            id,
            season = season_number,
            info_hash = %info_hash,
            "no episodes matched in season download — blacklisting stream"
        );
        if !info_hash.is_empty() {
            let _ = repo::blacklist_stream_by_hash(&queue.db_pool, id, info_hash).await;
        }
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        queue.fan_out_download(id).await;
        return false;
    }

    if let Ok(season_state) = repo::compute_state(&queue.db_pool, item).await {
        let _ = repo::update_media_item_state(&queue.db_pool, id, season_state).await;
    }
    let _ = repo::cascade_state_update(&queue.db_pool, item).await;

    let duration = start_time.elapsed();
    let display_title = format!("{} - {}", show.title, item.title);
    queue
        .notify(RivenEvent::MediaItemDownloadSuccess {
            id,
            title: display_title,
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
    tracing::info!(id, duration_secs = duration.as_secs_f64(), "season download flow completed");
    true
}
