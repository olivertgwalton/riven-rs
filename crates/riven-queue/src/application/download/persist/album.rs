use std::collections::HashSet;

use super::tracklist::{TrackRef, match_files_to_tracks};
use super::*;
use crate::application::download::helpers::{is_persistable_audio_file, track_vfs_path};

/// Persist an album download: each audio file the release holds is matched
/// to a track of the album and saved under it. On success this sends the
/// album's download events itself, so the caller should return after.
pub async fn persist_album(
    item: &MediaItem,
    dl: DownloadResult,
    info_hash: &str,
    queue: &JobQueue,
    hierarchy: &DownloadHierarchyContext,
    start_time: Instant,
    stream_id: Option<i64>,
    profile: Option<&str>,
) -> SeasonPersistOutcome {
    let id = item.id;
    let Some(artist) = hierarchy.show_title.as_deref() else {
        tracing::error!(id, "album has no artist");
        return SeasonPersistOutcome::Failed;
    };
    let tracks = match repo::list_tracks(id).await {
        Ok(tracks) => tracks,
        Err(error) => {
            tracing::error!(id, %error, "failed to load the album's tracks");
            return SeasonPersistOutcome::Failed;
        }
    };

    let files: Vec<&DownloadFile> = dl
        .files
        .iter()
        .filter(|file| is_persistable_audio_file(&file.filename))
        .filter(|file| has_playable_url(file))
        .collect();
    let names: Vec<&str> = files.iter().map(|file| file.filename.as_str()).collect();
    let refs: Vec<TrackRef<'_>> = tracks
        .iter()
        .map(|track| TrackRef {
            disc: track.disc_number.unwrap_or(1),
            number: track.track_number.unwrap_or(0),
            title: &track.title,
        })
        .collect();
    let owners = match_files_to_tracks(&names, &refs);

    let metadata = metadata_from_show_context(hierarchy);
    let library_profiles = queue
        .filesystem_settings
        .read()
        .await
        .matching_profile_keys(&metadata, FilesystemContentType::Music)
        .into_json();
    let multi_disc = refs.iter().any(|track| track.disc > 1);
    // Tracks that already hold this version: a release touching only these
    // is no progress, or the walk would stop on it every time.
    let already_saved: HashSet<i64> = match profile {
        Some(name) => repo::get_episode_ids_with_profile_for_season(id, name)
            .await
            .unwrap_or_default(),
        None => HashSet::new(),
    };

    let mut saved = 0usize;
    for (track, owner) in tracks.iter().zip(&owners) {
        let Some(file) = owner.map(|index| files[index]) else {
            continue;
        };
        if already_saved.contains(&track.id) {
            continue;
        }
        let path = track_vfs_path(
            artist,
            &item.title,
            item.year,
            multi_disc.then(|| track.disc_number.unwrap_or(1)),
            track.track_number.unwrap_or(0),
            &track.title,
            &file.filename,
        );
        match repo::create_media_entry(repo::MediaEntryInput {
            media_item_id: track.id,
            path: &path,
            file_size: file.file_size as i64,
            original_filename: &file.filename,
            download_url: file.download_url.as_deref(),
            stream_url: file.stream_url.as_deref(),
            plugin: &dl.plugin_name,
            provider: dl.provider.as_deref(),
            stream_id,
            resolution: None,
            ranking_profile_name: profile,
            library_profiles: Some(&library_profiles),
            usenet_info_hash: file.usenet_info_hash.as_deref(),
            usenet_file_index: file.usenet_file_index,
        })
        .await
        {
            Ok(_) => saved += 1,
            Err(e) if is_item_deleted_fk_error(&e) => {
                tracing::debug!(
                    track_id = track.id,
                    "track was deleted mid-persist, skipping"
                );
            }
            Err(e) => {
                tracing::error!(error = %e, track_id = track.id, "failed to create media entry for track");
            }
        }
    }

    if saved == 0 {
        tracing::warn!(
            id,
            info_hash,
            title = %item.title,
            audio_files = files.len(),
            "album release holds none of the album's missing tracks; blacklisting stream"
        );
        blacklist_stream(id, info_hash, &item.title).await;
        return SeasonPersistOutcome::Failed;
    }

    sync_item_request_state(item).await;
    let complete = repo::get_media_item(id)
        .await
        .ok()
        .flatten()
        .is_some_and(|album| album.state == MediaItemState::Completed);
    if !complete {
        queue
            .notify(RivenEvent::MediaItemDownloadPartialSuccess { id })
            .await;
        return SeasonPersistOutcome::Partial;
    }

    queue
        .filesystem_settings_revision
        .fetch_add(1, Ordering::SeqCst);
    let duration = start_time.elapsed();
    queue
        .notify(RivenEvent::MediaItemDownloadSuccess {
            id,
            title: format!("{artist} - {}", item.title),
            full_title: item.full_title.clone(),
            item_type: item.item_type,
            year: item.year,
            imdb_id: None,
            tmdb_id: None,
            tvdb_id: None,
            poster_path: item.poster_path.clone(),
            plugin_name: dl.plugin_name,
            provider: dl.provider,
            duration_seconds: duration.as_secs_f64(),
        })
        .await;
    tracing::info!(
        id,
        duration_secs = duration.as_secs_f64(),
        "album download flow completed"
    );
    SeasonPersistOutcome::Complete
}
