use std::sync::atomic::Ordering;
use std::time::Instant;

use riven_core::events::RivenEvent;
use riven_core::settings::{FilesystemContentType, FilesystemItemMetadata};
use riven_core::types::*;
use riven_db::entities::{MediaItem, Stream};
use riven_db::repo;

/// Returns true when the error is a FK violation caused by the media item being
/// deleted while a background persist task was still running.
fn is_item_deleted_fk_error(err: &anyhow::Error) -> bool {
    use sea_orm::{DbErr, SqlErr};
    matches!(
        err.downcast_ref::<DbErr>().and_then(DbErr::sql_err),
        Some(SqlErr::ForeignKeyConstraintViolation(msg))
            if msg.contains("filesystem_entries_media_item_id_fkey")
    )
}

use super::helpers::{
    episode_vfs_path, handle_bitrate_failure, is_persistable_video_file, looks_obfuscated,
    matches_episode_lookup, parse_file_path, select_episode_files, stream_raw_title,
    stream_resolution,
};
use crate::JobQueue;

mod packs;
mod single;
mod supplied;

pub use packs::{persist_season, persist_show};
pub use single::{persist_episode, persist_movie};
pub use supplied::persist_supplied_download;

/// A file is persistable only when the VFS can eventually fetch bytes from it.
/// `stream_url` is used directly; `download_url` is the input the VFS hands to
/// the provider's link-resolver to mint a fresh `stream_url` on demand. Without
/// either, the row produces a phantom entry in the VFS — `ls` shows the file
/// but `read()` fails, which surfaces as Plex seeing the title without ever
/// scanning media or opening a debrid connection.
///
/// The `matched:{id}` sentinel used by show-supplied downloads carries a real
/// `download_url`, so it passes this check.
fn has_playable_url(file: &DownloadFile) -> bool {
    file.download_url.as_deref().is_some_and(|s| !s.is_empty())
        || file.stream_url.as_deref().is_some_and(|s| !s.is_empty())
}
use crate::context::{DownloadHierarchyContext, load_download_hierarchy_context};
use crate::lifecycle::sync_item_request_state;
pub enum SeasonPersistOutcome {
    Failed,
    Partial,
    Complete,
}

pub(crate) fn metadata_from_show_context(ctx: &DownloadHierarchyContext) -> FilesystemItemMetadata {
    let genres = ctx
        .show_genres
        .as_ref()
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str())
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    FilesystemItemMetadata {
        genres,
        network: ctx.show_network.clone(),
        content_rating: ctx.show_content_rating,
        language: ctx.show_language.clone(),
        country: ctx.show_country.clone(),
        year: ctx.show_year,
        rating: ctx.show_rating,
        is_anime: ctx.show_is_anime,
    }
}

pub(crate) fn pretty_show_name(ctx: &DownloadHierarchyContext, fallback_title: &str) -> String {
    let title = ctx.show_title.as_deref().unwrap_or(fallback_title);
    let year_str = ctx.show_year.map(|y| format!(" ({y})")).unwrap_or_default();
    let id_str = ctx
        .show_tvdb_id
        .as_ref()
        .map(|id| format!(" {{tvdb-{id}}}"))
        .unwrap_or_default();
    format!("{title}{year_str}{id_str}")
}

/// Blacklist the stream behind `info_hash` for this media item (best-effort;
/// a non-empty hash is required and failures are logged, not propagated).
async fn blacklist_stream(id: i64, info_hash: &str, title: &str) {
    if !info_hash.is_empty()
        && let Err(err) = repo::blacklist_stream_by_hash(id, info_hash).await
    {
        tracing::warn!(id, info_hash, title, %err, "failed to blacklist stream");
    }
}

pub async fn finalize_download_success(
    id: i64,
    item: &MediaItem,
    queue: &JobQueue,
    start_time: Instant,
    provider: Option<String>,
    plugin_name: Option<String>,
) {
    sync_item_request_state(item).await;

    queue
        .filesystem_settings_revision
        .fetch_add(1, Ordering::SeqCst);

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
            plugin_name: plugin_name.unwrap_or_default(),
            provider,
            duration_seconds: duration.as_secs_f64(),
        })
        .await;
    tracing::info!(
        id,
        duration_secs = duration.as_secs_f64(),
        "download flow completed"
    );
}
