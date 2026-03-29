use anyhow::Result;
use sqlx::PgPool;

use crate::entities::*;
use riven_rank::ResolutionRanks;

pub async fn upsert_stream(
    pool: &PgPool,
    info_hash: &str,
    parsed_data: Option<serde_json::Value>,
    rank: Option<i64>,
) -> Result<Stream> {
    let stream = sqlx::query_as::<_, Stream>(
        "INSERT INTO streams (info_hash, parsed_data, rank) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (info_hash) DO UPDATE SET \
             parsed_data = COALESCE($2, streams.parsed_data), \
             rank = COALESCE($3, streams.rank) \
         RETURNING *",
    )
    .bind(info_hash)
    .bind(parsed_data)
    .bind(rank)
    .fetch_one(pool)
    .await?;
    Ok(stream)
}

/// Record the actual file size for a stream (learned from a download attempt).
/// This is stored globally on the stream so future scrapes of any item can
/// pre-filter the stream before it enters the ranked candidate pool.
pub async fn update_stream_file_size(
    pool: &PgPool,
    info_hash: &str,
    file_size_bytes: u64,
) -> Result<()> {
    sqlx::query!(
        "UPDATE streams SET file_size_bytes = $1 WHERE info_hash = $2",
        file_size_bytes as i64,
        info_hash
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn link_stream_to_item(pool: &PgPool, media_item_id: i64, stream_id: i64) -> Result<()> {
    sqlx::query!(
        "INSERT INTO media_item_streams (media_item_id, stream_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        media_item_id,
        stream_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_streams_for_item(pool: &PgPool, media_item_id: i64) -> Result<Vec<Stream>> {
    Ok(sqlx::query_as::<_, Stream>(
        "SELECT s.* FROM streams s JOIN media_item_streams ms ON s.id = ms.stream_id WHERE ms.media_item_id = $1",
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?)
}

fn build_stream_query(ranks: &ResolutionRanks, limit_one: bool) -> String {
    format!(
        r#"SELECT s.* FROM streams s
           JOIN media_item_streams ms ON s.id = ms.stream_id
           WHERE ms.media_item_id = $1
             AND s.id NOT IN (
                 SELECT stream_id FROM media_item_blacklisted_streams WHERE media_item_id = $1
             )
           ORDER BY
               CASE COALESCE(s.parsed_data->>'resolution', 'unknown')
                   WHEN '2160p' THEN {r2160p}
                   WHEN '1440p' THEN {r1440p}
                   WHEN '1080p' THEN {r1080p}
                   WHEN '720p' THEN {r720p}
                   WHEN '480p' THEN {r480p}
                   WHEN '360p' THEN {r360p}
                   WHEN 'unknown' THEN {unknown}
                   ELSE 0
               END DESC,
               s.rank DESC NULLS LAST,
               s.id ASC
           LIMIT {limit}"#,
        r2160p = ranks.r2160p,
        r1440p = ranks.r1440p,
        r1080p = ranks.r1080p,
        r720p = ranks.r720p,
        r480p = ranks.r480p,
        r360p = ranks.r360p,
        unknown = ranks.unknown,
        limit = if limit_one { 1 } else { 500 }
    )
}

pub async fn clear_blacklisted_streams(pool: &PgPool, media_item_id: i64) -> Result<()> {
    sqlx::query!(
        "DELETE FROM media_item_blacklisted_streams WHERE media_item_id = $1",
        media_item_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn load_resolution_ranks(pool: &PgPool) -> ResolutionRanks {
    match super::get_setting(pool, "rank_settings").await {
        Ok(Some(value)) => value
            .get("resolution_ranks")
            .and_then(|v| serde_json::from_value::<ResolutionRanks>(v.clone()).ok())
            .unwrap_or_default(),
        _ => ResolutionRanks::default(),
    }
}

pub async fn get_non_blacklisted_streams(pool: &PgPool, media_item_id: i64) -> Result<Vec<Stream>> {
    let ranks = load_resolution_ranks(pool).await;
    let sql = build_stream_query(&ranks, false);
    Ok(sqlx::query_as::<_, Stream>(&sql).bind(media_item_id).fetch_all(pool).await?)
}

/// Fetch only the highest-ranked non-blacklisted stream for an item.
pub async fn get_best_stream(pool: &PgPool, media_item_id: i64) -> Result<Option<Stream>> {
    let ranks = load_resolution_ranks(pool).await;
    let sql = build_stream_query(&ranks, true);
    Ok(sqlx::query_as::<_, Stream>(&sql).bind(media_item_id).fetch_optional(pool).await?)
}

pub async fn get_filesystem_entries(pool: &PgPool, media_item_id: i64) -> Result<Vec<FileSystemEntry>> {
    Ok(
        sqlx::query_as::<_, FileSystemEntry>("SELECT * FROM filesystem_entries WHERE media_item_id = $1")
            .bind(media_item_id)
            .fetch_all(pool)
            .await?,
    )
}

pub async fn get_media_entries(pool: &PgPool, media_item_id: i64) -> Result<Vec<FileSystemEntry>> {
    Ok(
        sqlx::query_as::<_, FileSystemEntry>(
            "SELECT * FROM filesystem_entries WHERE media_item_id = $1 AND entry_type = 'media'",
        )
        .bind(media_item_id)
        .fetch_all(pool)
        .await?,
    )
}

pub async fn get_media_entry_by_path(pool: &PgPool, path: &str) -> Result<Option<FileSystemEntry>> {
    Ok(
        sqlx::query_as::<_, FileSystemEntry>(
            "SELECT * FROM filesystem_entries WHERE path = $1 AND entry_type = 'media'",
        )
        .bind(path)
        .fetch_optional(pool)
        .await?,
    )
}

/// Return the ranking profile names that already have a downloaded entry for this item.
/// Used by the multi-version download flow to skip profiles that are already complete.
pub async fn get_downloaded_profile_names(pool: &PgPool, media_item_id: i64) -> Result<Vec<String>> {
    let rows: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT DISTINCT ranking_profile_name FROM filesystem_entries \
         WHERE media_item_id = $1 AND entry_type = 'media' AND ranking_profile_name IS NOT NULL",
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().flatten().collect())
}

/// For a Season item, return profile names that have been downloaded for at least
/// one episode in that season. Season pack downloads store entries on episode IDs
/// (not the season ID itself), so the regular `get_downloaded_profile_names` always
/// returns empty for seasons.
pub async fn get_downloaded_profile_names_for_season(
    pool: &PgPool,
    season_id: i64,
) -> Result<Vec<String>> {
    let rows: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT DISTINCT fe.ranking_profile_name \
         FROM filesystem_entries fe \
         INNER JOIN media_items ep ON ep.id = fe.media_item_id \
         WHERE ep.parent_id = $1 \
           AND fe.entry_type = 'media' \
           AND fe.ranking_profile_name IS NOT NULL",
    )
    .bind(season_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().flatten().collect())
}

pub async fn create_media_entry(
    pool: &PgPool,
    media_item_id: i64,
    path: &str,
    file_size: i64,
    original_filename: &str,
    download_url: Option<&str>,
    stream_url: Option<&str>,
    plugin: &str,
    provider: Option<&str>,
    stream_id: Option<i64>,
    resolution: Option<&str>,
    ranking_profile_name: Option<&str>,
) -> Result<FileSystemEntry> {
    let media_metadata = parse_filename_metadata(original_filename);

    let existing = sqlx::query_as::<_, FileSystemEntry>(
        "SELECT * FROM filesystem_entries WHERE media_item_id = $1 AND path = $2 AND entry_type = 'media'",
    )
    .bind(media_item_id)
    .bind(path)
    .fetch_optional(pool)
    .await?;

    if let Some(entry) = existing {
        let updated = sqlx::query_as::<_, FileSystemEntry>(
            "UPDATE filesystem_entries \
             SET file_size = $1, original_filename = $2, download_url = $3, stream_url = $4, \
                 plugin = $5, provider = $6, media_metadata = $7, \
                 stream_id = COALESCE($9, stream_id), \
                 resolution = COALESCE($10, resolution), \
                 ranking_profile_name = COALESCE($11, ranking_profile_name), \
                 updated_at = NOW() \
             WHERE id = $8 \
             RETURNING *",
        )
        .bind(file_size)
        .bind(original_filename)
        .bind(download_url)
        .bind(stream_url)
        .bind(plugin)
        .bind(provider)
        .bind(&media_metadata)
        .bind(entry.id)
        .bind(stream_id)
        .bind(resolution)
        .bind(ranking_profile_name)
        .fetch_one(pool)
        .await?;
        return Ok(updated);
    }

    let entry = sqlx::query_as::<_, FileSystemEntry>(
        "INSERT INTO filesystem_entries \
         (media_item_id, entry_type, path, file_size, original_filename, download_url, stream_url, \
          plugin, provider, media_metadata, stream_id, resolution, ranking_profile_name) \
         VALUES ($1, 'media', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
         RETURNING *",
    )
    .bind(media_item_id)
    .bind(path)
    .bind(file_size)
    .bind(original_filename)
    .bind(download_url)
    .bind(stream_url)
    .bind(plugin)
    .bind(provider)
    .bind(&media_metadata)
    .bind(stream_id)
    .bind(resolution)
    .bind(ranking_profile_name)
    .fetch_one(pool)
    .await?;
    Ok(entry)
}

fn parse_filename_metadata(filename: &str) -> serde_json::Value {
    let parsed = riven_rank::parse(filename);

    let (width, height) = match parsed.resolution.to_lowercase().trim_end_matches('p') {
        "2160" | "4k" | "uhd" => (Some(3840_i64), Some(2160_i64)),
        "1440" | "2k" | "qhd" => (Some(2560_i64), Some(1440_i64)),
        "1080" | "fhd" => (Some(1920_i64), Some(1080_i64)),
        "720" | "hd" => (Some(1280_i64), Some(720_i64)),
        "480" | "sd" => (Some(854_i64), Some(480_i64)),
        _ => (None, None),
    };

    let hdr_type = parsed.hdr.first().cloned();
    let bit_depth: Option<i64> = parsed.bit_depth.as_deref().and_then(|b| {
        b.trim_end_matches("-bit").trim_end_matches("bit").trim().parse().ok()
    });

    let audio_tracks: Vec<serde_json::Value> = parsed
        .audio
        .iter()
        .map(|codec| serde_json::json!({ "codec": codec }))
        .collect();

    let container_formats: Vec<String> = parsed.container.into_iter().collect();

    serde_json::json!({
        "filename": filename,
        "parsed_title": parsed.parsed_title,
        "year": parsed.year,
        "video": {
            "codec": parsed.codec,
            "resolution_width": width,
            "resolution_height": height,
            "bit_depth": bit_depth,
            "hdr_type": hdr_type,
            "frame_rate": null
        },
        "audio_tracks": audio_tracks,
        "subtitle_tracks": [],
        "quality_source": parsed.quality,
        "bitrate": null,
        "duration": null,
        "is_remux": false,
        "is_proper": parsed.proper,
        "is_repack": parsed.repack,
        "container_format": container_formats,
        "data_source": "parsed"
    })
}

// ── VFS directory helpers ──
// All queries are single prefix-scans on the indexed `path` column, avoiding
// the N+1 pattern that previously made library scans slow.

async fn list_vfs_dirs_at_depth(pool: &PgPool, pattern: &str, depth: u32) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT DISTINCT split_part(path, '/', {depth}) \
         FROM filesystem_entries \
         WHERE path LIKE $1 AND entry_type = 'media' \
         ORDER BY 1"
    );
    let rows: Vec<Option<String>> = sqlx::query_scalar(&sql)
        .bind(pattern)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().flatten().collect())
}

/// Distinct movie directory names (e.g. "The Matrix (1999) {tmdb-603}") that
/// have at least one media file in the VFS.
pub async fn list_vfs_movie_dirs(pool: &PgPool) -> Result<Vec<String>> {
    list_vfs_dirs_at_depth(pool, "/movies/%/%", 3).await
}

/// Distinct show directory names that have at least one episode file in the VFS.
pub async fn list_vfs_show_dirs(pool: &PgPool) -> Result<Vec<String>> {
    list_vfs_dirs_at_depth(pool, "/shows/%/%/%", 3).await
}

/// Distinct season directory names (e.g. "Season 04") under the given show
/// directory path (e.g. "/shows/Breaking Bad (2008) {tvdb-81189}").
pub async fn list_vfs_season_dirs(pool: &PgPool, show_path: &str) -> Result<Vec<String>> {
    list_vfs_dirs_at_depth(pool, &format!("{show_path}/%/%"), 4).await
}

/// VFS file paths directly inside `dir_path` (one level deep only).
pub async fn list_vfs_file_paths(pool: &PgPool, dir_path: &str) -> Result<Vec<String>> {
    let pattern = format!("{dir_path}/%");
    Ok(sqlx::query_scalar!(
        "SELECT path FROM filesystem_entries \
         WHERE path LIKE $1 AND entry_type = 'media'",
        pattern
    )
    .fetch_all(pool)
    .await?)
}

pub async fn delete_filesystem_entry(pool: &PgPool, entry_id: i64) -> Result<bool> {
    let rows = sqlx::query_scalar::<_, i64>(
        "WITH deleted AS (DELETE FROM filesystem_entries WHERE id = $1 AND entry_type = 'media' RETURNING id) SELECT COUNT(*) FROM deleted"
    )
    .bind(entry_id)
    .fetch_one(pool)
    .await?;
    Ok(rows > 0)
}

pub async fn update_stream_url(pool: &PgPool, entry_id: i64, stream_url: &str) -> Result<()> {
    sqlx::query!(
        "UPDATE filesystem_entries SET stream_url = $2, updated_at = NOW() WHERE id = $1",
        entry_id,
        stream_url
    )
    .execute(pool)
    .await?;
    Ok(())
}
