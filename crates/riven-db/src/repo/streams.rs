use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, QueryBuilder};

use crate::entities::*;
use riven_rank::ResolutionRanks;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct VfsEntryPath {
    pub path: String,
    pub library_profiles: Option<serde_json::Value>,
}

pub async fn upsert_stream(
    pool: &PgPool,
    info_hash: &str,
    magnet: &str,
    parsed_data: Option<serde_json::Value>,
    rank: Option<i64>,
    file_size_bytes: Option<u64>,
) -> Result<Stream> {
    let stream = sqlx::query_as::<_, Stream>(
        "INSERT INTO streams (info_hash, magnet, parsed_data, rank, file_size_bytes) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (info_hash) DO UPDATE SET \
             magnet = CASE WHEN $2 <> '' THEN $2 ELSE streams.magnet END, \
             parsed_data = COALESCE($3, streams.parsed_data), \
             rank = COALESCE($4, streams.rank), \
             file_size_bytes = COALESCE($5, streams.file_size_bytes), \
             updated_at = NOW() \
         RETURNING *",
    )
    .bind(info_hash)
    .bind(magnet)
    .bind(parsed_data)
    .bind(rank)
    .bind(file_size_bytes.map(|s| s as i64))
    .fetch_one(pool)
    .await?;
    Ok(stream)
}

/// Record the actual file size for a stream (learned from a download attempt).
pub async fn update_stream_file_size(
    pool: &PgPool,
    info_hash: &str,
    file_size_bytes: u64,
) -> Result<()> {
    sqlx::query!(
        "UPDATE streams SET file_size_bytes = $1, updated_at = NOW() WHERE info_hash = $2",
        file_size_bytes as i64,
        info_hash
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn link_stream_to_item(
    pool: &PgPool,
    media_item_id: i64,
    stream_id: i64,
) -> Result<bool> {
    let result = sqlx::query!(
        "INSERT INTO media_item_streams (media_item_id, stream_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        media_item_id,
        stream_id
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn get_streams_for_item(pool: &PgPool, media_item_id: i64) -> Result<Vec<Stream>> {
    Ok(sqlx::query_as::<_, Stream>(
        "SELECT s.* FROM streams s JOIN media_item_streams ms ON s.id = ms.stream_id WHERE ms.media_item_id = $1",
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?)
}

pub async fn get_stream_for_item(
    pool: &PgPool,
    media_item_id: i64,
    stream_id: i64,
) -> Result<Option<Stream>> {
    Ok(sqlx::query_as::<_, Stream>(
        "SELECT s.* FROM streams s JOIN media_item_streams ms ON s.id = ms.stream_id WHERE ms.media_item_id = $1 AND s.id = $2 LIMIT 1",
    )
    .bind(media_item_id)
    .bind(stream_id)
    .fetch_optional(pool)
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
               s.rank DESC NULLS LAST,
               CASE COALESCE(s.parsed_data->>'resolution', 'unknown')
                   WHEN '2160p' THEN {r2160p}
                   WHEN '1440p' THEN {r1440p}
                   WHEN '1080p' THEN {r1080p}
                   WHEN '720p'  THEN {r720p}
                   WHEN '480p'  THEN {r480p}
                   WHEN '360p'  THEN {r360p}
                   WHEN 'unknown' THEN {unknown}
                   ELSE 0
               END DESC,
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

/// Load resolution ranks from the `rank_settings` DB key.
/// Called once at `JobQueue` startup and on settings reload — callers pass the
/// cached value into stream queries so each query doesn't re-hit the DB.
pub async fn load_resolution_ranks(pool: &PgPool) -> ResolutionRanks {
    match super::get_setting(pool, "rank_settings").await {
        Ok(Some(value)) => value
            .get("resolution_ranks")
            .and_then(|v| serde_json::from_value::<ResolutionRanks>(v.clone()).ok())
            .unwrap_or_default(),
        _ => ResolutionRanks::default(),
    }
}

pub async fn get_non_blacklisted_streams(
    pool: &PgPool,
    media_item_id: i64,
    ranks: &ResolutionRanks,
) -> Result<Vec<Stream>> {
    let sql = build_stream_query(ranks, false);
    Ok(sqlx::query_as::<_, Stream>(&sql)
        .bind(media_item_id)
        .fetch_all(pool)
        .await?)
}

/// Fetch only the highest-ranked non-blacklisted stream for an item.
pub async fn get_best_stream(
    pool: &PgPool,
    media_item_id: i64,
    ranks: &ResolutionRanks,
) -> Result<Option<Stream>> {
    let sql = build_stream_query(ranks, true);
    Ok(sqlx::query_as::<_, Stream>(&sql)
        .bind(media_item_id)
        .fetch_optional(pool)
        .await?)
}

pub async fn get_filesystem_entries(
    pool: &PgPool,
    media_item_id: i64,
) -> Result<Vec<FileSystemEntry>> {
    Ok(sqlx::query_as::<_, FileSystemEntry>(
        "SELECT * FROM filesystem_entries WHERE media_item_id = $1",
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?)
}

pub async fn get_media_entries(pool: &PgPool, media_item_id: i64) -> Result<Vec<FileSystemEntry>> {
    Ok(sqlx::query_as::<_, FileSystemEntry>(
        "SELECT * FROM filesystem_entries WHERE media_item_id = $1 AND entry_type = 'media'",
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?)
}

/// Like `get_media_entries` but walks the full media tree rooted at `root_id`.
/// Needed for season-level IDs where entries are stored on child episodes.
pub async fn get_media_entries_recursive(
    pool: &PgPool,
    root_id: i64,
) -> Result<Vec<FileSystemEntry>> {
    Ok(sqlx::query_as::<_, FileSystemEntry>(
        "WITH RECURSIVE media_tree AS (
             SELECT id FROM media_items WHERE id = $1
             UNION
             SELECT child.id
             FROM media_items child
             INNER JOIN media_tree parent ON child.parent_id = parent.id
         )
         SELECT fe.*
         FROM filesystem_entries fe
         INNER JOIN media_tree mt ON fe.media_item_id = mt.id
         WHERE fe.entry_type = 'media'",
    )
    .bind(root_id)
    .fetch_all(pool)
    .await?)
}

pub async fn get_media_entry_paths_for_items(
    pool: &PgPool,
    root_ids: &[i64],
) -> Result<Vec<String>> {
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }

    let rows: Vec<String> = sqlx::query_scalar(
        "WITH RECURSIVE media_tree AS (
             SELECT id FROM media_items WHERE id = ANY($1)
             UNION
             SELECT child.id
             FROM media_items child
             INNER JOIN media_tree parent ON child.parent_id = parent.id
         )
         SELECT fe.path
         FROM filesystem_entries fe
         INNER JOIN media_tree mt ON fe.media_item_id = mt.id
         WHERE fe.entry_type = 'media'
         ORDER BY fe.path",
    )
    .bind(root_ids)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn get_media_entry_by_path(pool: &PgPool, path: &str) -> Result<Option<FileSystemEntry>> {
    Ok(sqlx::query_as::<_, FileSystemEntry>(
        "SELECT * FROM filesystem_entries WHERE path = $1 AND entry_type = 'media'",
    )
    .bind(path)
    .fetch_optional(pool)
    .await?)
}

pub async fn get_media_entry_by_id(
    pool: &PgPool,
    entry_id: i64,
) -> Result<Option<FileSystemEntry>> {
    Ok(sqlx::query_as::<_, FileSystemEntry>(
        "SELECT * FROM filesystem_entries WHERE id = $1 AND entry_type = 'media'",
    )
    .bind(entry_id)
    .fetch_optional(pool)
    .await?)
}

/// Return the most likely next playback target for episodic content.
/// Movies and non-episodic items return `None`.
pub async fn get_next_playback_entry(
    pool: &PgPool,
    entry_id: i64,
) -> Result<Option<FileSystemEntry>> {
    Ok(sqlx::query_as::<_, FileSystemEntry>(
        r#"SELECT next_fe.*
           FROM filesystem_entries current_fe
           INNER JOIN media_items current_ep
               ON current_ep.id = current_fe.media_item_id
              AND current_ep.item_type = 'episode'
           INNER JOIN media_items current_season
               ON current_season.id = current_ep.parent_id
              AND current_season.item_type = 'season'
           INNER JOIN media_items next_season
               ON next_season.parent_id = current_season.parent_id
              AND next_season.item_type = 'season'
           INNER JOIN media_items next_ep
               ON next_ep.parent_id = next_season.id
              AND next_ep.item_type = 'episode'
           INNER JOIN filesystem_entries next_fe
               ON next_fe.media_item_id = next_ep.id
              AND next_fe.entry_type = 'media'
           WHERE current_fe.id = $1
             AND current_fe.entry_type = 'media'
             AND (
                   next_season.season_number > current_season.season_number
                OR (
                       next_season.season_number = current_season.season_number
                   AND next_ep.episode_number > current_ep.episode_number
                )
             )
           ORDER BY
               next_season.season_number ASC NULLS LAST,
               next_ep.episode_number ASC NULLS LAST,
               next_fe.id ASC
           LIMIT 1"#,
    )
    .bind(entry_id)
    .fetch_optional(pool)
    .await?)
}

pub async fn list_filesystem_profile_entry_candidates(
    pool: &PgPool,
) -> Result<Vec<FilesystemProfileEntryCandidate>> {
    Ok(sqlx::query_as::<_, FilesystemProfileEntryCandidate>(
        r#"SELECT
               fe.id,
               fe.library_profiles,
               CASE
                   WHEN item.item_type = 'movie' THEN 'movie'
                   ELSE 'show'
               END AS content_type,
               CASE
                   WHEN item.item_type = 'movie' THEN item.genres
                   ELSE show_item.genres
               END AS genres,
               CASE
                   WHEN item.item_type = 'movie' THEN item.network
                   ELSE show_item.network
               END AS network,
               CASE
                   WHEN item.item_type = 'movie' THEN item.content_rating
                   ELSE show_item.content_rating
               END AS content_rating,
               CASE
                   WHEN item.item_type = 'movie' THEN item.language
                   ELSE show_item.language
               END AS language,
               CASE
                   WHEN item.item_type = 'movie' THEN item.country
                   ELSE show_item.country
               END AS country,
               CASE
                   WHEN item.item_type = 'movie' THEN COALESCE(item.year, EXTRACT(YEAR FROM item.aired_at)::integer)
                   ELSE COALESCE(show_item.year, EXTRACT(YEAR FROM show_item.aired_at)::integer)
               END AS year,
               CASE
                   WHEN item.item_type = 'movie' THEN item.rating
                   ELSE show_item.rating
               END AS rating,
               CASE
                   WHEN item.item_type = 'movie' THEN item.is_anime
                   ELSE COALESCE(show_item.is_anime, false)
               END AS is_anime
           FROM filesystem_entries fe
           INNER JOIN media_items item ON item.id = fe.media_item_id
           LEFT JOIN media_items season_item
               ON item.parent_id = season_item.id
              AND season_item.item_type = 'season'
           LEFT JOIN media_items show_item
               ON (
                   (item.item_type = 'show' AND item.id = show_item.id)
                   OR (item.item_type = 'season'
                       AND item.parent_id = show_item.id
                       AND show_item.item_type = 'show')
                   OR (item.item_type = 'episode'
                       AND season_item.parent_id = show_item.id
                       AND show_item.item_type = 'show')
               )
           WHERE fe.entry_type = 'media'
           ORDER BY fe.id"#,
    )
    .fetch_all(pool)
    .await?)
}

/// Return the ranking profile names that already have a downloaded entry for this item.
pub async fn get_downloaded_profile_names(
    pool: &PgPool,
    media_item_id: i64,
) -> Result<Vec<String>> {
    let rows: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT DISTINCT ranking_profile_name FROM filesystem_entries \
         WHERE media_item_id = $1 AND entry_type = 'media' AND ranking_profile_name IS NOT NULL",
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().flatten().collect())
}

/// For a Season item, return profile names that have been downloaded for at
/// least one episode in that season.
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

/// Upsert a media filesystem entry, replacing the former SELECT + INSERT/UPDATE
/// two-round-trip pattern with a single statement.
///
/// Requires the partial unique index `idx_fs_entries_media_path_unique` on
/// `(media_item_id, path) WHERE entry_type = 'media'` (migration 011).
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
    library_profiles: Option<&serde_json::Value>,
) -> Result<FileSystemEntry> {
    let media_metadata = parse_filename_metadata(original_filename);

    let entry = sqlx::query_as::<_, FileSystemEntry>(
        "INSERT INTO filesystem_entries \
         (media_item_id, entry_type, path, file_size, original_filename, download_url, stream_url, \
          plugin, provider, media_metadata, stream_id, resolution, ranking_profile_name, library_profiles) \
         VALUES ($1, 'media', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
         ON CONFLICT (media_item_id, path) WHERE entry_type = 'media' \
         DO UPDATE SET \
             file_size             = EXCLUDED.file_size, \
             original_filename     = EXCLUDED.original_filename, \
             download_url          = COALESCE(EXCLUDED.download_url, filesystem_entries.download_url), \
             stream_url            = COALESCE(EXCLUDED.stream_url, filesystem_entries.stream_url), \
             plugin                = EXCLUDED.plugin, \
             provider              = EXCLUDED.provider, \
             media_metadata        = EXCLUDED.media_metadata, \
             stream_id             = COALESCE(EXCLUDED.stream_id, filesystem_entries.stream_id), \
             resolution            = COALESCE(EXCLUDED.resolution, filesystem_entries.resolution), \
             ranking_profile_name  = COALESCE(EXCLUDED.ranking_profile_name, filesystem_entries.ranking_profile_name), \
             library_profiles      = EXCLUDED.library_profiles, \
             updated_at            = NOW() \
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
    .bind(library_profiles)
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
        b.trim_end_matches("-bit")
            .trim_end_matches("bit")
            .trim()
            .parse()
            .ok()
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

pub async fn list_vfs_dir_names(
    pool: &PgPool,
    pattern: &str,
    depth: u32,
) -> Result<Vec<VfsDirName>> {
    let sql = format!(
        "SELECT split_part(path, '/', {depth}) AS name, library_profiles \
         FROM filesystem_entries \
         WHERE path LIKE $1 AND entry_type = 'media' \
         ORDER BY 1"
    );
    Ok(sqlx::query_as::<_, VfsDirName>(&sql)
        .bind(pattern)
        .fetch_all(pool)
        .await?)
}

pub async fn list_vfs_file_names(pool: &PgPool, dir_path: &str) -> Result<Vec<VfsFileName>> {
    let sql = "SELECT split_part(path, '/', array_length(string_to_array(trim(both '/' from $1), '/'), 1) + 2) AS name, library_profiles \
         FROM filesystem_entries \
         WHERE path LIKE ($1 || '/%') AND entry_type = 'media' \
         ORDER BY 1";
    Ok(sqlx::query_as::<_, VfsFileName>(sql)
        .bind(dir_path)
        .fetch_all(pool)
        .await?)
}

pub async fn list_vfs_entry_paths(pool: &PgPool, pattern: &str) -> Result<Vec<VfsEntryPath>> {
    Ok(sqlx::query_as::<_, VfsEntryPath>(
        "SELECT path, library_profiles FROM filesystem_entries \
         WHERE path LIKE $1 AND entry_type = 'media' \
         ORDER BY path",
    )
    .bind(pattern)
    .fetch_all(pool)
    .await?)
}

pub async fn list_vfs_movie_dirs(pool: &PgPool) -> Result<Vec<String>> {
    list_vfs_dirs_at_depth(pool, "/movies/%/%", 3).await
}

pub async fn list_vfs_show_dirs(pool: &PgPool) -> Result<Vec<String>> {
    list_vfs_dirs_at_depth(pool, "/shows/%/%/%", 3).await
}

pub async fn list_vfs_season_dirs(pool: &PgPool, show_path: &str) -> Result<Vec<String>> {
    list_vfs_dirs_at_depth(pool, &format!("{show_path}/%/%"), 4).await
}

// ── VFS stat helpers ──

/// Aggregate stat (timestamps + entry count) for all media entries under `path_prefix`.
/// A `path_prefix` of `""` covers all entries; `/movies` covers only movies, etc.
#[derive(sqlx::FromRow)]
pub struct VfsDirStatResult {
    pub ctime: Option<DateTime<Utc>>,
    pub mtime: Option<DateTime<Utc>>,
    pub entry_count: i64,
}

pub async fn get_vfs_dir_stat(pool: &PgPool, path_prefix: &str) -> Result<VfsDirStatResult> {
    let pattern = format!("{path_prefix}/%");
    Ok(sqlx::query_as::<_, VfsDirStatResult>(
        "SELECT \
           MIN(created_at) AS ctime, \
           MAX(COALESCE(updated_at, created_at)) AS mtime, \
           COUNT(*) AS entry_count \
         FROM filesystem_entries \
         WHERE path LIKE $1 AND entry_type = 'media'",
    )
    .bind(pattern)
    .fetch_one(pool)
    .await?)
}

/// Count distinct directory names at `depth` (1-based split_part index) for entries
/// matching `pattern`.
pub async fn count_vfs_distinct_dirs(pool: &PgPool, pattern: &str, depth: u32) -> Result<i64> {
    let sql = format!(
        "SELECT COUNT(DISTINCT split_part(path, '/', {depth})) \
         FROM filesystem_entries \
         WHERE path LIKE $1 AND entry_type = 'media'"
    );
    Ok(sqlx::query_scalar::<_, i64>(&sql)
        .bind(pattern)
        .fetch_one(pool)
        .await?)
}

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

/// Returns `(was_deleted, owning_media_item_id)`. The id is captured in the
/// same DELETE so callers can recompute that item's state — losing a media
/// entry can flip Completed → Scraped/Indexed.
pub async fn delete_filesystem_entry(
    pool: &PgPool,
    entry_id: i64,
) -> Result<(bool, Option<i64>)> {
    let row: Option<(i64,)> = sqlx::query_as(
        "DELETE FROM filesystem_entries \
         WHERE id = $1 AND entry_type = 'media' \
         RETURNING media_item_id",
    )
    .bind(entry_id)
    .fetch_optional(pool)
    .await?;
    Ok((row.is_some(), row.map(|r| r.0)))
}

pub async fn update_stream_url(pool: &PgPool, entry_id: i64, stream_url: &str) -> Result<()> {
    sqlx::query("UPDATE filesystem_entries SET stream_url = $2 WHERE id = $1")
        .bind(entry_id)
        .bind(stream_url)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_library_profiles(
    pool: &PgPool,
    entry_id: i64,
    library_profiles: &serde_json::Value,
) -> Result<()> {
    sqlx::query(
        "UPDATE filesystem_entries SET library_profiles = $2, updated_at = NOW() WHERE id = $1",
    )
    .bind(entry_id)
    .bind(library_profiles)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_library_profiles_batch(
    pool: &PgPool,
    updates: &[(i64, serde_json::Value)],
) -> Result<u64> {
    if updates.is_empty() {
        return Ok(0);
    }

    let mut total = 0_u64;

    for chunk in updates.chunks(500) {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "UPDATE filesystem_entries AS fe \
             SET library_profiles = data.library_profiles, updated_at = NOW() \
             FROM (",
        );

        query_builder.push_values(chunk, |mut builder, (entry_id, library_profiles)| {
            builder
                .push_bind(*entry_id)
                .push(", ")
                .push_bind(library_profiles);
        });

        query_builder.push(
            ") AS data(id, library_profiles) \
             WHERE fe.id = data.id",
        );

        total += query_builder.build().execute(pool).await?.rows_affected();
    }

    Ok(total)
}
