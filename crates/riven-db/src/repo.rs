use anyhow::Result;
use chrono::{NaiveDate, Utc};
use riven_core::types::*;
use sqlx::PgPool;

use crate::entities::*;

// ── Media Item Queries ──

pub async fn get_media_item(pool: &PgPool, id: i64) -> Result<Option<MediaItem>> {
    let item = sqlx::query_as::<_, MediaItem>("SELECT * FROM media_items WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    Ok(item)
}

pub async fn get_media_item_by_imdb(pool: &PgPool, imdb_id: &str) -> Result<Option<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>("SELECT * FROM media_items WHERE imdb_id = $1")
        .bind(imdb_id)
        .fetch_optional(pool)
        .await?)
}

pub async fn get_media_item_by_tmdb(pool: &PgPool, tmdb_id: &str) -> Result<Option<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>("SELECT * FROM media_items WHERE tmdb_id = $1")
        .bind(tmdb_id)
        .fetch_optional(pool)
        .await?)
}

pub async fn get_media_item_by_tvdb(pool: &PgPool, tvdb_id: &str) -> Result<Option<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>("SELECT * FROM media_items WHERE tvdb_id = $1")
        .bind(tvdb_id)
        .fetch_optional(pool)
        .await?)
}

pub async fn list_movies(pool: &PgPool) -> Result<Vec<MediaItem>> {
    let items = sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items WHERE item_type = 'movie' ORDER BY title",
    )
    .fetch_all(pool)
    .await?;
    Ok(items)
}

pub async fn list_shows(pool: &PgPool) -> Result<Vec<MediaItem>> {
    let items = sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items WHERE item_type = 'show' ORDER BY title",
    )
    .fetch_all(pool)
    .await?;
    Ok(items)
}

pub async fn list_seasons(pool: &PgPool, show_id: i64) -> Result<Vec<MediaItem>> {
    let items = sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items WHERE item_type = 'season' AND parent_id = $1 ORDER BY season_number",
    )
    .bind(show_id)
    .fetch_all(pool)
    .await?;
    Ok(items)
}

pub async fn list_episodes(pool: &PgPool, season_id: i64) -> Result<Vec<MediaItem>> {
    let items = sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items WHERE item_type = 'episode' AND parent_id = $1 ORDER BY episode_number",
    )
    .bind(season_id)
    .fetch_all(pool)
    .await?;
    Ok(items)
}

pub async fn get_items_by_state(
    pool: &PgPool,
    state: MediaItemState,
    item_type: MediaItemType,
) -> Result<Vec<MediaItem>> {
    let items = sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items WHERE state = $1 AND item_type = $2",
    )
    .bind(state)
    .bind(item_type)
    .fetch_all(pool)
    .await?;
    Ok(items)
}

/// Fetch items ready for processing, respecting is_requested and a simple
/// backoff: items with failed_attempts > 0 are only retried once per hour.
pub async fn get_items_ready_for_processing(
    pool: &PgPool,
    state: MediaItemState,
    item_type: MediaItemType,
    limit: i64,
) -> Result<Vec<MediaItem>> {
    let items = sqlx::query_as::<_, MediaItem>(
        r#"SELECT * FROM media_items
           WHERE state = $1
             AND item_type = $2
             AND is_requested = true
             AND (
                 failed_attempts = 0
                 OR updated_at IS NULL
                 OR updated_at < NOW() - INTERVAL '1 hour'
             )
           ORDER BY failed_attempts ASC, created_at ASC
           LIMIT $3"#,
    )
    .bind(state)
    .bind(item_type)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(items)
}

/// Transition unreleased episodes/movies that have since aired back to indexed
/// so the scheduler picks them up for scraping.
pub async fn transition_unreleased_aired(pool: &PgPool) -> Result<u64> {
    let result = sqlx::query(
        r#"UPDATE media_items
           SET state = 'indexed', updated_at = NOW()
           WHERE state = 'unreleased'
             AND aired_at IS NOT NULL
             AND aired_at <= CURRENT_DATE
             AND is_requested = true"#,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Blacklist a stream (by info_hash) for a specific media item so the scheduler
/// will skip it and try the next available stream.
pub async fn blacklist_stream_by_hash(
    pool: &PgPool,
    media_item_id: i64,
    info_hash: &str,
) -> Result<()> {
    let stream_id = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM streams WHERE info_hash = $1",
    )
    .bind(info_hash)
    .fetch_optional(pool)
    .await?;

    if let Some(stream_id) = stream_id {
        sqlx::query(
            "INSERT INTO media_item_blacklisted_streams (media_item_id, stream_id)
             VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(media_item_id)
        .bind(stream_id)
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn increment_failed_attempts(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE media_items SET failed_attempts = failed_attempts + 1, updated_at = NOW() WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

// ── Media Item Mutations ──

/// Find an existing media item by type and any matching external ID.
pub async fn find_existing_media_item(
    pool: &PgPool,
    item_type: MediaItemType,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
) -> Result<Option<MediaItem>> {
    let item = sqlx::query_as::<_, MediaItem>(
        r#"SELECT * FROM media_items
           WHERE item_type = $1
             AND ((imdb_id = $2 AND $2 IS NOT NULL)
               OR (tmdb_id = $3 AND $3 IS NOT NULL)
               OR (tvdb_id = $4 AND $4 IS NOT NULL))
           LIMIT 1"#,
    )
    .bind(item_type)
    .bind(imdb_id)
    .bind(tmdb_id)
    .bind(tvdb_id)
    .fetch_optional(pool)
    .await?;
    Ok(item)
}

/// Returns `(item, was_created)`. `was_created` is false when an existing item was found.
pub async fn create_movie(
    pool: &PgPool,
    title: &str,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    item_request_id: Option<i64>,
) -> Result<(MediaItem, bool)> {
    if let Some(existing) = find_existing_media_item(pool, MediaItemType::Movie, imdb_id, tmdb_id, None).await? {
        return Ok((existing, false));
    }

    let now = Utc::now();
    let item = sqlx::query_as::<_, MediaItem>(
        r#"INSERT INTO media_items (title, imdb_id, tmdb_id, item_type, state, is_requested, created_at, item_request_id)
           VALUES ($1, $2, $3, 'movie', 'indexed', TRUE, $4, $5)
           RETURNING *"#,
    )
    .bind(title)
    .bind(imdb_id)
    .bind(tmdb_id)
    .bind(now)
    .bind(item_request_id)
    .fetch_one(pool)
    .await?;
    Ok((item, true))
}

/// Returns `(item, was_created)`. `was_created` is false when an existing item was found.
pub async fn create_show(
    pool: &PgPool,
    title: &str,
    imdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    item_request_id: Option<i64>,
) -> Result<(MediaItem, bool)> {
    if let Some(existing) = find_existing_media_item(pool, MediaItemType::Show, imdb_id, None, tvdb_id).await? {
        return Ok((existing, false));
    }

    let now = Utc::now();
    let item = sqlx::query_as::<_, MediaItem>(
        r#"INSERT INTO media_items (title, imdb_id, tvdb_id, item_type, state, is_requested, created_at, item_request_id)
           VALUES ($1, $2, $3, 'show', 'indexed', TRUE, $4, $5)
           RETURNING *"#,
    )
    .bind(title)
    .bind(imdb_id)
    .bind(tvdb_id)
    .bind(now)
    .bind(item_request_id)
    .fetch_one(pool)
    .await?;
    Ok((item, true))
}

pub async fn create_season(
    pool: &PgPool,
    show_id: i64,
    number: i32,
    title: Option<&str>,
    tvdb_id: Option<&str>,
    is_special: bool,
    item_request_id: Option<i64>,
    is_requested: bool,
) -> Result<MediaItem> {
    let now = Utc::now();
    let default_title = format!("Season {number:02}");
    let title_str = title.unwrap_or(&default_title);
    let item = sqlx::query_as::<_, MediaItem>(
        r#"INSERT INTO media_items (title, tvdb_id, item_type, state, season_number, is_special, parent_id, is_requested, created_at, item_request_id)
           VALUES ($1, $2, 'season', 'indexed', $3, $4, $5, $6, $7, $8)
           ON CONFLICT (parent_id, season_number) WHERE item_type = 'season'
           DO UPDATE SET
               title        = EXCLUDED.title,
               tvdb_id      = COALESCE(EXCLUDED.tvdb_id, media_items.tvdb_id),
               is_requested = EXCLUDED.is_requested OR media_items.is_requested,
               updated_at   = NOW()
           RETURNING *"#,
    )
    .bind(title_str)
    .bind(tvdb_id)
    .bind(number)
    .bind(is_special)
    .bind(show_id)
    .bind(is_requested)
    .bind(now)
    .bind(item_request_id)
    .fetch_one(pool)
    .await?;
    Ok(item)
}

pub async fn create_episode(
    pool: &PgPool,
    season_id: i64,
    number: i32,
    title: Option<&str>,
    tvdb_id: Option<&str>,
    aired_at: Option<NaiveDate>,
    runtime: Option<i32>,
    absolute_number: Option<i32>,
    item_request_id: Option<i64>,
    is_requested: bool,
    season_number: Option<i32>,
) -> Result<MediaItem> {
    let now = Utc::now();
    let default_title = format!("Episode {number:02}");
    let title_str = title.unwrap_or(&default_title);
    let state = match aired_at {
        Some(date) if date > Utc::now().date_naive() => MediaItemState::Unreleased,
        _ => MediaItemState::Indexed,
    };
    let item = sqlx::query_as::<_, MediaItem>(
        r#"INSERT INTO media_items (title, tvdb_id, item_type, state, episode_number, absolute_number, runtime, parent_id, aired_at, is_requested, season_number, created_at, item_request_id)
           VALUES ($1, $2, 'episode', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
           ON CONFLICT (parent_id, episode_number) WHERE item_type = 'episode'
           DO UPDATE SET
               title           = EXCLUDED.title,
               tvdb_id         = COALESCE(EXCLUDED.tvdb_id, media_items.tvdb_id),
               aired_at        = COALESCE(EXCLUDED.aired_at, media_items.aired_at),
               runtime         = COALESCE(EXCLUDED.runtime, media_items.runtime),
               absolute_number = COALESCE(EXCLUDED.absolute_number, media_items.absolute_number),
               season_number   = COALESCE(EXCLUDED.season_number, media_items.season_number),
               is_requested    = EXCLUDED.is_requested OR media_items.is_requested,
               -- Transition unreleased episodes that have since aired
               state = CASE
                   WHEN media_items.state = 'unreleased'
                    AND EXCLUDED.aired_at IS NOT NULL
                    AND EXCLUDED.aired_at <= CURRENT_DATE
                   THEN 'indexed'::media_item_state
                   ELSE media_items.state
               END,
               updated_at = NOW()
           RETURNING *"#,
    )
    .bind(title_str)
    .bind(tvdb_id)
    .bind(state)
    .bind(number)
    .bind(absolute_number)
    .bind(runtime)
    .bind(season_id)
    .bind(aired_at)
    .bind(is_requested)
    .bind(season_number)
    .bind(now)
    .bind(item_request_id)
    .fetch_one(pool)
    .await?;
    Ok(item)
}

fn to_json<T: serde::Serialize>(v: &T) -> serde_json::Value {
    serde_json::to_value(v).unwrap_or_default()
}

pub async fn update_media_item_index(
    pool: &PgPool,
    id: i64,
    indexed: &riven_core::types::IndexedMediaItem,
) -> Result<()> {
    let now = Utc::now();
    let genres_json = indexed.genres.as_ref().map(to_json);
    let aliases_json = indexed.aliases.as_ref().map(to_json);

    sqlx::query(
        r#"UPDATE media_items SET
            title = COALESCE($2, title),
            full_title = COALESCE($3, full_title),
            imdb_id = COALESCE($4, imdb_id),
            tvdb_id = COALESCE($5, tvdb_id),
            tmdb_id = COALESCE($6, tmdb_id),
            poster_path = COALESCE($7, poster_path),
            year = COALESCE($8, year),
            genres = COALESCE($9, genres),
            country = COALESCE($10, country),
            language = COALESCE($11, language),
            network = COALESCE($12, network),
            content_rating = COALESCE($13, content_rating),
            runtime = COALESCE($14, runtime),
            aliases = COALESCE($15, aliases),
            aired_at = COALESCE($16, aired_at),
            indexed_at = $17,
            updated_at = $17
           WHERE id = $1"#,
    )
    .bind(id)
    .bind(&indexed.title)
    .bind(&indexed.full_title)
    .bind(&indexed.imdb_id)
    .bind(&indexed.tvdb_id)
    .bind(&indexed.tmdb_id)
    .bind(&indexed.poster_path)
    .bind(indexed.year)
    .bind(genres_json)
    .bind(&indexed.country)
    .bind(&indexed.language)
    .bind(&indexed.network)
    .bind(indexed.content_rating)
    .bind(indexed.runtime)
    .bind(aliases_json)
    .bind(indexed.aired_at)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_media_item_state(
    pool: &PgPool,
    id: i64,
    state: MediaItemState,
) -> Result<()> {
    sqlx::query("UPDATE media_items SET state = $2, updated_at = NOW() WHERE id = $1")
        .bind(id)
        .bind(state)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_scraped(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE media_items SET scraped_at = NOW(), scraped_times = scraped_times + 1, updated_at = NOW() WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

// ── Stream Queries ──

pub async fn upsert_stream(
    pool: &PgPool,
    info_hash: &str,
    parsed_data: Option<serde_json::Value>,
    rank: Option<i64>,
) -> Result<Stream> {
    let stream = sqlx::query_as::<_, Stream>(
        r#"INSERT INTO streams (info_hash, parsed_data, rank)
           VALUES ($1, $2, $3)
           ON CONFLICT (info_hash) DO UPDATE SET
               parsed_data = COALESCE($2, streams.parsed_data),
               rank = COALESCE($3, streams.rank)
           RETURNING *"#,
    )
    .bind(info_hash)
    .bind(parsed_data)
    .bind(rank)
    .fetch_one(pool)
    .await?;
    Ok(stream)
}

pub async fn link_stream_to_item(pool: &PgPool, media_item_id: i64, stream_id: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO media_item_streams (media_item_id, stream_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(media_item_id)
    .bind(stream_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_streams_for_item(pool: &PgPool, media_item_id: i64) -> Result<Vec<Stream>> {
    let streams = sqlx::query_as::<_, Stream>(
        r#"SELECT s.* FROM streams s
           JOIN media_item_streams ms ON s.id = ms.stream_id
           WHERE ms.media_item_id = $1"#,
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?;
    Ok(streams)
}

pub async fn get_non_blacklisted_streams(
    pool: &PgPool,
    media_item_id: i64,
) -> Result<Vec<Stream>> {
    let streams = sqlx::query_as::<_, Stream>(
        r#"SELECT s.* FROM streams s
           JOIN media_item_streams ms ON s.id = ms.stream_id
           WHERE ms.media_item_id = $1
           AND s.id NOT IN (
               SELECT stream_id FROM media_item_blacklisted_streams WHERE media_item_id = $1
           )
           ORDER BY s.rank DESC NULLS LAST"#,
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?;
    Ok(streams)
}

/// Fetch only the highest-ranked non-blacklisted stream for an item.
/// Use this instead of `get_non_blacklisted_streams(...).into_iter().next()` —
/// it fetches a single row from the database instead of the full list.
pub async fn get_best_stream(pool: &PgPool, media_item_id: i64) -> Result<Option<Stream>> {
    let stream = sqlx::query_as::<_, Stream>(
        r#"SELECT s.* FROM streams s
           JOIN media_item_streams ms ON s.id = ms.stream_id
           WHERE ms.media_item_id = $1
           AND s.id NOT IN (
               SELECT stream_id FROM media_item_blacklisted_streams WHERE media_item_id = $1
           )
           ORDER BY s.rank DESC NULLS LAST
           LIMIT 1"#,
    )
    .bind(media_item_id)
    .fetch_optional(pool)
    .await?;
    Ok(stream)
}

// ── Filesystem Entry Queries ──

pub async fn get_filesystem_entries(
    pool: &PgPool,
    media_item_id: i64,
) -> Result<Vec<FileSystemEntry>> {
    let entries = sqlx::query_as::<_, FileSystemEntry>(
        "SELECT * FROM filesystem_entries WHERE media_item_id = $1",
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?;
    Ok(entries)
}

pub async fn get_media_entries(
    pool: &PgPool,
    media_item_id: i64,
) -> Result<Vec<FileSystemEntry>> {
    let entries = sqlx::query_as::<_, FileSystemEntry>(
        "SELECT * FROM filesystem_entries WHERE media_item_id = $1 AND entry_type = 'media'",
    )
    .bind(media_item_id)
    .fetch_all(pool)
    .await?;
    Ok(entries)
}

pub async fn get_media_entry_by_path(
    pool: &PgPool,
    path: &str,
) -> Result<Option<FileSystemEntry>> {
    let entry = sqlx::query_as::<_, FileSystemEntry>(
        "SELECT * FROM filesystem_entries WHERE path = $1 AND entry_type = 'media'",
    )
    .bind(path)
    .fetch_optional(pool)
    .await?;
    Ok(entry)
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
) -> Result<FileSystemEntry> {
    let media_metadata = parse_filename_metadata(original_filename);

    // Check for existing entry with same media_item_id and path to avoid duplicates
    let existing = sqlx::query_as::<_, FileSystemEntry>(
        "SELECT * FROM filesystem_entries WHERE media_item_id = $1 AND path = $2 AND entry_type = 'media'",
    )
    .bind(media_item_id)
    .bind(path)
    .fetch_optional(pool)
    .await?;

    if let Some(entry) = existing {
        // Update existing entry with new download/stream URLs
        let updated = sqlx::query_as::<_, FileSystemEntry>(
            r#"UPDATE filesystem_entries
               SET file_size = $1, original_filename = $2, download_url = $3, stream_url = $4,
                   plugin = $5, provider = $6, media_metadata = $7, updated_at = NOW()
               WHERE id = $8
               RETURNING *"#,
        )
        .bind(file_size)
        .bind(original_filename)
        .bind(download_url)
        .bind(stream_url)
        .bind(plugin)
        .bind(provider)
        .bind(&media_metadata)
        .bind(entry.id)
        .fetch_one(pool)
        .await?;
        return Ok(updated);
    }

    let entry = sqlx::query_as::<_, FileSystemEntry>(
        r#"INSERT INTO filesystem_entries
           (media_item_id, entry_type, path, file_size, original_filename, download_url, stream_url, plugin, provider, media_metadata)
           VALUES ($1, 'media', $2, $3, $4, $5, $6, $7, $8, $9)
           RETURNING *"#,
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

pub async fn update_stream_url(pool: &PgPool, entry_id: i64, stream_url: &str) -> Result<()> {
    sqlx::query("UPDATE filesystem_entries SET stream_url = $2, updated_at = NOW() WHERE id = $1")
        .bind(entry_id)
        .bind(stream_url)
        .execute(pool)
        .await?;
    Ok(())
}

// ── Item Request Queries ──

pub async fn get_item_request_by_id(pool: &PgPool, id: i64) -> Result<Option<ItemRequest>> {
    let request =
        sqlx::query_as::<_, ItemRequest>("SELECT * FROM item_requests WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(request)
}

/// Find an existing item request by any matching external ID.
pub async fn find_existing_item_request(
    pool: &PgPool,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
) -> Result<Option<ItemRequest>> {
    let request = sqlx::query_as::<_, ItemRequest>(
        r#"SELECT * FROM item_requests
           WHERE (imdb_id = $1 AND $1 IS NOT NULL)
              OR (tmdb_id = $2 AND $2 IS NOT NULL)
              OR (tvdb_id = $3 AND $3 IS NOT NULL)
           LIMIT 1"#,
    )
    .bind(imdb_id)
    .bind(tmdb_id)
    .bind(tvdb_id)
    .fetch_optional(pool)
    .await?;
    Ok(request)
}

pub async fn create_item_request(
    pool: &PgPool,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    request_type: ItemRequestType,
    requested_by: Option<&str>,
    external_request_id: Option<&str>,
    seasons: Option<&[i32]>,
) -> Result<ItemRequest> {
    if let Some(existing) = find_existing_item_request(pool, imdb_id, tmdb_id, tvdb_id).await? {
        // For shows: if new seasons are specified and the existing request has a seasons list,
        // merge them so "Request More" adds to the existing request rather than being a no-op.
        if let Some(new_seasons) = seasons {
            if !new_seasons.is_empty() {
                // If existing.seasons is null it means "all seasons" — keep null (already covers everything).
                // Otherwise merge the arrays.
                if let Some(ref existing_seasons_val) = existing.seasons {
                    let existing_vec: Vec<i32> = serde_json::from_value(existing_seasons_val.clone())
                        .unwrap_or_default();
                    let mut seen: std::collections::HashSet<i32> = existing_vec.iter().copied().collect();
                    let mut merged = existing_vec;
                    for &s in new_seasons {
                        if seen.insert(s) {
                            merged.push(s);
                        }
                    }
                    merged.sort_unstable();
                    let merged_json = serde_json::to_value(&merged).unwrap_or_default();
                    let updated = sqlx::query_as::<_, ItemRequest>(
                        "UPDATE item_requests SET seasons = $1 WHERE id = $2 RETURNING *",
                    )
                    .bind(merged_json)
                    .bind(existing.id)
                    .fetch_one(pool)
                    .await?;
                    return Ok(updated);
                }
            }
        }
        return Ok(existing);
    }

    let seasons_json = seasons.map(|s| serde_json::to_value(s).unwrap_or_default());
    let request = sqlx::query_as::<_, ItemRequest>(
        r#"INSERT INTO item_requests (imdb_id, tmdb_id, tvdb_id, request_type, requested_by, external_request_id, seasons)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           RETURNING *"#,
    )
    .bind(imdb_id)
    .bind(tmdb_id)
    .bind(tvdb_id)
    .bind(request_type)
    .bind(requested_by)
    .bind(external_request_id)
    .bind(seasons_json)
    .fetch_one(pool)
    .await?;
    Ok(request)
}

// ── State Computation ──

/// Compute the correct state for a media item based on its children/entries.
pub async fn compute_state(pool: &PgPool, item: &MediaItem) -> Result<MediaItemState> {
    match item.item_type {
        MediaItemType::Movie | MediaItemType::Episode => {
            let has_media = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM filesystem_entries WHERE media_item_id = $1 AND entry_type = 'media')",
            )
            .bind(item.id)
            .fetch_one(pool)
            .await?;

            if has_media {
                return Ok(MediaItemState::Completed);
            }

            let has_streams = sqlx::query_scalar::<_, bool>(
                r#"SELECT EXISTS(
                    SELECT 1 FROM media_item_streams ms
                    WHERE ms.media_item_id = $1
                    AND ms.stream_id NOT IN (
                        SELECT stream_id FROM media_item_blacklisted_streams WHERE media_item_id = $1
                    )
                )"#,
            )
            .bind(item.id)
            .fetch_one(pool)
            .await?;

            if has_streams {
                return Ok(MediaItemState::Scraped);
            }

            if let Some(aired) = item.aired_at {
                if aired > chrono::Utc::now().date_naive() {
                    return Ok(MediaItemState::Unreleased);
                }
            }

            Ok(MediaItemState::Indexed)
        }

        MediaItemType::Season => {
            let episodes = list_episodes(pool, item.id).await?;
            if episodes.is_empty() {
                return Ok(MediaItemState::Indexed);
            }
            aggregate_child_states(&episodes)
        }

        MediaItemType::Show => {
            let seasons = list_seasons(pool, item.id).await?;
            if seasons.is_empty() {
                return Ok(MediaItemState::Indexed);
            }
            let state = aggregate_child_states(&seasons)?;
            // If all completed but show is continuing, mark as ongoing
            if state == MediaItemState::Completed
                && item.show_status == Some(ShowStatus::Continuing)
            {
                return Ok(MediaItemState::Ongoing);
            }
            Ok(state)
        }
    }
}

fn aggregate_child_states(children: &[MediaItem]) -> Result<MediaItemState> {
    if children.is_empty() {
        return Ok(MediaItemState::Indexed);
    }

    // All completed → Completed
    if children.iter().all(|c| c.state == MediaItemState::Completed) {
        return Ok(MediaItemState::Completed);
    }

    // All unreleased → Unreleased (entire season/show hasn't aired yet)
    if children.iter().all(|c| c.state == MediaItemState::Unreleased) {
        return Ok(MediaItemState::Unreleased);
    }

    // Mix of completed + anything else → PartiallyCompleted
    let any_completed = children
        .iter()
        .any(|c| matches!(c.state, MediaItemState::Completed | MediaItemState::PartiallyCompleted));
    if any_completed {
        return Ok(MediaItemState::PartiallyCompleted);
    }

    // Any ongoing download in progress
    if children.iter().any(|c| c.state == MediaItemState::Ongoing) {
        return Ok(MediaItemState::Ongoing);
    }

    // Any scraped → ready to download
    if children.iter().any(|c| c.state == MediaItemState::Scraped) {
        return Ok(MediaItemState::Scraped);
    }

    // Mix of unreleased + indexed with nothing ready → Unreleased
    // (some aired, some not — treat the season as still pending release)
    if children.iter().any(|c| c.state == MediaItemState::Unreleased) {
        return Ok(MediaItemState::Unreleased);
    }

    Ok(MediaItemState::Indexed)
}

/// Cascade state updates from an episode up through season and show.
pub async fn cascade_state_update(pool: &PgPool, item: &MediaItem) -> Result<()> {
    if item.item_type == MediaItemType::Episode {
        if let Some(season_id) = item.parent_id {
            if let Some(season) = get_media_item(pool, season_id).await? {
                let new_state = compute_state(pool, &season).await?;
                if new_state != season.state {
                    update_media_item_state(pool, season.id, new_state).await?;
                    // Cascade to show
                    if let Some(show_id) = season.parent_id {
                        if let Some(show) = get_media_item(pool, show_id).await? {
                            let show_state = compute_state(pool, &show).await?;
                            if show_state != show.state {
                                update_media_item_state(pool, show.id, show_state).await?;
                            }
                        }
                    }
                }
            }
        }
    } else if item.item_type == MediaItemType::Season {
        if let Some(show_id) = item.parent_id {
            if let Some(show) = get_media_item(pool, show_id).await? {
                let show_state = compute_state(pool, &show).await?;
                if show_state != show.state {
                    update_media_item_state(pool, show.id, show_state).await?;
                }
            }
        }
    }
    Ok(())
}

// ── Retry / Maintenance ──

/// Reset `failed_attempts` to 0 for all requested items whose last update is
/// older than `older_than_secs`. This unblocks items that have been stuck
/// waiting for backoff to expire, scheduling a fresh scrape cycle.
pub async fn reset_failed_items(pool: &PgPool, older_than_secs: u64) -> Result<u64> {
    let cutoff = Utc::now() - chrono::Duration::seconds(older_than_secs as i64);
    let result = sqlx::query(
        r#"UPDATE media_items
           SET failed_attempts = 0, updated_at = NOW()
           WHERE is_requested = true
             AND failed_attempts > 0
             AND updated_at < $1"#,
    )
    .bind(cutoff)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

// ── Calendar Queries ──

/// Fetch upcoming unreleased items with the show title resolved in a single JOIN.
/// Replaces the N+1 pattern of calling get_media_item per episode/season.
pub async fn get_calendar_entries(pool: &PgPool, limit: i64) -> Result<Vec<crate::entities::CalendarRow>> {
    let rows = sqlx::query_as::<_, crate::entities::CalendarRow>(
        r#"SELECT
               mi.id,
               mi.item_type,
               mi.state,
               mi.title,
               COALESCE(
                   CASE
                       WHEN mi.item_type = 'episode' THEN grandparent.title
                       WHEN mi.item_type = 'season'  THEN parent.title
                   END,
                   mi.title
               ) AS show_title,
               mi.aired_at,
               mi.season_number,
               mi.episode_number,
               -- Use the ancestor show's IDs for detail-page links, not the
               -- episode/season's own IDs (which would 404 on TVDB/TMDB).
               COALESCE(
                   CASE
                       WHEN mi.item_type = 'episode' THEN grandparent.tmdb_id
                       WHEN mi.item_type = 'season'  THEN parent.tmdb_id
                   END,
                   mi.tmdb_id
               ) AS tmdb_id,
               COALESCE(
                   CASE
                       WHEN mi.item_type = 'episode' THEN grandparent.tvdb_id
                       WHEN mi.item_type = 'season'  THEN parent.tvdb_id
                   END,
                   mi.tvdb_id
               ) AS tvdb_id
           FROM media_items mi
           LEFT JOIN media_items parent      ON parent.id = mi.parent_id
           LEFT JOIN media_items grandparent ON grandparent.id = parent.parent_id
           WHERE mi.aired_at > CURRENT_DATE
             AND mi.state = 'unreleased'
           ORDER BY mi.aired_at ASC
           LIMIT $1"#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Fetch all requested items with a future air date as full MediaItem rows.
/// Used by the iCal feed generator (needs full MediaItem for build_ical).
pub async fn get_upcoming_unreleased(pool: &PgPool, limit: i64) -> Result<Vec<MediaItem>> {
    let items = sqlx::query_as::<_, MediaItem>(
        r#"SELECT * FROM media_items
           WHERE aired_at > CURRENT_DATE
             AND state = 'unreleased'
           ORDER BY aired_at ASC
           LIMIT $1"#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(items)
}

/// Count media items grouped by release year.
/// Movies use their `year` field; episodes derive the year from `aired_at`
/// (the `year` column is never populated for episodes).
/// Shows and seasons are excluded so the chart reflects watchable content.
pub async fn get_year_releases(pool: &PgPool) -> Result<Vec<(i32, i64)>> {
    let rows: Vec<(i32, i64)> = sqlx::query_as(
        r#"SELECT
               COALESCE(year, EXTRACT(YEAR FROM aired_at)::int) AS year,
               COUNT(*)::bigint AS count
           FROM media_items
           WHERE item_type IN ('movie', 'episode')
             AND COALESCE(year, EXTRACT(YEAR FROM aired_at)::int) IS NOT NULL
           GROUP BY 1
           ORDER BY 1 ASC"#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// ── Settings Queries ──

pub async fn get_setting(pool: &PgPool, key: &str) -> Result<Option<serde_json::Value>> {
    let row = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT value FROM settings WHERE key = $1",
    )
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

pub async fn set_setting(pool: &PgPool, key: &str, value: serde_json::Value) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO settings (key, value, updated_at)
           VALUES ($1, $2, NOW())
           ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()"#,
    )
    .bind(key)
    .bind(value)
    .execute(pool)
    .await?;
    Ok(())
}

// ── Paginated Items ──

fn apply_item_filters(
    qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>,
    types: Option<&[MediaItemType]>,
    search: Option<&str>,
    states: Option<&[MediaItemState]>,
) {
    if let Some(t) = types {
        if !t.is_empty() {
            qb.push(" AND item_type = ANY(");
            qb.push_bind(t.to_vec());
            qb.push(")");
        }
    }
    if let Some(s) = states {
        if !s.is_empty() {
            qb.push(" AND state = ANY(");
            qb.push_bind(s.to_vec());
            qb.push(")");
        }
    }
    if let Some(q) = search {
        if !q.is_empty() {
            qb.push(" AND LOWER(title) LIKE ");
            qb.push_bind(format!("%{}%", q.to_lowercase()));
        }
    }
}

pub async fn list_items_paginated(
    pool: &PgPool,
    page: i64,
    limit: i64,
    sort: Option<String>,
    types: Option<Vec<MediaItemType>>,
    search: Option<String>,
    states: Option<Vec<MediaItemState>>,
) -> Result<Vec<MediaItem>> {
    let page = page.max(1);
    let limit = limit.clamp(1, 200);
    let offset = (page - 1) * limit;

    let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT * FROM media_items WHERE 1=1");
    apply_item_filters(&mut qb, types.as_deref(), search.as_deref(), states.as_deref());

    let order = match sort.as_deref() {
        Some("date_asc") => " ORDER BY created_at ASC NULLS LAST",
        Some("title_asc") => " ORDER BY title ASC",
        Some("title_desc") => " ORDER BY title DESC",
        _ => " ORDER BY created_at DESC NULLS LAST",
    };
    qb.push(order);
    qb.push(" LIMIT ");
    qb.push_bind(limit);
    qb.push(" OFFSET ");
    qb.push_bind(offset);

    let items = qb.build_query_as::<MediaItem>().fetch_all(pool).await?;
    Ok(items)
}

pub async fn count_items_filtered(
    pool: &PgPool,
    types: Option<Vec<MediaItemType>>,
    search: Option<String>,
    states: Option<Vec<MediaItemState>>,
) -> Result<i64> {
    let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "SELECT COUNT(*) FROM media_items WHERE 1=1",
    );
    apply_item_filters(&mut qb, types.as_deref(), search.as_deref(), states.as_deref());
    let count = qb.build_query_scalar::<i64>().fetch_one(pool).await?;
    Ok(count)
}

// ── Statistics ──

#[derive(Debug, sqlx::FromRow)]
struct StatsRow {
    total_movies: i64,
    total_shows: i64,
    total_seasons: i64,
    total_episodes: i64,
    completed: i64,
    scraped: i64,
    indexed: i64,
    failed: i64,
    paused: i64,
    ongoing: i64,
    partially_completed: i64,
    unreleased: i64,
}

pub struct MediaStats {
    pub total_movies: i64,
    pub total_shows: i64,
    pub total_seasons: i64,
    pub total_episodes: i64,
    pub completed: i64,
    pub scraped: i64,
    pub indexed: i64,
    pub failed: i64,
    pub paused: i64,
    pub ongoing: i64,
    pub partially_completed: i64,
    pub unreleased: i64,
}

pub async fn get_stats(pool: &PgPool) -> Result<MediaStats> {
    let row = sqlx::query_as::<_, StatsRow>(
        r#"SELECT
             COUNT(*) FILTER (WHERE item_type = 'movie') AS total_movies,
             COUNT(*) FILTER (WHERE item_type = 'show') AS total_shows,
             COUNT(*) FILTER (WHERE item_type = 'season') AS total_seasons,
             COUNT(*) FILTER (WHERE item_type = 'episode') AS total_episodes,
             COUNT(*) FILTER (WHERE state = 'completed') AS completed,
             COUNT(*) FILTER (WHERE state = 'scraped') AS scraped,
             COUNT(*) FILTER (WHERE state = 'indexed') AS indexed,
             COUNT(*) FILTER (WHERE state = 'failed') AS failed,
             COUNT(*) FILTER (WHERE state = 'paused') AS paused,
             COUNT(*) FILTER (WHERE state = 'ongoing') AS ongoing,
             COUNT(*) FILTER (WHERE state = 'partially_completed') AS partially_completed,
             COUNT(*) FILTER (WHERE state = 'unreleased') AS unreleased
           FROM media_items"#,
    )
    .fetch_one(pool)
    .await?;

    Ok(MediaStats {
        total_movies: row.total_movies,
        total_shows: row.total_shows,
        total_seasons: row.total_seasons,
        total_episodes: row.total_episodes,
        completed: row.completed,
        scraped: row.scraped,
        indexed: row.indexed,
        failed: row.failed,
        paused: row.paused,
        ongoing: row.ongoing,
        partially_completed: row.partially_completed,
        unreleased: row.unreleased,
    })
}

/// Returns a map of ISO date strings (YYYY-MM-DD) to count of items that
/// transitioned to `completed` on that date, covering the past year.
pub async fn get_activity(pool: &PgPool) -> Result<std::collections::HashMap<String, i64>> {
    let rows: Vec<(NaiveDate, i64)> = sqlx::query_as(
        r#"SELECT
             DATE(updated_at AT TIME ZONE 'UTC') AS date,
             COUNT(*)::bigint AS count
           FROM media_items
           WHERE state = 'completed'
             AND updated_at IS NOT NULL
             AND updated_at >= NOW() - INTERVAL '1 year'
           GROUP BY DATE(updated_at AT TIME ZONE 'UTC')"#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|(date, count)| (date.to_string(), count)).collect())
}

// ── Bulk state mutations ──

pub async fn reset_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE media_items SET state = 'indexed', failed_attempts = 0, updated_at = NOW() WHERE id = ANY($1)",
    )
    .bind(ids)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn retry_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE media_items SET failed_attempts = 0, updated_at = NOW() WHERE id = ANY($1)",
    )
    .bind(ids)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn delete_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    let result = sqlx::query("DELETE FROM media_items WHERE id = ANY($1)")
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

pub async fn pause_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE media_items SET state = 'paused', updated_at = NOW() WHERE id = ANY($1)",
    )
    .bind(ids)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn unpause_items_by_ids(pool: &PgPool, ids: Vec<i64>) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE media_items SET state = 'indexed', updated_at = NOW() WHERE id = ANY($1) AND state = 'paused'",
    )
    .bind(ids)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Mark specific seasons of a show as requested and return their indexed episodes
/// ready for immediate scraping (matches riven-ts `update.success` → `requestScrape` path).
pub async fn mark_seasons_requested_and_get_episodes(
    pool: &PgPool,
    show_id: i64,
    season_numbers: &[i32],
) -> Result<Vec<MediaItem>> {
    // Mark the season items as requested
    sqlx::query(
        r#"UPDATE media_items
           SET is_requested = true, updated_at = NOW()
           WHERE parent_id = $1
             AND item_type = 'season'
             AND season_number = ANY($2)"#,
    )
    .bind(show_id)
    .bind(season_numbers)
    .execute(pool)
    .await?;

    // Mark the episode items in those seasons as requested too
    sqlx::query(
        r#"UPDATE media_items
           SET is_requested = true, updated_at = NOW()
           WHERE parent_id IN (
               SELECT id FROM media_items
               WHERE parent_id = $1
                 AND item_type = 'season'
                 AND season_number = ANY($2)
           )
             AND item_type = 'episode'"#,
    )
    .bind(show_id)
    .bind(season_numbers)
    .execute(pool)
    .await?;

    // Return the indexed episodes ready for scraping
    let episodes = sqlx::query_as::<_, MediaItem>(
        r#"SELECT e.* FROM media_items e
           JOIN media_items s ON e.parent_id = s.id
           WHERE s.parent_id = $1
             AND s.item_type = 'season'
             AND s.season_number = ANY($2)
             AND e.item_type = 'episode'
             AND e.state = 'indexed'
             AND e.is_requested = true"#,
    )
    .bind(show_id)
    .bind(season_numbers)
    .fetch_all(pool)
    .await?;

    Ok(episodes)
}

/// Fetch all requested, non-completed seasons for a show.
/// Used by the scheduler to fan out scrape requests for show seasons.
pub async fn get_requested_seasons_for_show(pool: &PgPool, show_id: i64) -> Result<Vec<MediaItem>> {
    let seasons = sqlx::query_as::<_, MediaItem>(
        r#"SELECT * FROM media_items
           WHERE parent_id = $1
             AND item_type = 'season'
             AND is_requested = true
             AND state != 'completed'
           ORDER BY season_number ASC"#,
    )
    .bind(show_id)
    .fetch_all(pool)
    .await?;
    Ok(seasons)
}

/// Fetch requested seasons in scraped state for a show.
/// Used by the retry library to fan out download requests to scraped seasons.
pub async fn get_scraped_seasons_for_show(pool: &PgPool, show_id: i64) -> Result<Vec<MediaItem>> {
    let seasons = sqlx::query_as::<_, MediaItem>(
        r#"SELECT * FROM media_items
           WHERE parent_id = $1
             AND item_type = 'season'
             AND is_requested = true
             AND state = 'scraped'
           ORDER BY season_number ASC"#,
    )
    .bind(show_id)
    .fetch_all(pool)
    .await?;
    Ok(seasons)
}

/// Fetch incomplete (indexed/scraped/ongoing) requested episodes for a season.
/// Used by fanOutDownload when no season-pack is found, to fall back to
/// episode-level scraping.
pub async fn get_incomplete_episodes_for_season(pool: &PgPool, season_id: i64) -> Result<Vec<MediaItem>> {
    let episodes = sqlx::query_as::<_, MediaItem>(
        r#"SELECT * FROM media_items
           WHERE parent_id = $1
             AND item_type = 'episode'
             AND is_requested = true
             AND state = ANY(ARRAY['indexed'::media_item_state, 'scraped'::media_item_state, 'ongoing'::media_item_state])
           ORDER BY episode_number ASC"#,
    )
    .bind(season_id)
    .fetch_all(pool)
    .await?;
    Ok(episodes)
}

/// Fetch indexed, requested episodes ready for scraping, with the parent show's
/// imdb_id filled in via JOIN (episodes don't store imdb_id themselves).
pub async fn get_episodes_ready_for_scraping(pool: &PgPool, limit: i64) -> Result<Vec<MediaItem>> {
    let episodes = sqlx::query_as::<_, MediaItem>(
        r#"SELECT
               e.id, e.title, e.full_title,
               COALESCE(e.imdb_id, show_item.imdb_id) AS imdb_id,
               e.tvdb_id, e.tmdb_id, e.poster_path,
               e.created_at, e.updated_at, e.indexed_at, e.scraped_at, e.scraped_times,
               e.aliases, e.network, e.country, e.language, e.aired_at, e.year, e.genres,
               e.rating, e.content_rating, e.state, e.failed_attempts, e.item_type,
               e.is_requested, e.show_status, e.season_number, e.is_special, e.parent_id,
               e.episode_number, e.absolute_number, e.runtime, e.item_request_id, e.active_stream_id
           FROM media_items e
           LEFT JOIN media_items season_item ON e.parent_id = season_item.id AND season_item.item_type = 'season'
           LEFT JOIN media_items show_item ON season_item.parent_id = show_item.id AND show_item.item_type = 'show'
           WHERE e.state = 'indexed'
             AND e.item_type = 'episode'
             AND e.is_requested = true
             AND (
                 e.failed_attempts = 0
                 OR e.updated_at IS NULL
                 OR e.updated_at < NOW() - INTERVAL '1 hour'
             )
           ORDER BY e.failed_attempts ASC, e.created_at ASC
           LIMIT $1"#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(episodes)
}

/// Fetch all indexed, requested episodes that belong to a given show.
/// Used to immediately queue scraping after a show is indexed.
pub async fn get_indexed_episodes_for_show(pool: &PgPool, show_id: i64) -> Result<Vec<MediaItem>> {
    let episodes = sqlx::query_as::<_, MediaItem>(
        r#"SELECT e.* FROM media_items e
           JOIN media_items s ON e.parent_id = s.id
           WHERE s.parent_id = $1
             AND s.item_type = 'season'
             AND e.item_type = 'episode'
             AND e.state = 'indexed'
             AND e.is_requested = true"#,
    )
    .bind(show_id)
    .fetch_all(pool)
    .await?;
    Ok(episodes)
}

pub async fn add_media_item(
    pool: &PgPool,
    item_type: MediaItemType,
    title: String,
    imdb_id: Option<String>,
    tmdb_id: Option<String>,
    tvdb_id: Option<String>,
) -> Result<MediaItem> {
    match item_type {
        MediaItemType::Movie => {
            create_movie(pool, &title, imdb_id.as_deref(), tmdb_id.as_deref(), None)
                .await
                .map(|(item, _)| item)
        }
        MediaItemType::Show => {
            create_show(pool, &title, imdb_id.as_deref(), tvdb_id.as_deref(), None)
                .await
                .map(|(item, _)| item)
        }
        _ => Err(anyhow::anyhow!("Only Movie and Show types can be added directly")),
    }
}

// ── All Settings ──

pub async fn get_all_settings(pool: &PgPool) -> Result<serde_json::Value> {
    use sqlx::Row as _;
    let rows = sqlx::query("SELECT key, value FROM settings")
        .fetch_all(pool)
        .await?;

    let mut map = serde_json::Map::new();
    for row in rows {
        let key: String = row.try_get("key")?;
        let value: serde_json::Value = row.try_get("value")?;
        map.insert(key, value);
    }
    Ok(serde_json::Value::Object(map))
}

pub async fn set_all_settings(
    pool: &PgPool,
    settings: serde_json::Value,
) -> Result<serde_json::Value> {
    if let serde_json::Value::Object(ref map) = settings {
        for (key, value) in map {
            set_setting(pool, key, value.clone()).await?;
        }
        Ok(settings)
    } else {
        Err(anyhow::anyhow!("Settings must be a JSON object"))
    }
}
