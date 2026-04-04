use anyhow::Result;
use chrono::Utc;
use riven_core::types::*;
use sqlx::PgPool;

use crate::entities::*;

/// Shared INSERT … ON CONFLICT … RETURNING * implementation for top-level items
/// (movies and shows). `second_id_col` is either `"tmdb_id"` or `"tvdb_id"`;
/// `item_type` is either `"movie"` or `"show"`.
async fn upsert_top_level_item(
    pool: &PgPool,
    title: &str,
    imdb_id: Option<&str>,
    second_id_col: &'static str,
    second_id_val: Option<&str>,
    item_type: &'static str,
    item_request_id: Option<i64>,
) -> Result<(MediaItem, bool)> {
    let (type_val, tmdb_id, tvdb_id) = match item_type {
        "movie" => (MediaItemType::Movie, second_id_val, None),
        _ => (MediaItemType::Show, None, second_id_val),
    };
    if let Some(existing) =
        find_existing_media_item(pool, type_val, imdb_id, tmdb_id, tvdb_id).await?
    {
        // Update is_requested and link to the current request when not already set.
        let needs_update = !existing.is_requested
            || (item_request_id.is_some() && existing.item_request_id != item_request_id);
        if needs_update {
            sqlx::query(
                "UPDATE media_items \
                 SET is_requested = TRUE, \
                     item_request_id = COALESCE($1, item_request_id), \
                     updated_at = NOW() \
                 WHERE id = $2",
            )
            .bind(item_request_id)
            .bind(existing.id)
            .execute(pool)
            .await?;
        }
        return Ok((existing, false));
    }
    let sql = format!(
        "INSERT INTO media_items (title, imdb_id, {second_id_col}, item_type, state, is_requested, created_at, item_request_id) \
         VALUES ($1, $2, $3, '{item_type}', 'indexed', TRUE, $4, $5) \
         RETURNING *"
    );
    let item = sqlx::query_as::<_, MediaItem>(&sql)
        .bind(title)
        .bind(imdb_id)
        .bind(second_id_val)
        .bind(Utc::now())
        .bind(item_request_id)
        .fetch_one(pool)
        .await?;
    Ok((item, true))
}

pub async fn get_media_item(pool: &PgPool, id: i64) -> Result<Option<MediaItem>> {
    Ok(
        sqlx::query_as::<_, MediaItem>("SELECT * FROM media_items WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?,
    )
}

pub async fn get_media_item_by_imdb(pool: &PgPool, id: &str) -> Result<Option<MediaItem>> {
    Ok(
        sqlx::query_as::<_, MediaItem>("SELECT * FROM media_items WHERE imdb_id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?,
    )
}

pub async fn get_media_item_by_tmdb(pool: &PgPool, id: &str) -> Result<Option<MediaItem>> {
    Ok(
        sqlx::query_as::<_, MediaItem>("SELECT * FROM media_items WHERE tmdb_id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?,
    )
}

pub async fn get_media_item_by_tvdb(pool: &PgPool, id: &str) -> Result<Option<MediaItem>> {
    Ok(
        sqlx::query_as::<_, MediaItem>("SELECT * FROM media_items WHERE tvdb_id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?,
    )
}

pub async fn list_movies(pool: &PgPool) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items WHERE item_type = 'movie' ORDER BY title",
    )
    .fetch_all(pool)
    .await?)
}

pub async fn list_shows(pool: &PgPool) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items WHERE item_type = 'show' ORDER BY title",
    )
    .fetch_all(pool)
    .await?)
}

pub async fn get_items_by_state(
    pool: &PgPool,
    state: MediaItemState,
    item_type: MediaItemType,
) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items WHERE state = $1 AND item_type = $2",
    )
    .bind(state)
    .bind(item_type)
    .fetch_all(pool)
    .await?)
}

/// Fetch items ready for processing by a single state. Used for Ongoing shows
/// and other single-state lookups.
pub async fn get_items_ready_for_processing(
    pool: &PgPool,
    state: MediaItemState,
    item_type: MediaItemType,
    limit: i64,
) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items
         WHERE state = $1 AND item_type = $2 AND is_requested = true
         ORDER BY failed_attempts ASC, created_at ASC
         LIMIT $3",
    )
    .bind(state)
    .bind(item_type)
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

/// Fetch all pending top-level items needing a retry: Indexed, Scraped, or PartiallyCompleted.
pub async fn get_pending_items_for_retry(
    pool: &PgPool,
    item_type: MediaItemType,
    limit: i64,
) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items
         WHERE state = ANY(ARRAY['indexed'::media_item_state, 'scraped'::media_item_state, 'partially_completed'::media_item_state])
           AND item_type = $1
           AND is_requested = true
         ORDER BY failed_attempts ASC, created_at ASC
         LIMIT $2",
    )
    .bind(item_type)
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

/// Fetch items stuck in Ongoing that haven't been updated in at least `min_age_minutes`.
/// Used by the retry scheduler to recover items left in Ongoing after a crash or lost
/// download session, without interfering with actively-downloading items.
pub async fn get_stuck_ongoing_items(
    pool: &PgPool,
    item_type: MediaItemType,
    min_age_minutes: i32,
    limit: i64,
) -> Result<Vec<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items
         WHERE state = 'ongoing' AND item_type = $1 AND is_requested = true
           AND (updated_at IS NULL OR updated_at < NOW() - ($2 * INTERVAL '1 minute'))
         ORDER BY updated_at ASC NULLS FIRST
         LIMIT $3",
    )
    .bind(item_type)
    .bind(f64::from(min_age_minutes))
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

/// Return the earliest requested unreleased descendant air date for a show.
pub async fn get_next_unreleased_air_date_for_show(
    pool: &PgPool,
    show_id: i64,
) -> Result<Option<chrono::NaiveDate>> {
    let next_air_date = sqlx::query_scalar::<_, Option<chrono::NaiveDate>>(
        r#"SELECT MIN(child.aired_at) AS "min?"
           FROM media_items child
           WHERE child.aired_at IS NOT NULL
             AND child.state = 'unreleased'
             AND child.is_requested = true
             AND (
               child.parent_id = $1
               OR child.parent_id IN (
                 SELECT season.id
                 FROM media_items season
                 WHERE season.parent_id = $1
               )
             )"#,
    )
    .bind(show_id)
    .fetch_one(pool)
    .await?;

    Ok(next_air_date)
}

/// Find an existing media item by type and any matching external ID.
pub async fn find_existing_media_item(
    pool: &PgPool,
    item_type: MediaItemType,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
) -> Result<Option<MediaItem>> {
    Ok(sqlx::query_as::<_, MediaItem>(
        "SELECT * FROM media_items
         WHERE item_type = $1
           AND ((imdb_id = $2 AND $2 IS NOT NULL)
             OR (tmdb_id = $3 AND $3 IS NOT NULL)
             OR (tvdb_id = $4 AND $4 IS NOT NULL))
         LIMIT 1",
    )
    .bind(item_type)
    .bind(imdb_id)
    .bind(tmdb_id)
    .bind(tvdb_id)
    .fetch_optional(pool)
    .await?)
}

/// Returns `(item, was_created)`. `was_created` is false when an existing item was found.
pub async fn create_movie(
    pool: &PgPool,
    title: &str,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    item_request_id: Option<i64>,
) -> Result<(MediaItem, bool)> {
    upsert_top_level_item(
        pool,
        title,
        imdb_id,
        "tmdb_id",
        tmdb_id,
        "movie",
        item_request_id,
    )
    .await
}

/// Returns `(item, was_created)`. `was_created` is false when an existing item was found.
pub async fn create_show(
    pool: &PgPool,
    title: &str,
    imdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    item_request_id: Option<i64>,
) -> Result<(MediaItem, bool)> {
    upsert_top_level_item(
        pool,
        title,
        imdb_id,
        "tvdb_id",
        tvdb_id,
        "show",
        item_request_id,
    )
    .await
}

pub(crate) fn to_json<T: serde::Serialize>(v: &T) -> serde_json::Value {
    serde_json::to_value(v).unwrap_or_default()
}

pub async fn update_media_item_index(
    pool: &PgPool,
    id: i64,
    indexed: &riven_core::types::IndexedMediaItem,
) -> Result<()> {
    let now = Utc::now();
    sqlx::query!(
        r#"UPDATE media_items SET
            title          = COALESCE($2,  title),
            full_title     = COALESCE($3,  full_title),
            imdb_id        = COALESCE($4,  imdb_id),
            tvdb_id        = COALESCE($5,  tvdb_id),
            tmdb_id        = COALESCE($6,  tmdb_id),
            poster_path    = COALESCE($7,  poster_path),
            year           = COALESCE($8,  year),
            genres         = COALESCE($9,  genres),
            country        = COALESCE($10, country),
            language       = COALESCE($11, language),
            network        = COALESCE($12, network),
            content_rating = COALESCE($13, content_rating),
            runtime        = COALESCE($14, runtime),
            aliases        = COALESCE($15, aliases),
            aired_at       = COALESCE($16, aired_at),
            show_status    = COALESCE($18, show_status),
            indexed_at = $17, updated_at = $17
           WHERE id = $1"#,
        id,
        indexed.title.as_deref(),
        indexed.full_title.as_deref(),
        indexed.imdb_id.as_deref(),
        indexed.tvdb_id.as_deref(),
        indexed.tmdb_id.as_deref(),
        indexed.poster_path.as_deref(),
        indexed.year,
        indexed.genres.as_ref().map(to_json),
        indexed.country.as_deref(),
        indexed.language.as_deref(),
        indexed.network.as_deref(),
        indexed.content_rating as Option<ContentRating>,
        indexed.runtime,
        indexed.aliases.as_ref().map(to_json),
        indexed.aired_at,
        now,
        indexed.status as Option<riven_core::types::ShowStatus>
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_media_item_state(pool: &PgPool, id: i64, state: MediaItemState) -> Result<()> {
    sqlx::query!(
        "UPDATE media_items SET state = $2, updated_at = NOW() WHERE id = $1",
        id,
        state as MediaItemState
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_scraped(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query!(
        "UPDATE media_items SET scraped_at = NOW(), scraped_times = scraped_times + 1, updated_at = NOW() WHERE id = $1",
        id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn transition_unreleased_aired(pool: &PgPool) -> Result<u64> {
    Ok(sqlx::query!(
        r#"UPDATE media_items SET state = 'indexed', updated_at = NOW()
           WHERE state = 'unreleased' AND aired_at IS NOT NULL
             AND aired_at <= CURRENT_DATE AND is_requested = true"#
    )
    .execute(pool)
    .await?
    .rows_affected())
}

pub async fn blacklist_stream_by_hash(
    pool: &PgPool,
    media_item_id: i64,
    info_hash: &str,
) -> Result<()> {
    let stream_id = sqlx::query_scalar!("SELECT id FROM streams WHERE info_hash = $1", info_hash)
        .fetch_optional(pool)
        .await?;
    if let Some(stream_id) = stream_id {
        sqlx::query!(
            "INSERT INTO media_item_blacklisted_streams (media_item_id, stream_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            media_item_id,
            stream_id
        )
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn increment_failed_attempts(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query!(
        "UPDATE media_items SET failed_attempts = failed_attempts + 1, updated_at = NOW() WHERE id = $1",
        id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn reset_failed_attempts(pool: &PgPool, id: i64) -> Result<()> {
    sqlx::query("UPDATE media_items SET failed_attempts = 0, updated_at = NOW() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn reset_failed_items(pool: &PgPool, older_than_secs: u64) -> Result<u64> {
    let cutoff = Utc::now() - chrono::Duration::seconds(older_than_secs as i64);
    Ok(sqlx::query!(
        r#"UPDATE media_items SET failed_attempts = 0, updated_at = NOW()
           WHERE is_requested = true AND failed_attempts > 0 AND updated_at < $1"#,
        cutoff
    )
    .execute(pool)
    .await?
    .rows_affected())
}

/// Delete top-level items (movies/shows) whose content-service request is no
/// longer active.  Seasons and episodes cascade via the DB foreign key.
/// Items that were added manually (no `item_request_id`) are never touched.
///
/// `active_external_ids` is the union of every `external_request_id` returned
/// by all content-service plugins in the current run.
pub async fn delete_items_removed_from_content_services(
    pool: &PgPool,
    active_external_ids: &[String],
) -> Result<u64> {
    let result = sqlx::query!(
        r#"DELETE FROM media_items
           WHERE item_type IN ('movie', 'show')
             AND item_request_id IN (
               SELECT id FROM item_requests
               WHERE external_request_id IS NOT NULL
                 AND NOT (external_request_id = ANY($1))
             )"#,
        active_external_ids
    )
    .execute(pool)
    .await?;

    // Clean up the now-orphaned item_requests.
    sqlx::query!(
        r#"DELETE FROM item_requests
           WHERE external_request_id IS NOT NULL
             AND NOT (external_request_id = ANY($1))"#,
        active_external_ids
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
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
        _ => Err(anyhow::anyhow!(
            "Only Movie and Show types can be added directly"
        )),
    }
}
