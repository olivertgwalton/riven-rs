use anyhow::Result;
use chrono::{DateTime, Utc};
use riven_core::entities::{
    filesystem_entries, media_item_blacklisted_streams, media_item_streams, streams, usenet_meta,
};
use riven_core::types::FileSystemEntryType;
use sea_orm::ActiveValue::{Set, Unchanged};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult,
    QueryFilter, QuerySelect, Statement,
};
use std::collections::BTreeSet;

use crate::entities::*;
use crate::orm;
use riven_rank::ResolutionRanks;

pub async fn upsert_stream(
    info_hash: &str,
    magnet: &str,
    parsed_data: Option<serde_json::Value>,
    rank: Option<i64>,
    file_size_bytes: Option<u64>,
) -> Result<Stream> {
    // Kept as a raw Statement: the magnet column uses a CASE expression on
    // conflict (`CASE WHEN $2 <> '' THEN $2 ELSE streams.magnet END`) that the
    // ActiveModel upsert path can't express cleanly.
    let file_size = file_size_bytes.map(|s| i64::try_from(s).unwrap_or(i64::MAX));
    let stream = Stream::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO streams (info_hash, magnet, parsed_data, rank, file_size_bytes) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (info_hash) DO UPDATE SET \
             magnet = CASE WHEN $2 <> '' THEN $2 ELSE streams.magnet END, \
             parsed_data = COALESCE($3, streams.parsed_data), \
             rank = COALESCE($4, streams.rank), \
             file_size_bytes = COALESCE($5, streams.file_size_bytes), \
             updated_at = NOW() \
         RETURNING *",
        [
            info_hash.into(),
            magnet.into(),
            parsed_data.into(),
            rank.into(),
            file_size.into(),
        ],
    ))
    .one(orm())
    .await?
    .ok_or_else(|| anyhow::anyhow!("upsert_stream returned no row"))?;
    Ok(stream)
}

/// Delete streams referenced by nothing — not a candidate list, not a
/// blacklist, not any item's active_stream, and not a filesystem entry. These
/// are stale cached scrape results that accumulate over time; they are
/// recreated on the next scrape, so deletion is fully recoverable. Run
/// periodically by the queue Scheduler. Returns the number of rows removed.
pub async fn delete_orphan_streams() -> Result<u64> {
    // Kept as a raw Statement: the DELETE has four correlated NOT EXISTS
    // subqueries against other tables, outside what the builder DELETE expresses.
    let result = orm()
        .execute(Statement::from_string(
            DbBackend::Postgres,
            "DELETE FROM streams s \
             WHERE NOT EXISTS (SELECT 1 FROM media_item_streams m WHERE m.stream_id = s.id) \
               AND NOT EXISTS (SELECT 1 FROM media_item_blacklisted_streams b WHERE b.stream_id = s.id) \
               AND NOT EXISTS (SELECT 1 FROM media_items i WHERE i.active_stream_id = s.id) \
               AND NOT EXISTS (SELECT 1 FROM filesystem_entries f WHERE f.stream_id = s.id)",
        ))
        .await?;
    Ok(result.rows_affected())
}

/// Record the actual file size for a stream (learned from a download attempt).
pub async fn update_stream_file_size(info_hash: &str, file_size_bytes: u64) -> Result<()> {
    streams::Entity::update_many()
        .col_expr(
            streams::Column::FileSizeBytes,
            Expr::value(i64::try_from(file_size_bytes).unwrap_or(i64::MAX)),
        )
        .col_expr(streams::Column::UpdatedAt, Expr::cust("NOW()"))
        .filter(streams::Column::InfoHash.eq(info_hash))
        .exec(orm())
        .await?;
    Ok(())
}

pub async fn link_stream_to_item(media_item_id: i64, stream_id: i64) -> Result<bool> {
    let insert = media_item_streams::Entity::insert(media_item_streams::ActiveModel {
        media_item_id: Set(media_item_id),
        stream_id: Set(stream_id),
    })
    .on_conflict(
        OnConflict::columns([
            media_item_streams::Column::MediaItemId,
            media_item_streams::Column::StreamId,
        ])
        .do_nothing()
        .to_owned(),
    );
    // `do_nothing` + an existing row surfaces as `RecordNotInserted`; that is
    // the ON CONFLICT DO NOTHING no-op, not an error — and means not inserted.
    let inserted = match insert.exec(orm()).await {
        Ok(_) => true,
        Err(sea_orm::DbErr::RecordNotInserted) => false,
        Err(error) => return Err(error.into()),
    };
    if inserted {
        super::state::recompute(&[media_item_id]).await?;
    }
    Ok(inserted)
}

pub async fn get_streams_for_item(media_item_id: i64) -> Result<Vec<Stream>> {
    Ok(streams::Entity::find()
        .inner_join(media_item_streams::Entity)
        .filter(media_item_streams::Column::MediaItemId.eq(media_item_id))
        .into_model::<Stream>()
        .all(orm())
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

pub async fn clear_blacklisted_streams(media_item_id: i64) -> Result<()> {
    media_item_blacklisted_streams::Entity::delete_many()
        .filter(media_item_blacklisted_streams::Column::MediaItemId.eq(media_item_id))
        .filter(media_item_blacklisted_streams::Column::Permanent.eq(false))
        .exec(orm())
        .await?;
    super::state::recompute(&[media_item_id]).await?;
    Ok(())
}

/// Permanently blacklist a stream (by usenet/release info hash) for an item, so
/// it survives the scrape-time blacklist clear. Used when the health check
/// confirms a release is broken. Returns `false` if no matching stream row.
pub async fn blacklist_stream_permanent_by_hash(
    media_item_id: i64,
    info_hash: &str,
) -> Result<bool> {
    let stream_id: Option<i64> = streams::Entity::find()
        .filter(streams::Column::InfoHash.eq(info_hash))
        .select_only()
        .column(streams::Column::Id)
        .into_tuple()
        .one(orm())
        .await?;
    let Some(stream_id) = stream_id else {
        return Ok(false);
    };
    media_item_blacklisted_streams::Entity::insert(media_item_blacklisted_streams::ActiveModel {
        media_item_id: Set(media_item_id),
        stream_id: Set(stream_id),
        permanent: Set(true),
    })
    .on_conflict(
        OnConflict::columns([
            media_item_blacklisted_streams::Column::MediaItemId,
            media_item_blacklisted_streams::Column::StreamId,
        ])
        .update_column(media_item_blacklisted_streams::Column::Permanent)
        .to_owned(),
    )
    .exec(orm())
    .await?;
    // Blacklisting changes `has_non_blacklisted_stream`, which can flip the item
    // out of `Scraped`; recompute so the derived state can't go stale.
    super::state::recompute(&[media_item_id]).await?;
    Ok(true)
}

/// Load resolution ranks from the `rank_settings` DB key.
/// Called once at `JobQueue` startup and on settings reload — callers pass the
/// cached value into stream queries so each query doesn't re-hit the DB.
pub async fn load_resolution_ranks() -> ResolutionRanks {
    match super::get_setting("rank_settings").await {
        Ok(Some(value)) => value
            .get("resolution_ranks")
            .and_then(|v| serde_json::from_value::<ResolutionRanks>(v.clone()).ok())
            .unwrap_or_default(),
        _ => ResolutionRanks::default(),
    }
}

pub async fn get_non_blacklisted_streams(
    media_item_id: i64,
    ranks: &ResolutionRanks,
) -> Result<Vec<Stream>> {
    // Raw Statement: dynamic ranking SQL with the resolution CASE expression.
    let sql = build_stream_query(ranks, false);
    Ok(Stream::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        sql,
        [media_item_id.into()],
    ))
    .all(orm())
    .await?)
}

/// Fetch only the highest-ranked non-blacklisted stream for an item.
pub async fn get_best_stream(
    media_item_id: i64,
    ranks: &ResolutionRanks,
) -> Result<Option<Stream>> {
    let sql = build_stream_query(ranks, true);
    Ok(Stream::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        sql,
        [media_item_id.into()],
    ))
    .one(orm())
    .await?)
}

pub async fn get_filesystem_entries(media_item_id: i64) -> Result<Vec<FileSystemEntry>> {
    Ok(filesystem_entries::Entity::find()
        .filter(filesystem_entries::Column::MediaItemId.eq(media_item_id))
        .into_model::<FileSystemEntry>()
        .all(orm())
        .await?)
}

pub async fn get_media_entries(media_item_id: i64) -> Result<Vec<FileSystemEntry>> {
    Ok(filesystem_entries::Entity::find()
        .filter(filesystem_entries::Column::MediaItemId.eq(media_item_id))
        .filter(filesystem_entries::Column::EntryType.eq(FileSystemEntryType::Media))
        .into_model::<FileSystemEntry>()
        .all(orm())
        .await?)
}

/// Like `get_media_entries` but walks the full media tree rooted at `root_id`.
/// Needed for season-level IDs where entries are stored on child episodes.
pub async fn get_media_entries_recursive(root_id: i64) -> Result<Vec<FileSystemEntry>> {
    // Raw Statement: recursive CTE walking the media tree.
    Ok(
        FileSystemEntry::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "WITH RECURSIVE media_tree AS (
             SELECT id FROM media_items WHERE id = $1
             UNION
             SELECT child.id
             FROM media_items child
             INNER JOIN media_tree parent ON child.parent_id = parent.id
         )
         SELECT fe.id, fe.file_size, fe.created_at, fe.updated_at, fe.media_item_id,
                fe.entry_type::text AS entry_type, fe.path, fe.original_filename, fe.download_url,
                fe.stream_url, fe.plugin, fe.provider, fe.provider_download_id, fe.library_profiles,
                fe.media_metadata, fe.language, fe.parent_original_filename, fe.subtitle_content,
                fe.file_hash, fe.video_file_size, fe.opensubtitles_id, fe.source_provider,
                fe.source_id, fe.stream_id, fe.resolution, fe.ranking_profile_name,
                fe.usenet_info_hash, fe.usenet_file_index
         FROM filesystem_entries fe
         INNER JOIN media_tree mt ON fe.media_item_id = mt.id
         WHERE fe.entry_type = 'media'",
            [root_id.into()],
        ))
        .all(orm())
        .await?,
    )
}

pub async fn get_media_entry_paths_for_items(root_ids: &[i64]) -> Result<Vec<String>> {
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Raw Statement: recursive CTE with id = ANY($1).
    let rows = orm()
        .query_all(Statement::from_sql_and_values(
            DbBackend::Postgres,
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
            [root_ids.to_vec().into()],
        ))
        .await?;

    let mut paths = Vec::with_capacity(rows.len());
    for row in rows {
        paths.push(row.try_get::<String>("", "path")?);
    }
    Ok(paths)
}

pub async fn get_media_entry_by_path(path: &str) -> Result<Option<FileSystemEntry>> {
    Ok(filesystem_entries::Entity::find()
        .filter(filesystem_entries::Column::Path.eq(path))
        .filter(filesystem_entries::Column::EntryType.eq(FileSystemEntryType::Media))
        .into_model::<FileSystemEntry>()
        .one(orm())
        .await?)
}

/// Look up a filesystem entry by VFS path regardless of entry_type. Used by
/// the VFS layer to resolve subtitle entries (which share path-space with the
/// media file they sit beside).
pub async fn get_filesystem_entry_by_path(path: &str) -> Result<Option<FileSystemEntry>> {
    Ok(filesystem_entries::Entity::find()
        .filter(filesystem_entries::Column::Path.eq(path))
        .into_model::<FileSystemEntry>()
        .one(orm())
        .await?)
}

/// Insert or replace a subtitle filesystem entry for `(media_item_id, language)`.
/// Subtitle content is stored inline in `subtitle_content`; the VFS serves it
/// directly without going through the streaming code path.
pub async fn upsert_subtitle_entry(
    media_item_id: i64,
    path: &str,
    language: &str,
    subtitle_content: &str,
    source_provider: &str,
    source_id: Option<&str>,
    parent_original_filename: Option<&str>,
) -> Result<FileSystemEntry> {
    let file_size = i64::try_from(subtitle_content.len()).unwrap_or(i64::MAX);

    // Delete+insert, not ON CONFLICT: the unique index is partial
    // (entry_type='subtitle') and Postgres only matches ON CONFLICT against
    // full-relation indexes.
    filesystem_entries::Entity::delete_many()
        .filter(filesystem_entries::Column::MediaItemId.eq(media_item_id))
        .filter(filesystem_entries::Column::Language.eq(language))
        .filter(filesystem_entries::Column::EntryType.eq(FileSystemEntryType::Subtitle))
        .exec(orm())
        .await?;

    let inserted = filesystem_entries::ActiveModel {
        media_item_id: Set(media_item_id),
        entry_type: Set(FileSystemEntryType::Subtitle),
        path: Set(path.to_owned()),
        file_size: Set(file_size),
        language: Set(Some(language.to_owned())),
        subtitle_content: Set(Some(subtitle_content.to_owned())),
        source_provider: Set(Some(source_provider.to_owned())),
        source_id: Set(source_id.map(str::to_owned)),
        parent_original_filename: Set(parent_original_filename.map(str::to_owned)),
        ..Default::default()
    }
    .insert(orm())
    .await?;

    get_media_entry_by_id_any(inserted.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("inserted subtitle entry {} not found", inserted.id))
}

/// Fetch a filesystem entry by id regardless of entry_type, as the public
/// `FileSystemEntry` struct. Used to re-fetch after inserts.
async fn get_media_entry_by_id_any(entry_id: i64) -> Result<Option<FileSystemEntry>> {
    Ok(filesystem_entries::Entity::find_by_id(entry_id)
        .into_model::<FileSystemEntry>()
        .one(orm())
        .await?)
}

pub async fn get_media_entry_by_id(entry_id: i64) -> Result<Option<FileSystemEntry>> {
    Ok(filesystem_entries::Entity::find()
        .filter(filesystem_entries::Column::Id.eq(entry_id))
        .filter(filesystem_entries::Column::EntryType.eq(FileSystemEntryType::Media))
        .into_model::<FileSystemEntry>()
        .one(orm())
        .await?)
}

/// Return the most likely next playback target for episodic content.
/// Movies and non-episodic items return `None`.
pub async fn get_next_playback_entry(entry_id: i64) -> Result<Option<FileSystemEntry>> {
    // Raw Statement: multi-table self-join across episodes/seasons with the
    // next-episode ordering logic.
    Ok(FileSystemEntry::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"SELECT next_fe.id, next_fe.file_size, next_fe.created_at, next_fe.updated_at,
                  next_fe.media_item_id, next_fe.entry_type::text AS entry_type, next_fe.path,
                  next_fe.original_filename, next_fe.download_url, next_fe.stream_url, next_fe.plugin,
                  next_fe.provider, next_fe.provider_download_id, next_fe.library_profiles,
                  next_fe.media_metadata, next_fe.language, next_fe.parent_original_filename,
                  next_fe.subtitle_content, next_fe.file_hash, next_fe.video_file_size,
                  next_fe.opensubtitles_id, next_fe.source_provider, next_fe.source_id,
                  next_fe.stream_id, next_fe.resolution, next_fe.ranking_profile_name,
                  next_fe.usenet_info_hash, next_fe.usenet_file_index
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
        [entry_id.into()],
    ))
    .one(orm())
    .await?)
}

pub async fn list_filesystem_profile_entry_candidates()
-> Result<Vec<FilesystemProfileEntryCandidate>> {
    // Raw Statement: large multi-CASE projection across self-joins. item_type
    // is consumed only inside CASE expressions (not selected), so no enum cast
    // is needed on the output.
    Ok(FilesystemProfileEntryCandidate::find_by_statement(Statement::from_string(
        DbBackend::Postgres,
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
               -- Cast the enum to text: SeaORM's generated SELECTs cast enum
               -- columns to text automatically, but a raw `find_by_statement`
               -- does not, and decoding a bare `content_rating` enum into
               -- `Option<ContentRating>` (which reads via String) fails with a
               -- type mismatch, aborting the whole rematch query.
               (CASE
                   WHEN item.item_type = 'movie' THEN item.content_rating
                   ELSE show_item.content_rating
               END)::text AS content_rating,
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
    ))
    .all(orm())
    .await?)
}

#[derive(Debug, Default)]
pub struct FilesystemLibraryFilterOptions {
    pub genres: Vec<String>,
    pub networks: Vec<String>,
    pub languages: Vec<String>,
    pub countries: Vec<String>,
    pub content_ratings: Vec<String>,
}

/// Return the exact metadata values present in the current filesystem library.
/// Values are only deduplicated; their spelling and casing are left untouched.
pub async fn list_filesystem_library_filter_options() -> Result<FilesystemLibraryFilterOptions> {
    let candidates = list_filesystem_profile_entry_candidates().await?;
    let mut genres = BTreeSet::new();
    let mut networks = BTreeSet::new();
    let mut languages = BTreeSet::new();
    let mut countries = BTreeSet::new();
    let mut content_ratings = BTreeSet::new();

    for candidate in candidates {
        if let Some(values) = candidate
            .genres
            .as_ref()
            .and_then(serde_json::Value::as_array)
        {
            genres.extend(
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string),
            );
        }
        networks.extend(candidate.network);
        languages.extend(candidate.language);
        countries.extend(candidate.country);
        if let Some(rating) = candidate.content_rating
            && let Ok(serde_json::Value::String(value)) = serde_json::to_value(rating)
        {
            content_ratings.insert(value);
        }
    }

    Ok(FilesystemLibraryFilterOptions {
        genres: genres.into_iter().collect(),
        networks: networks.into_iter().collect(),
        languages: languages.into_iter().collect(),
        countries: countries.into_iter().collect(),
        content_ratings: content_ratings.into_iter().collect(),
    })
}

/// Return the ranking profile names that already have a downloaded entry for this item.
pub async fn get_downloaded_profile_names(media_item_id: i64) -> Result<Vec<String>> {
    let rows: Vec<Option<String>> = filesystem_entries::Entity::find()
        .filter(filesystem_entries::Column::MediaItemId.eq(media_item_id))
        .filter(filesystem_entries::Column::EntryType.eq(FileSystemEntryType::Media))
        .filter(filesystem_entries::Column::RankingProfileName.is_not_null())
        .select_only()
        .column(filesystem_entries::Column::RankingProfileName)
        .distinct()
        .into_tuple()
        .all(orm())
        .await?;
    Ok(rows.into_iter().flatten().collect())
}

/// For a Season item, return profile names that have been downloaded for at
/// least one episode in that season.
pub async fn get_downloaded_profile_names_for_season(season_id: i64) -> Result<Vec<String>> {
    let rows: Vec<Option<String>> = filesystem_entries::Entity::find()
        .inner_join(riven_core::entities::media_items::Entity)
        .filter(riven_core::entities::media_items::Column::ParentId.eq(season_id))
        .filter(filesystem_entries::Column::EntryType.eq(FileSystemEntryType::Media))
        .filter(filesystem_entries::Column::RankingProfileName.is_not_null())
        .select_only()
        .column(filesystem_entries::Column::RankingProfileName)
        .distinct()
        .into_tuple()
        .all(orm())
        .await?;
    Ok(rows.into_iter().flatten().collect())
}

/// Upsert a media filesystem entry, replacing the former SELECT + INSERT/UPDATE
/// two-round-trip pattern with a single statement.
///
/// Requires the partial unique index `idx_fs_entries_media_path_unique` on
/// `(media_item_id, path) WHERE entry_type = 'media'` (migration 011).
pub struct MediaEntryInput<'a> {
    pub media_item_id: i64,
    pub path: &'a str,
    pub file_size: i64,
    pub original_filename: &'a str,
    pub download_url: Option<&'a str>,
    pub stream_url: Option<&'a str>,
    pub plugin: &'a str,
    pub provider: Option<&'a str>,
    pub stream_id: Option<i64>,
    pub resolution: Option<&'a str>,
    pub ranking_profile_name: Option<&'a str>,
    pub library_profiles: Option<&'a serde_json::Value>,
    pub usenet_info_hash: Option<&'a str>,
    pub usenet_file_index: Option<i32>,
}

pub async fn create_media_entry(input: MediaEntryInput<'_>) -> Result<FileSystemEntry> {
    let MediaEntryInput {
        media_item_id,
        path,
        file_size,
        original_filename,
        download_url,
        stream_url,
        plugin,
        provider,
        stream_id,
        resolution,
        ranking_profile_name,
        library_profiles,
        usenet_info_hash,
        usenet_file_index,
    } = input;
    let media_metadata = parse_filename_metadata(original_filename);

    // Raw Statement: ON CONFLICT targets a *partial* unique index
    // (`WHERE entry_type = 'media'`) and the DO UPDATE mixes EXCLUDED with
    // COALESCE(EXCLUDED, existing) per-column — not expressible via ActiveModel
    // upsert. Re-fetch the row through `get_media_entry_by_id` afterward.
    let row = orm()
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO filesystem_entries \
             (media_item_id, entry_type, path, file_size, original_filename, download_url, stream_url, \
              plugin, provider, media_metadata, stream_id, resolution, ranking_profile_name, library_profiles, \
              usenet_info_hash, usenet_file_index) \
             VALUES ($1, 'media', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) \
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
                 usenet_info_hash      = COALESCE(EXCLUDED.usenet_info_hash, filesystem_entries.usenet_info_hash), \
                 usenet_file_index     = COALESCE(EXCLUDED.usenet_file_index, filesystem_entries.usenet_file_index), \
                 updated_at            = NOW() \
             RETURNING id",
            [
                media_item_id.into(),
                path.into(),
                file_size.into(),
                original_filename.into(),
                download_url.into(),
                stream_url.into(),
                plugin.into(),
                provider.into(),
                media_metadata.into(),
                stream_id.into(),
                resolution.into(),
                ranking_profile_name.into(),
                library_profiles.cloned().into(),
                usenet_info_hash.into(),
                usenet_file_index.into(),
            ],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("create_media_entry returned no row"))?;
    let entry_id: i64 = row.try_get::<i64>("", "id")?;

    let entry = get_media_entry_by_id_any(entry_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("created media entry {entry_id} not found"))?;

    super::state::recompute(&[media_item_id]).await?;

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

pub async fn list_vfs_dir_names(pattern: &str, depth: u32) -> Result<Vec<VfsDirName>> {
    // Raw Statement: split_part with interpolated depth.
    let sql = format!(
        "SELECT split_part(path, '/', {depth}) AS name, library_profiles \
         FROM filesystem_entries \
         WHERE path LIKE $1 AND entry_type = 'media' \
         ORDER BY 1"
    );
    Ok(
        VfsDirName::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [pattern.into()],
        ))
        .all(orm())
        .await?,
    )
}

pub async fn list_vfs_file_names(dir_path: &str) -> Result<Vec<VfsFileName>> {
    // Raw Statement: split_part over an array_length expression.
    let sql = "SELECT split_part(path, '/', array_length(string_to_array(trim(both '/' from $1), '/'), 1) + 2) AS name, library_profiles \
         FROM filesystem_entries \
         WHERE path LIKE ($1 || '/%') AND entry_type IN ('media', 'subtitle') \
         ORDER BY 1";
    Ok(
        VfsFileName::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [dir_path.into()],
        ))
        .all(orm())
        .await?,
    )
}

/// Aggregate stat (timestamps + entry count) for all media entries under `path_prefix`.
/// A `path_prefix` of `""` covers all entries; `/movies` covers only movies, etc.
#[derive(sea_orm::FromQueryResult)]
pub struct VfsDirStatResult {
    pub ctime: Option<DateTime<Utc>>,
    pub mtime: Option<DateTime<Utc>>,
    pub entry_count: i64,
}

pub async fn get_vfs_dir_stat(path_prefix: &str) -> Result<VfsDirStatResult> {
    // Raw Statement: aggregate over MIN/MAX/COUNT with COALESCE.
    let pattern = format!("{path_prefix}/%");
    VfsDirStatResult::find_by_statement(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT \
           MIN(created_at) AS ctime, \
           MAX(COALESCE(updated_at, created_at)) AS mtime, \
           COUNT(*) AS entry_count \
         FROM filesystem_entries \
         WHERE path LIKE $1 AND entry_type = 'media'",
        [pattern.into()],
    ))
    .one(orm())
    .await?
    .ok_or_else(|| anyhow::anyhow!("get_vfs_dir_stat returned no row"))
}

/// Count distinct directory names at `depth` (1-based split_part index) for entries
/// matching `pattern`.
pub async fn count_vfs_distinct_dirs(pattern: &str, depth: u32) -> Result<i64> {
    // Raw Statement: COUNT(DISTINCT split_part(...)) with interpolated depth.
    let sql = format!(
        "SELECT COUNT(DISTINCT split_part(path, '/', {depth})) AS count \
         FROM filesystem_entries \
         WHERE path LIKE $1 AND entry_type = 'media'"
    );
    let row = orm()
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [pattern.into()],
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("count_vfs_distinct_dirs returned no row"))?;
    Ok(row.try_get::<i64>("", "count")?)
}

/// Deletes the `usenet_meta` row for `info_hash` — but only if no
/// `filesystem_entries` row still references it. A single info_hash's
/// segment map can back multiple media items at once (a season-pack NZB's
/// shared RAR volumes span several episodes' virtual files), so this must
/// never remove data a sibling entry still depends on.
///
/// Called whenever a usenet-backed filesystem entry is removed, so that a
/// later re-scrape landing on the same release (NZB info_hash is
/// deterministic from its content, so a repeat scrape of an unchanged
/// release reliably reproduces the same hash) re-runs ingest-time
/// validation — segment availability, RAR structure, PAR2 block checks —
/// instead of `UsenetStreamer::ingest`'s idempotency fast path silently
/// reusing a stale, possibly already-known-bad, cached parse.
pub async fn delete_orphaned_usenet_meta(info_hash: &str) -> Result<bool> {
    let still_referenced = filesystem_entries::Entity::find()
        .filter(filesystem_entries::Column::UsenetInfoHash.eq(info_hash))
        .limit(1)
        .one(orm())
        .await?
        .is_some();
    if still_referenced {
        return Ok(false);
    }
    let result = usenet_meta::Entity::delete_by_id(info_hash.to_owned())
        .exec(orm())
        .await?;
    Ok(result.rows_affected > 0)
}

/// Returns `(was_deleted, owning_media_item_id)`. Losing a media entry can
/// flip Completed → Scraped/Indexed, so the affected item is recomputed
/// before returning.
pub async fn delete_filesystem_entry(entry_id: i64) -> Result<(bool, Option<i64>)> {
    // Raw Statement: DELETE ... RETURNING to learn the owning item (and, for
    // usenet entries, the info_hash to check for orphaned meta) in one trip.
    let row = orm()
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM filesystem_entries \
             WHERE id = $1 AND entry_type = 'media' \
             RETURNING media_item_id, usenet_info_hash",
            [entry_id.into()],
        ))
        .await?;
    let (media_item_id, usenet_info_hash) = match &row {
        Some(row) => (
            Some(row.try_get::<i64>("", "media_item_id")?),
            row.try_get::<Option<String>>("", "usenet_info_hash")?,
        ),
        None => (None, None),
    };
    if let Some(info_hash) = usenet_info_hash.as_deref() {
        delete_orphaned_usenet_meta(info_hash).await?;
    }
    if let Some(id) = media_item_id {
        super::state::recompute(&[id]).await?;
    }
    Ok((media_item_id.is_some(), media_item_id))
}

pub async fn update_stream_url(entry_id: i64, stream_url: &str) -> Result<()> {
    filesystem_entries::ActiveModel {
        id: Unchanged(entry_id),
        stream_url: Set(Some(stream_url.to_owned())),
        ..Default::default()
    }
    .update(orm())
    .await?;
    Ok(())
}

pub async fn update_library_profiles_batch(updates: &[(i64, serde_json::Value)]) -> Result<u64> {
    if updates.is_empty() {
        return Ok(0);
    }

    // Kept as a per-row UPDATE loop: the original used a single multi-row
    // UPDATE ... FROM (VALUES ...) join, which SeaORM's query builder can't
    // express. Each row is a plain ActiveModel update. `Set` on the JsonBinary
    // column mirrors the proven `Set(seasons_json)` pattern in requests.rs.
    let now = Utc::now();
    let mut total = 0_u64;
    for (entry_id, library_profiles) in updates {
        let result = filesystem_entries::Entity::update_many()
            .set(filesystem_entries::ActiveModel {
                library_profiles: Set(Some(library_profiles.clone())),
                updated_at: Set(Some(now)),
                ..Default::default()
            })
            .filter(filesystem_entries::Column::Id.eq(*entry_id))
            .exec(orm())
            .await?;
        total += result.rows_affected;
    }

    Ok(total)
}
