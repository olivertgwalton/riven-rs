use anyhow::Result;
use chrono::{DateTime, Utc};
use riven_core::entities::{
    filesystem_entries, media_item_blacklisted_streams, media_item_streams, media_items, streams,
};
use riven_core::types::FileSystemEntryType;
use sea_orm::ActiveValue::{Set, Unchanged};
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult,
    QueryFilter, QuerySelect, Statement,
};
use std::collections::{BTreeSet, HashSet};

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
    let row = StreamUpsert {
        info_hash: info_hash.to_owned(),
        magnet: magnet.to_owned(),
        parsed_data,
        rank,
        file_size_bytes,
    };
    upsert_streams(std::slice::from_ref(&row))
        .await?
        .pop()
        .ok_or_else(|| anyhow::anyhow!("upsert_stream returned no row"))
}

/// One row for [`upsert_streams`].
pub struct StreamUpsert {
    pub info_hash: String,
    pub magnet: String,
    pub parsed_data: Option<serde_json::Value>,
    pub rank: Option<i64>,
    pub file_size_bytes: Option<u64>,
}

/// Batch form of [`upsert_stream`] — one statement for the whole set instead of
/// one round trip per stream. A manual scrape can produce hundreds of results,
/// and at that size the per-row latency dominates the work.
///
/// Returns the upserted rows, in unspecified order. Postgres caps a statement
/// at 65535 bound parameters and this binds five per row, so the input is
/// chunked rather than trusted to be small.
///
/// Rows repeating an `info_hash` are collapsed to the first occurrence:
/// `ON CONFLICT DO UPDATE` rejects a statement that would touch the same target
/// row twice, so a duplicate would otherwise fail the whole batch.
pub async fn upsert_streams(rows: &[StreamUpsert]) -> Result<Vec<Stream>> {
    // Five columns are Set per row, so five bound parameters per row.
    const PARAMS_PER_ROW: usize = 5;
    const MAX_ROWS_PER_STATEMENT: usize = 65535 / PARAMS_PER_ROW;

    let mut seen: HashSet<&str> = HashSet::with_capacity(rows.len());
    let deduped: Vec<&StreamUpsert> = rows
        .iter()
        .filter(|row| seen.insert(row.info_hash.as_str()))
        .collect();

    let mut out = Vec::with_capacity(deduped.len());
    for chunk in deduped.chunks(MAX_ROWS_PER_STATEMENT) {
        let models = chunk.iter().map(|row| streams::ActiveModel {
            info_hash: Set(row.info_hash.clone()),
            magnet: Set(row.magnet.clone()),
            parsed_data: Set(row.parsed_data.clone()),
            rank: Set(row.rank),
            file_size_bytes: Set(row
                .file_size_bytes
                .map(|s| i64::try_from(s).unwrap_or(i64::MAX))),
            ..Default::default()
        });

        // The same conflict clause as the single-row `upsert_stream`, reading
        // the incoming values from EXCLUDED. Unlike that one this does go
        // through the builder: `OnConflict::value` takes an arbitrary
        // expression, so the CASE and COALESCE columns are expressible here.
        out.extend(
            streams::Entity::insert_many(models)
                .on_conflict(
                    OnConflict::column(streams::Column::InfoHash)
                        .value(
                            streams::Column::Magnet,
                            Expr::cust(
                                "CASE WHEN EXCLUDED.magnet <> '' \
                                 THEN EXCLUDED.magnet ELSE streams.magnet END",
                            ),
                        )
                        .value(
                            streams::Column::ParsedData,
                            Expr::cust("COALESCE(EXCLUDED.parsed_data, streams.parsed_data)"),
                        )
                        .value(
                            streams::Column::Rank,
                            Expr::cust("COALESCE(EXCLUDED.rank, streams.rank)"),
                        )
                        .value(
                            streams::Column::FileSizeBytes,
                            Expr::cust(
                                "COALESCE(EXCLUDED.file_size_bytes, streams.file_size_bytes)",
                            ),
                        )
                        .value(streams::Column::UpdatedAt, Expr::cust("NOW()"))
                        .to_owned(),
                )
                .exec_with_returning(orm())
                .await?,
        );
    }
    Ok(out)
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
        .execute_raw(Statement::from_string(
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
    Ok(link_streams_to_item(media_item_id, &[stream_id]).await? > 0)
}

/// Batch form of [`link_stream_to_item`]: one insert for every link, and a
/// single state recompute afterwards rather than one per link.
///
/// Returns the number of links actually created (existing links conflict away).
pub async fn link_streams_to_item(media_item_id: i64, stream_ids: &[i64]) -> Result<u64> {
    if stream_ids.is_empty() {
        return Ok(0);
    }

    // Two columns Set per row, so two bound parameters per row.
    const MAX_ROWS_PER_STATEMENT: usize = 65535 / 2;

    let mut inserted = 0u64;
    for chunk in stream_ids.chunks(MAX_ROWS_PER_STATEMENT) {
        let models = chunk
            .iter()
            .map(|&stream_id| media_item_streams::ActiveModel {
                media_item_id: Set(media_item_id),
                stream_id: Set(stream_id),
            });
        // `do_nothing` + RETURNING gives back only the rows that were really
        // inserted, so existing links don't count towards the recompute below.
        // A chunk where every link already existed surfaces as
        // `RecordNotInserted` — the ON CONFLICT no-op, not an error.
        match media_item_streams::Entity::insert_many(models)
            .on_conflict(
                OnConflict::columns([
                    media_item_streams::Column::MediaItemId,
                    media_item_streams::Column::StreamId,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec_with_returning(orm())
            .await
        {
            Ok(rows) => inserted += rows.len() as u64,
            Err(sea_orm::DbErr::RecordNotInserted) => {}
            Err(error) => return Err(error.into()),
        }
    }

    if inserted > 0 {
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
    Ok(blacklist_streams_permanent_by_hashes(media_item_id, &[info_hash]).await? > 0)
}

/// Permanently blacklist every matching stream in one lookup and one upsert.
///
/// Returns the number of supplied hashes that matched a stream. State is
/// recomputed once for the whole batch.
pub async fn blacklist_streams_permanent_by_hashes(
    media_item_id: i64,
    info_hashes: &[&str],
) -> Result<usize> {
    if info_hashes.is_empty() {
        return Ok(0);
    }

    let stream_ids: Vec<i64> = streams::Entity::find()
        .filter(streams::Column::InfoHash.is_in(info_hashes.iter().copied()))
        .select_only()
        .column(streams::Column::Id)
        .into_tuple()
        .all(orm())
        .await?;
    if stream_ids.is_empty() {
        return Ok(0);
    }

    let models = stream_ids
        .iter()
        .map(|&stream_id| media_item_blacklisted_streams::ActiveModel {
            media_item_id: Set(media_item_id),
            stream_id: Set(stream_id),
            permanent: Set(true),
        });
    media_item_blacklisted_streams::Entity::insert_many(models)
        .on_conflict(
            OnConflict::columns([
                media_item_blacklisted_streams::Column::MediaItemId,
                media_item_blacklisted_streams::Column::StreamId,
            ])
            .update_column(media_item_blacklisted_streams::Column::Permanent)
            .to_owned(),
        )
        .exec_without_returning(orm())
        .await?;

    // Blacklisting changes `has_non_blacklisted_stream`, which can flip the item
    // out of `Scraped`; recompute so the derived state can't go stale.
    super::state::recompute(&[media_item_id]).await?;
    Ok(stream_ids.len())
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
        .query_all_raw(Statement::from_sql_and_values(
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

#[derive(Debug, Clone, FromQueryResult)]
pub struct DownloadReleaseInfo {
    pub resolution: Option<String>,
    pub quality: Option<String>,
    pub release_group: Option<String>,
}

/// The resolution/quality/release-group of the release behind the most
/// recently created filesystem entry for an item — i.e. the one a
/// `MediaItemDownloadSuccess` event for this item just fired for. Used by
/// notification templates. `quality`/`release_group` come from the linked
/// stream's parsed release data via a jsonb lookup the query builder can't
/// express; `None` if the item has no filesystem entries yet, or its stream
/// row is missing (usenet entries may have no `stream_id`).
pub async fn get_latest_release_info(item_id: i64) -> Result<Option<DownloadReleaseInfo>> {
    let sql = r#"SELECT
               fe.resolution AS resolution,
               s.parsed_data->>'quality' AS quality,
               s.parsed_data->>'group' AS release_group
           FROM filesystem_entries fe
           LEFT JOIN streams s ON s.id = fe.stream_id
           WHERE fe.media_item_id = $1 AND fe.entry_type = 'media'
           ORDER BY fe.created_at DESC
           LIMIT 1"#;
    Ok(
        DownloadReleaseInfo::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [item_id.into()],
        ))
        .one(orm())
        .await?,
    )
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

/// For a Season item, return profile names that every *expected* episode
/// (requested, and not still `unreleased`) already has a downloaded entry
/// for — i.e. a profile genuinely has nothing left to grab for this season,
/// not just "at least one episode happens to have it".
///
/// The distinction matters because the season-level download loop treats a
/// profile in this list as fully satisfied and skips it entirely on every
/// future pass. Reporting a profile "done" as soon as *any* episode has it
/// (the previous behavior) meant a season that downloaded its first couple
/// of episodes at a given quality would never be revisited for the rest —
/// every later episode's matching stream sat unblacklisted and unused
/// forever, since the profile that would have driven the download attempt
/// toward it was already considered finished.
///
/// Raw SQL: the "every expected episode has this profile" comparison is a
/// `HAVING count(...) >= (subquery)` the query builder can't express.
pub async fn get_downloaded_profile_names_for_season(season_id: i64) -> Result<Vec<String>> {
    let rows = orm()
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT fe.ranking_profile_name AS profile_name
             FROM filesystem_entries fe
             JOIN media_items ep ON ep.id = fe.media_item_id
             WHERE ep.parent_id = $1
               AND ep.is_requested = true
               AND ep.state <> 'unreleased'
               AND fe.entry_type = 'media'
               AND fe.ranking_profile_name IS NOT NULL
             GROUP BY fe.ranking_profile_name
             HAVING COUNT(DISTINCT fe.media_item_id) >= (
                 SELECT COUNT(*) FROM media_items expected
                 WHERE expected.parent_id = $1
                   AND expected.is_requested = true
                   AND expected.state <> 'unreleased'
             )",
            [season_id.into()],
        ))
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.try_get::<String>("", "profile_name").ok())
        .collect())
}

/// Episode ids under a season that already have an entry for the given
/// ranking profile.
///
/// A season-pack (or single-episode) stream frequently covers a mix of
/// already-satisfied and still-missing episodes — e.g. the same release
/// group posts one file per episode, and the top-ranked one by tie-break
/// happens to be an episode already downloaded. Persisting into an
/// already-satisfied episode is harmless on its own (it just upserts the
/// same row), but if the caller counts that as "this stream made progress"
/// it stops trying further candidates — permanently starving every other
/// missing episode, since the same top-ranked-but-already-done stream wins
/// the tie-break on every future retry too. Callers use this set to skip
/// persisting into episodes that don't need it, so a stream that only
/// touches already-done episodes is correctly reported as making no
/// progress.
pub async fn get_episode_ids_with_profile_for_season(
    season_id: i64,
    profile_name: &str,
) -> Result<HashSet<i64>> {
    let rows = orm()
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT DISTINCT fe.media_item_id AS episode_id
             FROM filesystem_entries fe
             JOIN media_items ep ON ep.id = fe.media_item_id
             WHERE ep.parent_id = $1
               AND fe.entry_type = 'media'
               AND fe.ranking_profile_name = $2",
            [season_id.into(), profile_name.into()],
        ))
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.try_get::<i64>("", "episode_id").ok())
        .collect())
}

/// Same as [`get_episode_ids_with_profile_for_season`], but for a multi-season
/// pack matched against a whole show.
pub async fn get_episode_ids_with_profile_for_show(
    show_id: i64,
    profile_name: &str,
) -> Result<HashSet<i64>> {
    let rows = orm()
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT DISTINCT fe.media_item_id AS episode_id
             FROM filesystem_entries fe
             JOIN media_items ep ON ep.id = fe.media_item_id
             JOIN media_items season ON season.id = ep.parent_id
             WHERE season.parent_id = $1
               AND fe.entry_type = 'media'
               AND fe.ranking_profile_name = $2",
            [show_id.into(), profile_name.into()],
        ))
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.try_get::<i64>("", "episode_id").ok())
        .collect())
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
        .query_one_raw(Statement::from_sql_and_values(
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

use riven_rank::derive_media_metadata as parse_filename_metadata;

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
        .query_one_raw(Statement::from_sql_and_values(
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
    Ok(delete_orphaned_usenet_metas(&[info_hash.to_owned()]).await? > 0)
}

/// Batch form of [`delete_orphaned_usenet_meta`].
pub async fn delete_orphaned_usenet_metas(info_hashes: &[String]) -> Result<u64> {
    if info_hashes.is_empty() {
        return Ok(0);
    }

    let result = orm()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM usenet_meta u \
             WHERE u.info_hash = ANY($1) \
               AND NOT EXISTS (\
                   SELECT 1 FROM filesystem_entries f \
                   WHERE f.usenet_info_hash = u.info_hash\
               )",
            [info_hashes.to_vec().into()],
        ))
        .await?;
    Ok(result.rows_affected())
}

/// Returns `(was_deleted, owning_media_item_id)`. Losing a media entry can
/// flip Completed → Scraped/Indexed, so the affected item is recomputed
/// before returning.
pub async fn delete_filesystem_entry(entry_id: i64) -> Result<(bool, Option<i64>)> {
    let media_item_id = delete_filesystem_entries(&[entry_id]).await?.pop();
    Ok((media_item_id.is_some(), media_item_id))
}

/// Reject a downloaded file outright: permanently blacklist the release
/// behind it (so the next retry can't re-select the exact same wrong-title or
/// bad-quality stream) and remove its tracked entry, then clear the owning
/// item's retry backoff so the search for a replacement runs on the next
/// scheduler pass instead of waiting out whatever cooldown it had
/// accumulated.
///
/// Returns `false` if the entry didn't exist; blacklisting a stream that
/// somehow has no linked `streams` row (shouldn't happen, but not fatal) is
/// skipped rather than failing the whole removal.
pub async fn blacklist_and_remove_filesystem_entry(entry_id: i64) -> Result<bool> {
    let Some(entry) = get_media_entry_by_id(entry_id).await? else {
        return Ok(false);
    };

    if let Some(stream_id) = entry.stream_id
        && let Some(stream) = streams::Entity::find_by_id(stream_id).one(orm()).await?
    {
        blacklist_stream_permanent_by_hash(entry.media_item_id, &stream.info_hash).await?;

        // A season/show-pack release is linked to (and selected as a
        // candidate against) the *season's or show's* own media_item_id —
        // persist matches its files to individual episodes, but the
        // candidate query that picks the stream in the first place runs at
        // whichever level the download job targets. Blacklisting only the
        // episode leaves the exact same pack fully eligible on the next
        // season- or show-level regrab, since that query never looks at the
        // episode's blacklist rows at all.
        if let Some(hierarchy) =
            super::hierarchy::get_media_item_hierarchy(entry.media_item_id).await?
        {
            if let Some(season_id) = hierarchy.resolved_season_id {
                blacklist_stream_permanent_by_hash(season_id, &stream.info_hash).await?;
            }
            if let Some(show_id) = hierarchy.resolved_show_id {
                blacklist_stream_permanent_by_hash(show_id, &stream.info_hash).await?;
            }
        }
    }

    let (deleted, media_item_id) = delete_filesystem_entry(entry_id).await?;
    if let Some(media_item_id) = media_item_id {
        media_items::Entity::update_many()
            .col_expr(media_items::Column::FailedAttempts, Expr::value(0))
            .col_expr(media_items::Column::LastScrapeAttemptAt, Expr::cust("NULL"))
            .col_expr(media_items::Column::UpdatedAt, Expr::cust("NOW()"))
            .filter(media_items::Column::Id.eq(media_item_id))
            .exec(orm())
            .await?;
        super::state::recompute(&[media_item_id]).await?;
    }

    Ok(deleted)
}

/// Batch form of [`delete_filesystem_entry`] — one DELETE for the whole set
/// rather than a round trip per entry, plus one state recompute per affected
/// item instead of one per entry. A season-pack regrab deletes a couple of
/// dozen entries at a time, all belonging to the same show.
///
/// Semantics match calling the single-entry form in a loop: only `media`
/// entries are removed, orphaned usenet meta is cleaned up for each distinct
/// info hash, and affected items have their state recomputed.
///
/// Returns the ids of the media items that owned the deleted entries.
pub async fn delete_filesystem_entries(entry_ids: &[i64]) -> Result<Vec<i64>> {
    if entry_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Raw Statement: DELETE ... RETURNING has no builder form — the same
    // reason the single-entry `delete_filesystem_entry` above stays raw.
    let rows = orm()
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM filesystem_entries \
             WHERE id = ANY($1) AND entry_type = 'media' \
             RETURNING media_item_id, usenet_info_hash",
            [entry_ids.to_vec().into()],
        ))
        .await?;

    let mut media_item_ids: BTreeSet<i64> = BTreeSet::new();
    let mut info_hashes: BTreeSet<String> = BTreeSet::new();
    for row in &rows {
        media_item_ids.insert(row.try_get::<i64>("", "media_item_id")?);
        if let Some(hash) = row.try_get::<Option<String>>("", "usenet_info_hash")? {
            info_hashes.insert(hash);
        }
    }

    delete_orphaned_usenet_metas(&info_hashes.into_iter().collect::<Vec<_>>()).await?;

    let media_item_ids: Vec<i64> = media_item_ids.into_iter().collect();
    if !media_item_ids.is_empty() {
        super::state::recompute(&media_item_ids).await?;
    }
    Ok(media_item_ids)
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
