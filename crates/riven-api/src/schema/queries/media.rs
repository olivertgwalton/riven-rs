use async_graphql::*;
use riven_core::types::*;
use riven_db::entities::*;
use riven_db::repo;
use std::collections::HashMap;

use crate::schema::helpers::derive_media_metadata;
use crate::schema::typed_items::MediaItemUnion;
use crate::schema::types::*;

#[derive(Default)]
pub struct MediaQuery;

#[Object]
impl MediaQuery {
    async fn media_item_by_id(&self, ctx: &Context<'_>, id: i64) -> Result<Option<MediaItemUnion>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_media_item(pool, id).await?.map(MediaItemUnion::from))
    }

    async fn media_items(&self, ctx: &Context<'_>) -> Result<Vec<MediaItemUnion>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let items = sqlx::query_as::<_, MediaItem>(
            "SELECT * FROM media_items ORDER BY created_at DESC LIMIT 25",
        )
        .fetch_all(pool)
        .await?;
        Ok(items.into_iter().map(MediaItemUnion::from).collect())
    }

    async fn media_item_by_imdb(
        &self,
        ctx: &Context<'_>,
        imdb_id: String,
    ) -> Result<Option<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_media_item_by_imdb(pool, &imdb_id).await?)
    }

    async fn media_item_by_tmdb(
        &self,
        ctx: &Context<'_>,
        tmdb_id: String,
    ) -> Result<Option<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_media_item_by_tmdb(pool, &tmdb_id).await?)
    }

    async fn media_item_by_tvdb(
        &self,
        ctx: &Context<'_>,
        tvdb_id: String,
    ) -> Result<Option<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_media_item_by_tvdb(pool, &tvdb_id).await?)
    }

    async fn media_item_full_by_tmdb(
        &self,
        ctx: &Context<'_>,
        tmdb_id: String,
    ) -> Result<Option<MediaItemFull>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let Some(item) = repo::get_media_item_by_tmdb(pool, &tmdb_id).await? else {
            return Ok(None);
        };
        self.media_item_full_inner(pool, item).await.map(Some)
    }

    async fn media_item_full_by_tvdb(
        &self,
        ctx: &Context<'_>,
        tvdb_id: String,
    ) -> Result<Option<MediaItemFull>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let Some(item) = repo::get_media_item_by_tvdb(pool, &tvdb_id).await? else {
            return Ok(None);
        };
        self.media_item_full_inner(pool, item).await.map(Some)
    }

    async fn media_item_full(&self, ctx: &Context<'_>, id: i64) -> Result<Option<MediaItemFull>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let Some(item) = repo::get_media_item(pool, id).await? else {
            return Ok(None);
        };
        self.media_item_full_inner(pool, item).await.map(Some)
    }

    async fn media_item_state_by_tmdb(
        &self,
        ctx: &Context<'_>,
        tmdb_id: String,
    ) -> Result<Option<MediaItemStateTree>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let Some(item) = repo::get_media_item_by_tmdb(pool, &tmdb_id).await? else {
            return Ok(None);
        };
        self.media_item_state_tree_inner(pool, item).await.map(Some)
    }

    async fn media_item_state_by_tvdb(
        &self,
        ctx: &Context<'_>,
        tvdb_id: String,
    ) -> Result<Option<MediaItemStateTree>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let Some(item) = repo::get_media_item_by_tvdb(pool, &tvdb_id).await? else {
            return Ok(None);
        };
        self.media_item_state_tree_inner(pool, item).await.map(Some)
    }

    async fn movies(&self, ctx: &Context<'_>) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::list_movies(pool).await?)
    }

    async fn shows(&self, ctx: &Context<'_>) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::list_shows(pool).await?)
    }

    async fn seasons(
        &self,
        ctx: &Context<'_>,
        show_id: i64,
        include_specials: Option<bool>,
    ) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        if include_specials == Some(false) {
            Ok(repo::list_seasons_excluding_specials(pool, show_id).await?)
        } else {
            Ok(repo::list_seasons(pool, show_id).await?)
        }
    }

    async fn episodes(&self, ctx: &Context<'_>, season_id: i64) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::list_episodes(pool, season_id).await?)
    }

    async fn filesystem_entries(
        &self,
        ctx: &Context<'_>,
        media_item_id: i64,
    ) -> Result<Vec<FileSystemEntry>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_filesystem_entries(pool, media_item_id).await?)
    }

    async fn items_by_state(
        &self,
        ctx: &Context<'_>,
        state: MediaItemState,
        item_type: MediaItemType,
    ) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_items_by_state(pool, state, item_type).await?)
    }

    async fn items(
        &self,
        ctx: &Context<'_>,
        page: Option<i64>,
        limit: Option<i64>,
        sort: Option<String>,
        types: Option<Vec<MediaItemType>>,
        search: Option<String>,
        states: Option<Vec<MediaItemState>>,
    ) -> Result<ItemsPage> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let page = page.unwrap_or(1);
        let limit = limit.unwrap_or(20);
        let items = repo::list_items_paginated(
            pool,
            page,
            limit,
            sort,
            types.clone(),
            search.clone(),
            states.clone(),
        )
        .await?;
        let total_items = repo::count_items_filtered(pool, types, search, states).await?;
        let total_pages = ((total_items + limit - 1) / limit).max(1);
        Ok(ItemsPage { items, page, limit, total_items, total_pages })
    }

    async fn episode_by_tvdb(
        &self,
        ctx: &Context<'_>,
        tvdb_id: String,
        episode_number: i32,
        season_number: Option<i32>,
    ) -> Result<Option<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::find_episode_by_show_tvdb(pool, &tvdb_id, episode_number, season_number).await?)
    }

    /// Return the number of media files expected for a media item:
    /// - Movie / Episode → 1
    /// - Season → total episode count
    /// - Show → total processable episode count (continuing shows exclude the last season)
    async fn expected_file_count(&self, ctx: &Context<'_>, id: i64) -> Result<i64> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let item = repo::get_media_item(pool, id)
            .await?
            .ok_or_else(|| Error::new("Item not found"))?;
        let count = match item.item_type {
            MediaItemType::Movie | MediaItemType::Episode => 1,
            MediaItemType::Season => repo::count_episodes_in_season(pool, id).await?,
            MediaItemType::Show => repo::count_expected_files_for_show(pool, id).await?,
        };
        Ok(count)
    }

    /// Return lookup key strings for an episode:
    /// `["abs:{absolute_number}", "{season_number}:{episode_number}"]`.
    async fn lookup_keys(&self, ctx: &Context<'_>, id: i64) -> Result<Vec<String>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let item = repo::get_media_item(pool, id)
            .await?
            .ok_or_else(|| Error::new("Item not found"))?;
        let mut keys = Vec::new();
        if let Some(abs) = item.absolute_number {
            keys.push(format!("abs:{abs}"));
        }
        if let (Some(season), Some(episode)) = (item.season_number, item.episode_number) {
            keys.push(format!("{season}:{episode}"));
        }
        Ok(keys)
    }
}

// ── Non-GraphQL helpers ───────────────────────────────────────────────────────

impl MediaQuery {
    pub(crate) async fn media_item_state_tree_inner(
        &self,
        pool: &sqlx::PgPool,
        item: MediaItem,
    ) -> async_graphql::Result<MediaItemStateTree> {
        let (seasons, expected_file_count) = if item.item_type == MediaItemType::Show {
            let seasons = repo::list_seasons(pool, item.id).await?;
            let season_ids: Vec<i64> = seasons.iter().map(|s| s.id).collect();
            let episodes = if season_ids.is_empty() {
                Vec::new()
            } else {
                sqlx::query_as::<_, MediaItem>(
                    "SELECT * FROM media_items \
                     WHERE item_type = 'episode' AND parent_id = ANY($1) \
                     ORDER BY parent_id, episode_number",
                )
                .bind(&season_ids)
                .fetch_all(pool)
                .await?
            };

            let mut episodes_by_season: HashMap<i64, Vec<MediaItem>> = HashMap::new();
            for episode in episodes {
                episodes_by_season
                    .entry(episode.parent_id.unwrap_or_default())
                    .or_default()
                    .push(episode);
            }

            let show_expected: i64 = {
                let qualifying: Vec<&MediaItem> = seasons
                    .iter()
                    .filter(|s| {
                        s.is_requested
                            && s.is_special != Some(true)
                            && s.state != MediaItemState::Unreleased
                            && s.state != MediaItemState::Ongoing
                    })
                    .collect();
                let n = qualifying.len();
                let cap = if item.show_status == Some(ShowStatus::Continuing) {
                    n.saturating_sub(1).max(1)
                } else {
                    n
                };
                qualifying[..cap.min(n)]
                    .iter()
                    .map(|s| episodes_by_season.get(&s.id).map_or(0, |eps| eps.len()) as i64)
                    .sum()
            };

            let seasons: Vec<SeasonState> = seasons
                .into_iter()
                .map(|season| {
                    let eps: Vec<EpisodeState> = episodes_by_season
                        .remove(&season.id)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|episode| EpisodeState {
                            id: episode.id,
                            episode_number: episode.episode_number,
                            state: episode.state,
                        })
                        .collect();
                    let expected_file_count = eps.len() as i64;
                    SeasonState {
                        id: season.id,
                        season_number: season.season_number,
                        state: season.state,
                        is_requested: season.is_requested,
                        expected_file_count,
                        episodes: eps,
                    }
                })
                .collect();

            (seasons, show_expected)
        } else {
            (vec![], 1i64)
        };

        Ok(MediaItemStateTree {
            id: item.id,
            state: item.state,
            imdb_id: item.imdb_id,
            tmdb_id: item.tmdb_id,
            tvdb_id: item.tvdb_id,
            expected_file_count,
            seasons,
        })
    }

    pub(super) async fn media_item_full_inner(
        &self,
        pool: &sqlx::PgPool,
        item: MediaItem,
    ) -> async_graphql::Result<MediaItemFull> {
        let with_metadata = |mut e: FileSystemEntry| {
            if e.media_metadata.is_none()
                && let Some(ref filename) = e.original_filename
            {
                e.media_metadata = Some(derive_media_metadata(filename));
            }
            e
        };

        let all_entries = repo::get_filesystem_entries(pool, item.id).await?;
        let media_entries: Vec<_> = all_entries
            .into_iter()
            .filter(|e| e.entry_type == FileSystemEntryType::Media)
            .map(with_metadata)
            .collect();
        let filesystem_entry = media_entries.first().cloned();
        let filesystem_entries = media_entries;

        let seasons = if item.item_type == MediaItemType::Show {
            let seasons = repo::list_seasons(pool, item.id).await?;
            let season_ids: Vec<i64> = seasons.iter().map(|s| s.id).collect();
            let episodes = if season_ids.is_empty() {
                Vec::new()
            } else {
                sqlx::query_as::<_, MediaItem>(
                    "SELECT * FROM media_items \
                     WHERE item_type = 'episode' AND parent_id = ANY($1) \
                     ORDER BY parent_id, episode_number",
                )
                .bind(&season_ids)
                .fetch_all(pool)
                .await?
            };
            let episode_ids: Vec<i64> = episodes.iter().map(|e| e.id).collect();
            let episode_entries = if episode_ids.is_empty() {
                Vec::new()
            } else {
                sqlx::query_as::<_, FileSystemEntry>(
                    "SELECT * FROM filesystem_entries \
                     WHERE entry_type = 'media' AND media_item_id = ANY($1)",
                )
                .bind(&episode_ids)
                .fetch_all(pool)
                .await?
            };

            let mut episodes_by_season: HashMap<i64, Vec<MediaItem>> = HashMap::new();
            for episode in episodes {
                episodes_by_season
                    .entry(episode.parent_id.unwrap_or_default())
                    .or_default()
                    .push(episode);
            }
            let mut entries_by_episode: HashMap<i64, Vec<FileSystemEntry>> = HashMap::new();
            for entry in episode_entries {
                entries_by_episode
                    .entry(entry.media_item_id)
                    .or_default()
                    .push(with_metadata(entry));
            }

            let mut season_fulls = Vec::with_capacity(seasons.len());
            for season in seasons {
                let mut episode_fulls = Vec::new();
                for episode in episodes_by_season.remove(&season.id).unwrap_or_default() {
                    let ep_media = entries_by_episode.remove(&episode.id).unwrap_or_default();
                    let ep_fs = ep_media.first().cloned();
                    episode_fulls.push(EpisodeFull {
                        item: episode,
                        filesystem_entry: ep_fs,
                        filesystem_entries: ep_media,
                    });
                }
                season_fulls.push(SeasonFull { item: season, episodes: episode_fulls });
            }
            season_fulls
        } else {
            vec![]
        };

        Ok(MediaItemFull { item, filesystem_entry, filesystem_entries, seasons })
    }
}
