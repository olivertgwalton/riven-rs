use async_graphql::*;
use riven_core::entities::{filesystem_entries, media_items};
use riven_core::types::*;
use riven_db::entities::*;
use riven_db::orm;
use riven_db::repo;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

use crate::schema::helpers::{derive_media_metadata, episode_lookup_keys};
use crate::schema::typed_items::MediaItemUnion;
use crate::schema::types::*;

/// Group items that are pre-sorted by their group key into consecutive runs.
/// Returns `Vec<(key, Vec<T>)>` — callers look up by key via linear scan,
/// which beats a HashMap for the small N typical of show/season queries.
fn group_sorted_by_key<T, K, F>(items: impl IntoIterator<Item = T>, key: F) -> Vec<(K, Vec<T>)>
where
    K: Eq,
    F: Fn(&T) -> K,
{
    let mut groups: Vec<(K, Vec<T>)> = Vec::new();
    for item in items {
        let k = key(&item);
        match groups.last_mut() {
            Some((last, bucket)) if *last == k => bucket.push(item),
            _ => groups.push((k, vec![item])),
        }
    }
    groups
}

fn take_group<T, K: PartialEq>(groups: &mut Vec<(K, Vec<T>)>, key: &K) -> Vec<T> {
    groups
        .iter()
        .position(|(candidate, _)| candidate == key)
        .map(|index| groups.swap_remove(index).1)
        .unwrap_or_default()
}

/// Load a show's seasons together with its episodes grouped per season.
///
/// Fetches the show's seasons, then all their episodes in one query
/// through the shared repository batch helper.
async fn load_show_tree(
    show_id: i64,
) -> async_graphql::Result<(
    Vec<MediaItem>,
    std::collections::HashMap<i64, Vec<MediaItem>>,
)> {
    let seasons = repo::list_seasons(show_id).await?;
    let season_ids: Vec<i64> = seasons.iter().map(|s| s.id).collect();
    let episodes_by_season = repo::list_episodes_for_seasons(&season_ids).await?;
    Ok((seasons, episodes_by_season))
}

/// Generous upper bound for the batched library-status lookups — a carousel
/// row is a couple dozen cards at most, so this is headroom for that, not a
/// tuned limit. Guards against one request building an arbitrarily large
/// `IN (...)` list rather than actually protecting real usage.
const MAX_BULK_STATUS_IDS: usize = 500;

fn check_bulk_ids_len(ids: &[String]) -> Result<()> {
    if ids.len() > MAX_BULK_STATUS_IDS {
        return Err(Error::new(format!(
            "too many ids in one request: {} (max {MAX_BULK_STATUS_IDS})",
            ids.len()
        )));
    }
    Ok(())
}

#[derive(Default)]
pub struct MediaQuery;

#[Object]
impl MediaQuery {
    async fn media_item_by_id(
        &self,
        _ctx: &Context<'_>,
        id: i64,
    ) -> Result<Option<MediaItemUnion>> {
        Ok(repo::get_media_item(id).await?.map(MediaItemUnion::from))
    }

    async fn media_items(&self, _ctx: &Context<'_>) -> Result<Vec<MediaItemUnion>> {
        let items = media_items::Entity::find()
            .order_by_desc(media_items::Column::CreatedAt)
            .limit(25)
            .into_model::<MediaItem>()
            .all(orm())
            .await?;
        Ok(items.into_iter().map(MediaItemUnion::from).collect())
    }

    async fn media_item_by_imdb(
        &self,
        _ctx: &Context<'_>,
        imdb_id: String,
    ) -> Result<Option<MediaItem>> {
        Ok(repo::get_media_item_by_imdb(&imdb_id).await?)
    }

    async fn media_item_by_tmdb(
        &self,
        _ctx: &Context<'_>,
        tmdb_id: String,
    ) -> Result<Option<MediaItem>> {
        Ok(repo::get_media_item_by_tmdb(&tmdb_id).await?)
    }

    async fn media_item_by_tvdb(
        &self,
        _ctx: &Context<'_>,
        tvdb_id: String,
    ) -> Result<Option<MediaItem>> {
        Ok(repo::get_media_item_by_tvdb(&tvdb_id).await?)
    }

    async fn media_item_full_by_tmdb(
        &self,
        _ctx: &Context<'_>,
        tmdb_id: String,
    ) -> Result<Option<MediaItemFull>> {
        let item = repo::get_media_item_by_tmdb(&tmdb_id).await?;
        self.media_item_full_for(item).await
    }

    async fn media_item_full_by_tvdb(
        &self,
        _ctx: &Context<'_>,
        tvdb_id: String,
    ) -> Result<Option<MediaItemFull>> {
        let item = repo::get_media_item_by_tvdb(&tvdb_id).await?;
        self.media_item_full_for(item).await
    }

    async fn media_item_full(&self, _ctx: &Context<'_>, id: i64) -> Result<Option<MediaItemFull>> {
        let Some(item) = repo::get_media_item(id).await? else {
            return Ok(None);
        };
        self.media_item_full_inner(item).await.map(Some)
    }

    async fn media_item_state_by_tmdb(
        &self,
        _ctx: &Context<'_>,
        tmdb_id: String,
    ) -> Result<Option<MediaItemStateTree>> {
        let item = repo::get_media_item_by_tmdb(&tmdb_id).await?;
        self.media_item_state_for(item).await
    }

    async fn media_item_state_by_tvdb(
        &self,
        _ctx: &Context<'_>,
        tvdb_id: String,
    ) -> Result<Option<MediaItemStateTree>> {
        let item = repo::get_media_item_by_tvdb(&tvdb_id).await?;
        self.media_item_state_for(item).await
    }

    /// Bulk library-status lookup for a set of TMDB ids: one round trip for
    /// a whole suggested-content carousel row instead of one query per card.
    /// Ids not found in the library are simply absent from the result — the
    /// caller treats "no matching entry" as "not yet requested".
    async fn media_item_statuses_by_tmdb_ids(
        &self,
        _ctx: &Context<'_>,
        tmdb_ids: Vec<String>,
    ) -> Result<Vec<MediaItemStatus>> {
        check_bulk_ids_len(&tmdb_ids)?;
        Ok(repo::list_media_items_by_tmdb_ids(&tmdb_ids)
            .await?
            .into_iter()
            .map(MediaItemStatus::from)
            .collect())
    }

    /// Bulk library-status lookup for a set of TVDB ids — the show-side
    /// counterpart to `mediaItemStatusesByTmdbIds`, same batching rationale.
    async fn media_item_statuses_by_tvdb_ids(
        &self,
        _ctx: &Context<'_>,
        tvdb_ids: Vec<String>,
    ) -> Result<Vec<MediaItemStatus>> {
        check_bulk_ids_len(&tvdb_ids)?;
        Ok(repo::list_media_items_by_tvdb_ids(&tvdb_ids)
            .await?
            .into_iter()
            .map(MediaItemStatus::from)
            .collect())
    }

    async fn movies(&self, _ctx: &Context<'_>) -> Result<Vec<MediaItem>> {
        Ok(repo::list_movies().await?)
    }

    async fn shows(&self, _ctx: &Context<'_>) -> Result<Vec<MediaItem>> {
        Ok(repo::list_shows().await?)
    }

    async fn seasons(
        &self,
        _ctx: &Context<'_>,
        show_id: i64,
        include_specials: Option<bool>,
    ) -> Result<Vec<MediaItem>> {
        if include_specials == Some(false) {
            Ok(repo::list_seasons_excluding_specials(show_id).await?)
        } else {
            Ok(repo::list_seasons(show_id).await?)
        }
    }

    async fn episodes(&self, _ctx: &Context<'_>, season_id: i64) -> Result<Vec<MediaItem>> {
        Ok(repo::list_episodes(season_id).await?)
    }

    async fn filesystem_entries(
        &self,
        _ctx: &Context<'_>,
        media_item_id: i64,
    ) -> Result<Vec<FileSystemEntry>> {
        Ok(repo::get_filesystem_entries(media_item_id).await?)
    }

    async fn items_by_state(
        &self,
        _ctx: &Context<'_>,
        state: MediaItemState,
        item_type: MediaItemType,
    ) -> Result<Vec<MediaItem>> {
        Ok(repo::get_items_by_state(state, item_type).await?)
    }

    async fn items(
        &self,
        _ctx: &Context<'_>,
        page: Option<i64>,
        limit: Option<i64>,
        sort: Option<String>,
        types: Option<Vec<MediaItemType>>,
        search: Option<String>,
        states: Option<Vec<MediaItemState>>,
    ) -> Result<ItemsPage> {
        let page = page.unwrap_or(1);
        let limit = limit.unwrap_or(20);
        let items = repo::list_items_paginated(
            page,
            limit,
            sort,
            types.clone(),
            search.clone(),
            states.clone(),
        )
        .await?;
        let total_items = repo::count_items_filtered(types, search, states).await?;
        let total_pages = ((total_items + limit - 1) / limit).max(1);
        Ok(ItemsPage {
            items,
            page,
            limit,
            total_items,
            total_pages,
        })
    }

    async fn episode_by_tvdb(
        &self,
        _ctx: &Context<'_>,
        tvdb_id: String,
        episode_number: i32,
        season_number: Option<i32>,
    ) -> Result<Option<MediaItem>> {
        Ok(repo::find_episode_by_show_tvdb(&tvdb_id, episode_number, season_number).await?)
    }

    /// Return the number of media files expected for a media item:
    /// - Movie / Episode → 1
    /// - Season → total episode count
    /// - Show → total processable episode count (continuing shows exclude the last season)
    async fn expected_file_count(&self, _ctx: &Context<'_>, id: i64) -> Result<i64> {
        let item = repo::get_media_item(id)
            .await?
            .ok_or_else(|| Error::new("Item not found"))?;
        let count = match item.item_type {
            MediaItemType::Movie | MediaItemType::Episode => 1,
            MediaItemType::Season => repo::count_episodes_in_season(id).await?,
            MediaItemType::Show => repo::count_expected_files_for_show(id).await?,
        };
        Ok(count)
    }

    /// Return lookup key strings for an episode:
    /// `["abs:{absolute_number}", "{season_number}:{episode_number}"]`.
    async fn lookup_keys(&self, _ctx: &Context<'_>, id: i64) -> Result<Vec<String>> {
        let item = repo::get_media_item(id)
            .await?
            .ok_or_else(|| Error::new("Item not found"))?;
        Ok(episode_lookup_keys(&item))
    }
}

impl MediaQuery {
    /// Build a `MediaItemFull` from an already-resolved lookup result,
    /// short-circuiting to `None` when the item was not found.
    async fn media_item_full_for(&self, item: Option<MediaItem>) -> Result<Option<MediaItemFull>> {
        let Some(item) = item else {
            return Ok(None);
        };
        self.media_item_full_inner(item).await.map(Some)
    }

    /// Build a `MediaItemStateTree` from an already-resolved lookup result,
    /// short-circuiting to `None` when the item was not found.
    async fn media_item_state_for(
        &self,
        item: Option<MediaItem>,
    ) -> Result<Option<MediaItemStateTree>> {
        let Some(item) = item else {
            return Ok(None);
        };
        self.media_item_state_tree_inner(item).await.map(Some)
    }

    pub(crate) async fn media_item_state_tree_inner(
        &self,
        item: MediaItem,
    ) -> async_graphql::Result<MediaItemStateTree> {
        let (seasons, expected_file_count) = if item.item_type == MediaItemType::Show {
            let (seasons, mut episodes_by_season) = load_show_tree(item.id).await?;

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
                    .map(|s| {
                        episodes_by_season
                            .get(&s.id)
                            .map_or(0, |episodes| episodes.len() as i64)
                    })
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
            item_request_id: item.item_request_id,
        })
    }

    pub(super) async fn media_item_full_inner(
        &self,
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

        let all_entries = repo::get_filesystem_entries(item.id).await?;
        let media_entries: Vec<_> = all_entries
            .into_iter()
            .filter(|e| e.entry_type == FileSystemEntryType::Media)
            .map(with_metadata)
            .collect();
        let filesystem_entry = media_entries.first().cloned();
        let filesystem_entries = media_entries;

        let seasons = if item.item_type == MediaItemType::Show {
            let (seasons, mut episodes_by_season) = load_show_tree(item.id).await?;
            let episode_ids: Vec<i64> = episodes_by_season
                .values()
                .flat_map(|episodes| episodes.iter().map(|episode| episode.id))
                .collect();
            let mut episode_entries = if episode_ids.is_empty() {
                Vec::new()
            } else {
                filesystem_entries::Entity::find()
                    .filter(filesystem_entries::Column::EntryType.eq(FileSystemEntryType::Media))
                    .filter(
                        filesystem_entries::Column::MediaItemId.is_in(episode_ids.iter().copied()),
                    )
                    .into_model::<FileSystemEntry>()
                    .all(orm())
                    .await?
            };

            episode_entries.sort_by_key(|e| e.media_item_id);
            let mut entries_by_episode =
                group_sorted_by_key(episode_entries.into_iter().map(with_metadata), |e| {
                    e.media_item_id
                });

            let mut season_fulls = Vec::with_capacity(seasons.len());
            for season in seasons {
                let season_episodes = episodes_by_season.remove(&season.id).unwrap_or_default();
                let mut episode_fulls = Vec::with_capacity(season_episodes.len());
                for episode in season_episodes {
                    let ep_media = take_group(&mut entries_by_episode, &episode.id);
                    let ep_fs = ep_media.first().cloned();
                    episode_fulls.push(EpisodeFull {
                        item: episode,
                        filesystem_entry: ep_fs,
                        filesystem_entries: ep_media,
                    });
                }
                season_fulls.push(SeasonFull {
                    item: season,
                    episodes: episode_fulls,
                });
            }
            season_fulls
        } else {
            vec![]
        };

        Ok(MediaItemFull {
            item,
            filesystem_entry,
            filesystem_entries,
            seasons,
        })
    }
}
