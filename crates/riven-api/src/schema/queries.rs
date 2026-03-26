use async_graphql::*;
use riven_core::types::*;
use riven_db::entities::*;
use riven_db::repo;
use std::sync::Arc;
use riven_core::plugin::{PluginRegistry, SettingField};
use riven_core::settings::RivenSettings;

use super::helpers::derive_media_metadata;
use super::types::*;
use super::types::PluginInfo;

// ── Core query ──

#[derive(Default)]
pub struct CoreQuery;

#[Object]
impl CoreQuery {
    /// Get a media item by ID.
    async fn media_item(&self, ctx: &Context<'_>, id: i64) -> Result<Option<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_media_item(pool, id).await?)
    }

    /// Look up media item by IMDB ID.
    async fn media_item_by_imdb(
        &self,
        ctx: &Context<'_>,
        imdb_id: String,
    ) -> Result<Option<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_media_item_by_imdb(pool, &imdb_id).await?)
    }

    /// Look up media item by TMDB ID.
    async fn media_item_by_tmdb(
        &self,
        ctx: &Context<'_>,
        tmdb_id: String,
    ) -> Result<Option<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_media_item_by_tmdb(pool, &tmdb_id).await?)
    }

    /// Look up media item by TVDB ID.
    async fn media_item_by_tvdb(
        &self,
        ctx: &Context<'_>,
        tvdb_id: String,
    ) -> Result<Option<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_media_item_by_tvdb(pool, &tvdb_id).await?)
    }

    /// Get a media item's full data by TMDB ID.
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

    /// Get a media item's full data by TVDB ID.
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

    /// Get a media item with its filesystem entry and full season/episode tree (for shows).
    async fn media_item_full(
        &self,
        ctx: &Context<'_>,
        id: i64,
    ) -> Result<Option<MediaItemFull>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let Some(item) = repo::get_media_item(pool, id).await? else {
            return Ok(None);
        };
        self.media_item_full_inner(pool, item).await.map(Some)
    }

    /// List all movies.
    async fn movies(&self, ctx: &Context<'_>) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::list_movies(pool).await?)
    }

    /// List all shows.
    async fn shows(&self, ctx: &Context<'_>) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::list_shows(pool).await?)
    }

    /// Get seasons for a show.
    async fn seasons(&self, ctx: &Context<'_>, show_id: i64) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::list_seasons(pool, show_id).await?)
    }

    /// Get episodes for a season.
    async fn episodes(&self, ctx: &Context<'_>, season_id: i64) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::list_episodes(pool, season_id).await?)
    }

    /// Get filesystem entries for a media item.
    async fn filesystem_entries(
        &self,
        ctx: &Context<'_>,
        media_item_id: i64,
    ) -> Result<Vec<FileSystemEntry>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_filesystem_entries(pool, media_item_id).await?)
    }

    /// Get streams for a media item.
    async fn streams(&self, ctx: &Context<'_>, media_item_id: i64) -> Result<Vec<Stream>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_streams_for_item(pool, media_item_id).await?)
    }

    /// Get items by state and type.
    async fn items_by_state(
        &self,
        ctx: &Context<'_>,
        state: MediaItemState,
        item_type: MediaItemType,
    ) -> Result<Vec<MediaItem>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_items_by_state(pool, state, item_type).await?)
    }

    /// List items with pagination and optional filtering.
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

        Ok(ItemsPage {
            items,
            page,
            limit,
            total_items,
            total_pages,
        })
    }

    /// Get the current rank settings. Returns defaults if not yet configured.
    async fn rank_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_setting(pool, "rank_settings")
            .await?
            .unwrap_or_else(|| serde_json::to_value(riven_rank::RankSettings::default()).unwrap()))
    }

    /// Get all stored settings as a JSON object.
    async fn all_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_all_settings(pool).await?)
    }

    /// Get info about all registered plugins.
    async fn plugin_info(&self, ctx: &Context<'_>) -> Result<Vec<PluginInfo>> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        Ok(registry
            .all_plugins_info()
            .await
            .into_iter()
            .map(|p| PluginInfo {
                name: p.name,
                version: p.version,
                valid: p.valid,
                schema: serde_json::to_value(p.schema)
                    .unwrap_or(serde_json::Value::Array(vec![])),
            })
            .collect())
    }

    /// Get DB-stored settings for a specific plugin.
    async fn plugin_settings(
        &self,
        ctx: &Context<'_>,
        plugin: String,
    ) -> Result<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let key = format!("plugin.{plugin}");
        Ok(repo::get_setting(pool, &key)
            .await?
            .unwrap_or(serde_json::Value::Object(Default::default())))
    }

    /// Return the SettingField schema for the general (non-plugin) settings.
    async fn general_settings_schema(&self) -> Result<serde_json::Value> {
        let schema: Vec<SettingField> = vec![
            SettingField::new("dubbed_anime_only", "Dubbed anime only", "boolean")
                .with_description("Only fetch dubbed versions of anime titles."),
            SettingField::new("retry_interval_secs", "Retry interval (seconds)", "number")
                .with_default("86400")
                .with_description("How often to retry stuck items. 0 = disabled. Default: 86400 (24 h)."),
            SettingField::new("minimum_average_bitrate_movies", "Min bitrate — movies (Mbps)", "number")
                .with_placeholder("Disabled")
                .with_description("Reject movie streams below this average bitrate. Leave blank to disable."),
            SettingField::new("minimum_average_bitrate_episodes", "Min bitrate — episodes (Mbps)", "number")
                .with_placeholder("Disabled")
                .with_description("Reject episode streams below this average bitrate. Leave blank to disable."),
            SettingField::new("maximum_average_bitrate_movies", "Max bitrate — movies (Mbps)", "number")
                .with_placeholder("Disabled")
                .with_description("Reject movie streams above this average bitrate (e.g. 50 to avoid large REMUXes). Leave blank to disable."),
            SettingField::new("maximum_average_bitrate_episodes", "Max bitrate — episodes (Mbps)", "number")
                .with_placeholder("Disabled")
                .with_description("Reject episode streams above this average bitrate. Leave blank to disable."),
        ];
        Ok(serde_json::to_value(schema).unwrap_or(serde_json::Value::Array(vec![])))
    }

    /// Get general (non-plugin) settings. Returns defaults merged with DB values.
    async fn general_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let defaults = RivenSettings::default();
        let mut result = serde_json::json!({
            "dubbed_anime_only": defaults.dubbed_anime_only,
            "minimum_average_bitrate_movies": defaults.minimum_average_bitrate_movies,
            "minimum_average_bitrate_episodes": defaults.minimum_average_bitrate_episodes,
            "maximum_average_bitrate_movies": defaults.maximum_average_bitrate_movies,
            "maximum_average_bitrate_episodes": defaults.maximum_average_bitrate_episodes,
            "retry_interval_secs": defaults.retry_interval_secs,
        });
        if let Some(stored) = repo::get_setting(pool, "general").await? {
            if let (Some(obj), Some(stored_obj)) = (result.as_object_mut(), stored.as_object()) {
                for (k, v) in stored_obj {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
        Ok(result)
    }
}

// ── CoreQuery helpers (not exposed as GraphQL) ──

impl CoreQuery {
    pub(super) async fn media_item_full_inner(
        &self,
        pool: &sqlx::PgPool,
        item: MediaItem,
    ) -> async_graphql::Result<MediaItemFull> {
        let with_metadata = |mut e: FileSystemEntry| {
            if e.media_metadata.is_none() {
                if let Some(ref filename) = e.original_filename {
                    e.media_metadata = Some(derive_media_metadata(filename));
                }
            }
            e
        };

        let filesystem_entry = repo::get_filesystem_entries(pool, item.id)
            .await?
            .into_iter()
            .find(|e| e.entry_type == FileSystemEntryType::Media)
            .map(with_metadata);

        let seasons = if item.item_type == MediaItemType::Show {
            let seasons = repo::list_seasons(pool, item.id).await?;
            let mut season_fulls = Vec::new();
            for season in seasons {
                let episodes = repo::list_episodes(pool, season.id).await?;
                let mut episode_fulls = Vec::new();
                for episode in episodes {
                    let ep_fs = repo::get_filesystem_entries(pool, episode.id)
                        .await?
                        .into_iter()
                        .find(|e| e.entry_type == FileSystemEntryType::Media)
                        .map(with_metadata);
                    episode_fulls.push(EpisodeFull {
                        item: episode,
                        filesystem_entry: ep_fs,
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
            seasons,
        })
    }
}
