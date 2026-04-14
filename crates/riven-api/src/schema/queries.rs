use async_graphql::*;
use riven_core::plugin::{PluginRegistry, SettingField};
use riven_core::settings::RivenSettings;
use riven_core::types::*;
use riven_db::entities::*;
use riven_db::repo;
use std::collections::HashMap;
use std::sync::Arc;

use super::helpers::derive_media_metadata;
use super::typed_items::MediaItemUnion;
use super::types::InstanceStatus;
use super::types::MediaItemStateTree;
use super::types::PluginInfo;
use super::types::*;

// ── Helpers ──

/// Inject `"default": N` into every `CustomRank` entry inside `custom_ranks`.
/// Used by both `rank_settings` and `quality_profiles` so the frontend always
/// has `cr.default` available regardless of which profile is active.
/// Remove `"rank": 0` from every CustomRank entry in `custom_ranks`.
/// Old DB data used 0 as the "unset" sentinel before `rank` became `Option<i64>`.
/// Stripping them lets the frontend fall through to the injected `"default"` value.
fn strip_zero_ranks(json: &mut serde_json::Value) {
    let Some(custom_ranks) = json.get_mut("custom_ranks").and_then(|v| v.as_object_mut()) else {
        return;
    };
    for cat in custom_ranks.values_mut() {
        if let Some(fields) = cat.as_object_mut() {
            for entry in fields.values_mut() {
                if let Some(obj) = entry.as_object_mut()
                    && obj.get("rank").and_then(|v| v.as_i64()) == Some(0)
                {
                    obj.remove("rank");
                }
            }
        }
    }
}

fn inject_rank_defaults(json: &mut serde_json::Value) {
    let defaults = riven_rank::defaults::RankingModel::default().to_category_map();
    let (Some(custom_ranks), Some(def_obj)) = (
        json.get_mut("custom_ranks").and_then(|v| v.as_object_mut()),
        defaults.as_object(),
    ) else {
        return;
    };
    for (cat, cat_defaults) in def_obj {
        if let (Some(rank_cat), Some(cat_obj)) = (
            custom_ranks.get_mut(cat).and_then(|v| v.as_object_mut()),
            cat_defaults.as_object(),
        ) {
            for (field, default_score) in cat_obj {
                if let Some(entry) = rank_cat.get_mut(field).and_then(|v| v.as_object_mut()) {
                    entry.insert("default".to_string(), default_score.clone());
                }
            }
        }
    }
}

async fn infer_setup_completed(pool: &sqlx::PgPool, registry: &PluginRegistry) -> Result<bool> {
    let enabled_profile_count = repo::get_enabled_profiles(pool).await?.len();
    let valid_enabled_plugin_count = registry
        .all_plugins_info()
        .await
        .into_iter()
        .filter(|plugin| plugin.enabled && plugin.valid)
        .count();

    Ok(valid_enabled_plugin_count > 0 && enabled_profile_count > 0)
}

// ── Core query ──

#[derive(Default)]
pub struct CoreQuery;

#[Object]
impl CoreQuery {
    /// Get a media item by ID as a discriminated union (Movie | Show | Season | Episode).
    async fn media_item_by_id(&self, ctx: &Context<'_>, id: i64) -> Result<Option<MediaItemUnion>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_media_item(pool, id)
            .await?
            .map(MediaItemUnion::from))
    }

    /// List up to 25 media items across all types, newest first.
    async fn media_items(&self, ctx: &Context<'_>) -> Result<Vec<MediaItemUnion>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let items = sqlx::query_as::<_, MediaItem>(
            "SELECT * FROM media_items ORDER BY created_at DESC LIMIT 25",
        )
        .fetch_all(pool)
        .await?;
        Ok(items.into_iter().map(MediaItemUnion::from).collect())
    }

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
    async fn media_item_full(&self, ctx: &Context<'_>, id: i64) -> Result<Option<MediaItemFull>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let Some(item) = repo::get_media_item(pool, id).await? else {
            return Ok(None);
        };
        self.media_item_full_inner(pool, item).await.map(Some)
    }

    /// Get a media item's lightweight state tree by TMDB ID.
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

    /// Get a media item's lightweight state tree by TVDB ID.
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
    /// Pass `include_specials: false` to exclude season 0 (special episodes), matching
    /// the default TypeScript behaviour. Omitting the argument returns all seasons.
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
    /// Each `custom_ranks` entry is annotated with a `"default"` field carrying
    /// the built-in score so the UI can display the effective value without a
    /// separate query.
    async fn rank_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        // Deserialise through RankSettings so serde fills in every missing field
        // with its default value — this ensures the canonical JSON always has the
        // full custom_ranks schema regardless of what (partial) data is in the DB.
        let settings: riven_rank::RankSettings = repo::get_setting(pool, "rank_settings")
            .await?
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        let mut json = serde_json::to_value(&settings)
            .map_err(|e| Error::new(format!("failed to serialise rank settings: {e}")))?;
        strip_zero_ranks(&mut json);
        inject_rank_defaults(&mut json);
        Ok(json)
    }

    /// Return all quality profiles as an ordered array of
    /// `{ id, label, description, settings }` objects.
    /// Each profile's `custom_ranks` entries include a `"default"` field so the
    /// UI shows the correct placeholder score after applying a profile.
    async fn quality_profiles(&self) -> Result<serde_json::Value> {
        let profiles: serde_json::Value = riven_rank::QualityProfile::ALL
            .iter()
            .map(|&p| {
                let mut settings =
                    serde_json::to_value(p.base_settings()).unwrap_or(serde_json::Value::Null);
                inject_rank_defaults(&mut settings);
                serde_json::json!({
                    "id":          p.id(),
                    "label":       p.label(),
                    "description": p.description(),
                    "settings":    settings,
                })
            })
            .collect();
        Ok(profiles)
    }

    /// Return the built-in default score for every CustomRank field, structured
    /// identically to `custom_ranks` in `rankSettings`.
    async fn rank_defaults(&self) -> Result<serde_json::Value> {
        Ok(riven_rank::RankingModel::default().to_category_map())
    }

    /// Return all ranking profiles (built-in + custom) with their enabled status.
    async fn custom_profiles(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let profiles = repo::list_ranking_profiles(pool).await?;
        Ok(serde_json::to_value(profiles)?)
    }

    /// Get all stored settings as a JSON object.
    async fn all_settings(&self, ctx: &Context<'_>) -> Result<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_all_settings(pool).await?)
    }

    /// Return instance-level status flags used by frontend bootstrap flows.
    async fn instance_status(&self, ctx: &Context<'_>) -> Result<InstanceStatus> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let explicit_setup_completed =
            match repo::get_setting(pool, "instance.setup_completed").await? {
                Some(serde_json::Value::Bool(value)) => value,
                _ => false,
            };
        let inferred_setup_completed = if explicit_setup_completed {
            false
        } else {
            let registry = ctx.data::<Arc<PluginRegistry>>()?;
            infer_setup_completed(pool, registry).await?
        };

        Ok(InstanceStatus {
            setup_completed: explicit_setup_completed || inferred_setup_completed,
        })
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
                enabled: p.enabled,
                valid: p.valid,
                schema: serde_json::to_value(p.schema).unwrap_or(serde_json::Value::Array(vec![])),
            })
            .collect())
    }

    /// Get effective settings for a specific plugin (env vars merged with any DB overrides).
    async fn plugin_settings(
        &self,
        ctx: &Context<'_>,
        plugin: String,
    ) -> Result<serde_json::Value> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let pool = ctx.data::<sqlx::PgPool>()?;
        let mut settings = registry
            .get_plugin_settings_json(&plugin)
            .await
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let enabled = repo::get_plugin_enabled(pool, &plugin).await?;
        if let Some(obj) = settings.as_object_mut() {
            obj.insert("enabled".to_string(), serde_json::Value::Bool(enabled));
        }
        Ok(settings)
    }

    /// Return whether a plugin is explicitly enabled.
    async fn plugin_enabled(&self, ctx: &Context<'_>, plugin: String) -> Result<bool> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_plugin_enabled(pool, &plugin).await?)
    }

    /// Return the SettingField schema for the general (non-plugin) settings.
    async fn general_settings_schema(&self) -> Result<serde_json::Value> {
        let schema: Vec<SettingField> = vec![
            SettingField::new("dubbed_anime_only", "Dubbed anime only", "boolean")
                .with_description("Only fetch dubbed versions of anime titles."),
            SettingField::new("retry_interval_secs", "Retry interval (seconds)", "number")
                .with_default("600")
                .with_description("How often to retry stuck items. 0 = disabled. Default: 600 (10 m)."),
            SettingField::new("schedule_offset_minutes", "Re-index offset (minutes)", "number")
                .with_default("30")
                .with_description("How long after a known release/air date to wait before re-indexing an unreleased or ongoing item."),
            SettingField::new("unknown_air_date_offset_days", "Fallback re-index delay (days)", "number")
                .with_default("7")
                .with_description("Fallback delay used when an unreleased or ongoing item has no known future air date."),
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
            SettingField::new("filesystem", "Filesystem", "object")
                .with_description("Virtual filesystem mount settings and filtered library profile aliases.")
                .with_fields(vec![
                    SettingField::new("mount_path", "Mount path", "string")
                        .with_placeholder("/mount")
                        .with_description("Where the virtual filesystem should be mounted."),
                    SettingField::new("library_profiles", "Library profiles", "dictionary")
                        .with_description("Named filtered views that expose matching items under additional virtual paths.")
                        .with_key_placeholder("profile_key")
                        .with_add_label("Add profile")
                        .with_item_fields(vec![
                            SettingField::new("name", "Name", "string")
                                .required()
                                .with_description("Display name for this profile."),
                            SettingField::new("library_path", "Library path", "string")
                                .required()
                                .with_placeholder("/anime")
                                .with_description("Virtual path prefix to expose for this profile."),
                            SettingField::new("enabled", "Enabled", "boolean")
                                .with_description("Disable a profile without deleting its rules."),
                            SettingField::new("filter_rules", "Filter rules", "object")
                                .with_description("Only items matching all configured rules will appear in this profile. Positive values inside token lists use OR matching; prefix a value with ! to exclude it.")
                                .with_fields(vec![
                                    SettingField::new("content_types", "Content types", "string_array")
                                        .with_options(&["movie", "show"])
                                        .with_description("Restrict the profile to movies, shows, or both."),
                                    SettingField::new("genres", "Genres", "string_array")
                                        .with_description("Genre filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("networks", "Networks", "string_array")
                                        .with_description("Network filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("languages", "Languages", "string_array")
                                        .with_description("Language filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("countries", "Countries", "string_array")
                                        .with_description("Country filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("content_ratings", "Content ratings", "string_array")
                                        .with_description("Content rating filters. Any positive value may match. Prefix a value with ! to exclude it."),
                                    SettingField::new("min_year", "Min year", "number")
                                        .with_description("Minimum release year for matching items."),
                                    SettingField::new("max_year", "Max year", "number")
                                        .with_description("Maximum release year for matching items."),
                                    SettingField::new("min_rating", "Min rating", "number")
                                        .with_description("Minimum numeric rating for matching items."),
                                    SettingField::new("max_rating", "Max rating", "number")
                                        .with_description("Maximum numeric rating for matching items."),
                                    SettingField::new("is_anime", "Anime filter", "nullable_boolean")
                                        .with_description("Only anime, only non-anime, or leave unset for any item."),
                                ]),
                        ]),
                ]),
        ];
        Ok(serde_json::to_value(schema).unwrap_or(serde_json::Value::Array(vec![])))
    }

    /// Fetch an episode by the parent show's TVDB ID, episode number, and optional season
    /// number. When `season_number` is omitted the lookup uses absolute episode numbering.
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
            "schedule_offset_minutes": defaults.schedule_offset_minutes,
            "unknown_air_date_offset_days": defaults.unknown_air_date_offset_days,
            "filesystem": defaults.filesystem,
        });
        if let Some(stored) = repo::get_setting(pool, "general").await?
            && let (Some(obj), Some(stored_obj)) = (result.as_object_mut(), stored.as_object())
        {
            for (k, v) in stored_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
        Ok(result)
    }
}

// ── CoreQuery helpers (not exposed as GraphQL) ──

impl CoreQuery {
    pub(super) async fn media_item_state_tree_inner(
        &self,
        pool: &sqlx::PgPool,
        item: MediaItem,
    ) -> async_graphql::Result<MediaItemStateTree> {
        let (seasons, expected_file_count) = if item.item_type == MediaItemType::Show {
            let seasons = repo::list_seasons(pool, item.id).await?;
            let season_ids: Vec<i64> = seasons.iter().map(|season| season.id).collect();
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

            // Compute the show-level expected file count from already-loaded data
            // (same logic as count_expected_files_for_show, no extra DB query).
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
            let season_ids: Vec<i64> = seasons.iter().map(|season| season.id).collect();
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
            let episode_ids: Vec<i64> = episodes.iter().map(|episode| episode.id).collect();
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
