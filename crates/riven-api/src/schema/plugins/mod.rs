use async_graphql::*;
use riven_core::plugin::PluginRegistry;
use riven_core::types::{ContentServiceResponse, ExternalIds};
use riven_db::repo;
use std::sync::Arc;

use crate::schema::auth::require_settings_access;

// ── Settings type ──────────────────────────────────────────────────────────────

/// Merged plugin settings. Each field returns that plugin's current settings
/// JSON with an `enabled` key injected.
pub struct Settings {
    registry: Arc<PluginRegistry>,
    pool: sqlx::PgPool,
}

impl Settings {
    async fn field(&self, name: &str) -> Result<serde_json::Value> {
        let mut settings = self
            .registry
            .get_plugin_settings_json(name)
            .await
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let enabled = repo::get_plugin_enabled(&self.pool, name).await?;
        if let Some(obj) = settings.as_object_mut() {
            obj.insert("enabled".to_string(), serde_json::Value::Bool(enabled));
        }
        Ok(settings)
    }
}

#[Object]
impl Settings {
    async fn comet(&self) -> Result<serde_json::Value> {
        self.field("comet").await
    }
    async fn plex(&self) -> Result<serde_json::Value> {
        self.field("plex").await
    }
    async fn emby(&self) -> Result<serde_json::Value> {
        self.field("emby").await
    }
    async fn jellyfin(&self) -> Result<serde_json::Value> {
        self.field("jellyfin").await
    }
    async fn listrr(&self) -> Result<serde_json::Value> {
        self.field("listrr").await
    }
    async fn mdblist(&self) -> Result<serde_json::Value> {
        self.field("mdblist").await
    }
    async fn seerr(&self) -> Result<serde_json::Value> {
        self.field("seerr").await
    }
    async fn stremthru(&self) -> Result<serde_json::Value> {
        self.field("stremthru").await
    }
    async fn aiostreams(&self) -> Result<serde_json::Value> {
        self.field("aiostreams").await
    }
    async fn tmdb(&self) -> Result<serde_json::Value> {
        self.field("tmdb").await
    }
    async fn torrentio(&self) -> Result<serde_json::Value> {
        self.field("torrentio").await
    }
    async fn trakt(&self) -> Result<serde_json::Value> {
        self.field("trakt").await
    }
    async fn tvdb(&self) -> Result<serde_json::Value> {
        self.field("tvdb").await
    }
    async fn notifications(&self) -> Result<serde_json::Value> {
        self.field("notifications").await
    }
}

// ── SettingsQuery ──────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct SettingsQuery;

#[Object]
impl SettingsQuery {
    /// Return the current settings for every plugin as a typed object.
    async fn settings(&self, ctx: &Context<'_>) -> Result<Settings> {
        require_settings_access(ctx)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?.clone();
        let pool = ctx.data::<sqlx::PgPool>()?.clone();
        Ok(Settings { registry, pool })
    }
}

// ── Per-plugin isValid queries ─────────────────────────────────────────────────

macro_rules! is_valid_query {
    ($struct_name:ident, $fn_name:ident, $plugin_name:literal) => {
        #[derive(Default)]
        pub struct $struct_name;

        #[Object]
        impl $struct_name {
            async fn $fn_name(&self, ctx: &Context<'_>) -> Result<bool> {
                let registry = ctx.data::<Arc<PluginRegistry>>()?;
                Ok(registry.validate_plugin_current($plugin_name).await)
            }
        }
    };
}

is_valid_query!(CometQuery, comet_is_valid, "comet");
is_valid_query!(PlexQuery, plex_is_valid, "plex");
is_valid_query!(EmbyQuery, emby_is_valid, "emby");
is_valid_query!(JellyfinQuery, jellyfin_is_valid, "jellyfin");
is_valid_query!(StremThruQuery, stremthru_is_valid, "stremthru");
is_valid_query!(AioStreamsQuery, aiostreams_is_valid, "aiostreams");
is_valid_query!(TmdbQuery, tmdb_is_valid, "tmdb");
is_valid_query!(TorrentioQuery, torrentio_is_valid, "torrentio");
is_valid_query!(TraktQuery, trakt_is_valid, "trakt");
is_valid_query!(TvdbQuery, tvdb_is_valid, "tvdb");
is_valid_query!(NotificationsQuery, notifications_is_valid, "notifications");

// ── Content provider queries ───────────────────────────────────────────────────

#[derive(Default)]
pub struct ListrrQuery;

#[Object]
impl ListrrQuery {
    async fn listrr_is_valid(&self, ctx: &Context<'_>) -> Result<bool> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        Ok(registry.validate_plugin_current("listrr").await)
    }

    /// Fetch movies from the specified Listrr list IDs.
    async fn listrr_movies(
        &self,
        ctx: &Context<'_>,
        list_ids: Vec<String>,
    ) -> Result<Vec<ExternalIds>> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let args = serde_json::json!({ "list_ids": list_ids });
        let result = registry
            .query_plugin_content("listrr", "movies", &args)
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        Ok(result.movies)
    }

    /// Fetch shows from the specified Listrr list IDs.
    async fn listrr_shows(
        &self,
        ctx: &Context<'_>,
        list_ids: Vec<String>,
    ) -> Result<Vec<ExternalIds>> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let args = serde_json::json!({ "list_ids": list_ids });
        let result = registry
            .query_plugin_content("listrr", "shows", &args)
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        Ok(result.shows)
    }
}

#[derive(Default)]
pub struct MdblistQuery;

#[Object]
impl MdblistQuery {
    async fn mdblist_is_valid(&self, ctx: &Context<'_>) -> Result<bool> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        Ok(registry.validate_plugin_current("mdblist").await)
    }

    /// Fetch all items (movies + shows) from the specified MDBList list names/URLs.
    async fn mdblist_items(
        &self,
        ctx: &Context<'_>,
        list_names: Vec<String>,
    ) -> Result<ContentServiceResponse> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let args = serde_json::json!({ "list_names": list_names });
        registry
            .query_plugin_content("mdblist", "all", &args)
            .await
            .map_err(|e| Error::new(e.to_string()))
    }
}

#[derive(Default)]
pub struct SeerrQuery;

#[Object]
impl SeerrQuery {
    async fn seerr_is_valid(&self, ctx: &Context<'_>) -> Result<bool> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        Ok(registry.validate_plugin_current("seerr").await)
    }

    /// Fetch movies from Seerr. `filter` defaults to the plugin's configured filter.
    async fn seerr_movies(
        &self,
        ctx: &Context<'_>,
        filter: Option<String>,
    ) -> Result<Vec<ExternalIds>> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let args = match filter {
            Some(f) => serde_json::json!({ "filter": f }),
            None => serde_json::Value::Object(Default::default()),
        };
        let result = registry
            .query_plugin_content("seerr", "movies", &args)
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        Ok(result.movies)
    }

    /// Fetch shows from Seerr. `filter` defaults to the plugin's configured filter.
    async fn seerr_shows(
        &self,
        ctx: &Context<'_>,
        filter: Option<String>,
    ) -> Result<Vec<ExternalIds>> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let args = match filter {
            Some(f) => serde_json::json!({ "filter": f }),
            None => serde_json::Value::Object(Default::default()),
        };
        let result = registry
            .query_plugin_content("seerr", "shows", &args)
            .await
            .map_err(|e| Error::new(e.to_string()))?;
        Ok(result.shows)
    }
}

// ── PluginsQuery — merged root ─────────────────────────────────────────────────

#[derive(MergedObject, Default)]
pub struct PluginsQuery(
    SettingsQuery,
    CometQuery,
    PlexQuery,
    EmbyQuery,
    JellyfinQuery,
    ListrrQuery,
    MdblistQuery,
    SeerrQuery,
    StremThruQuery,
    AioStreamsQuery,
    TmdbQuery,
    TorrentioQuery,
    TraktQuery,
    TvdbQuery,
    NotificationsQuery,
);
