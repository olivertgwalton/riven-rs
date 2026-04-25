use async_trait::async_trait;

use super::{PluginContext, SettingField};
use crate::events::{
    DownloadSuccessInfo, EventType, HookResponse, IndexRequest, RivenEvent, ScrapeRequest,
};
use crate::settings::PluginSettings;
use crate::types::{CachedStoreEntry, ContentServiceResponse, MediaItemType};

/// Plugin lifecycle and event handling.
///
/// Plugins override the per-event hooks they care about (`on_*`). The default
/// `handle_event` dispatches to those hooks based on the event variant — plugins
/// should not override `handle_event` directly.
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    fn show_in_settings(&self) -> bool {
        true
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[]
    }

    async fn validate(
        &self,
        _settings: &PluginSettings,
        _http: &crate::http::HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![]
    }

    /// Fetch content on-demand for a GraphQL query.
    /// `query` is `"movies"`, `"shows"`, or `"all"`; `args` is plugin-specific JSON.
    /// Returns empty by default; content-provider plugins override this.
    async fn query_content(
        &self,
        _query: &str,
        _args: &serde_json::Value,
        _ctx: &PluginContext,
    ) -> anyhow::Result<ContentServiceResponse> {
        Ok(ContentServiceResponse::default())
    }

    // ── Per-event hooks (override what you care about) ────────────────────────

    async fn on_core_started(&self, _ctx: &PluginContext) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_content_service_requested(
        &self,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_index_requested(
        &self,
        _req: &IndexRequest<'_>,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_index_success(
        &self,
        _id: i64,
        _title: &str,
        _item_type: MediaItemType,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_scrape_requested(
        &self,
        _req: &ScrapeRequest<'_>,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_download_requested(
        &self,
        _id: i64,
        _info_hash: &str,
        _magnet: &str,
        _cached_stores: &[CachedStoreEntry],
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_download_cache_check_requested(
        &self,
        _hashes: &[String],
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_download_provider_list_requested(
        &self,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_download_success(
        &self,
        _info: &DownloadSuccessInfo<'_>,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_items_deleted(
        &self,
        _item_ids: &[i64],
        _external_request_ids: &[String],
        _deleted_paths: &[String],
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_stream_link_requested(
        &self,
        _magnet: &str,
        _info_hash: &str,
        _provider: Option<&str>,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_active_playback_sessions_requested(
        &self,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    async fn on_debrid_user_info_requested(
        &self,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }

    /// Default dispatcher — do not override. Routes a `RivenEvent` to the matching `on_*` hook.
    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::CoreStarted => self.on_core_started(ctx).await,
            RivenEvent::CoreShutdown => Ok(HookResponse::Empty),
            RivenEvent::ContentServiceRequested => self.on_content_service_requested(ctx).await,
            RivenEvent::ItemRequestCreated { .. } => Ok(HookResponse::Empty),
            RivenEvent::ItemRequestUpdated { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemIndexRequested {
                id,
                item_type,
                imdb_id,
                tvdb_id,
                tmdb_id,
            } => {
                let req = IndexRequest {
                    id: *id,
                    item_type: *item_type,
                    imdb_id: imdb_id.as_deref(),
                    tvdb_id: tvdb_id.as_deref(),
                    tmdb_id: tmdb_id.as_deref(),
                };
                self.on_index_requested(&req, ctx).await
            }
            RivenEvent::MediaItemIndexSuccess {
                id,
                title,
                item_type,
            } => self.on_index_success(*id, title, *item_type, ctx).await,
            RivenEvent::MediaItemIndexError { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemIndexErrorIncorrectState { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemScrapeRequested {
                id,
                item_type,
                imdb_id,
                title,
                season,
                episode,
            } => {
                let req = ScrapeRequest {
                    id: *id,
                    item_type: *item_type,
                    imdb_id: imdb_id.as_deref(),
                    title,
                    season: *season,
                    episode: *episode,
                };
                self.on_scrape_requested(&req, ctx).await
            }
            RivenEvent::MediaItemScrapeSuccess { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemScrapeError { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemScrapeErrorIncorrectState { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemScrapeErrorNoNewStreams { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemDownloadRequested {
                id,
                info_hash,
                magnet,
                cached_stores,
            } => {
                self.on_download_requested(*id, info_hash, magnet, cached_stores, ctx)
                    .await
            }
            RivenEvent::MediaItemDownloadCacheCheckRequested { hashes } => {
                self.on_download_cache_check_requested(hashes, ctx).await
            }
            RivenEvent::MediaItemDownloadError { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemDownloadErrorIncorrectState { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemDownloadPartialSuccess { .. } => Ok(HookResponse::Empty),
            RivenEvent::MediaItemDownloadProviderListRequested => {
                self.on_download_provider_list_requested(ctx).await
            }
            RivenEvent::MediaItemDownloadSuccess {
                id,
                title,
                full_title,
                item_type,
                year,
                imdb_id,
                tmdb_id,
                poster_path,
                plugin_name,
                provider,
                duration_seconds,
            } => {
                let info = DownloadSuccessInfo {
                    id: *id,
                    title,
                    full_title: full_title.as_deref(),
                    item_type: *item_type,
                    year: *year,
                    imdb_id: imdb_id.as_deref(),
                    tmdb_id: tmdb_id.as_deref(),
                    poster_path: poster_path.as_deref(),
                    plugin_name,
                    provider: provider.as_deref(),
                    duration_seconds: *duration_seconds,
                };
                self.on_download_success(&info, ctx).await
            }
            RivenEvent::MediaItemStreamLinkRequested {
                magnet,
                info_hash,
                provider,
            } => {
                self.on_stream_link_requested(magnet, info_hash, provider.as_deref(), ctx)
                    .await
            }
            RivenEvent::MediaItemsDeleted {
                item_ids,
                external_request_ids,
                deleted_paths,
            } => {
                self.on_items_deleted(item_ids, external_request_ids, deleted_paths, ctx)
                    .await
            }
            RivenEvent::DebridUserInfoRequested => self.on_debrid_user_info_requested(ctx).await,
            RivenEvent::ActivePlaybackSessionsRequested => {
                self.on_active_playback_sessions_requested(ctx).await
            }
        }
    }
}
