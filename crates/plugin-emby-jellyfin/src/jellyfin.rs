use async_trait::async_trait;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_db::repo;

use crate::notify_paths;

#[derive(Default)]
pub struct JellyfinPlugin;

register_plugin!(JellyfinPlugin);

#[async_trait]
impl Plugin for JellyfinPlugin {
    fn name(&self) -> &'static str { "jellyfin" }
    fn version(&self) -> &'static str { "0.1.0" }
    fn subscribed_events(&self) -> &[EventType] { &[EventType::MediaItemDownloadSuccess] }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        Ok(settings.has("url") && settings.has("apikey"))
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![
            SettingField::new("url", "Server URL", "url")
                .required()
                .with_placeholder("http://localhost:8096"),
            SettingField::new("apikey", "API Key", "password").required(),
        ]
    }

    async fn handle_event(&self, event: &RivenEvent, ctx: &PluginContext) -> anyhow::Result<HookResponse> {
        if let RivenEvent::MediaItemDownloadSuccess { id, title, .. } = event {
            let url = ctx.require_setting("url")?.trim_end_matches('/').to_string();
            let api_key = ctx.require_setting("apikey")?;
            let entries = repo::get_media_entries(&ctx.db_pool, *id).await?;
            if entries.is_empty() {
                tracing::warn!(id, title, "jellyfin: no filesystem entries");
                return Ok(HookResponse::Empty);
            }
            let paths: Vec<String> = entries.into_iter().map(|e| e.path).collect();
            notify_paths(&ctx.http_client, &url, api_key, &paths, "jellyfin").await?;
        }
        Ok(HookResponse::Empty)
    }
}
