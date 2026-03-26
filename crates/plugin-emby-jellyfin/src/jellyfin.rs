use async_trait::async_trait;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;

#[derive(Default)]
pub struct JellyfinPlugin;

register_plugin!(JellyfinPlugin);

#[async_trait]
impl Plugin for JellyfinPlugin {
    fn name(&self) -> &'static str {
        "jellyfin"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemDownloadSuccess]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        // Jellyfin's API uses the same endpoint structure as Emby for validation.
        // We only check that required settings are present so the plugin remains
        // active even if the server is temporarily unreachable at startup.
        Ok(settings.has("url") && settings.has("apikey"))
    }


    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("url", "Server URL", "url")
                .required()
                .with_placeholder("http://localhost:8096"),
            SettingField::new("apikey", "API Key", "password").required(),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::MediaItemDownloadSuccess { title, .. } => {
                let url = ctx.require_setting("url")?.trim_end_matches('/').to_string();
                let api_key = ctx.require_setting("apikey")?;

                // Jellyfin's library refresh API operates at the library level,
                // not per-path — the path from the event is intentionally ignored.
                let resp = ctx
                    .http_client
                    .post(format!("{url}/Library/Refresh"))
                    .query(&[("api_key", api_key)])
                    .send()
                    .await?;

                if !resp.status().is_success() {
                    anyhow::bail!("jellyfin refresh failed with status {}", resp.status());
                }

                tracing::info!(title, "jellyfin library refresh triggered");
                Ok(HookResponse::Empty)
            }
            _ => Ok(HookResponse::Empty),
        }
    }
}
