use async_trait::async_trait;
use serde::Serialize;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_db::repo;

#[derive(Default)]
pub struct EmbyPlugin;

register_plugin!(EmbyPlugin);

#[async_trait]
impl Plugin for EmbyPlugin {
    fn name(&self) -> &'static str {
        "emby"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemDownloadSuccess]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
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
            RivenEvent::MediaItemDownloadSuccess { id, title, .. } => {
                let url = ctx.require_setting("url")?.trim_end_matches('/').to_string();
                let api_key = ctx.require_setting("apikey")?;

                let entries = repo::get_media_entries(&ctx.db_pool, *id).await?;

                if entries.is_empty() {
                    tracing::warn!(id, title, "emby: no filesystem entries for media item");
                    return Ok(HookResponse::Empty);
                }

                for entry in &entries {
                    refresh_path(&ctx.http_client, &url, api_key, &entry.path).await?;
                }

                Ok(HookResponse::Empty)
            }
            _ => Ok(HookResponse::Empty),
        }
    }
}

#[derive(Serialize)]
struct LibraryUpdate<'a> {
    #[serde(rename = "Updates")]
    updates: Vec<PathUpdate<'a>>,
}

#[derive(Serialize)]
struct PathUpdate<'a> {
    #[serde(rename = "Path")]
    path: &'a str,
    #[serde(rename = "UpdateType")]
    update_type: &'static str,
}

async fn refresh_path(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
    path: &str,
) -> anyhow::Result<()> {
    let body = LibraryUpdate {
        updates: vec![PathUpdate {
            path,
            update_type: "Created",
        }],
    };

    let resp = client
        .post(format!("{url}/Library/Media/Updated"))
        .query(&[("api_key", api_key)])
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("emby refresh failed with status {}", resp.status());
    }

    tracing::info!(path, "emby library path refreshed");
    Ok(())
}
