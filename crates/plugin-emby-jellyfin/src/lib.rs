use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_db::repo;
use serde::Serialize;

#[derive(Default)]
pub struct EmbyPlugin;

#[derive(Default)]
pub struct JellyfinPlugin;

register_plugin!(EmbyPlugin);
register_plugin!(JellyfinPlugin);

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

/// Notify a Jellyfin/Emby server that the given VFS paths were created.
/// All paths are sent in a single request.
pub(crate) async fn notify_paths(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    paths: &[String],
    plugin: &'static str,
) -> anyhow::Result<()> {
    let updates = paths
        .iter()
        .map(|p| PathUpdate {
            path: p,
            update_type: "Created",
        })
        .collect();

    let resp = client
        .post(format!("{base_url}/Library/Media/Updated"))
        .query(&[("api_key", api_key)])
        .json(&LibraryUpdate { updates })
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("{plugin} notify failed: {}", resp.status());
    }

    tracing::info!(plugin, paths = paths.len(), "library paths notified");
    Ok(())
}

fn media_server_settings_schema() -> Vec<SettingField> {
    vec![
        SettingField::new("url", "Server URL", "url")
            .required()
            .with_placeholder("http://localhost:8096"),
        SettingField::new("apikey", "API Key", "password").required(),
    ]
}

async fn notify_media_server(
    plugin: &'static str,
    event: &RivenEvent,
    ctx: &PluginContext,
) -> anyhow::Result<HookResponse> {
    let RivenEvent::MediaItemDownloadSuccess { id, title, .. } = event else {
        return Ok(HookResponse::Empty);
    };

    let url = ctx
        .require_setting("url")?
        .trim_end_matches('/')
        .to_string();
    let api_key = ctx.require_setting("apikey")?;
    let entries = repo::get_media_entries(&ctx.db_pool, *id).await?;
    if entries.is_empty() {
        tracing::warn!(id, title, "{plugin}: no filesystem entries");
        return Ok(HookResponse::Empty);
    }
    let paths: Vec<String> = entries.into_iter().map(|e| e.path).collect();
    notify_paths(&ctx.http_client, &url, api_key, &paths, plugin).await?;
    Ok(HookResponse::Empty)
}

macro_rules! impl_media_server_plugin {
    ($plugin_ty:ident, $name:literal) => {
        #[async_trait]
        impl Plugin for $plugin_ty {
            fn name(&self) -> &'static str {
                $name
            }

            fn subscribed_events(&self) -> &[EventType] {
                &[EventType::MediaItemDownloadSuccess]
            }

            async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
                Ok(settings.has("url") && settings.has("apikey"))
            }

            fn settings_schema(&self) -> Vec<SettingField> {
                media_server_settings_schema()
            }

            async fn handle_event(
                &self,
                event: &RivenEvent,
                ctx: &PluginContext,
            ) -> anyhow::Result<HookResponse> {
                notify_media_server($name, event, ctx).await
            }
        }
    };
}

impl_media_server_plugin!(EmbyPlugin, "emby");
impl_media_server_plugin!(JellyfinPlugin, "jellyfin");
