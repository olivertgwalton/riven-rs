use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{ContentCollection, Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use serde::Deserialize;

const MDBLIST_BASE_URL: &str = "https://api.mdblist.com/";

#[derive(Default)]
pub struct MdblistPlugin;

register_plugin!(MdblistPlugin);

#[async_trait]
impl Plugin for MdblistPlugin {
    fn name(&self) -> &'static str {
        "mdblist"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::ContentServiceRequested]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        let api_key = match settings.get("apikey") {
            Some(k) => k,
            None => return Ok(false),
        };
        // mdblist uses query param auth, not header
        let resp = reqwest::Client::new()
            .get(format!("{MDBLIST_BASE_URL}user?apikey={api_key}"))
            .send()
            .await;
        Ok(resp.is_ok())
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("apikey", "API Key", "password").required(),
            SettingField::new("lists", "Lists", "text")
                .with_placeholder("list-url, another-url")
                .with_description("Comma-separated MDBList list URLs or IDs."),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::ContentServiceRequested => {
                let api_key = ctx.require_setting("apikey")?;

                let lists = ctx.settings.get_list("lists");

                let mut content = ContentCollection::default();

                for list_name in &lists {
                    // Validate format: username/listname
                    if !list_name.contains('/')
                        || list_name.starts_with('/')
                        || list_name.ends_with('/')
                    {
                        tracing::warn!(
                            list_name,
                            "invalid mdblist list name format (expected username/listname)"
                        );
                        continue;
                    }

                    let items = fetch_list_items(&ctx.http_client, api_key, list_name).await?;

                    for item in items {
                        match item.media_type.as_deref() {
                            Some("movie") => {
                                content.insert_movie(ExternalIds {
                                    imdb_id: item.imdb_id,
                                    tmdb_id: item.tmdb_id.map(|id| id.to_string()),
                                    tvdb_id: item.tvdb_id.map(|id| id.to_string()),
                                    ..Default::default()
                                });
                            }
                            Some("show") => {
                                content.insert_show(ExternalIds {
                                    imdb_id: item.imdb_id,
                                    tvdb_id: item.tvdb_id.map(|id| id.to_string()),
                                    ..Default::default()
                                });
                            }
                            _ => {}
                        }
                    }
                }

                Ok(content.into_hook_response())
            }
            _ => Ok(HookResponse::Empty),
        }
    }
}

async fn fetch_list_items(
    client: &reqwest::Client,
    api_key: &str,
    list_name: &str,
) -> anyhow::Result<Vec<MdblistItem>> {
    let mut all_items = Vec::new();
    let mut offset = 0;

    loop {
        let url =
            format!("{MDBLIST_BASE_URL}lists/{list_name}/items?apikey={api_key}&offset={offset}");

        let resp = client.get(&url).send().await?;
        let has_more = resp
            .headers()
            .get("x-has-more")
            .and_then(|v| v.to_str().ok())
            == Some("true");

        let items: Vec<MdblistItem> = resp.json().await?;
        let count = items.len();
        all_items.extend(items);

        if !has_more || count == 0 {
            break;
        }
        offset += count;
    }

    Ok(all_items)
}

#[derive(Deserialize)]
struct MdblistItem {
    #[serde(rename = "mediatype")]
    media_type: Option<String>,
    imdb_id: Option<String>,
    tmdb_id: Option<String>,
    tvdb_id: Option<i64>,
}
