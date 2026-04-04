use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{validate_api_key, ContentCollection, Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use serde::Deserialize;

const LISTRR_BASE_URL: &str = "https://listrr.pro/api/";

#[derive(Default)]
pub struct ListrrPlugin;

register_plugin!(ListrrPlugin);

#[async_trait]
impl Plugin for ListrrPlugin {
    fn name(&self) -> &'static str {
        "listrr"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::ContentServiceRequested]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        validate_api_key(
            settings,
            "apikey",
            &format!("{LISTRR_BASE_URL}List/My/1"),
            "x-api-key",
        )
        .await
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("apikey", "API Key", "password").required(),
            SettingField::new("movielists", "Movie List IDs", "text")
                .with_placeholder("id1, id2")
                .with_description("Comma-separated Listrr movie list IDs."),
            SettingField::new("showlists", "Show List IDs", "text")
                .with_placeholder("id1, id2")
                .with_description("Comma-separated Listrr show list IDs."),
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

                let movie_lists = ctx.settings.get_list("movielists");
                let show_lists = ctx.settings.get_list("showlists");

                let mut content = ContentCollection::default();

                // Fetch movie lists
                for list_id in &movie_lists {
                    if list_id.len() != 24 {
                        tracing::warn!(list_id, "invalid listrr list ID (must be 24 chars)");
                        continue;
                    }

                    let items =
                        fetch_list_items(&ctx.http_client, api_key, "Movies", list_id).await?;

                    for item in items {
                        content.insert_movie(item);
                    }
                }

                // Fetch show lists
                for list_id in &show_lists {
                    if list_id.len() != 24 {
                        tracing::warn!(list_id, "invalid listrr list ID (must be 24 chars)");
                        continue;
                    }

                    let items =
                        fetch_list_items(&ctx.http_client, api_key, "Shows", list_id).await?;

                    for item in items {
                        content.insert_show(item);
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
    list_type: &str,
    list_id: &str,
) -> anyhow::Result<Vec<ExternalIds>> {
    let mut all_items = Vec::new();
    let mut page = 1;

    loop {
        let url =
            format!("{LISTRR_BASE_URL}List/{list_type}/{list_id}/ReleaseDate/Descending/{page}");

        let resp: ListrrResponse = client
            .get(&url)
            .header("x-api-key", api_key)
            .send()
            .await?
            .json()
            .await?;

        for item in &resp.items {
            all_items.push(ExternalIds {
                imdb_id: item.imdb_id.clone(),
                tvdb_id: item.tvdb_id.map(|id| id.to_string()),
                tmdb_id: item.tmdb_id.map(|id| id.to_string()),
                ..Default::default()
            });
        }

        if page >= resp.total_pages.unwrap_or(1) {
            break;
        }
        page += 1;
    }

    Ok(all_items)
}

#[derive(Deserialize)]
struct ListrrResponse {
    #[serde(default)]
    items: Vec<ListrrItem>,
    #[serde(rename = "totalPages")]
    total_pages: Option<i32>,
}

#[derive(Deserialize)]
struct ListrrItem {
    #[serde(rename = "imDbId")]
    imdb_id: Option<String>,
    #[serde(rename = "tvDbId")]
    tvdb_id: Option<i64>,
    #[serde(rename = "tmDbId")]
    tmdb_id: Option<i64>,
}
