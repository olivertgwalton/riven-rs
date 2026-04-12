use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::http::profiles;
use riven_core::plugin::{ContentCollection, Plugin, PluginContext, validate_api_key};
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

    async fn validate(
        &self,
        settings: &PluginSettings,
        http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        validate_api_key(
            http,
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
                let content =
                    fetch_configured_content(&ctx.http, api_key, &movie_lists, &show_lists).await?;
                Ok(content.into_hook_response())
            }
            _ => Ok(HookResponse::Empty),
        }
    }

    async fn query_content(
        &self,
        query: &str,
        args: &serde_json::Value,
        ctx: &PluginContext,
    ) -> anyhow::Result<riven_core::types::ContentServiceResponse> {
        let api_key = ctx.require_setting("apikey")?;
        let list_ids: Vec<String> = args
            .get("list_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let (movie_ids, show_ids) = match query {
            "movies" => (list_ids.clone(), vec![]),
            "shows" => (vec![], list_ids.clone()),
            _ => (list_ids.clone(), list_ids.clone()),
        };

        let content = fetch_configured_content(&ctx.http, api_key, &movie_ids, &show_ids).await?;
        Ok(content.into_response())
    }
}

async fn fetch_configured_content(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    movie_lists: &[String],
    show_lists: &[String],
) -> anyhow::Result<ContentCollection> {
    let mut content = ContentCollection::default();
    for list_id in movie_lists {
        if list_id.len() != 24 {
            tracing::warn!(list_id, "invalid listrr list ID (must be 24 chars)");
            continue;
        }
        for item in fetch_list_items(http, api_key, "Movies", list_id).await? {
            content.insert_movie(item);
        }
    }
    for list_id in show_lists {
        if list_id.len() != 24 {
            tracing::warn!(list_id, "invalid listrr list ID (must be 24 chars)");
            continue;
        }
        for item in fetch_list_items(http, api_key, "Shows", list_id).await? {
            content.insert_show(item);
        }
    }
    Ok(content)
}

async fn fetch_list_items(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    list_type: &str,
    list_id: &str,
) -> anyhow::Result<Vec<ExternalIds>> {
    let mut all_items = Vec::new();
    let mut page = 1;

    loop {
        let url =
            format!("{LISTRR_BASE_URL}List/{list_type}/{list_id}/ReleaseDate/Descending/{page}");

        let resp: ListrrResponse = http
            .get_json(profiles::LISTRR, url.clone(), |client| {
                client.get(&url).header("x-api-key", api_key)
            })
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
