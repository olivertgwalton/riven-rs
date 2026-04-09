use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{ContentCollection, Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use url::Url;

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
                .with_description("Comma-separated MDBList list URLs, slugs, or IDs."),
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
                let mut seen_movie_ids = HashSet::new();
                let mut seen_show_ids = HashSet::new();

                for raw_list in &lists {
                    let Some(list_name) = normalize_list_name(raw_list) else {
                        tracing::warn!(list = raw_list, "invalid MDBList list reference");
                        continue;
                    };

                    let items = fetch_list_items(&ctx.http_client, api_key, &list_name).await?;

                    for item in items.movies {
                        let Some(dedupe_key) = item
                            .id
                            .map(|id| id.to_string())
                            .or_else(|| item.ids.tmdb.map(|id| id.to_string()))
                            .or_else(|| item.ids.imdb.clone())
                        else {
                            continue;
                        };

                        if !seen_movie_ids.insert(dedupe_key) {
                            continue;
                        }

                        content.insert_movie(ExternalIds {
                            imdb_id: item.ids.imdb,
                            tmdb_id: item.ids.tmdb.map(|id| id.to_string()),
                            tvdb_id: item.ids.tvdb.map(|id| id.to_string()),
                            ..Default::default()
                        });
                    }

                    for item in items.shows {
                        let Some(dedupe_key) = item
                            .id
                            .map(|id| id.to_string())
                            .or_else(|| item.tvdb_id.map(|id| id.to_string()))
                            .or_else(|| item.imdb_id.clone())
                        else {
                            continue;
                        };

                        if !seen_show_ids.insert(dedupe_key) {
                            continue;
                        }

                        content.insert_show(ExternalIds {
                            imdb_id: item.imdb_id,
                            tvdb_id: item.tvdb_id.map(|id| id.to_string()),
                            ..Default::default()
                        });
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
) -> anyhow::Result<MdblistListItems> {
    let mut movie_ids = HashMap::new();
    let mut show_ids = HashMap::new();
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

        let items: MdblistListItemsResponse = resp.json().await?;
        let mut count = 0;

        for item in items.movies.unwrap_or_default() {
            if let Some(id) = item.id.or(item.ids.tmdb) {
                count += 1;
                movie_ids.entry(id).or_insert(item);
            }
        }

        for item in items.shows.unwrap_or_default() {
            if let Some(id) = item.id.or(item.tvdb_id) {
                count += 1;
                show_ids.entry(id).or_insert(item);
            }
        }

        if !has_more || count == 0 {
            break;
        }
        offset += count;
    }

    Ok(MdblistListItems {
        movies: movie_ids.into_values().collect(),
        shows: show_ids.into_values().collect(),
    })
}

fn normalize_list_name(value: &str) -> Option<String> {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }

    if !trimmed.contains("://") {
        return Some(trimmed.to_string());
    }

    let parsed = Url::parse(trimmed).ok()?;
    let segments = parsed
        .path_segments()?
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    match segments.as_slice() {
        ["lists", owner, list, ..] => Some(format!("{owner}/{list}")),
        [owner, list, ..] => Some(format!("{owner}/{list}")),
        [single] => Some((*single).to_string()),
        _ => None,
    }
}

#[derive(Deserialize)]
struct MdblistListItemsResponse {
    movies: Option<Vec<MdblistMovie>>,
    shows: Option<Vec<MdblistShow>>,
}

struct MdblistListItems {
    movies: Vec<MdblistMovie>,
    shows: Vec<MdblistShow>,
}

#[derive(Clone, Deserialize)]
struct MdblistMovie {
    id: Option<i64>,
    #[serde(default)]
    ids: MdblistMovieIds,
}

#[derive(Clone, Default, Deserialize)]
struct MdblistMovieIds {
    imdb: Option<String>,
    tmdb: Option<i64>,
    tvdb: Option<i64>,
}

#[derive(Clone, Deserialize)]
struct MdblistShow {
    id: Option<i64>,
    imdb_id: Option<String>,
    tvdb_id: Option<i64>,
}
