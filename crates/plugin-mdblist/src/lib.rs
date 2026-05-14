use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{ContentCollection, Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use url::Url;

const MDBLIST_BASE_URL: &str = "https://api.mdblist.com/";

pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("mdblist").with_rate_limit(50, Duration::from_secs(1));

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

    async fn validate(
        &self,
        settings: &PluginSettings,
        http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        let api_key = match settings.get("apikey") {
            Some(k) => k,
            None => return Ok(false),
        };
        // mdblist uses query param auth, not header
        let resp = http
            .send(PROFILE, |client| {
                client.get(format!("{MDBLIST_BASE_URL}user?apikey={api_key}"))
            })
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

    async fn query_content(
        &self,
        _query: &str,
        args: &serde_json::Value,
        ctx: &PluginContext,
    ) -> anyhow::Result<riven_core::types::ContentServiceResponse> {
        let api_key = ctx.require_setting("apikey")?;
        let list_names: Vec<String> = args
            .get("list_names")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let content = fetch_and_build_content(&ctx.http, api_key, &list_names).await?;
        Ok(content.into_response())
    }

    async fn on_content_service_requested(
        &self,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let api_key = ctx.require_setting("apikey")?;
        let lists = ctx.settings.get_list("lists");
        let content = fetch_and_build_content(&ctx.http, api_key, &lists).await?;
        Ok(content.into_hook_response())
    }
}

async fn fetch_and_build_content(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    raw_lists: &[String],
) -> anyhow::Result<ContentCollection> {
    let mut content = ContentCollection::default();
    let mut seen_movie_ids = HashSet::new();
    let mut seen_show_ids = HashSet::new();

    for raw_list in raw_lists {
        let Some(list_name) = normalize_list_name(raw_list) else {
            tracing::warn!(list = raw_list, "invalid MDBList list reference");
            continue;
        };
        let items = fetch_list_items(http, api_key, &list_name).await?;

        for item in items.movies {
            let Some(dedupe_key) = item
                .id
                .map(|id| id.to_string())
                .or_else(|| item.tmdb_id().map(|id| id.to_string()))
                .or_else(|| item.imdb_id())
            else {
                continue;
            };
            if !seen_movie_ids.insert(dedupe_key) {
                continue;
            }
            content.insert_movie(ExternalIds {
                imdb_id: item.imdb_id(),
                tmdb_id: item.tmdb_id().map(|id| id.to_string()),
                tvdb_id: item.tvdb_id().map(|id| id.to_string()),
                ..Default::default()
            });
        }

        for item in items.shows {
            let Some(dedupe_key) = item
                .id
                .map(|id| id.to_string())
                .or_else(|| item.tvdb_id().map(|id| id.to_string()))
                .or_else(|| item.imdb_id())
            else {
                continue;
            };
            if !seen_show_ids.insert(dedupe_key) {
                continue;
            }
            content.insert_show(ExternalIds {
                imdb_id: item.imdb_id(),
                tvdb_id: item.tvdb_id().map(|id| id.to_string()),
                ..Default::default()
            });
        }
    }
    Ok(content)
}

async fn fetch_list_items(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    list_name: &str,
) -> anyhow::Result<MdblistListItems> {
    let mut movie_ids = HashMap::new();
    let mut show_ids = HashMap::new();
    let mut offset = 0;

    loop {
        let url =
            format!("{MDBLIST_BASE_URL}lists/{list_name}/items?apikey={api_key}&offset={offset}");

        let resp = http
            .send_data(PROFILE, Some(url.clone()), |client| {
                client.get(&url)
            })
            .await?;
        let has_more = resp
            .headers()
            .get("x-has-more")
            .and_then(|v| v.to_str().ok())
            == Some("true");

        let items: MdblistListItemsResponse = resp.json()?;
        let mut count = 0;

        for item in items.movies.unwrap_or_default() {
            if let Some(id) = item.id.or(item.tmdb_id()) {
                count += 1;
                movie_ids.entry(id).or_insert(item);
            }
        }

        for item in items.shows.unwrap_or_default() {
            if let Some(id) = item.id.or(item.tvdb_id()) {
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
    movies: Option<Vec<MdblistItem>>,
    shows: Option<Vec<MdblistItem>>,
}

struct MdblistListItems {
    movies: Vec<MdblistItem>,
    shows: Vec<MdblistItem>,
}

/// A single movie/show entry from the list-items endpoint.
///
/// MDBList is inconsistent about where it exposes external IDs: every item
/// carries a nested `ids` object, but only some also repeat `imdb_id`/`tvdb_id`
/// at the top level — shows have been observed with the top-level fields
/// missing entirely. Parse both shapes and let the accessors prefer the nested
/// `ids` object so neither movies nor shows silently lose their IDs (which
/// would land them in the library as un-indexable "Unknown" entries).
#[derive(Clone, Deserialize)]
struct MdblistItem {
    id: Option<i64>,
    imdb_id: Option<String>,
    tvdb_id: Option<i64>,
    #[serde(default)]
    ids: MdblistIds,
}

#[derive(Clone, Default, Deserialize)]
struct MdblistIds {
    imdb: Option<String>,
    tmdb: Option<i64>,
    tvdb: Option<i64>,
}

impl MdblistItem {
    fn imdb_id(&self) -> Option<String> {
        self.ids.imdb.clone().or_else(|| self.imdb_id.clone())
    }

    fn tmdb_id(&self) -> Option<i64> {
        self.ids.tmdb
    }

    fn tvdb_id(&self) -> Option<i64> {
        self.ids.tvdb.or(self.tvdb_id)
    }
}

#[cfg(test)]
mod tests;
