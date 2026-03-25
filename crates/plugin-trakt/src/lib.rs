use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

const TRAKT_BASE_URL: &str = "https://api.trakt.tv";
const TRAKT_API_VERSION: &str = "2";

#[derive(Default)]
pub struct TraktPlugin;

register_plugin!(TraktPlugin);

#[async_trait]
impl Plugin for TraktPlugin {
    fn name(&self) -> &'static str {
        "trakt"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::ContentServiceRequested]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        Ok(settings.has("clientid"))
    }


    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("clientid", "Client ID", "password").required(),
            SettingField::new("accesstoken", "Access Token", "password"),
            SettingField::new("watchlist", "Enable Watchlist", "boolean").with_default("false"),
            SettingField::new("userlists", "User Lists", "text")
                .with_placeholder("list-slug, another-list")
                .with_description("Comma-separated Trakt list slugs."),
            SettingField::new("fetchtrending", "Fetch Trending", "boolean").with_default("false"),
            SettingField::new("trendingcount", "Trending Count", "number").with_default("10"),
            SettingField::new("fetchpopular", "Fetch Popular", "boolean").with_default("false"),
            SettingField::new("popularcount", "Popular Count", "number").with_default("10"),
            SettingField::new("fetchwatched", "Fetch Watched History", "boolean").with_default("false"),
            SettingField::new("watchedperiod", "Watched Period", "text")
                .with_default("weekly")
                .with_placeholder("weekly")
                .with_description("Period for watched history (daily, weekly, monthly, yearly, all)."),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::ContentServiceRequested => {
                let client_id = ctx.require_setting("clientid")?;
                let access_token = ctx.settings.get("accesstoken");

                let mut movies: HashMap<String, ExternalIds> = HashMap::new();
                let mut shows: HashMap<String, ExternalIds> = HashMap::new();

                // Watchlist (requires access token)
                if ctx.settings.get_or("watchlist", "false").to_lowercase() == "true" {
                    if let Some(token) = access_token {
                        collect_wrapped(
                            fetch_watchlist(&ctx.http_client, client_id, token, "movies").await?,
                            &mut movies,
                        );
                        collect_wrapped(
                            fetch_watchlist(&ctx.http_client, client_id, token, "shows").await?,
                            &mut shows,
                        );
                    } else {
                        tracing::warn!("trakt watchlist enabled but accesstoken not set");
                    }
                }

                // User lists
                let user_lists = ctx.settings.get_list("userlists");
                if let Some(token) = access_token {
                    for list_slug in &user_lists {
                        collect_wrapped(
                            fetch_user_list(
                                &ctx.http_client,
                                client_id,
                                token,
                                list_slug,
                                "movies",
                            )
                            .await?,
                            &mut movies,
                        );
                        collect_wrapped(
                            fetch_user_list(
                                &ctx.http_client,
                                client_id,
                                token,
                                list_slug,
                                "shows",
                            )
                            .await?,
                            &mut shows,
                        );
                    }
                }

                // Trending
                if ctx.settings.get_or("fetchtrending", "false").to_lowercase() == "true" {
                    let count: usize = ctx
                        .settings
                        .get_or("trendingcount", "10")
                        .parse()
                        .unwrap_or(10);
                    collect_wrapped(
                        fetch_trending(&ctx.http_client, client_id, "movies", count).await?,
                        &mut movies,
                    );
                    collect_wrapped(
                        fetch_trending(&ctx.http_client, client_id, "shows", count).await?,
                        &mut shows,
                    );
                }

                // Popular
                if ctx.settings.get_or("fetchpopular", "false").to_lowercase() == "true" {
                    let count: usize = ctx
                        .settings
                        .get_or("popularcount", "10")
                        .parse()
                        .unwrap_or(10);
                    collect_direct(
                        fetch_popular(&ctx.http_client, client_id, "movies", count).await?,
                        &mut movies,
                    );
                    collect_direct(
                        fetch_popular(&ctx.http_client, client_id, "shows", count).await?,
                        &mut shows,
                    );
                }

                // Most watched
                if ctx.settings.get_or("fetchwatched", "false").to_lowercase() == "true" {
                    let count: usize = ctx
                        .settings
                        .get_or("watchedcount", "10")
                        .parse()
                        .unwrap_or(10);
                    let period = ctx.settings.get_or("watchedperiod", "weekly");
                    collect_wrapped(
                        fetch_watched(&ctx.http_client, client_id, "movies", &period, count)
                            .await?,
                        &mut movies,
                    );
                    collect_wrapped(
                        fetch_watched(&ctx.http_client, client_id, "shows", &period, count)
                            .await?,
                        &mut shows,
                    );
                }

                tracing::info!(
                    movies = movies.len(),
                    shows = shows.len(),
                    "trakt content service completed"
                );

                Ok(HookResponse::ContentService(Box::new(ContentServiceResponse {
                    movies: movies.into_values().collect(),
                    shows: shows.into_values().collect(),
                })))
            }
            _ => Ok(HookResponse::Empty),
        }
    }
}

// ── Response types ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct TraktIds {
    imdb: Option<String>,
    tmdb: Option<i64>,
    tvdb: Option<i64>,
}

/// Wrapper shape: `{ movie: { ids: ... } }` or `{ show: { ids: ... } }`
#[derive(Deserialize)]
struct WrappedItem {
    movie: Option<TraktInner>,
    show: Option<TraktInner>,
}

#[derive(Deserialize)]
struct TraktInner {
    ids: TraktIds,
}

/// Direct shape: `{ ids: ... }` (popular endpoint)
#[derive(Deserialize)]
struct DirectItem {
    ids: TraktIds,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn ids_to_external(ids: &TraktIds) -> Option<ExternalIds> {
    if ids.imdb.is_none() && ids.tmdb.is_none() && ids.tvdb.is_none() {
        return None;
    }
    Some(ExternalIds {
        imdb_id: ids.imdb.clone(),
        tmdb_id: ids.tmdb.map(|n| n.to_string()),
        tvdb_id: ids.tvdb.map(|n| n.to_string()),
        ..Default::default()
    })
}

fn collect_wrapped(items: Vec<WrappedItem>, map: &mut HashMap<String, ExternalIds>) {
    for item in items {
        let inner = item.movie.or(item.show);
        if let Some(inner) = inner {
            if let Some(ext) = ids_to_external(&inner.ids) {
                let key = ext
                    .imdb_id
                    .clone()
                    .or_else(|| ext.tmdb_id.clone())
                    .unwrap_or_default();
                map.entry(key).or_insert(ext);
            }
        }
    }
}

fn collect_direct(items: Vec<DirectItem>, map: &mut HashMap<String, ExternalIds>) {
    for item in items {
        if let Some(ext) = ids_to_external(&item.ids) {
            let key = ext
                .imdb_id
                .clone()
                .or_else(|| ext.tmdb_id.clone())
                .unwrap_or_default();
            map.entry(key).or_insert(ext);
        }
    }
}

fn trakt_get(client: &reqwest::Client, url: &str, client_id: &str) -> reqwest::RequestBuilder {
    client
        .get(url)
        .header("trakt-api-key", client_id)
        .header("trakt-api-version", TRAKT_API_VERSION)
}

fn trakt_auth_get(
    client: &reqwest::Client,
    url: &str,
    client_id: &str,
    access_token: &str,
) -> reqwest::RequestBuilder {
    trakt_get(client, url, client_id)
        .header("Authorization", format!("Bearer {access_token}"))
}

// ── Fetch functions ───────────────────────────────────────────────────────────

async fn fetch_watchlist(
    client: &reqwest::Client,
    client_id: &str,
    access_token: &str,
    media_type: &str,
) -> anyhow::Result<Vec<WrappedItem>> {
    let url = format!("{TRAKT_BASE_URL}/sync/watchlist/{media_type}");
    let items: Vec<WrappedItem> = trakt_auth_get(client, &url, client_id, access_token)
        .send()
        .await?
        .json()
        .await?;
    Ok(items)
}

async fn fetch_user_list(
    client: &reqwest::Client,
    client_id: &str,
    access_token: &str,
    list_slug: &str,
    media_type: &str,
) -> anyhow::Result<Vec<WrappedItem>> {
    // list_slug format: "username/listname"
    let (username, listname) = list_slug.split_once('/').unwrap_or(("me", list_slug));
    let url =
        format!("{TRAKT_BASE_URL}/users/{username}/lists/{listname}/items/{media_type}");
    let items: Vec<WrappedItem> = trakt_auth_get(client, &url, client_id, access_token)
        .send()
        .await?
        .json()
        .await?;
    Ok(items)
}

async fn fetch_trending(
    client: &reqwest::Client,
    client_id: &str,
    media_type: &str,
    limit: usize,
) -> anyhow::Result<Vec<WrappedItem>> {
    let url = format!("{TRAKT_BASE_URL}/{media_type}/trending?limit={limit}");
    let items: Vec<WrappedItem> = trakt_get(client, &url, client_id)
        .send()
        .await?
        .json()
        .await?;
    Ok(items)
}

async fn fetch_popular(
    client: &reqwest::Client,
    client_id: &str,
    media_type: &str,
    limit: usize,
) -> anyhow::Result<Vec<DirectItem>> {
    let url = format!("{TRAKT_BASE_URL}/{media_type}/popular?limit={limit}");
    let items: Vec<DirectItem> = trakt_get(client, &url, client_id)
        .send()
        .await?
        .json()
        .await?;
    Ok(items)
}

async fn fetch_watched(
    client: &reqwest::Client,
    client_id: &str,
    media_type: &str,
    period: &str,
    limit: usize,
) -> anyhow::Result<Vec<WrappedItem>> {
    let url =
        format!("{TRAKT_BASE_URL}/{media_type}/watched/{period}?limit={limit}");
    let items: Vec<WrappedItem> = trakt_get(client, &url, client_id)
        .send()
        .await?
        .json()
        .await?;
    Ok(items)
}
