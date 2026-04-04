use async_trait::async_trait;
use serde::Deserialize;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{ContentCollection, Plugin, PluginContext};
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
            SettingField::new("fetchwatched", "Fetch Watched History", "boolean")
                .with_default("false"),
            SettingField::new("watchedcount", "Watched Count", "number").with_default("10"),
            SettingField::new("watchedperiod", "Watched Period", "text")
                .with_default("weekly")
                .with_placeholder("weekly")
                .with_description(
                    "Period for watched history (daily, weekly, monthly, yearly, all).",
                ),
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
                let mut content = ContentCollection::default();
                let trending_count = ctx.settings.get_parsed_or("trendingcount", 10usize);
                let popular_count = ctx.settings.get_parsed_or("popularcount", 10usize);
                let watched_count = ctx.settings.get_parsed_or("watchedcount", 10usize);
                let watched_period = ctx.settings.get_or("watchedperiod", "weekly");

                // Watchlist (requires access token)
                if ctx.settings.get_bool("watchlist") {
                    if let Some(token) = access_token {
                        collect_wrapped(
                            fetch_watchlist(&ctx.http_client, client_id, token, "movies").await?,
                            &mut content,
                            true,
                        );
                        collect_wrapped(
                            fetch_watchlist(&ctx.http_client, client_id, token, "shows").await?,
                            &mut content,
                            false,
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
                            &mut content,
                            true,
                        );
                        collect_wrapped(
                            fetch_user_list(&ctx.http_client, client_id, token, list_slug, "shows")
                                .await?,
                            &mut content,
                            false,
                        );
                    }
                }

                // Trending
                if ctx.settings.get_bool("fetchtrending") {
                    collect_wrapped(
                        fetch_trending(&ctx.http_client, client_id, "movies", trending_count)
                            .await?,
                        &mut content,
                        true,
                    );
                    collect_wrapped(
                        fetch_trending(&ctx.http_client, client_id, "shows", trending_count)
                            .await?,
                        &mut content,
                        false,
                    );
                }

                // Popular
                if ctx.settings.get_bool("fetchpopular") {
                    collect_direct(
                        fetch_popular(&ctx.http_client, client_id, "movies", popular_count).await?,
                        &mut content,
                        true,
                    );
                    collect_direct(
                        fetch_popular(&ctx.http_client, client_id, "shows", popular_count).await?,
                        &mut content,
                        false,
                    );
                }

                // Most watched
                if ctx.settings.get_bool("fetchwatched") {
                    collect_wrapped(
                        fetch_watched(
                            &ctx.http_client,
                            client_id,
                            "movies",
                            &watched_period,
                            watched_count,
                        )
                        .await?,
                        &mut content,
                        true,
                    );
                    collect_wrapped(
                        fetch_watched(
                            &ctx.http_client,
                            client_id,
                            "shows",
                            &watched_period,
                            watched_count,
                        )
                        .await?,
                        &mut content,
                        false,
                    );
                }

                tracing::info!(
                    movies = content.movie_count(),
                    shows = content.show_count(),
                    "trakt content service completed"
                );

                Ok(content.into_hook_response())
            }
            _ => Ok(HookResponse::Empty),
        }
    }
}

#[derive(Deserialize)]
struct TraktIds {
    imdb: Option<String>,
    tmdb: Option<i64>,
    tvdb: Option<i64>,
}

#[derive(Deserialize)]
struct WrappedItem {
    movie: Option<TraktInner>,
    show: Option<TraktInner>,
}

#[derive(Deserialize)]
struct TraktInner {
    ids: TraktIds,
}

#[derive(Deserialize)]
struct DirectItem {
    ids: TraktIds,
}

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

fn collect_wrapped(items: Vec<WrappedItem>, content: &mut ContentCollection, is_movie: bool) {
    for item in items {
        let inner = item.movie.or(item.show);
        if let Some(inner) = inner {
            if let Some(ext) = ids_to_external(&inner.ids) {
                insert_external_ids(content, ext, is_movie);
            }
        }
    }
}

fn collect_direct(items: Vec<DirectItem>, content: &mut ContentCollection, is_movie: bool) {
    for item in items {
        if let Some(ext) = ids_to_external(&item.ids) {
            insert_external_ids(content, ext, is_movie);
        }
    }
}

fn insert_external_ids(content: &mut ContentCollection, ext: ExternalIds, is_movie: bool) {
    if is_movie {
        content.insert_movie(ext);
    } else {
        content.insert_show(ext);
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
    trakt_get(client, url, client_id).header("Authorization", format!("Bearer {access_token}"))
}

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
    let url = format!("{TRAKT_BASE_URL}/users/{username}/lists/{listname}/items/{media_type}");
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
    let url = format!("{TRAKT_BASE_URL}/{media_type}/watched/{period}?limit={limit}");
    let items: Vec<WrappedItem> = trakt_get(client, &url, client_id)
        .send()
        .await?
        .json()
        .await?;
    Ok(items)
}
