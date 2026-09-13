use async_trait::async_trait;
use serde::Deserialize;

use riven_core::events::{EventType, HookResponse};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{ContentCollection, Plugin, PluginContext};
use riven_core::settings::PluginSettings;
use riven_core::types::*;

const TRAKT_BASE_URL: &str = "https://api.trakt.tv";
const TRAKT_API_VERSION: &str = "2";

pub const PROFILE: HttpServiceProfile = HttpServiceProfile::new("trakt");

#[derive(Default)]
pub struct TraktPlugin;

#[async_trait]
impl Plugin for TraktPlugin {
    fn name(&self) -> &'static str {
        "trakt"
    }

    fn category(&self) -> &'static str {
        "services"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::ContentServiceRequested]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        _http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(settings.has("clientid"))
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::{FieldType, SettingField};
        vec![
            SettingField::new("clientid", "Client ID", FieldType::Password).required(),
            SettingField::new("accesstoken", "Access Token", FieldType::Password),
            SettingField::new("watchlist", "Enable Watchlist", FieldType::Boolean)
                .with_default("false"),
            SettingField::new("userlists", "User Lists", FieldType::Text)
                .with_placeholder("list-slug, another-list")
                .with_description("Comma-separated Trakt list slugs."),
            SettingField::new("fetchtrending", "Fetch Trending", FieldType::Boolean)
                .with_default("false"),
            SettingField::new("trendingcount", "Trending Count", FieldType::Number)
                .with_default("10"),
            SettingField::new("fetchpopular", "Fetch Popular", FieldType::Boolean)
                .with_default("false"),
            SettingField::new("popularcount", "Popular Count", FieldType::Number)
                .with_default("10"),
            SettingField::new("fetchwatched", "Fetch Watched History", FieldType::Boolean)
                .with_default("false"),
            SettingField::new("watchedcount", "Watched Count", FieldType::Number)
                .with_default("10"),
            SettingField::new("watchedperiod", "Watched Period", FieldType::Text)
                .with_default("weekly")
                .with_placeholder("weekly")
                .with_description(
                    "Period for watched history (daily, weekly, monthly, yearly, all).",
                ),
        ]
    }

    async fn on_content_service_requested(
        &self,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let client_id = ctx.require_setting("clientid")?;
        let access_token = ctx.settings.get("accesstoken");
        let trending_count = ctx.settings.get_parsed_or("trendingcount", 10usize);
        let popular_count = ctx.settings.get_parsed_or("popularcount", 10usize);
        let watched_count = ctx.settings.get_parsed_or("watchedcount", 10usize);
        let watched_period = ctx.settings.get_or("watchedperiod", "weekly");

        // (path, send the access token, is movie), in request order.
        let mut requests: Vec<(String, bool, bool)> = Vec::new();
        let mut add = |path: &dyn Fn(&str) -> String, authed: bool| {
            for (media, is_movie) in [("movies", true), ("shows", false)] {
                requests.push((path(media), authed, is_movie));
            }
        };

        if ctx.settings.get_bool("watchlist") {
            if access_token.is_some() {
                add(&|media: &str| format!("sync/watchlist/{media}"), true);
            } else {
                tracing::warn!("trakt watchlist enabled but accesstoken not set");
            }
        }
        if access_token.is_some() {
            for list_slug in ctx.settings.get_list("userlists") {
                let (username, listname) = list_slug.split_once('/').unwrap_or(("me", &list_slug));
                add(
                    &|media: &str| format!("users/{username}/lists/{listname}/items/{media}"),
                    true,
                );
            }
        }
        if ctx.settings.get_bool("fetchtrending") {
            add(
                &|media: &str| format!("{media}/trending?limit={trending_count}"),
                false,
            );
        }
        if ctx.settings.get_bool("fetchpopular") {
            add(
                &|media: &str| format!("{media}/popular?limit={popular_count}"),
                false,
            );
        }
        if ctx.settings.get_bool("fetchwatched") {
            add(
                &|media: &str| format!("{media}/watched/{watched_period}?limit={watched_count}"),
                false,
            );
        }

        let mut content = ContentCollection::default();
        for (path, authed, is_movie) in requests {
            let token = access_token.filter(|_| authed);
            collect(
                trakt_get(&ctx.http, client_id, token, &path).await?,
                &mut content,
                is_movie,
            );
        }

        tracing::info!(
            movies = content.movie_count(),
            shows = content.show_count(),
            "trakt content service completed"
        );

        Ok(content.into_hook_response())
    }
}

#[derive(Deserialize)]
struct TraktIds {
    imdb: Option<String>,
    tmdb: Option<i64>,
    tvdb: Option<i64>,
}

/// A Trakt list entry: the media object itself (`ids`, from `popular`) or a
/// wrapper holding it under `movie`/`show` (every other endpoint).
#[derive(Deserialize)]
struct TraktItem {
    ids: Option<TraktIds>,
    movie: Option<Box<TraktItem>>,
    show: Option<Box<TraktItem>>,
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

fn collect(items: Vec<TraktItem>, content: &mut ContentCollection, is_movie: bool) {
    for item in items {
        let ids = item
            .ids
            .or_else(|| item.movie.or(item.show).and_then(|inner| inner.ids));
        let Some(ext) = ids.as_ref().and_then(ids_to_external) else {
            continue;
        };
        if is_movie {
            content.insert_movie(ext);
        } else {
            content.insert_show(ext);
        }
    }
}

async fn trakt_get(
    http: &riven_core::http::HttpClient,
    client_id: &str,
    access_token: Option<&str>,
    path: &str,
) -> anyhow::Result<Vec<TraktItem>> {
    let url = format!("{TRAKT_BASE_URL}/{path}");
    tracing::debug!(url = %url, "requesting trakt");
    http.get_json(PROFILE, url.clone(), |client| {
        let request = client
            .get(&url)
            .header("trakt-api-key", client_id)
            .header("trakt-api-version", TRAKT_API_VERSION);
        match access_token {
            Some(token) => request.header("Authorization", format!("Bearer {token}")),
            None => request,
        }
    })
    .await
}

#[cfg(test)]
mod tests;
