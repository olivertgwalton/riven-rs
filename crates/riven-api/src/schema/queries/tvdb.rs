use async_graphql::{Context, Error, Object, Result};
use riven_core::http::HttpClient;
use riven_core::plugin::PluginRegistry;

use crate::profiles::{TMDB, TVDB};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::schema::metadata::details::{MediaDetails, PersonDetails, Source, TvdbPerson};
use crate::schema::metadata::{TMDB_API_BASE, get_tmdb_api_key, get_tvdb_api_key};
use crate::schema::queries::trakt;

const TVDB_API_BASE: &str = "https://api4.thetvdb.com/v4";
const TVDB_TOKEN_EXPIRY: Duration = Duration::from_secs(25 * 24 * 60 * 60);

static TVDB_TOKEN_CACHE: OnceLock<Mutex<Option<(String, Instant)>>> = OnceLock::new();

#[derive(Default)]
pub struct CoreTvdbQuery;

#[Object]
impl CoreTvdbQuery {
    /// Everything the show detail page renders, in one shape shared with
    /// `movieDetails`. `id` is a TVDB series id; `tmdbId` is only used to ask
    /// Trakt for related titles when the page was reached from a TMDB id.
    async fn show_details(
        &self,
        ctx: &Context<'_>,
        id: i64,
        tmdb_id: Option<String>,
    ) -> Result<MediaDetails> {
        let token = get_tvdb_token(ctx).await?;
        let page = HashMap::from([("page".to_string(), "0".to_string())]);

        // Ask TVDB for the English record rather than for every translation and
        // picking one here — `/translations/eng` and `/episodes/official/eng`
        // are already localised, so nothing downstream has to choose.
        let extended = format!("/series/{id}/extended");
        let translations = format!("/series/{id}/translations/eng");
        let english_episodes = format!("/series/{id}/episodes/official/eng");
        let (series, translation, episodes) = futures::join!(
            tvdb_get_value(ctx, &token, &extended, None),
            tvdb_get_value(ctx, &token, &translations, None),
            tvdb_get_value(ctx, &token, &english_episodes, Some(&page)),
        );

        let mut data = series?
            .get_mut("data")
            .map(serde_json::Value::take)
            .ok_or_else(|| Error::new("TVDB series response missing data"))?;

        // A failed lookup costs the English wording or the episode list, not
        // the page — the series record carries its own name and overview.
        if let Ok(translation) = translation {
            for field in ["name", "overview"] {
                if let Some(value) = translation.pointer(&format!("/data/{field}"))
                    && value.as_str().is_some_and(|text| !text.is_empty())
                {
                    data[field] = value.clone();
                }
            }
        }
        if let Ok(episodes) = episodes
            && let Some(episodes) = episodes.pointer("/data/episodes")
            && episodes.as_array().is_some_and(|list| !list.is_empty())
        {
            data["episodes"] = episodes.clone();
        }

        let mut details: MediaDetails = serde_json::from_value(data)
            .map_err(|e| Error::new(format!("unexpected TVDB series payload: {e}")))?;
        details.source = Source::Tvdb;

        let (trakt_id, id_type) = match tmdb_id {
            Some(tmdb_id) => (tmdb_id, "tmdb"),
            None => (id.to_string(), "tvdb"),
        };
        details.trakt = trakt::recommendations(ctx, &trakt_id, id_type, "show")
            .await
            .unwrap_or_default();
        Ok(details)
    }

    async fn resolve_tmdb_to_tvdb(
        &self,
        ctx: &Context<'_>,
        tmdb_id: String,
    ) -> Result<Option<i64>> {
        resolve_tmdb_to_tvdb_id(ctx, &tmdb_id).await
    }
}

#[derive(Default)]
struct TmdbExternalIds {
    tvdb_id: Option<i64>,
    imdb_id: Option<String>,
}

pub async fn resolve_tmdb_to_tvdb_id(ctx: &Context<'_>, tmdb_id: &str) -> Result<Option<i64>> {
    let externals = fetch_tmdb_external_ids(ctx, tmdb_id).await?;
    if let Some(tvdb_id) = externals.tvdb_id {
        return Ok(Some(tvdb_id));
    }

    let token = get_tvdb_token(ctx).await?;

    let remote_ids = externals
        .imdb_id
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(tmdb_id));
    for remote_id in remote_ids {
        let remote_lookup =
            tvdb_get_value(ctx, &token, &format!("/search/remoteid/{remote_id}"), None).await?;
        if let Some(series_id) = remote_lookup
            .get("data")
            .and_then(|value| value.as_array())
            .and_then(|items| {
                items.iter().find_map(|item| {
                    item.get("series")
                        .and_then(|series| series.get("id"))
                        .and_then(serde_json::Value::as_i64)
                })
            })
        {
            return Ok(Some(series_id));
        }
    }

    let direct_series = tvdb_get_value(ctx, &token, &format!("/series/{tmdb_id}"), None).await;
    match direct_series {
        Ok(value) => Ok(value
            .get("data")
            .and_then(|item| item.get("id"))
            .and_then(serde_json::Value::as_i64)),
        Err(_) => Ok(None),
    }
}

/// The reverse of [`resolve_tmdb_to_tvdb_id`]: TVDB's `/movies/{id}/extended`
/// and `/series/{id}/extended` both carry a `remoteIds` array (the same one
/// `MediaDetails::ids` reads for the show detail page) with a `TheMovieDB.com`
/// entry when TVDB knows the mapping.
pub async fn resolve_tvdb_to_tmdb_id(
    ctx: &Context<'_>,
    tvdb_id: &str,
    media_type: &str,
) -> Result<Option<String>> {
    let token = get_tvdb_token(ctx).await?;
    let kind = if media_type == "movie" { "movies" } else { "series" };
    let extended = tvdb_get_value(ctx, &token, &format!("/{kind}/{tvdb_id}/extended"), None).await?;

    Ok(extended
        .pointer("/data/remoteIds")
        .and_then(serde_json::Value::as_array)
        .and_then(|remotes| {
            remotes.iter().find_map(|remote| {
                let source = remote.get("sourceName").and_then(serde_json::Value::as_str)?;
                if source.eq_ignore_ascii_case("themoviedb.com") {
                    remote
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                } else {
                    None
                }
            })
        }))
}

async fn fetch_tmdb_external_ids(ctx: &Context<'_>, tmdb_id: &str) -> Result<TmdbExternalIds> {
    let registry = ctx.data::<Arc<PluginRegistry>>()?;
    let http = ctx.data::<HttpClient>()?;
    let api_key = get_tmdb_api_key(registry).await?;

    let value: serde_json::Value = match http
        .get_json(TMDB, format!("tmdb:external_ids:tv:{tmdb_id}"), |client| {
            client
                .get(format!("{TMDB_API_BASE}/3/tv/{tmdb_id}/external_ids"))
                .bearer_auth(&api_key)
        })
        .await
    {
        Ok(value) => value,
        Err(_) => return Ok(TmdbExternalIds::default()),
    };

    Ok(TmdbExternalIds {
        tvdb_id: value.get("tvdb_id").and_then(serde_json::Value::as_i64),
        imdb_id: value
            .get("imdb_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned),
    })
}

async fn get_tvdb_token(ctx: &Context<'_>) -> Result<String> {
    let cache = TVDB_TOKEN_CACHE.get_or_init(|| Mutex::new(None));
    if let Some((token, created_at)) = cache.lock().expect("tvdb token cache poisoned").clone()
        && created_at.elapsed() < TVDB_TOKEN_EXPIRY
    {
        return Ok(token);
    }

    let registry = ctx.data::<Arc<PluginRegistry>>()?;
    let http = ctx.data::<HttpClient>()?;
    let api_key = get_tvdb_api_key(registry).await?;

    let value: serde_json::Value = http
        .get_json(TVDB, "tvdb:login".to_string(), |client| {
            client
                .post(format!("{TVDB_API_BASE}/login"))
                .json(&serde_json::json!({ "apikey": api_key }))
        })
        .await
        .map_err(|e| Error::new(format!("TVDB login request failed: {e}")))?;

    let token = value
        .get("data")
        .and_then(|data| data.get("token"))
        .and_then(|token| token.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::new("TVDB login response missing token"))?;

    *cache.lock().expect("tvdb token cache poisoned") = Some((token.clone(), Instant::now()));
    Ok(token)
}

/// A TVDB person, for the shared `personDetails` resolver in `tmdb.rs`.
pub(super) async fn person_details(ctx: &Context<'_>, id: i64) -> Result<PersonDetails> {
    let token = get_tvdb_token(ctx).await?;
    let short = HashMap::from([("short".to_string(), "false".to_string())]);

    let extended = format!("/people/{id}/extended");
    let translations = format!("/people/{id}/translations/eng");
    let (person, translation) = futures::join!(
        tvdb_get_value(ctx, &token, &extended, Some(&short)),
        tvdb_get_value(ctx, &token, &translations, None),
    );

    let mut data = person?
        .get_mut("data")
        .map(serde_json::Value::take)
        .ok_or_else(|| Error::new("TVDB person response missing data"))?;

    // As with a series, TVDB is asked for the English record rather than for
    // every translation.
    if let Ok(translation) = translation
        && let Some(overview) = translation.pointer("/data/overview")
        && overview.as_str().is_some_and(|text| !text.is_empty())
    {
        data["biography"] = overview.clone();
    }

    Ok(TvdbPerson(data).into())
}

async fn tvdb_get_value(
    ctx: &Context<'_>,
    token: &str,
    path: &str,
    query: Option<&HashMap<String, String>>,
) -> Result<serde_json::Value> {
    let http = ctx.data::<HttpClient>()?;
    let dedupe_key = format!("tvdb:{path}:{query:?}");

    http.get_json(TVDB, dedupe_key, |client| {
        let mut request = client
            .get(format!("{TVDB_API_BASE}{path}"))
            .bearer_auth(token);

        if let Some(query) = query {
            request = request.query(query);
        }

        request
    })
    .await
    .map_err(|e| Error::new(format!("TVDB request failed: {e}")))
}
