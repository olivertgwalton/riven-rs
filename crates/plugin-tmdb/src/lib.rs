use async_trait::async_trait;
use chrono::NaiveDate;
use serde::Deserialize;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::http::profiles;
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::*;

const TMDB_BASE_URL: &str = "https://api.themoviedb.org/3/";

#[derive(Default)]
pub struct TmdbPlugin;

register_plugin!(TmdbPlugin);

#[async_trait]
impl Plugin for TmdbPlugin {
    fn name(&self) -> &'static str {
        "tmdb"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemIndexRequested]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        _http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        Ok(settings.has("apikey"))
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![SettingField::new("apikey", "API Key", "password").required()]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let Some(request) = event.index_request() else {
            return Ok(HookResponse::Empty);
        };
        if request.item_type != MediaItemType::Movie {
            return Ok(HookResponse::Empty);
        }

        let api_key = ctx.require_setting("apikey")?;

        let indexed = if let Some(tmdb_id) = request.tmdb_id {
            fetch_movie_by_tmdb_id(&ctx.http, api_key, tmdb_id).await?
        } else if let Some(imdb_id) = request.imdb_id {
            let tmdb_id = find_tmdb_id(&ctx.http, api_key, imdb_id).await?;
            fetch_movie_by_tmdb_id(&ctx.http, api_key, &tmdb_id).await?
        } else {
            return Ok(HookResponse::Empty);
        };

        Ok(HookResponse::Index(Box::new(indexed)))
    }
}

async fn find_tmdb_id(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    imdb_id: &str,
) -> anyhow::Result<String> {
    let url = format!("{TMDB_BASE_URL}find/{imdb_id}?external_source=imdb_id");
    tracing::debug!(url = %url, imdb_id, "requesting tmdb id lookup");
    let resp: TmdbFindResponse = http
        .get_json(profiles::TMDB, url.clone(), |client| {
            client.get(&url).bearer_auth(api_key)
        })
        .await?;

    resp.movie_results
        .first()
        .map(|m| m.id.to_string())
        .ok_or_else(|| anyhow::anyhow!("no TMDB movie found for IMDB ID {imdb_id}"))
}

async fn fetch_movie_by_tmdb_id(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    tmdb_id: &str,
) -> anyhow::Result<IndexedMediaItem> {
    let url =
        format!("{TMDB_BASE_URL}movie/{tmdb_id}?append_to_response=external_ids,release_dates");
    tracing::debug!(url = %url, tmdb_id, "requesting tmdb movie details");
    let movie: TmdbMovieResponse = http
        .get_json(profiles::TMDB, url.clone(), |client| {
            client.get(&url).bearer_auth(api_key)
        })
        .await?;

    let year = movie
        .release_date
        .as_ref()
        .and_then(|d| d.split('-').next())
        .and_then(|y| y.parse().ok());

    let aired_at = movie
        .release_date
        .as_ref()
        .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());

    let genres = movie
        .genres
        .as_ref()
        .map(|g| g.iter().map(|genre| genre.name.clone()).collect());
    let is_anime = genres.as_ref().is_some_and(|genres: &Vec<String>| {
        genres.iter().any(|genre| {
            let genre = genre.to_ascii_lowercase();
            genre == "animation" || genre == "anime"
        })
    }) && movie
        .original_language
        .as_deref()
        .is_some_and(|language| !language.eq_ignore_ascii_case("en"));

    let imdb_id = movie.external_ids.as_ref().and_then(|e| e.imdb_id.clone());
    let content_rating = movie
        .release_dates
        .as_ref()
        .and_then(parse_content_rating_from_release_dates);

    Ok(IndexedMediaItem {
        title: Some(movie.title),
        tmdb_id: Some(tmdb_id.to_string()),
        imdb_id,
        poster_path: movie
            .poster_path
            .map(|p| format!("https://image.tmdb.org/t/p/w500{p}")),
        year,
        genres,
        country: movie
            .production_countries
            .as_ref()
            .and_then(|c| c.first())
            .map(|c| c.iso_3166_1.clone()),
        language: movie.original_language,
        content_rating,
        is_anime: Some(is_anime),
        runtime: movie.runtime,
        aired_at,
        ..Default::default()
    })
}

fn parse_content_rating_from_release_dates(
    release_dates: &TmdbReleaseDates,
) -> Option<ContentRating> {
    let parse_result = |result: &TmdbReleaseDateResult| {
        let certification = result.certification.trim();
        (!certification.is_empty())
            .then_some(certification)
            .and_then(parse_content_rating)
    };

    release_dates
        .results
        .iter()
        .find(|entry| entry.iso_3166_1.eq_ignore_ascii_case("US"))
        .and_then(|entry| entry.release_dates.iter().find_map(parse_result))
        .or_else(|| {
            release_dates
                .results
                .iter()
                .find_map(|entry| entry.release_dates.iter().find_map(parse_result))
        })
}

fn parse_content_rating(rating: &str) -> Option<ContentRating> {
    match rating.trim().to_ascii_uppercase().as_str() {
        "G" => Some(ContentRating::G),
        "PG" => Some(ContentRating::Pg),
        "PG-13" => Some(ContentRating::Pg13),
        "R" => Some(ContentRating::R),
        "NC-17" => Some(ContentRating::Nc17),
        "TV-Y" => Some(ContentRating::TvY),
        "TV-Y7" => Some(ContentRating::TvY7),
        "TV-G" => Some(ContentRating::TvG),
        "TV-PG" => Some(ContentRating::TvPg),
        "TV-14" => Some(ContentRating::Tv14),
        "TV-MA" => Some(ContentRating::TvMa),
        _ => None,
    }
}

// ── TMDB API response types ──

#[derive(Deserialize)]
struct TmdbFindResponse {
    movie_results: Vec<TmdbFindMovie>,
}

#[derive(Deserialize)]
struct TmdbFindMovie {
    id: i64,
}

#[derive(Deserialize)]
struct TmdbMovieResponse {
    title: String,
    poster_path: Option<String>,
    release_date: Option<String>,
    runtime: Option<i32>,
    original_language: Option<String>,
    genres: Option<Vec<TmdbGenre>>,
    production_countries: Option<Vec<TmdbCountry>>,
    external_ids: Option<TmdbExternalIds>,
    release_dates: Option<TmdbReleaseDates>,
}

#[derive(Deserialize)]
struct TmdbGenre {
    name: String,
}

#[derive(Deserialize)]
struct TmdbCountry {
    iso_3166_1: String,
}

#[derive(Deserialize)]
struct TmdbExternalIds {
    imdb_id: Option<String>,
}

#[derive(Deserialize)]
struct TmdbReleaseDates {
    results: Vec<TmdbReleaseDateCountry>,
}

#[derive(Deserialize)]
struct TmdbReleaseDateCountry {
    iso_3166_1: String,
    release_dates: Vec<TmdbReleaseDateResult>,
}

#[derive(Deserialize)]
struct TmdbReleaseDateResult {
    certification: String,
}
