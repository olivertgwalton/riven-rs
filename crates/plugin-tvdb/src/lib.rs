use async_trait::async_trait;
use chrono::NaiveDate;
use parking_lot::Mutex;
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::types::*;

const TVDB_BASE_URL: &str = "https://api4.thetvdb.com/v4/";
const DEFAULT_API_KEY: &str = "6be85335-5c4f-4d8d-b945-d3ed0eb8cdce";
const TOKEN_EXPIRY: Duration = Duration::from_secs(25 * 24 * 3600);

#[derive(Default)]
pub struct TvdbPlugin {
    token: Mutex<Option<(String, Instant)>>,
}

impl TvdbPlugin {
    async fn get_token(&self, client: &reqwest::Client, api_key: &str) -> anyhow::Result<String> {
        {
            let guard = self.token.lock();
            if let Some((ref token, ref created)) = *guard {
                if created.elapsed() < TOKEN_EXPIRY {
                    return Ok(token.clone());
                }
            }
        }

        let resp: TvdbResponse<TvdbLoginData> = client
            .post(format!("{TVDB_BASE_URL}login"))
            .json(&serde_json::json!({ "apikey": api_key }))
            .send()
            .await?
            .json()
            .await?;

        let token = resp.data.token;
        *self.token.lock() = Some((token.clone(), Instant::now()));
        Ok(token)
    }
}

register_plugin!(TvdbPlugin);

#[async_trait]
impl Plugin for TvdbPlugin {
    fn name(&self) -> &'static str {
        "tvdb"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemIndexRequested]
    }

    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![SettingField::new("apikey", "API Key", "password")]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let Some(request) = event.index_request() else {
            return Ok(HookResponse::Empty);
        };
        if request.item_type != MediaItemType::Show {
            return Ok(HookResponse::Empty);
        }
        let Some(tvdb_id) = request.tvdb_id else {
            return Ok(HookResponse::Empty);
        };

        let api_key = ctx.settings.get_or("apikey", DEFAULT_API_KEY);
        let token = self.get_token(&ctx.http_client, &api_key).await?;

        let indexed = fetch_series(&ctx.http_client, &token, tvdb_id).await?;
        Ok(HookResponse::Index(Box::new(indexed)))
    }
}

async fn fetch_series(
    client: &reqwest::Client,
    token: &str,
    tvdb_id: &str,
) -> anyhow::Result<IndexedMediaItem> {
    let url = format!("{TVDB_BASE_URL}series/{tvdb_id}/extended?short=true&meta=translations");
    let resp: TvdbResponse<TvdbSeries> = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await?
        .json()
        .await?;
    let series = resp.data;

    let episodes = fetch_all_episodes(client, token, tvdb_id).await?;

    let title =
        extract_english_name(&series).unwrap_or_else(|| series.name.clone().unwrap_or_default());

    let imdb_id = series.remote_ids.as_ref().and_then(|ids| {
        ids.iter()
            .find(|r| r.source_name.as_deref() == Some("IMDB"))
            .and_then(|r| r.id.clone())
    });

    let genres = series
        .genres
        .as_ref()
        .map(|g| g.iter().filter_map(|n| n.name.clone()).collect());
    let genres_lower = genres
        .as_ref()
        .map(|genres: &Vec<String>| {
            genres
                .iter()
                .map(|genre| genre.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let is_anime = genres_lower.iter().any(|genre| genre == "anime")
        || (genres_lower.iter().any(|genre| genre == "animation")
            && series
                .original_language
                .as_deref()
                .is_some_and(|language| !language.eq_ignore_ascii_case("eng")));

    let country = series
        .original_country
        .clone()
        .or_else(|| series.country.clone());

    let status = match series.status.as_ref().and_then(|s| s.name.as_deref()) {
        Some("Continuing") => Some(ShowStatus::Continuing),
        _ => Some(ShowStatus::Ended),
    };

    let content_rating = series.content_ratings.as_ref().and_then(|ratings| {
        ratings
            .iter()
            .find(|r| r.country.as_deref() == Some("usa"))
            .and_then(|r| parse_content_rating(r.name.as_deref().unwrap_or("")))
    });

    let mut season_map: HashMap<i32, Vec<IndexedEpisode>> = HashMap::new();
    for ep in &episodes {
        let season_num = ep.season_number.unwrap_or(0);
        let ep_num = ep.number.unwrap_or(0);

        let aired = ep
            .aired
            .as_ref()
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());

        let indexed_ep = IndexedEpisode {
            number: ep_num,
            absolute_number: ep.absolute_number,
            title: ep.name.clone(),
            tvdb_id: ep.id.map(|id| id.to_string()),
            aired_at: aired,
            runtime: ep.runtime,
            poster_path: ep.image.clone(),
            content_rating: None,
        };

        season_map.entry(season_num).or_default().push(indexed_ep);
    }

    let seasons: Vec<IndexedSeason> = {
        let mut seasons: Vec<_> = season_map
            .into_iter()
            .map(|(num, mut eps)| {
                eps.sort_by_key(|e| e.number);
                IndexedSeason {
                    number: num,
                    title: None,
                    tvdb_id: None,
                    episodes: eps,
                }
            })
            .collect();
        seasons.sort_by_key(|s| s.number);
        seasons
    };

    let aliases = series.aliases.as_ref().map(|aliases| {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for alias in aliases {
            if let (Some(ref lang), Some(ref name)) = (&alias.language, &alias.name) {
                map.entry(lang.clone()).or_default().push(name.clone());
            }
        }
        map
    });

    let year = series
        .year
        .as_ref()
        .and_then(|y| y.parse().ok())
        .or_else(|| {
            series
                .first_aired
                .as_ref()
                .and_then(|d| d.split('-').next())
                .and_then(|y| y.parse().ok())
        });

    let aired_at = series
        .first_aired
        .as_ref()
        .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());

    Ok(IndexedMediaItem {
        title: Some(title),
        tvdb_id: Some(tvdb_id.to_string()),
        imdb_id,
        poster_path: series.image,
        year,
        genres,
        country,
        language: series.original_language.clone(),
        network: series
            .original_network
            .as_ref()
            .and_then(|n| n.name.clone()),
        content_rating,
        is_anime: Some(is_anime),
        status,
        aliases,
        aired_at,
        seasons: Some(seasons),
        ..Default::default()
    })
}

async fn fetch_all_episodes(
    client: &reqwest::Client,
    token: &str,
    tvdb_id: &str,
) -> anyhow::Result<Vec<TvdbEpisode>> {
    let mut all_episodes = Vec::new();
    let mut page = 0;

    loop {
        let url = format!("{TVDB_BASE_URL}series/{tvdb_id}/episodes/official/eng?page={page}");
        let resp: TvdbResponse<TvdbEpisodePage> = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await?
            .json()
            .await?;

        all_episodes.extend(resp.data.episodes);

        match resp.links.and_then(|l| l.next) {
            Some(_) => page += 1,
            None => break,
        }
    }

    Ok(all_episodes)
}

fn extract_english_name(series: &TvdbSeries) -> Option<String> {
    series
        .translations
        .as_ref()
        .and_then(|t| t.name_translations.as_ref())
        .and_then(|nt| nt.iter().find(|t| t.language == "eng"))
        .map(|t| t.name.clone())
}

fn parse_content_rating(rating: &str) -> Option<ContentRating> {
    match rating {
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

#[derive(Deserialize)]
struct TvdbResponse<T> {
    data: T,
    #[serde(default)]
    links: Option<TvdbLinks>,
}

#[derive(Deserialize)]
struct TvdbLinks {
    next: Option<String>,
}

#[derive(Deserialize)]
struct TvdbLoginData {
    token: String,
}

#[derive(Deserialize)]
struct TvdbSeries {
    name: Option<String>,
    image: Option<String>,
    year: Option<String>,
    #[serde(rename = "firstAired")]
    first_aired: Option<String>,
    #[serde(rename = "originalLanguage")]
    original_language: Option<String>,
    #[serde(rename = "originalCountry")]
    original_country: Option<String>,
    country: Option<String>,
    #[serde(rename = "originalNetwork")]
    original_network: Option<Named>,
    genres: Option<Vec<Named>>,
    status: Option<Named>,
    aliases: Option<Vec<TvdbAlias>>,
    #[serde(rename = "remoteIds")]
    remote_ids: Option<Vec<TvdbRemoteId>>,
    #[serde(rename = "contentRatings")]
    content_ratings: Option<Vec<TvdbContentRating>>,
    translations: Option<TvdbTranslations>,
}

#[derive(Deserialize)]
struct TvdbTranslations {
    #[serde(rename = "nameTranslations")]
    name_translations: Option<Vec<TvdbTranslation>>,
}

#[derive(Deserialize)]
struct TvdbTranslation {
    language: String,
    name: String,
}

#[derive(Deserialize)]
struct Named {
    name: Option<String>,
}

#[derive(Deserialize)]
struct TvdbAlias {
    language: Option<String>,
    name: Option<String>,
}

#[derive(Deserialize)]
struct TvdbRemoteId {
    #[serde(rename = "sourceName")]
    source_name: Option<String>,
    id: Option<String>,
}

#[derive(Deserialize)]
struct TvdbContentRating {
    country: Option<String>,
    name: Option<String>,
}

/// TVDB sometimes omits or nulls `episodes` for series with no official episodes.
#[derive(Deserialize)]
struct TvdbEpisodePage {
    #[serde(default)]
    episodes: Vec<TvdbEpisode>,
}

#[derive(Deserialize)]
struct TvdbEpisode {
    id: Option<i64>,
    name: Option<String>,
    number: Option<i32>,
    #[serde(rename = "seasonNumber")]
    season_number: Option<i32>,
    #[serde(rename = "absoluteNumber")]
    absolute_number: Option<i32>,
    aired: Option<String>,
    /// TVDB occasionally returns runtime as a float (e.g. 22.5); deserialize
    /// leniently and truncate to i32.
    #[serde(default, deserialize_with = "deserialize_runtime")]
    runtime: Option<i32>,
    image: Option<String>,
}

fn deserialize_runtime<'de, D>(d: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Option<serde_json::Value> = Option::deserialize(d)?;
    Ok(v.and_then(|v| match v {
        serde_json::Value::Number(n) => n.as_f64().map(|f| f as i32),
        _ => None,
    }))
}
