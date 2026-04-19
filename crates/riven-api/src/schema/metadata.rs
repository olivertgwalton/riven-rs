use async_graphql::{Error, Result, SimpleObject};
use riven_core::plugin::PluginRegistry;

pub const TMDB_API_BASE: &str = "https://api.themoviedb.org";
pub const TMDB_IMAGE_BASE: &str = "https://image.tmdb.org/t/p";

// ── Output types ──────────────────────────────────────────────────────────────

#[derive(SimpleObject)]
pub struct TmdbListItem {
    pub id: i64,
    pub title: String,
    pub poster_path: Option<String>,
    pub media_type: String,
    pub year: String,
    pub vote_average: Option<f64>,
    pub vote_count: Option<i64>,
    pub popularity: Option<f64>,
    pub overview: Option<String>,
    pub backdrop_path: Option<String>,
    pub genre_ids: Vec<i64>,
    pub release_date: Option<String>,
    pub first_air_date: Option<String>,
    pub original_title: Option<String>,
    pub original_language: Option<String>,
    pub indexer: String,
}

#[derive(SimpleObject)]
pub struct TmdbPage {
    pub results: Vec<TmdbListItem>,
    pub page: i64,
    pub total_pages: i64,
    pub total_results: i64,
}

#[derive(SimpleObject)]
pub struct TmdbLogoAndCert {
    pub logo: Option<String>,
    pub certification: Option<String>,
}

#[derive(SimpleObject)]
pub struct TmdbCollectionPart {
    pub id: i64,
    pub title: String,
    pub overview: Option<String>,
    pub poster_path: Option<String>,
    pub backdrop_path: Option<String>,
    pub release_date: Option<String>,
    pub media_type: String,
    pub year: String,
}

#[derive(SimpleObject)]
pub struct TmdbCollectionDetails {
    pub id: i64,
    pub name: String,
    pub overview: Option<String>,
    pub poster_path: Option<String>,
    pub backdrop_path: Option<String>,
    pub parts: Vec<TmdbCollectionPart>,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

pub fn transform_item(item: &serde_json::Value, default_type: &str) -> TmdbListItem {
    let media_type = item
        .get("media_type")
        .and_then(|v| v.as_str())
        .unwrap_or(default_type)
        .to_owned();
    let title = item
        .get("title")
        .or_else(|| item.get("name"))
        .or_else(|| item.get("original_title"))
        .or_else(|| item.get("original_name"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();

    let release_date = item
        .get("release_date")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let first_air_date = item
        .get("first_air_date")
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    let year = if media_type == "movie" {
        release_date.as_deref()
    } else {
        first_air_date.as_deref()
    }
    .and_then(|d| d.split('-').next())
    .unwrap_or("N/A")
    .to_owned();

    let poster_path = item
        .get("poster_path")
        .or_else(|| item.get("profile_path"))
        .or_else(|| item.get("logo_path"))
        .and_then(|v| v.as_str())
        .map(|p| format!("{TMDB_IMAGE_BASE}/w500{p}"));

    let backdrop_path = item
        .get("backdrop_path")
        .and_then(|v| v.as_str())
        .map(|p| format!("{TMDB_IMAGE_BASE}/w1280{p}"));

    let genre_ids = item
        .get("genre_ids")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_i64()).collect())
        .unwrap_or_default();

    TmdbListItem {
        id: item.get("id").and_then(|v| v.as_i64()).unwrap_or(0),
        title,
        poster_path,
        media_type,
        year,
        vote_average: item.get("vote_average").and_then(|v| v.as_f64()),
        vote_count: item.get("vote_count").and_then(|v| v.as_i64()),
        popularity: item.get("popularity").and_then(|v| v.as_f64()),
        overview: item
            .get("overview")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        backdrop_path,
        genre_ids,
        release_date,
        first_air_date,
        original_title: item
            .get("original_title")
            .or_else(|| item.get("original_name"))
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        original_language: item
            .get("original_language")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        indexer: "tmdb".to_owned(),
    }
}

pub fn transform_collection(data: &serde_json::Value) -> TmdbCollectionDetails {
    let mut parts = data
        .get("parts")
        .and_then(|v| v.as_array())
        .map(|parts| {
            parts
                .iter()
                .map(|movie| {
                    let release_date = movie
                        .get("release_date")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned);
                    let title = movie
                        .get("title")
                        .or_else(|| movie.get("name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_owned();
                    TmdbCollectionPart {
                        id: movie.get("id").and_then(|v| v.as_i64()).unwrap_or_default(),
                        title,
                        overview: movie
                            .get("overview")
                            .and_then(|v| v.as_str())
                            .map(str::to_owned),
                        poster_path: image_path(
                            movie.get("poster_path").and_then(|v| v.as_str()),
                            "w500",
                        ),
                        backdrop_path: image_path(
                            movie.get("backdrop_path").and_then(|v| v.as_str()),
                            "w1920",
                        ),
                        year: release_date
                            .as_deref()
                            .and_then(|date| date.split('-').next())
                            .filter(|year| !year.is_empty())
                            .unwrap_or("N/A")
                            .to_owned(),
                        release_date,
                        media_type: "movie".to_owned(),
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    parts.sort_by(|a, b| a.release_date.cmp(&b.release_date));

    TmdbCollectionDetails {
        id: data.get("id").and_then(|v| v.as_i64()).unwrap_or_default(),
        name: data
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned(),
        overview: data
            .get("overview")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        poster_path: image_path(data.get("poster_path").and_then(|v| v.as_str()), "w500"),
        backdrop_path: image_path(data.get("backdrop_path").and_then(|v| v.as_str()), "w1920"),
        parts,
    }
}

fn image_path(path: Option<&str>, size: &str) -> Option<String> {
    path.map(|path| {
        if path.starts_with("http") {
            path.to_owned()
        } else {
            format!("{TMDB_IMAGE_BASE}/{size}{path}")
        }
    })
}

pub async fn get_tmdb_api_key(registry: &PluginRegistry) -> Result<String> {
    let settings = registry
        .get_plugin_settings_json("tmdb")
        .await
        .ok_or_else(|| Error::new("TMDB plugin is not configured"))?;
    settings
        .get("apikey")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::new("TMDB API key is not configured"))
}

pub async fn get_tvdb_api_key(registry: &PluginRegistry) -> Result<String> {
    let settings = registry
        .get_plugin_settings_json("tvdb")
        .await
        .ok_or_else(|| Error::new("TVDB plugin is not configured"))?;
    settings
        .get("apikey")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::new("TVDB API key is not configured"))
}
