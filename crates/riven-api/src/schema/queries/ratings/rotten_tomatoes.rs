use async_graphql::Context;
use riven_core::http::profiles::HttpServiceProfile;
use serde::Deserialize;

use super::RatingScore;
use super::util::{optional_http, score_item};

const RT_ALGOLIA_API_KEY: &str = "175588f6e5f8319b27702e4cc4013561";
const RT_ALGOLIA_APP_ID: &str = "79FRDP12PN";
const RT_ALGOLIA_URL: &str = "https://79frdp12pn-dsn.algolia.net/1/indexes/*/queries";

pub(super) const ROTTEN_TOMATOES: HttpServiceProfile = HttpServiceProfile::new("rotten_tomatoes");

pub(super) async fn scores(
    ctx: &Context<'_>,
    media_type: &str,
    title: Option<&str>,
    year: Option<i64>,
) -> Vec<RatingScore> {
    let Some(title) = title.filter(|title| !title.trim().is_empty()) else {
        return Vec::new();
    };
    let Some(http) = optional_http(ctx, "Rotten Tomatoes lookup") else {
        return Vec::new();
    };

    let body = serde_json::json!({
        "requests": [{
            "indexName": "content_rt",
            "query": search_query(title, media_type),
            "params": format!(
                "filters={}&hitsPerPage=20",
                encode_query_value(&format!("isEmsSearchable=1 AND type:\"{media_type}\""))
            ),
        }]
    });

    let response: RTAlgoliaSearchResponse = match http
        .get_json(
            ROTTEN_TOMATOES,
            format!("rt:{media_type}:{title}:{year:?}"),
            |client| {
                client
                    .post(RT_ALGOLIA_URL)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .header(
                        "x-algolia-agent",
                        "Algolia for JavaScript (4.14.3); Browser (lite)",
                    )
                    .header("x-algolia-api-key", RT_ALGOLIA_API_KEY)
                    .header("x-algolia-application-id", RT_ALGOLIA_APP_ID)
                    .json(&body)
            },
        )
        .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, media_type, title, "Rotten Tomatoes lookup failed");
            return Vec::new();
        }
    };

    response
        .content_hits()
        .and_then(|hits| best_match(hits, title, year))
        .and_then(|hit| hit.scores(media_type))
        .unwrap_or_default()
}

pub(super) fn encode_query_value(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            b' ' => vec!['%', '2', '0'],
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn search_query(title: &str, media_type: &str) -> String {
    if media_type == "movie" {
        title
            .replace(|c: char| c.is_ascii_punctuation(), " ")
            .split_whitespace()
            .filter(|word| !word.eq_ignore_ascii_case("the"))
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        title.to_owned()
    }
}

pub(super) fn best_match<'a>(
    hits: &'a [RTAlgoliaHit],
    title: &str,
    year: Option<i64>,
) -> Option<&'a RTAlgoliaHit> {
    hits.iter()
        .filter(|hit| hit.rotten_tomatoes.is_some())
        .find(|hit| title_matches(hit, title) && year_matches(hit, year))
}

fn title_matches(hit: &RTAlgoliaHit, title: &str) -> bool {
    let wanted = normalize_title(title);
    let mut candidates = vec![hit.title.as_str()];
    candidates.extend(hit.aka.iter().flatten().map(String::as_str));
    candidates.extend(hit.titles.iter().flatten().map(String::as_str));
    candidates
        .into_iter()
        .any(|candidate| normalize_title(candidate) == wanted)
}

fn year_matches(hit: &RTAlgoliaHit, year: Option<i64>) -> bool {
    year.is_none_or(|year| hit.release_year == 0 || (hit.release_year - year).abs() <= 1)
}

fn normalize_title(value: &str) -> String {
    let normalized = value
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    normalized
        .strip_prefix("the ")
        .unwrap_or(&normalized)
        .to_owned()
}

#[derive(Deserialize)]
struct RTAlgoliaSearchResponse {
    results: Vec<RTAlgoliaResult>,
}

impl RTAlgoliaSearchResponse {
    fn content_hits(&self) -> Option<&[RTAlgoliaHit]> {
        self.results
            .iter()
            .find(|result| result.index == "content_rt")
            .map(|result| result.hits.as_slice())
    }
}

#[derive(Deserialize)]
struct RTAlgoliaResult {
    hits: Vec<RTAlgoliaHit>,
    index: String,
}

#[derive(Deserialize)]
pub(super) struct RTAlgoliaHit {
    pub(super) title: String,
    pub(super) titles: Option<Vec<String>>,
    #[serde(default, rename = "releaseYear")]
    pub(super) release_year: i64,
    pub(super) vanity: String,
    pub(super) aka: Option<Vec<String>>,
    #[serde(rename = "rottenTomatoes")]
    pub(super) rotten_tomatoes: Option<RTRatings>,
}

impl RTAlgoliaHit {
    pub(super) fn scores(&self, media_type: &str) -> Option<Vec<RatingScore>> {
        let rt = self.rotten_tomatoes.as_ref()?;
        let url = format!(
            "https://www.rottentomatoes.com/{}/{}",
            if media_type == "movie" { "m" } else { "tv" },
            self.vanity
        );
        let mut scores = Vec::with_capacity(2);

        if rt.critics_score > 0 {
            let (name, image) = rt.critics_badge(media_type);
            scores.push(score_item(
                name,
                image,
                format!("{}%", rt.critics_score),
                url.clone(),
            ));
        }

        if rt.audience_score > 0 {
            let (name, image) = rt.audience_badge();
            scores.push(score_item(
                name,
                image,
                format!("{}%", rt.audience_score),
                url,
            ));
        }

        Some(scores)
    }
}

#[derive(Deserialize)]
pub(super) struct RTRatings {
    #[serde(default, rename = "audienceScore")]
    pub(super) audience_score: i64,
    #[serde(default, rename = "certifiedFresh")]
    pub(super) certified_fresh: bool,
    #[serde(default, rename = "criticsScore")]
    pub(super) critics_score: i64,
}

impl RTRatings {
    fn critics_badge(&self, media_type: &str) -> (&'static str, &'static str) {
        if media_type == "movie" && self.certified_fresh {
            ("rt_tomatometer_certified_fresh", "rt_certified_fresh.svg")
        } else if self.critics_score >= 60 {
            ("rt_tomatometer_fresh", "rt_fresh.svg")
        } else {
            ("rt_tomatometer_rotten", "rt_rotten.svg")
        }
    }

    fn audience_badge(&self) -> (&'static str, &'static str) {
        if self.audience_score >= 60 {
            ("rt_popcornmeter_fresh", "rt_aud_fresh.svg")
        } else {
            ("rt_popcornmeter_stale", "rt_aud_rotten.svg")
        }
    }
}
