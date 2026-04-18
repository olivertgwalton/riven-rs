use async_graphql::{Context, Error, Object, Result, SimpleObject};
use riven_core::http::HttpClient;
use riven_core::http::profiles::{ANILIST, ANIZIP};
use serde::Deserialize;

const ANILIST_GRAPHQL_URL: &str = "https://graphql.anilist.co";
const ANIZIP_MAPPINGS_URL: &str = "https://api.ani.zip/v1/mappings";

#[derive(Default)]
pub struct CoreAnilistQuery;

#[derive(SimpleObject)]
pub struct AnilistListItem {
    pub id: i64,
    pub title: String,
    pub poster_path: Option<String>,
    pub media_type: String,
    pub year: String,
}

#[derive(SimpleObject)]
pub struct AnilistPage {
    pub results: Vec<AnilistListItem>,
    pub page: i64,
}

#[derive(SimpleObject)]
pub struct AnilistRating {
    pub id: i64,
    pub score: Option<f64>,
}

#[derive(SimpleObject)]
pub struct AnilistMappings {
    pub anilist_id: i64,
    pub tmdb_id: Option<i64>,
    pub tvdb_id: Option<i64>,
}

#[Object]
impl CoreAnilistQuery {
    async fn trending_anilist(
        &self,
        ctx: &Context<'_>,
        page: Option<i32>,
        per_page: Option<i32>,
    ) -> Result<AnilistPage> {
        let page = page.unwrap_or(1);
        let per_page = per_page.unwrap_or(20);

        let payload = serde_json::json!({
            "query": r#"
                query ($page: Int!, $perPage: Int!) {
                  Page(page: $page, perPage: $perPage) {
                    media(type: ANIME, sort: TRENDING_DESC) {
                      id
                      title {
                        romaji
                        english
                        native
                      }
                      coverImage {
                        large
                      }
                      seasonYear
                      format
                    }
                  }
                }
            "#,
            "variables": {
                "page": page,
                "perPage": per_page
            }
        });

        let response: AniListGraphqlResponse<AniListTrendingData> =
            anilist_post(ctx, &payload, "trending").await?;

        if let Some(errors) = response.errors {
            return Err(Error::new(join_graphql_errors(&errors)));
        }

        let results = response
            .data
            .map(|data| {
                data.page
                    .media
                    .into_iter()
                    .map(|item| AnilistListItem {
                        id: item.id,
                        title: item
                            .title
                            .english
                            .or(item.title.romaji)
                            .or(item.title.native)
                            .unwrap_or_default(),
                        poster_path: item.cover_image.and_then(|image| image.large),
                        media_type: item.format.unwrap_or_else(|| "ANIME".to_string()),
                        year: item
                            .season_year
                            .map(|year| year.to_string())
                            .unwrap_or_else(|| "N/A".to_string()),
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(AnilistPage {
            results,
            page: i64::from(page),
        })
    }

    async fn anilist_rating(&self, ctx: &Context<'_>, id: i32) -> Result<AnilistRating> {
        let payload = serde_json::json!({
            "query": r#"
                query ($id: Int) {
                  Media(id: $id, type: ANIME) {
                    id
                    averageScore
                    meanScore
                  }
                }
            "#,
            "variables": {
                "id": id
            }
        });

        let response: AniListGraphqlResponse<AniListRatingData> =
            anilist_post(ctx, &payload, "rating").await?;

        if let Some(errors) = response.errors {
            return Err(Error::new(join_graphql_errors(&errors)));
        }

        let media = response.data.and_then(|data| data.media);
        let score = media
            .as_ref()
            .and_then(|item| item.average_score.or(item.mean_score))
            .map(|raw| raw / 10.0);

        Ok(AnilistRating {
            id: i64::from(id),
            score,
        })
    }

    async fn anilist_mappings(&self, ctx: &Context<'_>, id: i32) -> Result<AnilistMappings> {
        let http = ctx.data::<HttpClient>()?;
        let dedupe_key = format!("anizip:mappings:{id}");
        let response: AniZipMappingsResponse = http
            .get_json(ANIZIP, dedupe_key, |client| {
                client
                    .get(ANIZIP_MAPPINGS_URL)
                    .query(&[("anilist_id", id)])
                    .header("Accept", "application/json")
            })
            .await
            .map_err(|e| Error::new(format!("AniZip mappings request failed: {e}")))?;

        Ok(AnilistMappings {
            anilist_id: i64::from(id),
            tmdb_id: response
                .themoviedb_id
                .as_ref()
                .and_then(parse_mapping_id)
                .or_else(|| response.mappings.themoviedb_id.as_ref().and_then(parse_mapping_id)),
            tvdb_id: response
                .thetvdb_id
                .as_ref()
                .and_then(parse_mapping_id)
                .or_else(|| response.mappings.thetvdb_id.as_ref().and_then(parse_mapping_id)),
        })
    }
}

#[derive(Deserialize)]
struct AniListGraphqlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphqlError>>,
}

#[derive(Deserialize)]
struct GraphqlError {
    message: String,
}

#[derive(Deserialize)]
struct AniListTrendingData {
    #[serde(rename = "Page")]
    page: AniListTrendingPage,
}

#[derive(Deserialize)]
struct AniListTrendingPage {
    media: Vec<AniListTrendingMedia>,
}

#[derive(Deserialize)]
struct AniListTrendingMedia {
    id: i64,
    title: AniListTitle,
    #[serde(rename = "coverImage")]
    cover_image: Option<AniListCoverImage>,
    #[serde(rename = "seasonYear")]
    season_year: Option<i64>,
    format: Option<String>,
}

#[derive(Deserialize)]
struct AniListTitle {
    romaji: Option<String>,
    english: Option<String>,
    native: Option<String>,
}

#[derive(Deserialize)]
struct AniListCoverImage {
    large: Option<String>,
}

#[derive(Deserialize)]
struct AniListRatingData {
    #[serde(rename = "Media")]
    media: Option<AniListRatingMedia>,
}

#[derive(Deserialize)]
struct AniListRatingMedia {
    #[serde(rename = "averageScore")]
    average_score: Option<f64>,
    #[serde(rename = "meanScore")]
    mean_score: Option<f64>,
}

#[derive(Deserialize)]
struct AniZipMappingsResponse {
    #[serde(default)]
    themoviedb_id: Option<serde_json::Value>,
    #[serde(default)]
    thetvdb_id: Option<serde_json::Value>,
    #[serde(default)]
    mappings: AniZipNestedMappings,
}

#[derive(Default, Deserialize)]
struct AniZipNestedMappings {
    #[serde(default)]
    themoviedb_id: Option<serde_json::Value>,
    #[serde(default)]
    thetvdb_id: Option<serde_json::Value>,
}

fn parse_mapping_id(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(number) => number.as_i64(),
        serde_json::Value::String(string) => string.parse().ok(),
        _ => None,
    }
}

fn join_graphql_errors(errors: &[GraphqlError]) -> String {
    errors
        .iter()
        .map(|error| error.message.as_str())
        .collect::<Vec<_>>()
        .join("; ")
}

async fn anilist_post<T>(
    ctx: &Context<'_>,
    payload: &serde_json::Value,
    operation: &str,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let http = ctx.data::<HttpClient>()?;
    let dedupe_key = format!("anilist:{operation}:{payload}");

    http.get_json(ANILIST, dedupe_key, |client| {
        client
            .post(ANILIST_GRAPHQL_URL)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .json(payload)
    })
    .await
    .map_err(|e| Error::new(format!("AniList {operation} request failed: {e}")))
}
