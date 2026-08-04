use async_graphql::{Context, Error, Json, Object, Result};
use riven_core::http::HttpClient;
use riven_core::plugin::PluginRegistry;

use crate::profiles::TMDB;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use riven_core::entities::helpers::{Artwork, artwork_url};

use crate::schema::metadata::details::{MediaDetails, PersonDetails, Source, TmdbPerson};
use crate::schema::metadata::{
    TMDB_API_BASE, TmdbCollectionDetails, TmdbLogoAndCert, TmdbPage, get_tmdb_api_key,
    transform_collection, transform_item,
};
use crate::schema::queries::trakt;

/// Sub-resources the movie detail page needs alongside the movie itself.
const MOVIE_APPEND: &str =
    "external_ids,images,recommendations,similar,videos,credits,release_dates";
const PERSON_APPEND: &str = "combined_credits,external_ids";

#[derive(Default)]
pub struct CoreTmdbQuery;

#[Object]
impl CoreTmdbQuery {
    /// Everything the movie detail page renders, in one shape shared with
    /// `showDetails`.
    async fn movie_details(&self, ctx: &Context<'_>, id: i64) -> Result<MediaDetails> {
        let data = tmdb_json(
            ctx,
            format!("movie_details:{id}"),
            |request| request.query(&[("append_to_response", MOVIE_APPEND)]),
            &format!("/3/movie/{id}"),
        )
        .await?;

        let mut details: MediaDetails = serde_json::from_value(data)
            .map_err(|e| Error::new(format!("unexpected TMDB movie payload: {e}")))?;
        details.source = Source::Tmdb;
        // A missing Trakt key or a failed lookup just means no related titles.
        details.trakt = trakt::recommendations(ctx, &id.to_string(), "tmdb", "movie")
            .await
            .unwrap_or_default();
        Ok(details)
    }

    /// A cast/crew member or a company, both rendered through one shape.
    async fn person_details(
        &self,
        ctx: &Context<'_>,
        id: i64,
        indexer: Option<String>,
    ) -> Result<PersonDetails> {
        if indexer.as_deref() == Some("tvdb") {
            return super::tvdb::person_details(ctx, id).await;
        }
        let data = tmdb_json(
            ctx,
            format!("person_details:{id}"),
            |request| request.query(&[("append_to_response", PERSON_APPEND)]),
            &format!("/3/person/{id}"),
        )
        .await?;
        let person: TmdbPerson = serde_json::from_value(data)
            .map_err(|e| Error::new(format!("unexpected TMDB person payload: {e}")))?;
        Ok(person.into())
    }

    async fn company_details(&self, ctx: &Context<'_>, id: i64) -> Result<PersonDetails> {
        let filmography = |media_type: &'static str| async move {
            let data = tmdb_json(
                ctx,
                format!("company_titles:{media_type}:{id}"),
                move |request| {
                    request.query(&[
                        ("with_companies", id.to_string()),
                        ("sort_by", "popularity.desc".to_owned()),
                    ])
                },
                &format!("/3/discover/{media_type}"),
            )
            .await;
            data.map(|page| map_tmdb_page(page, media_type).results)
                .unwrap_or_default()
        };

        let endpoint = format!("/3/company/{id}");
        let (company, movies, shows) = futures::join!(
            tmdb_json(ctx, format!("company:{id}"), |request| request, &endpoint),
            filmography("movie"),
            filmography("tv"),
        );

        Ok(crate::schema::metadata::details::company_details(
            &company?, movies, shows,
        ))
    }

    async fn tmdb_collection_details(
        &self,
        ctx: &Context<'_>,
        id: i64,
    ) -> Result<TmdbCollectionDetails> {
        let data = tmdb_json(
            ctx,
            format!("collection:{id}"),
            |request| request,
            &format!("/3/collection/{id}"),
        )
        .await?;
        Ok(transform_collection(&data))
    }

    async fn tmdb_category(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "type")] media_type: String,
        category: String,
        page: Option<i64>,
    ) -> Result<TmdbPage> {
        if !matches!(media_type.as_str(), "movie" | "tv") {
            return Err(Error::new(format!("Invalid media type: {media_type}")));
        }
        if !matches!(category.as_str(), "popular" | "top_rated") {
            return Err(Error::new(format!("Invalid TMDB category: {category}")));
        }

        let page = page.unwrap_or(1);
        let data = tmdb_json(
            ctx,
            format!("category:{media_type}:{category}:{page}"),
            move |request| {
                request.query(&[
                    ("page", page.to_string()),
                    ("language", "en-US".to_string()),
                ])
            },
            &format!("/3/{media_type}/{category}"),
        )
        .await?;

        Ok(with_genres(ctx, map_tmdb_page(data, &media_type)).await)
    }

    async fn search_tmdb(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "type")] media_type: String,
        params: Option<Json<serde_json::Value>>,
        search_mode: Option<String>,
    ) -> Result<TmdbPage> {
        let is_search = matches!(search_mode.as_deref(), Some("search") | Some("hybrid"));
        let endpoint = match (media_type.as_str(), is_search) {
            ("movie", true) => "/3/search/movie",
            ("movie", false) => "/3/discover/movie",
            ("tv", true) => "/3/search/tv",
            ("tv", false) => "/3/discover/tv",
            ("person", _) => "/3/search/person",
            ("company", _) => "/3/search/company",
            _ => return Err(Error::new(format!("Invalid media type: {media_type}"))),
        };

        let mut query_params: Vec<(String, String)> = Vec::new();
        if let Some(Json(obj)) = params
            && let Some(map) = obj.as_object()
        {
            for (k, v) in map {
                if k == "searchMode" {
                    continue;
                }
                let val = match v {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => continue,
                };
                if !val.is_empty() {
                    query_params.push((k.clone(), val));
                }
            }
        }

        if media_type == "tv" && !is_search {
            for (k, v) in &mut query_params {
                if k == "sort_by" {
                    *v = v.replace("primary_release_date", "first_air_date");
                }
            }
        }

        let data = tmdb_json(
            ctx,
            format!("search:{media_type}:{endpoint}:{query_params:?}"),
            move |request| request.query(&query_params),
            endpoint,
        )
        .await?;

        Ok(with_genres(ctx, map_tmdb_page(data, &media_type)).await)
    }

    async fn tmdb_logo_and_cert(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "type")] media_type: String,
        id: i64,
    ) -> Result<TmdbLogoAndCert> {
        let (endpoint, append) = match media_type.as_str() {
            "movie" => (format!("/3/movie/{id}"), "images,release_dates"),
            "tv" => (format!("/3/tv/{id}"), "images,content_ratings"),
            _ => return Err(Error::new(format!("Invalid media type: {media_type}"))),
        };

        let data = match tmdb_json(
            ctx,
            format!("logo_cert:{media_type}:{id}"),
            move |request| request.query(&[("append_to_response", append)]),
            &endpoint,
        )
        .await
        {
            Ok(data) => data,
            Err(_) => {
                return Ok(TmdbLogoAndCert {
                    logo: None,
                    certification: None,
                });
            }
        };

        let logos = data
            .get("images")
            .and_then(|i| i.get("logos"))
            .and_then(|l| l.as_array());

        let logo = logos.and_then(|logos| {
            logos
                .iter()
                .find(|l| l.get("iso_639_1").and_then(|v| v.as_str()) == Some("en"))
                .or_else(|| logos.first())
                .and_then(|l| l.get("file_path").and_then(|v| v.as_str()))
                .and_then(|path| artwork_url(Some(path), Artwork::Logo))
        });

        let certification = if media_type == "movie" {
            data.get("release_dates")
                .and_then(|r| r.get("results"))
                .and_then(|r| r.as_array())
                .and_then(|results| {
                    results
                        .iter()
                        .find(|r| r.get("iso_3166_1").and_then(|v| v.as_str()) == Some("US"))
                        .and_then(|r| r.get("release_dates"))
                        .and_then(|d| d.as_array())
                        .and_then(|dates| {
                            dates.iter().find_map(|d| {
                                let cert =
                                    d.get("certification").and_then(|v| v.as_str())?.to_owned();
                                if cert.is_empty() { None } else { Some(cert) }
                            })
                        })
                })
        } else {
            data.get("content_ratings")
                .and_then(|r| r.get("results"))
                .and_then(|r| r.as_array())
                .and_then(|results| {
                    results
                        .iter()
                        .find(|r| r.get("iso_3166_1").and_then(|v| v.as_str()) == Some("US"))
                        .and_then(|r| r.get("rating").and_then(|v| v.as_str()))
                        .filter(|r| !r.is_empty())
                        .map(str::to_owned)
                })
        };

        Ok(TmdbLogoAndCert {
            logo,
            certification,
        })
    }

    async fn trending_tmdb(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "type")] media_type: String,
        time_window: String,
        page: Option<i64>,
    ) -> Result<TmdbPage> {
        if !matches!(media_type.as_str(), "movie" | "tv" | "all") {
            return Err(Error::new(format!("Invalid media type: {media_type}")));
        }
        if !matches!(time_window.as_str(), "day" | "week") {
            return Err(Error::new(format!("Invalid time window: {time_window}")));
        }

        let page = page.unwrap_or(1);
        let data = tmdb_json(
            ctx,
            format!("trending:{media_type}:{time_window}:{page}"),
            move |request| request.query(&[("page", page.to_string())]),
            &format!("/3/trending/{media_type}/{time_window}"),
        )
        .await?;

        Ok(with_genres(ctx, map_tmdb_page(data, &media_type)).await)
    }
}

async fn tmdb_json<F>(
    ctx: &Context<'_>,
    dedupe_key: String,
    build_request: F,
    endpoint: &str,
) -> Result<serde_json::Value>
where
    F: Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
{
    let registry = ctx.data::<Arc<PluginRegistry>>()?;
    let http = ctx.data::<HttpClient>()?;
    let api_key = get_tmdb_api_key(registry).await?;

    http.get_json(TMDB, format!("tmdb:{dedupe_key}"), |client| {
        let request = client
            .get(format!("{TMDB_API_BASE}{endpoint}"))
            .bearer_auth(&api_key);
        build_request(request)
    })
    .await
    .map_err(|e| Error::new(format!("TMDB request failed: {e}")))
}

/// TMDB's genre id → name tables, one fetch per media type for the process.
///
/// The frontend used to carry a hardcoded copy of this; it is upstream data, so
/// it is read from upstream. A failed fetch costs the names, not the page.
static GENRES: OnceLock<tokio::sync::OnceCell<HashMap<i64, String>>> = OnceLock::new();

async fn genre_names(ctx: &Context<'_>) -> &'static HashMap<i64, String> {
    GENRES
        .get_or_init(tokio::sync::OnceCell::new)
        .get_or_init(|| async {
            let mut names = HashMap::new();
            for kind in ["movie", "tv"] {
                let Ok(list) = tmdb_json(
                    ctx,
                    format!("genres:{kind}"),
                    |request| request,
                    &format!("/3/genre/{kind}/list"),
                )
                .await
                else {
                    continue;
                };
                for genre in list
                    .get("genres")
                    .and_then(|g| g.as_array())
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                {
                    if let (Some(id), Some(name)) = (
                        genre.get("id").and_then(serde_json::Value::as_i64),
                        genre.get("name").and_then(|n| n.as_str()),
                    ) {
                        names.entry(id).or_insert_with(|| name.to_owned());
                    }
                }
            }
            names
        })
        .await
}

/// Resolve `genre_ids` to names on every row of a page.
async fn with_genres(ctx: &Context<'_>, mut page: TmdbPage) -> TmdbPage {
    let names = genre_names(ctx).await;
    for item in &mut page.results {
        item.genres = item
            .genre_ids
            .iter()
            .filter_map(|id| names.get(id).cloned())
            .collect();
    }
    page
}

fn map_tmdb_page(data: serde_json::Value, media_type: &str) -> TmdbPage {
    let results = data
        .get("results")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .map(|item| transform_item(item, media_type))
                .collect()
        })
        .unwrap_or_default();

    TmdbPage {
        results,
        page: data
            .get("page")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(1),
        total_pages: data
            .get("total_pages")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(1),
        total_results: data
            .get("total_results")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
    }
}
