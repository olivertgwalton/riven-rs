use async_graphql::{Context, Error, Json, Object, Result};
use riven_core::http::HttpClient;
use riven_core::http::profiles::TMDB;
use riven_core::plugin::PluginRegistry;
use std::sync::Arc;

use crate::schema::metadata::{
    TMDB_API_BASE, TMDB_IMAGE_BASE, TmdbLogoAndCert, TmdbPage, get_tmdb_api_key, transform_item,
};

#[derive(Default)]
pub struct CoreTmdbQuery;

#[Object]
impl CoreTmdbQuery {
    async fn tmdb_details(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "type")] media_type: String,
        id: i64,
        append_to_response: Option<String>,
    ) -> Result<Json<serde_json::Value>> {
        let endpoint = match media_type.as_str() {
            "movie" => format!("/3/movie/{id}"),
            "tv" => format!("/3/tv/{id}"),
            "person" => format!("/3/person/{id}"),
            "company" => format!("/3/company/{id}"),
            _ => return Err(Error::new(format!("Invalid media type: {media_type}"))),
        };

        let append = append_to_response
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_owned);

        let data = tmdb_json(ctx, format!("details:{media_type}:{id}:{append:?}"), move |request| {
            if let Some(append) = append.as_deref() {
                request.query(&[("append_to_response", append)])
            } else {
                request
            }
        }, &endpoint)
        .await?;

        Ok(Json(data))
    }

    async fn tmdb_collection(&self, ctx: &Context<'_>, id: i64) -> Result<Json<serde_json::Value>> {
        let data = tmdb_json(ctx, format!("collection:{id}"), |request| request, &format!("/3/collection/{id}")).await?;
        Ok(Json(data))
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
                request.query(&[("page", page.to_string()), ("language", "en-US".to_string())])
            },
            &format!("/3/{media_type}/{category}"),
        )
        .await?;

        Ok(map_tmdb_page(data, &media_type))
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
        if let Some(Json(obj)) = params {
            if let Some(map) = obj.as_object() {
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

        Ok(map_tmdb_page(data, &media_type))
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
                .map(|path| format!("{TMDB_IMAGE_BASE}/w500{path}"))
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

        Ok(map_tmdb_page(data, &media_type))
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

fn map_tmdb_page(data: serde_json::Value, media_type: &str) -> TmdbPage {
    let results = data
        .get("results")
        .and_then(|v| v.as_array())
        .map(|items| items.iter().map(|item| transform_item(item, media_type)).collect())
        .unwrap_or_default();

    TmdbPage {
        results,
        page: data.get("page").and_then(|v| v.as_i64()).unwrap_or(1),
        total_pages: data.get("total_pages").and_then(|v| v.as_i64()).unwrap_or(1),
        total_results: data.get("total_results").and_then(|v| v.as_i64()).unwrap_or(0),
    }
}
