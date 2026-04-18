use async_graphql::{Json, *};
use riven_core::plugin::PluginRegistry;
use std::sync::Arc;

use crate::schema::metadata::{
    get_tmdb_api_key, transform_item, TmdbLogoAndCert, TmdbPage, TMDB_API_BASE, TMDB_IMAGE_BASE,
};

#[derive(Default)]
pub struct TmdbQuery;

#[Object]
impl TmdbQuery {
    async fn search_tmdb(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "type")] media_type: String,
        params: Option<Json<serde_json::Value>>,
        search_mode: Option<String>,
    ) -> Result<TmdbPage> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let api_key = get_tmdb_api_key(registry).await?;

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

        let resp = reqwest::Client::new()
            .get(format!("{TMDB_API_BASE}{endpoint}"))
            .bearer_auth(&api_key)
            .query(&query_params)
            .send()
            .await
            .map_err(|e| Error::new(format!("TMDB request failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(Error::new(format!("TMDB API error: {}", resp.status())));
        }

        let data: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::new(format!("TMDB response parse error: {e}")))?;

        let results = data
            .get("results")
            .and_then(|v| v.as_array())
            .map(|items| items.iter().map(|item| transform_item(item, &media_type)).collect())
            .unwrap_or_default();

        Ok(TmdbPage {
            results,
            page: data.get("page").and_then(|v| v.as_i64()).unwrap_or(1),
            total_pages: data.get("total_pages").and_then(|v| v.as_i64()).unwrap_or(1),
            total_results: data.get("total_results").and_then(|v| v.as_i64()).unwrap_or(0),
        })
    }

    async fn tmdb_logo_and_cert(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "type")] media_type: String,
        id: i64,
    ) -> Result<TmdbLogoAndCert> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let api_key = get_tmdb_api_key(registry).await?;

        let (endpoint, append) = match media_type.as_str() {
            "movie" => (format!("/3/movie/{id}"), "images,release_dates"),
            "tv" => (format!("/3/tv/{id}"), "images,content_ratings"),
            _ => return Err(Error::new(format!("Invalid media type: {media_type}"))),
        };

        let resp = reqwest::Client::new()
            .get(format!("{TMDB_API_BASE}{endpoint}"))
            .bearer_auth(&api_key)
            .query(&[("append_to_response", append)])
            .send()
            .await
            .map_err(|e| Error::new(format!("TMDB request failed: {e}")))?;

        if !resp.status().is_success() {
            return Ok(TmdbLogoAndCert { logo: None, certification: None });
        }

        let data: serde_json::Value = resp
            .json()
            .await
            .map_err(|_| Error::new("TMDB response parse error"))?;

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

        Ok(TmdbLogoAndCert { logo, certification })
    }
}
