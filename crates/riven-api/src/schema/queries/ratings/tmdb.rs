use async_graphql::{Context, Error, Result};
use riven_core::http::HttpClient;
use riven_core::http::profiles::TMDB;
use riven_core::plugin::PluginRegistry;
use serde::Deserialize;
use std::sync::Arc;

use crate::schema::metadata::{TMDB_API_BASE, get_tmdb_api_key};

pub(super) async fn rating_details(
    ctx: &Context<'_>,
    media_type: &str,
    id: i64,
) -> Result<TmdbRatingDetails> {
    let (http, api_key) = backend(ctx).await?;
    http.get_json(
        TMDB,
        format!("tmdb:rating_details:{media_type}:{id}"),
        |client| {
            client
                .get(format!("{TMDB_API_BASE}/3/{media_type}/{id}"))
                .bearer_auth(&api_key)
                .query(&[("append_to_response", "external_ids")])
        },
    )
    .await
    .map_err(|e| Error::new(format!("TMDB ratings request failed: {e}")))
}

async fn backend(ctx: &Context<'_>) -> Result<(HttpClient, String)> {
    let registry = ctx.data::<Arc<PluginRegistry>>()?;
    Ok((
        ctx.data::<HttpClient>()?.clone(),
        get_tmdb_api_key(registry).await?,
    ))
}

#[derive(Deserialize)]
pub(super) struct TmdbRatingDetails {
    title: Option<String>,
    name: Option<String>,
    release_date: Option<String>,
    first_air_date: Option<String>,
    pub(super) vote_average: Option<f64>,
    external_ids: Option<TmdbExternalIds>,
}

impl TmdbRatingDetails {
    pub(super) fn title(&self) -> Option<&str> {
        self.title.as_deref().or(self.name.as_deref())
    }

    pub(super) fn year(&self) -> Option<i64> {
        self.release_date
            .as_deref()
            .or(self.first_air_date.as_deref())
            .and_then(|date| date.split('-').next())
            .and_then(|year| year.parse().ok())
    }

    pub(super) fn imdb_id(&self) -> Option<String> {
        self.external_ids
            .as_ref()
            .and_then(|ids| ids.imdb_id.clone())
            .filter(|value| !value.is_empty())
    }
}

#[derive(Deserialize)]
struct TmdbExternalIds {
    imdb_id: Option<String>,
}
