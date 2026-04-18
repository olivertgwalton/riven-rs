use async_graphql::{Context, Error, Object, Result, SimpleObject};
use riven_core::http::HttpClient;
use riven_core::http::profiles::TRAKT;
use riven_core::plugin::PluginRegistry;
use serde::Deserialize;
use std::sync::Arc;

const TRAKT_BASE_URL: &str = "https://api.trakt.tv";

#[derive(Default)]
pub struct CoreTraktQuery;

#[derive(SimpleObject)]
pub struct TraktListItem {
    pub id: i64,
    pub title: String,
    pub poster_path: Option<String>,
    pub media_type: String,
    pub year: String,
    pub indexer: String,
}

#[Object]
impl CoreTraktQuery {
    async fn trakt_recommendations(
        &self,
        ctx: &Context<'_>,
        id: String,
        id_type: String,
        media_type: String,
    ) -> Result<Vec<TraktListItem>> {
        if !matches!(id_type.as_str(), "tmdb" | "tvdb") {
            return Err(Error::new(format!("Invalid Trakt id type: {id_type}")));
        }

        let (query_type, endpoint_prefix, normalized_media_type) = match media_type.as_str() {
            "movie" => ("movie", "movies", "movie"),
            "show" | "tv" => ("show", "shows", "tv"),
            _ => {
                return Err(Error::new(format!(
                    "Invalid Trakt media type: {media_type}"
                )));
            }
        };

        let Some(client_id) = get_trakt_client_id(ctx).await? else {
            return Ok(vec![]);
        };

        let search_url = format!("{TRAKT_BASE_URL}/search/{id_type}/{id}");
        let search_results: Vec<TraktSearchResult> = trakt_get(
            ctx,
            &client_id,
            &search_url,
            Some(&[("type", query_type)]),
            "search",
        )
        .await?;

        let slug = search_results
            .into_iter()
            .find_map(|result| match query_type {
                "movie" => result.movie.and_then(|item| item.ids.slug),
                _ => result.show.and_then(|item| item.ids.slug),
            });

        let Some(slug) = slug else {
            return Ok(vec![]);
        };

        let related_url = format!("{TRAKT_BASE_URL}/{endpoint_prefix}/{slug}/related");
        let related_items: Vec<TraktRelatedItem> = trakt_get(
            ctx,
            &client_id,
            &related_url,
            Some(&[("extended", "images")]),
            "related",
        )
        .await?;

        Ok(related_items
            .into_iter()
            .filter_map(|item| {
                let tmdb_id = item.ids.tmdb?;
                Some(TraktListItem {
                    id: tmdb_id,
                    title: item.title.unwrap_or_default(),
                    poster_path: item
                        .images
                        .and_then(|images| images.poster)
                        .and_then(|posters| posters.into_iter().find(|poster| !poster.is_empty()))
                        .map(normalize_image_url),
                    media_type: normalized_media_type.to_string(),
                    year: item
                        .year
                        .map(|year| year.to_string())
                        .unwrap_or_else(|| "N/A".to_string()),
                    indexer: "tmdb".to_string(),
                })
            })
            .collect())
    }
}

async fn get_trakt_client_id(ctx: &Context<'_>) -> Result<Option<String>> {
    let registry = ctx.data::<Arc<PluginRegistry>>()?;
    Ok(registry
        .get_plugin_settings_json("trakt")
        .await
        .and_then(|settings| {
            settings
                .get("clientid")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        }))
}

async fn trakt_get<T>(
    ctx: &Context<'_>,
    client_id: &str,
    url: &str,
    query: Option<&[(&str, &str)]>,
    operation: &str,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let http = ctx.data::<HttpClient>()?;
    let dedupe_key = format!("trakt:{operation}:{url}:{query:?}");

    http.get_json(TRAKT, dedupe_key, |client| {
        let mut request = client
            .get(url)
            .header("Content-Type", "application/json")
            .header("trakt-api-version", "2")
            .header("trakt-api-key", client_id)
            .header("User-Agent", "riven-rs");

        if let Some(query) = query {
            request = request.query(query);
        }

        request
    })
    .await
    .map_err(|e| Error::new(format!("Trakt {operation} request failed: {e}")))
}

fn normalize_image_url(raw: String) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw
    } else {
        format!("https://{raw}")
    }
}

#[derive(Deserialize)]
struct TraktSearchResult {
    movie: Option<TraktSearchItem>,
    show: Option<TraktSearchItem>,
}

#[derive(Deserialize)]
struct TraktSearchItem {
    ids: TraktIds,
}

#[derive(Deserialize)]
struct TraktRelatedItem {
    title: Option<String>,
    year: Option<i64>,
    ids: TraktIds,
    images: Option<TraktImages>,
}

#[derive(Deserialize)]
struct TraktIds {
    slug: Option<String>,
    tmdb: Option<i64>,
}

#[derive(Deserialize)]
struct TraktImages {
    poster: Option<Vec<String>>,
}
