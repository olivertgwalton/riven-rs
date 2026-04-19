use async_graphql::{Context, Error, Object, Result, SimpleObject};
use riven_core::http::HttpClient;
use riven_core::http::profiles::TMDB;
use riven_core::plugin::PluginRegistry;
use serde::Deserialize;
use std::sync::Arc;

use crate::schema::metadata::{TMDB_API_BASE, get_tmdb_api_key};

use super::anilist::fetch_anilist_mappings;
use super::tvdb::resolve_tmdb_to_tvdb_id;

#[derive(Default)]
pub struct CoreExternalIdsQuery;

#[derive(SimpleObject)]
pub struct IdResolution {
    pub id: String,
    pub resolved: bool,
}

#[Object]
impl CoreExternalIdsQuery {
    async fn resolve_external_id(
        &self,
        ctx: &Context<'_>,
        from: String,
        to: String,
        id: String,
        media_type: Option<String>,
    ) -> Result<IdResolution> {
        resolve_external_id(ctx, &from, &to, &id, media_type.as_deref()).await
    }
}

async fn resolve_external_id(
    ctx: &Context<'_>,
    from: &str,
    to: &str,
    id: &str,
    media_type: Option<&str>,
) -> Result<IdResolution> {
    let from = key(from);
    let to = key(to);

    if from == to {
        return Ok(resolution(id.to_owned(), true));
    }

    let resolved = match (from.as_str(), to.as_str()) {
        ("tmdb", "tvdb") => {
            if required_media_type(media_type)? == "tv" {
                resolve_tmdb_to_tvdb_id(ctx, id)
                    .await?
                    .map(|id| id.to_string())
            } else {
                None
            }
        }
        ("tmdb", "imdb") => tmdb_external_ids(ctx, required_media_type(media_type)?, id)
            .await?
            .imdb_id
            .filter(|id| !id.is_empty()),
        ("anilist", "tmdb") => anilist_external_id(ctx, id, "tmdb").await?,
        ("anilist", "tvdb") => anilist_external_id(ctx, id, "tvdb").await?,
        ("riven", "tmdb") => riven_external_id(ctx, id, "tmdb_id").await?,
        ("riven", "tvdb") => riven_external_id(ctx, id, "tvdb_id").await?,
        _ => None,
    };

    Ok(match resolved {
        Some(id) => resolution(id, true),
        None => resolution(id.to_owned(), false),
    })
}

async fn tmdb_external_ids(
    ctx: &Context<'_>,
    media_type: &str,
    id: &str,
) -> Result<TmdbExternalIds> {
    let registry = ctx.data::<Arc<PluginRegistry>>()?;
    let http = ctx.data::<HttpClient>()?.clone();
    let api_key = get_tmdb_api_key(registry).await?;

    http.get_json(
        TMDB,
        format!("tmdb:external_ids:{media_type}:{id}"),
        |client| {
            client
                .get(format!("{TMDB_API_BASE}/3/{media_type}/{id}/external_ids"))
                .bearer_auth(&api_key)
        },
    )
    .await
    .map_err(|e| Error::new(format!("TMDB external ID request failed: {e}")))
}

async fn anilist_external_id(ctx: &Context<'_>, id: &str, target: &str) -> Result<Option<String>> {
    let mapping = fetch_anilist_mappings(ctx, parse_id(id, "AniList")?).await?;
    Ok(match target {
        "tmdb" => mapping.tmdb_id,
        "tvdb" => mapping.tvdb_id,
        _ => None,
    }
    .map(|id| id.to_string()))
}

async fn riven_external_id(ctx: &Context<'_>, id: &str, field: &str) -> Result<Option<String>> {
    let pool = ctx.data::<sqlx::PgPool>()?;
    let item =
        riven_db::repo::media::get_media_item(pool, parse_id(id, "Riven media item")?).await?;
    Ok(item.and_then(|item| match field {
        "tmdb_id" => item.tmdb_id,
        "tvdb_id" => item.tvdb_id,
        _ => None,
    }))
}

fn resolution(id: String, resolved: bool) -> IdResolution {
    IdResolution { id, resolved }
}

fn key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn required_media_type(value: Option<&str>) -> Result<&'static str> {
    match value.map(key).as_deref() {
        Some("movie") => Ok("movie"),
        Some("tv") => Ok("tv"),
        Some(other) => Err(Error::new(format!("Unsupported mediaType: {other}"))),
        None => Err(Error::new("mediaType is required")),
    }
}

fn parse_id<T>(id: &str, label: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    id.trim()
        .parse()
        .map_err(|_| Error::new(format!("{label} ID must be numeric")))
}

#[derive(Deserialize)]
struct TmdbExternalIds {
    imdb_id: Option<String>,
}
