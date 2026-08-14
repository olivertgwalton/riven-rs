use async_graphql::*;
use redis::AsyncCommands;
use riven_core::http::{HttpClient, HttpServiceProfile};
use riven_core::nzb::{nzb_indexer_redis_key, nzb_info_hash, nzb_url_redis_key};
use riven_core::plugin::PluginRegistry;
use riven_core::types::MediaItemType;
use riven_db::entities::MediaItem;
use riven_db::repo;
use riven_queue::{JobQueue, RankStreamsJob};
use std::sync::Arc;
use std::time::Duration;

use crate::schema::auth::{Capability, require};
use crate::schema::discovery::{
    apply_cache_status, discover_streams, ensure_download_target, ensure_show_target,
    resolve_pack_seasons,
};
use crate::schema::types::DiscoveredStream;

const PREVIEW_NZB_PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("manual-nzb-preview").with_rate_limit(10, Duration::from_secs(60));

/// TTL for `download_explicit_nzb`'s Redis entry — deliberately longer than
/// `riven_core::nzb::NZB_URL_TTL_SECS` (7 days, sized for a real scrape's URL
/// going stale).
/// See the call site for why: a manually-supplied URL's item is also marked
/// `manual_scrape_only`, which forecloses the automatic-retry path that would
/// otherwise be the safety net for a download that needs a delayed retry.
const MANUAL_NZB_URL_TTL_SECS: u64 = 60 * 60 * 24 * 30;

/// A manually-supplied NZB URL a caller with `ScrapeItems` controls directly
/// gets fetched server-side (`preview_manual_nzb`) or handed to
/// `plugin-usenet` to fetch later (`download_explicit_nzb`). Without this
/// check that is a same-origin-request forgery primitive: an authenticated
/// but otherwise unprivileged caller could point it at a cloud metadata
/// endpoint, another container, or a localhost-bound admin port, and use the
/// response status/error text this resolver would otherwise echo back as an
/// oracle to enumerate what's reachable. Rejects anything that doesn't
/// resolve to a public address, except this crate's own loopback temp-upload
/// URL — which must additionally land on `gql_port`, this instance's real
/// listen port, or the shape check alone (`uploaded_nzb_filename`) would let
/// an attacker reuse the trusted `127.0.0.1` exemption to reach *any* other
/// port on the container's loopback interface.
///
/// Returns the exact address the caller must connect to (`None` for the
/// trusted loopback-upload exemption, where no pinning is needed): DNS is
/// resolved and validated here, but a caller that re-resolves the hostname
/// itself when it actually connects (ordinary redirect-following HTTP
/// clients do exactly this) reopens the same SSRF this function exists to
/// close — either a compromised/malicious upstream's redirect, or the
/// authoritative DNS answer simply changing between this check and the real
/// connection (DNS rebinding). Callers that make a real outbound connection
/// must pin to the returned address rather than letting the URL's hostname
/// be resolved a second time.
async fn validate_nzb_fetch_target(
    raw: &str,
    gql_port: u16,
) -> Result<Option<(String, std::net::SocketAddr)>> {
    let parsed = url::Url::parse(raw).map_err(|error| {
        tracing::debug!(%error, "manual NZB URL failed to parse");
        async_graphql::Error::new("NZB URL must start with http:// or https://")
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(async_graphql::Error::new(
            "NZB URL must start with http:// or https://",
        ));
    }
    if riven_core::nzb::uploaded_nzb_filename(raw).is_some()
        && parsed.port_or_known_default() == Some(gql_port)
    {
        return Ok(None);
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| async_graphql::Error::new("NZB URL is not reachable"))?;
    let port = parsed.port_or_known_default().unwrap_or(80);
    let addr = riven_core::http::ssrf_guard::resolve_public_target(host, port)
        .await
        .map_err(|error| {
            tracing::debug!(%error, "manual NZB URL lookup_host failed");
            async_graphql::Error::new("NZB URL is not reachable")
        })?
        .ok_or_else(|| async_graphql::Error::new("NZB URL is not reachable"))?;
    Ok(Some((host.to_string(), addr)))
}

/// Fetches `nzb_url` and reads it as text. `pinned_target` is
/// [`validate_nzb_fetch_target`]'s result: `Some((host, addr))` for a real
/// external URL, fetched through [`riven_core::http::ssrf_guard::build_pinned_client`]
/// rather than the shared rate-limited client — that client follows
/// redirects and performs its own DNS resolution, either of which would undo
/// the validation that was just done. `None` is the trusted loopback-upload
/// exemption, safe to fetch through the ordinary shared client.
async fn fetch_capped_nzb_text(
    http: &HttpClient,
    nzb_url: &str,
    pinned_target: Option<(String, std::net::SocketAddr)>,
) -> Result<String> {
    let response = match pinned_target {
        Some((host, addr)) => {
            let client = riven_core::http::ssrf_guard::build_pinned_client(&host, addr).map_err(
                |error| {
                    tracing::debug!(%error, "failed to build pinned NZB fetch client");
                    async_graphql::Error::new("Failed to fetch NZB")
                },
            )?;
            client.get(nzb_url).send().await.map_err(|error| {
                tracing::debug!(%error, "manual NZB fetch request failed");
                async_graphql::Error::new("Failed to fetch NZB")
            })?
        }
        None => http
            .send(PREVIEW_NZB_PROFILE, |client| client.get(nzb_url))
            .await
            .map_err(|error| {
                tracing::debug!(%error, "manual NZB fetch request failed");
                async_graphql::Error::new("Failed to fetch NZB")
            })?,
    };
    riven_core::http::ssrf_guard::read_capped_text(response)
        .await
        .map_err(|error| {
            let message = match &error {
                riven_core::http::ssrf_guard::CappedReadError::TooLarge => {
                    "NZB response is too large"
                }
                riven_core::http::ssrf_guard::CappedReadError::InvalidUtf8 => "Failed to read NZB",
                riven_core::http::ssrf_guard::CappedReadError::NotSuccess(_)
                | riven_core::http::ssrf_guard::CappedReadError::Transport(_) => {
                    "Failed to fetch NZB"
                }
            };
            tracing::debug!(%error, "manual NZB fetch failed");
            async_graphql::Error::new(message)
        })
}

/// The `dn=` (display name) parameter from a magnet URI, URL-decoded. `None`
/// for a bare hash or a magnet with no `dn=` — the only case
/// [`preview_manual_magnet`] can't offer a real title for.
fn extract_magnet_display_name(magnet: &str) -> Option<String> {
    let (_, query) = magnet.split_once('?')?;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == "dn")
        .map(|(_, value)| value.into_owned())
        .filter(|name| !name.trim().is_empty())
}

#[derive(Default)]
pub struct StreamsMutations;

/// Shared by every "user picked/pasted a specific release" mutation: create or
/// prepare the real target item only now, after the pick, mirroring
/// [`download_discovered_stream`]'s original branching so a manually-pasted
/// magnet/hash/NZB resolves a target exactly like a scraped one does.
///
/// For TV, the stream is matched against its parsed seasons (or the
/// caller-supplied `seasons` / `season_number`). A single-season pack links
/// to that season; a multi-season pack links to the **show** so the download
/// flow can fill every season it contains.
async fn resolve_manual_download_target(
    registry: &PluginRegistry,
    item_type: MediaItemType,
    title: &str,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    seasons: Option<&[i32]>,
    parsed_data: Option<&serde_json::Value>,
) -> Result<MediaItem> {
    if item_type == MediaItemType::Movie {
        ensure_download_target(
            registry, item_type, title, imdb_id, tmdb_id, tvdb_id, None, None,
        )
        .await
    } else if item_type == MediaItemType::Episode {
        ensure_download_target(
            registry,
            MediaItemType::Episode,
            title,
            imdb_id,
            tmdb_id,
            tvdb_id,
            season_number,
            episode_number,
        )
        .await
    } else {
        let pack_seasons = resolve_pack_seasons(parsed_data, seasons, season_number);
        match pack_seasons.as_slice() {
            [] => Err(async_graphql::Error::new("No season selected for download")),
            [single] => {
                ensure_download_target(
                    registry,
                    MediaItemType::Season,
                    title,
                    imdb_id,
                    tmdb_id,
                    tvdb_id,
                    Some(*single),
                    None,
                )
                .await
            }
            many => ensure_show_target(registry, title, imdb_id, tvdb_id, many).await,
        }
    }
}

#[Object]
impl StreamsMutations {
    /// Discover stream candidates without creating or mutating media items.
    async fn discover_streams(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        seasons: Option<Vec<i32>>,
        episode_number: Option<i32>,
        cached_only: Option<bool>,
    ) -> Result<Vec<DiscoveredStream>> {
        require(ctx, Capability::ScrapeItems)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;

        discover_streams(
            registry.as_ref(),
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            seasons.as_deref(),
            episode_number,
            cached_only.unwrap_or(false),
        )
        .await
    }

    /// Create or prepare the real target item only after the user picks a specific stream.
    ///
    /// For TV, the stream is matched against its parsed seasons (or the
    /// caller-supplied `seasons` / `season_number`). A single-season pack links
    /// to that season; a multi-season pack links to the **show** so the download
    /// flow can fill every season it contains.
    async fn download_discovered_stream(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        season_number: Option<i32>,
        episode_number: Option<i32>,
        seasons: Option<Vec<i32>>,
        info_hash: String,
        magnet: String,
        parsed_data: Option<serde_json::Value>,
        rank: Option<i64>,
    ) -> Result<String> {
        require(ctx, Capability::ScrapeItems)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let target = resolve_manual_download_target(
            registry.as_ref(),
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            season_number,
            episode_number,
            seasons.as_deref(),
            parsed_data.as_ref(),
        )
        .await?;

        let stream = repo::upsert_stream(&info_hash, &magnet, parsed_data, rank, None).await?;
        repo::link_stream_to_item(target.id, stream.id).await?;
        repo::mark_manual_scrape_only(target.id).await?;

        job_queue
            .push_rank_streams(RankStreamsJob {
                id: target.id,
                preferred_info_hash: Some(info_hash),
            })
            .await;

        Ok("Download queued".to_string())
    }

    /// Manually queue a specific NZB by URL, bypassing indexer search entirely
    /// — the usenet equivalent of [`download_discovered_stream`]'s explicit
    /// magnet/hash field. The URL is hashed into the same `nzb-`-prefixed
    /// `info_hash` a real newznab scrape would produce and stashed in Redis
    /// under the same keys ([`nzb_url_redis_key`]), so `plugin-usenet`'s
    /// download-time fetch can't tell the difference from a scraped result.
    ///
    /// TV has no parsed release metadata to infer seasons from here (there was
    /// no scrape), so the caller must supply `season_number`/`episode_number`
    /// or `seasons` explicitly.
    async fn download_explicit_nzb(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        title: String,
        imdb_id: Option<String>,
        tmdb_id: Option<String>,
        tvdb_id: Option<String>,
        season_number: Option<i32>,
        episode_number: Option<i32>,
        seasons: Option<Vec<i32>>,
        nzb_url: String,
    ) -> Result<String> {
        require(ctx, Capability::ScrapeItems)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;
        let redis_conn = ctx.data::<redis::aio::ConnectionManager>()?;
        let gql_port = ctx.data::<crate::schema::GqlPort>()?.0;

        let nzb_url = nzb_url.trim();
        validate_nzb_fetch_target(nzb_url, gql_port).await?;

        let target = resolve_manual_download_target(
            registry.as_ref(),
            item_type,
            &title,
            imdb_id.as_deref(),
            tmdb_id.as_deref(),
            tvdb_id.as_deref(),
            season_number,
            episode_number,
            seasons.as_deref(),
            None,
        )
        .await?;

        let info_hash = nzb_info_hash(nzb_url);
        let mut redis_conn = redis_conn.clone();
        // Longer than the 7-day TTL a real scrape's URL gets: this item is
        // also being marked `manual_scrape_only` below, which opts it out of
        // the automatic retry scheduler entirely — so if its own download
        // ever needs a delayed retry (escalating cooldown after a transient
        // failure, or the queue just being backed up) that pushes past seven
        // days, the URL would otherwise expire with no automatic path left to
        // recover it.
        redis_conn
            .set_ex::<_, _, ()>(
                nzb_url_redis_key(&info_hash),
                nzb_url,
                MANUAL_NZB_URL_TTL_SECS,
            )
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        redis_conn
            .set_ex::<_, _, ()>(
                nzb_indexer_redis_key(&info_hash),
                "manual",
                MANUAL_NZB_URL_TTL_SECS,
            )
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;

        let stream = repo::upsert_stream(&info_hash, "", None, None, None).await?;
        repo::link_stream_to_item(target.id, stream.id).await?;
        repo::mark_manual_scrape_only(target.id).await?;

        job_queue
            .push_rank_streams(RankStreamsJob {
                id: target.id,
                preferred_info_hash: Some(info_hash),
            })
            .await;

        Ok("Download queued".to_string())
    }

    /// Turns a manually-pasted magnet link/hash into a full [`DiscoveredStream`]
    /// card — same shape, same badges as a real scrape result — instead of
    /// downloading it sight-unseen. Parses whatever the magnet's own `dn=`
    /// carries through [`riven_rank::parse`] (the exact parser real scrape
    /// results go through) for resolution/quality/audio/etc., and checks
    /// debrid cache status via the same dispatch [`discover_streams`] uses.
    /// Creates or mutates nothing — the pick only becomes real when the
    /// resulting card's "Download This" calls `downloadDiscoveredStream`,
    /// same as any other result in the list.
    async fn preview_manual_magnet(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        info_hash: String,
        magnet: String,
        season_number: Option<i32>,
        episode_number: Option<i32>,
    ) -> Result<DiscoveredStream> {
        require(ctx, Capability::ScrapeItems)?;
        let registry = ctx.data::<Arc<PluginRegistry>>()?;

        let title = extract_magnet_display_name(&magnet).unwrap_or_else(|| info_hash.clone());
        let parsed = riven_rank::parse(&title);

        let mut stream = DiscoveredStream {
            key: format!("manual:{}", info_hash.to_lowercase()),
            title,
            magnet,
            info_hash,
            parsed_data: serde_json::to_value(&parsed).ok(),
            rank: None,
            file_size_bytes: None,
            is_cached: false,
            item_type,
            season_number,
            episode_number,
        };
        apply_cache_status(registry.as_ref(), std::slice::from_mut(&mut stream)).await;

        Ok(stream)
    }

    /// The NZB equivalent of [`preview_manual_magnet`]: fetches the URL
    /// (whether pasted directly or handed back by the upload endpoint),
    /// peeks its release title the same way `plugin-usenet` does at ingest
    /// time, and parses that through [`riven_rank::parse`]. Usenet has no
    /// debrid-style cache concept — every `nzb-` hash is unconditionally
    /// "cached" here, matching `plugin-usenet`'s own cache-check response.
    /// Nothing is persisted; the fetched content is discarded once peeked.
    async fn preview_manual_nzb(
        &self,
        ctx: &Context<'_>,
        item_type: MediaItemType,
        nzb_url: String,
        season_number: Option<i32>,
        episode_number: Option<i32>,
    ) -> Result<DiscoveredStream> {
        require(ctx, Capability::ScrapeItems)?;
        let http = ctx.data::<HttpClient>()?;
        let gql_port = ctx.data::<crate::schema::GqlPort>()?.0;

        let nzb_url = nzb_url.trim();
        let pinned_target = validate_nzb_fetch_target(nzb_url, gql_port).await?;
        let xml = fetch_capped_nzb_text(http, nzb_url, pinned_target).await?;

        // Full parse rather than the cheap `peek_release_title` used at
        // ingest time (log lines only): a preview also wants the total size,
        // which needs every file's segment list materialized anyway, and
        // this is a one-shot user action rather than a hot path.
        let document = riven_usenet::parse_nzb_document(&xml).ok();
        let title = document
            .as_ref()
            .and_then(riven_usenet::NzbDocument::release_title)
            .unwrap_or_else(|| riven_usenet::UNKNOWN_FILE_LABEL.to_string());
        let file_size_bytes = document.as_ref().map(|doc| {
            doc.files
                .iter()
                .map(|file| file.segments.total_bytes())
                .sum::<u64>() as i64
        });
        let parsed = riven_rank::parse(&title);
        let info_hash = nzb_info_hash(nzb_url);

        Ok(DiscoveredStream {
            key: format!("manual-nzb:{}", info_hash.to_lowercase()),
            title,
            magnet: String::new(),
            info_hash,
            parsed_data: serde_json::to_value(&parsed).ok(),
            rank: None,
            file_size_bytes,
            is_cached: true,
            item_type,
            season_number,
            episode_number,
        })
    }
}
