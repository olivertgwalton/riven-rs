//! Stremio addon endpoints, exposing the Riven library as a stream source.
//!
//! Riven is the addon here, not the consumer: you copy the manifest URL out of
//! Riven, paste it into Stremio, and items already in the library turn up in
//! Stremio's stream picker pointing back at the `/media/{entry_id}` bridge.
//!
//! Only the `stream` resource is advertised. Catalogs would mean re-publishing
//! the library as browsable rows, which Stremio already gets from Cinemeta —
//! the useful part is answering "do you have this?" for an ID Stremio already
//! resolved.

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header::HOST},
    response::{IntoResponse, Response},
};
use riven_core::types::MediaItemType;
use riven_db::entities::FileSystemEntry;
use serde_json::{Value, json};

use super::ApiState;
use super::auth::{check_stremio_token, has_valid_api_key, stremio_addon_token};

const ADDON_ID: &str = "com.rivenmedia.riven.library";
const ADDON_NAME: &str = "Riven Library";

/// Byte-serving path used in stream URLs. It's an alias of `/media/{entry_id}`
/// living under `/stremio` on purpose: everything Stremio touches — manifest,
/// stream lookups and the bytes themselves — then sits under one prefix, so a
/// single proxy rule covers the lot. `/media/{entry_id}` is unchanged and stays
/// the session-authenticated path the frontend uses.
const MEDIA_PATH_PREFIX: &str = "/stremio/media";

/// Containers Stremio's built-in HTML5 player can't handle, so playback has to
/// go through the desktop app's bundled player. Marking these `notWebReady`
/// lets Stremio pick the right path instead of failing on a black screen.
const NOT_WEB_READY_EXTENSIONS: [&str; 6] = ["mkv", "avi", "ts", "m2ts", "wmv", "flv"];

/// Reconstruct the externally-visible origin, preferring proxy headers since
/// Riven usually sits behind one. Stremio requires absolute stream URLs, and it
/// resolves them from its own network — so the host it used to reach us is the
/// only host we know also works for it.
fn public_base_url(headers: &HeaderMap) -> String {
    let first = |value: &str| value.split(',').next().unwrap_or(value).trim().to_string();

    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .map(first)
        .filter(|scheme| !scheme.is_empty())
        .unwrap_or_else(|| "http".to_string());

    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(HOST))
        .and_then(|value| value.to_str().ok())
        .map(first)
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| "localhost".to_string());

    format!("{scheme}://{host}")
}

/// Split a Stremio stream ID into its parts. Movies arrive as `tt0111161`,
/// series as `tt0903747:2:5`. A trailing `.json` is part of the route, not the
/// ID. Non-numeric season/episode segments are dropped rather than erroring —
/// an unparseable ID just means "nothing in the library matches".
fn parse_stream_id(raw: &str) -> Option<(String, Option<i32>, Option<i32>)> {
    let trimmed = raw.strip_suffix(".json").unwrap_or(raw);
    let mut parts = trimmed.split(':');
    let imdb_id = parts.next()?.trim();
    if imdb_id.is_empty() {
        return None;
    }
    let season = parts.next().and_then(|value| value.trim().parse().ok());
    let episode = parts.next().and_then(|value| value.trim().parse().ok());
    Some((imdb_id.to_string(), season, episode))
}

fn is_not_web_ready(filename: Option<&str>) -> bool {
    let Some(name) = filename else {
        // Unknown container: assume the desktop player is needed. Guessing
        // "web ready" and being wrong is a failed playback; guessing the other
        // way just routes through a player that handles both.
        return true;
    };
    name.rsplit_once('.')
        .map(|(_, extension)| {
            let extension = extension.to_ascii_lowercase();
            NOT_WEB_READY_EXTENSIONS.contains(&extension.as_str())
        })
        .unwrap_or(true)
}

fn format_size(bytes: i64) -> String {
    if bytes <= 0 {
        return "unknown size".to_string();
    }
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[0])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

/// Find the library's media entries for a Stremio ID.
///
/// Stremio addresses series episodes by the *show's* IMDB ID plus season and
/// episode, but Riven stores entries against the episode row — so a show lookup
/// has to descend show → season → episode before asking for entries. Movies and
/// directly-addressed rows resolve in one step.
async fn resolve_media_entries(
    kind: &str,
    imdb_id: &str,
    season: Option<i32>,
    episode: Option<i32>,
) -> anyhow::Result<Vec<FileSystemEntry>> {
    let Some(item) = riven_db::repo::get_media_item_by_imdb(imdb_id).await? else {
        return Ok(Vec::new());
    };

    // `get_media_item_by_imdb` filters on the ID alone, so refuse to answer a
    // `movie` request with a series row (or vice versa) rather than serving a
    // confidently wrong file.
    let kind_matches = match kind {
        "movie" => item.item_type == MediaItemType::Movie,
        "series" => matches!(
            item.item_type,
            MediaItemType::Show | MediaItemType::Season | MediaItemType::Episode
        ),
        _ => false,
    };
    if !kind_matches {
        tracing::debug!(
            imdb_id,
            kind,
            item_type = ?item.item_type,
            "stremio request kind does not match the library item type"
        );
        return Ok(Vec::new());
    }

    match item.item_type {
        MediaItemType::Movie => Ok(riven_db::repo::get_media_entries(item.id).await?),
        MediaItemType::Episode => {
            // An episode row reached by the show's ID would be a data oddity;
            // only serve it when its own numbering agrees with the request.
            let mismatched = matches!((season, item.season_number), (Some(a), Some(b)) if a != b)
                || matches!((episode, item.episode_number), (Some(a), Some(b)) if a != b);
            if mismatched {
                return Ok(Vec::new());
            }
            Ok(riven_db::repo::get_media_entries(item.id).await?)
        }
        MediaItemType::Season => Ok(riven_db::repo::get_media_entries_recursive(item.id).await?),
        MediaItemType::Show => {
            let (Some(season_number), Some(episode_number)) = (season, episode) else {
                // A bare show ID has no single playable file. Stremio only asks
                // this way for the show page itself, where streams aren't shown.
                return Ok(Vec::new());
            };
            let seasons = riven_db::repo::list_seasons(item.id).await?;
            let Some(season_row) = seasons
                .into_iter()
                .find(|row| row.season_number == Some(season_number))
            else {
                return Ok(Vec::new());
            };
            let episodes = riven_db::repo::list_episodes(season_row.id).await?;
            let Some(episode_row) = episodes
                .into_iter()
                .find(|row| row.episode_number == Some(episode_number))
            else {
                return Ok(Vec::new());
            };
            Ok(riven_db::repo::get_media_entries(episode_row.id).await?)
        }
    }
}

/// Render one library entry as a Stremio stream object.
fn stream_object(entry: &FileSystemEntry, base_url: &str, token: &str) -> Value {
    let filename = entry
        .original_filename
        .as_deref()
        .or_else(|| entry.path.rsplit('/').next())
        .unwrap_or("unknown");

    // video_file_size is the playable file; file_size can cover the whole
    // release, so it's only a fallback for the size shown to the user.
    let size = entry.video_file_size.unwrap_or(entry.file_size);

    let mut detail = format_size(size);
    if let Some(resolution) = entry.resolution.as_deref().filter(|r| !r.is_empty()) {
        detail = format!("{resolution} • {detail}");
    }

    let url = if token.is_empty() {
        format!("{base_url}{MEDIA_PATH_PREFIX}/{}", entry.id)
    } else {
        format!("{base_url}{MEDIA_PATH_PREFIX}/{}?token={token}", entry.id)
    };

    let mut behavior_hints = json!({
        "filename": filename,
        "notWebReady": is_not_web_ready(Some(filename)),
    });
    if size > 0 {
        behavior_hints["videoSize"] = json!(size);
    }
    if let Some(resolution) = entry.resolution.as_deref().filter(|r| !r.is_empty()) {
        // Keeps Stremio's "next episode" autoplay on a consistent quality.
        behavior_hints["bingeGroup"] = json!(format!("riven-{resolution}"));
    }

    json!({
        "name": ADDON_NAME,
        "description": format!("{filename}\n{detail}"),
        "url": url,
        "behaviorHints": behavior_hints,
    })
}

pub(super) async fn manifest_handler(
    State(state): State<ApiState>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !check_stremio_token(&state, &token) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    let _ = headers;

    Json(json!({
        "id": ADDON_ID,
        "version": env!("CARGO_PKG_VERSION"),
        "name": ADDON_NAME,
        "description": "Play items from your Riven library directly in Stremio.",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": [],
        "behaviorHints": { "configurable": false },
    }))
    .into_response()
}

pub(super) async fn stream_handler(
    State(state): State<ApiState>,
    Path((token, kind, id)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    if !check_stremio_token(&state, &token) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    if kind != "movie" && kind != "series" {
        return Json(json!({ "streams": [] })).into_response();
    }

    let Some((imdb_id, season, episode)) = parse_stream_id(&id) else {
        return Json(json!({ "streams": [] })).into_response();
    };

    let entries = match resolve_media_entries(&kind, &imdb_id, season, episode).await {
        Ok(entries) => entries,
        Err(error) => {
            tracing::error!(
                imdb_id = %imdb_id,
                season,
                episode,
                error = %error,
                "stremio stream lookup failed"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let base_url = public_base_url(&headers);
    let streams: Vec<Value> = entries
        .iter()
        .map(|entry| stream_object(entry, &base_url, &token))
        .collect();

    tracing::debug!(
        imdb_id = %imdb_id,
        season,
        episode,
        count = streams.len(),
        "stremio stream request served"
    );

    Json(json!({ "streams": streams })).into_response()
}

/// The manifest URL to paste into Stremio, for the frontend's copy button.
/// Authenticated the normal way (header or `?api_key=`) — this is the endpoint
/// that *hands out* the addon token, so it must not accept it as a credential.
pub(super) async fn manifest_url_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    raw_query: axum::extract::RawQuery,
) -> Response {
    if !has_valid_api_key(&state, &headers, raw_query.0.as_deref()) {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let base_url = public_base_url(&headers);
    let token = stremio_addon_token(&state).unwrap_or_default();
    let manifest_url = if token.is_empty() {
        format!("{base_url}/stremio/manifest.json")
    } else {
        format!("{base_url}/stremio/{token}/manifest.json")
    };

    Json(json!({
        "manifestUrl": manifest_url,
        // Same URL under Stremio's protocol handler: opening it installs the
        // addon directly instead of making the user paste into the Addons page.
        "installUrl": manifest_url.replacen("https://", "stremio://", 1).replacen("http://", "stremio://", 1),
        "authRequired": !token.is_empty(),
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use riven_core::types::FileSystemEntryType;

    fn entry(filename: &str, size: i64, resolution: Option<&str>) -> FileSystemEntry {
        FileSystemEntry {
            id: 42,
            file_size: size,
            created_at: chrono::Utc::now(),
            updated_at: None,
            media_item_id: 7,
            entry_type: FileSystemEntryType::Media,
            path: format!("/movies/Some Movie (2023)/{filename}"),
            original_filename: Some(filename.to_string()),
            download_url: None,
            stream_url: None,
            plugin: None,
            provider: None,
            provider_download_id: None,
            library_profiles: None,
            media_metadata: None,
            language: None,
            parent_original_filename: None,
            subtitle_content: None,
            file_hash: None,
            video_file_size: None,
            opensubtitles_id: None,
            stream_id: None,
            resolution: resolution.map(str::to_string),
            ranking_profile_name: None,
            source_provider: None,
            source_id: None,
            usenet_info_hash: None,
            usenet_file_index: None,
        }
    }

    #[test]
    fn stream_object_points_at_the_media_bridge_with_the_addon_token() {
        let value = stream_object(
            &entry("Movie.2023.2160p.BluRay.mkv", 8_589_934_592, Some("2160p")),
            "https://riven.example.uk",
            "deadbeef",
        );

        assert_eq!(
            value["url"],
            json!("https://riven.example.uk/stremio/media/42?token=deadbeef")
        );
        assert_eq!(value["behaviorHints"]["videoSize"], json!(8_589_934_592i64));
        assert_eq!(value["behaviorHints"]["notWebReady"], json!(true));
        assert_eq!(value["behaviorHints"]["bingeGroup"], json!("riven-2160p"));
        assert_eq!(
            value["description"],
            json!("Movie.2023.2160p.BluRay.mkv\n2160p • 8.00 GiB")
        );
    }

    #[test]
    fn stream_object_omits_the_token_when_auth_is_disabled() {
        let value = stream_object(&entry("Movie.mp4", 1024, None), "http://localhost:8080", "");
        assert_eq!(
            value["url"],
            json!("http://localhost:8080/stremio/media/42")
        );
        assert_eq!(value["behaviorHints"]["notWebReady"], json!(false));
        // No resolution recorded means no binge group to pin quality to.
        assert_eq!(value["behaviorHints"].get("bingeGroup"), None);
    }

    #[test]
    fn stream_object_prefers_video_file_size_over_whole_release_size() {
        let mut model = entry("Movie.mkv", 90_000_000_000, None);
        model.video_file_size = Some(1024);
        let value = stream_object(&model, "http://x", "t");
        assert_eq!(value["behaviorHints"]["videoSize"], json!(1024));
    }

    #[test]
    fn parses_movie_and_series_ids() {
        assert_eq!(
            parse_stream_id("tt0111161.json"),
            Some(("tt0111161".to_string(), None, None))
        );
        assert_eq!(
            parse_stream_id("tt0903747:2:5.json"),
            Some(("tt0903747".to_string(), Some(2), Some(5)))
        );
        assert_eq!(
            parse_stream_id("tt0903747:2:5"),
            Some(("tt0903747".to_string(), Some(2), Some(5)))
        );
    }

    #[test]
    fn rejects_empty_id_and_tolerates_junk_segments() {
        assert_eq!(parse_stream_id(".json"), None);
        assert_eq!(
            parse_stream_id("tt1:abc:5"),
            Some(("tt1".to_string(), None, Some(5)))
        );
    }

    #[test]
    fn mkv_needs_the_desktop_player_but_mp4_does_not() {
        assert!(is_not_web_ready(Some("Movie.2023.2160p.mkv")));
        assert!(!is_not_web_ready(Some("Movie.2023.1080p.mp4")));
        assert!(
            is_not_web_ready(Some("Movie.2023.MKV")),
            "extension match is case-insensitive"
        );
        // Unknown container errs toward the player that handles everything.
        assert!(is_not_web_ready(None));
        assert!(is_not_web_ready(Some("no-extension")));
    }

    #[test]
    fn base_url_prefers_proxy_headers_and_takes_first_hop() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "internal:8080".parse().unwrap());
        assert_eq!(public_base_url(&headers), "http://internal:8080");

        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert("x-forwarded-host", "riven.example.uk".parse().unwrap());
        assert_eq!(public_base_url(&headers), "https://riven.example.uk");

        headers.insert("x-forwarded-proto", "https, http".parse().unwrap());
        assert_eq!(public_base_url(&headers), "https://riven.example.uk");
    }

    #[test]
    fn formats_sizes_in_binary_units() {
        assert_eq!(format_size(0), "unknown size");
        assert_eq!(format_size(-1), "unknown size");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1024 * 1024 * 3), "3.00 MiB");
    }
}
