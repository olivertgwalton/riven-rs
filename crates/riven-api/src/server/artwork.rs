//! Artwork proxy for media-server images.
//!
//! Plex, Emby and Jellyfin all authenticate image requests with the same
//! credential they use for everything else — a Plex server token, an Emby API
//! key — and all three accept it in the query string. The plugins used to take
//! that shortcut, building `…/thumb?X-Plex-Token=…` straight into
//! [`ActivePlaybackSession::image_url`].
//!
//! That put an administrative credential for the media server into the response
//! of `activePlaybackSessions`, a query every authenticated riven user can run,
//! and then — because the frontend renders the value as an `<img src>` — into
//! the DOM, the browser cache, and the `Referer` of whatever the dashboard
//! loaded next.
//!
//! So the credential stays here. The plugins emit a riven-relative
//! [`artwork_path`], this route resolves it by asking the plugin that minted it
//! to do the fetch, and the browser only ever sees riven's own origin and the
//! image bytes.
//!
//! [`ActivePlaybackSession::image_url`]: riven_core::types::ActivePlaybackSession::image_url
//! [`artwork_path`]: riven_core::types::artwork_path

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use riven_core::events::{HookResponse, RivenEvent};
use riven_core::plugin::PluginRegistry;
use serde::Deserialize;

use super::ApiState;
use super::auth::{AuthError, authorize_request};

#[derive(Deserialize)]
pub(super) struct ArtworkQuery {
    /// The plugin's own handle for the image, opaque here. Validated by the
    /// plugin that receives it — only it knows what a legitimate one looks like.
    #[serde(rename = "ref")]
    reference: String,
}

/// Artwork is immutable for a given reference (Plex and Emby both mint a new
/// one when the image changes), so it can be cached hard. `private` because the
/// response is session-authenticated and must not land in a shared proxy.
const ARTWORK_CACHE_CONTROL: &str = "private, max-age=86400";

pub(super) async fn artwork_handler(
    State(state): State<ApiState>,
    Path(server): Path<String>,
    Query(query): Query<ArtworkQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    // Any authenticated caller. This is artwork for a playback session they can
    // already see through `activePlaybackSessions`; the point of the route is
    // that the *credential* stays server-side, not that the picture is secret.
    // Anonymous is still refused — the fetch it triggers is made with the media
    // server's admin token, so it must not be reachable without a session.
    if let Err(error) = authorize_request(&state, &headers, None).await {
        return match error {
            AuthError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized").into_response(),
            AuthError::Forbidden => (StatusCode::FORBIDDEN, "Forbidden").into_response(),
        };
    }

    fetch(&state.registry, &server, &query.reference).await
}

async fn fetch(registry: &Arc<PluginRegistry>, server: &str, reference: &str) -> Response {
    let results = registry
        .dispatch(&RivenEvent::ArtworkRequested {
            server: server.to_string(),
            reference: reference.to_string(),
        })
        .await;

    for (plugin, result) in results {
        match result {
            Ok(HookResponse::Artwork(artwork)) => {
                let content_type = HeaderValue::from_str(&artwork.content_type)
                    .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
                return (
                    [
                        (axum::http::header::CONTENT_TYPE, content_type),
                        (
                            CACHE_CONTROL,
                            HeaderValue::from_static(ARTWORK_CACHE_CONTROL),
                        ),
                    ],
                    artwork.bytes,
                )
                    .into_response();
            }
            // The plugin declined — it is not the one that minted this path.
            Ok(_) => {}
            Err(error) => {
                // Logged, not returned: the message can name the media server's
                // internal URL, and the caller only needs to know it failed.
                tracing::warn!(plugin, server, %error, "artwork fetch failed");
            }
        }
    }

    StatusCode::NOT_FOUND.into_response()
}
