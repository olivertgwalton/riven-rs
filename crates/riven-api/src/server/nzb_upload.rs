//! Manual Scrape's "upload an NZB file" entry point.
//!
//! Accepts a raw `.nzb` file, stages it under
//! [`riven_core::nzb::NZB_UPLOAD_DIR`], and hands back a loopback URL that
//! slots straight into the existing `downloadExplicitNzb` mutation — nothing
//! downstream of that mutation needs to know the URL came from an upload
//! rather than a pasted link. See `riven_core::nzb` for the storage/cleanup
//! contract and why the URL is loopback rather than the instance's public
//! one.
//!
//! Safety notes, since this is the one place in the API that accepts and
//! persists an arbitrary file body:
//! - Capped at [`MAX_UPLOAD_BYTES`] via a route-scoped `DefaultBodyLimit`, so
//!   a caller can't fill the container's `/tmp` by uploading something huge.
//! - Content is sniffed for a plausible XML/NZB prefix before it's written to
//!   disk at all — rejects arbitrary binaries dressed up as `.nzb`.
//! - The on-disk filename is always a fresh server-generated UUID, never
//!   anything derived from the client's filename or content — no path ever
//!   touches user input.
//! - Requires the same `ScrapeItems` capability as every other Manual Scrape
//!   mutation, checked the same way GraphQL's `require()` does.

use axum::Json;
use axum::extract::{Multipart, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::schema::auth::{Capability, RequestAuth};

use super::ApiState;
use super::auth::authorize_request;

/// Generous headroom over any real NZB (typically low hundreds of KB even for
/// large multi-file season packs) while still bounding worst-case disk use
/// per upload.
pub(super) const MAX_UPLOAD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Serialize)]
struct UploadResponse {
    url: String,
}

pub(super) async fn upload_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Response {
    let auth = match authorize_request(&state, &headers, None).await {
        Ok(auth) => auth,
        Err(_) => return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response(),
    };
    if !capability_granted(&auth) {
        return (StatusCode::FORBIDDEN, "Forbidden").into_response();
    }

    let field = match multipart.next_field().await {
        Ok(Some(field)) => field,
        Ok(None) => return (StatusCode::BAD_REQUEST, "No file field in upload").into_response(),
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };

    let bytes = match field.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };

    // Belt-and-suspenders: the route-scoped `DefaultBodyLimit` should already
    // have refused an oversized request before the body was even fully read,
    // but a future refactor that drops that layer must not silently turn this
    // into an unbounded write.
    if bytes.len() > MAX_UPLOAD_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, "NZB file too large").into_response();
    }

    if !looks_like_nzb(&bytes) {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            "File does not look like a valid NZB (expected XML starting with <?xml or <nzb)",
        )
            .into_response();
    }

    match riven_core::nzb::store_nzb_upload(&bytes, state.gql_port).await {
        Ok(url) => Json(UploadResponse { url }).into_response(),
        Err(error) => {
            tracing::warn!(%error, "failed to store NZB upload");
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to store upload").into_response()
        }
    }
}

fn capability_granted(auth: &RequestAuth) -> bool {
    Capability::ScrapeItems.granted_to(auth.role)
}

/// Cheap content sniff, not a real NZB grammar check — `riven_usenet`'s own
/// parser is what actually validates structure at ingest time. This exists
/// only to reject obviously-wrong uploads (images, random binaries) before
/// they're written to disk at all.
///
/// Works on raw bytes rather than decoding to `&str` first: upload content
/// isn't guaranteed valid UTF-8, and `<?xml`/`<nzb` are pure ASCII anyway, so
/// a byte-level prefix check needs no decoding (and none of the panic risk a
/// fixed-byte-count slice of a `&str` would carry on a multi-byte boundary).
fn looks_like_nzb(bytes: &[u8]) -> bool {
    let mut head = bytes;
    if let Some(rest) = head.strip_prefix(b"\xEF\xBB\xBF") {
        head = rest; // UTF-8 BOM
    }
    let head = head.trim_ascii_start();
    (head.len() >= 5 && head[..5].eq_ignore_ascii_case(b"<?xml"))
        || (head.len() >= 4 && head[..4].eq_ignore_ascii_case(b"<nzb"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_real_nzb_shapes() {
        assert!(looks_like_nzb(b"<?xml version=\"1.0\"?><nzb>...</nzb>"));
        assert!(looks_like_nzb(b"<nzb xmlns=\"...\">"));
        assert!(looks_like_nzb(b"  \n\t<?xml version=\"1.0\"?>")); // leading whitespace
        assert!(looks_like_nzb(b"\xEF\xBB\xBF<?xml version=\"1.0\"?>")); // BOM
        assert!(looks_like_nzb(b"<?XML VERSION=\"1.0\"?>")); // case-insensitive
    }

    #[test]
    fn rejects_non_nzb_content() {
        assert!(!looks_like_nzb(b""));
        assert!(!looks_like_nzb(b"\x89PNG\r\n\x1a\n")); // PNG magic bytes
        assert!(!looks_like_nzb(b"just some text"));
        assert!(!looks_like_nzb(b"<html><body>not an nzb</body></html>"));
        // Short enough that a naive fixed-length slice would panic rather
        // than correctly report "doesn't match".
        assert!(!looks_like_nzb(b"<"));
        assert!(!looks_like_nzb(b"<?xm"));
    }

    /// Arbitrary bytes that aren't valid UTF-8 must not panic the sniff —
    /// this runs on unauthenticated-shape (any bytes a client sends) input
    /// before any validation has happened.
    #[test]
    fn handles_invalid_utf8_without_panicking() {
        assert!(!looks_like_nzb(&[0xff, 0xfe, 0x00, 0x01, 0x02]));
    }
}
