use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

use apalis_board_api::ui::ServeUI;
use riven_core::auth::Capability;

use super::ApiState;
use super::auth::{AuthError, authorize_request};

/// Admin gate for the apalis board.
///
/// The board crate ships no authentication of its own, and both halves of it are
/// privileged: `/api/v1` reads every queued job's payload — download URLs,
/// provider identifiers, the lot — and `PUT /api/v1/queues/{queue}/tasks`
/// (`push_task`) deserialises a request body straight into a job and enqueues
/// it. Unguarded, that let an anonymous caller drive scrapes and downloads on
/// the instance's debrid account and usenet providers.
///
/// CORS was never a defence here: it constrains browser JS, not a plain HTTP
/// client.
///
/// The bar is `ManageSettings` — admin — because operating the queues is an
/// operator action, not a library one. `authorize_request` accepts a session or
/// the configured API key, so the board UI works from an admin's browser (its
/// `fetch` calls are same-origin and carry the cookie) and scripts can still use
/// the API key.
pub(super) async fn require_board_admin(
    State(state): State<ApiState>,
    request: Request,
    next: Next,
) -> Response {
    let (parts, body) = request.into_parts();

    let auth = match authorize_request(&state, &parts.headers, parts.uri.query()).await {
        Ok(auth) => auth,
        Err(AuthError::Unauthorized) => {
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
        Err(AuthError::Forbidden) => return (StatusCode::FORBIDDEN, "Forbidden").into_response(),
    };

    if !Capability::ManageSettings.granted_to(auth.role) {
        tracing::warn!(
            role = ?auth.role,
            path = %parts.uri.path(),
            "board access refused: caller is not an admin"
        );
        return (StatusCode::FORBIDDEN, "Forbidden").into_response();
    }

    next.run(Request::from_parts(parts, body)).await
}

/// Serves the board UI's own bundle from the root path.
///
/// Global rather than scoped under `/board` because the embedded `index.html`
/// hardcodes `<base href="/" />` and absolute asset paths
/// (`/apalis-board-web-*.js`, `/input-*.css`, `/*_bg.wasm`), so the browser asks
/// for them at the root regardless of where the UI is mounted.
///
/// Deliberately *not* behind [`require_board_admin`]: these are four inert,
/// publicly-published files from an open-source crate and carry no instance
/// data. Everything that does — the queues, the tasks, `push_task` — is under
/// `/api/v1`, which is guarded. `ServeUI::get_file` only ever matches the
/// embedded set, so this cannot be turned into an arbitrary-file read.
pub(super) async fn board_assets_middleware(
    uri: axum::http::Uri,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = uri.path();
    if path.contains('.')
        && let Some(file) = ServeUI::get_file(path)
    {
        let bytes = file.contents().to_vec();
        let content_type = ServeUI::content_type(path);
        let mut builder = axum::http::Response::builder()
            .status(200)
            .header("content-type", content_type);
        if let Some(cc) = ServeUI::cache_control(path) {
            builder = builder.header("cache-control", cc);
        }
        return builder
            .body(axum::body::Body::from(bytes))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }
    next.run(req).await
}
