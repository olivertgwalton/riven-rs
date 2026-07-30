use axum::http::HeaderMap;
use chrono::Utc;

use better_auth::prelude::{AuthSession, AuthUser};

use crate::schema::auth::{RequestAuth, UserRole};

use super::ApiState;

/// `query` is the raw request query string (e.g. from `Uri::query()`), checked
/// for `api_key=...` when no header credential is present. This exists for
/// callers that can't set custom headers — notably Overseerr/Jellyseerr's
/// webhook notification agent, which only exposes a URL and a JSON payload
/// template, so its webhook to `SeerrMutations::seerr_handle_webhook` can only
/// authenticate via a token embedded in the URL itself.
pub(super) fn check_api_key(state: &ApiState, headers: &HeaderMap, query: Option<&str>) -> bool {
    let Some(ref expected) = state.api_key else {
        return true;
    };
    if expected.is_empty() {
        return true;
    }
    let header_value = headers
        .get("x-api-key")
        .or_else(|| headers.get("authorization"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_start_matches("Bearer ").trim());
    if header_value == Some(expected.as_str()) {
        return true;
    }
    let query_value = query.and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes())
            .find(|(key, _)| key == "api_key")
            .map(|(_, value)| value.into_owned())
    });
    query_value.as_deref() == Some(expected.as_str())
}

/// The addon token for this instance, or `None` when no API key is configured.
/// Derivation lives in `riven_core::stremio` so the settings schema and this
/// HTTP layer can never disagree about the value.
pub(super) fn stremio_addon_token(state: &ApiState) -> Option<String> {
    riven_core::stremio::addon_token(state.api_key.as_deref().unwrap_or_default())
}

/// Verify a token from a Stremio addon URL. Returns `true` when no API key is
/// configured, matching `check_api_key`'s open-by-default behaviour.
pub(super) fn check_stremio_token(state: &ApiState, token: &str) -> bool {
    riven_core::stremio::verify_addon_token(state.api_key.as_deref().unwrap_or_default(), token)
}

pub(super) enum AuthError {
    Unauthorized,
    Forbidden,
}

/// Resolve the caller's role.
///
/// Order matters. A `better-auth` session is the primary credential: it is
/// verified here, against this process's own store, so the role it yields is one
/// riven established rather than one it was told. The API key is the fallback,
/// for machine callers that have no session.
///
/// This replaced a scheme where the SvelteKit frontend signed `x-riven-user-role`
/// with a shared HMAC secret and riven trusted it. That put the trust boundary in
/// a Node process which also proxied media, and made "who is an admin" a claim
/// rather than a lookup — anyone holding the signing secret could mint any role.
pub(super) async fn authorize_request(
    state: &ApiState,
    headers: &HeaderMap,
    query: Option<&str>,
) -> Result<RequestAuth, AuthError> {
    if let Some(role) = session_role(state, headers).await? {
        return Ok(RequestAuth { role });
    }

    // No session. Fall back to the API key, which also covers the
    // "no API key configured" case where the instance is deliberately open.
    if check_api_key(state, headers, query) {
        return Ok(RequestAuth::trusted_api_key());
    }

    tracing::warn!("auth rejected: no valid session and no matching api key");
    Err(AuthError::Unauthorized)
}

/// The session token, from `Authorization: Bearer` or the session cookie —
/// matching what better-auth's own extractors accept, so a caller authenticates
/// the same way whether it hits riven's routes or better-auth's.
fn session_token(headers: &HeaderMap, cookie_name: &str) -> Option<String> {
    if let Some(token) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| !token.is_empty())
    {
        return Some(token.to_string());
    }

    let cookies = headers
        .get(axum::http::header::COOKIE)
        .and_then(|value| value.to_str().ok())?;
    let prefix = format!("{cookie_name}=");
    cookies
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix(&prefix))
        .filter(|token| !token.is_empty())
        .map(str::to_string)
}

/// `Ok(None)` means "no session was presented" — the caller should fall back to
/// the API key. An *invalid* session is `Err(Unauthorized)` rather than a
/// fallback, so a stale cookie can't silently escalate to the API key's admin
/// role on an instance where the key is unset.
async fn session_role(
    state: &ApiState,
    headers: &HeaderMap,
) -> Result<Option<UserRole>, AuthError> {
    let cookie_name = &state.auth.config().session.cookie_name;
    let Some(token) = session_token(headers, cookie_name) else {
        return Ok(None);
    };

    let store = state.auth.store();
    let session = store
        .get_session(&token)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "session lookup failed");
            AuthError::Unauthorized
        })?
        .ok_or(AuthError::Unauthorized)?;

    if session.expires_at() <= Utc::now() {
        tracing::debug!("auth rejected: session expired");
        return Err(AuthError::Unauthorized);
    }

    let user = store
        .get_user_by_id(&session.user_id())
        .await
        .map_err(|error| {
            tracing::warn!(%error, "user lookup failed");
            AuthError::Unauthorized
        })?
        .ok_or(AuthError::Unauthorized)?;

    // A ban with no expiry is permanent; one in the past has lapsed.
    let banned = user.banned() && user.ban_expires().is_none_or(|until| until > Utc::now());
    if banned {
        tracing::warn!(user_id = %user.id(), "auth rejected: user is banned");
        return Err(AuthError::Forbidden);
    }

    Ok(Some(role_from_user(user.role())))
}

/// Map better-auth's free-text admin-plugin role onto riven's ladder.
///
/// Unrecognised and absent roles both land on `User`, the least privilege —
/// a typo in the column must not become an escalation.
fn role_from_user(role: Option<&str>) -> UserRole {
    match role.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some("admin") => UserRole::Admin,
        Some("manager") => UserRole::Manager,
        _ => UserRole::User,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(name: &str, value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
        headers
    }

    #[test]
    fn a_bearer_token_wins_over_the_cookie() {
        let mut headers = headers_with("authorization", "Bearer from-header");
        headers.insert(
            axum::http::header::COOKIE,
            "riven.session_token=from-cookie".parse().unwrap(),
        );
        assert_eq!(
            session_token(&headers, "riven.session_token").as_deref(),
            Some("from-header")
        );
    }

    #[test]
    fn the_session_cookie_is_found_among_others() {
        let headers = headers_with("cookie", "theme=dark; riven.session_token=abc123; other=1");
        assert_eq!(
            session_token(&headers, "riven.session_token").as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn a_differently_named_cookie_is_not_mistaken_for_the_session() {
        let headers = headers_with("cookie", "not_riven.session_token=abc123");
        assert_eq!(session_token(&headers, "riven.session_token"), None);
        assert_eq!(
            session_token(&HeaderMap::new(), "riven.session_token"),
            None
        );
    }

    #[test]
    fn empty_credentials_are_treated_as_absent() {
        assert_eq!(
            session_token(&headers_with("authorization", "Bearer "), "sess"),
            None
        );
        assert_eq!(
            session_token(&headers_with("cookie", "sess="), "sess"),
            None
        );
    }

    /// An unknown role must not inherit privilege — the escalation this guards
    /// against is a typo or a role added by a future better-auth plugin.
    #[test]
    fn roles_map_to_the_ladder_and_default_to_least_privilege() {
        assert_eq!(role_from_user(Some("admin")), UserRole::Admin);
        assert_eq!(role_from_user(Some("ADMIN")), UserRole::Admin);
        assert_eq!(role_from_user(Some(" manager ")), UserRole::Manager);
        assert_eq!(role_from_user(Some("user")), UserRole::User);
        assert_eq!(role_from_user(Some("superadmin")), UserRole::User);
        assert_eq!(role_from_user(Some("")), UserRole::User);
        assert_eq!(role_from_user(None), UserRole::User);
    }
}
