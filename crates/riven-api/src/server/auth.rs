use axum::http::HeaderMap;
use chrono::Utc;

use better_auth::prelude::{AuthSession, AuthUser};

use crate::schema::auth::{RequestAuth, UserRole};

use super::ApiState;

/// Whether the caller presented the configured API key.
///
/// **An unconfigured key is not a valid key.** This used to return `true` when
/// `api_key` was unset, which read as "no auth required" — and since the only
/// caller granted [`RequestAuth::trusted_api_key`] on the strength of it, an
/// instance that had simply not set a key handed every anonymous request full
/// admin over GraphQL. Absence of a credential is not proof of one.
///
/// `query` is the raw request query string (e.g. from `Uri::query()`), checked
/// for `api_key=...` when no header credential is present. This exists for
/// callers that can't set custom headers — notably Overseerr/Jellyseerr's
/// webhook notification agent, which only exposes a URL and a JSON payload
/// template, so its webhook to `SeerrMutations::seerr_handle_webhook` can only
/// authenticate via a token embedded in the URL itself.
pub(super) fn has_valid_api_key(
    state: &ApiState,
    headers: &HeaderMap,
    query: Option<&str>,
) -> bool {
    api_key_matches(state.api_key.as_deref(), headers, query)
}

/// The decision itself, split from [`ApiState`] so it can be tested without
/// standing up a schema, a job queue and a database.
fn api_key_matches(configured: Option<&str>, headers: &HeaderMap, query: Option<&str>) -> bool {
    let Some(expected) = configured.filter(|key| !key.is_empty()) else {
        return false;
    };
    let header_value = headers
        .get("x-api-key")
        .or_else(|| headers.get("authorization"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_start_matches("Bearer ").trim());
    if header_value == Some(expected) {
        return true;
    }
    let query_value = query.and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes())
            .find(|(key, _)| key == "api_key")
            .map(|(_, value)| value.into_owned())
    });
    query_value.as_deref() == Some(expected)
}

/// The addon token for this instance, or `None` when no API key is configured.
/// Derivation lives in `riven_core::stremio` so the settings schema and this
/// HTTP layer can never disagree about the value.
pub(super) fn stremio_addon_token(state: &ApiState) -> Option<String> {
    riven_core::stremio::addon_token(state.api_key.as_deref().unwrap_or_default())
}

/// Verify a token from a Stremio addon URL.
///
/// Without an API key there is no token to verify against — `addon_token`
/// cannot mint one either — so the answer is `false` rather than "anything
/// goes". These URLs carry no cookie, so the token is the only thing standing
/// between the open internet and the instance's media.
pub(super) fn check_stremio_token(state: &ApiState, token: &str) -> bool {
    stremio_token_matches(state.api_key.as_deref(), token)
}

fn stremio_token_matches(configured: Option<&str>, token: &str) -> bool {
    let Some(api_key) = configured.filter(|key| !key.is_empty()) else {
        return false;
    };
    riven_core::stremio::verify_addon_token(api_key, token)
}

pub(super) enum AuthError {
    Unauthorized,
    Forbidden,
}

/// Resolve the caller's role.
///
/// Order matters. A `better-auth` session is the primary credential: it is
/// verified here, against this process's own store, so the role it yields is one
/// riven established rather than one it was told. A configured API key is the
/// fallback, for machine callers that have no session.
///
/// There is no third case. An anonymous caller is rejected — including on an
/// instance with no API key set, which used to be treated as "auth disabled" and
/// granted admin. Nothing needs that hole: sign-in and first-user sign-up are on
/// `/auth`, not GraphQL, so a fresh install can still bootstrap itself.
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

    if has_valid_api_key(state, headers, query) {
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

    /// The hole this closed: an instance that had never set an API key treated
    /// every anonymous caller as holding one, and the only caller of this
    /// granted admin on the strength of it.
    #[test]
    fn an_unconfigured_api_key_matches_nothing() {
        let headers = headers_with("x-api-key", "anything");
        assert!(!api_key_matches(None, &headers, None));
        assert!(!api_key_matches(Some(""), &headers, None));
        assert!(!api_key_matches(None, &HeaderMap::new(), None));
        assert!(!api_key_matches(
            None,
            &HeaderMap::new(),
            Some("api_key=anything")
        ));

        // Same for the Stremio addon token, whose URLs carry no cookie at all.
        assert!(!stremio_token_matches(None, "any-token"));
        assert!(!stremio_token_matches(Some(""), ""));
    }

    #[test]
    fn a_configured_api_key_matches_header_or_query() {
        const KEY: &str = "s3cret";
        assert!(api_key_matches(
            Some(KEY),
            &headers_with("x-api-key", KEY),
            None
        ));
        assert!(api_key_matches(
            Some(KEY),
            &headers_with("authorization", &format!("Bearer {KEY}")),
            None
        ));
        assert!(api_key_matches(
            Some(KEY),
            &HeaderMap::new(),
            Some("api_key=s3cret")
        ));
        assert!(!api_key_matches(
            Some(KEY),
            &headers_with("x-api-key", "wrong"),
            None
        ));
        assert!(!api_key_matches(Some(KEY), &HeaderMap::new(), None));
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
