use axum::http::HeaderMap;
use subtle::ConstantTimeEq;

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
    if header_value.is_some_and(|value| secret_eq(value, expected)) {
        return true;
    }
    let query_value = query.and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes())
            .find(|(key, _)| key == "api_key")
            .map(|(_, value)| value.into_owned())
    });
    query_value.is_some_and(|value| secret_eq(&value, expected))
}

/// Constant-time comparison, matching how `legacy_password` compares hashes.
///
/// The lengths are compared first and in variable time, which leaks only the
/// length of the configured key — `ct_eq` requires equal-length inputs anyway.
fn secret_eq(candidate: &str, expected: &str) -> bool {
    candidate.len() == expected.len() && bool::from(candidate.as_bytes().ct_eq(expected.as_bytes()))
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

pub(super) struct Unauthorized;

/// Resolve the caller's role.
///
/// Order matters. A session is the primary credential: it is verified here,
/// against this process's own store, so the role it yields is one riven
/// established rather than one it was told. A configured API key is the
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
) -> Result<RequestAuth, Unauthorized> {
    if let Some(role) = session_role(state, headers).await? {
        return Ok(RequestAuth { role });
    }

    if has_valid_api_key(state, headers, query) {
        return Ok(RequestAuth::trusted_api_key());
    }

    tracing::warn!("auth rejected: no valid session and no matching api key");
    Err(Unauthorized)
}

/// `Ok(None)` means "no session was established from what was presented" — the
/// caller should fall back to the API key.
///
/// A credential that matches no session row yields `Ok(None)` rather than an
/// error, because `Authorization: Bearer` carries *either* a session token or
/// the API key and there is no way to tell them apart before the lookup. Failing
/// here made `Authorization: Bearer <api-key>` — a documented, supported way to
/// call the API — impossible: it was read as a session token, found nothing, and
/// returned 401 without ever reaching `has_valid_api_key`.
///
/// A session that *exists* but is unusable (expired or revoked) still returns
/// `Err`. Falling through only offers the value to `api_key_matches`, which
/// grants nothing unless it equals the configured key.
async fn session_role(
    state: &ApiState,
    headers: &HeaderMap,
) -> Result<Option<UserRole>, Unauthorized> {
    use super::authn::SessionState;

    let session_state = super::authn::authenticate(&state.auth, headers)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "session lookup failed");
            Unauthorized
        })?;

    match session_state {
        SessionState::Anonymous => Ok(None),
        SessionState::Unauthorized => {
            tracing::debug!("auth rejected: session expired or revoked");
            Err(Unauthorized)
        }
        SessionState::Valid { user, .. } => Ok(Some(role_from_user(user.role.as_deref()))),
    }
}

/// Map the free-text role column onto riven's ladder.
///
/// Unrecognised and absent roles both land on `User`, the least privilege —
/// a typo in the column must not become an escalation.
pub(super) fn role_from_user(role: Option<&str>) -> UserRole {
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

    /// `Authorization: Bearer <api-key>` is documented as a supported way to
    /// call the API, and `api_key_matches` reads that header — but every such
    /// request was rejected before reaching it, because `session_role` treated
    /// the value as a session token and failed on the miss.
    #[test]
    fn an_api_key_presented_as_a_bearer_token_is_accepted() {
        const KEY: &str = "s3cret";
        assert!(api_key_matches(
            Some(KEY),
            &headers_with("authorization", &format!("Bearer {KEY}")),
            None
        ));
    }

    #[test]
    fn secret_comparison_rejects_near_misses_and_length_differences() {
        assert!(secret_eq("abc123", "abc123"));
        assert!(!secret_eq("abc124", "abc123"));
        assert!(!secret_eq("abc12", "abc123"));
        assert!(!secret_eq("abc1234", "abc123"));
        assert!(!secret_eq("", "abc123"));
        assert!(secret_eq("", ""));
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

    /// An unknown role must not inherit privilege — the escalation this
    /// guards against is a typo in the role column.
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
