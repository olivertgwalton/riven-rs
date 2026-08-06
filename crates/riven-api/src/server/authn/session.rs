//! Sessions: opaque bearer tokens carried by the cookie or an
//! `Authorization: Bearer` header.
//!
//! The raw token exists in exactly two places — the client's cookie and the
//! response that minted it. `auth_sessions.token` holds only its SHA-256, so
//! read access to the database yields nothing that can be replayed as a
//! login. Sessions last 30 days flat.

use axum::http::HeaderMap;
use chrono::{TimeDelta, Utc};
use riven_core::entities::auth::{session, user};
use sea_orm::ActiveModelTrait;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};

use super::{ApiError, ApiResult, AuthService, hash_token, random_token};

/// Base cookie name. [`cookie_name`] adds the `__Host-` prefix on secure
/// deployments, so the name on the wire is one of two fixed strings.
const SESSION_COOKIE: &str = "riven.session_token";
const SESSION_TTL_SECONDS: i64 = 30 * 24 * 60 * 60;

/// What presenting (or not presenting) a credential established.
pub enum SessionState {
    /// No token, or a token matching no session. Not an error: the same
    /// `Authorization: Bearer` slot carries the API key, so the caller must
    /// be allowed to fall through to that check.
    Anonymous,
    /// A real session that is expired or revoked, or whose user vanished.
    Unauthorized,
    /// Boxed because the models dwarf the other variants and this enum
    /// travels through every authorized request.
    Valid {
        user: Box<user::Model>,
        session: Box<session::Model>,
    },
}

/// `__Host-` is a browser-enforced promise that the cookie is `Secure`,
/// `Path=/` and carries no `Domain` — which is precisely what stops a hostile
/// or compromised sibling subdomain (`other.example.com`) from planting a
/// cookie that shadows the real session on `riven.example.com`. The prefix is
/// only legal on a `Secure` cookie, so a plain-HTTP local run keeps the bare
/// name rather than setting a cookie every browser would silently reject.
pub fn cookie_name(base: &str, secure: bool) -> String {
    if secure {
        format!("__Host-{base}")
    } else {
        base.to_string()
    }
}

/// The bearer token from `Authorization: Bearer` or the session cookie.
pub fn extract_token(headers: &HeaderMap, secure: bool) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .or_else(|| cookie_value(headers, &cookie_name(SESSION_COOKIE, secure)))
}

pub fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookies = headers
        .get(axum::http::header::COOKIE)
        .and_then(|value| value.to_str().ok())?;
    let prefix = format!("{name}=");
    cookies
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix(&prefix))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Authenticate a request from its headers alone.
pub async fn authenticate(
    auth: &AuthService,
    headers: &HeaderMap,
) -> Result<SessionState, sea_orm::DbErr> {
    let Some(token) = extract_token(headers, auth.cookie_secure) else {
        return Ok(SessionState::Anonymous);
    };
    let Some(found) = session::Entity::find()
        .filter(session::Column::Token.eq(hash_token(&token)))
        .one(&auth.db)
        .await?
    else {
        return Ok(SessionState::Anonymous);
    };
    if !found.active || found.expires_at <= Utc::now() {
        return Ok(SessionState::Unauthorized);
    }
    let Some(user) = user::Entity::find_by_id(&found.user_id)
        .one(&auth.db)
        .await?
    else {
        return Ok(SessionState::Unauthorized);
    };
    Ok(SessionState::Valid {
        user: Box::new(user),
        session: Box::new(found),
    })
}

/// The session or a 401 — for endpoints that require a signed-in caller. The
/// API key deliberately does not count: machine callers talk to GraphQL, not
/// to account management.
pub async fn require_user(
    auth: &AuthService,
    headers: &HeaderMap,
) -> ApiResult<(user::Model, session::Model)> {
    match authenticate(auth, headers).await? {
        SessionState::Valid { user, session } => Ok((*user, *session)),
        _ => Err(ApiError::unauthorized("Not signed in")),
    }
}

/// Mint a session and return the **raw** token, which is the only point at
/// which it exists server-side; the row keeps only its hash.
pub async fn create_session(
    db: &DatabaseConnection,
    user_id: &str,
    user_agent: Option<String>,
) -> Result<String, sea_orm::DbErr> {
    let token = random_token();
    let now = Utc::now();
    session::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        token: Set(hash_token(&token)),
        user_id: Set(user_id.to_string()),
        expires_at: Set(now + TimeDelta::seconds(SESSION_TTL_SECONDS)),
        created_at: Set(now),
        updated_at: Set(now),
        ip_address: Set(None),
        user_agent: Set(user_agent),
        active: Set(true),
        impersonated_by: Set(None),
    }
    .insert(db)
    .await?;
    Ok(token)
}

/// Delete the session behind whatever credential the request carried.
pub async fn revoke_session(auth: &AuthService, headers: &HeaderMap) -> Result<(), sea_orm::DbErr> {
    if let Some(token) = extract_token(headers, auth.cookie_secure) {
        session::Entity::delete_many()
            .filter(session::Column::Token.eq(hash_token(&token)))
            .exec(&auth.db)
            .await?;
    }
    Ok(())
}

/// `SameSite=Lax` is the CSRF defense for every cookie-authenticated POST
/// under `/auth`: a cross-site POST simply does not carry the cookie.
pub fn session_cookie(secure: bool, token: &str) -> String {
    cookie(
        &cookie_name(SESSION_COOKIE, secure),
        token,
        SESSION_TTL_SECONDS,
        secure,
    )
}

pub fn clear_session_cookie(secure: bool) -> String {
    cookie(&cookie_name(SESSION_COOKIE, secure), "", 0, secure)
}

/// Minimal `Set-Cookie` formatter. Values are hex tokens minted by this
/// module, so no escaping is ever needed. `Path=/` and the absent `Domain`
/// are load-bearing — both are required for the `__Host-` prefix above.
pub fn cookie(name: &str, value: &str, max_age_seconds: i64, secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!("{name}={value}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age_seconds}{secure}")
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
            format!("{SESSION_COOKIE}=from-cookie").parse().unwrap(),
        );
        assert_eq!(
            extract_token(&headers, false).as_deref(),
            Some("from-header")
        );
    }

    #[test]
    fn the_session_cookie_is_found_among_others_and_not_mistaken() {
        let headers = headers_with(
            "cookie",
            &format!("theme=dark; {SESSION_COOKIE}=abc123; other=1"),
        );
        assert_eq!(extract_token(&headers, false).as_deref(), Some("abc123"));

        let headers = headers_with("cookie", &format!("not_{SESSION_COOKIE}=abc123"));
        assert_eq!(extract_token(&headers, false), None);
        assert_eq!(
            extract_token(&headers_with("authorization", "Bearer "), false),
            None
        );
    }

    /// The prefixed and bare names are distinct cookies; reading must use the
    /// same flag the writer did, or a secure deployment authenticates nobody.
    #[test]
    fn secure_deployments_read_and_write_the_host_prefixed_cookie() {
        let set = session_cookie(true, "tok");
        assert!(set.starts_with("__Host-riven.session_token=tok; "), "{set}");
        assert!(set.contains("; Secure"));
        assert!(set.contains("; Path=/"));
        assert!(
            !set.contains("Domain"),
            "__Host- forbids a Domain attribute"
        );

        let headers = headers_with("cookie", "__Host-riven.session_token=abc123");
        assert_eq!(extract_token(&headers, true).as_deref(), Some("abc123"));
        assert_eq!(extract_token(&headers, false), None);
    }

    /// `__Host-` is only legal alongside `Secure`, so a plain-HTTP local run
    /// must keep the bare name rather than set a cookie browsers reject.
    #[test]
    fn plain_http_keeps_the_bare_cookie_name() {
        let set = session_cookie(false, "tok");
        assert!(set.starts_with("riven.session_token=tok; "), "{set}");
        assert!(!set.contains("Secure"));
        assert_eq!(
            clear_session_cookie(false),
            "riven.session_token=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"
        );
    }

    /// The stored value must never equal the credential, or the hash is
    /// pointless — this is the whole property being bought.
    #[test]
    fn the_stored_token_is_a_hash_not_the_credential() {
        let token = random_token();
        let stored = hash_token(&token);
        assert_ne!(stored, token);
        assert_eq!(stored.len(), 64);
        assert_eq!(stored, hash_token(&token), "hashing must be deterministic");
        assert_ne!(stored, hash_token(&random_token()));
    }
}
