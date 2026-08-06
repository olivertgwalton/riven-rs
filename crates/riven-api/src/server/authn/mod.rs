//! Native authentication: sessions, email/username + password, passkeys,
//! OIDC, password reset and admin user management, backed by the `auth_*`
//! tables (and `passkeys`) in riven's own database.
//!
//! The wire is the set of routes the frontend's `auth-client.ts` speaks;
//! every field is snake_case, matching the Rust structs directly. Errors are
//! `{ "message": … }` with a matching status code.

mod account;
mod oauth;
mod passkey;
mod password;
mod ratelimit;
mod session;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use rand_core::{OsRng, RngCore};
use riven_core::entities::auth::user;
use riven_core::settings::OidcProviderSettings;
use sea_orm::DatabaseConnection;
use serde_json::json;
use tower_http::set_header::SetResponseHeaderLayer;
use webauthn_rs::{Webauthn, WebauthnBuilder};

use super::ApiState;
use super::legacy_password::DualFormatHasher;
use super::oidc::{self, ResolvedProvider};

pub use session::{SessionState, authenticate, create_session, session_cookie};

/// The instance's auth handle: database, password hasher, WebAuthn relying
/// party and the OIDC providers that resolved at startup.
pub struct AuthService {
    pub(crate) db: DatabaseConnection,
    /// Public origin browsers reach riven at — the WebAuthn origin, the base
    /// of every OAuth redirect URI, and (via its scheme) the cookie policy.
    pub(crate) base_url: String,
    pub(crate) cookie_secure: bool,
    pub(crate) hasher: DualFormatHasher,
    /// `None` when `base_url` has no host to derive a relying party from —
    /// passkey routes then answer with an explanatory error.
    pub(crate) webauthn: Option<Webauthn>,
    pub(crate) oidc: Vec<ResolvedProvider>,
}

impl AuthService {
    pub(crate) fn provider(&self, id: &str) -> Option<&ResolvedProvider> {
        self.oidc.iter().find(|provider| provider.id == id)
    }
}

pub async fn build(
    base_url: &str,
    oidc_settings: &[OidcProviderSettings],
) -> anyhow::Result<Arc<AuthService>> {
    let db = riven_db::orm().clone();
    Ok(Arc::new(AuthService {
        hasher: DualFormatHasher::new(db.clone()),
        db,
        base_url: base_url.trim_end_matches('/').to_string(),
        cookie_secure: base_url.starts_with("https://"),
        webauthn: build_webauthn(base_url),
        oidc: oidc::resolve_providers(oidc_settings).await,
    }))
}

/// The relying party is the host of `base_url` — the one piece of auth config
/// that cannot change after the fact, because every passkey is sealed to it
/// by the authenticator itself.
fn build_webauthn(base_url: &str) -> Option<Webauthn> {
    let url = url::Url::parse(base_url).ok()?;
    let rp_id = url.host_str()?.to_string();
    let webauthn = WebauthnBuilder::new(&rp_id, &url)
        .ok()?
        .rp_name("Riven")
        // Ports are not part of a relying party ID; without this a
        // `localhost:8080` deployment would refuse its own origin.
        .allow_any_port(true)
        .build()
        .ok()?;
    tracing::info!(%rp_id, "passkeys are bound to this relying-party ID");
    Some(webauthn)
}

/// Every route the frontend's `auth-client.ts` calls, mounted under `/auth`
/// by `server.rs`. Auth responses are personal and short-lived, hence the
/// blanket `no-store`.
pub fn router() -> axum::Router<ApiState> {
    axum::Router::new()
        .route("/get-session", get(account::get_session))
        .route("/sign-in/email", post(password::sign_in_email))
        .route("/sign-in/username", post(password::sign_in_username))
        .route("/sign-up/email", post(password::sign_up))
        .route("/sign-out", post(account::sign_out))
        .route("/change-password", post(password::change_password))
        .route("/request-password-reset", post(password::request_password_reset))
        .route("/reset-password", post(password::reset_password))
        .route("/change-email", post(account::change_email))
        .route("/update-user", post(account::update_user))
        .route("/delete-user", post(account::delete_user))
        .route("/list-accounts", get(account::list_accounts))
        .route("/unlink-account", post(account::unlink_account))
        .route("/list-sessions", get(account::list_sessions))
        .route("/sign-in/social", post(oauth::sign_in_social))
        .route("/callback/{provider}", get(oauth::callback))
        .route("/passkey/generate-register-options", get(passkey::generate_register_options))
        .route("/passkey/verify-registration", post(passkey::verify_registration))
        .route(
            "/passkey/generate-authenticate-options",
            get(passkey::generate_authenticate_options),
        )
        .route("/passkey/verify-authentication", post(passkey::verify_authentication))
        .route("/passkey/list-user-passkeys", get(passkey::list_user_passkeys))
        .route("/passkey/delete-passkey", post(passkey::delete_passkey))
        .route("/passkey/update-passkey", post(passkey::update_passkey))
        .route("/admin/list-users", get(account::list_users))
        .route("/admin/create-user", post(account::create_user))
        .route("/admin/remove-user", post(account::remove_user))
        .route("/first-user", get(password::first_user_availability))
        .route("/oidc-providers", get(oidc_providers))
        // Plex is a PIN-and-poll flow rather than OAuth2, so it lives in
        // `plex.rs` — but it is mounted here to sit behind the same rate
        // limit: `start` makes an unauthenticated outbound call to plex.tv,
        // which is otherwise free amplification. The frontend polls every
        // two seconds, well inside the default budget.
        .route("/plex/start", post(super::plex::start))
        .route("/plex/poll/{handle}", get(super::plex::poll))
        .layer(SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        ))
        // Outermost, so a throttled request is refused before it reaches a
        // handler — the point is to not spend an Argon2 hash on it.
        .layer(axum::middleware::from_fn(ratelimit::limit))
}

/// Which OIDC providers resolved at startup, for the login page to render
/// buttons for. Unauthenticated by necessity — the login page has no session.
async fn oidc_providers(State(state): State<ApiState>) -> Json<serde_json::Value> {
    Json(
        state
            .auth
            .oidc
            .iter()
            .map(|p| json!({ "id": p.id, "name": p.name }))
            .collect(),
    )
}

/// The shared tail of every successful sign-in: a session row, the cookie,
/// and a `{ token, user }` body.
pub(super) async fn signed_in_response(
    auth: &AuthService,
    user: &user::Model,
    headers: &HeaderMap,
) -> ApiResult<Response> {
    let user_agent = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let token = create_session(&auth.db, &user.id, user_agent).await?;
    Ok((
        [(
            header::SET_COOKIE,
            session_cookie(auth.cookie_secure, &token),
        )],
        Json(json!({ "token": token, "user": user })),
    )
        .into_response())
}

// --- Errors -----------------------------------------------------------------

/// A status code and a `{ "message": … }` body — what `auth-client.ts` reads
/// back and renders inline.
pub struct ApiError(pub StatusCode, pub String);

pub type ApiResult<T> = Result<T, ApiError>;

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, message.into())
    }
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self(StatusCode::UNAUTHORIZED, message.into())
    }
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self(StatusCode::FORBIDDEN, message.into())
    }
    /// Logs the cause for the operator; the caller gets the generic message.
    pub fn internal(context: &str, error: impl std::fmt::Display) -> Self {
        tracing::warn!(%error, "{context}");
        Self(StatusCode::INTERNAL_SERVER_ERROR, context.to_string())
    }
}

impl From<sea_orm::DbErr> for ApiError {
    fn from(error: sea_orm::DbErr) -> Self {
        Self::internal("Database error", error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "message": self.1 }))).into_response()
    }
}

// --- Short-lived one-time state ---------------------------------------------

/// 32 random bytes, hex-encoded — session tokens, challenge handles, OAuth
/// state, reset tokens.
pub(super) fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// How a bearer credential is stored: SHA-256, hex.
///
/// Deliberately *not* a password KDF. The input is already 256 bits of CSPRNG
/// output, so there is no guessing surface for a slow hash to blunt, and
/// these run on every authenticated request. The hash exists for one reason:
/// so that read access to the database yields nothing that can be replayed.
pub(super) fn hash_token(token: &str) -> String {
    hex::encode(<sha2::Sha256 as sha2::Digest>::digest(token.as_bytes()))
}

/// In-memory single-use values with a TTL, keyed by an unguessable handle —
/// WebAuthn challenges and OAuth state. In-memory on purpose: riven is one
/// process, and a restart mid-ceremony just means the user starts over.
pub(super) struct TtlMap<V> {
    ttl: Duration,
    inner: Mutex<HashMap<String, (Instant, V)>>,
}

impl<V> TtlMap<V> {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn insert(&self, value: V) -> String {
        let key = random_token();
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.retain(|_, (created, _)| created.elapsed() < self.ttl);
        inner.insert(key.clone(), (Instant::now(), value));
        key
    }

    /// Remove and return — single-use by construction, so a challenge or
    /// state value can never be replayed.
    pub fn take(&self, key: &str) -> Option<V> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let (created, value) = inner.remove(key)?;
        (created.elapsed() < self.ttl).then_some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The port is deliberately absent from the relying party: WebAuthn RP
    /// IDs are domains, so `localhost:8080` would be rejected by browsers.
    #[test]
    fn the_passkey_relying_party_is_the_public_url_host() {
        assert!(build_webauthn("https://riven.example.com/").is_some());
        assert!(build_webauthn("http://localhost:8080").is_some());
        assert!(build_webauthn("riven.example.com").is_none());
    }

    #[test]
    fn ttl_map_entries_are_single_use_and_expire() {
        let map = TtlMap::new(Duration::from_secs(60));
        let key = map.insert(42);
        assert_eq!(map.take(&key), Some(42));
        assert_eq!(map.take(&key), None);

        let expired = TtlMap::new(Duration::ZERO);
        let key = expired.insert(42);
        assert_eq!(expired.take(&key), None);
    }
}
