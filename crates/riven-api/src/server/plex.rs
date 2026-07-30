//! Plex sign-in.
//!
//! Plex is not an OAuth2 provider, so better-auth's OAuth plugin cannot cover
//! it: there is no authorization-code exchange and no token endpoint. It uses a
//! PIN-and-poll flow instead —
//!
//! 1. `POST https://plex.tv/api/v2/pins` mints a PIN (`id` + 4-character `code`)
//! 2. the browser is sent to `app.plex.tv/auth#?clientID=…&code=…`
//! 3. the caller polls `GET https://plex.tv/api/v2/pins/{id}` until `authToken`
//!    appears, which is Plex saying the user approved it
//! 4. `GET https://plex.tv/api/v2/user` turns that token into a profile
//!
//! Riven exposes steps 1 and 3 as two endpoints and does 4 internally, then
//! links the Plex identity to a local user and mints a better-auth session — so
//! from the frontend's point of view a Plex sign-in ends the same way a password
//! sign-in does: a session cookie.
//!
//! Ported from the SvelteKit `plex-oauth.ts` this replaces, which ran the same
//! four steps in Node.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header::USER_AGENT};
use axum::response::{IntoResponse, Response};
use axum::{Json, http::header::SET_COOKIE};
use better_auth::prelude::{AuthSession, AuthUser};
use better_auth_core::types::CreateAccount;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::ApiState;
use super::authn::RivenAuth;

const PLEX_PINS_URL: &str = "https://plex.tv/api/v2/pins";
const PLEX_USER_URL: &str = "https://plex.tv/api/v2/user";
const PLEX_AUTH_URL: &str = "https://app.plex.tv/auth#?";
/// `provider_id` on the account row. Matches what the TypeScript flow wrote, so
/// a user who linked Plex before the migration keeps the same account row.
const PROVIDER_ID: &str = "plex";

/// Total deadline for each call to plex.tv.
///
/// `stream_client` is shared with the VFS, which deliberately sets only a
/// `read_timeout` — a total deadline there would cap throughput on a
/// multi-megabyte range read rather than detect a fault. These are small JSON
/// requests to a third party, so the opposite is wanted: a server that accepts
/// the connection and then trickles forever must not pin this handler task.
/// Applied per request, which overrides the client-level policy for these calls
/// only.
const PLEX_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Identifies this installation to Plex. Stable per instance: Plex ties the PIN
/// and the resulting token to it, so a value that changed per request would
/// invalidate every PIN as soon as it was polled.
fn client_identifier(state: &ApiState) -> String {
    // Derived from the API key so it is stable across restarts without needing
    // another setting. Hashed rather than used directly — this value is sent to
    // a third party and echoed in a URL the user's browser visits.
    let key = state.api_key.as_deref().unwrap_or("riven-plex-fallback");
    let digest = <sha2::Sha256 as sha2::Digest>::digest(key.as_bytes());
    hex::encode(&digest[..16])
}

fn plex_headers(state: &ApiState, token: Option<&str>) -> reqwest::header::HeaderMap {
    use reqwest::header::{ACCEPT, HeaderMap as ReqHeaders, HeaderValue};
    let mut headers = ReqHeaders::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    let id = client_identifier(state);
    for (name, value) in [
        ("X-Plex-Product", "Riven Media".to_string()),
        ("X-Plex-Version", env!("CARGO_PKG_VERSION").to_string()),
        ("X-Plex-Client-Identifier", id),
        ("X-Plex-Platform", "Web".to_string()),
        ("X-Plex-Device", "Browser".to_string()),
    ] {
        if let Ok(value) = HeaderValue::from_str(&value) {
            headers.insert(name, value);
        }
    }
    if let Some(token) = token
        && let Ok(value) = HeaderValue::from_str(token)
    {
        headers.insert("X-Plex-Token", value);
    }
    headers
}

#[derive(Deserialize)]
struct PlexPin {
    id: i64,
    code: String,
    #[serde(rename = "authToken")]
    auth_token: Option<String>,
}

/// Only `id`, `email` and `confirmed` drive the linking decision today. The rest
/// are kept because they are what a profile sync would populate (`name`,
/// `image`) and because dropping them would silently discard what Plex already
/// returned.
#[derive(Deserialize)]
#[expect(
    dead_code,
    reason = "retained for a future profile sync; see doc comment"
)]
struct PlexProfile {
    id: i64,
    username: Option<String>,
    email: Option<String>,
    /// Whether Plex has confirmed the address on this account.
    ///
    /// Load-bearing: Plex lets an *unconfirmed* account sign in and reach this
    /// endpoint — its own docs tell users to resend verification from the
    /// signed-in account page — so anyone can register a Plex account under
    /// someone else's address and get a usable token without touching that
    /// inbox. Without this flag the email match below would accept that as
    /// proof of identity. Defaults to `false`, so a Plex response that omits
    /// the field fails closed.
    #[serde(default)]
    confirmed: bool,
    thumb: Option<String>,
    title: Option<String>,
}

#[derive(Serialize)]
pub(super) struct PinResponse {
    id: i64,
    /// Where to send the browser. The user approves there, then the caller polls.
    auth_url: String,
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "message": message }))).into_response()
}

/// Step 1: mint a PIN and hand back the URL to send the user to.
pub(super) async fn start(State(state): State<ApiState>) -> Response {
    let response = state
        .stream_client
        .post(PLEX_PINS_URL)
        .headers(plex_headers(&state, None))
        .query(&[("strong", "true")])
        .timeout(PLEX_REQUEST_TIMEOUT)
        .send()
        .await;

    let pin: PlexPin = match response {
        Ok(response) if response.status().is_success() => match response.json().await {
            Ok(pin) => pin,
            Err(error_) => {
                tracing::warn!(error = %error_, "plex pin response was not parseable");
                return error(StatusCode::BAD_GATEWAY, "Unexpected response from Plex");
            }
        },
        Ok(response) => {
            tracing::warn!(status = %response.status(), "plex refused to mint a pin");
            return error(StatusCode::BAD_GATEWAY, "Plex refused to start sign-in");
        }
        Err(error_) => {
            tracing::warn!(error = %error_, "could not reach plex.tv");
            return error(StatusCode::BAD_GATEWAY, "Could not reach Plex");
        }
    };

    let auth_url = format!(
        "{PLEX_AUTH_URL}clientID={client}&code={code}&context%5Bdevice%5D%5Bproduct%5D=Riven%20Media",
        client = urlencoding_encode(&client_identifier(&state)),
        code = urlencoding_encode(&pin.code),
    );

    Json(PinResponse {
        id: pin.id,
        auth_url,
    })
    .into_response()
}

/// Step 3+4: poll the PIN; when Plex has a token, resolve the profile, link it
/// to a local user and mint a session.
///
/// `202 Accepted` means "not approved yet, keep polling" — distinct from an
/// error, so the frontend can loop without treating it as a failure.
pub(super) async fn poll(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(pin_id): Path<i64>,
) -> Response {
    let pin: PlexPin = match state
        .stream_client
        .get(format!("{PLEX_PINS_URL}/{pin_id}"))
        .headers(plex_headers(&state, None))
        .timeout(PLEX_REQUEST_TIMEOUT)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => match response.json().await {
            Ok(pin) => pin,
            Err(_) => return error(StatusCode::BAD_GATEWAY, "Unexpected response from Plex"),
        },
        Ok(_) => return error(StatusCode::BAD_GATEWAY, "Plex rejected the sign-in code"),
        Err(_) => return error(StatusCode::BAD_GATEWAY, "Could not reach Plex"),
    };

    let Some(token) = pin.auth_token.filter(|token| !token.is_empty()) else {
        return (StatusCode::ACCEPTED, Json(json!({ "pending": true }))).into_response();
    };

    let profile: PlexProfile = match state
        .stream_client
        .get(PLEX_USER_URL)
        .headers(plex_headers(&state, Some(&token)))
        .timeout(PLEX_REQUEST_TIMEOUT)
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => match response.json().await {
            Ok(profile) => profile,
            Err(_) => return error(StatusCode::BAD_GATEWAY, "Unexpected profile from Plex"),
        },
        Ok(_) => return error(StatusCode::UNAUTHORIZED, "Plex rejected the token"),
        Err(_) => return error(StatusCode::BAD_GATEWAY, "Could not reach Plex"),
    };

    match link_and_start_session(&state.auth, &profile, &token, &headers).await {
        Ok(cookie) => {
            let body = Json(json!({ "pending": false }));
            match Response::builder()
                .status(StatusCode::OK)
                .header(SET_COOKIE, cookie)
                .header("content-type", "application/json")
                .body(body.into_response().into_body())
            {
                Ok(response) => response,
                Err(_) => error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Could not build response",
                ),
            }
        }
        Err(message) => error(StatusCode::FORBIDDEN, &message),
    }
}

/// Link the Plex identity to a local user, then mint a session cookie.
///
/// **Linking is by existing account row or by email, and never creates a user.**
/// Riven is a private media server: a stranger with a Plex account must not be
/// able to sign in just because the instance is reachable. Accounts are created
/// by an admin; Plex only attaches to one that already exists.
async fn link_and_start_session(
    auth: &Arc<RivenAuth>,
    profile: &PlexProfile,
    token: &str,
    headers: &HeaderMap,
) -> Result<String, String> {
    let store = auth.store();
    let plex_id = profile.id.to_string();

    // Prefer an existing link; fall back to matching the Plex email, but only
    // when Plex says it has confirmed that address.
    //
    // Without the `confirmed` check the fallback treats a third party's
    // assertion as proof of identity: an attacker registers a Plex account under
    // a riven user's email, never verifies it, signs in anyway (Plex permits
    // that), and this match hands them that user's session — then persists the
    // link below, making it durable. Note the match is keyed on the *incoming*
    // `plex_id` being unknown, so it fires even for a user who already linked
    // their real Plex account.
    //
    // The store's error is logged rather than folded into the caller's message:
    // the user gets a generic failure, the operator gets the cause.
    let user = match store.get_account(&plex_id, PROVIDER_ID).await {
        Ok(Some(account)) => store
            .get_user_by_id(&better_auth_core::entity::AuthAccount::user_id(&account))
            .await
            .map_err(|error| {
                tracing::warn!(%error, "could not load the user behind a linked plex account");
                "Could not load the linked account".to_string()
            })?,
        _ => match profile.email.as_deref() {
            Some(email) if profile.confirmed => {
                store.get_user_by_email(email).await.map_err(|error| {
                    tracing::warn!(%error, "could not look up a user by plex email");
                    "Could not look up the account".to_string()
                })?
            }
            Some(_) => {
                tracing::warn!(
                    plex_id = %plex_id,
                    "plex sign-in refused: the plex account's email is unconfirmed, so it \
                     cannot be used to claim an existing riven account"
                );
                None
            }
            None => None,
        },
    };

    let Some(user) = user else {
        tracing::warn!(
            plex_id = %plex_id,
            "plex sign-in refused: no riven account matches this plex identity"
        );
        return Err("No Riven account is linked to this Plex user".to_string());
    };

    // Record the link so subsequent sign-ins skip the email match, and keep the
    // freshest Plex token on the row.
    if matches!(store.get_account(&plex_id, PROVIDER_ID).await, Ok(None)) {
        let create = CreateAccount {
            user_id: user.id().to_string(),
            account_id: plex_id.clone(),
            provider_id: PROVIDER_ID.to_string(),
            access_token: Some(token.to_string()),
            refresh_token: None,
            id_token: None,
            access_token_expires_at: None,
            refresh_token_expires_at: None,
            scope: None,
            // No password: this account authenticates through Plex, not a
            // credential. A NULL here is what keeps it out of the password flow.
            password: None,
        };
        if let Err(error) = store.create_account(create).await {
            // Non-fatal: the sign-in still stands, the next one just re-matches
            // by email.
            tracing::warn!(%error, "could not persist the plex account link");
        }
    }

    let session = auth
        .session_manager()
        .create_session(
            &user,
            None,
            headers
                .get(USER_AGENT)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
        )
        .await
        .map_err(|error| {
            tracing::warn!(%error, "could not create a session for a plex sign-in");
            "Could not start a session".to_string()
        })?;

    // better-auth's own helper, not a hand-rolled string. The string this
    // replaced omitted `Secure`, which better-auth derives from the scheme of
    // `base_url` — so on the documented HTTPS deployment a Plex sign-in handed
    // out a session cookie that a plaintext request to the same host would leak,
    // while every other sign-in path on the same instance set the flag.
    // Deferring to the helper also means future changes to the session cookie
    // policy reach this path instead of silently skipping it.
    Ok(
        better_auth_core::utils::cookie_utils::create_session_cookie(
            session.token(),
            auth.config(),
        ),
    )
}

fn urlencoding_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_auth_url_carries_the_client_id_and_code() {
        let url = format!(
            "{PLEX_AUTH_URL}clientID={}&code={}",
            urlencoding_encode("abc123"),
            urlencoding_encode("PIN4")
        );
        assert!(url.starts_with("https://app.plex.tv/auth#?"));
        assert!(url.contains("clientID=abc123"));
        assert!(url.contains("code=PIN4"));
    }

    #[test]
    fn values_needing_escaping_are_encoded() {
        assert_eq!(urlencoding_encode("a b&c"), "a+b%26c");
    }
}
