//! OIDC sign-in: authorization-code flow with PKCE.
//!
//! `sign-in/social` hands the frontend the provider's authorization URL (a
//! `fetch` cannot forward a 302 into a top-level navigation); the provider
//! sends the browser to `/auth/callback/{provider}`, which exchanges the
//! code, maps the userinfo claims onto a local user, and ends the same way
//! every other sign-in does: a session cookie. Any failure after the redirect
//! is logged and lands the browser back on `/?error=sign-in-failed` — the
//! user retries from the login page, the operator reads the log.

use std::sync::LazyLock;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header::LOCATION, header::SET_COOKIE};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{TimeDelta, Utc};
use riven_core::entities::auth::{account, user};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, Set};
use serde::Deserialize;
use serde_json::json;
use sha2::Digest;

use super::session::{cookie, cookie_name, cookie_value, create_session, session_cookie};
use super::{ApiError, ApiResult, TtlMap, random_token};
use crate::server::ApiState;
use crate::server::oidc::ResolvedProvider;

/// One in-flight sign-in per `state` value, single-use, 10 minutes.
struct Pending {
    provider_id: String,
    code_verifier: String,
    callback_url: String,
}

const STATE_TTL_SECONDS: i64 = 600;

static PENDING: LazyLock<TtlMap<Pending>> =
    LazyLock::new(|| TtlMap::new(Duration::from_secs(STATE_TTL_SECONDS as u64)));

/// Binds the callback to the browser that started the sign-in.
///
/// The `state` parameter alone proves only that *someone* started a flow, and
/// an attacker knows their own: they can authenticate at the provider as
/// themselves, decline to follow the final redirect, and hand the victim the
/// resulting `?code=…&state=…` URL. The victim's browser completes the
/// exchange and is issued a session for the *attacker's* account, which is
/// then watched from the inside. RFC 6749 §10.12 requires this binding.
///
/// The cookie carries the same opaque handle rather than any state of its
/// own, so there is nothing here to sign — the server already holds the
/// payload, and equality against a single-use 256-bit value is the whole
/// check.
///
/// `SameSite=Lax` is load-bearing rather than incidental: the callback is a
/// cross-site top-level navigation from the provider, which Lax sends and
/// `Strict` would drop — silently breaking every OIDC sign-in.
const STATE_COOKIE: &str = "riven.oauth_state";

#[derive(Deserialize)]
pub(super) struct SignInSocial {
    provider: String,
    callback_url: Option<String>,
}

pub(super) async fn sign_in_social(
    State(state): State<ApiState>,
    Json(body): Json<SignInSocial>,
) -> ApiResult<Response> {
    let auth = &state.auth;
    let provider = auth
        .provider(&body.provider)
        .ok_or_else(|| ApiError::bad_request("Unknown sign-in provider"))?;

    // The callback lands a signed-in browser here, so anything not clearly
    // riven's own falls back to `/` rather than becoming an open redirect.
    // Browsers read both `//host` and `/\host` as protocol-relative, and a
    // control character in the value splits the `Location` header, so neither
    // shape counts as root-relative.
    let callback_url = body
        .callback_url
        .filter(|url| {
            let root_relative = url.starts_with('/')
                && !matches!(url.as_bytes().get(1), Some(b'/' | b'\\'))
                && !url.chars().any(char::is_control);
            root_relative
                || url.trim_end_matches('/') == auth.base_url.trim_end_matches('/')
                || url.starts_with(&format!("{}/", auth.base_url.trim_end_matches('/')))
        })
        .unwrap_or_else(|| "/".to_string());

    let code_verifier = random_token();
    let code_challenge = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(code_verifier.as_bytes()));
    let state_key = PENDING.insert(Pending {
        provider_id: provider.id.clone(),
        code_verifier,
        callback_url,
    });

    let mut url = url::Url::parse(&provider.auth_url)
        .map_err(|error| ApiError::internal("Provider has an invalid authorization URL", error))?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &provider.client_id)
        .append_pair("redirect_uri", &redirect_uri(&state, &provider.id))
        .append_pair("scope", &provider.scopes.join(" "))
        .append_pair("state", &state_key)
        .append_pair("code_challenge", &code_challenge)
        .append_pair("code_challenge_method", "S256");

    Ok((
        [(
            SET_COOKIE,
            cookie(
                &cookie_name(STATE_COOKIE, auth.cookie_secure),
                &state_key,
                STATE_TTL_SECONDS,
                auth.cookie_secure,
            ),
        )],
        Json(json!({ "url": url.to_string(), "redirect": true })),
    )
        .into_response())
}

fn redirect_uri(state: &ApiState, provider_id: &str) -> String {
    format!(
        "{}/auth/callback/{provider_id}",
        state.auth.base_url.trim_end_matches('/')
    )
}

#[derive(Deserialize)]
pub(super) struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    id_token: Option<String>,
    expires_in: Option<i64>,
    scope: Option<String>,
}

pub(super) async fn callback(
    State(state): State<ApiState>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<CallbackQuery>,
) -> Response {
    match callback_inner(&state, &provider_id, &headers, query).await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(provider = %provider_id, %error, "OIDC sign-in failed");
            (
                StatusCode::FOUND,
                [(LOCATION, "/?error=sign-in-failed".to_string())],
            )
                .into_response()
        }
    }
}

async fn callback_inner(
    state: &ApiState,
    provider_id: &str,
    headers: &HeaderMap,
    query: CallbackQuery,
) -> anyhow::Result<Response> {
    let auth = &state.auth;

    if let Some(error) = query.error {
        anyhow::bail!("provider returned: {error}");
    }
    // Checked before the handle is spent, so a forged callback cannot consume
    // a pending sign-in the real browser is still going to complete.
    anyhow::ensure!(
        query.state.is_some()
            && cookie_value(headers, &cookie_name(STATE_COOKIE, auth.cookie_secure)) == query.state,
        "callback did not come from the browser that started this sign-in"
    );
    let pending = query
        .state
        .as_deref()
        .and_then(|key| PENDING.take(key))
        .filter(|pending| pending.provider_id == provider_id)
        .ok_or_else(|| anyhow::anyhow!("unknown, expired or mismatched state"))?;
    let code = query
        .code
        .ok_or_else(|| anyhow::anyhow!("callback carried no code"))?;
    let provider = auth
        .provider(provider_id)
        .ok_or_else(|| anyhow::anyhow!("provider no longer configured"))?;

    // Code → tokens. Client authentication is HTTP Basic, the one method the
    // spec requires every provider to support.
    let response = state
        .stream_client
        .post(&provider.token_url)
        .basic_auth(&provider.client_id, Some(&provider.client_secret))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("redirect_uri", &redirect_uri(state, &provider.id)),
            ("code_verifier", &pending.code_verifier),
        ])
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "token endpoint answered {}",
        response.status()
    );
    let token: TokenResponse = response.json().await?;

    // Tokens → identity, via the standard userinfo claims.
    let response = state
        .stream_client
        .get(&provider.userinfo_url)
        .bearer_auth(&token.access_token)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "userinfo endpoint answered {}",
        response.status()
    );
    let info = crate::server::oidc::map_user_info(response.json().await?)
        .map_err(|error| anyhow::anyhow!("userinfo claims: {error}"))?;

    let user = link_or_create_user(state, provider, &info, &token).await?;
    let token = create_session(&auth.db, &user.id, None).await?;

    Ok((
        StatusCode::FOUND,
        [
            (LOCATION, pending.callback_url),
            (SET_COOKIE, session_cookie(auth.cookie_secure, &token)),
            (
                SET_COOKIE,
                cookie(
                    &cookie_name(STATE_COOKIE, auth.cookie_secure),
                    "",
                    0,
                    auth.cookie_secure,
                ),
            ),
        ],
    )
        .into_response())
}

/// An existing `(provider, sub)` account row wins outright. Failing that, a
/// user with the same email is auto-linked — but only when the provider
/// vouches for the address (`email_verified`) or the operator opted the
/// provider into `trust_unverified_email`; otherwise a stranger who registers
/// an unconfirmed address at the IdP could claim the matching riven account.
/// A brand-new user is created only when the provider allows sign-up, and the
/// very first user is the admin — same rule as password sign-up.
async fn link_or_create_user(
    state: &ApiState,
    provider: &ResolvedProvider,
    info: &crate::server::oidc::OAuthUserInfo,
    token: &TokenResponse,
) -> anyhow::Result<user::Model> {
    let db = &state.auth.db;
    let now = Utc::now();
    let email = info.email.trim().to_ascii_lowercase();

    let linked = account::Entity::find()
        .filter(account::Column::ProviderId.eq(&provider.id))
        .filter(account::Column::AccountId.eq(&info.id))
        .one(db)
        .await?;

    let user = match &linked {
        Some(account) => user::Entity::find_by_id(&account.user_id)
            .one(db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("linked account without user"))?,
        None => {
            let by_email = user::Entity::find()
                .filter(user::Column::Email.eq(&email))
                .one(db)
                .await?;
            match by_email {
                Some(user) => {
                    anyhow::ensure!(
                        info.email_verified || provider.trust_unverified_email,
                        "{email} is unverified at the provider; refusing to link it to an \
                         existing account"
                    );
                    user
                }
                None => {
                    anyhow::ensure!(
                        !provider.disable_sign_up,
                        "no riven account matches {email} and sign-up is disabled"
                    );
                    let first = user::Entity::find().count(db).await? == 0;
                    user::ActiveModel {
                        id: Set(uuid::Uuid::new_v4().to_string()),
                        name: Set(info.name.clone()),
                        email: Set(Some(email.clone())),
                        email_verified: Set(info.email_verified),
                        image: Set(info.image.clone()),
                        username: Set(None),
                        display_username: Set(None),
                        role: Set(Some(if first { "admin" } else { "user" }.to_string())),
                        created_at: Set(now),
                        updated_at: Set(now),
                    }
                    .insert(db)
                    .await?
                }
            }
        }
    };

    // Persist the link (or the freshest tokens on an existing one).
    let expires_at = token.expires_in.map(|secs| now + TimeDelta::seconds(secs));
    let update = account::ActiveModel {
        id: Set(linked
            .as_ref()
            .map_or_else(|| uuid::Uuid::new_v4().to_string(), |a| a.id.clone())),
        access_token: Set(Some(token.access_token.clone())),
        refresh_token: Set(token.refresh_token.clone()),
        id_token: Set(token.id_token.clone()),
        access_token_expires_at: Set(expires_at),
        scope: Set(token.scope.clone()),
        updated_at: Set(now),
        ..Default::default()
    };
    if linked.is_some() {
        update.update(db).await?;
    } else {
        let mut create = update;
        create.account_id = Set(info.id.clone());
        create.provider_id = Set(provider.id.clone());
        create.user_id = Set(user.id.clone());
        create.refresh_token_expires_at = Set(None);
        create.password = Set(None);
        create.created_at = Set(now);
        create.insert(db).await?;
    }

    Ok(user)
}
