//! WebAuthn passkeys via `webauthn-rs`: registration, discoverable sign-in,
//! and credential management.
//!
//! The `credential` column holds `webauthn-rs`'s serialized `Passkey` — the
//! same format the better-auth implementation wrote, so credentials
//! registered before the native rewrite keep verifying. The challenge
//! between `generate-*-options` and `verify-*` lives in an in-memory
//! [`TtlMap`]; the browser carries only an unguessable handle in a
//! short-lived cookie. Sign-in is always *discoverable*: the authenticator
//! picks the account, the user never types a username first.

use std::sync::LazyLock;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, header::SET_COOKIE};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use riven_core::entities::auth::{passkey, user};
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use serde::Deserialize;
use serde_json::{Value, json};
use webauthn_rs::prelude::{
    CredentialID, DiscoverableAuthentication, DiscoverableKey, Passkey as WebauthnPasskey,
    PasskeyRegistration, PublicKeyCredential, RegisterPublicKeyCredential,
};

use super::session::{cookie, cookie_name, cookie_value, require_user};
use super::{ApiError, ApiResult, AuthService, TtlMap, signed_in_response};
use crate::server::ApiState;

/// Correlates a `verify-*` call with the options generated for it. Five
/// minutes matches the platform prompts' own timeouts.
const CHALLENGE_COOKIE: &str = "riven.passkey_challenge";
const CHALLENGE_TTL_SECONDS: i64 = 300;

enum Challenge {
    Registration {
        user_id: String,
        state: Box<PasskeyRegistration>,
    },
    Authentication {
        state: Box<DiscoverableAuthentication>,
    },
}

static CHALLENGES: LazyLock<TtlMap<Challenge>> =
    LazyLock::new(|| TtlMap::new(Duration::from_secs(CHALLENGE_TTL_SECONDS as u64)));

fn webauthn(auth: &AuthService) -> ApiResult<&webauthn_rs::Webauthn> {
    auth.webauthn.as_ref().ok_or_else(|| {
        ApiError::internal(
            "Passkeys are unavailable",
            "public URL has no host, so no relying party could be derived",
        )
    })
}

fn store_challenge(auth: &AuthService, challenge: Challenge) -> (axum::http::HeaderName, String) {
    let handle = CHALLENGES.insert(challenge);
    (
        SET_COOKIE,
        cookie(
            &cookie_name(CHALLENGE_COOKIE, auth.cookie_secure),
            &handle,
            CHALLENGE_TTL_SECONDS,
            auth.cookie_secure,
        ),
    )
}

fn take_challenge(auth: &AuthService, headers: &HeaderMap) -> ApiResult<Challenge> {
    cookie_value(headers, &cookie_name(CHALLENGE_COOKIE, auth.cookie_secure))
        .and_then(|handle| CHALLENGES.take(&handle))
        .ok_or_else(|| ApiError::bad_request("Challenge not found"))
}

/// Counter / backup flags out of the serialized `Passkey`. webauthn-rs does
/// not expose accessors for these persisted attributes, so this reads the
/// 0.5.x serialized shape — the same shape already sitting in the
/// `credential` column — and fails closed if it drifts.
fn snapshot(credential: &WebauthnPasskey) -> ApiResult<(String, i64, bool, &'static str)> {
    let unexpected = |what: &str| ApiError::internal("Unexpected passkey shape", what);
    let serialized = serde_json::to_string(credential)
        .map_err(|error| ApiError::internal("Could not serialize the passkey", error))?;
    let value: Value = serde_json::from_str(&serialized)
        .map_err(|error| ApiError::internal("Could not reparse the passkey", error))?;
    let cred = value
        .get("cred")
        .ok_or_else(|| unexpected("missing cred"))?;
    let counter = cred
        .get("counter")
        .and_then(Value::as_u64)
        .ok_or_else(|| unexpected("missing counter"))?;
    let backed_up = cred
        .get("backup_state")
        .and_then(Value::as_bool)
        .ok_or_else(|| unexpected("missing backup_state"))?;
    let multi_device = cred
        .get("backup_eligible")
        .and_then(Value::as_bool)
        .ok_or_else(|| unexpected("missing backup_eligible"))?;

    Ok((
        serialized,
        i64::try_from(counter).unwrap_or(i64::MAX),
        backed_up,
        if multi_device {
            "multiDevice"
        } else {
            "singleDevice"
        },
    ))
}

/// What the frontend's passkey list renders; the raw row also carries the
/// bulky serialized credential, which nothing client-side needs.
fn passkey_json(row: &passkey::Model) -> Value {
    json!({
        "id": row.id,
        "name": row.name,
        "device_type": row.device_type,
        "backed_up": row.backed_up,
        "transports": row.transports,
        "created_at": row.created_at,
    })
}

pub(super) async fn generate_register_options(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;
    let webauthn = webauthn(auth)?;

    // A registered credential is excluded so the same authenticator is not
    // offered twice for one account.
    let existing = passkey::Entity::find()
        .filter(passkey::Column::UserId.eq(&user.id))
        .all(&auth.db)
        .await?;
    let exclude: Vec<CredentialID> = existing
        .iter()
        .filter_map(|row| URL_SAFE_NO_PAD.decode(&row.credential_id).ok())
        .map(CredentialID::from)
        .collect();

    let handle = user
        .username
        .clone()
        .or_else(|| user.email.clone())
        .unwrap_or_else(|| user.id.clone());

    // The user id inside the credential is a fresh UUID rather than riven's
    // user id: it is written into the authenticator forever, and sign-in
    // never reads it back (rows are matched by credential id).
    let (options, reg_state) = webauthn
        .start_passkey_registration(uuid::Uuid::new_v4(), &handle, &handle, Some(exclude))
        .map_err(|error| ApiError::internal("Could not start passkey registration", error))?;

    let options_json = serde_json::to_value(options.public_key)
        .map_err(|error| ApiError::internal("Could not serialize passkey options", error))?;
    let cookie = store_challenge(
        auth,
        Challenge::Registration {
            user_id: user.id,
            state: Box::new(reg_state),
        },
    );
    Ok(([cookie], Json(options_json)).into_response())
}

#[derive(Deserialize)]
pub(super) struct VerifyRegistration {
    response: Value,
    name: Option<String>,
}

pub(super) async fn verify_registration(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<VerifyRegistration>,
) -> ApiResult<Json<Value>> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;
    let webauthn = webauthn(auth)?;

    let Challenge::Registration {
        user_id,
        state: reg_state,
    } = take_challenge(auth, &headers)?
    else {
        return Err(ApiError::bad_request("Challenge not found"));
    };
    if user_id != user.id {
        return Err(ApiError::forbidden("Not your registration challenge"));
    }

    let registration: RegisterPublicKeyCredential =
        serde_json::from_value(body.response).map_err(|error| {
            tracing::debug!(%error, "malformed passkey registration response");
            ApiError::bad_request("Malformed passkey registration response")
        })?;
    let transports = registration.response.transports.as_ref().map(|transports| {
        transports
            .iter()
            .filter_map(|t| serde_json::to_value(t).ok())
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect::<Vec<_>>()
            .join(",")
    });

    let credential = webauthn
        .finish_passkey_registration(&registration, &reg_state)
        .map_err(|error| {
            tracing::warn!(%error, "passkey registration failed verification");
            ApiError::bad_request("Passkey registration failed")
        })?;

    let (serialized, counter, backed_up, device_type) = snapshot(&credential)?;
    let now = Utc::now();
    let row = passkey::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        name: Set(body.name),
        // Informational column from the better-auth schema; verification uses
        // `credential`, so nothing reads this back.
        public_key: Set(String::new()),
        user_id: Set(user.id),
        credential_id: Set(URL_SAFE_NO_PAD.encode(credential.cred_id().as_ref())),
        counter: Set(counter),
        device_type: Set(device_type.to_string()),
        backed_up: Set(backed_up),
        transports: Set(transports.filter(|t| !t.is_empty())),
        credential: Set(serialized),
        aaguid: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(&auth.db)
    .await?;

    Ok(Json(passkey_json(&row)))
}

pub(super) async fn generate_authenticate_options(
    State(state): State<ApiState>,
) -> ApiResult<Response> {
    let auth = &state.auth;
    let (options, auth_state) = webauthn(auth)?
        .start_discoverable_authentication()
        .map_err(|error| ApiError::internal("Could not start passkey authentication", error))?;

    let mut options_json = serde_json::to_value(options.public_key)
        .map_err(|error| ApiError::internal("Could not serialize passkey options", error))?;
    // Discoverable options carry no allow-list; an empty array would make
    // some browsers treat it as "allow nothing".
    if let Some(root) = options_json.as_object_mut()
        && root
            .get("allowCredentials")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
    {
        let _ = root.remove("allowCredentials");
    }

    let cookie = store_challenge(
        auth,
        Challenge::Authentication {
            state: Box::new(auth_state),
        },
    );
    Ok(([cookie], Json(options_json)).into_response())
}

#[derive(Deserialize)]
pub(super) struct VerifyAuthentication {
    response: Value,
}

pub(super) async fn verify_authentication(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<VerifyAuthentication>,
) -> ApiResult<Response> {
    let auth = &state.auth;
    let webauthn = webauthn(auth)?;
    let failed = || ApiError::unauthorized("Passkey authentication failed");

    let Challenge::Authentication { state: auth_state } = take_challenge(auth, &headers)? else {
        return Err(ApiError::bad_request("Challenge not found"));
    };
    let assertion: PublicKeyCredential =
        serde_json::from_value(body.response).map_err(|error| {
            tracing::debug!(%error, "malformed passkey authentication response");
            ApiError::bad_request("Malformed passkey authentication response")
        })?;

    let credential_id = if assertion.id.is_empty() {
        URL_SAFE_NO_PAD.encode(assertion.raw_id.as_ref())
    } else {
        assertion.id.clone()
    };
    let row = passkey::Entity::find()
        .filter(passkey::Column::CredentialId.eq(&credential_id))
        .one(&auth.db)
        .await?
        .ok_or_else(failed)?;

    let mut credential: WebauthnPasskey = serde_json::from_str(&row.credential)
        .map_err(|error| ApiError::internal("Stored passkey is unreadable", error))?;
    let result = webauthn
        .finish_discoverable_authentication(
            &assertion,
            *auth_state,
            &[DiscoverableKey::from(credential.clone())],
        )
        .map_err(|error| {
            tracing::warn!(%error, "passkey authentication failed verification");
            failed()
        })?;

    // `None` means the assertion came from a different credential than the
    // stored one — which, after the id match above, should not happen.
    if credential.update_credential(&result).is_none() {
        return Err(failed());
    }

    // Persist the updated counter/backup state for clone detection.
    let (serialized, counter, backed_up, device_type) = snapshot(&credential)?;
    passkey::ActiveModel {
        id: Set(row.id.clone()),
        credential: Set(serialized),
        counter: Set(counter),
        backed_up: Set(backed_up),
        device_type: Set(device_type.to_string()),
        updated_at: Set(Utc::now()),
        ..Default::default()
    }
    .update(&auth.db)
    .await?;

    let user = user::Entity::find_by_id(&row.user_id)
        .one(&auth.db)
        .await?
        .ok_or_else(failed)?;
    signed_in_response(auth, &user, &headers).await
}

pub(super) async fn list_user_passkeys(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;
    let passkeys = passkey::Entity::find()
        .filter(passkey::Column::UserId.eq(&user.id))
        .all(&auth.db)
        .await?;
    Ok(Json(passkeys.iter().map(passkey_json).collect()))
}

async fn owned_passkey(
    auth: &AuthService,
    user_id: &str,
    passkey_id: &str,
) -> ApiResult<passkey::Model> {
    passkey::Entity::find_by_id(passkey_id)
        .one(&auth.db)
        .await?
        .filter(|row| row.user_id == user_id)
        .ok_or_else(|| ApiError::bad_request("Passkey not found"))
}

#[derive(Deserialize)]
pub(super) struct DeletePasskey {
    id: String,
}

pub(super) async fn delete_passkey(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<DeletePasskey>,
) -> ApiResult<Json<Value>> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;
    let row = owned_passkey(auth, &user.id, &body.id).await?;
    passkey::Entity::delete_by_id(&row.id)
        .exec(&auth.db)
        .await?;
    Ok(Json(json!({ "status": true })))
}

#[derive(Deserialize)]
pub(super) struct UpdatePasskey {
    id: String,
    name: String,
}

pub(super) async fn update_passkey(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<UpdatePasskey>,
) -> ApiResult<Json<Value>> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;
    let row = owned_passkey(auth, &user.id, &body.id).await?;
    let updated = passkey::ActiveModel {
        id: Set(row.id),
        name: Set(Some(body.name)),
        updated_at: Set(Utc::now()),
        ..Default::default()
    }
    .update(&auth.db)
    .await?;
    Ok(Json(json!({ "passkey": passkey_json(&updated) })))
}
