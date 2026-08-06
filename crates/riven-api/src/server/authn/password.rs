//! Email/username + password: sign-in, first-user sign-up, password change
//! and password reset.

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;
use chrono::{TimeDelta, Utc};
use riven_core::entities::auth::{account, session, user, verification};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, PaginatorTrait, QueryFilter, Set, TransactionTrait,
};
use serde::Deserialize;
use serde_json::json;

use super::session::require_user;
use super::{ApiError, ApiResult, hash_token, random_token, signed_in_response};
use crate::server::ApiState;

pub const CREDENTIAL_PROVIDER: &str = "credential";
const MIN_PASSWORD_LENGTH: usize = 8;

/// Password-reset tokens live an hour, in `auth_verifications` under this
/// identifier prefix, with the user id as `value`.
///
/// Only the token's SHA-256 is stored, for the same reason session tokens
/// are hashed: a reset token is a bearer credential that mints a password,
/// so a database read must not hand out working ones.
const RESET_PREFIX: &str = "reset-password:";

/// Key for the Postgres advisory lock that serialises the first-user decision.
/// Arbitrary but fixed; it only has to be unique within this database.
const FIRST_USER_LOCK: i64 = 0x7269_7665_6e5f_7531;

/// Serialises "is this account the first one?" across every path that can
/// answer it — password sign-up and OIDC sign-in both make a new user an admin
/// when the table is empty.
///
/// A plain `COUNT` under READ COMMITTED lets two concurrent callers both see an
/// empty table and both insert themselves as admin. The lock is
/// transaction-scoped, so the count and the insert commit together and it
/// releases on commit or rollback — which is why this takes a transaction and
/// not a pooled connection.
pub(super) async fn lock_first_user(tx: &DatabaseTransaction) -> Result<(), sea_orm::DbErr> {
    tx.execute_unprepared(&format!("SELECT pg_advisory_xact_lock({FIRST_USER_LOCK})"))
        .await?;
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct SignIn {
    email: Option<String>,
    username: Option<String>,
    password: String,
}

/// The `credential` account row: where the password hash lives. One per user
/// at most; OAuth/Plex accounts have their own rows with `password = NULL`.
pub(super) async fn credential_account(
    db: &DatabaseConnection,
    user_id: &str,
) -> Result<Option<account::Model>, sea_orm::DbErr> {
    account::Entity::find()
        .filter(account::Column::UserId.eq(user_id))
        .filter(account::Column::ProviderId.eq(CREDENTIAL_PROVIDER))
        .one(db)
        .await
}

pub(super) fn validate_password(password: &str) -> ApiResult<()> {
    if password.len() < MIN_PASSWORD_LENGTH {
        return Err(ApiError::bad_request(format!(
            "Password must be at least {MIN_PASSWORD_LENGTH} characters"
        )));
    }
    Ok(())
}

pub(super) fn normalize_email(email: &str) -> ApiResult<String> {
    let email = email.trim().to_ascii_lowercase();
    if email.len() < 3 || !email.contains('@') {
        return Err(ApiError::bad_request("Invalid email address"));
    }
    Ok(email)
}

/// Lowercased login handle; `display_username` keeps the case as typed.
pub(super) fn normalize_username(username: &str) -> ApiResult<String> {
    let username = username.trim().to_ascii_lowercase();
    if username.is_empty() || username.contains('@') {
        return Err(ApiError::bad_request("Invalid username"));
    }
    Ok(username)
}

/// Both password sign-ins; a miss and a wrong password produce the same
/// generic 401, so the endpoint does not confirm which identifiers exist.
async fn sign_in(state: &ApiState, headers: &HeaderMap, body: SignIn) -> ApiResult<Response> {
    let auth = &state.auth;
    let invalid = || ApiError::unauthorized("Invalid credentials");

    let user = match (&body.email, &body.username) {
        (Some(email), _) => {
            user::Entity::find()
                .filter(user::Column::Email.eq(email.trim().to_ascii_lowercase()))
                .one(&auth.db)
                .await?
        }
        (None, Some(username)) => {
            user::Entity::find()
                .filter(user::Column::Username.eq(username.trim().to_ascii_lowercase()))
                .one(&auth.db)
                .await?
        }
        (None, None) => return Err(ApiError::bad_request("Missing email or username")),
    }
    .ok_or_else(invalid)?;

    let hash = credential_account(&auth.db, &user.id)
        .await?
        .and_then(|account| account.password)
        .ok_or_else(invalid)?;

    let verified = auth
        .hasher
        .verify(&hash, &body.password)
        .await
        .map_err(|error| ApiError::internal("Password verification failed", error))?;
    if !verified {
        return Err(invalid());
    }

    signed_in_response(auth, &user, headers).await
}

pub(super) async fn sign_in_email(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<SignIn>,
) -> ApiResult<Response> {
    sign_in(&state, &headers, body).await
}

pub(super) async fn sign_in_username(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<SignIn>,
) -> ApiResult<Response> {
    sign_in(&state, &headers, body).await
}

#[derive(Deserialize)]
pub(super) struct SignUp {
    username: String,
    email: String,
    password: String,
}

/// Public sign-up is allowed exactly once, for the first account, which
/// becomes the admin. Riven is a private media server: every later user is
/// created by that admin through `/admin/create-user`.
pub(super) async fn sign_up(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<SignUp>,
) -> ApiResult<Response> {
    let auth = &state.auth;
    let tx = auth.db.begin().await?;
    lock_first_user(&tx).await?;
    if user::Entity::find().count(&tx).await? > 0 {
        return Err(ApiError::forbidden(
            "Sign-up is closed: this instance already has an account",
        ));
    }

    let user = create_user_with_password(
        auth,
        &tx,
        &body.username,
        &body.email,
        &body.password,
        "admin",
    )
    .await?;
    tx.commit().await?;
    tracing::info!(user_id = %user.id, "first user created as admin");
    signed_in_response(auth, &user, &headers).await
}

/// Shared by sign-up and `/admin/create-user`. Takes the connection rather than
/// reaching for `auth.db`, so sign-up can run it inside the transaction holding
/// the first-user lock.
pub(super) async fn create_user_with_password(
    auth: &super::AuthService,
    db: &impl ConnectionTrait,
    username: &str,
    email: &str,
    password: &str,
    role: &str,
) -> ApiResult<user::Model> {
    let email = normalize_email(email)?;
    let normalized = normalize_username(username)?;
    validate_password(password)?;

    let taken = user::Entity::find()
        .filter(
            sea_orm::Condition::any()
                .add(user::Column::Email.eq(&email))
                .add(user::Column::Username.eq(&normalized)),
        )
        .one(db)
        .await?
        .is_some();
    if taken {
        return Err(ApiError::bad_request("Email or username is already in use"));
    }

    let hash = auth
        .hasher
        .hash(password)
        .await
        .map_err(|error| ApiError::internal("Password hashing failed", error))?;

    let now = Utc::now();
    let user = user::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        // `name` mirrors `username` (migration m037): one identity, two columns.
        name: Set(Some(normalized.clone())),
        email: Set(Some(email)),
        email_verified: Set(false),
        image: Set(None),
        username: Set(Some(normalized)),
        display_username: Set(Some(username.trim().to_string())),
        role: Set(Some(role.to_string())),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(db)
    .await?;

    insert_credential_account(db, &user.id, hash).await?;
    Ok(user)
}

pub(super) async fn insert_credential_account(
    db: &impl ConnectionTrait,
    user_id: &str,
    password_hash: String,
) -> ApiResult<account::Model> {
    let now = Utc::now();
    account::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        // For the credential provider the "identity at the provider" is the
        // user itself.
        account_id: Set(user_id.to_string()),
        provider_id: Set(CREDENTIAL_PROVIDER.to_string()),
        user_id: Set(user_id.to_string()),
        access_token: Set(None),
        refresh_token: Set(None),
        id_token: Set(None),
        access_token_expires_at: Set(None),
        refresh_token_expires_at: Set(None),
        scope: Set(None),
        password: Set(Some(password_hash)),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(db)
    .await
    .map_err(ApiError::from)
}

/// Whether the sign-up form should be offered — read by the login page, which
/// by definition has no session.
pub(super) async fn first_user_availability(
    State(state): State<ApiState>,
) -> ApiResult<Json<serde_json::Value>> {
    let count = user::Entity::find().count(&state.auth.db).await?;
    Ok(Json(json!({ "available": count == 0 })))
}

#[derive(Deserialize)]
pub(super) struct ChangePassword {
    current_password: String,
    new_password: String,
    #[serde(default)]
    revoke_other_sessions: bool,
}

pub(super) async fn change_password(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<ChangePassword>,
) -> ApiResult<Json<serde_json::Value>> {
    let auth = &state.auth;
    let (user, current_session) = require_user(auth, &headers).await?;
    validate_password(&body.new_password)?;

    let account = credential_account(&auth.db, &user.id)
        .await?
        .ok_or_else(|| ApiError::bad_request("No password is set on this account"))?;
    let hash = account
        .password
        .clone()
        .ok_or_else(|| ApiError::bad_request("No password is set on this account"))?;

    let verified = auth
        .hasher
        .verify(&hash, &body.current_password)
        .await
        .map_err(|error| ApiError::internal("Password verification failed", error))?;
    if !verified {
        return Err(ApiError::bad_request("Invalid password"));
    }

    let new_hash = auth
        .hasher
        .hash(&body.new_password)
        .await
        .map_err(|error| ApiError::internal("Password hashing failed", error))?;
    account::ActiveModel {
        id: Set(account.id),
        password: Set(Some(new_hash)),
        updated_at: Set(Utc::now()),
        ..Default::default()
    }
    .update(&auth.db)
    .await?;

    if body.revoke_other_sessions {
        session::Entity::delete_many()
            .filter(session::Column::UserId.eq(&user.id))
            .filter(session::Column::Id.ne(&current_session.id))
            .exec(&auth.db)
            .await?;
    }

    Ok(Json(json!({ "success": true })))
}

#[derive(Deserialize)]
pub(super) struct RequestPasswordReset {
    email: String,
}

/// Mint a reset token. Riven has no mail provider, so the token goes to the
/// server log for the operator to relay. The response is a constant
/// `{status:true}` whether or not the email matched, so this endpoint cannot
/// be used to enumerate accounts.
pub(super) async fn request_password_reset(
    State(state): State<ApiState>,
    Json(body): Json<RequestPasswordReset>,
) -> ApiResult<Json<serde_json::Value>> {
    let auth = &state.auth;
    let user = user::Entity::find()
        .filter(user::Column::Email.eq(body.email.trim().to_ascii_lowercase()))
        .one(&auth.db)
        .await?;

    if let Some(user) = user {
        let token = random_token();
        let now = Utc::now();
        verification::ActiveModel {
            id: Set(uuid::Uuid::new_v4().to_string()),
            identifier: Set(format!("{RESET_PREFIX}{}", hash_token(&token))),
            value: Set(user.id.clone()),
            expires_at: Set(now + TimeDelta::hours(1)),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&auth.db)
        .await?;

        tracing::info!(
            user_id = %user.id,
            "password reset requested; no mail provider is configured, so relay this \
             token manually (valid one hour): POST /auth/reset-password \
             {{\"token\": \"{token}\", \"new_password\": \"…\"}}"
        );
    } else {
        tracing::info!("password reset requested for an unknown email");
    }

    Ok(Json(json!({ "status": true })))
}

#[derive(Deserialize)]
pub(super) struct ResetPassword {
    token: String,
    new_password: String,
}

pub(super) async fn reset_password(
    State(state): State<ApiState>,
    Json(body): Json<ResetPassword>,
) -> ApiResult<Json<serde_json::Value>> {
    let auth = &state.auth;
    validate_password(&body.new_password)?;

    let invalid = || ApiError::bad_request("Invalid or expired reset token");
    let found = verification::Entity::find()
        .filter(
            verification::Column::Identifier
                .eq(format!("{RESET_PREFIX}{}", hash_token(&body.token))),
        )
        .one(&auth.db)
        .await?
        .ok_or_else(invalid)?;

    // Spent either way: an expired token is deleted rather than left around.
    verification::Entity::delete_by_id(&found.id)
        .exec(&auth.db)
        .await?;
    if found.expires_at <= Utc::now() {
        return Err(invalid());
    }

    let user_id = found.value;
    let hash = auth
        .hasher
        .hash(&body.new_password)
        .await
        .map_err(|error| ApiError::internal("Password hashing failed", error))?;

    // Upsert: a user who only ever signed in via OAuth/Plex gains a password.
    match credential_account(&auth.db, &user_id).await? {
        Some(account) => {
            account::ActiveModel {
                id: Set(account.id),
                password: Set(Some(hash)),
                updated_at: Set(Utc::now()),
                ..Default::default()
            }
            .update(&auth.db)
            .await?;
        }
        None => {
            insert_credential_account(&auth.db, &user_id, hash).await?;
        }
    }

    // A reset means the old credential may be compromised — every session dies.
    session::Entity::delete_many()
        .filter(session::Column::UserId.eq(&user_id))
        .exec(&auth.db)
        .await?;

    tracing::info!(%user_id, "password reset completed; all sessions revoked");
    Ok(Json(json!({ "status": true })))
}
