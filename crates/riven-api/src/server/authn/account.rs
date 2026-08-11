//! Account management: the signed-in user's own account, plus the admin's
//! user management. Users and sessions serialize as their entities; account
//! rows are mapped by hand because they carry the password hash and provider
//! tokens.

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, header::SET_COOKIE};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use riven_core::auth::UserRole;
use riven_core::entities::auth::{account, passkey, session, user, verification};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, TransactionTrait,
};
use serde::Deserialize;
use serde_json::json;

use super::password::{
    CREDENTIAL_PROVIDER, create_user_with_password, credential_account, normalize_email,
    normalize_username,
};
use super::session::{
    SessionState, authenticate, clear_session_cookie, require_user, revoke_session,
};
use super::{ApiError, ApiResult};
use crate::server::ApiState;
use crate::server::auth::role_from_user;

pub(super) async fn get_session(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    match authenticate(&state.auth, &headers).await? {
        SessionState::Valid { user, session } => {
            Ok(Json(json!({ "user": user, "session": session })))
        }
        _ => Err(ApiError::unauthorized("Not signed in")),
    }
}

pub(super) async fn sign_out(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let auth = &state.auth;
    revoke_session(auth, &headers).await?;
    Ok((
        [(SET_COOKIE, clear_session_cookie(auth.cookie_secure))],
        Json(json!({ "success": true })),
    )
        .into_response())
}

#[derive(Deserialize)]
pub(super) struct UpdateUser {
    username: Option<String>,
    image: Option<String>,
}

/// Partial update: omitted fields are left alone. Email is deliberately not
/// accepted here — `/change-email` owns it.
pub(super) async fn update_user(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<UpdateUser>,
) -> ApiResult<Json<serde_json::Value>> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;
    if body.username.is_none() && body.image.is_none() {
        return Err(ApiError::bad_request("No fields to update"));
    }

    let mut update = user::ActiveModel {
        id: Set(user.id.clone()),
        updated_at: Set(Utc::now()),
        ..Default::default()
    };

    if let Some(raw) = &body.username {
        let username = normalize_username(raw)?;
        let taken = user::Entity::find()
            .filter(user::Column::Username.eq(&username))
            .filter(user::Column::Id.ne(&user.id))
            .one(&auth.db)
            .await?
            .is_some();
        if taken {
            return Err(ApiError::bad_request("Username is already taken"));
        }
        // `name` mirrors `username` (migration m037): one identity, two columns.
        update.username = Set(Some(username.clone()));
        update.name = Set(Some(username));
        update.display_username = Set(Some(raw.trim().to_string()));
    }
    if let Some(image) = body.image {
        let image = image.trim().to_string();
        update.image = Set((!image.is_empty()).then_some(image));
    }

    update.update(&auth.db).await?;
    Ok(Json(json!({ "status": true })))
}

/// Confirms whoever is holding the session also knows the password.
///
/// `Ok(false)` means there is nothing to confirm against: the account signs in
/// through OAuth or a passkey and has no credential row, so the session itself
/// is the only proof available. Callers decide whether that is good enough.
async fn confirm_password(
    auth: &super::AuthService,
    user_id: &str,
    password: Option<&str>,
) -> ApiResult<bool> {
    let Some(hash) = credential_account(&auth.db, user_id)
        .await?
        .and_then(|account| account.password)
    else {
        return Ok(false);
    };
    let password =
        password.ok_or_else(|| ApiError::bad_request("Your current password is required"))?;
    let verified = auth
        .hasher
        .verify(&hash, password)
        .await
        .map_err(|error| ApiError::internal("Password verification failed", error))?;
    if !verified {
        return Err(ApiError::bad_request("Invalid password"));
    }
    Ok(true)
}

#[derive(Deserialize)]
pub(super) struct ChangeEmail {
    new_email: String,
    /// Absent only for accounts that have no password at all.
    current_password: Option<String>,
}

/// Direct update, no confirmation mail — riven has no mail provider, and a
/// gate that can never be passed is worse than no gate. The password is the
/// gate instead: the address is the handle a password reset is sent to, so
/// letting a borrowed session move it hands over the account.
pub(super) async fn change_email(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<ChangeEmail>,
) -> ApiResult<Json<serde_json::Value>> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;
    confirm_password(auth, &user.id, body.current_password.as_deref()).await?;
    let email = normalize_email(&body.new_email)?;

    let taken = user::Entity::find()
        .filter(user::Column::Email.eq(&email))
        .filter(user::Column::Id.ne(&user.id))
        .one(&auth.db)
        .await?
        .is_some();
    if taken {
        return Err(ApiError::bad_request("Email is already in use"));
    }

    user::ActiveModel {
        id: Set(user.id),
        email: Set(Some(email)),
        email_verified: Set(false),
        updated_at: Set(Utc::now()),
        ..Default::default()
    }
    .update(&auth.db)
    .await?;
    Ok(Json(json!({ "status": true })))
}

#[derive(Deserialize)]
pub(super) struct DeleteUser {
    password: String,
}

/// Deletes the caller's own account. The password is the confirmation step:
/// without it, a borrowed session could destroy the account.
pub(super) async fn delete_user(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<DeleteUser>,
) -> ApiResult<Response> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;

    if !confirm_password(auth, &user.id, Some(&body.password)).await? {
        return Err(ApiError::bad_request(
            "No password is set on this account; ask an admin to remove it",
        ));
    }

    let tx = auth.db.begin().await?;
    delete_user_rows(&tx, &user.id).await?;
    tx.commit().await?;
    tracing::info!(user_id = %user.id, "account self-deleted");
    Ok((
        [(SET_COOKIE, clear_session_cookie(auth.cookie_secure))],
        Json(json!({ "success": true })),
    )
        .into_response())
}

/// Everything hanging off a user, children first. Runs inside the caller's
/// transaction: a failure partway through would otherwise strand a user with
/// no credential, or credentials with no user.
async fn delete_user_rows(db: &impl ConnectionTrait, user_id: &str) -> Result<(), sea_orm::DbErr> {
    session::Entity::delete_many()
        .filter(session::Column::UserId.eq(user_id))
        .exec(db)
        .await?;
    account::Entity::delete_many()
        .filter(account::Column::UserId.eq(user_id))
        .exec(db)
        .await?;
    passkey::Entity::delete_many()
        .filter(passkey::Column::UserId.eq(user_id))
        .exec(db)
        .await?;
    // Pending password resets point at the user via `value`.
    verification::Entity::delete_many()
        .filter(verification::Column::Value.eq(user_id))
        .exec(db)
        .await?;
    user::Entity::delete_by_id(user_id).exec(db).await?;
    Ok(())
}

pub(super) async fn list_accounts(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;
    let accounts = account::Entity::find()
        .filter(account::Column::UserId.eq(&user.id))
        .all(&auth.db)
        .await?;
    Ok(Json(
        accounts
            .iter()
            .map(|a| {
                json!({
                    "id": a.id,
                    "provider_id": a.provider_id,
                    "account_id": a.account_id,
                    "created_at": a.created_at,
                })
            })
            .collect(),
    ))
}

#[derive(Deserialize)]
pub(super) struct UnlinkAccount {
    provider_id: String,
}

pub(super) async fn unlink_account(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<UnlinkAccount>,
) -> ApiResult<Json<serde_json::Value>> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;

    // The credential row is where the password hash lives, and the password is
    // what `/change-email` and `/delete-user` confirm against. Deleting it here
    // would disarm both gates without ever proving the password — a borrowed
    // session could unlink, then move the email it holds. Passwords are managed
    // through `/change-password`, which does prove it.
    if body.provider_id == CREDENTIAL_PROVIDER {
        return Err(ApiError::bad_request(
            "The password sign-in can't be unlinked; change your password instead",
        ));
    }

    let accounts = account::Entity::find()
        .filter(account::Column::UserId.eq(&user.id))
        .all(&auth.db)
        .await?;
    let matching: Vec<&account::Model> = accounts
        .iter()
        .filter(|a| a.provider_id == body.provider_id)
        .collect();
    if matching.is_empty() {
        return Err(ApiError::bad_request("No such linked account"));
    }
    // Removing the last sign-in method would strand the user permanently.
    if matching.len() == accounts.len() {
        return Err(ApiError::bad_request("You can't unlink your last account"));
    }

    let ids: Vec<String> = matching.iter().map(|a| a.id.clone()).collect();
    account::Entity::delete_many()
        .filter(account::Column::Id.is_in(ids))
        .exec(&auth.db)
        .await?;
    Ok(Json(json!({ "status": true })))
}

pub(super) async fn list_sessions(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<session::Model>>> {
    let auth = &state.auth;
    let (user, _) = require_user(auth, &headers).await?;
    let sessions = session::Entity::find()
        .filter(session::Column::UserId.eq(&user.id))
        .filter(session::Column::ExpiresAt.gt(Utc::now()))
        .all(&auth.db)
        .await?;
    Ok(Json(sessions))
}

// --- Admin ------------------------------------------------------------------

async fn require_admin(state: &ApiState, headers: &HeaderMap) -> ApiResult<user::Model> {
    let (user, _) = require_user(&state.auth, headers).await?;
    if role_from_user(user.role.as_deref()) != UserRole::Admin {
        return Err(ApiError::forbidden("Admin role required"));
    }
    Ok(user)
}

#[derive(Deserialize)]
pub(super) struct ListUsersQuery {
    limit: Option<u64>,
    sort_by: Option<String>,
    sort_direction: Option<String>,
}

pub(super) async fn list_users(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<ListUsersQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let _ = require_admin(&state, &headers).await?;
    let db = &state.auth.db;

    let column = match query.sort_by.as_deref() {
        Some("name") => user::Column::Name,
        Some("email") => user::Column::Email,
        Some("username") => user::Column::Username,
        Some("role") => user::Column::Role,
        _ => user::Column::CreatedAt,
    };
    let order = if query.sort_direction.as_deref() == Some("desc") {
        sea_orm::Order::Desc
    } else {
        sea_orm::Order::Asc
    };

    let total = user::Entity::find().count(db).await?;
    let users = user::Entity::find()
        .order_by(column, order)
        .limit(query.limit.unwrap_or(100))
        .all(db)
        .await?;
    Ok(Json(json!({ "users": users, "total": total })))
}

#[derive(Deserialize)]
pub(super) struct CreateUser {
    username: String,
    email: String,
    password: String,
    role: Option<String>,
}

pub(super) async fn create_user(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<CreateUser>,
) -> ApiResult<Json<serde_json::Value>> {
    let _ = require_admin(&state, &headers).await?;
    let role = match body.role.as_deref().unwrap_or("user") {
        role @ ("admin" | "manager" | "user") => role,
        other => return Err(ApiError::bad_request(format!("Unknown role: {other}"))),
    };
    let user = create_user_with_password(
        &state.auth,
        &state.auth.db,
        &body.username,
        &body.email,
        &body.password,
        role,
    )
    .await?;
    Ok(Json(json!({ "user": user })))
}

#[derive(Deserialize)]
pub(super) struct UpdateUserRole {
    user_id: String,
    role: String,
}

pub(super) async fn update_user_role(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<UpdateUserRole>,
) -> ApiResult<Json<serde_json::Value>> {
    let caller = require_admin(&state, &headers).await?;
    let db = &state.auth.db;

    let role = match body.role.as_str() {
        role @ ("admin" | "manager" | "user") => role,
        other => return Err(ApiError::bad_request(format!("Unknown role: {other}"))),
    };

    if body.user_id == caller.id {
        return Err(ApiError::bad_request("Cannot change your own role"));
    }

    // Demoting the last admin would leave an instance nobody can administer.
    // `target` is fetched (and locked) inside the transaction, not before it:
    // reading it beforehand would let a concurrent request promote it to
    // admin in between, so this guard would evaluate against a stale
    // "not admin" role and skip the admin count entirely. Counting and
    // updating as separate statements would race too — two admins demoting
    // each other at the same time could each see "2 admins" and both
    // proceed, leaving zero. `FOR UPDATE` inside a transaction locks every
    // admin row for its duration, so a concurrent demotion blocks until this
    // one commits (or rolls back) and then re-counts against the result.
    let tx = db.begin().await?;
    let target = user::Entity::find_by_id(&body.user_id)
        .lock_exclusive()
        .one(&tx)
        .await?
        .ok_or_else(|| ApiError::bad_request("No such user"))?;
    if role_from_user(target.role.as_deref()) == UserRole::Admin && role != "admin" {
        let admin_ids: Vec<String> = user::Entity::find()
            .filter(user::Column::Role.eq("admin"))
            .select_only()
            .column(user::Column::Id)
            .lock_exclusive()
            .into_tuple()
            .all(&tx)
            .await?;
        if admin_ids.len() <= 1 {
            return Err(ApiError::bad_request("Cannot demote the last admin"));
        }
    }

    let updated = user::ActiveModel {
        id: Set(target.id),
        role: Set(Some(role.to_string())),
        updated_at: Set(Utc::now()),
        ..Default::default()
    }
    .update(&tx)
    .await?;
    tx.commit().await?;
    tracing::info!(user_id = %updated.id, role, by = %caller.id, "user role changed by admin");
    Ok(Json(json!({ "user": updated })))
}

#[derive(Deserialize)]
pub(super) struct RemoveUser {
    user_id: String,
}

pub(super) async fn remove_user(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(body): Json<RemoveUser>,
) -> ApiResult<Json<serde_json::Value>> {
    let caller = require_admin(&state, &headers).await?;
    let db = &state.auth.db;

    if body.user_id == caller.id {
        return Err(ApiError::bad_request(
            "Use delete-user to remove your own account",
        ));
    }
    let target = user::Entity::find_by_id(&body.user_id)
        .one(db)
        .await?
        .ok_or_else(|| ApiError::bad_request("No such user"))?;

    // Removing the last admin would leave an instance nobody can administer.
    if role_from_user(target.role.as_deref()) == UserRole::Admin {
        let admins = user::Entity::find()
            .filter(user::Column::Role.eq("admin"))
            .count(db)
            .await?;
        if admins <= 1 {
            return Err(ApiError::bad_request("Cannot remove the last admin"));
        }
    }

    let tx = db.begin().await?;
    delete_user_rows(&tx, &target.id).await?;
    tx.commit().await?;
    tracing::info!(user_id = %target.id, by = %caller.id, "user removed by admin");
    Ok(Json(json!({ "success": true })))
}
