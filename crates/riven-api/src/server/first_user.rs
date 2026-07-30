//! First-user sign-up.
//!
//! Riven is a private media server, so `/auth/sign-up/email` must not stay open
//! — but a fresh install has no users and therefore no way in. The rule is
//! "sign-up is allowed exactly until the first account exists, and that account
//! is an admin", and it is enforced by a store hook rather than by a bespoke
//! route: sign-up then keeps better-auth's own validation, password hashing,
//! account creation and auto sign-in.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use better_auth::seaorm::{HookControl, SeaOrmHookContext, SeaOrmHooks};
use better_auth_core::types::CreateUser;
use better_auth_core::types::ListUsersParams;
use better_auth_core::{AuthError, AuthResult};
use riven_core::entities::auth::RivenAuthSchema;
use riven_core::entities::auth::user;
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, PaginatorTrait};
use serde_json::json;

use super::ApiState;
use super::authn::RivenAuth;

/// Path suffix of better-auth's public sign-up route. Compared as a suffix
/// because axum's `nest` strips the `/auth` prefix before the handler sees it.
const SIGN_UP_PATH: &str = "/sign-up/email";

/// Key for the Postgres advisory lock that serialises sign-up. Arbitrary but
/// fixed; it only has to be unique within this database.
const FIRST_USER_LOCK: i64 = 0x7269_7665_6e5f_7531;

/// Makes the first account an admin and refuses every later public sign-up.
///
/// This runs inside better-auth's own sign-up transaction, so the "is the table
/// empty" check and the insert commit together.
pub struct FirstUserIsAdmin;

#[async_trait::async_trait]
impl SeaOrmHooks<RivenAuthSchema> for FirstUserIsAdmin {
    async fn before_create_user(
        &self,
        create_user: &mut CreateUser,
        ctx: &SeaOrmHookContext<'_>,
    ) -> AuthResult<HookControl> {
        // Only public sign-up is gated. An admin adding users through
        // `/admin/create-user` reaches the same store method and must not be
        // caught by this.
        let is_public_sign_up = ctx
            .request
            .as_ref()
            .is_some_and(|request| request.path.ends_with(SIGN_UP_PATH));
        if !is_public_sign_up {
            return Ok(HookControl::Continue);
        }

        // A plain `COUNT` under READ COMMITTED would let two concurrent sign-ups
        // both see an empty table. The lock is transaction-scoped, so it is
        // released by the same commit that inserts the row.
        if let Some(tx) = ctx.tx
            && tx.get_database_backend() == DatabaseBackend::Postgres
        {
            tx.execute_unprepared(&format!("SELECT pg_advisory_xact_lock({FIRST_USER_LOCK})"))
                .await
                .map_err(|error| AuthError::internal(format!("Could not lock sign-up: {error}")))?;
        }

        if count_users(ctx).await? > 0 {
            tracing::warn!("public sign-up refused: this instance already has an account");
            return Ok(HookControl::Cancel);
        }

        create_user.role = Some("admin".to_string());
        // Nothing here can send mail, so an unverified first admin would be
        // locked out of anything gated on verification.
        create_user.email_verified = Some(true);
        tracing::info!("first user is being created as an admin");
        Ok(HookControl::Continue)
    }
}

async fn count_users(ctx: &SeaOrmHookContext<'_>) -> AuthResult<u64> {
    let query = user::Entity::find();
    let result = match ctx.tx {
        Some(tx) => query.count(tx).await,
        None => query.count(ctx.db).await,
    };
    result.map_err(|error| AuthError::internal(format!("Could not count users: {error}")))
}

/// Whether the sign-up form should be offered.
///
/// Unauthenticated by necessity — it is read by the login page, which by
/// definition has no session. It reveals only what the presence of a sign-up
/// form would reveal anyway.
pub(super) async fn availability(State(state): State<ApiState>) -> Response {
    match user_count(&state.auth).await {
        Ok(count) => Json(json!({ "available": count == 0 })).into_response(),
        Err(response) => response,
    }
}

async fn user_count(auth: &Arc<RivenAuth>) -> Result<usize, Response> {
    auth.store()
        .list_users(ListUsersParams {
            limit: Some(1),
            ..Default::default()
        })
        .await
        .map(|(_, total)| total)
        .map_err(|error| {
            tracing::warn!(%error, "could not count users");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": "Could not reach the user store" })),
            )
                .into_response()
        })
}
