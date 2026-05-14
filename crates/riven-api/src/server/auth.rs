use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::schema::auth::{RequestAuth, UserRole};

use super::ApiState;

/// Middleware that rejects any request whose `x-api-key` / `Authorization`
/// header doesn't match the configured API key. Apply via
/// `axum::middleware::from_fn_with_state(state.clone(), require_api_key)`
/// to gate route groups (Apalis UI, Apalis job-queue API, the seerr
/// webhook, the static frontend fallback).
///
/// `check_api_key` already returns `true` when no API key is configured,
/// so this middleware is a no-op in that mode and we keep
/// `start_server`'s existing "refuse to start without key + CORS list"
/// invariant as the safety net.
pub(super) async fn require_api_key(
    State(state): State<ApiState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    if !check_api_key(&state, req.headers()) && !peer.ip().is_loopback() {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    next.run(req).await
}

pub(super) const FRONTEND_AUTH_SOURCE_HEADER: &str = "x-riven-auth-source";
pub(super) const FRONTEND_ROLE_HEADER: &str = "x-riven-user-role";
pub(super) const FRONTEND_USER_ID_HEADER: &str = "x-riven-user-id";
pub(super) const FRONTEND_AUTH_TIMESTAMP_HEADER: &str = "x-riven-auth-timestamp";
pub(super) const FRONTEND_AUTH_SIGNATURE_HEADER: &str = "x-riven-auth-signature";
const FRONTEND_AUTH_MAX_SKEW_SECS: i64 = 300;

pub(super) fn check_api_key(state: &ApiState, headers: &HeaderMap) -> bool {
    let Some(ref expected) = state.api_key else {
        return true;
    };
    if expected.is_empty() {
        return true;
    }
    let provided = headers
        .get("x-api-key")
        .or_else(|| headers.get("authorization"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_start_matches("Bearer ").trim());
    provided == Some(expected.as_str())
}

pub(super) enum AuthError {
    Unauthorized,
    Forbidden,
}

fn signing_payload(user_id: &str, role: &str, timestamp: i64) -> String {
    format!("v1\n{user_id}\n{role}\n{timestamp}")
}

pub(super) fn authorize_request(
    state: &ApiState,
    headers: &HeaderMap,
) -> Result<RequestAuth, AuthError> {
    if !check_api_key(state, headers) {
        tracing::warn!("auth rejected: api key missing or mismatched");
        return Err(AuthError::Unauthorized);
    }

    let source = headers
        .get(FRONTEND_AUTH_SOURCE_HEADER)
        .and_then(|value| value.to_str().ok());

    if source != Some("frontend") {
        return Ok(RequestAuth::trusted_api_key());
    }

    let role_header = match headers
        .get(FRONTEND_ROLE_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        Some(role @ ("admin" | "manager" | "user")) => role,
        other => {
            tracing::warn!(
                received = ?other,
                "frontend auth rejected: missing or invalid x-riven-user-role header"
            );
            return Err(AuthError::Forbidden);
        }
    };

    let user_id = match headers
        .get(FRONTEND_USER_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
    {
        Some(id) => id,
        None => {
            tracing::warn!("frontend auth rejected: missing or empty x-riven-user-id header");
            return Err(AuthError::Forbidden);
        }
    };

    let timestamp = match headers
        .get(FRONTEND_AUTH_TIMESTAMP_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
    {
        Some(ts) => ts,
        None => {
            tracing::warn!(
                user_id,
                "frontend auth rejected: missing or unparseable x-riven-auth-timestamp header"
            );
            return Err(AuthError::Forbidden);
        }
    };

    let now = Utc::now().timestamp();
    let skew = now - timestamp;
    if skew.abs() > FRONTEND_AUTH_MAX_SKEW_SECS {
        tracing::warn!(
            user_id,
            client_timestamp = timestamp,
            server_timestamp = now,
            skew_secs = skew,
            max_skew_secs = FRONTEND_AUTH_MAX_SKEW_SECS,
            "frontend auth rejected: clock skew exceeds maximum (check NTP on host)"
        );
        return Err(AuthError::Forbidden);
    }

    let signature = match headers
        .get(FRONTEND_AUTH_SIGNATURE_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        Some(sig) => sig,
        None => {
            tracing::warn!(
                user_id,
                "frontend auth rejected: missing x-riven-auth-signature header"
            );
            return Err(AuthError::Forbidden);
        }
    };

    let secret = match state
        .frontend_auth_signing_secret
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        Some(s) => s,
        None => {
            tracing::warn!(
                user_id,
                "frontend auth rejected: backend RIVEN_SETTING__FRONTEND_AUTH_SIGNING_SECRET is unset or empty"
            );
            return Err(AuthError::Forbidden);
        }
    };

    let provided_signature = match hex::decode(signature) {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(
                user_id,
                error = %error,
                "frontend auth rejected: x-riven-auth-signature is not valid hex"
            );
            return Err(AuthError::Forbidden);
        }
    };
    let mut mac = match Hmac::<Sha256>::new_from_slice(secret.as_bytes()) {
        Ok(mac) => mac,
        Err(error) => {
            tracing::warn!(
                user_id,
                error = %error,
                "frontend auth rejected: failed to initialise HMAC with configured secret"
            );
            return Err(AuthError::Forbidden);
        }
    };
    mac.update(signing_payload(user_id, role_header, timestamp).as_bytes());
    if let Err(error) = mac.verify_slice(&provided_signature) {
        tracing::warn!(
            user_id,
            role = role_header,
            timestamp,
            error = %error,
            "frontend auth rejected: HMAC signature mismatch (frontend and backend secrets differ, \
             or signing payload format differs from `v1\\n{{user_id}}\\n{{role}}\\n{{timestamp}}`)"
        );
        return Err(AuthError::Forbidden);
    }

    let role = match role_header {
        "admin" => UserRole::Admin,
        "manager" => UserRole::Manager,
        "user" => UserRole::User,
        _ => return Err(AuthError::Forbidden),
    };

    tracing::debug!(user_id, role = role_header, "frontend auth accepted");
    Ok(RequestAuth { role })
}
