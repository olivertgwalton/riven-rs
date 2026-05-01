use axum::http::HeaderMap;
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::schema::auth::{RequestAuth, UserRole};

use super::ApiState;

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
        _ => return Err(AuthError::Forbidden),
    };

    let user_id = headers
        .get(FRONTEND_USER_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or(AuthError::Forbidden)?;

    let timestamp = headers
        .get(FRONTEND_AUTH_TIMESTAMP_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(AuthError::Forbidden)?;

    let now = Utc::now().timestamp();
    if (now - timestamp).abs() > FRONTEND_AUTH_MAX_SKEW_SECS {
        return Err(AuthError::Forbidden);
    }

    let signature = headers
        .get(FRONTEND_AUTH_SIGNATURE_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(AuthError::Forbidden)?;

    let secret = state
        .frontend_auth_signing_secret
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or(AuthError::Forbidden)?;

    let provided_signature = hex::decode(signature).map_err(|_e| AuthError::Forbidden)?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_e| AuthError::Forbidden)?;
    mac.update(signing_payload(user_id, role_header, timestamp).as_bytes());
    mac.verify_slice(&provided_signature)
        .map_err(|_e| AuthError::Forbidden)?;

    let role = match role_header {
        "admin" => UserRole::Admin,
        "manager" => UserRole::Manager,
        "user" => UserRole::User,
        _ => return Err(AuthError::Forbidden),
    };

    Ok(RequestAuth { role })
}
