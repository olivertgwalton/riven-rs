use axum::http::HeaderMap;

use super::ApiState;

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
