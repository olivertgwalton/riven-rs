use axum::{extract::State, http::StatusCode, response::IntoResponse};

use super::ApiState;

pub(super) async fn seerr_webhook(State(state): State<ApiState>) -> impl IntoResponse {
    tracing::info!("seerr webhook received, triggering content service");
    riven_queue::flows::request_content::enqueue(&state.job_queue).await;
    StatusCode::OK
}
