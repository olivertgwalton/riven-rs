use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use apalis_board_api::ui::ServeUI;

pub(super) async fn board_assets_middleware(
    uri: axum::http::Uri,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = uri.path();
    if path.contains('.')
        && let Some(file) = ServeUI::get_file(path)
    {
        let bytes = file.contents().to_vec();
        let content_type = ServeUI::content_type(path);
        let mut builder = axum::http::Response::builder()
            .status(200)
            .header("content-type", content_type);
        if let Some(cc) = ServeUI::cache_control(path) {
            builder = builder.header("cache-control", cc);
        }
        return builder
            .body(axum::body::Body::from(bytes))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }
    next.run(req).await
}
