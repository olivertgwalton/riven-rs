use async_graphql::Data;
use async_graphql::http::{
    ALL_WEBSOCKET_PROTOCOLS, GraphiQLSource, create_multipart_mixed_stream,
    is_accept_multipart_mixed,
};
use async_graphql_axum::{
    GraphQLBatchRequest, GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket,
    rejection::GraphQLRejection,
};
use axum::{
    body::Body,
    extract::{FromRequest, FromRequestParts, State, WebSocketUpgrade},
    http::{HeaderMap, Request, StatusCode},
    response::{Html, IntoResponse, Response},
};
use futures::StreamExt;

use super::ApiState;
use super::auth::{AuthError, authorize_request};

pub(super) async fn graphql_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    req: Request<Body>,
) -> Response {
    let auth = match authorize_request(&state, &headers) {
        Ok(auth) => auth,
        Err(AuthError::Unauthorized) => {
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
        Err(AuthError::Forbidden) => return (StatusCode::FORBIDDEN, "Forbidden").into_response(),
    };

    let accepts_multipart = headers
        .get("accept")
        .and_then(|value| value.to_str().ok())
        .map(is_accept_multipart_mixed)
        .unwrap_or_default();

    if accepts_multipart {
        let req = match GraphQLRequest::<GraphQLRejection>::from_request(req, &()).await {
            Ok(req) => req,
            Err(error) => return error.into_response(),
        };
        let stream = state.schema.execute_stream(req.into_inner().data(auth));
        let body = Body::from_stream(
            create_multipart_mixed_stream(stream, std::time::Duration::from_secs(30))
                .map(Ok::<_, std::io::Error>),
        );

        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "multipart/mixed; boundary=graphql")
            .body(body)
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    let req = match GraphQLBatchRequest::<GraphQLRejection>::from_request(req, &()).await {
        Ok(req) => req,
        Err(error) => return error.into_response(),
    };

    let gql_resp: GraphQLResponse = state
        .schema
        .execute_batch(req.into_inner().data(auth))
        .await
        .into();
    gql_resp.into_response()
}

pub(super) async fn graphql_get_handler(
    State(state): State<ApiState>,
    req: Request<Body>,
) -> Response {
    let is_ws = req
        .headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);

    if is_ws {
        let auth = match authorize_request(&state, req.headers()) {
            Ok(auth) => auth,
            Err(AuthError::Unauthorized) => {
                return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
            }
            Err(AuthError::Forbidden) => {
                return (StatusCode::FORBIDDEN, "Forbidden").into_response();
            }
        };
        let (mut parts, _body) = req.into_parts();
        let protocol = match GraphQLProtocol::from_request_parts(&mut parts, &()).await {
            Ok(p) => p,
            Err(e) => return e.into_response(),
        };
        let ws = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
            Ok(ws) => ws,
            Err(e) => return e.into_response(),
        };
        let schema = state.schema.clone();
        let mut connection_data = Data::default();
        connection_data.insert(auth);
        return ws
            .protocols(ALL_WEBSOCKET_PROTOCOLS)
            .on_upgrade(move |socket| {
                GraphQLWebSocket::new(socket, schema, protocol)
                    .with_data(connection_data)
                    .serve()
            })
            .into_response();
    }

    Html(GraphiQLSource::build().endpoint("/graphql").finish()).into_response()
}
