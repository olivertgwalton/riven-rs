use async_graphql::Data;
use async_graphql::http::{ALL_WEBSOCKET_PROTOCOLS, GraphiQLSource};
use async_graphql_axum::{
    GraphQLBatchRequest, GraphQLProtocol, GraphQLResponse, GraphQLWebSocket,
    rejection::GraphQLRejection,
};
use axum::{
    body::Body,
    extract::{FromRequest, FromRequestParts, State, WebSocketUpgrade},
    http::{HeaderMap, Request, StatusCode},
    response::{Html, IntoResponse, Response},
};

use super::ApiState;
use super::auth::{self, authorize_request};

pub(super) async fn graphql_handler(
    State(state): State<ApiState>,
    headers: HeaderMap,
    req: Request<Body>,
) -> Response {
    let auth = match authorize_request(&state, &headers, req.uri().query()).await {
        Ok(auth) => auth,
        Err(auth::Unauthorized) => {
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    };

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
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));

    if is_ws {
        let auth = match authorize_request(&state, req.headers(), req.uri().query()).await {
            Ok(auth) => auth,
            Err(auth::Unauthorized) => {
                return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
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
