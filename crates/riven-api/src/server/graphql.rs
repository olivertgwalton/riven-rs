use async_graphql::Data;
use async_graphql::http::{
    ALL_WEBSOCKET_PROTOCOLS, GraphiQLSource, is_accept_multipart_mixed,
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
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::time::Duration;

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
            apollo_multipart_stream(stream, Duration::from_secs(30))
                .map(Ok::<_, std::io::Error>),
        );

        return Response::builder()
            .status(StatusCode::OK)
            // Unquoted form is required: Apollo iOS keys its multipart parser
            // table on the literal directive string `subscriptionSpec=1.0`,
            // and rejects responses whose value is quoted.
            .header(
                "content-type",
                "multipart/mixed; boundary=graphql; subscriptionSpec=1.0",
            )
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

/// Multipart subscription stream emitting Apollo's `subscriptionSpec=1.0`
/// wire format: each chunk's body is `{"payload": <graphql response>}` rather
/// than the bare response. async-graphql's `create_multipart_mixed_stream`
/// helper does not wrap the response, which is incompatible with Apollo iOS /
/// Apollo Kotlin / Apollo Router clients (they silently drop unwrapped chunks).
///
/// Reference: <https://www.apollographql.com/docs/router/executing-operations/subscription-multipart-protocol/>
fn apollo_multipart_stream<'a>(
    input: impl Stream<Item = async_graphql::Response> + Send + 'a,
    heartbeat_interval: Duration,
) -> Pin<Box<dyn Stream<Item = Bytes> + Send + 'a>> {
    use serde::Serialize;
    use tokio::time::Instant;

    // Each chunk's boundary is `\r\n--graphql` per RFC 1341. async-graphql's
    // built-in helper omits the leading CRLF on the first part, which the
    // browser frontend's regex-based parser tolerates but Apollo iOS does not
    // (its chunker anchors the boundary search on `\r\n--`, then rejects a
    // chunk that starts with `--` as the close delimiter). Prepend the CRLF
    // here so the very first part is a recognised boundary.
    const PART_HEADER: &[u8] = b"\r\n--graphql\r\nContent-Type: application/json\r\n\r\n";
    const HEARTBEAT_BODY: &[u8] = b"{}\r\n";
    const CRLF: &[u8] = b"\r\n";
    const EOF: &[u8] = b"\r\n--graphql--\r\n";

    #[derive(Serialize)]
    struct Chunk<'a> {
        payload: &'a async_graphql::Response,
    }

    fn make_chunk(payload: &async_graphql::Response) -> Bytes {
        let chunk = Chunk { payload };
        let json = serde_json::to_vec(&chunk).unwrap_or_else(|_| b"null".to_vec());
        let mut buf = Vec::with_capacity(PART_HEADER.len() + json.len() + CRLF.len());
        buf.extend_from_slice(PART_HEADER);
        buf.extend_from_slice(&json);
        buf.extend_from_slice(CRLF);
        Bytes::from(buf)
    }

    fn heartbeat() -> Bytes {
        let mut buf = Vec::with_capacity(PART_HEADER.len() + HEARTBEAT_BODY.len());
        buf.extend_from_slice(PART_HEADER);
        buf.extend_from_slice(HEARTBEAT_BODY);
        Bytes::from(buf)
    }

    enum State<S> {
        Streaming {
            input: Pin<Box<S>>,
            next_heartbeat: Instant,
        },
        Done,
    }

    let initial = State::Streaming {
        input: Box::pin(input),
        next_heartbeat: Instant::now() + heartbeat_interval,
    };

    Box::pin(futures::stream::unfold(initial, move |state| async move {
        match state {
            State::Done => None,
            State::Streaming {
                mut input,
                next_heartbeat,
            } => {
                let timeout = tokio::time::sleep_until(next_heartbeat);
                tokio::pin!(timeout);
                tokio::select! {
                    biased;
                    item = input.next() => match item {
                        Some(resp) => Some((
                            make_chunk(&resp),
                            State::Streaming {
                                input,
                                next_heartbeat: Instant::now() + heartbeat_interval,
                            },
                        )),
                        None => Some((Bytes::from_static(EOF), State::Done)),
                    },
                    _ = &mut timeout => Some((
                        heartbeat(),
                        State::Streaming {
                            input,
                            next_heartbeat: Instant::now() + heartbeat_interval,
                        },
                    )),
                }
            }
        }
    }))
}
