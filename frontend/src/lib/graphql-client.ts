/**
 * Minimal GraphQL client for communicating with the Rust backend.
 *
 * Every call goes straight to the backend from the browser, carrying the
 * better-auth session cookie. There is no SvelteKit proxy any more: the backend
 * owns authentication, so a proxy would only re-wrap a cookie the backend can
 * read itself.
 *
 * Usage (components and universal `load` functions alike):
 *   import { gqlClient } from '$lib/graphql-client';
 *   const data = await gqlClient<{ removeItems: number }>(MUTATION, vars);
 *
 *   import { gqlSubscribeClient } from '$lib/graphql-client';
 *   const unsubscribe = gqlSubscribeClient<...>(SUBSCRIPTION, vars, { onData, onError });
 *
 * Subscriptions go over a single shared WebSocket via the `graphql-ws`
 * transport, so any number of concurrent subscriptions multiplex onto one
 * TCP connection regardless of HTTP version. This avoids exhausting the
 * per-origin HTTP/1.1 connection cap on bare-HTTP deployments.
 */

import { createClient, type Client as GraphQLWSClient } from "graphql-ws";

interface GraphQLResponse<T> {
	data?: T;
	errors?: Array<{ message: string; locations?: unknown; path?: unknown }>;
}

interface GraphQLSubscribeHandlers<T> {
	onData: (data: T) => void;
	onError?: (error: Error) => void;
}

/**
 * Always same-origin: riven serves this bundle itself, so the API is never on
 * another host. That keeps the session cookie first-party.
 */
const GRAPHQL_URL = "/graphql";
const JSON_CONTENT_TYPE = "application/json";

function getGraphQLData<T>(result: GraphQLResponse<T>): T {
	if (result.errors && result.errors.length > 0) {
		throw new Error(result.errors.map((e) => e.message).join("; "));
	}

	if (result.data === undefined) {
		throw new Error("GraphQL response contained no data");
	}

	return result.data;
}

/// Lazily-constructed singleton `graphql-ws` client. All client-side
/// subscriptions share this one WebSocket, so concurrent subscription
/// count no longer pressures the per-origin HTTP connection cap.
///
/// Constructed on first use because module evaluation runs during SSR
/// where `window` is undefined.
let wsClient: GraphQLWSClient | null = null;

function getWsClient(): GraphQLWSClient {
	if (wsClient) return wsClient;
	if (typeof window === "undefined") {
		throw new Error(
			"gqlSubscribeClient called during SSR (WebSocket unavailable)",
		);
	}
	const httpUrl = new URL(GRAPHQL_URL, window.location.origin);
	httpUrl.protocol = httpUrl.protocol === "https:" ? "wss:" : "ws:";
	wsClient = createClient({
		url: httpUrl.toString(),
		// The browser sends the better-auth session cookie on the upgrade
		// request and the backend authorises the connection from it.
		lazy: true,
		// Reconnect with exponential backoff up to ~20s on transient
		// network failures. graphql-ws handles this automatically once
		// `shouldRetry` is truthy.
		shouldRetry: () => true,
		retryAttempts: Infinity,
		retryWait: (retries) =>
			new Promise((resolve) =>
				setTimeout(resolve, Math.min(1000 * 2 ** retries, 20000)),
			),
	});
	return wsClient;
}

/**
 * Execute a GraphQL operation against the backend.
 *
 * `credentials: "include"` is required: the session cookie is the only
 * credential.
 */
export async function gqlClient<T>(
	query: string,
	variables?: Record<string, unknown>,
	signal?: AbortSignal,
): Promise<T> {
	const response = await fetch(GRAPHQL_URL, {
		method: "POST",
		headers: { "Content-Type": JSON_CONTENT_TYPE },
		credentials: "include",
		body: JSON.stringify({ query, variables: variables ?? {} }),
		signal,
	});

	if (!response.ok) {
		throw new Error(
			`GraphQL request failed: ${response.status} ${response.statusText}`,
		);
	}

	const result: GraphQLResponse<T> = await response.json();

	return getGraphQLData(result);
}

/**
 * Execute a client-side GraphQL subscription over the shared WebSocket
 * connection (graphql-ws / graphql-transport-ws protocol). Any number of
 * concurrent subscriptions multiplex onto one TCP connection.
 *
 * Auth is established at WebSocket upgrade time from the session cookie.
 */
export function gqlSubscribeClient<T>(
	query: string,
	variables: Record<string, unknown> | undefined,
	handlers: GraphQLSubscribeHandlers<T>,
): () => void {
	let active = true;
	const unsubscribe = getWsClient().subscribe<T>(
		{ query, variables: variables ?? {} },
		{
			next: (result) => {
				if (!active) return;
				if (result.errors && result.errors.length > 0) {
					handlers.onError?.(
						new Error(result.errors.map((e) => e.message).join("; ")),
					);
					return;
				}
				if (result.data !== undefined && result.data !== null) {
					handlers.onData(result.data as T);
				}
			},
			error: (err) => {
				if (!active) return;
				handlers.onError?.(err instanceof Error ? err : new Error(String(err)));
			},
			complete: () => {
				if (active) handlers.onError?.(new Error("Stream ended"));
			},
		},
	);

	return () => {
		active = false;
		unsubscribe();
	};
}
