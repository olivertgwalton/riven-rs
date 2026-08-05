import { createPasskey, getPasskeyAssertion } from "$lib/passkeys";

/**
 * Thin client for the backend's authentication endpoints.
 *
 * The backend mounts `better-auth` at `/auth` and owns sessions, password
 * verification, 2FA, passkeys and API keys. This file is only an HTTP client for
 * that surface — there is deliberately no `better-auth` package dependency, so
 * the frontend cannot drift out of wire-compatibility with an alpha Rust
 * reimplementation of the protocol. The routes below are the stable REST surface
 * (`/auth/sign-in/username`, `/auth/get-session`, …), not SDK internals.
 *
 * Always same-origin: riven serves this bundle itself.
 */
const AUTH_URL = "/auth";

/**
 * There is no `name` here on purpose. An account is a username and an email;
 * `name` is only a field better-auth's own request shapes insist on, and the
 * database keeps it equal to the username (migration
 * `037_auth_username_is_name.sql`), so it is never a second identity. The
 * wrappers below fill it in where the wire demands it, so no call site has to
 * think about it.
 */
export interface AuthUser {
	id: string;
	email?: string | null;
	image?: string | null;
	username?: string | null;
	displayUsername?: string | null;
	role?: string | null;
	emailVerified?: boolean;
	twoFactorEnabled?: boolean;
	createdAt?: string;
	updatedAt?: string;
}

export interface AuthSession {
	id: string;
	userId: string;
	expiresAt: string;
}

/** A row from `/list-accounts`: one authentication method linked to the user. */
export interface LinkedAccount {
	id?: string;
	providerId: string;
	accountId: string;
	createdAt?: string;
	updatedAt?: string;
	scopes?: string[];
}

/** One entry from `/oidc-providers`: a configured provider whose issuer
 * resolved via OIDC discovery, so it is actually usable right now. */
export interface OidcProviderSummary {
	id: string;
	name: string;
}

export interface AuthResult<T> {
	data: T | null;
	error: { message: string; status: number } | null;
}

/** `name` and `transports` are omitted rather than nulled when unset. */
export interface Passkey {
	id: string;
	name?: string | null;
	createdAt: string;
	deviceType: string;
	backedUp: boolean;
	transports?: string;
}

/**
 * Every call carries the session cookie — it is the only credential, and a
 * cross-origin request drops it without `credentials: "include"`.
 *
 * Errors are returned rather than thrown so callers can render them inline;
 * that matches how the previous SDK behaved and keeps call sites unchanged.
 */
async function call<T>(
	path: string,
	init?: { method?: string; body?: unknown },
): Promise<AuthResult<T>> {
	let response: Response;
	try {
		response = await fetch(`${AUTH_URL}${path}`, {
			method: init?.method ?? "GET",
			credentials: "include",
			headers: init?.body ? { "Content-Type": "application/json" } : undefined,
			body: init?.body ? JSON.stringify(init.body) : undefined,
		});
	} catch {
		return {
			data: null,
			error: { message: "Could not reach the server", status: 0 },
		};
	}

	const text = await response.text();
	const payload = text ? safeParse(text) : null;

	if (!response.ok) {
		const message =
			(payload as { message?: string } | null)?.message ??
			`Request failed (${response.status})`;
		return { data: null, error: { message, status: response.status } };
	}

	return { data: payload as T, error: null };
}

const EMPTY_RESPONSE = {
	message: "Server returned an empty response",
	status: 0,
};

function safeParse(text: string): unknown {
	try {
		return JSON.parse(text);
	} catch {
		return null;
	}
}

export const authClient = {
	/** `null` when unauthenticated — a 401 here is expected, not an error. */
	async getSession(): Promise<
		AuthResult<{ user: AuthUser; session: AuthSession } | null>
	> {
		const result = await call<{ user: AuthUser; session: AuthSession }>(
			"/get-session",
		);
		if (result.error?.status === 401) {
			return { data: null, error: null };
		}
		return result;
	},

	signIn: {
		username(body: { username: string; password: string }) {
			return call<{ user: AuthUser; token?: string }>("/sign-in/username", {
				method: "POST",
				body,
			});
		},
		email(body: { email: string; password: string }) {
			return call<{ user: AuthUser; token?: string }>("/sign-in/email", {
				method: "POST",
				body,
			});
		},

		/**
		 * With no session the backend issues discoverable options — no
		 * `allowCredentials` — so the authenticator picks the account and the
		 * user never types a username.
		 */
		async passkey(init?: { conditional?: boolean; signal?: AbortSignal }) {
			const { data: options, error } =
				await authClient.passkey.generateAuthenticateOptions();
			if (error || !options) {
				return { data: null, error: error ?? EMPTY_RESPONSE };
			}
			const assertion = await getPasskeyAssertion(options, init);
			return call<{ user: AuthUser; session: AuthSession }>(
				"/passkey/verify-authentication",
				{ method: "POST", body: { response: assertion } },
			);
		},

		/**
		 * Starts an OIDC sign-in: the backend returns the provider's
		 * authorization URL rather than a redirect response — `disableRedirect`
		 * is what asks for that, since a `fetch()` call cannot hand a 302 back to
		 * the browser for a top-level navigation. The caller navigates
		 * `window.location` to the returned `url` itself.
		 */
		social(body: { provider: string; callbackURL: string }) {
			return call<{ url: string }>("/sign-in/social", {
				method: "POST",
				body: { ...body, disableRedirect: true },
			});
		},
	},

	/**
	 * Only the first account can be created this way, and it becomes the admin.
	 * The backend refuses every later sign-up, so ask `firstUserAvailable()`
	 * before offering the form.
	 */
	signUp: {
		email(body: { username: string; email: string; password: string }) {
			return call<{ user: AuthUser; token?: string }>("/sign-up/email", {
				method: "POST",
				body: { ...body, name: body.username },
			});
		},
	},

	firstUserAvailable() {
		return call<{ available: boolean }>("/first-user");
	},

	/** Only providers that resolved via OIDC discovery at startup — see
	 * `oidc::resolve_providers` on the backend. */
	oidcProviders() {
		return call<OidcProviderSummary[]>("/oidc-providers");
	},

	/**
	 * `poll` answers `{ pending: true }` with a 202 until the user approves.
	 *
	 * `handle` is an opaque token for this sign-in attempt, not the Plex PIN id.
	 * The id used to be the path parameter, which made polling enumerable — PIN
	 * ids are sequential, and a poll that finds an approved PIN sets a session
	 * cookie. Only the caller that ran `start` holds the handle.
	 */
	plex: {
		start() {
			return call<{ handle: string; auth_url: string }>("/plex/start", {
				method: "POST",
			});
		},
		poll(handle: string) {
			return call<{ pending: boolean }>(
				`/plex/poll/${encodeURIComponent(handle)}`,
			);
		},
	},

	passkey: {
		/**
		 * `name` labels the credential in riven's own list; browsers ignore it
		 * and use their own naming in the OS keychain.
		 */
		generateRegisterOptions(options?: {
			name?: string;
			authenticatorAttachment?: "platform" | "cross-platform";
		}) {
			const query = new URLSearchParams();
			if (options?.name) query.set("name", options.name);
			if (options?.authenticatorAttachment) {
				query.set("authenticatorAttachment", options.authenticatorAttachment);
			}
			const suffix = query.size ? `?${query}` : "";
			return call<PublicKeyCredentialCreationOptionsJSON>(
				`/passkey/generate-register-options${suffix}`,
			);
		},

		generateAuthenticateOptions() {
			return call<PublicKeyCredentialRequestOptionsJSON>(
				"/passkey/generate-authenticate-options",
			);
		},

		/** Registers a passkey against the *current* session's user. */
		async add(options?: { name?: string }) {
			const { data: creationOptions, error } =
				await authClient.passkey.generateRegisterOptions(options);
			if (error || !creationOptions) {
				return { data: null, error: error ?? EMPTY_RESPONSE };
			}
			const credential = await createPasskey(creationOptions);
			return call<Passkey>("/passkey/verify-registration", {
				method: "POST",
				body: { response: credential, name: options?.name },
			});
		},

		list() {
			return call<Passkey[]>("/passkey/list-user-passkeys");
		},

		remove(body: { id: string }) {
			return call<{ status: boolean }>("/passkey/delete-passkey", {
				method: "POST",
				body,
			});
		},

		rename(body: { id: string; name: string }) {
			return call<{ passkey: Passkey }>("/passkey/update-passkey", {
				method: "POST",
				body,
			});
		},
	},

	signOut() {
		return call<{ success: boolean }>("/sign-out", { method: "POST" });
	},

	changePassword(body: {
		currentPassword: string;
		newPassword: string;
		revokeOtherSessions?: boolean;
	}) {
		return call<{ success: boolean }>("/change-password", {
			method: "POST",
			body,
		});
	},

	changeEmail(body: { newEmail: string }) {
		return call<{ status: boolean }>("/change-email", { method: "POST", body });
	},

	/**
	 * Partial update — omitted fields are left alone, and an all-empty body is
	 * rejected by the backend with "No fields to update". `email` is not
	 * accepted here; `changeEmail` owns that.
	 */
	updateUser(body: { username?: string; image?: string }) {
		return call<{ status: boolean }>("/update-user", { method: "POST", body });
	},

	/**
	 * Deletes the *caller's own* account and clears the session cookie.
	 *
	 * The password is the confirmation step: riven runs with delete-user
	 * verification off (no mail provider), so without it the only thing standing
	 * between a borrowed session and a deleted account is a session-freshness
	 * check.
	 */
	deleteUser(body: { password: string }) {
		return call<{ success: boolean; message: string }>("/delete-user", {
			method: "POST",
			body,
		});
	},

	listAccounts() {
		return call<LinkedAccount[]>("/list-accounts");
	},

	unlinkAccount(body: { providerId: string; accountId?: string }) {
		return call<{ status: boolean }>("/unlink-account", {
			method: "POST",
			body,
		});
	},

	listSessions() {
		return call<AuthSession[]>("/list-sessions");
	},

	/** Admin plugin — requires the caller's session to carry the admin role. */
	admin: {
		listUsers(query?: {
			limit?: number;
			sortBy?: string;
			sortDirection?: string;
		}) {
			const params = new URLSearchParams();
			for (const [key, value] of Object.entries(query ?? {})) {
				if (value !== undefined) params.set(key, String(value));
			}
			const suffix = params.size ? `?${params}` : "";
			return call<{ users: AuthUser[]; total: number }>(
				`/admin/list-users${suffix}`,
			);
		},
		createUser(body: {
			username: string;
			email: string;
			password: string;
			role?: string;
		}) {
			return call<{ user: AuthUser }>("/admin/create-user", {
				method: "POST",
				body: { ...body, name: body.username },
			});
		},
		removeUser(body: { userId: string }) {
			return call<{ success: boolean }>("/admin/remove-user", {
				method: "POST",
				body,
			});
		},
	},
};

export const { signIn, signOut, getSession } = authClient;
