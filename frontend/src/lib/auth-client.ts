import { createPasskey, getPasskeyAssertion } from "$lib/passkeys";

/**
 * Thin client for the backend's authentication endpoints (`riven-api`'s
 * `server/authn`). Field names are snake_case end to end — the backend
 * serializes its Rust structs directly, with no rename layer on either side.
 *
 * Always same-origin: riven serves this bundle itself.
 */
const AUTH_URL = "/auth";

export interface AuthUser {
	id: string;
	name?: string | null;
	email?: string | null;
	email_verified?: boolean;
	image?: string | null;
	username?: string | null;
	display_username?: string | null;
	role?: string | null;
	created_at?: string;
	updated_at?: string;
}

export interface AuthSession {
	id: string;
	user_id: string;
	expires_at: string;
}

/** A row from `/list-accounts`: one authentication method linked to the user. */
export interface LinkedAccount {
	id: string;
	provider_id: string;
	account_id: string;
	created_at?: string;
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

export interface Passkey {
	id: string;
	name?: string | null;
	created_at: string;
	device_type: string;
	backed_up: boolean;
	transports?: string | null;
}

/**
 * Every call carries the session cookie — it is the only credential.
 *
 * Errors are returned rather than thrown so callers can render them inline.
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
			return call<{ user: AuthUser; token?: string }>(
				"/passkey/verify-authentication",
				{ method: "POST", body: { response: assertion } },
			);
		},

		/**
		 * Starts an OIDC sign-in: the backend returns the provider's
		 * authorization URL (a `fetch()` cannot hand a 302 to the browser for a
		 * top-level navigation), and the caller navigates to it itself.
		 */
		social(body: { provider: string; callback_url: string }) {
			return call<{ url: string }>("/sign-in/social", {
				method: "POST",
				body,
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
				body,
			});
		},
	},

	firstUserAvailable() {
		return call<{ available: boolean }>("/first-user");
	},

	oidcProviders() {
		return call<OidcProviderSummary[]>("/oidc-providers");
	},

	/**
	 * `poll` answers `{ pending: true }` with a 202 until the user approves.
	 * `handle` is an opaque token for this sign-in attempt, not the Plex PIN id.
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
		generateRegisterOptions() {
			return call<PublicKeyCredentialCreationOptionsJSON>(
				"/passkey/generate-register-options",
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
				await authClient.passkey.generateRegisterOptions();
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
		current_password: string;
		new_password: string;
		revoke_other_sessions?: boolean;
	}) {
		return call<{ success: boolean }>("/change-password", {
			method: "POST",
			body,
		});
	},

	changeEmail(body: { new_email: string }) {
		return call<{ status: boolean }>("/change-email", { method: "POST", body });
	},

	/** Partial update — omitted fields are left alone. `email` is not accepted
	 * here; `changeEmail` owns that. */
	updateUser(body: { username?: string; image?: string }) {
		return call<{ status: boolean }>("/update-user", { method: "POST", body });
	},

	/**
	 * Deletes the *caller's own* account and clears the session cookie. The
	 * password is the confirmation step.
	 */
	deleteUser(body: { password: string }) {
		return call<{ success: boolean }>("/delete-user", {
			method: "POST",
			body,
		});
	},

	listAccounts() {
		return call<LinkedAccount[]>("/list-accounts");
	},

	unlinkAccount(body: { provider_id: string }) {
		return call<{ status: boolean }>("/unlink-account", {
			method: "POST",
			body,
		});
	},

	listSessions() {
		return call<AuthSession[]>("/list-sessions");
	},

	/** Admin — requires the caller's session to carry the admin role. */
	admin: {
		listUsers(query?: {
			limit?: number;
			sort_by?: string;
			sort_direction?: string;
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
				body,
			});
		},
		removeUser(body: { user_id: string }) {
			return call<{ success: boolean }>("/admin/remove-user", {
				method: "POST",
				body,
			});
		},
	},
};

export const { signIn, signOut, getSession } = authClient;
