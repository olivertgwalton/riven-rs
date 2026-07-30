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

export interface AuthUser {
    id: string;
    email?: string | null;
    name?: string | null;
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
    init?: { method?: string; body?: unknown }
): Promise<AuthResult<T>> {
    let response: Response;
    try {
        response = await fetch(`${AUTH_URL}${path}`, {
            method: init?.method ?? "GET",
            credentials: "include",
            headers: init?.body ? { "Content-Type": "application/json" } : undefined,
            body: init?.body ? JSON.stringify(init.body) : undefined
        });
    } catch {
        return { data: null, error: { message: "Could not reach the server", status: 0 } };
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

const EMPTY_RESPONSE = { message: "Server returned an empty response", status: 0 };

function safeParse(text: string): unknown {
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

export const authClient = {
    /** `null` when unauthenticated — a 401 here is expected, not an error. */
    async getSession(): Promise<AuthResult<{ user: AuthUser; session: AuthSession } | null>> {
        const result = await call<{ user: AuthUser; session: AuthSession }>("/get-session");
        if (result.error?.status === 401) {
            return { data: null, error: null };
        }
        return result;
    },

    signIn: {
        username(body: { username: string; password: string }) {
            return call<{ user: AuthUser; token?: string }>("/sign-in/username", {
                method: "POST",
                body
            });
        },
        email(body: { email: string; password: string }) {
            return call<{ user: AuthUser; token?: string }>("/sign-in/email", {
                method: "POST",
                body
            });
        },

        /**
         * With no session the backend issues discoverable options — no
         * `allowCredentials` — so the authenticator picks the account and the
         * user never types a username.
         */
        async passkey(init?: { conditional?: boolean; signal?: AbortSignal }) {
            const { data: options, error } = await authClient.passkey.generateAuthenticateOptions();
            if (error || !options) {
                return { data: null, error: error ?? EMPTY_RESPONSE };
            }
            const assertion = await getPasskeyAssertion(options, init);
            return call<{ user: AuthUser; session: AuthSession }>(
                "/passkey/verify-authentication",
                { method: "POST", body: { response: assertion } }
            );
        }
    },

    /**
     * Only the first account can be created this way, and it becomes the admin.
     * The backend refuses every later sign-up, so ask `firstUserAvailable()`
     * before offering the form.
     */
    signUp: {
        email(body: { name: string; username: string; email: string; password: string }) {
            return call<{ user: AuthUser; token?: string }>("/sign-up/email", {
                method: "POST",
                body
            });
        }
    },

    firstUserAvailable() {
        return call<{ available: boolean }>("/first-user");
    },

    /** `poll` answers `{ pending: true }` with a 202 until the user approves. */
    plex: {
        start() {
            return call<{ id: number; auth_url: string }>("/plex/start", { method: "POST" });
        },
        poll(pinId: number) {
            return call<{ pending: boolean }>(`/plex/poll/${pinId}`);
        }
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
                `/passkey/generate-register-options${suffix}`
            );
        },

        generateAuthenticateOptions() {
            return call<PublicKeyCredentialRequestOptionsJSON>(
                "/passkey/generate-authenticate-options"
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
                body: { response: credential, name: options?.name }
            });
        },

        list() {
            return call<Passkey[]>("/passkey/list-user-passkeys");
        },

        remove(body: { id: string }) {
            return call<{ status: boolean }>("/passkey/delete-passkey", { method: "POST", body });
        },

        rename(body: { id: string; name: string }) {
            return call<{ passkey: Passkey }>("/passkey/update-passkey", { method: "POST", body });
        }
    },

    signOut() {
        return call<{ success: boolean }>("/sign-out", { method: "POST" });
    },

    changePassword(body: {
        currentPassword: string;
        newPassword: string;
        revokeOtherSessions?: boolean;
    }) {
        return call<{ success: boolean }>("/change-password", { method: "POST", body });
    },

    setPassword(body: { newPassword: string }) {
        return call<{ success: boolean }>("/set-password", { method: "POST", body });
    },

    changeEmail(body: { newEmail: string }) {
        return call<{ success: boolean }>("/change-email", { method: "POST", body });
    },

    updateUser(body: { name?: string; image?: string }) {
        return call<{ user: AuthUser }>("/update-user", { method: "POST", body });
    },

    listAccounts() {
        return call<{ providerId: string; accountId: string }[]>("/list-accounts");
    },

    listSessions() {
        return call<AuthSession[]>("/list-sessions");
    },

    /** Admin plugin — requires the caller's session to carry the admin role. */
    admin: {
        listUsers() {
            return call<{ users: AuthUser[] }>("/admin/list-users");
        },
        createUser(body: { email: string; password: string; name?: string; role?: string }) {
            return call<{ user: AuthUser }>("/admin/create-user", { method: "POST", body });
        },
        removeUser(body: { userId: string }) {
            return call<{ success: boolean }>("/admin/remove-user", { method: "POST", body });
        }
    }
};

export const { signIn, signOut, getSession } = authClient;
