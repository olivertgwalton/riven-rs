/**
 * WebAuthn helpers over the browser's native JSON serialisation APIs. The
 * backend speaks the same wire format, so no manual base64url handling is
 * needed on either side.
 */

/** Thrown when the user dismisses the platform prompt — a decision, not a fault. */
export class PasskeyCancelledError extends Error {
	constructor() {
		super("Passkey prompt dismissed");
		this.name = "PasskeyCancelledError";
	}
}

export function browserSupportsPasskeys(): boolean {
	return (
		typeof window !== "undefined" &&
		typeof window.PublicKeyCredential?.parseCreationOptionsFromJSON ===
			"function"
	);
}

/** Whether passkeys can be offered from inside the username field. */
export async function supportsConditionalMediation(): Promise<boolean> {
	if (!browserSupportsPasskeys()) return false;
	try {
		return await PublicKeyCredential.isConditionalMediationAvailable();
	} catch {
		return false;
	}
}

function rethrow(error: unknown): never {
	if (
		error instanceof DOMException &&
		(error.name === "NotAllowedError" || error.name === "AbortError")
	) {
		throw new PasskeyCancelledError();
	}
	throw error;
}

export async function createPasskey(
	options: PublicKeyCredentialCreationOptionsJSON,
): Promise<RegistrationResponseJSON> {
	try {
		const credential = (await navigator.credentials.create({
			publicKey: PublicKeyCredential.parseCreationOptionsFromJSON(options),
		})) as PublicKeyCredential | null;
		if (!credential) throw new PasskeyCancelledError();
		return credential.toJSON() as RegistrationResponseJSON;
	} catch (error) {
		rethrow(error);
	}
}

/**
 * `signal` lets a conditional (autofill) request be torn down before a modal
 * one starts: only one `get()` may be outstanding, and the second otherwise
 * rejects with `NotAllowedError`.
 */
export async function getPasskeyAssertion(
	options: PublicKeyCredentialRequestOptionsJSON,
	init?: { conditional?: boolean; signal?: AbortSignal },
): Promise<AuthenticationResponseJSON> {
	try {
		const credential = (await navigator.credentials.get({
			signal: init?.signal,
			mediation: init?.conditional ? "conditional" : undefined,
			publicKey: PublicKeyCredential.parseRequestOptionsFromJSON(options),
		})) as PublicKeyCredential | null;
		if (!credential) throw new PasskeyCancelledError();
		return credential.toJSON() as AuthenticationResponseJSON;
	} catch (error) {
		rethrow(error);
	}
}
