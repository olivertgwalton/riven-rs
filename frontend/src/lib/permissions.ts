import { error } from "@sveltejs/kit";
import type { Capability } from "$lib/gql/schema";

export type { Capability };

/**
 * The capabilities the backend granted this caller, verbatim.
 *
 * Nothing here decides anything. The thresholds live in
 * `crates/riven-api/src/schema/auth.rs`, where `Capability::minimum_role` is
 * read both by the guards that reject a mutation and by the `viewer` query that
 * produces this list — so the UI cannot offer an action the API would refuse.
 */
export type Permissions = readonly Capability[];

/** Nothing granted. The empty list is the absence of an answer, not a policy. */
export const NO_PERMISSIONS: Permissions = [];

export function can(
	permissions: Permissions | undefined,
	capability: Capability,
): boolean {
	return permissions?.includes(capability) ?? false;
}

/**
 * Client-side route guard.
 *
 * A UX gate only. The backend authorises every GraphQL call from the session
 * cookie, so bypassing this shows an empty page rather than data.
 */
export function requireCapability(
	permissions: Permissions | undefined,
	capability: Capability,
) {
	if (!can(permissions, capability)) {
		error(403, "Forbidden");
	}
}
