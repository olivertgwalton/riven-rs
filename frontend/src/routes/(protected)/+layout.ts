import type { LayoutLoad } from "./$types";
import { redirect } from "@sveltejs/kit";
import { browser } from "$app/environment";
import { gqlClient } from "$lib/graphql-client";
import { NO_PERMISSIONS, type Permissions } from "$lib/permissions";
import type { InstanceStatus } from "$lib/gql/schema";
import { authClient } from "$lib/auth-client";

// One round trip for both: the setup gate and the caller's capabilities, which
// the backend derives from the same role it authorises mutations with.
const LAYOUT_QUERY = `query {
    instanceStatus { setupCompleted }
    viewer { capabilities }
}`;

interface LayoutQueryResult {
    instanceStatus: Pick<InstanceStatus, "setupCompleted">;
    viewer: { capabilities: Permissions };
}

/**
 * Auth and first-run gating for every protected route.
 *
 * This used to live in `hooks.server.ts`, which ran on a Node server that owned
 * the session. In a static bundle there is no such server, so the gate runs in
 * the browser against the backend's `/auth/get-session`.
 *
 * That is a UX gate, not a security boundary — a determined user can skip it in
 * devtools. It does not matter: every GraphQL call is authorised by the backend
 * from the session cookie, so skipping the redirect reveals an empty shell, not
 * data. The security boundary moved to the backend deliberately.
 */
export const load = (async ({ route }) => {
    // Prerendering has no session and no backend; the gate re-runs on hydration.
    if (!browser) {
        return { user: null, permissions: NO_PERMISSIONS };
    }

    const { data: session } = await authClient.getSession();
    if (!session?.user) {
        redirect(302, "/auth/login");
    }

    const result = await gqlClient<LayoutQueryResult>(LAYOUT_QUERY).catch(() => null);
    // A failure here must not lock the app into the setup wizard, so assume
    // setup is done and let the route's own error handling surface the fault.
    // Permissions fail closed instead: an unknown role must not unlock anything.
    const setupCompleted = result?.instanceStatus.setupCompleted !== false;
    const permissions = result?.viewer.capabilities ?? NO_PERMISSIONS;

    const isSetupRoute = route.id === "/(protected)/setup";
    if (!setupCompleted && !isSetupRoute) {
        redirect(302, "/setup");
    }
    if (setupCompleted && isSetupRoute) {
        redirect(302, "/");
    }

    return { user: session.user, permissions };
}) satisfies LayoutLoad;
