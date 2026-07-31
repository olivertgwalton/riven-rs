import type { PageLoad } from "./$types";
import { load as settingsLoad } from "../settings/+page";
import { requireCapability } from "$lib/permissions";

// Whether the wizard should show at all is decided once, in the auth hook
// (`instanceNeedsSetup`): it redirects away from /setup when setup is done and
// requires a session for protected routes. So this load just gates on settings
// access and reuses the settings loader for the wizard's data.
export const load: PageLoad = async (event) => {
	const { permissions } = await event.parent();
	requireCapability(permissions, "MANAGE_SETTINGS");
	return settingsLoad(event as never);
};
