import type { PageLoad } from "./$types";
import { requireCapability } from "$lib/permissions";

export const load = (async ({ parent }) => {
	const { permissions } = await parent();
	requireCapability(permissions, "MANAGE_SETTINGS");
	return {};
}) satisfies PageLoad;
