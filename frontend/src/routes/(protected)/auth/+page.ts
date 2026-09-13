import type { PageLoad } from "./$types";
import {
	authClient,
	type AuthUser,
	type LinkedAccount,
} from "$lib/auth-client";
import { can } from "$lib/permissions";
import { createScopedLogger } from "$lib/logger";

const logger = createScopedLogger("profile-page");

/**
 * What the old `+page.server.ts` did, minus the server: the data comes from the
 * backend's `/auth` endpoints over the session cookie.
 */
export const load: PageLoad = async ({ parent }) => {
	const { user, permissions } = await parent();
	const canManageUsers = can(permissions, "MANAGE_SETTINGS");

	const accountsTask = authClient.listAccounts().then(({ data, error }) => {
		if (error) logger.error("failed to list linked accounts", error.message);
		return data ?? ([] as LinkedAccount[]);
	});

	// Non-admins are not shown the section at all, and the backend would reject
	// the call anyway — so it is not even made.
	const managedUsersTask = canManageUsers
		? authClient.admin
				.listUsers({
					limit: 100,
					sort_by: "created_at",
					sort_direction: "desc",
				})
				.then(({ data, error }) => {
					if (error) logger.error("failed to list users", error.message);
					return data?.users ?? ([] as AuthUser[]);
				})
		: Promise.resolve([] as AuthUser[]);

	const [accounts, managedUsers] = await Promise.all([
		accountsTask,
		managedUsersTask,
	]);

	return {
		user,
		canManageUsers,
		accounts,
		managedUsers,
	};
};
