import type { PageLoad } from "./$types";
import { superValidate } from "sveltekit-superforms";
import { zod4 } from "sveltekit-superforms/adapters";
import {
	changeUserDataSchema,
	createUserSchema,
	emailChangeSchema,
	passwordChangeSchema,
} from "$lib/schemas/auth";
import {
	authClient,
	type AuthUser,
	type LinkedAccount,
} from "$lib/auth-client";
import { can } from "$lib/permissions";
import { createScopedLogger } from "$lib/logger";

const logger = createScopedLogger("profile-page");

/**
 * What the old `+page.server.ts` did, minus the server.
 *
 * The forms are still superforms — `superValidate` is happy in a universal load
 * — but the data behind them comes from the backend's `/auth` endpoints over the
 * session cookie instead of from `auth.api.*` calls on a Node server.
 */
export const load: PageLoad = async ({ parent }) => {
	const { user, permissions } = await parent();
	const canManageUsers = can(permissions, "MANAGE_SETTINGS");

	const [
		passwordChangeForm,
		emailChangeForm,
		changeUserDataForm,
		createUserForm,
	] = await Promise.all([
		superValidate(zod4(passwordChangeSchema)),
		superValidate(zod4(emailChangeSchema)),
		superValidate(zod4(changeUserDataSchema)),
		superValidate(zod4(createUserSchema)),
	]);

	const accountsTask = authClient.listAccounts().then(({ data, error }) => {
		if (error) logger.error("failed to list linked accounts", error.message);
		return data ?? ([] as LinkedAccount[]);
	});

	// Non-admins are not shown the section at all, and the backend would reject
	// the call anyway — so it is not even made.
	const managedUsersTask = canManageUsers
		? authClient.admin
				.listUsers({ limit: 100, sort_by: "created_at", sort_direction: "desc" })
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
		passwordChangeForm,
		emailChangeForm,
		changeUserDataForm,
		createUserForm,
	};
};
