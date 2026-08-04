import { z } from "zod";

/**
 * The role strings `auth_users.role` actually holds. Lowercase because that is
 * what `role_from_user` in `crates/riven-api/src/server/auth.rs` matches on —
 * the GraphQL `UserRole` enum is the screaming-case view of the same ladder.
 */
export const userRoleSchema = z.enum(["user", "manager", "admin"]);

export const passwordChangeSchema = z
	.object({
		oldPassword: z.string().min(1, "Your old password cannot be empty"),
		newPassword: z
			.string()
			.min(8, "Your password must have 8 characters or more."),
		confirmNewPassword: z.string(),
		revokeSessions: z.coerce.boolean<boolean>(),
	})
	.refine((data) => data.newPassword === data.confirmNewPassword, {
		message: "New password and confirmation do not match.",
		path: ["confirmNewPassword"],
	});

export type PasswordChangeSchema = z.infer<typeof passwordChangeSchema>;

export const emailChangeSchema = z.object({
	newEmail: z.email("Invalid email address"),
});

export type EmailChangeSchema = z.infer<typeof emailChangeSchema>;

/**
 * The character and length rules for a username, and the only place they are
 * enforced on the way in — the database derives the column but does not judge
 * it. Minimum length and character set match better-auth-rs's own
 * `utils::username`, which it applies at both ends, creation *and* sign-in: a
 * username the library would refuse is not merely rejected on save, it could
 * never be typed at the login page either. The 20-character cap is riven's,
 * stricter than the library's 30.
 */
const usernameSchema = z
	.string()
	.min(3, "Username must be at least 3 characters long")
	.max(20, "Username must be at most 20 characters long")
	.regex(
		/^[A-Za-z0-9._]+$/,
		"Username may only contain letters, numbers, '.' and '_'",
	);

export const changeUserDataSchema = z.object({
	newUsername: z
		.union([z.literal(""), usernameSchema])
		.optional()
		.default(""),
	newAvatar: z
		.union([z.url("Avatar must be a valid URL"), z.literal("")])
		.optional()
		.default(""),
});

export type ChangeUserDataSchema = z.infer<typeof changeUserDataSchema>;

export const createUserSchema = z
	.object({
		username: usernameSchema,
		email: z.email("Invalid email address"),
		password: z.string().min(8, "Password must have 8 characters or more."),
		confirmPassword: z.string(),
		role: userRoleSchema.default("user"),
	})
	.refine((data) => data.password === data.confirmPassword, {
		message: "Password and confirmation do not match.",
		path: ["confirmPassword"],
	});

export type CreateUserSchema = z.infer<typeof createUserSchema>;
