import { z } from "zod";

/**
 * The role strings `auth_users.role` actually holds. Lowercase because that is
 * what `role_from_user` in `crates/riven-api/src/server/auth.rs` matches on —
 * the GraphQL `UserRole` enum is the screaming-case view of the same ladder.
 */
export const userRoleSchema = z.enum(["user", "manager", "admin"]);
export type UserRoleValue = z.infer<typeof userRoleSchema>;

export const passwordChangeSchema = z
    .object({
        oldPassword: z.string().min(1, "Your old password cannot be empty"),
        newPassword: z.string().min(8, "Your password must have 8 characters or more."),
        confirmNewPassword: z.string(),
        revokeSessions: z.coerce.boolean<boolean>()
    })
    .refine((data) => data.newPassword === data.confirmNewPassword, {
        message: "New password and confirmation do not match.",
        path: ["confirmNewPassword"]
    });

export type PasswordChangeSchema = z.infer<typeof passwordChangeSchema>;

export const emailChangeSchema = z.object({
    newEmail: z.email("Invalid email address")
});

export type EmailChangeSchema = z.infer<typeof emailChangeSchema>;

export const changeUserDataSchema = z.object({
    newUsername: z
        .union([
            z.literal(""),
            z
                .string()
                .min(3, "Username must be at least 3 characters long")
                .max(31, "Username must be at most 31 characters long")
        ])
        .optional()
        .default(""),
    newName: z.string().max(100, "Name must be at most 100 characters long").optional().default(""),
    newAvatar: z
        .union([z.url("Avatar must be a valid URL"), z.literal("")])
        .optional()
        .default("")
});

export type ChangeUserDataSchema = z.infer<typeof changeUserDataSchema>;

export const createUserSchema = z
    .object({
        username: z
            .string()
            .min(3, "Username must be at least 3 characters long")
            .max(31, "Username must be at most 31 characters long"),
        email: z.email("Invalid email address"),
        password: z.string().min(8, "Password must have 8 characters or more."),
        confirmPassword: z.string(),
        role: userRoleSchema.default("user")
    })
    .refine((data) => data.password === data.confirmPassword, {
        message: "Password and confirmation do not match.",
        path: ["confirmPassword"]
    });

export type CreateUserSchema = z.infer<typeof createUserSchema>;
