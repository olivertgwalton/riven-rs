-- Banning came from better-auth's admin plugin, not from riven. A self-hosted
-- instance administers its users by removing them, and no riven endpoint ever
-- set or read these columns, so they only ever held their defaults.
ALTER TABLE "auth_users" DROP COLUMN IF EXISTS "banned";
ALTER TABLE "auth_users" DROP COLUMN IF EXISTS "ban_reason";
ALTER TABLE "auth_users" DROP COLUMN IF EXISTS "ban_expires";
