-- Banning came from better-auth's admin plugin: `/auth/admin/ban-user` set
-- these columns and the old session check read them on every authorized
-- request. Riven's own UI and docs never exposed either, and native auth has
-- no ban concept — a self-hosted instance administers its users by removing
-- them.
--
-- Nothing reads the flag any more, so a session still held by a user who was
-- banned through that undocumented endpoint would start working again the
-- moment the column goes. Revoke those first. The guard is for fresh databases,
-- where m036 never created the column.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'auth_users' AND column_name = 'banned'
    ) THEN
        DELETE FROM "auth_sessions"
        WHERE "user_id" IN (
            SELECT "id" FROM "auth_users"
            WHERE "banned" AND ("ban_expires" IS NULL OR "ban_expires" > now())
        );
    END IF;
END $$;

ALTER TABLE "auth_users" DROP COLUMN IF EXISTS "banned";
ALTER TABLE "auth_users" DROP COLUMN IF EXISTS "ban_reason";
ALTER TABLE "auth_users" DROP COLUMN IF EXISTS "ban_expires";
