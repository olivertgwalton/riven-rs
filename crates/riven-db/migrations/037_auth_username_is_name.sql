-- An account has one handle. `auth_users.username` is not a second identity —
-- it is `name`, in the form sign-in looks for.
--
-- better-auth's `/admin/create-user` cannot send a username at all: its body is
-- email, password, name, role and a free-form `data` map, and `data` is stored
-- as user *metadata*, a column riven's `auth_users` does not have. So a user
-- added from the profile page arrived with `username = NULL` — created fine,
-- listed fine, and unable to sign in, because `/auth/sign-in/username` matched
-- no row and the login page reported "Invalid username or password", the same
-- thing it says for a wrong password.
--
-- Deriving the column here rather than in the API keeps the three columns in
-- step for every writer — sign-up, admin create, a rename, a psql session, some
-- future plugin — instead of once per code path that remembers to:
--
--   name              the handle as it was typed
--   display_username  the same string; what the UI shows
--   username          lower-cased; what `/sign-in/username` looks up

CREATE OR REPLACE FUNCTION auth_users_username_is_name() RETURNS TRIGGER AS $$
DECLARE
    handle text;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        -- A rename can arrive through any of the three, so the one that
        -- changed is the new handle. When none did, `name` re-derives what is
        -- already there and the row is untouched.
        IF NEW.name IS DISTINCT FROM OLD.name THEN
            handle := NEW.name;
        ELSIF NEW.display_username IS DISTINCT FROM OLD.display_username THEN
            handle := NEW.display_username;
        ELSIF NEW.username IS DISTINCT FROM OLD.username THEN
            handle := NEW.username;
        ELSE
            handle := NEW.name;
        END IF;
    ELSE
        handle := coalesce(NEW.name, NEW.display_username, NEW.username);
    END IF;

    handle := btrim(coalesce(handle, ''));

    -- Nothing to derive from: leave the row exactly as it came. Such an account
    -- cannot sign in with a password, but that is the writer's doing, and a
    -- trigger is the wrong place to refuse it.
    IF handle = '' THEN
        RETURN NEW;
    END IF;

    NEW.name := handle;
    NEW.display_username := handle;
    NEW.username := lower(handle);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS auth_users_username_is_name ON auth_users;
CREATE TRIGGER auth_users_username_is_name
    BEFORE INSERT OR UPDATE ON auth_users
    FOR EACH ROW
    EXECUTE FUNCTION auth_users_username_is_name();

-- Accounts already created without a username: they exist, they just cannot
-- log in. The unique index means a collision here would abort the migration
-- rather than pick a winner, which is the right way round for two accounts
-- claiming one handle.
UPDATE auth_users
SET username = lower(btrim(name)),
    display_username = btrim(name)
WHERE username IS NULL
  AND btrim(coalesce(name, '')) <> '';
