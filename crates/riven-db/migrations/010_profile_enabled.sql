-- Add enabled/disabled toggle and a built-in flag to ranking_profiles.
-- Built-in profiles (ultra_hd, hd, standard) are seeded here so the UI can
-- toggle them on/off like any custom profile.  Their settings are intentionally
-- empty — Rust code is the canonical source of truth for built-in settings.
ALTER TABLE ranking_profiles
    ADD COLUMN is_builtin BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN enabled    BOOLEAN NOT NULL DEFAULT false;

INSERT INTO ranking_profiles (name, settings, is_builtin, enabled)
VALUES
    ('ultra_hd', '{}', true, false),
    ('hd',       '{}', true, false),
    ('standard', '{}', true, false)
ON CONFLICT (name) DO UPDATE SET is_builtin = true;
