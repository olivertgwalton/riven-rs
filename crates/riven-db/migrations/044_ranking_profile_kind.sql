-- A ranking profile ranks either film and television or music: the two have
-- nothing in common to rank by. Every profile so far is a video one.
CREATE TYPE ranking_profile_kind AS ENUM ('video', 'music');

ALTER TABLE ranking_profiles
    ADD COLUMN IF NOT EXISTS kind ranking_profile_kind NOT NULL DEFAULT 'video';

-- Built-in music profiles, toggled like the video ones; their settings live in
-- Rust. Best available is on, so albums download with no setup.
INSERT INTO ranking_profiles (name, settings, is_builtin, enabled, kind)
VALUES
    ('music_best',     '{}', true, true,  'music'),
    ('music_hi_res',   '{}', true, false, 'music'),
    ('music_portable', '{}', true, false, 'music')
ON CONFLICT (name) DO UPDATE SET is_builtin = true, kind = 'music';
