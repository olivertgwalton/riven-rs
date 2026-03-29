-- Custom ranking profiles: user-defined, named copies of rank settings.
-- Built-in profiles (ultra_hd, hd, standard) are derived from Rust code and
-- are never stored here.
CREATE TABLE ranking_profiles (
    id         SERIAL PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    settings   JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
