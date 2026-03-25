-- Media item type enums
CREATE TYPE media_item_type AS ENUM ('movie', 'show', 'season', 'episode');
CREATE TYPE media_item_state AS ENUM ('indexed', 'unreleased', 'scraped', 'ongoing', 'partially_completed', 'completed', 'paused', 'failed');
CREATE TYPE show_status AS ENUM ('continuing', 'ended');
CREATE TYPE content_rating AS ENUM ('G', 'PG', 'PG-13', 'R', 'NC-17', 'TV-Y', 'TV-Y7', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA');
CREATE TYPE filesystem_entry_type AS ENUM ('media', 'subtitle');
CREATE TYPE item_request_type AS ENUM ('movie', 'show');
CREATE TYPE item_request_state AS ENUM ('pending', 'approved', 'declined', 'completed', 'failed');

-- Item requests
CREATE TABLE item_requests (
    id BIGSERIAL PRIMARY KEY,
    imdb_id TEXT UNIQUE,
    tmdb_id TEXT UNIQUE,
    tvdb_id TEXT UNIQUE,
    request_type item_request_type NOT NULL,
    requested_by TEXT,
    external_request_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    state item_request_state NOT NULL DEFAULT 'pending',
    seasons JSONB
);

-- Streams
CREATE TABLE streams (
    id BIGSERIAL PRIMARY KEY,
    info_hash TEXT NOT NULL UNIQUE,
    parsed_data JSONB
);

-- Media items (single table with discriminator)
CREATE TABLE media_items (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    full_title TEXT,
    imdb_id TEXT,
    tvdb_id TEXT,
    tmdb_id TEXT,
    poster_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    indexed_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ,
    scraped_times INT NOT NULL DEFAULT 0,
    aliases JSONB,
    network TEXT,
    country TEXT,
    language TEXT,
    aired_at DATE,
    year INT,
    genres JSONB,
    rating DOUBLE PRECISION,
    content_rating content_rating,
    state media_item_state NOT NULL DEFAULT 'indexed',
    failed_attempts INT NOT NULL DEFAULT 0,
    item_type media_item_type NOT NULL,
    is_requested BOOLEAN NOT NULL DEFAULT FALSE,
    -- Show fields
    show_status show_status,
    -- Season fields
    season_number INT,
    is_special BOOLEAN,
    parent_id BIGINT REFERENCES media_items(id) ON DELETE CASCADE,
    -- Episode fields
    episode_number INT,
    absolute_number INT,
    runtime INT,
    -- References
    item_request_id BIGINT REFERENCES item_requests(id),
    active_stream_id BIGINT REFERENCES streams(id)
);

CREATE INDEX idx_media_items_title ON media_items(title);
CREATE INDEX idx_media_items_created_at ON media_items(created_at);
CREATE INDEX idx_media_items_type_aired ON media_items(item_type, aired_at);
CREATE INDEX idx_media_items_imdb ON media_items(imdb_id);
CREATE INDEX idx_media_items_tmdb ON media_items(tmdb_id);
CREATE INDEX idx_media_items_tvdb ON media_items(tvdb_id);
CREATE INDEX idx_media_items_parent ON media_items(parent_id);
CREATE INDEX idx_media_items_state ON media_items(state);

-- Media item <-> stream many-to-many
CREATE TABLE media_item_streams (
    media_item_id BIGINT NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
    stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    PRIMARY KEY (media_item_id, stream_id)
);

-- Blacklisted streams many-to-many
CREATE TABLE media_item_blacklisted_streams (
    media_item_id BIGINT NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
    stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    PRIMARY KEY (media_item_id, stream_id)
);

-- Filesystem entries (single table with discriminator)
CREATE TABLE filesystem_entries (
    id BIGSERIAL PRIMARY KEY,
    file_size BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    media_item_id BIGINT NOT NULL REFERENCES media_items(id) ON DELETE CASCADE,
    entry_type filesystem_entry_type NOT NULL,
    path TEXT NOT NULL,
    -- Media entry fields
    original_filename TEXT,
    download_url TEXT,
    stream_url TEXT,
    plugin TEXT,
    provider TEXT,
    provider_download_id TEXT,
    library_profiles JSONB,
    media_metadata JSONB,
    -- Subtitle entry fields
    language TEXT,
    parent_original_filename TEXT,
    subtitle_content TEXT,
    file_hash TEXT,
    video_file_size BIGINT,
    opensubtitles_id TEXT
);

CREATE INDEX idx_fs_entries_media_item ON filesystem_entries(media_item_id);
CREATE INDEX idx_fs_entries_type ON filesystem_entries(entry_type);
CREATE INDEX idx_fs_entries_path ON filesystem_entries(path);
CREATE INDEX idx_fs_entries_original_filename ON filesystem_entries(original_filename);
