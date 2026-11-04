-- MusicBrainz identifies music as TMDB and TVDB identify film and television:
-- an artist by its artist MBID, an album by its release group MBID and a track
-- by its recording MBID. An album also keeps the release (one edition of the
-- release group) whose tracklist its tracks were taken from.
ALTER TABLE media_items
    ADD COLUMN IF NOT EXISTS musicbrainz_id TEXT,
    ADD COLUMN IF NOT EXISTS musicbrainz_release_id TEXT,
    ADD COLUMN IF NOT EXISTS disc_number INTEGER,
    ADD COLUMN IF NOT EXISTS track_number INTEGER;

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_items_artist_musicbrainz_unique
    ON media_items(musicbrainz_id)
    WHERE item_type = 'artist' AND musicbrainz_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_albums_unique
    ON media_items(parent_id, musicbrainz_id)
    WHERE item_type = 'album';

CREATE UNIQUE INDEX IF NOT EXISTS idx_tracks_unique
    ON media_items(parent_id, disc_number, track_number)
    WHERE item_type = 'track';

-- An artist request names the albums asked for, by release group MBID. NULL
-- asks for the whole discography and every album the artist releases later.
ALTER TABLE item_requests
    ADD COLUMN IF NOT EXISTS musicbrainz_id TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS albums JSONB;
