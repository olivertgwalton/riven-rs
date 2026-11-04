-- Music: an artist holds albums, an album holds tracks, as a show holds
-- seasons and a season episodes. A value added to an enum cannot be used in
-- the transaction that adds it, so the columns and indexes that name these
-- follow in m043.
ALTER TYPE media_item_type ADD VALUE IF NOT EXISTS 'artist';
ALTER TYPE media_item_type ADD VALUE IF NOT EXISTS 'album';
ALTER TYPE media_item_type ADD VALUE IF NOT EXISTS 'track';
ALTER TYPE item_request_type ADD VALUE IF NOT EXISTS 'artist';
