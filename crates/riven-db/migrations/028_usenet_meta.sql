-- NZB segment/RAR-slice maps that the usenet streamer uses to resolve
-- `(info_hash, byte_range)` requests into NNTP article fetches.
--
-- Previously stored in Redis with a 7-day TTL alongside the cached NZB body.
-- That made sense for debrid plugins where the addressable resource (a signed
-- stream URL) genuinely expires upstream — but usenet message-ids don't expire,
-- so the meta map is durable address-book data, not a refreshable cache. Letting
-- it age out turned library entries into permanent 404s with no recovery path.
--
-- Living here next to `streams` / `filesystem_entries`, the meta now shares
-- their lifecycle: as long as a library entry references this info_hash, its
-- byte addressing is reachable. Redis remains useful purely as an in-memory
-- L2 cache (handled at the application layer if desired).

CREATE TABLE IF NOT EXISTS usenet_meta (
    info_hash  TEXT PRIMARY KEY,
    meta       JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
