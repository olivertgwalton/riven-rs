-- Track when an existing item_request has been extended with additional
-- seasons (so the indexer knows to re-process), and whether the resulting
-- request covers only a subset of the show's seasons (so downstream callers
-- can decide between fan-out and full-show download).

ALTER TYPE item_request_state ADD VALUE IF NOT EXISTS 'requested_additional_seasons';

ALTER TABLE item_requests
    ADD COLUMN IF NOT EXISTS is_partial_request BOOLEAN NOT NULL DEFAULT false;
