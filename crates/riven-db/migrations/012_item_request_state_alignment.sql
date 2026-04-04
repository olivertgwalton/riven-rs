ALTER TABLE item_requests ALTER COLUMN state DROP DEFAULT;

ALTER TABLE item_requests
ALTER COLUMN state TYPE TEXT
USING state::TEXT;

UPDATE item_requests
SET state = CASE state
    WHEN 'pending' THEN 'requested'
    WHEN 'approved' THEN 'completed'
    WHEN 'declined' THEN 'failed'
    ELSE state
END;

DROP TYPE item_request_state;

CREATE TYPE item_request_state AS ENUM (
    'requested',
    'completed',
    'failed',
    'ongoing',
    'unreleased'
);

ALTER TABLE item_requests
ALTER COLUMN state TYPE item_request_state
USING state::item_request_state;

ALTER TABLE item_requests
ALTER COLUMN state SET DEFAULT 'requested';
