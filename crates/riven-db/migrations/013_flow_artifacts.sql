CREATE TABLE flow_artifacts (
    flow_name TEXT NOT NULL,
    item_id BIGINT NOT NULL,
    plugin_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (flow_name, item_id, plugin_name)
);

CREATE INDEX idx_flow_artifacts_created_at
    ON flow_artifacts (created_at);

CREATE INDEX idx_flow_artifacts_lookup
    ON flow_artifacts (flow_name, item_id);
