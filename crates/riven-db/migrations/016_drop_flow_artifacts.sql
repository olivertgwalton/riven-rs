-- Flow coordination has moved from PostgreSQL to Redis hashes.
-- Results are now stored under riven:flow:{prefix}:{id}:results with a 1-hour TTL,
-- eliminating the need for periodic stale-artifact cleanup.
DROP TABLE IF EXISTS flow_artifacts;
