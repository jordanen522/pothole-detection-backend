-- Returns all potholes within `radius` metres of a given point,
-- ordered by priority score descending.
CREATE OR REPLACE FUNCTION get_potholes_near(
    lat    DOUBLE PRECISION,
    lng    DOUBLE PRECISION,
    radius FLOAT            -- metres
)
RETURNS TABLE (
    pothole_id      UUID,
    canonical_lat   DOUBLE PRECISION,
    canonical_lng   DOUBLE PRECISION,
    severity_score  FLOAT,
    hit_count       INT,
    priority_score  FLOAT,
    traffic_weight  FLOAT,
    first_seen      TIMESTAMPTZ,
    last_seen       TIMESTAMPTZ
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
SELECT
    pothole_id,
    canonical_lat,
    canonical_lng,
    severity_score,
    hit_count,
    priority_score,
    traffic_weight,
    first_seen,
    last_seen
FROM potholes
WHERE ST_DWithin(
    ST_SetSRID(ST_MakePoint(canonical_lng, canonical_lat), 4326)::geography,
    ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
    radius
)
ORDER BY priority_score DESC;
$$;
