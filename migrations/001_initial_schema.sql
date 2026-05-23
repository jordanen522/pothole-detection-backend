-- =============================================================================
-- POTHOLE DETECTION — SUPABASE SCHEMA  (v1.2 — Hackathon / Local)
-- =============================================================================
-- Run order:
--   1. Extensions
--   2. Core tables  (events, potholes)
--   3. Queue tables (event_queue)
--   4. Monitoring   (monitoring_snapshots)
--   5. RPC helpers
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 0. EXTENSIONS
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS postgis;         -- GPS clustering
CREATE EXTENSION IF NOT EXISTS pg_stat_statements; -- query-level monitoring


-- ---------------------------------------------------------------------------
-- 1. POTHOLES  (canonical, deduplicated records)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS potholes (
                                        pothole_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    canonical_lat   DOUBLE PRECISION NOT NULL,
    canonical_lng   DOUBLE PRECISION NOT NULL,
    severity_score  FLOAT            NOT NULL DEFAULT 0,
    hit_count       INT              NOT NULL DEFAULT 1,
    priority_score  FLOAT            NOT NULL DEFAULT 0,
    traffic_weight  FLOAT            NOT NULL DEFAULT 1.0,
    first_seen      TIMESTAMPTZ      NOT NULL DEFAULT now(),
    last_seen       TIMESTAMPTZ      NOT NULL DEFAULT now()
    );

CREATE INDEX IF NOT EXISTS idx_potholes_priority
    ON potholes (priority_score DESC);

CREATE INDEX IF NOT EXISTS idx_potholes_geo
    ON potholes USING GIST (
    ST_SetSRID(ST_MakePoint(canonical_lng, canonical_lat), 4326)
    );


-- ---------------------------------------------------------------------------
-- 2. EVENTS  (raw, per-device hits)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
                                      event_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    device_id   TEXT             NOT NULL,
    pothole_id  UUID             REFERENCES potholes (pothole_id) ON DELETE SET NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    severity    FLOAT            NOT NULL,
    detected_at TIMESTAMPTZ      NOT NULL,
    app_version TEXT,
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT now()
    );

CREATE INDEX IF NOT EXISTS idx_events_device_detected
    ON events (device_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_pothole
    ON events (pothole_id);


-- ---------------------------------------------------------------------------
-- 3. EVENT QUEUE  (producer-consumer, DLQ pattern)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_queue (
                                           id          SERIAL PRIMARY KEY,
                                           device_id   TEXT        NOT NULL,
                                           payload     JSONB       NOT NULL,
                                           status      TEXT        NOT NULL DEFAULT 'pending'
                                           CHECK (status IN ('pending', 'processing', 'dead_letter')),
    retry_count INT         NOT NULL DEFAULT 0,
    error_msg   TEXT,                   -- last error for DLQ diagnosis
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );

CREATE INDEX IF NOT EXISTS idx_queue_status
    ON event_queue (status, created_at);

-- Auto-update updated_at on every write
CREATE OR REPLACE FUNCTION trg_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS set_queue_updated_at ON event_queue;
CREATE TRIGGER set_queue_updated_at
    BEFORE UPDATE ON event_queue
    FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();


-- ---------------------------------------------------------------------------
-- 4. MONITORING SNAPSHOTS  (15-minute ring buffer, 24-hour retention)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring_snapshots (
                                                    id                   SERIAL PRIMARY KEY,
                                                    captured_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- System resources
    cpu_percent          FLOAT,
    memory_used_mb       FLOAT,
    memory_percent       FLOAT,
    db_size_mb           FLOAT,

    -- Queue health
    queue_pending        INT,
    queue_dead_letter    INT,

    -- Throughput over the last 15-minute window
    events_processed     INT  DEFAULT 0,
    events_failed        INT  DEFAULT 0,
    events_rejected      INT  DEFAULT 0,

    -- Request rate (average pings/sec over the window)
    avg_pings_per_sec    FLOAT DEFAULT 0
    );

CREATE INDEX IF NOT EXISTS idx_snapshots_captured_at
    ON monitoring_snapshots (captured_at DESC);

-- Auto-purge rows older than 24 h (called by the Python worker after each snapshot)
CREATE OR REPLACE FUNCTION purge_old_snapshots()
RETURNS void LANGUAGE sql SECURITY DEFINER AS $$
DELETE FROM monitoring_snapshots
WHERE captured_at < now() - INTERVAL '24 hours';
$$;


-- ---------------------------------------------------------------------------
-- 5. RPC HELPERS
-- ---------------------------------------------------------------------------

-- 5a. Find the nearest pothole within `radius` metres
CREATE OR REPLACE FUNCTION find_nearby_pothole(
    lat    DOUBLE PRECISION,
    lng    DOUBLE PRECISION,
    radius FLOAT
)
RETURNS TABLE (pothole_id UUID)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
SELECT pothole_id
FROM   potholes
WHERE  ST_DWithin(
               ST_SetSRID(ST_MakePoint(canonical_lng, canonical_lat), 4326)::geography,
               ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
               radius
       )
ORDER BY ST_Distance(
                 ST_SetSRID(ST_MakePoint(canonical_lng, canonical_lat), 4326)::geography,
                 ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography
         )
    LIMIT 1;
$$;


-- 5b. Live queue counts (used by /monitoring/live)
CREATE OR REPLACE FUNCTION get_queue_counts()
RETURNS TABLE (status TEXT, cnt BIGINT)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
SELECT status, COUNT(*) AS cnt
FROM   event_queue
GROUP  BY status;
$$;


-- 5c. Current database size in MB
CREATE OR REPLACE FUNCTION get_db_size_mb()
RETURNS FLOAT
LANGUAGE sql STABLE SECURITY DEFINER AS $$
SELECT pg_database_size(current_database())::FLOAT / (1024.0 * 1024.0);
$$;


-- 5d. Table row-count estimates (lightweight, from pg_stat)
CREATE OR REPLACE FUNCTION get_table_stats()
RETURNS TABLE (table_name TEXT, estimated_rows BIGINT, total_size_kb BIGINT)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
SELECT
    relname::TEXT                                               AS table_name,
    reltuples::BIGINT                                           AS estimated_rows,
    (pg_total_relation_size(oid) / 1024)::BIGINT               AS total_size_kb
FROM   pg_class
WHERE  relkind = 'r'
  AND  relname IN ('events', 'potholes', 'event_queue', 'monitoring_snapshots')
ORDER  BY relname;
$$;
