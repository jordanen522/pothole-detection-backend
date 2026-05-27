-- =============================================================================
-- MIGRATION 006 — Privacy layer
-- =============================================================================
-- Changes:
--   1. Create nonanon_events (replaces event_queue).
--      Stores a daily-salted HMAC of device_id, never the raw value.
--      Payload JSON omits device_id entirely.
--   2. Alter events: swap device_id → device_id_hash (nullable so the
--      hourly strip job can null it out without deleting the row).
--   3. strip_old_device_hashes() — nulls device_id_hash on events older
--      than 1 hour.  Schedule this with pg_cron (see bottom of file).
--   4. Update get_queue_counts() and get_table_stats() to reference
--      nonanon_events instead of event_queue.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1. nonanon_events  (new queue table — no raw device IDs ever stored)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nonanon_events (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id_hash TEXT        NOT NULL,
    payload        JSONB       NOT NULL,        -- PotholeEventIn minus device_id
    status         TEXT        NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'dead_letter')),
    retry_count    INT         NOT NULL DEFAULT 0,
    error_msg      TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nonanon_events_status
    ON nonanon_events (status, created_at);

DROP TRIGGER IF EXISTS set_nonanon_updated_at ON nonanon_events;
CREATE TRIGGER set_nonanon_updated_at
    BEFORE UPDATE ON nonanon_events
    FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();


-- ---------------------------------------------------------------------------
-- 2. events — swap device_id for device_id_hash
-- ---------------------------------------------------------------------------
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS device_id_hash TEXT;

-- Drop old index first so we can cleanly recreate it
DROP INDEX IF EXISTS idx_events_device_detected;

-- Only drop device_id if it still exists (idempotent)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'events' AND column_name = 'device_id'
    ) THEN
        ALTER TABLE events DROP COLUMN device_id;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_events_device_hash_detected
    ON events (device_id_hash, detected_at DESC);


-- ---------------------------------------------------------------------------
-- 3. strip_old_device_hashes — called by pg_cron every hour
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION strip_old_device_hashes()
RETURNS void LANGUAGE sql SECURITY DEFINER AS $$
    UPDATE events
    SET    device_id_hash = NULL
    WHERE  device_id_hash IS NOT NULL
    AND    detected_at < now() - INTERVAL '1 hour';
$$;

-- Schedule with pg_cron (enable pg_cron in Supabase Dashboard → Database → Extensions first)
-- Run once to register; safe to re-run.
SELECT cron.schedule(
    'strip-device-hashes-hourly',
    '0 * * * *',
    'SELECT strip_old_device_hashes()'
) WHERE NOT EXISTS (
    SELECT 1 FROM cron.job WHERE jobname = 'strip-device-hashes-hourly'
);


-- ---------------------------------------------------------------------------
-- 4. Update RPCs that referenced event_queue
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_queue_counts()
RETURNS TABLE (status TEXT, cnt BIGINT)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT status, COUNT(*)::BIGINT AS cnt
    FROM   nonanon_events
    GROUP  BY status;
$$;

CREATE OR REPLACE FUNCTION get_table_stats()
RETURNS TABLE (table_name TEXT, estimated_rows BIGINT, total_size_kb BIGINT)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT
        relname::TEXT                                 AS table_name,
        reltuples::BIGINT                             AS estimated_rows,
        (pg_total_relation_size(oid) / 1024)::BIGINT  AS total_size_kb
    FROM  pg_class
    WHERE relkind = 'r'
    AND   relname IN ('events', 'potholes', 'nonanon_events', 'monitoring_snapshots')
    ORDER BY relname;
$$;


-- ---------------------------------------------------------------------------
-- 5. (Optional) Retire event_queue once you've confirmed no pending rows
--    Uncomment after verifying: SELECT COUNT(*) FROM event_queue WHERE status = 'pending';
-- ---------------------------------------------------------------------------
-- DROP TABLE IF EXISTS event_queue;
