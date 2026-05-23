-- 1. Auto-update updated_at on every write
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


-- 2. Auto-purge rows older than 24 h
CREATE OR REPLACE FUNCTION purge_old_snapshots()
RETURNS void LANGUAGE sql SECURITY DEFINER AS $$
DELETE FROM monitoring_snapshots
WHERE captured_at < now() - INTERVAL '24 hours';
$$;


-- 3. Live queue counts
CREATE OR REPLACE FUNCTION get_queue_counts()
RETURNS TABLE (status TEXT, cnt BIGINT)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
SELECT status, COUNT(*) AS cnt
FROM   event_queue
GROUP  BY status;
$$;


-- 4. Current database size in MB
CREATE OR REPLACE FUNCTION get_db_size_mb()
RETURNS FLOAT
LANGUAGE sql STABLE SECURITY DEFINER AS $$
SELECT pg_database_size(current_database())::FLOAT / (1024.0 * 1024.0);
$$;


-- 5. Table row-count estimates
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