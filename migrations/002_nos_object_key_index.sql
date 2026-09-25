-- Maintenance walks (scrub, recompression, migration, backfill) page through active objects in key order.
-- Built in the background after startup (CONCURRENTLY: writes continue meanwhile); a single statement.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_nos_objects_active_key ON nos_objects (object_key) WHERE deleted_at IS NULL
