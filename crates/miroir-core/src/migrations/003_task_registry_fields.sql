-- Migration 003: Task registry fields (no-op)
-- This migration is a no-op because the node_errors field was already included
-- in the initial schema (001_initial.sql). This migration exists to maintain
-- migration version continuity for databases that may have already applied it.
--
-- All required task registry fields are already present:
-- - miroir_id, created_at, status, node_tasks, error
-- - started_at, finished_at, index_uid, task_type, node_errors
--
-- No schema changes needed.

-- No-op: leave a marker that this migration was applied
-- (SELECT 1 is used to ensure the migration is recorded but doesn't modify schema)
SELECT 1 AS migration_003_noop;
