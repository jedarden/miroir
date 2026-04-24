-- Migration 003: Extend tasks table for PersistentTaskRegistry (Phase 3)
-- Adds columns needed by the TaskRegistry trait so MiroirTask survives pod restarts.

ALTER TABLE tasks ADD COLUMN started_at INTEGER;
ALTER TABLE tasks ADD COLUMN finished_at INTEGER;
ALTER TABLE tasks ADD COLUMN index_uid TEXT;
ALTER TABLE tasks ADD COLUMN task_type TEXT;
ALTER TABLE tasks ADD COLUMN node_errors TEXT NOT NULL DEFAULT '{}';
