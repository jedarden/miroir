-- Migration 006: TTL policy table (plan §13.14)
-- Adds per-index TTL sweep policy configuration
-- Allows operators to override global TTL settings per index

-- Create ttl_policy table
CREATE TABLE IF NOT EXISTS ttl_policy (
    index_uid TEXT PRIMARY KEY,
    sweep_interval_s INTEGER NOT NULL,
    max_deletes_per_sweep INTEGER NOT NULL,
    enabled INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
