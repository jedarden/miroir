-- Migration 004: Mode B operation state persistence
-- Persists phase state for leader-only singleton coordinators (plan §14.5).
-- When a leader fails, the new leader reads the persisted phase state
-- and resumes from the last committed phase boundary.

-- Table 15: mode_b_operations -- singleton coordinator operation state
CREATE TABLE IF NOT EXISTS mode_b_operations (
    operation_id      TEXT PRIMARY KEY,     -- unique operation ID
    operation_type    TEXT NOT NULL,        -- reshard | rebalance | alias_flip | settings_broadcast | ilm | scoped_key_rotation
    scope             TEXT NOT NULL,        -- e.g., "reshard:products", "rebalance", "ilm"
    phase             TEXT NOT NULL,        -- current phase identifier (operation-specific)
    phase_started_at  INTEGER NOT NULL,     -- when current phase started (UNIX ms)
    created_at        INTEGER NOT NULL,     -- operation creation time (UNIX ms)
    updated_at        INTEGER NOT NULL,     -- last update time (UNIX ms)
    state_json        TEXT NOT NULL,        -- operation-specific state (JSON)
    error             TEXT,                 -- error message if failed
    status            TEXT NOT NULL,        -- running | paused | completed | failed
    -- Reshard-specific fields (nullable for other operation types)
    index_uid         TEXT,                 -- index being resharded
    old_shards        INTEGER,              -- old shard count (reshard)
    target_shards     INTEGER,              -- target shard count (reshard)
    shadow_index      TEXT,                 -- shadow index name (reshard)
    documents_backfilled INTEGER DEFAULT 0, -- backfill progress (reshard)
    total_documents   INTEGER DEFAULT 0     -- total docs to backfill (reshard)
);

-- Index for looking up operations by scope (for leader recovery)
CREATE INDEX IF NOT EXISTS mode_b_operations_scope ON mode_b_operations(scope);

-- Index for looking up operations by type
CREATE INDEX IF NOT EXISTS mode_b_operations_type ON mode_b_operations(operation_type);

-- Index for cleanup of old completed operations
CREATE INDEX IF NOT EXISTS mode_b_operations_updated ON mode_b_operations(updated_at);
