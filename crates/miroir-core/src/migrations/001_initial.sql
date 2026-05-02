-- Migration 001: Core task store tables (tables 1-7 from plan §4)
-- Creates the foundational tables for task registry, sessions, jobs, and leader election.

-- Table 1: tasks — Miroir task registry
CREATE TABLE IF NOT EXISTS tasks (
    miroir_id   TEXT PRIMARY KEY,
    created_at  INTEGER NOT NULL,
    status      TEXT NOT NULL,   -- enqueued | processing | succeeded | failed | canceled
    node_tasks  TEXT NOT NULL,   -- JSON: {"node-0": 42, "node-1": 17}
    error       TEXT,
    started_at  INTEGER,
    finished_at INTEGER,
    index_uid   TEXT,
    task_type   TEXT,
    node_errors TEXT NOT NULL DEFAULT '{}'  -- JSON: {"node-0": "error message"}
);

-- Table 2: node_settings_version — per-(index, node) settings freshness
CREATE TABLE IF NOT EXISTS node_settings_version (
    index_uid   TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    version     INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (index_uid, node_id)
);

-- Table 3: aliases — atomic index aliases (single-target and multi-target)
CREATE TABLE IF NOT EXISTS aliases (
    name          TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,          -- 'single' | 'multi'
    current_uid   TEXT,                   -- non-null when kind='single'
    target_uids   TEXT,                   -- JSON array of UIDs; non-null when kind='multi'
    version       INTEGER NOT NULL,       -- monotonic flip counter
    created_at    INTEGER NOT NULL,
    history       TEXT NOT NULL           -- JSON array: last N prior states
);

-- Table 4: sessions — read-your-writes session pins
CREATE TABLE IF NOT EXISTS sessions (
    session_id            TEXT PRIMARY KEY,
    last_write_mtask_id   TEXT,           -- nullable: session may exist before any write
    last_write_at         INTEGER,
    pinned_group          INTEGER,        -- group_id that first reached per-group quorum
    min_settings_version  INTEGER NOT NULL,
    ttl                   INTEGER NOT NULL -- expiry timestamp (ms since epoch)
);

-- Table 5: idempotency_cache — write deduplication
CREATE TABLE IF NOT EXISTS idempotency_cache (
    key              TEXT PRIMARY KEY,
    body_sha256      BLOB NOT NULL,
    miroir_task_id   TEXT NOT NULL,
    expires_at       INTEGER NOT NULL
);

-- Table 6: jobs — work-queued background jobs
CREATE TABLE IF NOT EXISTS jobs (
    id                 TEXT PRIMARY KEY,
    type               TEXT NOT NULL,   -- dump_import | reshard_backfill | ...
    params             TEXT NOT NULL,   -- JSON
    state              TEXT NOT NULL,   -- queued | in_progress | completed | failed
    claimed_by         TEXT,            -- pod_id of current claimant
    claim_expires_at   INTEGER,         -- lease heartbeat expiry
    progress           TEXT NOT NULL    -- JSON: { bytes_processed, docs_routed, last_cursor, ... }
);

-- Table 7: leader_lease — singleton-coordinator lease
CREATE TABLE IF NOT EXISTS leader_lease (
    scope        TEXT PRIMARY KEY,   -- e.g. "reshard:<index>", "alias_flip:<name>"
    holder       TEXT NOT NULL,      -- pod_id of current leader
    expires_at   INTEGER NOT NULL    -- renewed every 3s with a 10s TTL
);
