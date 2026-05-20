-- Migration 002: Feature-flagged tables 8–14
-- These tables are only used when their respective features are enabled
-- (canary_runner, CDC, tenant_map API key mode, ILM rollover, search UI, admin sessions)

-- Table 8: canaries (plan §13.18)
CREATE TABLE IF NOT EXISTS canaries (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    index_uid        TEXT NOT NULL,
    interval_s       INTEGER NOT NULL,
    query_json       TEXT NOT NULL,
    assertions_json  TEXT NOT NULL,
    enabled          INTEGER NOT NULL,
    created_at       INTEGER NOT NULL
);

-- Table 9: canary_runs (plan §13.18)
-- Auto-pruned to run_history_per_canary (default 100) on insert
CREATE TABLE IF NOT EXISTS canary_runs (
    canary_id              TEXT NOT NULL,
    ran_at                 INTEGER NOT NULL,
    status                 TEXT NOT NULL,
    latency_ms             INTEGER NOT NULL,
    failed_assertions_json TEXT,
    PRIMARY KEY (canary_id, ran_at)
);

-- Trigger to auto-prune canary_runs to run_history_per_canary (default 100)
-- Fires after insert to keep only the N most recent runs per canary
CREATE TRIGGER IF NOT EXISTS canary_runs_auto_prune
AFTER INSERT ON canary_runs
BEGIN
    DELETE FROM canary_runs
    WHERE canary_id = NEW.canary_id
      AND ran_at NOT IN (
          SELECT ran_at
          FROM canary_runs
          WHERE canary_id = NEW.canary_id
          ORDER BY ran_at DESC
          LIMIT 100
      );
END;

-- Table 10: cdc_cursors (plan §13.13)
-- Composite PK on (sink_name, index_uid) for update-in-place
CREATE TABLE IF NOT EXISTS cdc_cursors (
    sink_name       TEXT NOT NULL,
    index_uid       TEXT NOT NULL,
    last_event_seq  INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (sink_name, index_uid)
);

-- Table 11: tenant_map (plan §13.15)
-- api_key_hash is a 32-byte BLOB (raw sha256, never plaintext)
CREATE TABLE IF NOT EXISTS tenant_map (
    api_key_hash  BLOB PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    group_id      INTEGER
);

-- Table 12: rollover_policies (plan §13.17)
CREATE TABLE IF NOT EXISTS rollover_policies (
    name            TEXT PRIMARY KEY,
    write_alias     TEXT NOT NULL,
    read_alias      TEXT NOT NULL,
    pattern         TEXT NOT NULL,
    triggers_json   TEXT NOT NULL,
    retention_json  TEXT NOT NULL,
    template_json   TEXT NOT NULL,
    enabled         INTEGER NOT NULL
);

-- Table 13: search_ui_config (plan §13.21)
CREATE TABLE IF NOT EXISTS search_ui_config (
    index_uid    TEXT PRIMARY KEY,
    config_json  TEXT NOT NULL,
    updated_at   INTEGER NOT NULL
);

-- Table 14: admin_sessions (plan §13.19)
CREATE TABLE IF NOT EXISTS admin_sessions (
    session_id      TEXT PRIMARY KEY,
    csrf_token      TEXT NOT NULL,
    admin_key_hash  TEXT NOT NULL,
    created_at      INTEGER NOT NULL,
    expires_at      INTEGER NOT NULL,
    revoked         INTEGER NOT NULL DEFAULT 0,
    user_agent      TEXT,
    source_ip       TEXT
);

-- Index for lazy eviction of expired admin sessions (plan §4 footnote)
CREATE INDEX IF NOT EXISTS admin_sessions_expires ON admin_sessions(expires_at);
