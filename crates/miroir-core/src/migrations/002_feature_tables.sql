-- Migration 002: Feature tables (tables 8-14 from plan §4)
-- Creates tables for canaries, CDC, tenant mapping, ILM, search UI config, and admin sessions.

-- Table 8: canaries — canary definitions
CREATE TABLE IF NOT EXISTS canaries (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    index_uid      TEXT NOT NULL,
    interval_s     INTEGER NOT NULL,
    query_json     TEXT NOT NULL,     -- JSON: the canary query body
    assertions_json TEXT NOT NULL,    -- JSON: array of assertion specs
    enabled        INTEGER NOT NULL,  -- 0 | 1
    created_at     INTEGER NOT NULL
);

-- Table 9: canary_runs — canary run history
CREATE TABLE IF NOT EXISTS canary_runs (
    canary_id              TEXT NOT NULL,
    ran_at                 INTEGER NOT NULL,
    status                 TEXT NOT NULL,    -- pass | fail | error
    latency_ms             INTEGER NOT NULL,
    failed_assertions_json TEXT,             -- JSON array or NULL when pass
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

-- Table 10: cdc_cursors — per-sink per-index CDC cursor
CREATE TABLE IF NOT EXISTS cdc_cursors (
    sink_name       TEXT NOT NULL,
    index_uid       TEXT NOT NULL,
    last_event_seq  INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (sink_name, index_uid)
);

-- Table 11: tenant_map — API-key → tenant mapping for tenant_affinity.mode: api_key
CREATE TABLE IF NOT EXISTS tenant_map (
    api_key_hash  BLOB PRIMARY KEY,   -- sha256(api_key)
    tenant_id     TEXT NOT NULL,
    group_id      INTEGER             -- nullable: NULL falls through to hash(tenant_id) % RG
);

-- Table 12: rollover_policies — ILM rollover policies
CREATE TABLE IF NOT EXISTS rollover_policies (
    name           TEXT PRIMARY KEY,
    write_alias    TEXT NOT NULL,
    read_alias     TEXT NOT NULL,
    pattern        TEXT NOT NULL,      -- e.g. "logs-{YYYY-MM-DD}"
    triggers_json  TEXT NOT NULL,      -- JSON: { max_docs, max_age, max_size_gb }
    retention_json TEXT NOT NULL,      -- JSON: { keep_indexes }
    template_json  TEXT NOT NULL,      -- JSON: { primary_key, settings_ref }
    enabled        INTEGER NOT NULL    -- 0 | 1
);

-- Table 13: search_ui_config — per-index search-UI configuration
CREATE TABLE IF NOT EXISTS search_ui_config (
    index_uid    TEXT PRIMARY KEY,
    config_json  TEXT NOT NULL,        -- JSON: the search_ui config
    updated_at   INTEGER NOT NULL
);

-- Table 14: admin_sessions — Admin UI session registry
CREATE TABLE IF NOT EXISTS admin_sessions (
    session_id      TEXT PRIMARY KEY,
    csrf_token      TEXT NOT NULL,
    admin_key_hash  TEXT NOT NULL,    -- sha256 of admin key used at login
    created_at      INTEGER NOT NULL,
    expires_at      INTEGER NOT NULL,
    revoked         INTEGER NOT NULL DEFAULT 0,
    user_agent      TEXT,
    source_ip       TEXT
);

-- Index for admin session expiry queries
CREATE INDEX IF NOT EXISTS admin_sessions_expires ON admin_sessions(expires_at);
