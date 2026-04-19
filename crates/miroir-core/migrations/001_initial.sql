-- Migration 001: Initial schema for tables 1–7
-- Creates schema_versions tracking table and all core tables

CREATE TABLE IF NOT EXISTS schema_versions (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    miroir_id   TEXT PRIMARY KEY,
    created_at  INTEGER NOT NULL,
    status      TEXT NOT NULL,
    node_tasks  TEXT NOT NULL,
    error       TEXT
);

CREATE TABLE IF NOT EXISTS node_settings_version (
    index_uid   TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    version     INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (index_uid, node_id)
);

CREATE TABLE IF NOT EXISTS aliases (
    name          TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,
    current_uid   TEXT,
    target_uids   TEXT,
    version       INTEGER NOT NULL,
    created_at    INTEGER NOT NULL,
    history       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id            TEXT PRIMARY KEY,
    last_write_mtask_id   TEXT,
    last_write_at         INTEGER,
    pinned_group          INTEGER,
    min_settings_version  INTEGER NOT NULL,
    ttl                   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency_cache (
    key              TEXT PRIMARY KEY,
    body_sha256      BLOB NOT NULL,
    miroir_task_id   TEXT NOT NULL,
    expires_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id                 TEXT PRIMARY KEY,
    type               TEXT NOT NULL,
    params             TEXT NOT NULL,
    state              TEXT NOT NULL,
    claimed_by         TEXT,
    claim_expires_at   INTEGER,
    progress           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leader_lease (
    scope        TEXT PRIMARY KEY,
    holder       TEXT NOT NULL,
    expires_at   INTEGER NOT NULL
);
