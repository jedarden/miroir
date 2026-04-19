use crate::task_store::*;
use crate::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

const SCHEMA_VERSION: i64 = 1;

/// DDL for schema_versions + tables 1–7.
const MIGRATION_V1: &str = r#"
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
"#;

pub struct SqliteTaskStore {
    conn: Mutex<Connection>,
}

impl SqliteTaskStore {
    /// Open (or create) the SQLite database at `path`, configure WAL + busy_timeout.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::configure(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open an in-memory database (for tests and single-pod dev).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::configure(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn configure(conn: &Connection) -> Result<()> {
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;")?;
        Ok(())
    }

    fn run_migration(conn: &Connection) -> Result<()> {
        // Create schema_versions first so we can query it
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_versions (
                version INTEGER PRIMARY KEY,
                applied_at INTEGER NOT NULL
            );",
        )?;

        let current: Option<i64> = conn
            .query_row(
                "SELECT MAX(version) FROM schema_versions",
                [],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        if current.unwrap_or(0) < SCHEMA_VERSION {
            conn.execute_batch(MIGRATION_V1)?;
            conn.execute(
                "INSERT OR IGNORE INTO schema_versions (version, applied_at) VALUES (?1, ?2)",
                params![SCHEMA_VERSION, now_ms()],
            )?;
        }

        Ok(())
    }

    // --- Table 1: tasks helpers ---

    fn task_row_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRow> {
        let node_tasks_json: String = row.get(3)?;
        let node_tasks: HashMap<String, u64> = serde_json::from_str(&node_tasks_json)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(TaskRow {
            miroir_id: row.get(0)?,
            created_at: row.get(1)?,
            status: row.get(2)?,
            node_tasks,
            error: row.get(4)?,
        })
    }

    // --- Table 3: aliases helpers ---

    fn alias_row_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AliasRow> {
        let target_uids_json: Option<String> = row.get(3)?;
        let target_uids: Option<Vec<String>> = target_uids_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        let history_json: String = row.get(6)?;
        let history: Vec<AliasHistoryEntry> = serde_json::from_str(&history_json)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(AliasRow {
            name: row.get(0)?,
            kind: row.get(1)?,
            current_uid: row.get(2)?,
            target_uids,
            version: row.get(4)?,
            created_at: row.get(5)?,
            history,
        })
    }
}

impl TaskStore for SqliteTaskStore {
    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        Self::run_migration(&conn)?;
        Ok(())
    }

    // --- Table 1: tasks ---

    fn insert_task(&self, task: &NewTask) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let node_tasks_json = serde_json::to_string(&task.node_tasks)?;
        conn.execute(
            "INSERT INTO tasks (miroir_id, created_at, status, node_tasks, error)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                task.miroir_id,
                task.created_at,
                task.status,
                node_tasks_json,
                task.error,
            ],
        )?;
        Ok(())
    }

    fn get_task(&self, miroir_id: &str) -> Result<Option<TaskRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT miroir_id, created_at, status, node_tasks, error
                 FROM tasks WHERE miroir_id = ?1",
                params![miroir_id],
                Self::task_row_from_row,
            )
            .optional()?)
    }

    fn update_task_status(&self, miroir_id: &str, status: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE tasks SET status = ?1 WHERE miroir_id = ?2",
            params![status, miroir_id],
        )?;
        Ok(rows > 0)
    }

    fn update_node_task(&self, miroir_id: &str, node_id: &str, task_uid: u64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        // Read-modify-write on node_tasks JSON
        let tx = conn.unchecked_transaction()?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT node_tasks FROM tasks WHERE miroir_id = ?1",
                params![miroir_id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(json) = existing else {
            return Ok(false);
        };
        let mut map: HashMap<String, u64> = serde_json::from_str(&json)?;
        map.insert(node_id.to_string(), task_uid);
        let updated = serde_json::to_string(&map)?;
        let rows = tx.execute(
            "UPDATE tasks SET node_tasks = ?1 WHERE miroir_id = ?2",
            params![updated, miroir_id],
        )?;
        tx.commit()?;
        Ok(rows > 0)
    }

    fn set_task_error(&self, miroir_id: &str, error: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE tasks SET error = ?1 WHERE miroir_id = ?2",
            params![error, miroir_id],
        )?;
        Ok(rows > 0)
    }

    fn list_tasks(&self, filter: &TaskFilter) -> Result<Vec<TaskRow>> {
        let conn = self.conn.lock().unwrap();
        let mut sql = "SELECT miroir_id, created_at, status, node_tasks, error FROM tasks"
            .to_string();
        if filter.status.is_some() {
            sql.push_str(" WHERE status = ?1");
        }
        sql.push_str(" ORDER BY created_at DESC");
        if let Some(limit) = filter.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = filter.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        let mut stmt = conn.prepare(&sql)?;
        let rows = if let Some(ref status) = filter.status {
            stmt.query_map(params![status], Self::task_row_from_row)?
        } else {
            stmt.query_map([], Self::task_row_from_row)?
        };
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    // --- Table 2: node_settings_version ---

    fn upsert_node_settings_version(
        &self,
        index_uid: &str,
        node_id: &str,
        version: i64,
        updated_at: i64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO node_settings_version (index_uid, node_id, version, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(index_uid, node_id) DO UPDATE SET version = ?3, updated_at = ?4",
            params![index_uid, node_id, version, updated_at],
        )?;
        Ok(())
    }

    fn get_node_settings_version(
        &self,
        index_uid: &str,
        node_id: &str,
    ) -> Result<Option<NodeSettingsVersionRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT index_uid, node_id, version, updated_at
                 FROM node_settings_version WHERE index_uid = ?1 AND node_id = ?2",
                params![index_uid, node_id],
                |row| {
                    Ok(NodeSettingsVersionRow {
                        index_uid: row.get(0)?,
                        node_id: row.get(1)?,
                        version: row.get(2)?,
                        updated_at: row.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    // --- Table 3: aliases ---

    fn create_alias(&self, alias: &NewAlias) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let target_uids_json = alias
            .target_uids
            .as_ref()
            .map(|uids| serde_json::to_string(uids))
            .transpose()?;
        let history_json = serde_json::to_string(&alias.history)?;
        conn.execute(
            "INSERT INTO aliases (name, kind, current_uid, target_uids, version, created_at, history)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                alias.name,
                alias.kind,
                alias.current_uid,
                target_uids_json,
                alias.version,
                alias.created_at,
                history_json,
            ],
        )?;
        Ok(())
    }

    fn get_alias(&self, name: &str) -> Result<Option<AliasRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT name, kind, current_uid, target_uids, version, created_at, history
                 FROM aliases WHERE name = ?1",
                params![name],
                Self::alias_row_from_row,
            )
            .optional()?)
    }

    fn flip_alias(&self, name: &str, new_uid: &str, history_retention: usize) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;

        // Read current
        let existing: Option<(String, i64, String)> = tx
            .query_row(
                "SELECT current_uid, version, history FROM aliases WHERE name = ?1 AND kind = 'single'",
                params![name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((old_uid, old_version, history_json)) = existing else {
            return Ok(false);
        };

        // Build new history
        let mut history: Vec<AliasHistoryEntry> = serde_json::from_str(&history_json)?;
        if !old_uid.is_empty() {
            history.push(AliasHistoryEntry {
                uid: old_uid,
                flipped_at: now_ms(),
            });
        }
        // Enforce retention bound
        while history.len() > history_retention {
            history.remove(0);
        }

        let new_history_json = serde_json::to_string(&history)?;
        let new_version = old_version + 1;

        let rows = tx.execute(
            "UPDATE aliases SET current_uid = ?1, version = ?2, history = ?3 WHERE name = ?4",
            params![new_uid, new_version, new_history_json, name],
        )?;
        tx.commit()?;
        Ok(rows > 0)
    }

    fn delete_alias(&self, name: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute("DELETE FROM aliases WHERE name = ?1", params![name])?;
        Ok(rows > 0)
    }

    // --- Table 4: sessions ---

    fn upsert_session(&self, session: &SessionRow) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO sessions (session_id, last_write_mtask_id, last_write_at, pinned_group, min_settings_version, ttl)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(session_id) DO UPDATE SET
                last_write_mtask_id = ?2,
                last_write_at = ?3,
                pinned_group = ?4,
                min_settings_version = ?5,
                ttl = ?6",
            params![
                session.session_id,
                session.last_write_mtask_id,
                session.last_write_at,
                session.pinned_group,
                session.min_settings_version,
                session.ttl,
            ],
        )?;
        Ok(())
    }

    fn get_session(&self, session_id: &str) -> Result<Option<SessionRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT session_id, last_write_mtask_id, last_write_at, pinned_group, min_settings_version, ttl
                 FROM sessions WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok(SessionRow {
                        session_id: row.get(0)?,
                        last_write_mtask_id: row.get(1)?,
                        last_write_at: row.get(2)?,
                        pinned_group: row.get(3)?,
                        min_settings_version: row.get(4)?,
                        ttl: row.get(5)?,
                    })
                },
            )
            .optional()?)
    }

    fn delete_expired_sessions(&self, now_ms: i64) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute("DELETE FROM sessions WHERE ttl < ?1", params![now_ms])?;
        Ok(rows)
    }

    // --- Table 5: idempotency_cache ---

    fn insert_idempotency_entry(&self, entry: &IdempotencyEntry) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO idempotency_cache (key, body_sha256, miroir_task_id, expires_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                entry.key,
                entry.body_sha256,
                entry.miroir_task_id,
                entry.expires_at,
            ],
        )?;
        Ok(())
    }

    fn get_idempotency_entry(&self, key: &str) -> Result<Option<IdempotencyEntry>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT key, body_sha256, miroir_task_id, expires_at
                 FROM idempotency_cache WHERE key = ?1",
                params![key],
                |row| {
                    Ok(IdempotencyEntry {
                        key: row.get(0)?,
                        body_sha256: row.get(1)?,
                        miroir_task_id: row.get(2)?,
                        expires_at: row.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    fn delete_expired_idempotency_entries(&self, now_ms: i64) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let rows =
            conn.execute("DELETE FROM idempotency_cache WHERE expires_at < ?1", params![now_ms])?;
        Ok(rows)
    }

    // --- Table 6: jobs ---

    fn insert_job(&self, job: &NewJob) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO jobs (id, type, params, state, claimed_by, claim_expires_at, progress)
             VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5)",
            params![job.id, job.type_, job.params, job.state, job.progress,],
        )?;
        Ok(())
    }

    fn get_job(&self, id: &str) -> Result<Option<JobRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT id, type, params, state, claimed_by, claim_expires_at, progress
                 FROM jobs WHERE id = ?1",
                params![id],
                |row| {
                    Ok(JobRow {
                        id: row.get(0)?,
                        type_: row.get(1)?,
                        params: row.get(2)?,
                        state: row.get(3)?,
                        claimed_by: row.get(4)?,
                        claim_expires_at: row.get(5)?,
                        progress: row.get(6)?,
                    })
                },
            )
            .optional()?)
    }

    fn claim_job(&self, id: &str, claimed_by: &str, claim_expires_at: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        // CAS: only claim if state is 'queued' (unclaimed)
        let rows = conn.execute(
            "UPDATE jobs SET claimed_by = ?1, claim_expires_at = ?2, state = 'in_progress'
             WHERE id = ?3 AND state = 'queued'",
            params![claimed_by, claim_expires_at, id],
        )?;
        Ok(rows > 0)
    }

    fn update_job_progress(&self, id: &str, state: &str, progress: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE jobs SET state = ?1, progress = ?2 WHERE id = ?3",
            params![state, progress, id],
        )?;
        Ok(rows > 0)
    }

    fn renew_job_claim(&self, id: &str, claim_expires_at: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE jobs SET claim_expires_at = ?1 WHERE id = ?2 AND claimed_by IS NOT NULL",
            params![claim_expires_at, id],
        )?;
        Ok(rows > 0)
    }

    fn list_jobs_by_state(&self, state: &str) -> Result<Vec<JobRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, type, params, state, claimed_by, claim_expires_at, progress
             FROM jobs WHERE state = ?1",
        )?;
        let rows = stmt.query_map(params![state], |row| {
            Ok(JobRow {
                id: row.get(0)?,
                type_: row.get(1)?,
                params: row.get(2)?,
                state: row.get(3)?,
                claimed_by: row.get(4)?,
                claim_expires_at: row.get(5)?,
                progress: row.get(6)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    // --- Table 7: leader_lease ---

    fn try_acquire_leader_lease(
        &self,
        scope: &str,
        holder: &str,
        expires_at: i64,
        now_ms: i64,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let existing: Option<LeaderLeaseRow> = conn
            .query_row(
                "SELECT scope, holder, expires_at FROM leader_lease WHERE scope = ?1",
                params![scope],
                |row| {
                    Ok(LeaderLeaseRow {
                        scope: row.get(0)?,
                        holder: row.get(1)?,
                        expires_at: row.get(2)?,
                    })
                },
            )
            .optional()?;

        match existing {
            None => {
                conn.execute(
                    "INSERT INTO leader_lease (scope, holder, expires_at) VALUES (?1, ?2, ?3)",
                    params![scope, holder, expires_at],
                )?;
                Ok(true)
            }
            Some(lease) if lease.holder == holder || lease.expires_at <= now_ms => {
                let rows = conn.execute(
                    "UPDATE leader_lease SET holder = ?1, expires_at = ?2 WHERE scope = ?3",
                    params![holder, expires_at, scope],
                )?;
                Ok(rows > 0)
            }
            Some(_) => Ok(false),
        }
    }

    fn renew_leader_lease(&self, scope: &str, holder: &str, expires_at: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE leader_lease SET expires_at = ?1 WHERE scope = ?2 AND holder = ?3",
            params![expires_at, scope, holder],
        )?;
        Ok(rows > 0)
    }

    fn get_leader_lease(&self, scope: &str) -> Result<Option<LeaderLeaseRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT scope, holder, expires_at FROM leader_lease WHERE scope = ?1",
                params![scope],
                |row| {
                    Ok(LeaderLeaseRow {
                        scope: row.get(0)?,
                        holder: row.get(1)?,
                        expires_at: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_store() -> SqliteTaskStore {
        let store = SqliteTaskStore::open_in_memory().unwrap();
        store.migrate().unwrap();
        store
    }

    // --- Table 1: tasks ---

    #[test]
    fn task_crud_round_trip() {
        let store = test_store();
        let mut node_tasks = HashMap::new();
        node_tasks.insert("node-0".to_string(), 42u64);
        node_tasks.insert("node-1".to_string(), 17u64);

        let new_task = NewTask {
            miroir_id: "test-task-1".to_string(),
            created_at: 1000,
            status: "enqueued".to_string(),
            node_tasks: node_tasks.clone(),
            error: None,
        };
        store.insert_task(&new_task).unwrap();

        let task = store.get_task("test-task-1").unwrap().unwrap();
        assert_eq!(task.miroir_id, "test-task-1");
        assert_eq!(task.status, "enqueued");
        assert_eq!(task.node_tasks, node_tasks);
        assert!(task.error.is_none());

        // Update status
        assert!(store.update_task_status("test-task-1", "processing").unwrap());
        let task = store.get_task("test-task-1").unwrap().unwrap();
        assert_eq!(task.status, "processing");

        // Update node task
        assert!(store.update_node_task("test-task-1", "node-0", 99).unwrap());
        let task = store.get_task("test-task-1").unwrap().unwrap();
        assert_eq!(task.node_tasks.get("node-0"), Some(&99u64));
        assert_eq!(task.node_tasks.get("node-1"), Some(&17u64));

        // Set error
        assert!(store.set_task_error("test-task-1", "boom").unwrap());
        let task = store.get_task("test-task-1").unwrap().unwrap();
        assert_eq!(task.error.as_deref(), Some("boom"));

        // Missing task
        assert!(store.get_task("no-such-task").unwrap().is_none());
        assert!(!store.update_task_status("no-such-task", "failed").unwrap());
    }

    #[test]
    fn task_list_with_filter() {
        let store = test_store();

        for i in 0..5 {
            let mut nt = HashMap::new();
            nt.insert("node-0".to_string(), i as u64);
            store
                .insert_task(&NewTask {
                    miroir_id: format!("task-{i}"),
                    created_at: i as i64 * 1000,
                    status: if i < 3 { "enqueued" } else { "succeeded" }.to_string(),
                    node_tasks: nt,
                    error: None,
                })
                .unwrap();
        }

        // All tasks
        let all = store.list_tasks(&TaskFilter::default()).unwrap();
        assert_eq!(all.len(), 5);

        // Filter by status
        let enqueued = store
            .list_tasks(&TaskFilter {
                status: Some("enqueued".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(enqueued.len(), 3);

        // With limit + offset
        let page = store
            .list_tasks(&TaskFilter {
                limit: Some(2),
                offset: Some(1),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(page.len(), 2);
    }

    // --- Table 2: node_settings_version ---

    #[test]
    fn node_settings_version_upsert_and_get() {
        let store = test_store();

        // Insert
        store
            .upsert_node_settings_version("idx-1", "node-0", 5, 1000)
            .unwrap();
        let row = store
            .get_node_settings_version("idx-1", "node-0")
            .unwrap()
            .unwrap();
        assert_eq!(row.version, 5);
        assert_eq!(row.updated_at, 1000);

        // Upsert (update)
        store
            .upsert_node_settings_version("idx-1", "node-0", 7, 2000)
            .unwrap();
        let row = store
            .get_node_settings_version("idx-1", "node-0")
            .unwrap()
            .unwrap();
        assert_eq!(row.version, 7);
        assert_eq!(row.updated_at, 2000);

        // Missing
        assert!(store
            .get_node_settings_version("idx-1", "node-99")
            .unwrap()
            .is_none());
    }

    // --- Table 3: aliases ---

    #[test]
    fn alias_single_crud_and_flip() {
        let store = test_store();

        store
            .create_alias(&NewAlias {
                name: "prod-logs".to_string(),
                kind: "single".to_string(),
                current_uid: Some("uid-v1".to_string()),
                target_uids: None,
                version: 1,
                created_at: 1000,
                history: vec![],
            })
            .unwrap();

        let alias = store.get_alias("prod-logs").unwrap().unwrap();
        assert_eq!(alias.current_uid.as_deref(), Some("uid-v1"));
        assert_eq!(alias.version, 1);

        // Flip
        assert!(store.flip_alias("prod-logs", "uid-v2", 10).unwrap());
        let alias = store.get_alias("prod-logs").unwrap().unwrap();
        assert_eq!(alias.current_uid.as_deref(), Some("uid-v2"));
        assert_eq!(alias.version, 2);
        assert_eq!(alias.history.len(), 1);
        assert_eq!(alias.history[0].uid, "uid-v1");

        // Flip again
        assert!(store.flip_alias("prod-logs", "uid-v3", 2).unwrap());
        let alias = store.get_alias("prod-logs").unwrap().unwrap();
        assert_eq!(alias.history.len(), 2); // retention = 2, so both kept

        // Flip once more — retention should trim
        assert!(store.flip_alias("prod-logs", "uid-v4", 2).unwrap());
        let alias = store.get_alias("prod-logs").unwrap().unwrap();
        assert_eq!(alias.history.len(), 2); // trimmed to 2

        // Delete
        assert!(store.delete_alias("prod-logs").unwrap());
        assert!(store.get_alias("prod-logs").unwrap().is_none());
    }

    #[test]
    fn alias_multi_target() {
        let store = test_store();

        store
            .create_alias(&NewAlias {
                name: "search-all".to_string(),
                kind: "multi".to_string(),
                current_uid: None,
                target_uids: Some(vec!["uid-a".to_string(), "uid-b".to_string()]),
                version: 1,
                created_at: 1000,
                history: vec![],
            })
            .unwrap();

        let alias = store.get_alias("search-all").unwrap().unwrap();
        assert_eq!(alias.kind, "multi");
        assert_eq!(
            alias.target_uids.unwrap(),
            vec!["uid-a".to_string(), "uid-b".to_string()]
        );
    }

    // --- Table 4: sessions ---

    #[test]
    fn session_upsert_get_and_expire() {
        let store = test_store();

        let session = SessionRow {
            session_id: "sess-1".to_string(),
            last_write_mtask_id: Some("task-1".to_string()),
            last_write_at: Some(1000),
            pinned_group: Some(2),
            min_settings_version: 5,
            ttl: 2000,
        };
        store.upsert_session(&session).unwrap();

        let got = store.get_session("sess-1").unwrap().unwrap();
        assert_eq!(got.last_write_mtask_id.as_deref(), Some("task-1"));
        assert_eq!(got.pinned_group, Some(2));
        assert_eq!(got.min_settings_version, 5);

        // Upsert (update)
        let updated = SessionRow {
            session_id: "sess-1".to_string(),
            last_write_mtask_id: Some("task-2".to_string()),
            last_write_at: Some(1500),
            pinned_group: None,
            min_settings_version: 6,
            ttl: 2500,
        };
        store.upsert_session(&updated).unwrap();
        let got = store.get_session("sess-1").unwrap().unwrap();
        assert_eq!(got.last_write_mtask_id.as_deref(), Some("task-2"));
        assert!(got.pinned_group.is_none());

        // Create expired session
        store
            .upsert_session(&SessionRow {
                session_id: "sess-old".to_string(),
                last_write_mtask_id: None,
                last_write_at: None,
                pinned_group: None,
                min_settings_version: 1,
                ttl: 500, // expired
            })
            .unwrap();

        let deleted = store.delete_expired_sessions(1000).unwrap();
        assert_eq!(deleted, 1);
        assert!(store.get_session("sess-old").unwrap().is_none());
        assert!(store.get_session("sess-1").unwrap().is_some());
    }

    // --- Table 5: idempotency_cache ---

    #[test]
    fn idempotency_crud_and_expire() {
        let store = test_store();

        let sha = vec![0u8; 32]; // dummy 32-byte hash
        store
            .insert_idempotency_entry(&IdempotencyEntry {
                key: "req-abc".to_string(),
                body_sha256: sha.clone(),
                miroir_task_id: "task-1".to_string(),
                expires_at: 5000,
            })
            .unwrap();

        let entry = store.get_idempotency_entry("req-abc").unwrap().unwrap();
        assert_eq!(entry.body_sha256, sha);
        assert_eq!(entry.miroir_task_id, "task-1");

        // Missing
        assert!(store.get_idempotency_entry("nope").unwrap().is_none());

        // Expire
        store
            .insert_idempotency_entry(&IdempotencyEntry {
                key: "req-old".to_string(),
                body_sha256: sha.clone(),
                miroir_task_id: "task-2".to_string(),
                expires_at: 100, // already expired
            })
            .unwrap();

        let deleted = store.delete_expired_idempotency_entries(1000).unwrap();
        assert_eq!(deleted, 1);
        assert!(store.get_idempotency_entry("req-old").unwrap().is_none());
        assert!(store.get_idempotency_entry("req-abc").unwrap().is_some());
    }

    // --- Table 6: jobs ---

    #[test]
    fn job_insert_claim_complete() {
        let store = test_store();

        store
            .insert_job(&NewJob {
                id: "job-1".to_string(),
                type_: "dump_import".to_string(),
                params: r#"{"index": "logs"}"#.to_string(),
                state: "queued".to_string(),
                progress: "{}".to_string(),
            })
            .unwrap();

        let job = store.get_job("job-1").unwrap().unwrap();
        assert_eq!(job.state, "queued");
        assert!(job.claimed_by.is_none());

        // Claim
        assert!(store.claim_job("job-1", "pod-a", 10000).unwrap());
        let job = store.get_job("job-1").unwrap().unwrap();
        assert_eq!(job.state, "in_progress");
        assert_eq!(job.claimed_by.as_deref(), Some("pod-a"));

        // Cannot double-claim
        assert!(!store.claim_job("job-1", "pod-b", 10001).unwrap());

        // Update progress
        assert!(store
            .update_job_progress("job-1", "in_progress", r#"{"bytes": 1024}"#)
            .unwrap());

        // Renew claim (heartbeat)
        assert!(store.renew_job_claim("job-1", 11000).unwrap());

        // Complete
        assert!(store
            .update_job_progress("job-1", "completed", r#"{"bytes": 4096}"#)
            .unwrap());
    }

    #[test]
    fn job_list_by_state() {
        let store = test_store();

        for i in 0..4 {
            store
                .insert_job(&NewJob {
                    id: format!("job-{i}"),
                    type_: "reshard_backfill".to_string(),
                    params: "{}".to_string(),
                    state: "queued".to_string(),
                    progress: "{}".to_string(),
                })
                .unwrap();
        }
        // Claim one
        store.claim_job("job-2", "pod-a", 99999).unwrap();

        let queued = store.list_jobs_by_state("queued").unwrap();
        assert_eq!(queued.len(), 3);

        let in_progress = store.list_jobs_by_state("in_progress").unwrap();
        assert_eq!(in_progress.len(), 1);
        assert_eq!(in_progress[0].id, "job-2");
    }

    // --- Table 7: leader_lease ---

    #[test]
    fn leader_lease_acquire_renew_steal() {
        let store = test_store();

        // First acquisition (now=0, expires=10000)
        assert!(store
            .try_acquire_leader_lease("reshard:idx-1", "pod-a", 10000, 0)
            .unwrap());

        // Same holder can re-acquire (now=5000, extends to 15000)
        assert!(store
            .try_acquire_leader_lease("reshard:idx-1", "pod-a", 15000, 5000)
            .unwrap());

        // Different holder, lease not expired — fails (now=6000, lease=15000)
        assert!(!store
            .try_acquire_leader_lease("reshard:idx-1", "pod-b", 20000, 6000)
            .unwrap());

        // Lease expired — different holder can steal (now=20000, lease=15000)
        assert!(store
            .try_acquire_leader_lease("reshard:idx-1", "pod-b", 30000, 20000)
            .unwrap());

        // Renew by current holder
        assert!(store.renew_leader_lease("reshard:idx-1", "pod-b", 35000).unwrap());

        // Wrong holder cannot renew
        assert!(!store.renew_leader_lease("reshard:idx-1", "pod-a", 35000).unwrap());

        // Get lease
        let lease = store.get_leader_lease("reshard:idx-1").unwrap().unwrap();
        assert_eq!(lease.holder, "pod-b");
        assert_eq!(lease.expires_at, 35000);
    }

    // --- Migration idempotency ---

    #[test]
    fn migration_is_idempotent() {
        let store = SqliteTaskStore::open_in_memory().unwrap();
        store.migrate().unwrap();

        // Insert data to prove it survives re-migration
        store
            .insert_task(&NewTask {
                miroir_id: "survivor".to_string(),
                created_at: 1,
                status: "enqueued".to_string(),
                node_tasks: HashMap::new(),
                error: None,
            })
            .unwrap();

        // Run migration again — should be a no-op
        store.migrate().unwrap();

        // Data still there
        assert!(store.get_task("survivor").unwrap().is_some());
    }

    #[test]
    fn schema_version_recorded() {
        let store = SqliteTaskStore::open_in_memory().unwrap();
        store.migrate().unwrap();

        let conn = store.conn.lock().unwrap();
        let version: i64 = conn
            .query_row(
                "SELECT MAX(version) FROM schema_versions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    // --- WAL mode ---

    #[test]
    fn wal_mode_enabled() {
        let store = SqliteTaskStore::open_in_memory().unwrap();
        let conn = store.conn.lock().unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "memory"); // in-memory DB uses memory mode, which is fine
    }

    #[test]
    fn wal_mode_on_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let store = SqliteTaskStore::open(&path).unwrap();
        store.migrate().unwrap();

        let conn = store.conn.lock().unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }

    // --- Concurrent writes (single-process) ---

    #[test]
    fn concurrent_writes_no_deadlock() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.db");
        let store = Arc::new(SqliteTaskStore::open(&path).unwrap());
        store.migrate().unwrap();

        let mut handles = vec![];
        for i in 0..4 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let mut nt = HashMap::new();
                nt.insert("node-0".to_string(), i as u64);
                s.insert_task(&NewTask {
                    miroir_id: format!("concurrent-{i}"),
                    created_at: i as i64,
                    status: "enqueued".to_string(),
                    node_tasks: nt,
                    error: None,
                })
                .unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All 4 tasks should be there
        let all = store.list_tasks(&TaskFilter::default()).unwrap();
        assert_eq!(all.len(), 4);
    }
}
