//! SQLite backend for the task store.

use super::error::{Result, TaskStoreError};
use super::schema::{
    AdminSession, Alias, Canary, CanaryRun, CdcCursor, IdempotencyEntry, Job, JobState,
    LeaderLease, RolloverPolicy, SearchUiConfig, Session, Task, TaskFilter, TaskStatus, Tenant,
    SCHEMA_VERSION,
};
use super::TaskStore;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

// Legacy compatibility types for trait signature
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NodeTask {
    pub task_uid: u64,
    pub status: NodeTaskStatus,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum NodeTaskStatus {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
}

// Legacy JobStatus for trait compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
    Canceled,
}

/// Convert a String parse error to a rusqlite error.
fn parse_error<E: std::fmt::Display>(e: E) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(ParseError(e.to_string())))
}

/// Wrapper for String errors to implement std::error::Error.
#[derive(Debug)]
struct ParseError(String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}

/// SQLite task store implementation.
pub struct SqliteTaskStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteTaskStore {
    /// Create a new SQLite task store.
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        Ok(store)
    }

    /// Execute a SQL statement with parameters.
    fn execute(&self, sql: &str, params: &[&dyn rusqlite::ToSql]) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| TaskStoreError::Internal(e.to_string()))?;
        conn.execute(sql, params).map_err(Into::into)
    }

    /// Query a single row.
    fn query_row<T, F>(&self, sql: &str, params: &[&dyn rusqlite::ToSql], f: F) -> Result<T>
    where
        F: FnOnce(&rusqlite::Row) -> rusqlite::Result<T>,
    {
        let conn = self
            .conn
            .lock()
            .map_err(|e| TaskStoreError::Internal(e.to_string()))?;
        conn.query_row(sql, params, f).map_err(Into::into)
    }

    /// Prepare and execute a query, returning all rows.
    fn query_map<T, F>(&self, sql: &str, params: &[&dyn rusqlite::ToSql], f: F) -> Result<Vec<T>>
    where
        F: FnMut(&rusqlite::Row) -> rusqlite::Result<T>,
    {
        let conn = self
            .conn
            .lock()
            .map_err(|e| TaskStoreError::Internal(e.to_string()))?;
        let mut stmt = conn.prepare(sql)?;
        let rows: std::result::Result<Vec<_>, _> = stmt.query_map(params, f)?.collect();
        Ok(rows?)
    }
}

#[async_trait::async_trait]
impl TaskStore for SqliteTaskStore {
    async fn initialize(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| TaskStoreError::Internal(e.to_string()))?;

        // Enable WAL mode for better concurrency
        // Use query_row because PRAGMA journal_mode returns the new mode
        let _mode: String = conn.query_row(
            "PRAGMA journal_mode=WAL",
            &[] as &[&dyn rusqlite::ToSql],
            |row| row.get(0),
        )?;

        // Create schema_version table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL
            )",
            &[] as &[&dyn rusqlite::ToSql],
        )?;

        // Check current version
        let current_version: Option<i64> = conn
            .query_row(
                "SELECT version FROM schema_version",
                &[] as &[&dyn rusqlite::ToSql],
                |row| row.get(0),
            )
            .ok();

        if current_version.is_none() {
            // Initialize schema
            Self::init_schema(&conn)?;
            conn.execute(
                "INSERT INTO schema_version (version) VALUES (1)",
                &[] as &[&dyn rusqlite::ToSql],
            )?;
        } else if current_version != Some(SCHEMA_VERSION) {
            return Err(TaskStoreError::InvalidData(format!(
                "schema version mismatch: expected {}, got {}",
                SCHEMA_VERSION,
                current_version.unwrap()
            )));
        }

        Ok(())
    }

    async fn schema_version(&self) -> Result<i64> {
        self.query_row(
            "SELECT version FROM schema_version",
            &[] as &[&dyn rusqlite::ToSql],
            |row| row.get(0),
        )
    }

    async fn task_insert(&self, task: &Task) -> Result<()> {
        let node_tasks_json = serde_json::to_string(&task.node_tasks)?;
        self.execute(
            "INSERT INTO tasks (miroir_id, created_at, status, node_tasks, error)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            &[
                &task.miroir_id as &dyn rusqlite::ToSql,
                &task.created_at,
                &task.status.to_string(),
                &node_tasks_json,
                &task.error.as_deref().unwrap_or(""),
            ] as &[&dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn task_get(&self, miroir_id: &str) -> Result<Option<Task>> {
        let result: Option<Task> = self
            .query_row(
                "SELECT miroir_id, created_at, status, node_tasks, error FROM tasks WHERE miroir_id = ?1",
                &[&miroir_id as &dyn rusqlite::ToSql],
                |row| {
                    let node_tasks_json: String = row.get(3)?;
                    let node_tasks: HashMap<String, u64> = serde_json::from_str(&node_tasks_json).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                    Ok(Task {
                        miroir_id: row.get(0)?,
                        created_at: row.get(1)?,
                        status: row.get::<_, String>(2)?.parse().map_err(|e| {
                            parse_error(e)
                        })?,
                        node_tasks,
                        error: {
                            let error: String = row.get(4)?;
                            if error.is_empty() { None } else { Some(error) }
                        },
                    })
                },
            )
            .ok();
        Ok(result)
    }

    async fn task_update_status(&self, miroir_id: &str, status: TaskStatus) -> Result<()> {
        self.execute(
            "UPDATE tasks SET status = ?1 WHERE miroir_id = ?2",
            &[
                &status.to_string() as &dyn rusqlite::ToSql,
                &miroir_id as &dyn rusqlite::ToSql,
            ],
        )?;
        Ok(())
    }

    async fn task_update_node(&self, miroir_id: &str, node_id: &str, task_uid: u64) -> Result<()> {
        // Get the task, update node_tasks (store only task_uid), and write back
        let mut task = self
            .task_get(miroir_id)
            .await?
            .ok_or_else(|| TaskStoreError::NotFound(miroir_id.to_string()))?;
        task.node_tasks.insert(node_id.to_string(), task_uid);
        let node_tasks_json = serde_json::to_string(&task.node_tasks)?;
        self.execute(
            "UPDATE tasks SET node_tasks = ?1 WHERE miroir_id = ?2",
            &[
                &node_tasks_json as &dyn rusqlite::ToSql,
                &miroir_id as &dyn rusqlite::ToSql,
            ],
        )?;
        Ok(())
    }

    async fn task_list(&self, filter: &TaskFilter) -> Result<Vec<Task>> {
        let mut sql =
            "SELECT miroir_id, created_at, status, node_tasks, error FROM tasks".to_string();
        let mut params = Vec::new();
        let mut wheres = Vec::new();

        if let Some(status) = filter.status {
            wheres.push("status = ?".to_string());
            params.push(status.to_string());
        }

        if !wheres.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&wheres.join(" AND "));
        }

        sql.push_str(" ORDER BY created_at DESC");

        if let Some(limit) = filter.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        if let Some(offset) = filter.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

        self.query_map(&sql, &params_refs, |row| {
            let node_tasks_json: String = row.get(3)?;
            let node_tasks: HashMap<String, u64> = serde_json::from_str(&node_tasks_json)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            Ok(Task {
                miroir_id: row.get(0)?,
                created_at: row.get(1)?,
                status: row.get::<_, String>(2)?.parse().map_err(parse_error)?,
                node_tasks,
                error: {
                    let error: String = row.get(4)?;
                    if error.is_empty() {
                        None
                    } else {
                        Some(error)
                    }
                },
            })
        })
    }

    async fn node_settings_version_get(&self, index: &str, node_id: &str) -> Result<Option<i64>> {
        let version: Option<i64> = self
            .query_row(
                "SELECT version FROM node_settings_version WHERE index_uid = ?1 AND node_id = ?2",
                &[
                    &index as &dyn rusqlite::ToSql,
                    &node_id as &dyn rusqlite::ToSql,
                ],
                |row| row.get(0),
            )
            .ok();
        Ok(version)
    }

    async fn node_settings_version_set(
        &self,
        index: &str,
        node_id: &str,
        version: i64,
    ) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        self.execute(
            "INSERT OR REPLACE INTO node_settings_version (index_uid, node_id, version, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            &[
                &index as &dyn rusqlite::ToSql,
                &node_id as &dyn rusqlite::ToSql,
                &version as &dyn rusqlite::ToSql,
                &now as &dyn rusqlite::ToSql,
            ],
        )?;
        Ok(())
    }

    async fn alias_upsert(&self, alias: &Alias) -> Result<()> {
        let history_json = serde_json::to_string(&alias.history)?;
        self.execute(
            "INSERT OR REPLACE INTO aliases (name, kind, current_uid, target_uids, version, created_at, history)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            &[
                &alias.name as &dyn rusqlite::ToSql,
                &alias.kind.to_string(),
                &alias.current_uid.as_deref().unwrap_or(""),
                &serde_json::to_string(&alias.target_uids)?,
                &alias.version,
                &alias.created_at,
                &history_json,
            ] as &[&dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn alias_get(&self, name: &str) -> Result<Option<Alias>> {
        let result: Option<Alias> = self
            .query_row(
                "SELECT name, kind, current_uid, target_uids, version, created_at, history
                 FROM aliases WHERE name = ?1",
                &[&name as &dyn rusqlite::ToSql],
                |row| {
                    let target_uids_json: String = row.get(3)?;
                    let target_uids: Vec<String> = serde_json::from_str(&target_uids_json)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                    let history_json: String = row.get(6)?;
                    let history: Vec<super::schema::AliasHistoryEntry> =
                        serde_json::from_str(&history_json)
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                    Ok(Alias {
                        name: row.get(0)?,
                        kind: row.get::<_, String>(1)?.parse().map_err(parse_error)?,
                        current_uid: {
                            let uid: String = row.get(2)?;
                            if uid.is_empty() {
                                None
                            } else {
                                Some(uid)
                            }
                        },
                        target_uids: Some(target_uids),
                        version: row.get(4)?,
                        created_at: row.get(5)?,
                        history,
                    })
                },
            )
            .ok();
        Ok(result)
    }

    async fn alias_delete(&self, name: &str) -> Result<()> {
        self.execute(
            "DELETE FROM aliases WHERE name = ?1",
            &[&name as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn alias_list(&self) -> Result<Vec<Alias>> {
        self.query_map(
            "SELECT name, kind, current_uid, target_uids, version, created_at, history FROM aliases",
            &[] as &[&dyn rusqlite::ToSql],
            |row| {
                let target_uids_json: String = row.get(3)?;
                let target_uids: Vec<String> = serde_json::from_str(&target_uids_json).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                let history_json: String = row.get(6)?;
                let history: Vec<super::schema::AliasHistoryEntry> =
                    serde_json::from_str(&history_json)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                Ok(Alias {
                    name: row.get(0)?,
                    kind: row.get::<_, String>(1)?.parse().map_err(|e| {
                        parse_error(e)
                    })?,
                    current_uid: {
                        let uid: String = row.get(2)?;
                        if uid.is_empty() { None } else { Some(uid) }
                    },
                    target_uids: Some(target_uids),
                    version: row.get(4)?,
                    created_at: row.get(5)?,
                    history,
                })
            },
        )
    }

    async fn session_upsert(&self, session: &Session) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO sessions (session_id, last_write_mtask_id, last_write_at, pinned_group, min_settings_version, ttl)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            &[
                &session.session_id as &dyn rusqlite::ToSql,
                &session.last_write_mtask_id.as_deref().unwrap_or(""),
                &session.last_write_at.map(|v| v as i64).unwrap_or(0),
                &session.pinned_group,
                &session.min_settings_version,
                &(session.ttl as i64),
            ] as &[&dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn session_get(&self, session_id: &str) -> Result<Option<Session>> {
        let result: Option<Session> = self
            .query_row(
                "SELECT session_id, last_write_mtask_id, last_write_at, pinned_group, min_settings_version, ttl
                 FROM sessions WHERE session_id = ?1",
                &[&session_id as &dyn rusqlite::ToSql],
                |row| {
                    Ok(Session {
                        session_id: row.get(0)?,
                        last_write_mtask_id: {
                            let id: String = row.get(1)?;
                            if id.is_empty() { None } else { Some(id) }
                        },
                        last_write_at: {
                            let at: i64 = row.get(2)?;
                            if at == 0 { None } else { Some(at as u64) }
                        },
                        pinned_group: row.get(3)?,
                        min_settings_version: row.get(4)?,
                        ttl: row.get(5)?,
                    })
                },
            )
            .ok();
        Ok(result)
    }

    async fn session_delete(&self, session_id: &str) -> Result<()> {
        self.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            &[&session_id as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn session_delete_by_index(&self, _index: &str) -> Result<()> {
        // This method is no longer applicable with the new schema
        // as sessions don't have an 'index' field anymore
        Ok(())
    }

    async fn idempotency_check(&self, key: &str) -> Result<Option<IdempotencyEntry>> {
        let result: Option<IdempotencyEntry> = self
            .query_row(
                "SELECT key, body_sha256, miroir_task_id, expires_at FROM idempotency_cache WHERE key = ?1",
                &[&key as &dyn rusqlite::ToSql],
                |row| Ok(IdempotencyEntry {
                    key: row.get(0)?,
                    body_sha256: row.get(1)?,
                    miroir_task_id: row.get(2)?,
                    expires_at: row.get(3)?,
                }),
            )
            .ok();
        Ok(result)
    }

    async fn idempotency_record(&self, entry: &IdempotencyEntry) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO idempotency_cache (key, body_sha256, miroir_task_id, expires_at)
             VALUES (?1, ?2, ?3, ?4)",
            &[
                &entry.key as &dyn rusqlite::ToSql,
                &entry.body_sha256 as &dyn rusqlite::ToSql,
                &entry.miroir_task_id as &dyn rusqlite::ToSql,
                &entry.expires_at as &dyn rusqlite::ToSql,
            ] as &[&dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn idempotency_prune(&self, before_ts: u64) -> Result<u64> {
        let count = self.execute(
            "DELETE FROM idempotency_cache WHERE expires_at < ?1",
            &[&before_ts as &dyn rusqlite::ToSql],
        )?;
        Ok(count as u64)
    }

    async fn job_enqueue(&self, job: &Job) -> Result<()> {
        self.execute(
            "INSERT INTO jobs (id, type, params, state, claimed_by, claim_expires_at, progress)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            &[
                &job.id as &dyn rusqlite::ToSql,
                &job.job_type,
                &job.params,
                &job.state.to_string(),
                &job.claimed_by.as_deref().unwrap_or(""),
                &job.claim_expires_at.map(|v| v as i64).unwrap_or(0),
                &job.progress,
            ] as &[&dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn job_dequeue(&self, worker_id: &str) -> Result<Option<Job>> {
        // Start a transaction
        let conn = self
            .conn
            .lock()
            .map_err(|e| TaskStoreError::Internal(e.to_string()))?;
        let tx = conn.unchecked_transaction()?;

        let now = chrono::Utc::now().timestamp_millis() as u64;
        let expires_at = now + (5 * 60 * 1000); // 5 minutes from now

        // Find and claim a job
        let job_id: Option<String> = tx
            .query_row(
                "SELECT id FROM jobs WHERE state = 'queued' ORDER BY id ASC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .ok();

        if let Some(ref job_id) = job_id {
            tx.execute(
                "UPDATE jobs SET state = 'in_progress', claimed_by = ?1, claim_expires_at = ?2 WHERE id = ?3",
                [
                    &worker_id as &dyn rusqlite::ToSql,
                    &expires_at as &dyn rusqlite::ToSql,
                    job_id as &dyn rusqlite::ToSql,
                ],
            )?;

            // Fetch the updated job
            let job: Job = tx.query_row(
                "SELECT id, type, params, state, claimed_by, claim_expires_at, progress
                 FROM jobs WHERE id = ?1",
                [job_id as &dyn rusqlite::ToSql],
                |row| {
                    Ok(Job {
                        id: row.get(0)?,
                        job_type: row.get(1)?,
                        params: row.get(2)?,
                        state: row.get::<_, String>(3)?.parse().map_err(parse_error)?,
                        claimed_by: {
                            let claimed: String = row.get(4)?;
                            if claimed.is_empty() {
                                None
                            } else {
                                Some(claimed)
                            }
                        },
                        claim_expires_at: {
                            let expires: i64 = row.get(5)?;
                            if expires == 0 {
                                None
                            } else {
                                Some(expires as u64)
                            }
                        },
                        progress: row.get(6)?,
                    })
                },
            )?;

            tx.commit()?;
            Ok(Some(job))
        } else {
            tx.commit()?;
            Ok(None)
        }
    }

    async fn job_update_status(
        &self,
        job_id: &str,
        status: JobState,
        result: Option<&str>,
    ) -> Result<()> {
        self.execute(
            "UPDATE jobs SET state = ?1, progress = ?2 WHERE id = ?3",
            &[
                &status.to_string(),
                &result.unwrap_or("").to_string(),
                &job_id as &dyn rusqlite::ToSql,
            ],
        )?;
        Ok(())
    }

    async fn job_get(&self, job_id: &str) -> Result<Option<Job>> {
        let result: Option<Job> = self
            .query_row(
                "SELECT id, type, params, state, claimed_by, claim_expires_at, progress
                 FROM jobs WHERE id = ?1",
                &[&job_id as &dyn rusqlite::ToSql],
                |row| {
                    Ok(Job {
                        id: row.get(0)?,
                        job_type: row.get(1)?,
                        params: row.get(2)?,
                        state: row.get::<_, String>(3)?.parse().map_err(parse_error)?,
                        claimed_by: {
                            let claimed: String = row.get(4)?;
                            if claimed.is_empty() {
                                None
                            } else {
                                Some(claimed)
                            }
                        },
                        claim_expires_at: {
                            let expires: i64 = row.get(5)?;
                            if expires == 0 {
                                None
                            } else {
                                Some(expires as u64)
                            }
                        },
                        progress: row.get(6)?,
                    })
                },
            )
            .ok();
        Ok(result)
    }

    async fn job_list(&self, status: Option<JobState>, limit: usize) -> Result<Vec<Job>> {
        let mut sql =
            "SELECT id, type, params, state, claimed_by, claim_expires_at, progress FROM jobs"
                .to_string();

        if status.is_some() {
            sql.push_str(" WHERE state = ?");
        }

        sql.push_str(&format!(" ORDER BY id DESC LIMIT {limit}"));

        let status_str: Option<String> = status.map(|s| s.to_string());
        let params: Vec<&dyn rusqlite::ToSql> = match &status_str {
            Some(s) => vec![s],
            None => vec![],
        };

        self.query_map(&sql, &params, |row| {
            Ok(Job {
                id: row.get(0)?,
                job_type: row.get(1)?,
                params: row.get(2)?,
                state: row.get::<_, String>(3)?.parse().map_err(parse_error)?,
                claimed_by: {
                    let claimed: String = row.get(4)?;
                    if claimed.is_empty() {
                        None
                    } else {
                        Some(claimed)
                    }
                },
                claim_expires_at: {
                    let expires: i64 = row.get(5)?;
                    if expires == 0 {
                        None
                    } else {
                        Some(expires as u64)
                    }
                },
                progress: row.get(6)?,
            })
        })
    }

    async fn leader_lease_acquire(&self, lease: &LeaderLease) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| TaskStoreError::Internal(e.to_string()))?;
        let tx = conn.unchecked_transaction()?;

        // Check if there's an existing valid lease
        let existing: Option<(String, u64)> = tx
            .query_row(
                "SELECT scope, expires_at FROM leader_lease WHERE expires_at > ?1",
                [&(chrono::Utc::now().timestamp_millis() as u64) as &dyn rusqlite::ToSql],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        let acquired = if existing.is_some() {
            false
        } else {
            tx.execute(
                "INSERT OR REPLACE INTO leader_lease (scope, holder, expires_at)
                 VALUES (?1, ?2, ?3)",
                [
                    &lease.scope as &dyn rusqlite::ToSql,
                    &lease.holder,
                    &lease.expires_at,
                ],
            )?;
            true
        };

        tx.commit()?;
        Ok(acquired)
    }

    async fn leader_lease_release(&self, scope: &str) -> Result<()> {
        self.execute(
            "DELETE FROM leader_lease WHERE scope = ?1",
            &[&scope as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn leader_lease_get(&self) -> Result<Option<LeaderLease>> {
        let result: Option<LeaderLease> = self
            .query_row(
                "SELECT scope, holder, expires_at FROM leader_lease LIMIT 1",
                &[] as &[&dyn rusqlite::ToSql],
                |row| {
                    Ok(LeaderLease {
                        scope: row.get(0)?,
                        holder: row.get(1)?,
                        expires_at: row.get(2)?,
                    })
                },
            )
            .ok();
        Ok(result)
    }

    async fn canary_upsert(&self, canary: &Canary) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO canaries (id, name, index_uid, interval_s, query_json, assertions_json, enabled, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            &[
                &canary.id as &dyn rusqlite::ToSql,
                &canary.name,
                &canary.index_uid,
                &canary.interval_s,
                &canary.query_json,
                &canary.assertions_json,
                &canary.enabled,
                &canary.created_at,
            ],
        )?;
        Ok(())
    }

    async fn canary_get(&self, name: &str) -> Result<Option<Canary>> {
        let result: Option<Canary> = self
            .query_row(
                "SELECT id, name, index_uid, interval_s, query_json, assertions_json, enabled, created_at
                 FROM canaries WHERE name = ?1",
                &[&name as &dyn rusqlite::ToSql],
                |row| Ok(Canary {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    index_uid: row.get(2)?,
                    interval_s: row.get(3)?,
                    query_json: row.get(4)?,
                    assertions_json: row.get(5)?,
                    enabled: row.get(6)?,
                    created_at: row.get(7)?,
                }),
            )
            .ok();
        Ok(result)
    }

    async fn canary_delete(&self, name: &str) -> Result<()> {
        self.execute(
            "DELETE FROM canaries WHERE name = ?1",
            &[&name as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn canary_list(&self) -> Result<Vec<Canary>> {
        self.query_map(
            "SELECT id, name, index_uid, interval_s, query_json, assertions_json, enabled, created_at FROM canaries",
            &[] as &[&dyn rusqlite::ToSql],
            |row| Ok(Canary {
                id: row.get(0)?,
                name: row.get(1)?,
                index_uid: row.get(2)?,
                interval_s: row.get(3)?,
                query_json: row.get(4)?,
                assertions_json: row.get(5)?,
                enabled: row.get(6)?,
                created_at: row.get(7)?,
            }),
        )
    }

    async fn canary_run_insert(&self, run: &CanaryRun) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO canary_runs (canary_id, ran_at, status, latency_ms, failed_assertions_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            &[
                &run.canary_id as &dyn rusqlite::ToSql,
                &run.ran_at,
                &run.status.to_string(),
                &run.latency_ms,
                &run.failed_assertions_json.as_deref().unwrap_or(""),
            ],
        )?;
        Ok(())
    }

    async fn canary_run_list(&self, canary_name: &str, limit: usize) -> Result<Vec<CanaryRun>> {
        self.query_map(
            &format!(
                "SELECT canary_id, ran_at, status, latency_ms, failed_assertions_json
                 FROM canary_runs WHERE canary_id = ?1 ORDER BY ran_at DESC LIMIT {limit}"
            ),
            &[&canary_name as &dyn rusqlite::ToSql],
            |row| {
                Ok(CanaryRun {
                    canary_id: row.get(0)?,
                    ran_at: row.get(1)?,
                    status: row.get::<_, String>(2)?.parse().map_err(parse_error)?,
                    latency_ms: row.get(3)?,
                    failed_assertions_json: {
                        let json: String = row.get(4)?;
                        if json.is_empty() {
                            None
                        } else {
                            Some(json)
                        }
                    },
                })
            },
        )
    }

    async fn canary_run_prune(&self, before_ts: u64) -> Result<u64> {
        let count = self.execute(
            "DELETE FROM canary_runs WHERE ran_at < ?1",
            &[&before_ts as &dyn rusqlite::ToSql],
        )?;
        Ok(count as u64)
    }

    async fn cdc_cursor_get(&self, sink: &str, index: &str) -> Result<Option<CdcCursor>> {
        let result: Option<CdcCursor> = self
            .query_row(
                "SELECT sink, [index], cursor, updated_at FROM cdc_cursors WHERE sink = ?1 AND [index] = ?2",
                &[&sink as &dyn rusqlite::ToSql, &index as &dyn rusqlite::ToSql],
                |row| Ok(CdcCursor {
                    sink_name: row.get(0)?,
                    index_uid: row.get(1)?,
                    last_event_seq: row.get(2)?,
                    updated_at: row.get(3)?,
                }),
            )
            .ok();
        Ok(result)
    }

    async fn cdc_cursor_set(&self, cursor: &CdcCursor) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO cdc_cursors (sink, [index], cursor, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            &[
                &cursor.sink_name as &dyn rusqlite::ToSql,
                &cursor.index_uid,
                &cursor.last_event_seq,
                &cursor.updated_at,
            ],
        )?;
        Ok(())
    }

    async fn cdc_cursor_list(&self, sink: &str) -> Result<Vec<CdcCursor>> {
        self.query_map(
            "SELECT sink, [index], cursor, updated_at FROM cdc_cursors WHERE sink = ?1",
            &[&sink as &dyn rusqlite::ToSql],
            |row| {
                Ok(CdcCursor {
                    sink_name: row.get(0)?,
                    index_uid: row.get(1)?,
                    last_event_seq: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            },
        )
    }

    async fn tenant_upsert(&self, tenant: &Tenant) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO tenant_map (api_key_hash, tenant_id, group_id)
             VALUES (?1, ?2, ?3)",
            &[
                &tenant.api_key_hash as &dyn rusqlite::ToSql,
                &tenant.tenant_id,
                &tenant.group_id,
            ],
        )?;
        Ok(())
    }

    async fn tenant_get(&self, api_key: &str) -> Result<Option<Tenant>> {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(api_key.as_bytes());
        let api_key_hash: Vec<u8> = hasher.finalize().to_vec();

        let result: Option<Tenant> = self
            .query_row(
                "SELECT api_key_hash, tenant_id, group_id
                 FROM tenant_map WHERE api_key_hash = ?1",
                &[&api_key_hash as &dyn rusqlite::ToSql],
                |row| {
                    Ok(Tenant {
                        api_key_hash: row.get(0)?,
                        tenant_id: row.get(1)?,
                        group_id: row.get(2)?,
                    })
                },
            )
            .ok();
        Ok(result)
    }

    async fn tenant_delete(&self, api_key: &str) -> Result<()> {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(api_key.as_bytes());
        let api_key_hash: Vec<u8> = hasher.finalize().to_vec();

        self.execute(
            "DELETE FROM tenant_map WHERE api_key_hash = ?1",
            &[&api_key_hash as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn tenant_list(&self) -> Result<Vec<Tenant>> {
        self.query_map(
            "SELECT api_key_hash, tenant_id, group_id FROM tenant_map",
            &[] as &[&dyn rusqlite::ToSql],
            |row| {
                Ok(Tenant {
                    api_key_hash: row.get(0)?,
                    tenant_id: row.get(1)?,
                    group_id: row.get(2)?,
                })
            },
        )
    }

    async fn rollover_policy_upsert(&self, policy: &RolloverPolicy) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO rollover_policies
             (name, write_alias, read_alias, pattern, triggers_json, retention_json, template_json, enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            &[
                &policy.name as &dyn rusqlite::ToSql,
                &policy.write_alias,
                &policy.read_alias,
                &policy.pattern,
                &policy.triggers_json,
                &policy.retention_json,
                &policy.template_json,
                &policy.enabled,
            ],
        )?;
        Ok(())
    }

    async fn rollover_policy_get(&self, name: &str) -> Result<Option<RolloverPolicy>> {
        let result: Option<RolloverPolicy> = self
            .query_row(
                "SELECT name, write_alias, read_alias, pattern, triggers_json, retention_json, template_json, enabled
                 FROM rollover_policies WHERE name = ?1",
                &[&name as &dyn rusqlite::ToSql],
                |row| {
                    Ok(RolloverPolicy {
                        name: row.get(0)?,
                        write_alias: row.get(1)?,
                        read_alias: row.get(2)?,
                        pattern: row.get(3)?,
                        triggers_json: row.get(4)?,
                        retention_json: row.get(5)?,
                        template_json: row.get(6)?,
                        enabled: row.get(7)?,
                    })
                },
            )
            .ok();
        Ok(result)
    }

    async fn rollover_policy_delete(&self, name: &str) -> Result<()> {
        self.execute(
            "DELETE FROM rollover_policies WHERE name = ?1",
            &[&name as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn rollover_policy_list(&self) -> Result<Vec<RolloverPolicy>> {
        self.query_map(
            "SELECT name, write_alias, read_alias, pattern, triggers_json, retention_json, template_json, enabled
             FROM rollover_policies",
            &[] as &[&dyn rusqlite::ToSql],
            |row| {
                Ok(RolloverPolicy {
                    name: row.get(0)?,
                    write_alias: row.get(1)?,
                    read_alias: row.get(2)?,
                    pattern: row.get(3)?,
                    triggers_json: row.get(4)?,
                    retention_json: row.get(5)?,
                    template_json: row.get(6)?,
                    enabled: row.get(7)?,
                })
            },
        )
    }

    async fn search_ui_config_upsert(&self, config: &SearchUiConfig) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO search_ui_config (index_uid, config_json, updated_at)
             VALUES (?1, ?2, ?3)",
            &[
                &config.index_uid as &dyn rusqlite::ToSql,
                &config.config_json,
                &config.updated_at,
            ],
        )?;
        Ok(())
    }

    async fn search_ui_config_get(&self, index_uid: &str) -> Result<Option<SearchUiConfig>> {
        let result: Option<SearchUiConfig> = self
            .query_row(
                "SELECT index_uid, config_json, updated_at FROM search_ui_config WHERE index_uid = ?1",
                &[&index_uid as &dyn rusqlite::ToSql],
                |row| Ok(SearchUiConfig {
                    index_uid: row.get(0)?,
                    config_json: row.get(1)?,
                    updated_at: row.get(2)?,
                }),
            )
            .ok();
        Ok(result)
    }

    async fn search_ui_config_delete(&self, index_uid: &str) -> Result<()> {
        self.execute(
            "DELETE FROM search_ui_config WHERE index_uid = ?1",
            &[&index_uid as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn search_ui_config_list(&self) -> Result<Vec<SearchUiConfig>> {
        self.query_map(
            "SELECT index_uid, config_json, updated_at FROM search_ui_config",
            &[] as &[&dyn rusqlite::ToSql],
            |row| {
                Ok(SearchUiConfig {
                    index_uid: row.get(0)?,
                    config_json: row.get(1)?,
                    updated_at: row.get(2)?,
                })
            },
        )
    }

    async fn admin_session_upsert(&self, session: &AdminSession) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO admin_sessions (session_id, csrf_token, admin_key_hash, created_at, expires_at, revoked, user_agent, source_ip)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            &[
                &session.session_id as &dyn rusqlite::ToSql,
                &session.csrf_token,
                &session.admin_key_hash,
                &session.created_at,
                &session.expires_at,
                &session.revoked,
                &session.user_agent.as_deref(),
                &session.source_ip.as_deref(),
            ],
        )?;
        Ok(())
    }

    async fn admin_session_get(&self, session_id: &str) -> Result<Option<AdminSession>> {
        let result: Option<AdminSession> = self
            .query_row(
                "SELECT session_id, csrf_token, admin_key_hash, created_at, expires_at, revoked, user_agent, source_ip FROM admin_sessions WHERE session_id = ?1",
                &[&session_id as &dyn rusqlite::ToSql],
                |row| Ok(AdminSession {
                    session_id: row.get(0)?,
                    csrf_token: row.get(1)?,
                    admin_key_hash: row.get(2)?,
                    created_at: row.get(3)?,
                    expires_at: row.get(4)?,
                    revoked: row.get(5)?,
                    user_agent: row.get(6)?,
                    source_ip: row.get(7)?,
                }),
            )
            .ok();
        Ok(result)
    }

    async fn admin_session_delete(&self, session_id: &str) -> Result<()> {
        self.execute(
            "DELETE FROM admin_sessions WHERE session_id = ?1",
            &[&session_id as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn admin_session_revoke(&self, session_id: &str) -> Result<()> {
        self.execute(
            "UPDATE admin_sessions SET revoked = 1 WHERE session_id = ?1",
            &[&session_id as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn admin_session_is_revoked(&self, session_id: &str) -> Result<bool> {
        let revoked: Option<bool> = self
            .query_row(
                "SELECT revoked FROM admin_sessions WHERE session_id = ?1",
                &[&session_id as &dyn rusqlite::ToSql],
                |row| row.get(0),
            )
            .ok();
        Ok(revoked.unwrap_or(false))
    }

    // Redis-only operations (not supported in SQLite mode)
    async fn ratelimit_increment(
        &self,
        _key: &str,
        _window_s: u64,
        _limit: u64,
    ) -> Result<(u64, u64)> {
        Err(TaskStoreError::InvalidData(
            "rate limiting requires Redis backend".to_string(),
        ))
    }

    async fn ratelimit_set_backoff(&self, _key: &str, _duration_s: u64) -> Result<()> {
        Err(TaskStoreError::InvalidData(
            "rate limiting requires Redis backend".to_string(),
        ))
    }

    async fn ratelimit_check_backoff(&self, _key: &str) -> Result<Option<u64>> {
        Err(TaskStoreError::InvalidData(
            "rate limiting requires Redis backend".to_string(),
        ))
    }

    async fn cdc_overflow_check(&self, _sink: &str) -> Result<bool> {
        Err(TaskStoreError::InvalidData(
            "CDC overflow requires Redis backend".to_string(),
        ))
    }

    async fn cdc_overflow_size(&self, _sink: &str) -> Result<u64> {
        Err(TaskStoreError::InvalidData(
            "CDC overflow requires Redis backend".to_string(),
        ))
    }

    async fn cdc_overflow_append(&self, _sink: &str, _data: &[u8]) -> Result<()> {
        Err(TaskStoreError::InvalidData(
            "CDC overflow requires Redis backend".to_string(),
        ))
    }

    async fn cdc_overflow_clear(&self, _sink: &str) -> Result<()> {
        Err(TaskStoreError::InvalidData(
            "CDC overflow requires Redis backend".to_string(),
        ))
    }

    async fn scoped_key_set(&self, _index: &str, _key: &str, _expires_at: u64) -> Result<()> {
        Err(TaskStoreError::InvalidData(
            "scoped key rotation requires Redis backend".to_string(),
        ))
    }

    async fn scoped_key_get(&self, _index: &str) -> Result<Option<String>> {
        Err(TaskStoreError::InvalidData(
            "scoped key rotation requires Redis backend".to_string(),
        ))
    }

    async fn scoped_key_observe(&self, _pod: &str, _index: &str, _key: &str) -> Result<()> {
        Err(TaskStoreError::InvalidData(
            "scoped key rotation requires Redis backend".to_string(),
        ))
    }

    async fn scoped_key_has_observed(&self, _pod: &str, _index: &str, _key: &str) -> Result<bool> {
        Err(TaskStoreError::InvalidData(
            "scoped key rotation requires Redis backend".to_string(),
        ))
    }

    async fn health_check(&self) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| TaskStoreError::Internal(e.to_string()))?;
        // Execute a simple query to check if the database is responsive
        let _: Option<i64> = conn.query_row("SELECT 1", [], |row| row.get(0)).ok();
        Ok(true)
    }
}

impl SqliteTaskStore {
    /// Initialize the database schema (plan §4 tables 1-7).
    fn init_schema(conn: &Connection) -> Result<()> {
        // Table 1: Tasks
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tasks (
                miroir_id   TEXT PRIMARY KEY,
                created_at  INTEGER NOT NULL,
                status      TEXT NOT NULL,
                node_tasks  TEXT NOT NULL,
                error       TEXT
            )",
            [],
        )?;

        // Table 2: Node settings version
        conn.execute(
            "CREATE TABLE IF NOT EXISTS node_settings_version (
                index_uid   TEXT NOT NULL,
                node_id     TEXT NOT NULL,
                version     INTEGER NOT NULL,
                updated_at  INTEGER NOT NULL,
                PRIMARY KEY (index_uid, node_id)
            )",
            [],
        )?;

        // Table 3: Aliases
        conn.execute(
            "CREATE TABLE IF NOT EXISTS aliases (
                name          TEXT PRIMARY KEY,
                kind          TEXT NOT NULL,
                current_uid   TEXT,
                target_uids   TEXT,
                version       INTEGER NOT NULL,
                created_at    INTEGER NOT NULL,
                history       TEXT NOT NULL
            )",
            [],
        )?;

        // Table 4: Sessions
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions (
                session_id            TEXT PRIMARY KEY,
                last_write_mtask_id   TEXT,
                last_write_at         INTEGER,
                pinned_group          INTEGER,
                min_settings_version  INTEGER NOT NULL,
                ttl                   INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 5: Idempotency cache
        conn.execute(
            "CREATE TABLE IF NOT EXISTS idempotency_cache (
                key              TEXT PRIMARY KEY,
                body_sha256      BLOB NOT NULL,
                miroir_task_id   TEXT NOT NULL,
                expires_at       INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 6: Jobs
        conn.execute(
            "CREATE TABLE IF NOT EXISTS jobs (
                id                 TEXT PRIMARY KEY,
                type               TEXT NOT NULL,
                params             TEXT NOT NULL,
                state              TEXT NOT NULL,
                claimed_by         TEXT,
                claim_expires_at   INTEGER,
                progress           TEXT NOT NULL
            )",
            [],
        )?;

        // Table 7: Leader lease
        conn.execute(
            "CREATE TABLE IF NOT EXISTS leader_lease (
                scope        TEXT PRIMARY KEY,
                holder       TEXT NOT NULL,
                expires_at   INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 8: Canaries
        conn.execute(
            "CREATE TABLE IF NOT EXISTS canaries (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                index_uid TEXT NOT NULL,
                interval_s INTEGER NOT NULL,
                query_json TEXT NOT NULL,
                assertions_json TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 9: Canary runs
        conn.execute(
            "CREATE TABLE IF NOT EXISTS canary_runs (
                canary_id TEXT NOT NULL,
                ran_at INTEGER NOT NULL,
                status TEXT NOT NULL,
                latency_ms INTEGER NOT NULL,
                failed_assertions_json TEXT,
                PRIMARY KEY (canary_id, ran_at)
            )",
            [],
        )?;

        // Table 10: CDC cursors
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cdc_cursors (
                sink_name TEXT NOT NULL,
                index_uid TEXT NOT NULL,
                last_event_seq INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (sink_name, index_uid)
            )",
            [],
        )?;

        // Table 11: Tenant map
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tenant_map (
                api_key_hash BLOB PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                group_id INTEGER
            )",
            [],
        )?;

        // Table 12: Rollover policies
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rollover_policies (
                name TEXT PRIMARY KEY,
                write_alias TEXT NOT NULL,
                read_alias TEXT NOT NULL,
                pattern TEXT NOT NULL,
                triggers_json TEXT NOT NULL,
                retention_json TEXT NOT NULL,
                template_json TEXT NOT NULL,
                enabled INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 13: Search UI config
        conn.execute(
            "CREATE TABLE IF NOT EXISTS search_ui_config (
                index_uid TEXT PRIMARY KEY,
                config_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 14: Admin sessions
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_sessions (
                session_id TEXT PRIMARY KEY,
                csrf_token TEXT NOT NULL,
                admin_key_hash TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0,
                user_agent TEXT,
                source_ip TEXT
            )",
            [],
        )?;

        Ok(())
    }
}

// Note: Display and FromStr for TaskStatus, AliasKind, JobState, and CanaryRunStatus
// are defined in schema.rs to avoid duplicate implementations

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enqueued => write!(f, "Enqueued"),
            Self::Processing => write!(f, "Processing"),
            Self::Succeeded => write!(f, "Succeeded"),
            Self::Failed => write!(f, "Failed"),
            Self::Canceled => write!(f, "Canceled"),
        }
    }
}

impl std::str::FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Enqueued" => Ok(Self::Enqueued),
            "Processing" => Ok(Self::Processing),
            "Succeeded" => Ok(Self::Succeeded),
            "Failed" => Ok(Self::Failed),
            "Canceled" => Ok(Self::Canceled),
            _ => Err(format!("invalid job status: {s}")),
        }
    }
}
