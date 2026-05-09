//! SQLite backend for the task store.

use super::error::{Result, TaskStoreError};
use super::schema::*;
use super::TaskStore;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

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
        let _mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", &[] as &[&dyn rusqlite::ToSql], |row| {
                row.get(0)
            })?;

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
                    let node_tasks: HashMap<String, NodeTask> = serde_json::from_str(&node_tasks_json).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
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

    async fn task_update_node(
        &self,
        miroir_id: &str,
        node_id: &str,
        node_task: &NodeTask,
    ) -> Result<()> {
        // Get the task, update node_tasks, and write back
        let mut task = self
            .task_get(miroir_id)
            .await?
            .ok_or_else(|| TaskStoreError::NotFound(miroir_id.to_string()))?;
        task.node_tasks
            .insert(node_id.to_string(), node_task.clone());
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
            let node_tasks: HashMap<String, NodeTask> = serde_json::from_str(&node_tasks_json)
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
                "SELECT version FROM node_settings_version WHERE index = ?1 AND node_id = ?2",
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
            "INSERT OR REPLACE INTO node_settings_version (index, node_id, version, updated_at)
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
        self.execute(
            "INSERT OR REPLACE INTO aliases (name, kind, current_uid, target_uids, version, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            &[
                &alias.name as &dyn rusqlite::ToSql,
                &alias.kind.to_string(),
                &alias.current_uid.as_deref().unwrap_or(""),
                &serde_json::to_string(&alias.target_uids)?,
                &alias.version,
                &alias.created_at,
                &alias.updated_at,
            ] as &[&dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn alias_get(&self, name: &str) -> Result<Option<Alias>> {
        let result: Option<Alias> = self
            .query_row(
                "SELECT name, kind, current_uid, target_uids, version, created_at, updated_at
                 FROM aliases WHERE name = ?1",
                &[&name as &dyn rusqlite::ToSql],
                |row| {
                    let target_uids_json: String = row.get(3)?;
                    let target_uids: Vec<String> = serde_json::from_str(&target_uids_json)
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
                        target_uids,
                        version: row.get(4)?,
                        created_at: row.get(5)?,
                        updated_at: row.get(6)?,
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
            "SELECT name, kind, current_uid, target_uids, version, created_at, updated_at FROM aliases",
            &[] as &[&dyn rusqlite::ToSql],
            |row| {
                let target_uids_json: String = row.get(3)?;
                let target_uids: Vec<String> = serde_json::from_str(&target_uids_json).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                Ok(Alias {
                    name: row.get(0)?,
                    kind: row.get::<_, String>(1)?.parse().map_err(|e| {
                        parse_error(e)
                    })?,
                    current_uid: {
                        let uid: String = row.get(2)?;
                        if uid.is_empty() { None } else { Some(uid) }
                    },
                    target_uids,
                    version: row.get(4)?,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            },
        )
    }

    async fn session_upsert(&self, session: &Session) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO sessions (session_id, index, settings_version, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            &[
                &session.session_id as &dyn rusqlite::ToSql,
                &session.index,
                &session.settings_version,
                &session.created_at,
                &session.expires_at,
            ] as &[&dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn session_get(&self, session_id: &str) -> Result<Option<Session>> {
        let result: Option<Session> = self
            .query_row(
                "SELECT session_id, index, settings_version, created_at, expires_at
                 FROM sessions WHERE session_id = ?1",
                &[&session_id as &dyn rusqlite::ToSql],
                |row| {
                    Ok(Session {
                        session_id: row.get(0)?,
                        index: row.get(1)?,
                        settings_version: row.get(2)?,
                        created_at: row.get(3)?,
                        expires_at: row.get(4)?,
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

    async fn session_delete_by_index(&self, index: &str) -> Result<()> {
        self.execute(
            "DELETE FROM sessions WHERE index = ?1",
            &[&index as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn idempotency_check(&self, key: &str) -> Result<Option<IdempotencyEntry>> {
        let result: Option<IdempotencyEntry> = self
            .query_row(
                "SELECT key, response, status_code, created_at FROM idempotency_cache WHERE key = ?1",
                &[&key as &dyn rusqlite::ToSql],
                |row| Ok(IdempotencyEntry {
                    key: row.get(0)?,
                    response: row.get(1)?,
                    status_code: row.get(2)?,
                    created_at: row.get(3)?,
                }),
            )
            .ok();
        Ok(result)
    }

    async fn idempotency_record(&self, entry: &IdempotencyEntry) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO idempotency_cache (key, response, status_code, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            &[
                &entry.key as &dyn rusqlite::ToSql,
                &entry.response,
                &entry.status_code,
                &entry.created_at,
            ] as &[&dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn idempotency_prune(&self, before_ts: u64) -> Result<u64> {
        let count = self.execute(
            "DELETE FROM idempotency_cache WHERE created_at < ?1",
            &[&before_ts as &dyn rusqlite::ToSql],
        )?;
        Ok(count as u64)
    }

    async fn job_enqueue(&self, job: &Job) -> Result<()> {
        self.execute(
            "INSERT INTO jobs (job_id, job_type, parameters, status, worker_id, result, error, created_at, started_at, completed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            &[
                &job.job_id as &dyn rusqlite::ToSql,
                &job.job_type,
                &job.parameters,
                &job.status.to_string(),
                &job.worker_id.as_deref().unwrap_or(""),
                &job.result.as_deref().unwrap_or(""),
                &job.error.as_deref().unwrap_or(""),
                &job.created_at,
                &job.started_at.map(|v| v as i64).unwrap_or(0),
                &job.completed_at.map(|v| v as i64).unwrap_or(0),
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

        // Find and claim a job
        let job: Option<Job> = tx
            .query_row(
                "SELECT job_id, job_type, parameters, status, worker_id, result, error, created_at, started_at, completed_at
                 FROM jobs WHERE status = 'Enqueued' ORDER BY created_at ASC LIMIT 1",
                [],
                |row| {
                    Ok(Job {
                        job_id: row.get(0)?,
                        job_type: row.get(1)?,
                        parameters: row.get(2)?,
                        status: JobStatus::Enqueued,
                        worker_id: row.get(4)?,
                        result: row.get(5)?,
                        error: row.get(6)?,
                        created_at: row.get(7)?,
                        started_at: row.get(8)?,
                        completed_at: row.get(9)?,
                    })
                },
            )
            .ok();

        if let Some(ref job) = job {
            tx.execute(
                "UPDATE jobs SET status = 'Processing', worker_id = ?1, started_at = ?2 WHERE job_id = ?3",
                [
                    &worker_id as &dyn rusqlite::ToSql,
                    &(chrono::Utc::now().timestamp_millis() as u64) as &dyn rusqlite::ToSql,
                    &job.job_id as &dyn rusqlite::ToSql,
                ],
            )?;
        }

        tx.commit()?;

        Ok(job)
    }

    async fn job_update_status(
        &self,
        job_id: &str,
        status: JobStatus,
        result: Option<&str>,
    ) -> Result<()> {
        let completed_at = if matches!(
            status,
            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Canceled
        ) {
            Some(chrono::Utc::now().timestamp_millis() as u64)
        } else {
            None
        };

        self.execute(
            "UPDATE jobs SET status = ?1, result = ?2, completed_at = ?3 WHERE job_id = ?4",
            &[
                &status.to_string(),
                &result.unwrap_or("").to_string(),
                &completed_at.map(|v| v as i64).unwrap_or(0),
                &job_id as &dyn rusqlite::ToSql,
            ],
        )?;
        Ok(())
    }

    async fn job_get(&self, job_id: &str) -> Result<Option<Job>> {
        let result: Option<Job> = self
            .query_row(
                "SELECT job_id, job_type, parameters, status, worker_id, result, error, created_at, started_at, completed_at
                 FROM jobs WHERE job_id = ?1",
                &[
                    &job_id as &dyn rusqlite::ToSql
                ],
                |row| Ok(Job {
                    job_id: row.get(0)?,
                    job_type: row.get(1)?,
                    parameters: row.get(2)?,
                    status: row.get::<_, String>(3)?.parse().map_err(|e| {
                        parse_error(e)
                    })?,
                    worker_id: {
                        let worker: String = row.get(4)?;
                        if worker.is_empty() { None } else { Some(worker) }
                    },
                    result: {
                        let result: String = row.get(5)?;
                        if result.is_empty() { None } else { Some(result) }
                    },
                    error: {
                        let error: String = row.get(6)?;
                        if error.is_empty() { None } else { Some(error) }
                    },
                    created_at: row.get(7)?,
                    started_at: {
                        let started: i64 = row.get(8)?;
                        if started == 0 { None } else { Some(started as u64) }
                    },
                    completed_at: {
                        let completed: i64 = row.get(9)?;
                        if completed == 0 { None } else { Some(completed as u64) }
                    },
                }),
            )
            .ok();
        Ok(result)
    }

    async fn job_list(&self, status: Option<JobStatus>, limit: usize) -> Result<Vec<Job>> {
        let mut sql = "SELECT job_id, job_type, parameters, status, worker_id, result, error, created_at, started_at, completed_at FROM jobs".to_string();

        if status.is_some() {
            sql.push_str(" WHERE status = ?");
        }

        sql.push_str(&format!(" ORDER BY created_at DESC LIMIT {limit}"));

        let status_str: Option<String> = status.map(|s| s.to_string());
        let params: Vec<&dyn rusqlite::ToSql> = match &status_str {
            Some(s) => vec![s],
            None => vec![],
        };

        self.query_map(&sql, &params, |row| {
            Ok(Job {
                job_id: row.get(0)?,
                job_type: row.get(1)?,
                parameters: row.get(2)?,
                status: row.get::<_, String>(3)?.parse().map_err(parse_error)?,
                worker_id: {
                    let worker: String = row.get(4)?;
                    if worker.is_empty() {
                        None
                    } else {
                        Some(worker)
                    }
                },
                result: {
                    let result: String = row.get(5)?;
                    if result.is_empty() {
                        None
                    } else {
                        Some(result)
                    }
                },
                error: {
                    let error: String = row.get(6)?;
                    if error.is_empty() {
                        None
                    } else {
                        Some(error)
                    }
                },
                created_at: row.get(7)?,
                started_at: {
                    let started: i64 = row.get(8)?;
                    if started == 0 {
                        None
                    } else {
                        Some(started as u64)
                    }
                },
                completed_at: {
                    let completed: i64 = row.get(9)?;
                    if completed == 0 {
                        None
                    } else {
                        Some(completed as u64)
                    }
                },
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
                "SELECT lease_id, expires_at FROM leader_lease WHERE expires_at > ?1",
                [&(chrono::Utc::now().timestamp_millis() as u64) as &dyn rusqlite::ToSql],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        let acquired = if existing.is_some() {
            false
        } else {
            tx.execute(
                "INSERT OR REPLACE INTO leader_lease (lease_id, holder, acquired_at, expires_at)
                 VALUES (?1, ?2, ?3, ?4)",
                [
                    &lease.lease_id as &dyn rusqlite::ToSql,
                    &lease.holder,
                    &lease.acquired_at,
                    &lease.expires_at,
                ],
            )?;
            true
        };

        tx.commit()?;
        Ok(acquired)
    }

    async fn leader_lease_release(&self, lease_id: &str) -> Result<()> {
        self.execute(
            "DELETE FROM leader_lease WHERE lease_id = ?1",
            &[&lease_id as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn leader_lease_get(&self) -> Result<Option<LeaderLease>> {
        let result: Option<LeaderLease> = self
            .query_row(
                "SELECT lease_id, holder, acquired_at, expires_at FROM leader_lease LIMIT 1",
                &[] as &[&dyn rusqlite::ToSql],
                |row| {
                    Ok(LeaderLease {
                        lease_id: row.get(0)?,
                        holder: row.get(1)?,
                        acquired_at: row.get(2)?,
                        expires_at: row.get(3)?,
                    })
                },
            )
            .ok();
        Ok(result)
    }

    async fn canary_upsert(&self, canary: &Canary) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO canaries (name, index, query, min_results, max_results, interval_s, enabled, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            &[
                &canary.name as &dyn rusqlite::ToSql,
                &canary.index,
                &canary.query,
                &canary.min_results,
                &canary.max_results,
                &canary.interval_s,
                &canary.enabled,
                &canary.created_at,
                &canary.updated_at,
            ],
        )?;
        Ok(())
    }

    async fn canary_get(&self, name: &str) -> Result<Option<Canary>> {
        let result: Option<Canary> = self
            .query_row(
                "SELECT name, index, query, min_results, max_results, interval_s, enabled, created_at, updated_at
                 FROM canaries WHERE name = ?1",
                &[&name as &dyn rusqlite::ToSql],
                |row| Ok(Canary {
                    name: row.get(0)?,
                    index: row.get(1)?,
                    query: row.get(2)?,
                    min_results: row.get(3)?,
                    max_results: row.get(4)?,
                    interval_s: row.get(5)?,
                    enabled: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
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
            "SELECT name, index, query, min_results, max_results, interval_s, enabled, created_at, updated_at FROM canaries",
            &[] as &[&dyn rusqlite::ToSql],
            |row| Ok(Canary {
                name: row.get(0)?,
                index: row.get(1)?,
                query: row.get(2)?,
                min_results: row.get(3)?,
                max_results: row.get(4)?,
                interval_s: row.get(5)?,
                enabled: row.get(6)?,
                created_at: row.get(7)?,
                updated_at: row.get(8)?,
            }),
        )
    }

    async fn canary_run_insert(&self, run: &CanaryRun) -> Result<()> {
        self.execute(
            "INSERT INTO canary_runs (run_id, canary_name, ran_at, passed, result_count, error, latency_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            &[
                &run.run_id as &dyn rusqlite::ToSql,
                &run.canary_name,
                &run.ran_at,
                &run.passed,
                &run.result_count,
                &run.error.as_deref().unwrap_or(""),
                &run.latency_ms,
            ],
        )?;
        Ok(())
    }

    async fn canary_run_list(&self, canary_name: &str, limit: usize) -> Result<Vec<CanaryRun>> {
        self.query_map(
            &format!(
                "SELECT run_id, canary_name, ran_at, passed, result_count, error, latency_ms
                 FROM canary_runs WHERE canary_name = ?1 ORDER BY ran_at DESC LIMIT {limit}"
            ),
            &[&canary_name as &dyn rusqlite::ToSql],
            |row| {
                Ok(CanaryRun {
                    run_id: row.get(0)?,
                    canary_name: row.get(1)?,
                    ran_at: row.get(2)?,
                    passed: row.get(3)?,
                    result_count: row.get(4)?,
                    error: {
                        let error: String = row.get(5)?;
                        if error.is_empty() {
                            None
                        } else {
                            Some(error)
                        }
                    },
                    latency_ms: row.get(6)?,
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
                "SELECT sink, index, cursor, updated_at FROM cdc_cursors WHERE sink = ?1 AND index = ?2",
                &[&sink as &dyn rusqlite::ToSql, &index as &dyn rusqlite::ToSql],
                |row| Ok(CdcCursor {
                    sink: row.get(0)?,
                    index: row.get(1)?,
                    cursor: row.get(2)?,
                    updated_at: row.get(3)?,
                }),
            )
            .ok();
        Ok(result)
    }

    async fn cdc_cursor_set(&self, cursor: &CdcCursor) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO cdc_cursors (sink, index, cursor, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            &[
                &cursor.sink as &dyn rusqlite::ToSql,
                &cursor.index,
                &cursor.cursor,
                &cursor.updated_at,
            ],
        )?;
        Ok(())
    }

    async fn cdc_cursor_list(&self, sink: &str) -> Result<Vec<CdcCursor>> {
        self.query_map(
            "SELECT sink, index, cursor, updated_at FROM cdc_cursors WHERE sink = ?1",
            &[&sink as &dyn rusqlite::ToSql],
            |row| {
                Ok(CdcCursor {
                    sink: row.get(0)?,
                    index: row.get(1)?,
                    cursor: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            },
        )
    }

    async fn tenant_upsert(&self, tenant: &Tenant) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO tenant_map (api_key, tenant_id, name, capabilities, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            &[
                &tenant.api_key as &dyn rusqlite::ToSql,
                &tenant.tenant_id,
                &tenant.name,
                &tenant.capabilities,
                &tenant.created_at,
                &tenant.updated_at,
            ],
        )?;
        Ok(())
    }

    async fn tenant_get(&self, api_key: &str) -> Result<Option<Tenant>> {
        let result: Option<Tenant> = self
            .query_row(
                "SELECT api_key, tenant_id, name, capabilities, created_at, updated_at
                 FROM tenant_map WHERE api_key = ?1",
                &[&api_key as &dyn rusqlite::ToSql],
                |row| {
                    Ok(Tenant {
                        api_key: row.get(0)?,
                        tenant_id: row.get(1)?,
                        name: row.get(2)?,
                        capabilities: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                    })
                },
            )
            .ok();
        Ok(result)
    }

    async fn tenant_delete(&self, api_key: &str) -> Result<()> {
        self.execute(
            "DELETE FROM tenant_map WHERE api_key = ?1",
            &[&api_key as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn tenant_list(&self) -> Result<Vec<Tenant>> {
        self.query_map(
            "SELECT api_key, tenant_id, name, capabilities, created_at, updated_at FROM tenant_map",
            &[] as &[&dyn rusqlite::ToSql],
            |row| {
                Ok(Tenant {
                    api_key: row.get(0)?,
                    tenant_id: row.get(1)?,
                    name: row.get(2)?,
                    capabilities: row.get(3)?,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
                })
            },
        )
    }

    async fn rollover_policy_upsert(&self, policy: &RolloverPolicy) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO rollover_policies (name, index_pattern, max_age_days, max_size_bytes, max_docs, enabled, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            &[
                &policy.name as &dyn rusqlite::ToSql,
                &policy.index_pattern,
                &policy.max_age_days,
                &policy.max_size_bytes,
                &policy.max_docs,
                &policy.enabled,
                &policy.created_at,
                &policy.updated_at,
            ],
        )?;
        Ok(())
    }

    async fn rollover_policy_get(&self, name: &str) -> Result<Option<RolloverPolicy>> {
        let result: Option<RolloverPolicy> = self
            .query_row(
                "SELECT name, index_pattern, max_age_days, max_size_bytes, max_docs, enabled, created_at, updated_at
                 FROM rollover_policies WHERE name = ?1",
                &[&name as &dyn rusqlite::ToSql],
                |row| Ok(RolloverPolicy {
                    name: row.get(0)?,
                    index_pattern: row.get(1)?,
                    max_age_days: row.get(2)?,
                    max_size_bytes: row.get(3)?,
                    max_docs: row.get(4)?,
                    enabled: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                }),
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
            "SELECT name, index_pattern, max_age_days, max_size_bytes, max_docs, enabled, created_at, updated_at FROM rollover_policies",
            &[] as &[&dyn rusqlite::ToSql],
            |row| Ok(RolloverPolicy {
                name: row.get(0)?,
                index_pattern: row.get(1)?,
                max_age_days: row.get(2)?,
                max_size_bytes: row.get(3)?,
                max_docs: row.get(4)?,
                enabled: row.get(5)?,
                created_at: row.get(6)?,
                updated_at: row.get(7)?,
            }),
        )
    }

    async fn search_ui_config_upsert(&self, config: &SearchUiConfig) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO search_ui_config (index, config, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            &[
                &config.index as &dyn rusqlite::ToSql,
                &config.config,
                &config.created_at,
                &config.updated_at,
            ],
        )?;
        Ok(())
    }

    async fn search_ui_config_get(&self, index: &str) -> Result<Option<SearchUiConfig>> {
        let result: Option<SearchUiConfig> = self
            .query_row(
                "SELECT index, config, created_at, updated_at FROM search_ui_config WHERE index = ?1",
                &[&index as &dyn rusqlite::ToSql],
                |row| Ok(SearchUiConfig {
                    index: row.get(0)?,
                    config: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                }),
            )
            .ok();
        Ok(result)
    }

    async fn search_ui_config_delete(&self, index: &str) -> Result<()> {
        self.execute(
            "DELETE FROM search_ui_config WHERE index = ?1",
            &[&index as &dyn rusqlite::ToSql],
        )?;
        Ok(())
    }

    async fn search_ui_config_list(&self) -> Result<Vec<SearchUiConfig>> {
        self.query_map(
            "SELECT index, config, created_at, updated_at FROM search_ui_config",
            &[] as &[&dyn rusqlite::ToSql],
            |row| {
                Ok(SearchUiConfig {
                    index: row.get(0)?,
                    config: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            },
        )
    }

    async fn admin_session_upsert(&self, session: &AdminSession) -> Result<()> {
        self.execute(
            "INSERT OR REPLACE INTO admin_sessions (session_id, user_id, created_at, expires_at, revoked)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            &[
                &session.session_id as &dyn rusqlite::ToSql,
                &session.user_id,
                &session.created_at,
                &session.expires_at,
                &session.revoked,
            ],
        )?;
        Ok(())
    }

    async fn admin_session_get(&self, session_id: &str) -> Result<Option<AdminSession>> {
        let result: Option<AdminSession> = self
            .query_row(
                "SELECT session_id, user_id, created_at, expires_at, revoked FROM admin_sessions WHERE session_id = ?1",
                &[&session_id as &dyn rusqlite::ToSql],
                |row| Ok(AdminSession {
                    session_id: row.get(0)?,
                    user_id: row.get(1)?,
                    created_at: row.get(2)?,
                    expires_at: row.get(3)?,
                    revoked: row.get(4)?,
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
    /// Initialize the database schema.
    fn init_schema(conn: &Connection) -> Result<()> {
        // Table 1: Tasks
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tasks (
                miroir_id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                status TEXT NOT NULL,
                node_tasks TEXT NOT NULL,
                error TEXT
            )",
            [],
        )?;

        // Table 2: Node settings version
        conn.execute(
            "CREATE TABLE IF NOT EXISTS node_settings_version (
                index TEXT NOT NULL,
                node_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (index, node_id)
            )",
            [],
        )?;

        // Table 3: Aliases
        conn.execute(
            "CREATE TABLE IF NOT EXISTS aliases (
                name TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                current_uid TEXT,
                target_uids TEXT NOT NULL,
                version INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 4: Sessions
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                index TEXT NOT NULL,
                settings_version INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 5: Idempotency cache
        conn.execute(
            "CREATE TABLE IF NOT EXISTS idempotency_cache (
                key TEXT PRIMARY KEY,
                response TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 6: Jobs
        conn.execute(
            "CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                parameters TEXT NOT NULL,
                status TEXT NOT NULL,
                worker_id TEXT,
                result TEXT,
                error TEXT,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                completed_at INTEGER
            )",
            [],
        )?;

        // Table 7: Leader lease
        conn.execute(
            "CREATE TABLE IF NOT EXISTS leader_lease (
                lease_id TEXT PRIMARY KEY,
                holder TEXT NOT NULL,
                acquired_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 8: Canaries
        conn.execute(
            "CREATE TABLE IF NOT EXISTS canaries (
                name TEXT PRIMARY KEY,
                index TEXT NOT NULL,
                query TEXT NOT NULL,
                min_results INTEGER NOT NULL,
                max_results INTEGER NOT NULL,
                interval_s INTEGER NOT NULL,
                enabled INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 9: Canary runs
        conn.execute(
            "CREATE TABLE IF NOT EXISTS canary_runs (
                run_id TEXT PRIMARY KEY,
                canary_name TEXT NOT NULL,
                ran_at INTEGER NOT NULL,
                passed INTEGER NOT NULL,
                result_count INTEGER NOT NULL,
                error TEXT,
                latency_ms INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 10: CDC cursors
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cdc_cursors (
                sink TEXT NOT NULL,
                index TEXT NOT NULL,
                cursor TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (sink, index)
            )",
            [],
        )?;

        // Table 11: Tenant map
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tenant_map (
                api_key TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                name TEXT NOT NULL,
                capabilities TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 12: Rollover policies
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rollover_policies (
                name TEXT PRIMARY KEY,
                index_pattern TEXT NOT NULL,
                max_age_days INTEGER,
                max_size_bytes INTEGER,
                max_docs INTEGER,
                enabled INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 13: Search UI config
        conn.execute(
            "CREATE TABLE IF NOT EXISTS search_ui_config (
                index TEXT PRIMARY KEY,
                config TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Table 14: Admin sessions
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_sessions (
                session_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0
            )",
            [],
        )?;

        Ok(())
    }
}

// String conversions for enums
impl std::fmt::Display for TaskStatus {
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

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Enqueued" => Ok(Self::Enqueued),
            "Processing" => Ok(Self::Processing),
            "Succeeded" => Ok(Self::Succeeded),
            "Failed" => Ok(Self::Failed),
            "Canceled" => Ok(Self::Canceled),
            _ => Err(format!("invalid task status: {s}")),
        }
    }
}

impl std::fmt::Display for NodeTaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enqueued => write!(f, "Enqueued"),
            Self::Processing => write!(f, "Processing"),
            Self::Succeeded => write!(f, "Succeeded"),
            Self::Failed => write!(f, "Failed"),
        }
    }
}

impl std::str::FromStr for NodeTaskStatus {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Enqueued" => Ok(Self::Enqueued),
            "Processing" => Ok(Self::Processing),
            "Succeeded" => Ok(Self::Succeeded),
            "Failed" => Ok(Self::Failed),
            _ => Err(format!("invalid node task status: {s}")),
        }
    }
}

impl std::fmt::Display for AliasKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Single => write!(f, "Single"),
            Self::Multi => write!(f, "Multi"),
        }
    }
}

impl std::str::FromStr for AliasKind {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Single" => Ok(Self::Single),
            "Multi" => Ok(Self::Multi),
            _ => Err(format!("invalid alias kind: {s}")),
        }
    }
}

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
