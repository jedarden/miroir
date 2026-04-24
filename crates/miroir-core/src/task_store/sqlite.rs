use crate::schema_migrations::{build_registry, MigrationRegistry};
use crate::task_store::*;
use crate::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

/// Get the migration registry for this binary.
fn registry() -> &'static MigrationRegistry {
    use std::sync::OnceLock;
    static REGISTRY: OnceLock<MigrationRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| build_registry())
}

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

        let current_version = current.unwrap_or(0);

        // Validate that the store version is not ahead of the binary version
        registry().validate_version(current_version)?;

        // Apply pending migrations
        let pending = registry().pending_migrations(current_version);
        for migration in pending {
            conn.execute_batch(migration.sql)?;
            conn.execute(
                "INSERT INTO schema_versions (version, applied_at) VALUES (?1, ?2)",
                params![migration.version, now_ms()],
            )?;
        }

        Ok(())
    }

    // --- Table 1: tasks helpers ---

    fn task_row_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRow> {
        let node_tasks_json: String = row.get(3)?;
        let node_tasks: HashMap<String, u64> = serde_json::from_str(&node_tasks_json)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        let node_errors_json: String = row.get(9)?;
        let node_errors: HashMap<String, String> = serde_json::from_str(&node_errors_json)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(TaskRow {
            miroir_id: row.get(0)?,
            created_at: row.get(1)?,
            status: row.get(2)?,
            node_tasks,
            error: row.get(4)?,
            started_at: row.get(5)?,
            finished_at: row.get(6)?,
            index_uid: row.get(7)?,
            task_type: row.get(8)?,
            node_errors,
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
        let node_errors_json = serde_json::to_string(&task.node_errors)?;
        conn.execute(
            "INSERT INTO tasks (miroir_id, created_at, status, node_tasks, error, started_at, finished_at, index_uid, task_type, node_errors)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                task.miroir_id,
                task.created_at,
                task.status,
                node_tasks_json,
                task.error,
                task.started_at,
                task.finished_at,
                task.index_uid,
                task.task_type,
                node_errors_json,
            ],
        )?;
        Ok(())
    }

    fn get_task(&self, miroir_id: &str) -> Result<Option<TaskRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT miroir_id, created_at, status, node_tasks, error, started_at, finished_at, index_uid, task_type, node_errors
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

    #[allow(unused_assignments)]
    fn list_tasks(&self, filter: &TaskFilter) -> Result<Vec<TaskRow>> {
        let conn = self.conn.lock().unwrap();
        let mut sql = "SELECT miroir_id, created_at, status, node_tasks, error, started_at, finished_at, index_uid, task_type, node_errors FROM tasks"
            .to_string();
        let mut conditions = Vec::new();
        let mut param_idx = 1;
        let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref status) = filter.status {
            conditions.push(format!("status = ?{param_idx}"));
            param_values.push(Box::new(status.clone()));
            param_idx += 1;
        }
        if let Some(ref index_uid) = filter.index_uid {
            conditions.push(format!("index_uid = ?{param_idx}"));
            param_values.push(Box::new(index_uid.clone()));
            param_idx += 1;
        }
        if let Some(ref task_type) = filter.task_type {
            conditions.push(format!("task_type = ?{param_idx}"));
            param_values.push(Box::new(task_type.clone()));
            param_idx += 1;
        }
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push_str(" ORDER BY created_at DESC");
        if let Some(limit) = filter.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = filter.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        let params_refs: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|p| p.as_ref()).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_refs.as_slice(), Self::task_row_from_row)?;
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

    // --- Tables 8-14: Feature-flagged tables ---

    fn prune_tasks(&self, cutoff_ms: i64, batch_size: u32) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        // SQLite doesn't support LIMIT in DELETE directly, so use a subquery
        let rows = conn.execute(
            "DELETE FROM tasks WHERE rowid IN (
                SELECT rowid FROM tasks
                WHERE created_at < ?1 AND status IN ('succeeded', 'failed', 'canceled')
                LIMIT ?2
            )",
            params![cutoff_ms, batch_size],
        )?;
        Ok(rows)
    }

    fn task_count(&self) -> Result<u64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))?;
        Ok(count as u64)
    }

    // --- Table 8: canaries ---

    fn upsert_canary(&self, canary: &NewCanary) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO canaries (id, name, index_uid, interval_s, query_json, assertions_json, enabled, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(id) DO UPDATE SET
                name = ?2,
                index_uid = ?3,
                interval_s = ?4,
                query_json = ?5,
                assertions_json = ?6,
                enabled = ?7",
            params![
                canary.id,
                canary.name,
                canary.index_uid,
                canary.interval_s,
                canary.query_json,
                canary.assertions_json,
                canary.enabled as i64,
                canary.created_at,
            ],
        )?;
        Ok(())
    }

    fn get_canary(&self, id: &str) -> Result<Option<CanaryRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT id, name, index_uid, interval_s, query_json, assertions_json, enabled, created_at
                 FROM canaries WHERE id = ?1",
                params![id],
                |row| {
                    Ok(CanaryRow {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        index_uid: row.get(2)?,
                        interval_s: row.get(3)?,
                        query_json: row.get(4)?,
                        assertions_json: row.get(5)?,
                        enabled: row.get::<_, i64>(6)? != 0,
                        created_at: row.get(7)?,
                    })
                },
            )
            .optional()?)
    }

    fn list_canaries(&self) -> Result<Vec<CanaryRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, index_uid, interval_s, query_json, assertions_json, enabled, created_at
             FROM canaries",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(CanaryRow {
                id: row.get(0)?,
                name: row.get(1)?,
                index_uid: row.get(2)?,
                interval_s: row.get(3)?,
                query_json: row.get(4)?,
                assertions_json: row.get(5)?,
                enabled: row.get::<_, i64>(6)? != 0,
                created_at: row.get(7)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    fn delete_canary(&self, id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute("DELETE FROM canaries WHERE id = ?1", params![id])?;
        Ok(rows > 0)
    }

    // --- Table 9: canary_runs ---

    fn insert_canary_run(&self, run: &NewCanaryRun, run_history_limit: usize) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;

        // Insert the new run
        tx.execute(
            "INSERT INTO canary_runs (canary_id, ran_at, status, latency_ms, failed_assertions_json)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                run.canary_id,
                run.ran_at,
                run.status,
                run.latency_ms,
                run.failed_assertions_json,
            ],
        )?;

        // Prune old runs to stay within the history limit
        // We want to keep only the most recent N runs (where N = run_history_limit)
        // Delete any runs that are NOT among the N most recent
        let limit = run_history_limit as i64;
        tx.execute(
            "DELETE FROM canary_runs
             WHERE canary_id = ?1
               AND ran_at NOT IN (
                   SELECT ran_at
                   FROM canary_runs
                   WHERE canary_id = ?1
                   ORDER BY ran_at DESC
                   LIMIT ?2
               )",
            params![run.canary_id, limit],
        )?;

        tx.commit()?;
        Ok(())
    }

    fn get_canary_runs(&self, canary_id: &str, limit: usize) -> Result<Vec<CanaryRunRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT canary_id, ran_at, status, latency_ms, failed_assertions_json
             FROM canary_runs
             WHERE canary_id = ?1
             ORDER BY ran_at DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![canary_id, limit as i64], |row| {
            Ok(CanaryRunRow {
                canary_id: row.get(0)?,
                ran_at: row.get(1)?,
                status: row.get(2)?,
                latency_ms: row.get(3)?,
                failed_assertions_json: row.get(4)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    // --- Table 10: cdc_cursors ---

    fn upsert_cdc_cursor(&self, cursor: &NewCdcCursor) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO cdc_cursors (sink_name, index_uid, last_event_seq, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(sink_name, index_uid) DO UPDATE SET
                last_event_seq = ?3,
                updated_at = ?4",
            params![
                cursor.sink_name,
                cursor.index_uid,
                cursor.last_event_seq,
                cursor.updated_at,
            ],
        )?;
        Ok(())
    }

    fn get_cdc_cursor(&self, sink_name: &str, index_uid: &str) -> Result<Option<CdcCursorRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT sink_name, index_uid, last_event_seq, updated_at
                 FROM cdc_cursors WHERE sink_name = ?1 AND index_uid = ?2",
                params![sink_name, index_uid],
                |row| {
                    Ok(CdcCursorRow {
                        sink_name: row.get(0)?,
                        index_uid: row.get(1)?,
                        last_event_seq: row.get(2)?,
                        updated_at: row.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    fn list_cdc_cursors(&self, sink_name: &str) -> Result<Vec<CdcCursorRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT sink_name, index_uid, last_event_seq, updated_at
             FROM cdc_cursors WHERE sink_name = ?1",
        )?;
        let rows = stmt.query_map(params![sink_name], |row| {
            Ok(CdcCursorRow {
                sink_name: row.get(0)?,
                index_uid: row.get(1)?,
                last_event_seq: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    // --- Table 11: tenant_map ---

    fn insert_tenant_mapping(&self, mapping: &NewTenantMapping) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO tenant_map (api_key_hash, tenant_id, group_id)
             VALUES (?1, ?2, ?3)",
            params![
                mapping.api_key_hash.as_slice(),
                mapping.tenant_id,
                mapping.group_id,
            ],
        )?;
        Ok(())
    }

    fn get_tenant_mapping(&self, api_key_hash: &[u8]) -> Result<Option<TenantMapRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT api_key_hash, tenant_id, group_id
                 FROM tenant_map WHERE api_key_hash = ?1",
                params![api_key_hash],
                |row| {
                    Ok(TenantMapRow {
                        api_key_hash: row.get(0)?,
                        tenant_id: row.get(1)?,
                        group_id: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    fn delete_tenant_mapping(&self, api_key_hash: &[u8]) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM tenant_map WHERE api_key_hash = ?1",
            params![api_key_hash],
        )?;
        Ok(rows > 0)
    }

    // --- Table 12: rollover_policies ---

    fn upsert_rollover_policy(&self, policy: &NewRolloverPolicy) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO rollover_policies (name, write_alias, read_alias, pattern, triggers_json, retention_json, template_json, enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(name) DO UPDATE SET
                write_alias = ?2,
                read_alias = ?3,
                pattern = ?4,
                triggers_json = ?5,
                retention_json = ?6,
                template_json = ?7,
                enabled = ?8",
            params![
                policy.name,
                policy.write_alias,
                policy.read_alias,
                policy.pattern,
                policy.triggers_json,
                policy.retention_json,
                policy.template_json,
                policy.enabled as i64,
            ],
        )?;
        Ok(())
    }

    fn get_rollover_policy(&self, name: &str) -> Result<Option<RolloverPolicyRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT name, write_alias, read_alias, pattern, triggers_json, retention_json, template_json, enabled
                 FROM rollover_policies WHERE name = ?1",
                params![name],
                |row| {
                    Ok(RolloverPolicyRow {
                        name: row.get(0)?,
                        write_alias: row.get(1)?,
                        read_alias: row.get(2)?,
                        pattern: row.get(3)?,
                        triggers_json: row.get(4)?,
                        retention_json: row.get(5)?,
                        template_json: row.get(6)?,
                        enabled: row.get::<_, i64>(7)? != 0,
                    })
                },
            )
            .optional()?)
    }

    fn list_rollover_policies(&self) -> Result<Vec<RolloverPolicyRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT name, write_alias, read_alias, pattern, triggers_json, retention_json, template_json, enabled
             FROM rollover_policies",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(RolloverPolicyRow {
                name: row.get(0)?,
                write_alias: row.get(1)?,
                read_alias: row.get(2)?,
                pattern: row.get(3)?,
                triggers_json: row.get(4)?,
                retention_json: row.get(5)?,
                template_json: row.get(6)?,
                enabled: row.get::<_, i64>(7)? != 0,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    fn delete_rollover_policy(&self, name: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute("DELETE FROM rollover_policies WHERE name = ?1", params![name])?;
        Ok(rows > 0)
    }

    // --- Table 13: search_ui_config ---

    fn upsert_search_ui_config(&self, config: &NewSearchUiConfig) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO search_ui_config (index_uid, config_json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(index_uid) DO UPDATE SET
                config_json = ?2,
                updated_at = ?3",
            params![config.index_uid, config.config_json, config.updated_at],
        )?;
        Ok(())
    }

    fn get_search_ui_config(&self, index_uid: &str) -> Result<Option<SearchUiConfigRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT index_uid, config_json, updated_at
                 FROM search_ui_config WHERE index_uid = ?1",
                params![index_uid],
                |row| {
                    Ok(SearchUiConfigRow {
                        index_uid: row.get(0)?,
                        config_json: row.get(1)?,
                        updated_at: row.get(2)?,
                    })
                },
            )
            .optional()?)
    }

    fn delete_search_ui_config(&self, index_uid: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM search_ui_config WHERE index_uid = ?1",
            params![index_uid],
        )?;
        Ok(rows > 0)
    }

    // --- Table 14: admin_sessions ---

    fn insert_admin_session(&self, session: &NewAdminSession) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO admin_sessions (session_id, csrf_token, admin_key_hash, created_at, expires_at, revoked, user_agent, source_ip)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)",
            params![
                session.session_id,
                session.csrf_token,
                session.admin_key_hash,
                session.created_at,
                session.expires_at,
                session.user_agent,
                session.source_ip,
            ],
        )?;
        Ok(())
    }

    fn get_admin_session(&self, session_id: &str) -> Result<Option<AdminSessionRow>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT session_id, csrf_token, admin_key_hash, created_at, expires_at, revoked, user_agent, source_ip
                 FROM admin_sessions WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok(AdminSessionRow {
                        session_id: row.get(0)?,
                        csrf_token: row.get(1)?,
                        admin_key_hash: row.get(2)?,
                        created_at: row.get(3)?,
                        expires_at: row.get(4)?,
                        revoked: row.get::<_, i64>(5)? != 0,
                        user_agent: row.get(6)?,
                        source_ip: row.get(7)?,
                    })
                },
            )
            .optional()?)
    }

    fn revoke_admin_session(&self, session_id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "UPDATE admin_sessions SET revoked = 1 WHERE session_id = ?1",
            params![session_id],
        )?;
        Ok(rows > 0)
    }

    fn delete_expired_admin_sessions(&self, now_ms: i64) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "DELETE FROM admin_sessions WHERE expires_at < ?1",
            params![now_ms],
        )?;
        Ok(rows)
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
            started_at: None,
            finished_at: None,
            index_uid: None,
            task_type: None,
            node_errors: HashMap::new(),
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
                    started_at: None,
                    finished_at: None,
                    index_uid: None,
                    task_type: None,
                    node_errors: HashMap::new(),
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
                started_at: None,
                finished_at: None,
                index_uid: None,
                task_type: None,
                node_errors: HashMap::new(),
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
        assert_eq!(version, registry().max_version());
    }

    // --- Schema version ahead error ---

    #[test]
    fn schema_version_ahead_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Create a store with current binary
        let store = SqliteTaskStore::open(&path).unwrap();
        store.migrate().unwrap();
        drop(store);

        // Artificially set schema version ahead of binary
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO schema_versions (version, applied_at) VALUES (?1, ?2)",
            params![registry().max_version() + 1, now_ms()],
        )
        .unwrap();
        drop(conn);

        // Re-opening should fail with SchemaVersionAhead error
        let result = SqliteTaskStore::open(&path).and_then(|s| s.migrate());
        assert!(result.is_err());
        match result.unwrap_err() {
            crate::MiroirError::SchemaVersionAhead {
                store_version,
                binary_version,
            } => {
                assert_eq!(store_version, registry().max_version() + 1);
                assert_eq!(binary_version, registry().max_version());
            }
            _ => panic!("expected SchemaVersionAhead error"),
        }
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
                    started_at: None,
                    finished_at: None,
                    index_uid: None,
                    task_type: None,
                    node_errors: HashMap::new(),
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

    // --- Table 8: canaries ---

    #[test]
    fn canary_upsert_get_list_delete() {
        let store = test_store();

        // Insert a canary
        store
            .upsert_canary(&NewCanary {
                id: "canary-1".to_string(),
                name: "Search health check".to_string(),
                index_uid: "logs".to_string(),
                interval_s: 60,
                query_json: r#"{"q": "error"}"#.to_string(),
                assertions_json: r#"[{"type": "min_hits", "value": 1}]"#.to_string(),
                enabled: true,
                created_at: 1000,
            })
            .unwrap();

        // Get the canary
        let canary = store.get_canary("canary-1").unwrap().unwrap();
        assert_eq!(canary.id, "canary-1");
        assert_eq!(canary.name, "Search health check");
        assert_eq!(canary.index_uid, "logs");
        assert_eq!(canary.interval_s, 60);
        assert!(canary.enabled);

        // List all canaries
        let canaries = store.list_canaries().unwrap();
        assert_eq!(canaries.len(), 1);
        assert_eq!(canaries[0].id, "canary-1");

        // Upsert (update) the canary
        store
            .upsert_canary(&NewCanary {
                id: "canary-1".to_string(),
                name: "Updated health check".to_string(),
                index_uid: "logs".to_string(),
                interval_s: 120,
                query_json: r#"{"q": "error"}"#.to_string(),
                assertions_json: r#"[{"type": "min_hits", "value": 1}]"#.to_string(),
                enabled: false,
                created_at: 1000,
            })
            .unwrap();

        let canary = store.get_canary("canary-1").unwrap().unwrap();
        assert_eq!(canary.name, "Updated health check");
        assert_eq!(canary.interval_s, 120);
        assert!(!canary.enabled);

        // Delete the canary
        assert!(store.delete_canary("canary-1").unwrap());
        assert!(store.get_canary("canary-1").unwrap().is_none());

        // Delete non-existent canary
        assert!(!store.delete_canary("no-such-canary").unwrap());
    }

    // --- Table 9: canary_runs ---

    #[test]
    fn canary_runs_insert_get_and_auto_prune() {
        let store = test_store();

        // Create a canary first (foreign key not enforced, but logical consistency)
        store
            .upsert_canary(&NewCanary {
                id: "canary-1".to_string(),
                name: "Test canary".to_string(),
                index_uid: "logs".to_string(),
                interval_s: 60,
                query_json: r#"{"q": "test"}"#.to_string(),
                assertions_json: r#"[]"#.to_string(),
                enabled: true,
                created_at: 1000,
            })
            .unwrap();

        // Insert 5 runs with history limit of 3
        for i in 0..5 {
            store
                .insert_canary_run(
                    &NewCanaryRun {
                        canary_id: "canary-1".to_string(),
                        ran_at: 1000 + i * 100,
                        status: if i == 2 { "fail" } else { "pass" }.to_string(),
                        latency_ms: 50 + i * 10,
                        failed_assertions_json: if i == 2 {
                            Some(r#"[{"assertion": "min_hits", "reason": "no hits"}]"#.to_string())
                        } else {
                            None
                        },
                    },
                    3, // run_history_limit
                )
                .unwrap();
        }

        // Only the 3 most recent runs should remain
        let runs = store.get_canary_runs("canary-1", 10).unwrap();
        assert_eq!(runs.len(), 3);
        // Runs are ordered by ran_at DESC, so we should see runs 4, 3, 2
        assert_eq!(runs[0].ran_at, 1400); // i=4
        assert_eq!(runs[1].ran_at, 1300); // i=3
        assert_eq!(runs[2].ran_at, 1200); // i=2
        assert_eq!(runs[2].status, "fail");
        assert!(runs[2].failed_assertions_json.is_some());

        // Test limit parameter
        let runs = store.get_canary_runs("canary-1", 2).unwrap();
        assert_eq!(runs.len(), 2);
    }

    #[test]
    fn canary_runs_empty_for_nonexistent_canary() {
        let store = test_store();
        let runs = store.get_canary_runs("no-such-canary", 10).unwrap();
        assert!(runs.is_empty());
    }

    // --- Table 10: cdc_cursors ---

    #[test]
    fn cdc_cursor_upsert_get_list() {
        let store = test_store();

        // Insert a cursor
        store
            .upsert_cdc_cursor(&NewCdcCursor {
                sink_name: "elasticsearch".to_string(),
                index_uid: "logs".to_string(),
                last_event_seq: 12345,
                updated_at: 2000,
            })
            .unwrap();

        // Get the cursor
        let cursor = store
            .get_cdc_cursor("elasticsearch", "logs")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.sink_name, "elasticsearch");
        assert_eq!(cursor.index_uid, "logs");
        assert_eq!(cursor.last_event_seq, 12345);

        // List all cursors for a sink
        store
            .upsert_cdc_cursor(&NewCdcCursor {
                sink_name: "elasticsearch".to_string(),
                index_uid: "metrics".to_string(),
                last_event_seq: 67890,
                updated_at: 2500,
            })
            .unwrap();

        let cursors = store.list_cdc_cursors("elasticsearch").unwrap();
        assert_eq!(cursors.len(), 2);

        // Upsert (update) the cursor
        store
            .upsert_cdc_cursor(&NewCdcCursor {
                sink_name: "elasticsearch".to_string(),
                index_uid: "logs".to_string(),
                last_event_seq: 13000,
                updated_at: 3000,
            })
            .unwrap();

        let cursor = store
            .get_cdc_cursor("elasticsearch", "logs")
            .unwrap()
            .unwrap();
        assert_eq!(cursor.last_event_seq, 13000);

        // Composite PK: different sink should not exist
        assert!(store
            .get_cdc_cursor("elasticsearch", "nonexistent")
            .unwrap()
            .is_none());
        assert!(store
            .get_cdc_cursor("unknown_sink", "logs")
            .unwrap()
            .is_none());
    }

    // --- Table 11: tenant_map ---

    #[test]
    fn tenant_map_insert_get_delete() {
        let store = test_store();

        // Create a 32-byte hash (sha256)
        let api_key_hash = vec![1u8; 32];

        // Insert a tenant mapping
        store
            .insert_tenant_mapping(&NewTenantMapping {
                api_key_hash: api_key_hash.clone(),
                tenant_id: "acme-corp".to_string(),
                group_id: Some(2),
            })
            .unwrap();

        // Get the mapping
        let mapping = store.get_tenant_mapping(&api_key_hash).unwrap().unwrap();
        assert_eq!(mapping.tenant_id, "acme-corp");
        assert_eq!(mapping.group_id, Some(2));

        // Missing mapping
        let unknown_hash = vec![99u8; 32];
        assert!(store.get_tenant_mapping(&unknown_hash).unwrap().is_none());

        // Delete the mapping
        assert!(store.delete_tenant_mapping(&api_key_hash).unwrap());
        assert!(store.get_tenant_mapping(&api_key_hash).unwrap().is_none());

        // Delete non-existent mapping
        assert!(!store.delete_tenant_mapping(&unknown_hash).unwrap());
    }

    #[test]
    fn tenant_map_nullable_group_id() {
        let store = test_store();

        let api_key_hash = vec![2u8; 32];

        store
            .insert_tenant_mapping(&NewTenantMapping {
                api_key_hash: api_key_hash.clone(),
                tenant_id: "default-tenant".to_string(),
                group_id: None, // NULL group_id falls back to hash(tenant_id) % RG
            })
            .unwrap();

        let mapping = store.get_tenant_mapping(&api_key_hash).unwrap().unwrap();
        assert_eq!(mapping.tenant_id, "default-tenant");
        assert_eq!(mapping.group_id, None);
    }

    // --- Table 12: rollover_policies ---

    #[test]
    fn rollover_policy_upsert_get_list_delete() {
        let store = test_store();

        // Insert a policy
        store
            .upsert_rollover_policy(&NewRolloverPolicy {
                name: "daily-logs".to_string(),
                write_alias: "logs-write".to_string(),
                read_alias: "logs-read".to_string(),
                pattern: "logs-{YYYY-MM-DD}".to_string(),
                triggers_json: r#"{"max_age": "1d", "max_docs": 1000000}"#.to_string(),
                retention_json: r#"{"keep_indexes": 30}"#.to_string(),
                template_json: r#"{"primary_key": "id", "settings_ref": "logs-template"}"#.to_string(),
                enabled: true,
            })
            .unwrap();

        // Get the policy
        let policy = store.get_rollover_policy("daily-logs").unwrap().unwrap();
        assert_eq!(policy.name, "daily-logs");
        assert_eq!(policy.write_alias, "logs-write");
        assert_eq!(policy.read_alias, "logs-read");
        assert_eq!(policy.pattern, "logs-{YYYY-MM-DD}");
        assert!(policy.enabled);

        // List all policies
        let policies = store.list_rollover_policies().unwrap();
        assert_eq!(policies.len(), 1);

        // Upsert (update) the policy
        store
            .upsert_rollover_policy(&NewRolloverPolicy {
                name: "daily-logs".to_string(),
                write_alias: "logs-write".to_string(),
                read_alias: "logs-read".to_string(),
                pattern: "logs-{YYYY-MM-DD}".to_string(),
                triggers_json: r#"{"max_age": "1d", "max_docs": 2000000}"#.to_string(), // changed
                retention_json: r#"{"keep_indexes": 30}"#.to_string(),
                template_json: r#"{"primary_key": "id", "settings_ref": "logs-template"}"#.to_string(),
                enabled: false, // changed
            })
            .unwrap();

        let policy = store.get_rollover_policy("daily-logs").unwrap().unwrap();
        assert!(!policy.enabled);

        // Delete the policy
        assert!(store.delete_rollover_policy("daily-logs").unwrap());
        assert!(store.get_rollover_policy("daily-logs").unwrap().is_none());
    }

    // --- Table 13: search_ui_config ---

    #[test]
    fn search_ui_config_upsert_get_delete() {
        let store = test_store();

        let config_json = r#"{"title": "Product Search", "facets": ["category", "price"], "sort": ["relevance", "price_asc"]}"#;

        // Insert config
        store
            .upsert_search_ui_config(&NewSearchUiConfig {
                index_uid: "products".to_string(),
                config_json: config_json.to_string(),
                updated_at: 5000,
            })
            .unwrap();

        // Get config
        let config = store.get_search_ui_config("products").unwrap().unwrap();
        assert_eq!(config.index_uid, "products");
        assert_eq!(config.config_json, config_json);

        // Upsert (update) config
        let updated_json = r#"{"title": "Product Search V2", "facets": ["category"]}"#;
        store
            .upsert_search_ui_config(&NewSearchUiConfig {
                index_uid: "products".to_string(),
                config_json: updated_json.to_string(),
                updated_at: 6000,
            })
            .unwrap();

        let config = store.get_search_ui_config("products").unwrap().unwrap();
        assert_eq!(config.config_json, updated_json);
        assert_eq!(config.updated_at, 6000);

        // Delete config
        assert!(store.delete_search_ui_config("products").unwrap());
        assert!(store.get_search_ui_config("products").unwrap().is_none());
    }

    // --- Table 14: admin_sessions ---

    #[test]
    fn admin_session_insert_get_revoke_expire() {
        let store = test_store();

        // Insert a session
        store
            .insert_admin_session(&NewAdminSession {
                session_id: "sess-admin-1".to_string(),
                csrf_token: "csrf-token-abc123".to_string(),
                admin_key_hash: "hash-of-admin-key".to_string(),
                created_at: 7000,
                expires_at: 17000, // expires 10s after creation
                user_agent: Some("Mozilla/5.0".to_string()),
                source_ip: Some("192.168.1.100".to_string()),
            })
            .unwrap();

        // Get the session
        let session = store.get_admin_session("sess-admin-1").unwrap().unwrap();
        assert_eq!(session.session_id, "sess-admin-1");
        assert_eq!(session.csrf_token, "csrf-token-abc123");
        assert_eq!(session.admin_key_hash, "hash-of-admin-key");
        assert_eq!(session.created_at, 7000);
        assert_eq!(session.expires_at, 17000);
        assert!(!session.revoked);
        assert_eq!(session.user_agent.as_deref(), Some("Mozilla/5.0"));
        assert_eq!(session.source_ip.as_deref(), Some("192.168.1.100"));

        // Revoke the session
        assert!(store.revoke_admin_session("sess-admin-1").unwrap());
        let session = store.get_admin_session("sess-admin-1").unwrap().unwrap();
        assert!(session.revoked);

        // Double revoke is idempotent (still returns true if row exists)
        assert!(store.revoke_admin_session("sess-admin-1").unwrap());

        // Test session expiration cleanup
        store
            .insert_admin_session(&NewAdminSession {
                session_id: "sess-expired".to_string(),
                csrf_token: "csrf-expired".to_string(),
                admin_key_hash: "hash-expired".to_string(),
                created_at: 1000,
                expires_at: 5000, // already expired
                user_agent: None,
                source_ip: None,
            })
            .unwrap();

        let deleted = store.delete_expired_admin_sessions(10000).unwrap();
        assert_eq!(deleted, 1);
        assert!(store.get_admin_session("sess-expired").unwrap().is_none());

        // Active session should not be deleted
        assert!(store.get_admin_session("sess-admin-1").unwrap().is_some());
    }

    #[test]
    fn admin_session_nullable_fields() {
        let store = test_store();

        store
            .insert_admin_session(&NewAdminSession {
                session_id: "sess-minimal".to_string(),
                csrf_token: "csrf".to_string(),
                admin_key_hash: "hash".to_string(),
                created_at: 1000,
                expires_at: 10000,
                user_agent: None,
                source_ip: None,
            })
            .unwrap();

        let session = store.get_admin_session("sess-minimal").unwrap().unwrap();
        assert!(session.user_agent.is_none());
        assert!(session.source_ip.is_none());
    }

    // --- prune_tasks ---

    #[test]
    fn prune_tasks_deletes_old_terminal_tasks() {
        let store = test_store();

        // Insert tasks with different statuses and timestamps
        for i in 0..10 {
            store
                .insert_task(&NewTask {
                    miroir_id: format!("task-{i}"),
                    created_at: i as i64 * 1000,
                    status: match i {
                        0..=2 => "succeeded",
                        3..=5 => "failed",
                        6..=7 => "canceled",
                        _ => "enqueued", // should NOT be pruned
                    }
                    .to_string(),
                    node_tasks: HashMap::new(),
                    error: None,
                    started_at: None,
                    finished_at: None,
                    index_uid: None,
                    task_type: None,
                    node_errors: HashMap::new(),
                })
                .unwrap();
        }

        // Prune tasks older than 3500ms (should delete tasks 0, 1, 2, 3)
        let deleted = store.prune_tasks(3500, 100).unwrap();
        assert_eq!(deleted, 4); // tasks 0, 1, 2, 3 (succeeded or failed, < 3500ms)

        // Verify task-4 (failed at 4000ms) still exists
        assert!(store.get_task("task-4").unwrap().is_some());
        // Verify task-8 (enqueued) still exists regardless of age
        assert!(store.get_task("task-8").unwrap().is_some());
    }

    // --- Property tests (proptest) ---

    mod proptest_tests {
        use super::*;
        use proptest::prelude::*;

        fn test_store() -> SqliteTaskStore {
            let store = SqliteTaskStore::open_in_memory().unwrap();
            store.migrate().unwrap();
            store
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(50))]

            /// Property: (insert, get) round-trip preserves all fields.
            #[test]
            fn task_insert_get_roundtrip(
                miroir_id in "[a-z0-9-]{1,32}",
                created_at in 0i64..1_000_000,
                status in "(enqueued|processing|succeeded|failed|canceled)",
                error in proptest::option::of("[a-zA-Z0-9 ]{0,64}"),
                n_nodes in 0usize..5usize,
            ) {
                let store = test_store();
                let mut node_tasks = HashMap::new();
                for i in 0..n_nodes {
                    node_tasks.insert(format!("node-{i}"), i as u64);
                }

                let new_task = NewTask {
                    miroir_id: miroir_id.clone(),
                    created_at,
                    status: status.clone(),
                    node_tasks: node_tasks.clone(),
                    error: error.clone(),
                    started_at: None,
                    finished_at: None,
                    index_uid: None,
                    task_type: None,
                    node_errors: HashMap::new(),
                };
                store.insert_task(&new_task).unwrap();

                let got = store.get_task(&miroir_id).unwrap().unwrap();
                prop_assert_eq!(got.miroir_id, miroir_id);
                prop_assert_eq!(got.created_at, created_at);
                prop_assert_eq!(got.status, status);
                prop_assert_eq!(got.node_tasks, node_tasks);
                prop_assert_eq!(got.error, error);
            }

            /// Property: (upsert, get) for node_settings_version round-trips.
            #[test]
            fn node_settings_version_upsert_roundtrip(
                index_uid in "[a-z0-9]{1,16}",
                node_id in "[a-z0-9]{1,16}",
                version in 1i64..10000,
                updated_at in 0i64..1_000_000,
            ) {
                let store = test_store();
                store.upsert_node_settings_version(&index_uid, &node_id, version, updated_at).unwrap();
                let got = store.get_node_settings_version(&index_uid, &node_id).unwrap().unwrap();
                prop_assert_eq!(got.index_uid, index_uid);
                prop_assert_eq!(got.node_id, node_id);
                prop_assert_eq!(got.version, version);
                prop_assert_eq!(got.updated_at, updated_at);
            }

            /// Property: alias (create, get) round-trip for single aliases.
            #[test]
            fn alias_single_roundtrip(
                name in "[a-z0-9-]{1,32}",
                current_uid in proptest::option::of("uid-[a-z0-9]{1,16}"),
                version in 1i64..100,
            ) {
                let store = test_store();
                let alias = NewAlias {
                    name: name.clone(),
                    kind: "single".to_string(),
                    current_uid: current_uid.clone(),
                    target_uids: None,
                    version,
                    created_at: 1000,
                    history: vec![],
                };
                store.create_alias(&alias).unwrap();

                let got = store.get_alias(&name).unwrap().unwrap();
                prop_assert_eq!(got.name, name);
                prop_assert_eq!(got.kind, "single");
                prop_assert_eq!(got.current_uid, current_uid);
                prop_assert_eq!(got.version, version);
            }

            /// Property: (insert, list) — inserted tasks appear in list.
            #[test]
            fn task_insert_list_visible(
                ids in proptest::collection::vec("[a-z0-9-]{1,16}", 1..10),
            ) {
                let store = test_store();
                let unique_ids: std::collections::HashSet<String> = ids.into_iter().collect();
                for (i, id) in unique_ids.iter().enumerate() {
                    let mut nt = HashMap::new();
                    nt.insert("node-0".to_string(), i as u64);
                    store.insert_task(&NewTask {
                        miroir_id: id.clone(),
                        created_at: i as i64 * 1000,
                        status: "enqueued".to_string(),
                        node_tasks: nt,
                        error: None,
                        started_at: None,
                        finished_at: None,
                        index_uid: None,
                        task_type: None,
                        node_errors: HashMap::new(),
                    }).unwrap();
                }

                let all = store.list_tasks(&TaskFilter::default()).unwrap();
                prop_assert_eq!(all.len(), unique_ids.len());
                let got_ids: std::collections::HashSet<String> =
                    all.iter().map(|t| t.miroir_id.clone()).collect();
                prop_assert_eq!(got_ids, unique_ids);
            }

            /// Property: idempotency (insert, get) round-trip.
            #[test]
            fn idempotency_roundtrip(
                key in "[a-z0-9-]{1,32}",
                task_id in "[a-z0-9-]{1,32}",
                expires_at in 5000i64..1_000_000,
            ) {
                let store = test_store();
                let sha = vec![0xABu8; 32];
                store.insert_idempotency_entry(&IdempotencyEntry {
                    key: key.clone(),
                    body_sha256: sha.clone(),
                    miroir_task_id: task_id.clone(),
                    expires_at,
                }).unwrap();

                let got = store.get_idempotency_entry(&key).unwrap().unwrap();
                prop_assert_eq!(got.key, key);
                prop_assert_eq!(got.body_sha256, sha);
                prop_assert_eq!(got.miroir_task_id, task_id);
                prop_assert_eq!(got.expires_at, expires_at);
            }

            /// Property: canary (upsert, list) — all unique canaries visible.
            #[test]
            fn canary_upsert_list_roundtrip(
                ids in proptest::collection::vec("[a-z0-9-]{1,16}", 1..8),
            ) {
                let store = test_store();
                let unique_ids: std::collections::HashSet<String> = ids.into_iter().collect();
                for (i, id) in unique_ids.iter().enumerate() {
                    store.upsert_canary(&NewCanary {
                        id: id.clone(),
                        name: format!("canary-{i}"),
                        index_uid: "logs".to_string(),
                        interval_s: 60 + i as i64,
                        query_json: r#"{"q":"test"}"#.to_string(),
                        assertions_json: "[]".to_string(),
                        enabled: i % 2 == 0,
                        created_at: i as i64 * 1000,
                    }).unwrap();
                }

                let all = store.list_canaries().unwrap();
                prop_assert_eq!(all.len(), unique_ids.len());
            }

            /// Property: rollover_policy (upsert, list) round-trip.
            #[test]
            fn rollover_policy_upsert_list_roundtrip(
                names in proptest::collection::vec("[a-z0-9-]{1,16}", 1..6),
            ) {
                let store = test_store();
                let unique_names: std::collections::HashSet<String> = names.into_iter().collect();
                for (_i, name) in unique_names.iter().enumerate() {
                    store.upsert_rollover_policy(&NewRolloverPolicy {
                        name: name.clone(),
                        write_alias: format!("{name}-w"),
                        read_alias: format!("{name}-r"),
                        pattern: "logs-*".to_string(),
                        triggers_json: "{}".to_string(),
                        retention_json: "{}".to_string(),
                        template_json: "{}".to_string(),
                        enabled: true,
                    }).unwrap();
                }

                let all = store.list_rollover_policies().unwrap();
                prop_assert_eq!(all.len(), unique_names.len());
            }
        }
    }

    // --- Restart resilience test ---

    #[test]
    fn task_survives_store_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resilience.db");

        // Phase 1: open, migrate, insert a task
        {
            let store = SqliteTaskStore::open(&path).unwrap();
            store.migrate().unwrap();
            let mut nt = HashMap::new();
            nt.insert("node-0".to_string(), 42u64);
            store
                .insert_task(&NewTask {
                    miroir_id: "survivor-task".to_string(),
                    created_at: 1000,
                    status: "enqueued".to_string(),
                    node_tasks: nt,
                    error: None,
                    started_at: None,
                    finished_at: None,
                    index_uid: None,
                    task_type: None,
                    node_errors: HashMap::new(),
                })
                .unwrap();
            // Drop store — simulates pod shutdown
        }

        // Phase 2: reopen the same database file
        {
            let store = SqliteTaskStore::open(&path).unwrap();
            store.migrate().unwrap();

            // Task survives the close/reopen cycle
            let task = store.get_task("survivor-task").unwrap().unwrap();
            assert_eq!(task.miroir_id, "survivor-task");
            assert_eq!(task.status, "enqueued");
            assert_eq!(task.node_tasks.get("node-0"), Some(&42u64));

            // Can continue updating the task
            assert!(store.update_task_status("survivor-task", "processing").unwrap());
            assert!(store.set_task_error("survivor-task", "recovered").unwrap());

            let updated = store.get_task("survivor-task").unwrap().unwrap();
            assert_eq!(updated.status, "processing");
            assert_eq!(updated.error.as_deref(), Some("recovered"));
        }

        // Phase 3: reopen again and verify the update stuck
        {
            let store = SqliteTaskStore::open(&path).unwrap();
            store.migrate().unwrap();

            let task = store.get_task("survivor-task").unwrap().unwrap();
            assert_eq!(task.status, "processing");
            assert_eq!(task.error.as_deref(), Some("recovered"));
        }
    }

    #[test]
    fn all_tables_survive_store_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full-resilience.db");

        // Phase 1: populate all 14 tables
        {
            let store = SqliteTaskStore::open(&path).unwrap();
            store.migrate().unwrap();

            // Table 1: tasks
            store.insert_task(&NewTask {
                miroir_id: "task-r".to_string(),
                created_at: 1000,
                status: "enqueued".to_string(),
                node_tasks: HashMap::new(),
                error: None,
                started_at: None,
                finished_at: None,
                index_uid: None,
                task_type: None,
                node_errors: HashMap::new(),
            }).unwrap();

            // Table 2: node_settings_version
            store.upsert_node_settings_version("idx-r", "node-r", 5, 1000).unwrap();

            // Table 3: aliases
            store.create_alias(&NewAlias {
                name: "alias-r".to_string(),
                kind: "single".to_string(),
                current_uid: Some("uid-v1".to_string()),
                target_uids: None,
                version: 1,
                created_at: 1000,
                history: vec![],
            }).unwrap();

            // Table 4: sessions
            store.upsert_session(&SessionRow {
                session_id: "sess-r".to_string(),
                last_write_mtask_id: None,
                last_write_at: None,
                pinned_group: None,
                min_settings_version: 1,
                ttl: 100000,
            }).unwrap();

            // Table 5: idempotency_cache
            store.insert_idempotency_entry(&IdempotencyEntry {
                key: "idemp-r".to_string(),
                body_sha256: vec![0; 32],
                miroir_task_id: "task-r".to_string(),
                expires_at: 100000,
            }).unwrap();

            // Table 6: jobs
            store.insert_job(&NewJob {
                id: "job-r".to_string(),
                type_: "test".to_string(),
                params: "{}".to_string(),
                state: "queued".to_string(),
                progress: "{}".to_string(),
            }).unwrap();

            // Table 7: leader_lease
            store.try_acquire_leader_lease("scope-r", "pod-r", 100000, 0).unwrap();

            // Table 8: canaries
            store.upsert_canary(&NewCanary {
                id: "canary-r".to_string(),
                name: "test-canary".to_string(),
                index_uid: "idx-r".to_string(),
                interval_s: 60,
                query_json: "{}".to_string(),
                assertions_json: "[]".to_string(),
                enabled: true,
                created_at: 1000,
            }).unwrap();

            // Table 9: canary_runs
            store.insert_canary_run(&NewCanaryRun {
                canary_id: "canary-r".to_string(),
                ran_at: 1000,
                status: "pass".to_string(),
                latency_ms: 50,
                failed_assertions_json: None,
            }, 100).unwrap();

            // Table 10: cdc_cursors
            store.upsert_cdc_cursor(&NewCdcCursor {
                sink_name: "sink-r".to_string(),
                index_uid: "idx-r".to_string(),
                last_event_seq: 42,
                updated_at: 1000,
            }).unwrap();

            // Table 11: tenant_map
            store.insert_tenant_mapping(&NewTenantMapping {
                api_key_hash: vec![1u8; 32],
                tenant_id: "tenant-r".to_string(),
                group_id: Some(2),
            }).unwrap();

            // Table 12: rollover_policies
            store.upsert_rollover_policy(&NewRolloverPolicy {
                name: "policy-r".to_string(),
                write_alias: "w-r".to_string(),
                read_alias: "r-r".to_string(),
                pattern: "p-r".to_string(),
                triggers_json: "{}".to_string(),
                retention_json: "{}".to_string(),
                template_json: "{}".to_string(),
                enabled: true,
            }).unwrap();

            // Table 13: search_ui_config
            store.upsert_search_ui_config(&NewSearchUiConfig {
                index_uid: "idx-r".to_string(),
                config_json: "{}".to_string(),
                updated_at: 1000,
            }).unwrap();

            // Table 14: admin_sessions
            store.insert_admin_session(&NewAdminSession {
                session_id: "admin-r".to_string(),
                csrf_token: "csrf-r".to_string(),
                admin_key_hash: "hash-r".to_string(),
                created_at: 1000,
                expires_at: 100000,
                user_agent: None,
                source_ip: None,
            }).unwrap();
        }

        // Phase 2: reopen and verify all 14 tables
        {
            let store = SqliteTaskStore::open(&path).unwrap();
            store.migrate().unwrap();

            assert!(store.get_task("task-r").unwrap().is_some());
            assert!(store.get_node_settings_version("idx-r", "node-r").unwrap().is_some());
            assert!(store.get_alias("alias-r").unwrap().is_some());
            assert!(store.get_session("sess-r").unwrap().is_some());
            assert!(store.get_idempotency_entry("idemp-r").unwrap().is_some());
            assert!(store.get_job("job-r").unwrap().is_some());
            assert!(store.get_leader_lease("scope-r").unwrap().is_some());
            assert!(store.get_canary("canary-r").unwrap().is_some());
            assert_eq!(store.get_canary_runs("canary-r", 10).unwrap().len(), 1);
            assert!(store.get_cdc_cursor("sink-r", "idx-r").unwrap().is_some());
            assert!(store.get_tenant_mapping(&vec![1u8; 32]).unwrap().is_some());
            assert!(store.get_rollover_policy("policy-r").unwrap().is_some());
            assert!(store.get_search_ui_config("idx-r").unwrap().is_some());
            assert!(store.get_admin_session("admin-r").unwrap().is_some());
        }
    }
}
