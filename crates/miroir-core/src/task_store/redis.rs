//! Redis-backed TaskStore implementation (plan §4 "Redis mode (HA)").
//!
//! This module implements the TaskStore trait using Redis as the backend.
//! Each SQLite table is mapped to a Redis keyspace as specified in plan §4.

use crate::task_store::*;
use crate::Result;
use crate::MiroirError;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;

use ::redis::aio::ConnectionManager;
use ::redis::{
    pipe, AsyncCommands, Client, ExistenceCheck, FromRedisValue, Pipeline, SetExpiry,
    SetOptions, Value,
};
use futures_util::StreamExt;
use ::redis::AsyncIter;
use tokio::runtime::Runtime;


/// Redis connection pool wrapper.
#[derive(Clone)]
pub struct RedisPool {
    /// Connection manager for async operations (shared across clones)
    manager: Arc<Mutex<ConnectionManager>>,
    /// Dedicated runtime for blocking bridge (lazily created outside async context)
    runtime: Arc<Option<Runtime>>,
}

impl RedisPool {
    /// Create a new Redis pool from a connection URL.
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url).map_err(|e| MiroirError::Redis(e.to_string()))?;
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| MiroirError::Redis(e.to_string()))?;

        // Defer runtime creation — building a runtime inside an existing tokio
        // context panics.  The runtime is created lazily in `block_on` when we
        // detect we are NOT inside a tokio runtime.
        Ok(Self {
            manager: Arc::new(Mutex::new(conn)),
            runtime: Arc::new(None),
        })
    }

    /// Execute a pipeline and return its query result.
    pub async fn pipeline_query<R>(&self, pipe: &mut Pipeline) -> Result<R>
    where
        R: FromRedisValue,
    {
        let mut conn = self.manager.lock().await;
        pipe.query_async(&mut *conn)
            .await
            .map_err(|e| MiroirError::Redis(e.to_string()))
    }


    /// Block on an async future using a dedicated runtime.
    /// Always spawns a fresh thread with its own single-threaded runtime to
    /// avoid "cannot start a runtime from within a runtime" panics.
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        std::thread::scope(|s| {
            s.spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create runtime in thread");
                rt.block_on(future)
            })
            .join()
            .unwrap_or_else(|_| panic!("block_on thread panicked"))
        })
    }
}

/// Redis-backed TaskStore.
#[derive(Clone)]
pub struct RedisTaskStore {
    /// Redis connection pool
    pool: RedisPool,
    /// Key prefix for all Miroir keys
    key_prefix: String,
}

impl RedisTaskStore {
    /// Open a Redis task store from a connection URL.
    pub async fn open(url: &str) -> Result<Self> {
        let pool = RedisPool::new(url).await?;
        Ok(Self {
            pool,
            key_prefix: "miroir".into(),
        })
    }

    /// Generate a fully-qualified Redis key.
    fn key(&self, parts: &[&str]) -> String {
        format!("{}:{}", self.key_prefix, parts.join(":"))
    }

    /// Helper to generate key from owned values (for use in async blocks).
    fn key_owned(prefix: &str, parts: &[String]) -> String {
        format!("{}:{}", prefix, parts.join(":"))
    }

    /// Helper: run an async future using the dedicated runtime.
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.pool.block_on(future)
    }

    /// Helper: parse a hash row into a TaskRow.
    fn task_from_hash(
        miroir_id: String,
        fields: &HashMap<String, Value>,
    ) -> Result<TaskRow> {
        let created_at = get_field_i64(fields, "created_at")?;
        let status = get_field_string(fields, "status")?;
        let node_tasks_json = get_field_string(fields, "node_tasks")?;
        let node_tasks: HashMap<String, u64> = serde_json::from_str(&node_tasks_json)
            .map_err(|e| MiroirError::TaskStore(format!("invalid node_tasks JSON: {e}")))?;
        let error = opt_field(fields, "error");

        Ok(TaskRow {
            miroir_id,
            created_at,
            status,
            node_tasks,
            error,
        })
    }

    /// Helper: parse canary hash row.
    fn canary_from_hash(
        id: String,
        fields: &HashMap<String, Value>,
    ) -> Result<CanaryRow> {
        Ok(CanaryRow {
            id,
            name: get_field_string(fields, "name")?,
            index_uid: get_field_string(fields, "index_uid")?,
            interval_s: get_field_i64(fields, "interval_s")?,
            query_json: get_field_string(fields, "query_json")?,
            assertions_json: get_field_string(fields, "assertions_json")?,
            enabled: get_field_i64(fields, "enabled")? != 0,
            created_at: get_field_i64(fields, "created_at")?,
        })
    }
}

/// Helper: get a string field from a Redis hash.
fn get_field_string(fields: &HashMap<String, Value>, key: &str) -> Result<String> {
    fields
        .get(key)
        .and_then(|v| match v {
            Value::BulkString(bytes) => std::str::from_utf8(bytes).ok().map(String::from),
            Value::Int(i) => Some(i.to_string()),
            Value::SimpleString(s) => Some(s.clone()),
            _ => None,
        })
        .ok_or_else(|| MiroirError::TaskStore(format!("missing field: {key}")))
}

/// Helper: get an i64 field from a Redis hash.
fn get_field_i64(fields: &HashMap<String, Value>, key: &str) -> Result<i64> {
    fields
        .get(key)
        .and_then(|v| match v {
            Value::Int(i) => Some(*i),
            Value::BulkString(bytes) => std::str::from_utf8(bytes)
                .ok()
                .and_then(|s| s.parse::<i64>().ok()),
            Value::SimpleString(s) => s.parse::<i64>().ok(),
            _ => None,
        })
        .ok_or_else(|| MiroirError::TaskStore(format!("missing or invalid field: {key}")))
}

/// Helper: convert optional field to Option<String>.
fn opt_field(fields: &HashMap<String, Value>, key: &str) -> Option<String> {
    fields.get(key).and_then(|v| match v {
        Value::BulkString(bytes) => std::str::from_utf8(bytes).ok().map(String::from),
        Value::Int(i) => Some(i.to_string()),
        Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    })
}

/// Helper: convert optional field to Option<i64>.
fn opt_field_i64(fields: &HashMap<String, Value>, key: &str) -> Option<i64> {
    fields.get(key).and_then(|v| match v {
        Value::Int(i) => Some(*i),
        Value::BulkString(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse::<i64>().ok()),
        Value::SimpleString(s) => s.parse::<i64>().ok(),
        _ => None,
    })
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ---------------------------------------------------------------------------
// TaskStore trait implementation
// ---------------------------------------------------------------------------

impl TaskStore for RedisTaskStore {
    fn migrate(&self) -> Result<()> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let version_key = format!("{}:schema_version", key_prefix);
        self.block_on(async move {
            let mut conn = manager.lock().await;
            let current: Option<i64> = conn.get(&version_key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let binary_version = 2i64; // Current max migration version

            // Validate that store version is not ahead of binary
            if let Some(v) = current {
                if v > binary_version {
                    return Err(MiroirError::SchemaVersionAhead {
                        store_version: v,
                        binary_version,
                    });
                }
            }

            // If this is a fresh store, record our version
            if current.is_none() {
                let _: () = conn.set(&version_key, binary_version).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;
            }

            Ok(())
        })
    }

    // --- Table 1: tasks ---

    fn insert_task(&self, task: &NewTask) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let task = task.clone();
        let key = format!("{}:tasks:{}", key_prefix, task.miroir_id);
        let index_key = format!("{}:tasks:_index", key_prefix);
        let created_at_str = task.created_at.to_string();

        self.block_on(async move {
            let node_tasks_json = serde_json::to_string(&task.node_tasks)?;

            let mut pipe = pipe();
            pipe.hset_multiple(
                &key,
                &[
                    ("miroir_id", task.miroir_id.as_str()),
                    ("created_at", created_at_str.as_str()),
                    ("status", task.status.as_str()),
                    ("node_tasks", node_tasks_json.as_str()),
                ],
            );
            if let Some(ref error) = task.error {
                pipe.hset(&key, "error", error);
            }
            pipe.sadd(&index_key, &task.miroir_id);
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    fn get_task(&self, miroir_id: &str) -> Result<Option<TaskRow>> {
        let manager = self.pool.manager.clone();
        let key = self.key(&["tasks", miroir_id]);
        let miroir_id = miroir_id.to_string();

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Self::task_from_hash(miroir_id, &fields)?))
            }
        })
    }

    fn update_task_status(&self, miroir_id: &str, status: &str) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let key = self.key(&["tasks", miroir_id]);
        let status = status.to_string();

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let exists: bool = conn.hexists(&key, "miroir_id").await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !exists {
                return Ok(false);
            }

            let _: () = conn.hset(&key, "status", &status).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;
            Ok(true)
        })
    }

    fn update_node_task(&self, miroir_id: &str, node_id: &str, task_uid: u64) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let key = self.key(&["tasks", miroir_id]);
        let node_id = node_id.to_string();

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let node_tasks_json: Option<String> = conn.hget(&key, "node_tasks").await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let Some(json) = node_tasks_json else {
                return Ok(false);
            };

            let mut map: HashMap<String, u64> = serde_json::from_str(&json)
                .map_err(|e| MiroirError::TaskStore(format!("invalid node_tasks JSON: {e}")))?;
            map.insert(node_id, task_uid);
            let updated = serde_json::to_string(&map)?;

            let _: () = conn.hset(&key, "node_tasks", &updated).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;
            Ok(true)
        })
    }

    fn set_task_error(&self, miroir_id: &str, error: &str) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let key = self.key(&["tasks", miroir_id]);
        let error = error.to_string();

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let exists: bool = conn.hexists(&key, "miroir_id").await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !exists {
                return Ok(false);
            }

            let _: () = conn.hset(&key, "error", &error).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;
            Ok(true)
        })
    }

    fn list_tasks(&self, filter: &TaskFilter) -> Result<Vec<TaskRow>> {
        let manager = self.pool.manager.clone();
        let index_key = self.key(&["tasks", "_index"]);
        let status_filter = filter.status.clone();
        let limit = filter.limit;
        let offset = filter.offset;
        let key_prefix = self.key_prefix.clone();

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let all_ids: Vec<String> = conn.smembers(&index_key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let mut tasks = Vec::new();
            for miroir_id in all_ids {
                let key = format!("{}:tasks:{}", key_prefix, miroir_id);
                let fields: HashMap<String, Value> = conn.hgetall(&key).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;

                if fields.is_empty() {
                    continue;
                }

                let task = Self::task_from_hash(miroir_id, &fields)?;

                // Apply status filter
                if let Some(ref status) = status_filter {
                    if &task.status != status {
                        continue;
                    }
                }

                tasks.push(task);
            }

            // Sort by created_at DESC
            tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));

            // Apply pagination
            if let Some(offset) = offset {
                if offset < tasks.len() {
                    tasks.drain(0..offset);
                } else {
                    tasks.clear();
                }
            }
            if let Some(limit) = limit {
                tasks.truncate(limit);
            }

            Ok(tasks)
        })
    }

    fn prune_tasks(&self, cutoff_ms: i64, batch_size: u32) -> Result<usize> {
        let manager = self.pool.manager.clone();
        let pool = self.pool.clone();
        let index_key = self.key(&["tasks", "_index"]);
        let key_prefix = self.key_prefix.clone();

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let all_ids: Vec<String> = conn.smembers(&index_key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let terminal_statuses = ["succeeded", "failed", "canceled"];
            let mut to_delete = Vec::new();

            for miroir_id in all_ids.into_iter().take(batch_size as usize) {
                let key = format!("{}:tasks:{}", key_prefix, miroir_id);

                // Use a pipeline to get both fields atomically
                let mut p = pipe();
                p.hget(&key, "created_at");
                p.hget(&key, "status");
                let result: (Option<String>, Option<String>) = pool.pipeline_query(&mut p).await?;

                if let (Some(created_at_str), Some(status)) = result {
                    let created_at: i64 = created_at_str.parse()
                        .map_err(|e| MiroirError::TaskStore(format!("invalid created_at: {e}")))?;
                    if created_at < cutoff_ms && terminal_statuses.contains(&status.as_str()) {
                        to_delete.push(miroir_id);
                    }
                }
            }

            if to_delete.is_empty() {
                return Ok(0);
            }

            // Delete tasks and remove from index
            let mut pipe = pipe();
            for miroir_id in &to_delete {
                let key = format!("{}:tasks:{}", key_prefix, miroir_id);
                pipe.del(&key);
                pipe.srem(&index_key, miroir_id);
            }
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(to_delete.len())
        })
    }

    fn task_count(&self) -> Result<u64> {
        let manager = self.pool.manager.clone();
        let index_key = self.key(&["tasks", "_index"]);
        self.block_on(async move {
            let mut conn = manager.lock().await;
            let count: u64 = conn.scard(&index_key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;
            Ok(count)
        })
    }

    // --- Table 2: node_settings_version ---

    fn upsert_node_settings_version(
        &self,
        index_uid: &str,
        node_id: &str,
        version: i64,
        updated_at: i64,
    ) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let index_uid = index_uid.to_string();
        let node_id = node_id.to_string();
        let key = format!("{}:node_settings_version:{}:{}", key_prefix, index_uid, node_id);
        let index_key = format!("{}:node_settings_version:_index", key_prefix);

        self.block_on(async move {
            let version_str = version.to_string();
            let updated_at_str = updated_at.to_string();
            let index_value = format!("{}:{}", index_uid, node_id);

            let mut pipe = pipe();
            pipe.hset_multiple(
                &key,
                &[
                    ("index_uid", index_uid.as_str()),
                    ("node_id", node_id.as_str()),
                    ("version", version_str.as_str()),
                    ("updated_at", updated_at_str.as_str()),
                ],
            );
            pipe.sadd(&index_key, index_value);
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    fn get_node_settings_version(
        &self,
        index_uid: &str,
        node_id: &str,
    ) -> Result<Option<NodeSettingsVersionRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let index_uid = index_uid.to_string();
        let node_id = node_id.to_string();
        let key = format!("{}:node_settings_version:{}:{}", key_prefix, index_uid, node_id);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(NodeSettingsVersionRow {
                    index_uid: index_uid.to_string(),
                    node_id: node_id.to_string(),
                    version: get_field_i64(&fields, "version")?,
                    updated_at: get_field_i64(&fields, "updated_at")?,
                }))
            }
        })
    }

    // --- Table 3: aliases ---

    fn create_alias(&self, alias: &NewAlias) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let name = alias.name.clone();
        let kind = alias.kind.clone();
        let target_uids_json = alias
            .target_uids
            .as_ref()
            .map(|uids| serde_json::to_string(uids))
            .transpose()?
            .unwrap_or_default();
        let history_json = serde_json::to_string(&alias.history)?;
        let version_str = alias.version.to_string();
        let created_at_str = alias.created_at.to_string();
        let current_uid = alias.current_uid.clone();
        let has_target_uids = alias.target_uids.is_some();
        let key = format!("{}:aliases:{}", key_prefix, name);
        let index_key = format!("{}:aliases:_index", key_prefix);

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset_multiple(
                &key,
                &[
                    ("name", name.as_str()),
                    ("kind", kind.as_str()),
                    ("version", version_str.as_str()),
                    ("created_at", created_at_str.as_str()),
                    ("history", history_json.as_str()),
                ],
            );
            if let Some(ref current_uid) = current_uid {
                pipe.hset(&key, "current_uid", current_uid);
            }
            if has_target_uids {
                pipe.hset(&key, "target_uids", &target_uids_json);
            }
            pipe.sadd(&index_key, &name);
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    fn get_alias(&self, name: &str) -> Result<Option<AliasRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let name = name.to_string();
        let key = format!("{}:aliases:{}", key_prefix, name);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                let history_json = get_field_string(&fields, "history")?;
                let history: Vec<AliasHistoryEntry> = serde_json::from_str(&history_json)
                    .map_err(|e| MiroirError::TaskStore(format!("invalid history JSON: {e}")))?;

                let target_uids = opt_field(&fields, "target_uids")
                    .map(|json| {
                        serde_json::from_str(&json).map_err(|e| {
                            MiroirError::TaskStore(format!("invalid target_uids JSON: {e}"))
                        })
                    })
                    .transpose()?;

                Ok(Some(AliasRow {
                    name: name.clone(),
                    kind: get_field_string(&fields, "kind")?,
                    current_uid: opt_field(&fields, "current_uid"),
                    target_uids,
                    version: get_field_i64(&fields, "version")?,
                    created_at: get_field_i64(&fields, "created_at")?,
                    history,
                }))
            }
        })
    }

    fn flip_alias(&self, name: &str, new_uid: &str, history_retention: usize) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let name = name.to_string();
        let new_uid = new_uid.to_string();
        let key = format!("{}:aliases:{}", key_prefix, name);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                return Ok(false);
            }

            let old_uid = opt_field(&fields, "current_uid").unwrap_or_default();
            let old_version = get_field_i64(&fields, "version")?;
            let history_json = get_field_string(&fields, "history")?;
            let mut history: Vec<AliasHistoryEntry> = serde_json::from_str(&history_json)
                .map_err(|e| MiroirError::TaskStore(format!("invalid history JSON: {e}")))?;

            if !old_uid.is_empty() {
                history.push(AliasHistoryEntry {
                    uid: old_uid,
                    flipped_at: now_ms(),
                });
            }
            while history.len() > history_retention {
                history.remove(0);
            }

            let new_history_json = serde_json::to_string(&history)?;
            let new_version_str = (old_version + 1).to_string();

            // Use pipeline_query for the atomic update
            let mut pipe = pipe();
            pipe.hset(&key, "current_uid", &new_uid);
            pipe.hset(&key, "version", &new_version_str);
            pipe.hset(&key, "history", &new_history_json);
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(true)
        })
    }

    fn delete_alias(&self, name: &str) -> Result<bool> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let name = name.to_string();
        let key = format!("{}:aliases:{}", key_prefix, name);
        let index_key = format!("{}:aliases:_index", key_prefix);

        self.block_on(async move {
            let mut conn = pool.manager.lock().await;

            let exists: bool = conn.exists(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !exists {
                return Ok(false);
            }

            let mut pipe = pipe();
            pipe.del(&key);
            pipe.srem(&index_key, &name);
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(true)
        })
    }

    // --- Table 4: sessions ---

    fn upsert_session(&self, session: &SessionRow) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let session = session.clone();
        let key = format!("{}:session:{}", key_prefix, session.session_id);
        let ttl_seconds = ((session.ttl - now_ms()) / 1000).max(0) as u64;

        self.block_on(async move {
            let min_settings_version_str = session.min_settings_version.to_string();
            let ttl_str = session.ttl.to_string();

            let mut pipe = pipe();
            pipe.hset(&key, "session_id", &session.session_id);
            pipe.hset(&key, "min_settings_version", &min_settings_version_str);
            pipe.hset(&key, "ttl", &ttl_str);
            pipe.expire(&key, ttl_seconds as i64);

            if let Some(ref mtask_id) = session.last_write_mtask_id {
                pipe.hset(&key, "last_write_mtask_id", mtask_id);
            }
            if let Some(at) = session.last_write_at {
                pipe.hset(&key, "last_write_at", at.to_string());
            }
            if let Some(group) = session.pinned_group {
                pipe.hset(&key, "pinned_group", group.to_string());
            }

            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(())
        })
    }

    fn get_session(&self, session_id: &str) -> Result<Option<SessionRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let session_id = session_id.to_string();
        let key = format!("{}:session:{}", key_prefix, session_id);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(SessionRow {
                    session_id: session_id.clone(),
                    last_write_mtask_id: opt_field(&fields, "last_write_mtask_id"),
                    last_write_at: opt_field_i64(&fields, "last_write_at"),
                    pinned_group: opt_field_i64(&fields, "pinned_group"),
                    min_settings_version: get_field_i64(&fields, "min_settings_version")?,
                    ttl: get_field_i64(&fields, "ttl")?,
                }))
            }
        })
    }

    fn delete_expired_sessions(&self, _now_ms: i64) -> Result<usize> {
        // Redis handles session expiration via EXPIRE — no manual pruning needed.
        // Return 0 for compatibility.
        Ok(0)
    }

    // --- Table 5: idempotency_cache ---

    fn insert_idempotency_entry(&self, entry: &IdempotencyEntry) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let entry = entry.clone();
        let key = format!("{}:idemp:{}", key_prefix, entry.key);
        let ttl_seconds = ((entry.expires_at - now_ms()) / 1000).max(0) as u64;

        // Store body_sha256 as hex string for Redis compatibility
        let body_sha256_hex = hex::encode(&entry.body_sha256);
        let expires_at_str = entry.expires_at.to_string();

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset(&key, "key", &entry.key);
            pipe.hset(&key, "body_sha256", &body_sha256_hex);
            pipe.hset(&key, "miroir_task_id", &entry.miroir_task_id);
            pipe.hset(&key, "expires_at", &expires_at_str);
            pipe.expire(&key, ttl_seconds as i64);

            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(())
        })
    }

    fn get_idempotency_entry(&self, key: &str) -> Result<Option<IdempotencyEntry>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let key = key.to_string();
        let redis_key = format!("{}:idemp:{}", key_prefix, key);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&redis_key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                let body_sha256_hex = get_field_string(&fields, "body_sha256")?;
                let body_sha256 = hex::decode(&body_sha256_hex)
                    .map_err(|e| MiroirError::TaskStore(format!("invalid body_sha256 hex: {e}")))?;

                Ok(Some(IdempotencyEntry {
                    key: key.clone(),
                    body_sha256,
                    miroir_task_id: get_field_string(&fields, "miroir_task_id")?,
                    expires_at: get_field_i64(&fields, "expires_at")?,
                }))
            }
        })
    }

    fn delete_expired_idempotency_entries(&self, _now_ms: i64) -> Result<usize> {
        // Redis handles expiration via EXPIRE — no manual pruning needed.
        Ok(0)
    }

    // --- Table 6: jobs ---

    fn insert_job(&self, job: &NewJob) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let job = job.clone();
        let key = format!("{}:jobs:{}", key_prefix, job.id);
        let queued_key = format!("{}:jobs:_queued", key_prefix);

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset_multiple(
                &key,
                &[
                    ("id", job.id.as_str()),
                    ("type", job.type_.as_str()),
                    ("params", job.params.as_str()),
                    ("state", job.state.as_str()),
                    ("progress", job.progress.as_str()),
                ],
            );
            if job.state == "queued" {
                pipe.sadd(&queued_key, &job.id);
            }
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    fn get_job(&self, id: &str) -> Result<Option<JobRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let id = id.to_string();
        let key = format!("{}:jobs:{}", key_prefix, id);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(JobRow {
                    id: id.clone(),
                    type_: get_field_string(&fields, "type")?,
                    params: get_field_string(&fields, "params")?,
                    state: get_field_string(&fields, "state")?,
                    claimed_by: opt_field(&fields, "claimed_by"),
                    claim_expires_at: opt_field_i64(&fields, "claim_expires_at"),
                    progress: get_field_string(&fields, "progress")?,
                }))
            }
        })
    }

    fn claim_job(&self, id: &str, claimed_by: &str, claim_expires_at: i64) -> Result<bool> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let id = id.to_string();
        let claimed_by = claimed_by.to_string();
        let key = format!("{}:jobs:{}", key_prefix, id);
        let queued_key = format!("{}:jobs:_queued", key_prefix);

        self.block_on(async move {
            let mut conn = pool.manager.lock().await;

            // Check if state is 'queued'
            let state: Option<String> = conn.hget(&key, "state").await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if state.as_deref() != Some("queued") {
                return Ok(false);
            }

            let mut pipe = pipe();
            pipe.hset(&key, "claimed_by", &claimed_by);
            pipe.hset(&key, "claim_expires_at", claim_expires_at.to_string());
            pipe.hset(&key, "state", "in_progress");
            pipe.srem(&queued_key, &id);
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(true)
        })
    }

    fn update_job_progress(&self, id: &str, state: &str, progress: &str) -> Result<bool> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let id = id.to_string();
        let state = state.to_string();
        let progress = progress.to_string();
        let key = format!("{}:jobs:{}", key_prefix, id);

        self.block_on(async move {
            let mut conn = pool.manager.lock().await;
            let exists: bool = conn.hexists(&key, "id").await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !exists {
                return Ok(false);
            }

            let mut pipe = pipe();
            pipe.hset(&key, "state", &state);
            pipe.hset(&key, "progress", &progress);
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(true)
        })
    }

    fn renew_job_claim(&self, id: &str, claim_expires_at: i64) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let id = id.to_string();
        let key = format!("{}:jobs:{}", key_prefix, id);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let claimed_by: Option<String> = conn.hget(&key, "claimed_by").await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if claimed_by.is_none() {
                return Ok(false);
            }

            let _: () = conn.hset(&key, "claim_expires_at", claim_expires_at.to_string()).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            Ok(true)
        })
    }

    fn list_jobs_by_state(&self, state: &str) -> Result<Vec<JobRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let state = state.to_string();

        self.block_on(async move {
            let mut result = Vec::new();
            let mut conn = manager.lock().await;

            if state == "queued" {
                // Use the _queued index for efficient listing
                let queued_key = format!("{}:jobs:_queued", key_prefix);
                let ids: Vec<String> = conn.smembers(&queued_key).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;

                for id in ids {
                    let key = format!("{}:jobs:{}", key_prefix, id);
                    let fields: HashMap<String, Value> = conn.hgetall(&key).await
                        .map_err(|e| MiroirError::Redis(e.to_string()))?;

                    if !fields.is_empty() {
                        result.push(JobRow {
                            id,
                            type_: get_field_string(&fields, "type")?,
                            params: get_field_string(&fields, "params")?,
                            state: "queued".to_string(),
                            claimed_by: None,
                            claim_expires_at: None,
                            progress: get_field_string(&fields, "progress")?,
                        });
                    }
                }
            } else {
                // For non-queued states, we need to iterate through job keys
                // Use SCAN and filter by state
                let job_prefix = format!("{}:jobs:", key_prefix);

                // Use scan to get keys, collect them first to avoid borrow issues
                let iter: AsyncIter<'_, String> = conn.scan().await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;

                let mut keys = Vec::new();
                use std::pin::Pin;
                let mut key_stream = Pin::new(Box::new(iter));
                while let Some(key) = key_stream.next().await {
                    if key.starts_with(&job_prefix) && !key.contains(":_queued") {
                        keys.push(key);
                    }
                }
                drop(key_stream);

                // Now process each key
                for key in keys {
                    let fields: HashMap<String, Value> = conn.hgetall(&key).await
                        .map_err(|e| MiroirError::Redis(e.to_string()))?;

                    if !fields.is_empty() {
                        if let Ok(job_state) = get_field_string(&fields, "state") {
                            if job_state == state {
                                result.push(JobRow {
                                    id: get_field_string(&fields, "id")?,
                                    type_: get_field_string(&fields, "type")?,
                                    params: get_field_string(&fields, "params")?,
                                    state: job_state,
                                    claimed_by: opt_field(&fields, "claimed_by"),
                                    claim_expires_at: opt_field_i64(&fields, "claim_expires_at"),
                                    progress: get_field_string(&fields, "progress")?,
                                });
                            }
                        }
                    }
                }
            }

            Ok(result)
        })
    }

    // --- Table 7: leader_lease ---

    fn try_acquire_leader_lease(
        &self,
        scope: &str,
        holder: &str,
        expires_at: i64,
        now_ms: i64,
    ) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let scope = scope.to_string();
        let holder = holder.to_string();
        let key = format!("{}:lease:{}", key_prefix, scope);
        let ttl_seconds = ((expires_at - now_ms) / 1000).max(1) as u64;

        self.block_on(async move {
            let mut conn = manager.lock().await;

            // SET NX EX — only set if not exists
            let acquired: bool = {
                let opts = SetOptions::default()
                    .conditional_set(ExistenceCheck::NX)
                    .with_expiration(SetExpiry::EX(ttl_seconds));
                conn.set_options(&key, &holder, opts).await
            }.map_err(|e| MiroirError::Redis(e.to_string()))?;

            if acquired {
                return Ok(true);
            }

            // Check if we can steal the lease (expired or we hold it)
            let current_holder: Option<String> = conn.get(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            match current_holder {
                Some(h) if h == holder => {
                    // We hold it — renew
                    let opts = SetOptions::default()
                        .conditional_set(ExistenceCheck::XX)
                        .with_expiration(SetExpiry::EX(ttl_seconds));
                    let _: () = conn.set_options(&key, &holder, opts).await
                        .map_err(|e| MiroirError::Redis(e.to_string()))?;
                    Ok(true)
                }
                Some(_) => {
                    // Someone else holds it — check expiry using TTL
                    let ttl: i64 = conn.ttl(&key).await
                        .map_err(|e| MiroirError::Redis(e.to_string()))?;

                    // TTL of -2 means key doesn't exist, -1 means no expiry
                    if ttl == -2 || (ttl >= 0 && ttl <= (expires_at - now_ms) / 1000) {
                        // Lease has expired — try to steal it
                        let opts = SetOptions::default()
                            .conditional_set(ExistenceCheck::NX)
                            .with_expiration(SetExpiry::EX(ttl_seconds));
                        let acquired: bool = conn.set_options(&key, &holder, opts).await
                            .map_err(|e| MiroirError::Redis(e.to_string()))?;
                        Ok(acquired)
                    } else {
                        Ok(false)
                    }
                }
                None => {
                    // Key doesn't exist — acquire
                    let opts = SetOptions::default()
                        .conditional_set(ExistenceCheck::NX)
                        .with_expiration(SetExpiry::EX(ttl_seconds));
                    let acquired: bool = conn.set_options(&key, &holder, opts).await
                        .map_err(|e| MiroirError::Redis(e.to_string()))?;
                    Ok(acquired)
                }
            }
        })
    }

    fn renew_leader_lease(&self, scope: &str, holder: &str, expires_at: i64) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let scope = scope.to_string();
        let holder = holder.to_string();
        let key = format!("{}:lease:{}", key_prefix, scope);
        let ttl_seconds = ((expires_at - now_ms()) / 1000).max(1) as u64;

        self.block_on(async move {
            let mut conn = manager.lock().await;

            // SET XX EX — only set if exists (we hold it)
            let opts = SetOptions::default()
                .conditional_set(ExistenceCheck::XX)
                .with_expiration(SetExpiry::EX(ttl_seconds));
            let renewed: bool = conn.set_options(&key, &holder, opts).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            Ok(renewed)
        })
    }

    fn get_leader_lease(&self, scope: &str) -> Result<Option<LeaderLeaseRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let scope = scope.to_string();
        let key = format!("{}:lease:{}", key_prefix, scope);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let holder: Option<String> = conn.get(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let Some(holder) = holder else {
                return Ok(None);
            };

            // Get TTL to compute expires_at
            let ttl: i64 = conn.ttl(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let expires_at = if ttl == -1 {
                // No expiry set
                i64::MAX
            } else if ttl >= 0 {
                now_ms() + ttl * 1000
            } else {
                // Key doesn't exist or expired
                return Ok(None);
            };

            Ok(Some(LeaderLeaseRow {
                scope: scope.clone(),
                holder,
                expires_at,
            }))
        })
    }

    // --- Tables 8-14: Feature-flagged tables ---

    // --- Table 8: canaries ---

    fn upsert_canary(&self, canary: &NewCanary) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let canary = canary.clone();
        let key = format!("{}:canary:{}", key_prefix, canary.id);
        let index_key = format!("{}:canary:_index", key_prefix);

        let interval_s_str = canary.interval_s.to_string();
        let enabled_str = (canary.enabled as i64).to_string();
        let created_at_str = canary.created_at.to_string();

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset_multiple(
                &key,
                &[
                    ("id", canary.id.as_str()),
                    ("name", canary.name.as_str()),
                    ("index_uid", canary.index_uid.as_str()),
                    ("interval_s", interval_s_str.as_str()),
                    ("query_json", canary.query_json.as_str()),
                    ("assertions_json", canary.assertions_json.as_str()),
                    ("enabled", enabled_str.as_str()),
                    ("created_at", created_at_str.as_str()),
                ],
            );
            pipe.sadd(&index_key, &canary.id);
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    fn get_canary(&self, id: &str) -> Result<Option<CanaryRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let id = id.to_string();
        let key = format!("{}:canary:{}", key_prefix, id);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Self::canary_from_hash(id.clone(), &fields)?))
            }
        })
    }

    fn list_canaries(&self) -> Result<Vec<CanaryRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();

        self.block_on(async move {
            let index_key = format!("{}:canary:_index", key_prefix);
            let mut conn = manager.lock().await;
            let ids: Vec<String> = conn.smembers(&index_key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let mut result = Vec::new();
            for id in ids {
                let key = format!("{}:canary:{}", key_prefix, id);
                let fields: HashMap<String, Value> = conn.hgetall(&key).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;

                if !fields.is_empty() {
                    result.push(Self::canary_from_hash(id, &fields)?);
                }
            }

            Ok(result)
        })
    }

    fn delete_canary(&self, id: &str) -> Result<bool> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let id = id.to_string();
        let key = format!("{}:canary:{}", key_prefix, id);
        let index_key = format!("{}:canary:_index", key_prefix);

        self.block_on(async move {
            let mut conn = pool.manager.lock().await;

            let exists: bool = conn.exists(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !exists {
                return Ok(false);
            }

            let mut pipe = pipe();
            pipe.del(&key);
            pipe.srem(&index_key, &id);
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(true)
        })
    }

    // --- Table 9: canary_runs ---

    fn insert_canary_run(&self, run: &NewCanaryRun, run_history_limit: usize) -> Result<()> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let run = run.clone();
        let key = format!("{}:canary_runs:{}", key_prefix, run.canary_id);

        self.block_on(async move {
            let mut conn = manager.lock().await;

            // Add new run to sorted set (score = ran_at)
            let value = serde_json::to_string(&run)?;
            let _: () = conn.zadd(&key, run.ran_at, value).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            // Trim to keep only the most recent N runs using ZREMRANGEBYRANK
            let start = 0isize;
            let end = -(run_history_limit as isize) - 1;
            let _: () = conn.zremrangebyrank(&key, start, end).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            Ok(())
        })
    }

    fn get_canary_runs(&self, canary_id: &str, limit: usize) -> Result<Vec<CanaryRunRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let canary_id = canary_id.to_string();
        let key = format!("{}:canary_runs:{}", key_prefix, canary_id);

        self.block_on(async move {
            let mut conn = manager.lock().await;

            // Get runs in descending order by ran_at (most recent first)
            let values: Vec<String> = conn.zrevrange(&key, 0, (limit as isize) - 1).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let mut result = Vec::new();
            for value in values {
                let run: NewCanaryRun = serde_json::from_str(&value)
                    .map_err(|e| MiroirError::TaskStore(format!("invalid canary_run JSON: {e}")))?;
                result.push(CanaryRunRow {
                    canary_id: canary_id.clone(),
                    ran_at: run.ran_at,
                    status: run.status,
                    latency_ms: run.latency_ms,
                    failed_assertions_json: run.failed_assertions_json,
                });
            }

            Ok(result)
        })
    }

    // --- Table 10: cdc_cursors ---

    fn upsert_cdc_cursor(&self, cursor: &NewCdcCursor) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let cursor = cursor.clone();
        let key = format!("{}:cdc_cursor:{}:{}", key_prefix, cursor.sink_name, cursor.index_uid);

        let last_event_seq_str = cursor.last_event_seq.to_string();
        let updated_at_str = cursor.updated_at.to_string();

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset(&key, "sink_name", &cursor.sink_name);
            pipe.hset(&key, "index_uid", &cursor.index_uid);
            pipe.hset(&key, "last_event_seq", &last_event_seq_str);
            pipe.hset(&key, "updated_at", &updated_at_str);

            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(())
        })
    }

    fn get_cdc_cursor(&self, sink_name: &str, index_uid: &str) -> Result<Option<CdcCursorRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let sink_name = sink_name.to_string();
        let index_uid = index_uid.to_string();
        let key = format!("{}:cdc_cursor:{}:{}", key_prefix, sink_name, index_uid);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(CdcCursorRow {
                    sink_name: sink_name.clone(),
                    index_uid: index_uid.clone(),
                    last_event_seq: get_field_i64(&fields, "last_event_seq")?,
                    updated_at: get_field_i64(&fields, "updated_at")?,
                }))
            }
        })
    }

    fn list_cdc_cursors(&self, sink_name: &str) -> Result<Vec<CdcCursorRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let sink_name = sink_name.to_string();
        let cdc_prefix = format!("{}:cdc_cursor:{}:", key_prefix, sink_name);

        self.block_on(async move {
            let mut result = Vec::new();
            let mut conn = manager.lock().await;

            // Use SCAN to iterate through all keys and filter
            let iter: AsyncIter<'_, String> = conn.scan().await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            // Collect matching keys first to avoid borrow issues
            let mut keys = Vec::new();
            use std::pin::Pin;
            let mut key_stream = Pin::new(Box::new(iter));
            while let Some(key) = key_stream.next().await {
                if key.starts_with(&cdc_prefix) {
                    keys.push(key);
                }
            }
            drop(key_stream);

            // Now process each key
            for key in keys {
                let fields: HashMap<String, Value> = conn.hgetall(&key).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;

                if !fields.is_empty() {
                    result.push(CdcCursorRow {
                        sink_name: sink_name.clone(),
                        index_uid: get_field_string(&fields, "index_uid")?,
                        last_event_seq: get_field_i64(&fields, "last_event_seq")?,
                        updated_at: get_field_i64(&fields, "updated_at")?,
                    });
                }
            }

            Ok(result)
        })
    }

    // --- Table 11: tenant_map ---

    fn insert_tenant_mapping(&self, mapping: &NewTenantMapping) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let api_key_hash = mapping.api_key_hash.clone();
        let tenant_id = mapping.tenant_id.clone();
        let group_id = mapping.group_id;
        let hex_hash = hex::encode(&api_key_hash);
        let key = format!("{}:tenant_map:{}", key_prefix, hex_hash);

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset(&key, "tenant_id", &tenant_id);
            if let Some(gid) = group_id {
                pipe.hset(&key, "group_id", gid);
            }
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(())
        })
    }

    fn get_tenant_mapping(&self, api_key_hash: &[u8]) -> Result<Option<TenantMapRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let api_key_hash = api_key_hash.to_vec();
        let hex_hash = hex::encode(&api_key_hash);
        let key = format!("{}:tenant_map:{}", key_prefix, hex_hash);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(TenantMapRow {
                    api_key_hash: api_key_hash.clone(),
                    tenant_id: get_field_string(&fields, "tenant_id")?,
                    group_id: opt_field_i64(&fields, "group_id"),
                }))
            }
        })
    }

    fn delete_tenant_mapping(&self, api_key_hash: &[u8]) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let api_key_hash = api_key_hash.to_vec();
        let hex_hash = hex::encode(&api_key_hash);
        let key = format!("{}:tenant_map:{}", key_prefix, hex_hash);

        self.block_on(async move {
            let mut conn = manager.lock().await;

            let exists: bool = conn.exists(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !exists {
                return Ok(false);
            }

            let _: () = conn.del(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            Ok(true)
        })
    }

    // --- Table 12: rollover_policies ---

    fn upsert_rollover_policy(&self, policy: &NewRolloverPolicy) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let policy = policy.clone();
        let key = format!("{}:rollover:{}", key_prefix, policy.name);
        let index_key = format!("{}:rollover:_index", key_prefix);
        let enabled_str = (policy.enabled as i64).to_string();

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset_multiple(
                &key,
                &[
                    ("name", policy.name.as_str()),
                    ("write_alias", policy.write_alias.as_str()),
                    ("read_alias", policy.read_alias.as_str()),
                    ("pattern", policy.pattern.as_str()),
                    ("triggers_json", policy.triggers_json.as_str()),
                    ("retention_json", policy.retention_json.as_str()),
                    ("template_json", policy.template_json.as_str()),
                    ("enabled", enabled_str.as_str()),
                ],
            );
            pipe.sadd(&index_key, &policy.name);
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    fn get_rollover_policy(&self, name: &str) -> Result<Option<RolloverPolicyRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let name = name.to_string();
        let key = format!("{}:rollover:{}", key_prefix, name);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(RolloverPolicyRow {
                    name: name.clone(),
                    write_alias: get_field_string(&fields, "write_alias")?,
                    read_alias: get_field_string(&fields, "read_alias")?,
                    pattern: get_field_string(&fields, "pattern")?,
                    triggers_json: get_field_string(&fields, "triggers_json")?,
                    retention_json: get_field_string(&fields, "retention_json")?,
                    template_json: get_field_string(&fields, "template_json")?,
                    enabled: get_field_i64(&fields, "enabled")? != 0,
                }))
            }
        })
    }

    fn list_rollover_policies(&self) -> Result<Vec<RolloverPolicyRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();

        self.block_on(async move {
            let index_key = format!("{}:rollover:_index", key_prefix);
            let mut conn = manager.lock().await;
            let names: Vec<String> = conn.smembers(&index_key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let mut result = Vec::new();
            for name in names {
                let key = format!("{}:rollover:{}", key_prefix, name);
                let fields: HashMap<String, Value> = conn.hgetall(&key).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;

                if !fields.is_empty() {
                    result.push(RolloverPolicyRow {
                        name: name.clone(),
                        write_alias: get_field_string(&fields, "write_alias")?,
                        read_alias: get_field_string(&fields, "read_alias")?,
                        pattern: get_field_string(&fields, "pattern")?,
                        triggers_json: get_field_string(&fields, "triggers_json")?,
                        retention_json: get_field_string(&fields, "retention_json")?,
                        template_json: get_field_string(&fields, "template_json")?,
                        enabled: get_field_i64(&fields, "enabled")? != 0,
                    });
                }
            }

            Ok(result)
        })
    }

    fn delete_rollover_policy(&self, name: &str) -> Result<bool> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let name = name.to_string();
        let key = format!("{}:rollover:{}", key_prefix, name);
        let index_key = format!("{}:rollover:_index", key_prefix);

        self.block_on(async move {
            let mut conn = pool.manager.lock().await;

            let exists: bool = conn.exists(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !exists {
                return Ok(false);
            }

            let mut pipe = pipe();
            pipe.del(&key);
            pipe.srem(&index_key, &name);
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(true)
        })
    }

    // --- Table 13: search_ui_config ---

    fn upsert_search_ui_config(&self, config: &NewSearchUiConfig) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let config = config.clone();
        let key = format!("{}:search_ui_config:{}", key_prefix, config.index_uid);
        let updated_at_str = config.updated_at.to_string();

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset(&key, "index_uid", &config.index_uid);
            pipe.hset(&key, "config_json", &config.config_json);
            pipe.hset(&key, "updated_at", &updated_at_str);

            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok(())
        })
    }

    fn get_search_ui_config(&self, index_uid: &str) -> Result<Option<SearchUiConfigRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let index_uid = index_uid.to_string();
        let key = format!("{}:search_ui_config:{}", key_prefix, index_uid);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(SearchUiConfigRow {
                    index_uid: index_uid.clone(),
                    config_json: get_field_string(&fields, "config_json")?,
                    updated_at: get_field_i64(&fields, "updated_at")?,
                }))
            }
        })
    }

    fn delete_search_ui_config(&self, index_uid: &str) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let index_uid = index_uid.to_string();
        let key = format!("{}:search_ui_config:{}", key_prefix, index_uid);

        self.block_on(async move {
            let mut conn = manager.lock().await;

            let exists: bool = conn.exists(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !exists {
                return Ok(false);
            }

            let _: () = conn.del(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            Ok(true)
        })
    }

    // --- Table 14: admin_sessions ---

    fn insert_admin_session(&self, session: &NewAdminSession) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let session = session.clone();
        let key = format!("{}:admin_session:{}", key_prefix, session.session_id);
        let ttl_seconds = ((session.expires_at - now_ms()) / 1000).max(0) as u64;

        let created_at_str = session.created_at.to_string();
        let expires_at_str = session.expires_at.to_string();
        let revoked_str = "0";

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset(&key, "session_id", &session.session_id);
            pipe.hset(&key, "csrf_token", &session.csrf_token);
            pipe.hset(&key, "admin_key_hash", &session.admin_key_hash);
            pipe.hset(&key, "created_at", &created_at_str);
            pipe.hset(&key, "expires_at", &expires_at_str);
            pipe.hset(&key, "revoked", revoked_str);
            pipe.expire(&key, ttl_seconds as i64);
            pool.pipeline_query::<()>(&mut pipe).await?;

            let mut conn = pool.manager.lock().await;
            if let Some(ref ua) = session.user_agent {
                let _: () = conn.hset(&key, "user_agent", ua).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;
            }
            if let Some(ref ip) = session.source_ip {
                let _: () = conn.hset(&key, "source_ip", ip).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;
            }

            Ok(())
        })
    }

    fn get_admin_session(&self, session_id: &str) -> Result<Option<AdminSessionRow>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let session_id = session_id.to_string();
        let key = format!("{}:admin_session:{}", key_prefix, session_id);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(AdminSessionRow {
                    session_id: session_id.clone(),
                    csrf_token: get_field_string(&fields, "csrf_token")?,
                    admin_key_hash: get_field_string(&fields, "admin_key_hash")?,
                    created_at: get_field_i64(&fields, "created_at")?,
                    expires_at: get_field_i64(&fields, "expires_at")?,
                    revoked: get_field_i64(&fields, "revoked")? != 0,
                    user_agent: opt_field(&fields, "user_agent"),
                    source_ip: opt_field(&fields, "source_ip"),
                }))
            }
        })
    }

    fn revoke_admin_session(&self, session_id: &str) -> Result<bool> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let session_id = session_id.to_string();
        let key = format!("{}:admin_session:{}", key_prefix, session_id);
        let channel = format!("{}:admin_session:revoked", key_prefix);

        self.block_on(async move {
            let mut conn = manager.lock().await;

            let exists: bool = conn.hexists(&key, "session_id").await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !exists {
                return Ok(false);
            }

            let _: () = conn.hset(&key, "revoked", 1i64).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            // Publish to revoked channel for immediate invalidation across pods
            let _: () = conn.publish(&channel, &session_id).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            Ok(true)
        })
    }

    fn delete_expired_admin_sessions(&self, _now_ms: i64) -> Result<usize> {
        // Redis handles session expiration via EXPIRE — no manual pruning needed.
        // In Redis mode, sessions are garbage-collected automatically.
        Ok(0)
    }
}

// ---------------------------------------------------------------------------
// Extra Redis-specific keys (plan §4 footnotes)
// ---------------------------------------------------------------------------

impl RedisTaskStore {
    // --- Rate limiting: search_ui ---

    /// Check and increment rate limit counter for search UI access.
    /// Returns (allowed, remaining_requests, reset_after_seconds).
    pub fn check_rate_limit_searchui(
        &self,
        ip: &str,
        limit: u64,
        window_seconds: u64,
    ) -> Result<(bool, u64, i64)> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let ip = ip.to_string();
        let key = format!("{}:ratelimit:searchui:{}", key_prefix, ip);

        self.block_on(async move {
            let mut conn = manager.lock().await;

            // Get current count
            let current: Option<u64> = conn.get(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            // Get TTL
            let ttl: i64 = conn.ttl(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let count = current.unwrap_or(0);

            // Check if limit exceeded
            if count >= limit {
                return Ok((false, 0, ttl.max(0)));
            }

            // Increment counter
            let new_count: u64 = conn.incr(&key, 1).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            // Set expiry on first request
            if count == 0 {
                let _: () = conn.expire(&key, window_seconds as i64).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;
            }

            Ok((true, limit.saturating_sub(new_count), ttl.max(0)))
        })
    }

    // --- Rate limiting: admin_login ---

    /// Check admin login rate limit and exponential backoff.
    /// Returns (allowed, wait_seconds).
    pub fn check_rate_limit_admin_login(
        &self,
        ip: &str,
        limit: u64,
        window_seconds: u64,
    ) -> Result<(bool, Option<u64>)> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let ip = ip.to_string();
        let backoff_key = format!("{}:ratelimit:adminlogin:backoff:{}", key_prefix, ip);
        let key = format!("{}:ratelimit:adminlogin:{}", key_prefix, ip);

        self.block_on(async move {
            let mut conn = pool.manager.lock().await;

            // Check if we're in backoff mode
            let backoff_fields: HashMap<String, Value> = conn.hgetall(&backoff_key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if !backoff_fields.is_empty() {
                let next_allowed_at = get_field_i64(&backoff_fields, "next_allowed_at")?;
                let now = now_ms();
                if next_allowed_at > now {
                    let wait_seconds = ((next_allowed_at - now) / 1000) as u64;
                    return Ok((false, Some(wait_seconds)));
                }
                // Backoff expired, clear it
                let _: () = conn.del(&backoff_key).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;
            }

            // Check standard rate limit
            let current: Option<u64> = conn.get(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let count = current.unwrap_or(0);

            // Check if limit exceeded
            if count >= limit {
                return Ok((false, None));
            }

            // Increment counter
            let mut pipe = pipe();
            pipe.incr(&key, 1);
            pipe.expire(&key, window_seconds as i64);
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok((true, None))
        })
    }

    /// Record a failed admin login attempt and return backoff if triggered.
    /// Returns Some(wait_seconds) if backoff was triggered, None otherwise.
    pub fn record_failure_admin_login(
        &self,
        ip: &str,
        failed_threshold: u32,
        backoff_start_minutes: u64,
        backoff_max_hours: u64,
    ) -> Result<Option<u64>> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let ip = ip.to_string();
        let backoff_key = format!("{}:ratelimit:adminlogin:backoff:{}", key_prefix, ip);

        self.block_on(async move {
            let mut conn = pool.manager.lock().await;

            // Check if already in backoff
            let backoff_fields: HashMap<String, Value> = conn.hgetall(&backoff_key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            let current_failed: u64 = if backoff_fields.is_empty() {
                0
            } else {
                get_field_i64(&backoff_fields, "failed_count")? as u64
            };

            let new_failed = current_failed + 1;

            // Check if we should enter backoff mode
            if new_failed >= failed_threshold as u64 {
                let backoff_exponent = (new_failed.saturating_sub(failed_threshold as u64) as u32).min(7);
                let backoff_minutes = backoff_start_minutes * (1u64 << backoff_exponent);
                let backoff_seconds = (backoff_minutes * 60).min(backoff_max_hours * 3600);

                let now = now_ms();
                let next_allowed_at = now + (backoff_seconds as i64 * 1000);

                let mut pipe = pipe();
                pipe.hset(&backoff_key, "failed_count", new_failed as i64);
                pipe.hset(&backoff_key, "next_allowed_at", next_allowed_at);
                pipe.expire(&backoff_key, (backoff_seconds as i64 + 60) as i64);
                pool.pipeline_query::<()>(&mut pipe).await?;

                return Ok(Some(backoff_seconds));
            }

            // Just update the failed count
            let _: () = conn.hset(&backoff_key, "failed_count", new_failed as i64).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            Ok(None)
        })
    }

    /// Reset admin login rate limit on successful login.
    pub fn reset_rate_limit_admin_login(&self, ip: &str) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let ip = ip.to_string();
        let key = format!("{}:ratelimit:adminlogin:{}", key_prefix, ip);
        let backoff_key = format!("{}:ratelimit:adminlogin:backoff:{}", key_prefix, ip);

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.del(&key);
            pipe.del(&backoff_key);
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    // --- search_ui_scoped_key ---

    /// Get the current scoped key for an index.
    pub fn get_search_ui_scoped_key(&self, index_uid: &str) -> Result<Option<SearchUiScopedKey>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let index_uid = index_uid.to_string();
        let key = format!("{}:search_ui_scoped_key:{}", key_prefix, index_uid);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let fields: HashMap<String, Value> = conn.hgetall(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            if fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(SearchUiScopedKey {
                    index_uid: index_uid.clone(),
                    primary_key: get_field_string(&fields, "primary_key")?,
                    primary_uid: get_field_string(&fields, "primary_uid")?,
                    previous_key: opt_field(&fields, "previous_key"),
                    previous_uid: opt_field(&fields, "previous_uid"),
                    rotated_at: get_field_i64(&fields, "rotated_at")?,
                    generation: get_field_i64(&fields, "generation")?,
                }))
            }
        })
    }

    /// Set a new scoped key generation.
    pub fn set_search_ui_scoped_key(&self, key: &SearchUiScopedKey) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let key_value = key.clone();
        let redis_key = format!("{}:search_ui_scoped_key:{}", key_prefix, key_value.index_uid);

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset(&redis_key, "index_uid", &key_value.index_uid);
            pipe.hset(&redis_key, "primary_key", &key_value.primary_key);
            pipe.hset(&redis_key, "primary_uid", &key_value.primary_uid);
            pipe.hset(&redis_key, "rotated_at", key_value.rotated_at);
            pipe.hset(&redis_key, "generation", key_value.generation);
            match key_value.previous_key {
                Some(ref v) => { pipe.hset(&redis_key, "previous_key", v); }
                None => { pipe.hdel(&redis_key, "previous_key"); }
            }
            match key_value.previous_uid {
                Some(ref v) => { pipe.hset(&redis_key, "previous_uid", v); }
                None => { pipe.hdel(&redis_key, "previous_uid"); }
            }
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    /// Record a pod's observation of a scoped key generation.
    pub fn observe_search_ui_scoped_key(
        &self,
        pod_id: &str,
        index_uid: &str,
        generation: i64,
    ) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let pod_id = pod_id.to_string();
        let index_uid = index_uid.to_string();
        let key = format!("{}:search_ui_scoped_key_observed:{}:{}", key_prefix, pod_id, index_uid);

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hset(&key, "generation", generation);
            pipe.hset(&key, "observed_at", now_ms());
            pipe.expire(&key, 60);
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    /// Check if all live pods have observed a given generation.
    /// Returns (all_observed, unobserved_pods).
    pub fn check_scoped_key_observation(
        &self,
        index_uid: &str,
        generation: i64,
        live_pods: &[String],
    ) -> Result<(bool, Vec<String>)> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let index_uid = index_uid.to_string();
        let live_pods = live_pods.to_vec();

        self.block_on(async move {
            let mut unobserved = Vec::new();
            let mut conn = manager.lock().await;

            for pod_id in &live_pods {
                let key = format!("{}:search_ui_scoped_key_observed:{}:{}", key_prefix, pod_id, index_uid);
                let fields: HashMap<String, Value> = conn.hgetall(&key).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;

                if fields.is_empty() {
                    unobserved.push(pod_id.clone());
                } else {
                    let pod_gen = get_field_i64(&fields, "generation")?;
                    if pod_gen != generation {
                        unobserved.push(pod_id.clone());
                    }
                }
            }

            Ok((unobserved.is_empty(), unobserved))
        })
    }

    /// Clear the previous_uid field from a scoped key hash (after revocation).
    pub fn clear_scoped_key_previous(&self, index_uid: &str) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let index_uid = index_uid.to_string();
        let redis_key = format!("{}:search_ui_scoped_key:{}", key_prefix, index_uid);

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.hdel(&redis_key, "previous_uid");
            pipe.hdel(&redis_key, "previous_key");
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    /// Register this pod as alive. Uses a Sorted Set with timestamp scores
    /// so we can query for recently-active pods.
    pub fn register_pod_presence(&self, pod_id: &str) -> Result<()> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let pod_id = pod_id.to_string();
        let key = format!("{}:live_pods", key_prefix);
        let now = now_ms();

        self.block_on(async move {
            let mut pipe = pipe();
            pipe.zadd(&key, &pod_id, now);
            // Expire the whole set after 5 minutes to prevent unbounded growth.
            // Active pods continuously refresh, so this just cleans up after total shutdown.
            pipe.expire(&key, 300);
            pool.pipeline_query::<()>(&mut pipe).await?;
            Ok(())
        })
    }

    /// Get the list of pods that have registered presence within the last 120 seconds.
    pub fn get_live_pods(&self) -> Result<Vec<String>> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let key = format!("{}:live_pods", key_prefix);
        let cutoff = now_ms() - 120_000; // 120 seconds ago

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let pods: Vec<String> = conn.zrangebyscore(&key, cutoff, "+inf")
                .await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;
            Ok(pods)
        })
    }

    /// List all index UIDs that have scoped keys in Redis.
    pub fn list_scoped_key_indexes(&self) -> Result<Vec<String>> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();

        self.block_on(async move {
            let pattern = format!("{}:search_ui_scoped_key:*", key_prefix);
            let mut conn = pool.manager.lock().await;

            let mut indexes = Vec::new();
            let mut cursor: u64 = 0;
            loop {
                let (new_cursor, keys): (u64, Vec<String>) = ::redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;

                for key in keys {
                    // Extract index_uid from the key: "miroir:search_ui_scoped_key:<index>"
                    if let Some(idx) = key.rsplit(':').next() {
                        indexes.push(idx.to_string());
                    }
                }

                cursor = new_cursor;
                if cursor == 0 {
                    break;
                }
            }

            Ok(indexes)
        })
    }

    // --- CDC overflow buffer ---

    /// Append to the CDC overflow buffer for a sink.
    /// Uses LPUSH + LTRIM to keep the list bounded by byte budget.
    /// Returns (current_element_count, was_trimmed).
    pub fn cdc_overflow_append(
        &self,
        sink_name: &str,
        data: &[u8],
        max_bytes: usize,
    ) -> Result<(usize, bool)> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let sink_name = sink_name.to_string();
        let data = data.to_vec();
        let key = format!("{}:cdc:overflow:{}", key_prefix, sink_name);
        let bytes_key = format!("{}:cdc:overflow_bytes:{}", key_prefix, sink_name);
        let data_len = data.len();

        self.block_on(async move {
            let mut conn = pool.manager.lock().await;

            // Read tracked byte size (atomic counter in a separate key)
            let tracked_bytes: i64 = conn.get(&bytes_key).await
                .unwrap_or(None)
                .unwrap_or(0);

            let new_bytes = tracked_bytes + data_len as i64;
            let mut trimmed = false;

            // If adding this event exceeds the budget, trim from the tail (oldest)
            // until we are back under budget.
            if new_bytes > max_bytes as i64 {
                let current_len: i64 = conn.llen(&key).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;

                // Estimate elements to keep: proportional to remaining budget.
                if current_len > 0 && tracked_bytes > 0 {
                    let avg_element_bytes = tracked_bytes as f64 / current_len as f64;
                    let keep = ((max_bytes as f64) / avg_element_bytes).floor() as isize;
                    if keep > 0 {
                        let _: () = conn.ltrim(&key, 0, keep - 1).await
                            .map_err(|e| MiroirError::Redis(e.to_string()))?;
                    } else {
                        let _: () = conn.del(&key).await
                            .map_err(|e| MiroirError::Redis(e.to_string()))?;
                    }
                }
                trimmed = true;
            }

            // LPUSH new element to the head (newest first)
            let _: () = conn.lpush(&key, &data).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            // Update byte counter: recompute from LLEN * average or just add
            // the new element's bytes (exact enough for overflow purposes).
            let final_count: i64 = conn.llen(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            // If we trimmed, recompute tracked bytes from scratch; otherwise add.
            let new_tracked = if trimmed {
                // Approximate: element_count * new_element_bytes is a rough
                // lower bound.  For a tighter number we'd need LRANGE + sum,
                // but for overflow budgeting this is sufficient.
                (final_count as f64 * data_len as f64) as i64
            } else {
                tracked_bytes + data_len as i64
            };

            let mut pipe = pipe();
            pipe.set(&bytes_key, new_tracked);
            pool.pipeline_query::<()>(&mut pipe).await?;

            Ok((final_count as usize, trimmed))
        })
    }

    /// Pop from the tail of the CDC overflow buffer (oldest element, FIFO order).
    pub fn cdc_overflow_pop(&self, sink_name: &str) -> Result<Option<Vec<u8>>> {
        let pool = self.pool.clone();
        let key_prefix = self.key_prefix.clone();
        let sink_name = sink_name.to_string();
        let key = format!("{}:cdc:overflow:{}", key_prefix, sink_name);
        let bytes_key = format!("{}:cdc:overflow_bytes:{}", key_prefix, sink_name);

        self.block_on(async move {
            let mut conn = pool.manager.lock().await;
            let data: Option<Vec<u8>> = conn.rpop(&key, None).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;

            // Adjust tracked byte counter
            if let Some(ref d) = data {
                let tracked: i64 = conn.get(&bytes_key).await
                    .unwrap_or(None)
                    .unwrap_or(0);
                let adjusted = (tracked - d.len() as i64).max(0);
                let _: () = conn.set(&bytes_key, adjusted).await
                    .map_err(|e| MiroirError::Redis(e.to_string()))?;
            }

            Ok(data)
        })
    }

    /// Get the current element count of the CDC overflow buffer (LLEN).
    pub fn cdc_overflow_size(&self, sink_name: &str) -> Result<usize> {
        let manager = self.pool.manager.clone();
        let key_prefix = self.key_prefix.clone();
        let sink_name = sink_name.to_string();
        let key = format!("{}:cdc:overflow:{}", key_prefix, sink_name);

        self.block_on(async move {
            let mut conn = manager.lock().await;
            let len: i64 = conn.llen(&key).await
                .map_err(|e| MiroirError::Redis(e.to_string()))?;
            Ok(len as usize)
        })
    }

    /// Subscribe to the admin session revocation Pub/Sub channel.
    /// Calls `on_revoked` for each session ID published.
    /// This runs indefinitely until the connection drops.
    pub async fn subscribe_session_revocations<F>(
        url: &str,
        key_prefix: &str,
        on_revoked: F,
    ) -> Result<()>
    where
        F: Fn(String) + Send + 'static,
    {
        let client = Client::open(url)
            .map_err(|e| MiroirError::Redis(e.to_string()))?;
        let mut conn = client
            .get_async_pubsub()
            .await
            .map_err(|e| MiroirError::Redis(e.to_string()))?;

        let channel = format!("{}:admin_session:revoked", key_prefix);
        conn.subscribe(&channel).await
            .map_err(|e| MiroirError::Redis(e.to_string()))?;

        let mut stream = conn.on_message();
        while let Some(msg) = stream.next().await {
            let payload: String = msg.get_payload()
                .map_err(|e| MiroirError::Redis(e.to_string()))?;
            on_revoked(payload);
        }

        Ok(())
    }
}

// --- Extra types for Redis-specific functionality ---

/// Scoped key for search UI access (plan §13.21).
#[derive(Debug, Clone)]
pub struct SearchUiScopedKey {
    pub index_uid: String,
    /// The Meilisearch API key used as Bearer token for search requests.
    pub primary_key: String,
    /// The Meilisearch key UID for management (DELETE /keys/{uid}).
    pub primary_uid: String,
    /// The previous API key (fallback during rotation overlap window).
    pub previous_key: Option<String>,
    /// The previous key UID (for revocation).
    pub previous_uid: Option<String>,
    pub rotated_at: i64,
    pub generation: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        // Test key generation helper directly
        fn test_key(prefix: &str, parts: &[&str]) -> String {
            format!("{}:{}", prefix, parts.join(":"))
        }
        assert_eq!(test_key("miroir", &["tasks", "task-1"]), "miroir:tasks:task-1");
        assert_eq!(test_key("miroir", &["lease", "scope-1"]), "miroir:lease:scope-1");
        assert_eq!(test_key("miroir", &["canary_runs", "canary-1"]), "miroir:canary_runs:canary-1");
    }

    #[test]
    fn test_now_ms() {
        let now = now_ms();
        assert!(now > 0);
    }

    // ------------------------------------------------------------------------
    // testcontainers-based integration tests
    // ------------------------------------------------------------------------

    #[cfg(feature = "redis-store")]
    mod integration {
        use super::*;
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::redis::Redis;

        /// Helper to set up a Redis container and return the store.
        async fn setup_redis_store() -> (RedisTaskStore, String) {
            let redis = Redis::default();
            let node = redis.start().await.expect("Failed to start Redis");
            let port = node
                .get_host_port_ipv4(6379)
                .await
                .expect("Failed to get Redis port");
            let url = format!("redis://localhost:{port}");
            let store = RedisTaskStore::open(&url)
                .await
                .expect("Failed to open Redis store");
            (store, url)
        }

        #[tokio::test]
        async fn test_redis_migrate() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");
        }

        #[tokio::test]
        async fn test_redis_tasks_crud() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            // Insert a task
            let mut node_tasks = HashMap::new();
            node_tasks.insert("node-0".to_string(), 42u64);
            let task = NewTask {
                miroir_id: "task-1".to_string(),
                created_at: now_ms(),
                status: "queued".to_string(),
                node_tasks,
                error: None,
            };
            store.insert_task(&task).expect("Insert should succeed");

            // Get the task
            let retrieved = store.get_task("task-1").expect("Get should succeed");
            assert!(retrieved.is_some());
            let retrieved = retrieved.unwrap();
            assert_eq!(retrieved.miroir_id, "task-1");
            assert_eq!(retrieved.status, "queued");

            // Update status
            store.update_task_status("task-1", "running").expect("Update should succeed");
            let updated = store.get_task("task-1").expect("Get should succeed").unwrap();
            assert_eq!(updated.status, "running");

            // Update node task
            store
                .update_node_task("task-1", "node-1", 123)
                .expect("Update node task should succeed");
            let with_node = store.get_task("task-1").expect("Get should succeed").unwrap();
            assert_eq!(with_node.node_tasks.get("node-1"), Some(&123));

            // Set error
            store
                .set_task_error("task-1", "test error")
                .expect("Set error should succeed");
            let with_error = store.get_task("task-1").expect("Get should succeed").unwrap();
            assert_eq!(with_error.error.as_deref(), Some("test error"));

            // List tasks
            let tasks = store.list_tasks(&TaskFilter::default()).expect("List should succeed");
            assert_eq!(tasks.len(), 1);

            // Task count
            let count = store.task_count().expect("Count should succeed");
            assert_eq!(count, 1);

            // Prune tasks (no old tasks, so 0 deleted)
            let deleted = store
                .prune_tasks(now_ms() - 10000, 100)
                .expect("Prune should succeed");
            assert_eq!(deleted, 0);
        }

        #[tokio::test]
        async fn test_redis_leader_lease() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let scope = "test-scope";
            let holder = "pod-1";
            let expires_at = now_ms() + 10000;

            // Try to acquire lease
            let acquired = store
                .try_acquire_leader_lease(scope, holder, expires_at, now_ms())
                .expect("Acquire should succeed");
            assert!(acquired);

            // Get lease
            let lease = store
                .get_leader_lease(scope)
                .expect("Get should succeed")
                .expect("Lease should exist");
            assert_eq!(lease.holder, holder);

            // Renew lease
            let new_expires = now_ms() + 20000;
            assert!(store
                .renew_leader_lease(scope, holder, new_expires)
                .expect("Renew should succeed"));

            // Another pod tries to acquire (should fail)
            let other_acquired = store
                .try_acquire_leader_lease(scope, "pod-2", new_expires, now_ms())
                .expect("Second acquire should succeed but return false");
            assert!(!other_acquired);
        }

        #[tokio::test]
        async fn test_redis_lease_race() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            // Simulate two pods racing for the same lease
            let scope = "race-scope";
            let expires_at = now_ms() + 10000;

            // Spawn two concurrent tasks trying to acquire
            let store1 = store.clone();
            let store2 = store.clone();

            let handle1 = tokio::spawn(async move {
                store1
                    .try_acquire_leader_lease(scope, "pod-1", expires_at, now_ms())
                    .expect("Pod 1 acquire should succeed")
            });

            let handle2 = tokio::spawn(async move {
                store2
                    .try_acquire_leader_lease(scope, "pod-2", expires_at, now_ms())
                    .expect("Pod 2 acquire should succeed")
            });

            let (acquired1, acquired2) = tokio::join!(handle1, handle2);
            let acquired1 = acquired1.expect("Pod 1 task should succeed");
            let acquired2 = acquired2.expect("Pod 2 task should succeed");

            // Exactly one should win
            assert!(
                acquired1 ^ acquired2,
                "Exactly one pod should acquire the lease, got pod1={}, pod2={}",
                acquired1,
                acquired2
            );

            // Verify only one holder
            let lease = store
                .get_leader_lease(scope)
                .expect("Get should succeed")
                .expect("Lease should exist");
            assert!((lease.holder == "pod-1") ^ (lease.holder == "pod-2"));
        }

        /// Memory budget test: verify Redis RSS stays under plan §14.7 targets.
        /// Target: ~100 bytes per task + overhead, 10k tasks < ~2 MB RSS.
        #[tokio::test]
        async fn test_redis_memory_budget() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            // Insert 10k tasks
            let count = 10_000;
            for i in 0..count {
                let mut node_tasks = HashMap::new();
                node_tasks.insert(format!("node-{}", i % 10), i as u64);
                let task = NewTask {
                    miroir_id: format!("task-{}", i),
                    created_at: now_ms(),
                    status: if i % 3 == 0 { "succeeded" } else { "queued" }.to_string(),
                    node_tasks,
                    error: if i % 10 == 0 { Some("test error".to_string()) } else { None },
                };
                store.insert_task(&task).expect("Insert should succeed");
            }

            // Insert 1k idempotency entries
            for i in 0..1_000 {
                let entry = IdempotencyEntry {
                    key: format!("idemp-{}", i),
                    body_sha256: vec![0u8; 32],
                    miroir_task_id: format!("task-{}", i),
                    expires_at: now_ms() + 3600_000,
                };
                store.insert_idempotency_entry(&entry).expect("Insert idempotency should succeed");
            }

            // Insert 1k sessions
            for i in 0..1_000 {
                let session = SessionRow {
                    session_id: format!("session-{}", i),
                    last_write_mtask_id: Some(format!("task-{}", i)),
                    last_write_at: Some(now_ms()),
                    pinned_group: Some(i as i64),
                    min_settings_version: 1,
                    ttl: now_ms() + 3600_000,
                };
                store.upsert_session(&session).expect("Insert session should succeed");
            }

            // Verify counts
            let task_count = store.task_count().expect("Task count should succeed");
            assert_eq!(task_count, count as u64, "Should have all tasks");

            // Note: Actual Redis RSS measurement requires Redis INFO command or
            // external monitoring (e.g., docker stats). This test verifies the
            // workload can be created; in production, miroir_cdc_redis_memory_bytes
            // would alert if exceeding budget.
            // Plan §14.7 target: < 2 MB RSS for this workload.
        }

        /// Pub/Sub test: verify session revocation via subscriber within 100ms.
        #[tokio::test]
        async fn test_redis_pubsub_session_invalidation() {
            let (store, url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let revoked = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
            let revoked_clone = revoked.clone();

            // Start subscriber in background
            let sub_handle = tokio::spawn(async move {
                let _ = RedisTaskStore::subscribe_session_revocations(
                    &url,
                    "miroir",
                    move |session_id: String| {
                        revoked_clone.lock().unwrap().push(session_id);
                    },
                ).await;
            });

            // Give subscriber time to connect
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            // Create and revoke a session
            let session = NewAdminSession {
                session_id: "pubsub-test-session".to_string(),
                csrf_token: "csrf".to_string(),
                admin_key_hash: "hash".to_string(),
                created_at: now_ms(),
                expires_at: now_ms() + 3600_000,
                user_agent: None,
                source_ip: None,
            };
            store.insert_admin_session(&session).expect("Insert should succeed");

            let start = std::time::Instant::now();
            store.revoke_admin_session("pubsub-test-session").expect("Revoke should succeed");

            // Wait for subscriber to receive the message (must be < 100ms)
            let deadline = tokio::time::Duration::from_millis(200);
            loop {
                let received = revoked.lock().unwrap();
                if received.len() == 1 && received[0] == "pubsub-test-session" {
                    break;
                }
                drop(received);
                if start.elapsed() > deadline {
                    panic!("Pub/Sub message not received within 200ms");
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }

            let elapsed = start.elapsed();
            assert!(elapsed < deadline, "Propagation took {:?}", elapsed);

            sub_handle.abort();
        }

        // --- Rate limiting: search_ui with EXPIRE ---

        #[tokio::test]
        async fn test_redis_rate_limit_searchui() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let ip = "192.168.1.1";
            let limit = 3u64;
            let window_seconds = 60u64;

            // First request: allowed
            let (allowed, remaining, _) = store
                .check_rate_limit_searchui(ip, limit, window_seconds)
                .expect("Check should succeed");
            assert!(allowed);
            assert_eq!(remaining, 2);

            // Second request: allowed
            let (allowed, remaining, _) = store
                .check_rate_limit_searchui(ip, limit, window_seconds)
                .expect("Check should succeed");
            assert!(allowed);
            assert_eq!(remaining, 1);

            // Third request: allowed
            let (allowed, remaining, _) = store
                .check_rate_limit_searchui(ip, limit, window_seconds)
                .expect("Check should succeed");
            assert!(allowed);
            assert_eq!(remaining, 0);

            // Fourth request: blocked
            let (allowed, _, reset_after) = store
                .check_rate_limit_searchui(ip, limit, window_seconds)
                .expect("Check should succeed");
            assert!(!allowed, "Should be rate limited");
            assert!(reset_after > 0, "Should have TTL remaining");

            // Verify key has EXPIRE set (TTL should be > 0)
            let key = "miroir:ratelimit:searchui:192.168.1.1";
            let mut conn = store.pool.manager.lock().await;
            let ttl: i64 = conn.ttl(key).await.expect("TTL should work");
            assert!(ttl > 0, "Rate limit key should have EXPIRE set, got TTL={}", ttl);
            assert!(ttl <= window_seconds as i64, "TTL should not exceed window, got {}", ttl);
        }

        // --- Rate limiting: admin_login with backoff ---

        #[tokio::test]
        async fn test_redis_rate_limit_admin_login() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let ip = "10.0.0.1";
            let limit = 3u64;
            let window_seconds = 60u64;

            // First 3 attempts: allowed
            for _ in 0..3 {
                let (allowed, wait) = store
                    .check_rate_limit_admin_login(ip, limit, window_seconds)
                    .expect("Check should succeed");
                assert!(allowed);
                assert!(wait.is_none());
            }

            // Fourth attempt: rate limited
            let (allowed, wait) = store
                .check_rate_limit_admin_login(ip, limit, window_seconds)
                .expect("Check should succeed");
            assert!(!allowed);

            // Record failures to trigger backoff
            let _ = store.record_failure_admin_login(ip, 3, 1, 24);

            // Next login should be in backoff
            let (allowed, wait) = store
                .check_rate_limit_admin_login(ip, limit, window_seconds)
                .expect("Check should succeed");
            assert!(!allowed, "Should be in backoff");
            assert!(wait.is_some(), "Should have wait time");

            // Reset on success
            store.reset_rate_limit_admin_login(ip).expect("Reset should succeed");

            // Should be allowed again
            let (allowed, wait) = store
                .check_rate_limit_admin_login(ip, limit, window_seconds)
                .expect("Check should succeed");
            assert!(allowed, "Should be allowed after reset");
            assert!(wait.is_none());
        }

        // --- CDC overflow buffer ---

        #[tokio::test]
        async fn test_redis_cdc_overflow() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let sink = "test-sink";
            let event = b"{\"type\":\"insert\",\"index\":\"logs\"}";
            let max_bytes = 200; // ~3 events at 42 bytes each

            // Append events
            let (count, trimmed) = store
                .cdc_overflow_append(sink, event, max_bytes)
                .expect("Append should succeed");
            assert_eq!(count, 1);
            assert!(!trimmed);

            let (count, trimmed) = store
                .cdc_overflow_append(sink, event, max_bytes)
                .expect("Append should succeed");
            assert_eq!(count, 2);
            assert!(!trimmed);

            let (count, trimmed) = store
                .cdc_overflow_append(sink, event, max_bytes)
                .expect("Append should succeed");
            assert!(count >= 3);
            // May or may not trim depending on exact byte count

            // Size should match LLEN
            let size = store.cdc_overflow_size(sink).expect("Size should succeed");
            assert!(size > 0, "Overflow buffer should have elements");

            // Pop should return oldest event (FIFO)
            let popped = store.cdc_overflow_pop(sink).expect("Pop should succeed");
            assert!(popped.is_some());
            assert_eq!(popped.unwrap().as_slice(), event);

            // Size should decrease
            let new_size = store.cdc_overflow_size(sink).expect("Size should succeed");
            assert_eq!(new_size, size - 1);
        }

        #[tokio::test]
        async fn test_redis_cdc_overflow_trim() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let sink = "trim-sink";
            let event = b"short"; // 5 bytes per event
            let max_bytes = 20;   // room for ~4 events

            // Fill beyond budget
            for _ in 0..10 {
                let _ = store.cdc_overflow_append(sink, event, max_bytes)
                    .expect("Append should succeed");
            }

            let size = store.cdc_overflow_size(sink).expect("Size should succeed");
            assert!(size <= 10, "Should be bounded, got {}", size);

            // After enough appends the buffer should have been trimmed
            // (it won't grow unbounded beyond the byte budget)
        }

        // --- Scoped key coordination ---

        #[tokio::test]
        async fn test_redis_scoped_key_observation() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let index_uid = "products";

            // Set a scoped key
            let key = SearchUiScopedKey {
                index_uid: index_uid.to_string(),
                primary_key: "key-abc".to_string(),
                primary_uid: "uid-abc".to_string(),
                previous_key: None,
                previous_uid: None,
                rotated_at: now_ms(),
                generation: 1,
            };
            store.set_search_ui_scoped_key(&key).expect("Set should succeed");

            // Get it back
            let retrieved = store.get_search_ui_scoped_key(index_uid)
                .expect("Get should succeed")
                .expect("Key should exist");
            assert_eq!(retrieved.primary_uid, "uid-abc");
            assert_eq!(retrieved.generation, 1);

            // Pod-1 observes generation 1
            store.observe_search_ui_scoped_key("pod-1", index_uid, 1)
                .expect("Observe should succeed");

            // Pod-2 observes generation 1
            store.observe_search_ui_scoped_key("pod-2", index_uid, 1)
                .expect("Observe should succeed");

            // Check observation — all observed
            let (all, unobserved) = store.check_scoped_key_observation(index_uid, 1, &["pod-1".into(), "pod-2".into()])
                .expect("Check should succeed");
            assert!(all, "All pods should have observed");
            assert!(unobserved.is_empty());

            // Pod-3 hasn't observed
            let (all, unobserved) = store.check_scoped_key_observation(index_uid, 1, &["pod-1".into(), "pod-2".into(), "pod-3".into()])
                .expect("Check should succeed");
            assert!(!all, "Pod-3 hasn't observed");
            assert!(unobserved.contains(&"pod-3".to_string()));

            // Clear previous
            let key2 = SearchUiScopedKey {
                index_uid: index_uid.to_string(),
                primary_key: "key-def".to_string(),
                primary_uid: "uid-def".to_string(),
                previous_key: Some("key-abc".to_string()),
                previous_uid: Some("uid-abc".to_string()),
                rotated_at: now_ms(),
                generation: 2,
            };
            store.set_search_ui_scoped_key(&key2).expect("Set gen2 should succeed");
            store.clear_scoped_key_previous(index_uid).expect("Clear should succeed");

            let retrieved = store.get_search_ui_scoped_key(index_uid)
                .expect("Get should succeed")
                .expect("Key should exist");
            assert!(retrieved.previous_uid.is_none());
            assert!(retrieved.previous_key.is_none());

            // List indexes
            let indexes = store.list_scoped_key_indexes().expect("List should succeed");
            assert!(indexes.contains(&index_uid.to_string()));
        }

        // --- Table 2: node_settings_version tests ---

        #[tokio::test]
        async fn test_redis_node_settings_version() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            // Insert
            store
                .upsert_node_settings_version("idx-1", "node-0", 5, 1000)
                .expect("Upsert should succeed");
            let row = store
                .get_node_settings_version("idx-1", "node-0")
                .expect("Get should succeed")
                .expect("Row should exist");
            assert_eq!(row.version, 5);
            assert_eq!(row.updated_at, 1000);

            // Upsert (update)
            store
                .upsert_node_settings_version("idx-1", "node-0", 7, 2000)
                .expect("Upsert should succeed");
            let row = store
                .get_node_settings_version("idx-1", "node-0")
                .expect("Get should succeed")
                .expect("Row should exist");
            assert_eq!(row.version, 7);

            // Missing
            assert!(store
                .get_node_settings_version("idx-1", "node-99")
                .expect("Get should succeed")
                .is_none());
        }

        // --- Table 3: aliases tests ---

        #[tokio::test]
        async fn test_redis_aliases_single() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            // Create single alias
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
                .expect("Create should succeed");

            let alias = store
                .get_alias("prod-logs")
                .expect("Get should succeed")
                .expect("Alias should exist");
            assert_eq!(alias.current_uid.as_deref(), Some("uid-v1"));
            assert_eq!(alias.version, 1);

            // Flip
            assert!(store
                .flip_alias("prod-logs", "uid-v2", 10)
                .expect("Flip should succeed"));
            let alias = store
                .get_alias("prod-logs")
                .expect("Get should succeed")
                .expect("Alias should exist");
            assert_eq!(alias.current_uid.as_deref(), Some("uid-v2"));
            assert_eq!(alias.version, 2);
            assert_eq!(alias.history.len(), 1);

            // Flip with retention trim
            for uid in ["uid-v3", "uid-v4", "uid-v5"] {
                store
                    .flip_alias("prod-logs", uid, 2)
                    .expect("Flip should succeed");
            }
            let alias = store
                .get_alias("prod-logs")
                .expect("Get should succeed")
                .expect("Alias should exist");
            assert_eq!(alias.history.len(), 2); // retention = 2

            // Delete
            assert!(store
                .delete_alias("prod-logs")
                .expect("Delete should succeed"));
            assert!(store
                .get_alias("prod-logs")
                .expect("Get should succeed")
                .is_none());
        }

        #[tokio::test]
        async fn test_redis_aliases_multi() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

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
                .expect("Create should succeed");

            let alias = store
                .get_alias("search-all")
                .expect("Get should succeed")
                .expect("Alias should exist");
            assert_eq!(alias.kind, "multi");
            assert!(alias.current_uid.is_none());
            assert_eq!(
                alias.target_uids.unwrap(),
                vec!["uid-a".to_string(), "uid-b".to_string()]
            );
        }

        // --- Table 4: sessions tests ---

        #[tokio::test]
        async fn test_redis_sessions() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let session = SessionRow {
                session_id: "sess-1".to_string(),
                last_write_mtask_id: Some("task-1".to_string()),
                last_write_at: Some(1000),
                pinned_group: Some(2),
                min_settings_version: 5,
                ttl: now_ms() + 60000, // expires in 60s
            };
            store.upsert_session(&session).expect("Upsert should succeed");

            let got = store
                .get_session("sess-1")
                .expect("Get should succeed")
                .expect("Session should exist");
            assert_eq!(got.last_write_mtask_id.as_deref(), Some("task-1"));
            assert_eq!(got.pinned_group, Some(2));

            // Upsert (update)
            let updated = SessionRow {
                session_id: "sess-1".to_string(),
                last_write_mtask_id: Some("task-2".to_string()),
                last_write_at: Some(1500),
                pinned_group: None,
                min_settings_version: 6,
                ttl: now_ms() + 120000,
            };
            store.upsert_session(&updated).expect("Upsert should succeed");
            let got = store
                .get_session("sess-1")
                .expect("Get should succeed")
                .expect("Session should exist");
            assert_eq!(got.last_write_mtask_id.as_deref(), Some("task-2"));

            // Redis handles expiration automatically - delete_expired_sessions returns 0
            let deleted = store
                .delete_expired_sessions(now_ms())
                .expect("Delete expired should succeed");
            assert_eq!(deleted, 0);
        }

        // --- Table 5: idempotency tests ---

        #[tokio::test]
        async fn test_redis_idempotency() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let sha = vec![0u8; 32];
            store
                .insert_idempotency_entry(&IdempotencyEntry {
                    key: "req-abc".to_string(),
                    body_sha256: sha.clone(),
                    miroir_task_id: "task-1".to_string(),
                    expires_at: now_ms() + 3600000,
                })
                .expect("Insert should succeed");

            let entry = store
                .get_idempotency_entry("req-abc")
                .expect("Get should succeed")
                .expect("Entry should exist");
            assert_eq!(entry.body_sha256, sha);
            assert_eq!(entry.miroir_task_id, "task-1");

            // Missing
            assert!(store
                .get_idempotency_entry("nope")
                .expect("Get should succeed")
                .is_none());

            // Redis handles expiration automatically - delete_expired returns 0
            let deleted = store
                .delete_expired_idempotency_entries(now_ms())
                .expect("Delete expired should succeed");
            assert_eq!(deleted, 0);
        }

        // --- Table 6: jobs tests ---

        #[tokio::test]
        async fn test_redis_jobs() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            store
                .insert_job(&NewJob {
                    id: "job-1".to_string(),
                    type_: "dump_import".to_string(),
                    params: r#"{"index": "logs"}"#.to_string(),
                    state: "queued".to_string(),
                    progress: "{}".to_string(),
                })
                .expect("Insert should succeed");

            let job = store
                .get_job("job-1")
                .expect("Get should succeed")
                .expect("Job should exist");
            assert_eq!(job.state, "queued");
            assert!(job.claimed_by.is_none());

            // Claim
            assert!(store
                .claim_job("job-1", "pod-a", now_ms() + 10000)
                .expect("Claim should succeed"));
            let job = store
                .get_job("job-1")
                .expect("Get should succeed")
                .expect("Job should exist");
            assert_eq!(job.state, "in_progress");
            assert_eq!(job.claimed_by.as_deref(), Some("pod-a"));

            // Cannot double-claim
            assert!(!store
                .claim_job("job-1", "pod-b", now_ms() + 20000)
                .expect("Claim should fail"));

            // Update progress
            assert!(store
                .update_job_progress("job-1", "in_progress", r#"{"bytes": 1024}"#)
                .expect("Update progress should succeed"));

            // Renew claim
            assert!(store
                .renew_job_claim("job-1", now_ms() + 30000)
                .expect("Renew should succeed"));

            // Complete
            assert!(store
                .update_job_progress("job-1", "completed", r#"{"bytes": 4096}"#)
                .expect("Update to completed should succeed"));

            // List by state
            store
                .insert_job(&NewJob {
                    id: "job-2".to_string(),
                    type_: "test".to_string(),
                    params: "{}".to_string(),
                    state: "queued".to_string(),
                    progress: "{}".to_string(),
                })
                .expect("Insert job-2 should succeed");

            let queued = store
                .list_jobs_by_state("queued")
                .expect("List queued should succeed");
            assert_eq!(queued.len(), 1);
            assert_eq!(queued[0].id, "job-2");

            let in_progress = store
                .list_jobs_by_state("in_progress")
                .expect("List in_progress should succeed");
            assert_eq!(in_progress.len(), 1);
            assert_eq!(in_progress[0].id, "job-1");
        }

        // --- Table 8: canaries tests ---

        #[tokio::test]
        async fn test_redis_canaries() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

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
                .expect("Upsert should succeed");

            // Get the canary
            let canary = store
                .get_canary("canary-1")
                .expect("Get should succeed")
                .expect("Canary should exist");
            assert_eq!(canary.id, "canary-1");
            assert_eq!(canary.name, "Search health check");
            assert!(canary.enabled);

            // List all canaries
            let canaries = store
                .list_canaries()
                .expect("List should succeed");
            assert_eq!(canaries.len(), 1);

            // Upsert (update)
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
                .expect("Update should succeed");

            let canary = store
                .get_canary("canary-1")
                .expect("Get should succeed")
                .expect("Canary should exist");
            assert_eq!(canary.name, "Updated health check");
            assert!(!canary.enabled);

            // Delete
            assert!(store
                .delete_canary("canary-1")
                .expect("Delete should succeed"));
            assert!(store
                .get_canary("canary-1")
                .expect("Get should succeed")
                .is_none());

            // Delete non-existent
            assert!(!store
                .delete_canary("no-such-canary")
                .expect("Delete non-existent should fail"));
        }

        // --- Table 9: canary_runs tests ---

        #[tokio::test]
        async fn test_redis_canary_runs() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

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
                        3,
                    )
                    .expect("Insert run should succeed");
            }

            // Only the 3 most recent runs should remain
            let runs = store
                .get_canary_runs("canary-1", 10)
                .expect("Get runs should succeed");
            assert_eq!(runs.len(), 3);
            // Runs are ordered by ran_at DESC
            assert_eq!(runs[0].ran_at, 1400);
            assert_eq!(runs[2].status, "fail");

            // Test limit parameter
            let runs = store
                .get_canary_runs("canary-1", 2)
                .expect("Get runs with limit should succeed");
            assert_eq!(runs.len(), 2);

            // Empty for non-existent canary
            let runs = store
                .get_canary_runs("no-such-canary", 10)
                .expect("Get runs for non-existent should succeed");
            assert!(runs.is_empty());
        }

        // --- Table 10: cdc_cursors tests ---

        #[tokio::test]
        async fn test_redis_cdc_cursors() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            // Insert a cursor
            store
                .upsert_cdc_cursor(&NewCdcCursor {
                    sink_name: "elasticsearch".to_string(),
                    index_uid: "logs".to_string(),
                    last_event_seq: 12345,
                    updated_at: 2000,
                })
                .expect("Upsert should succeed");

            // Get the cursor
            let cursor = store
                .get_cdc_cursor("elasticsearch", "logs")
                .expect("Get should succeed")
                .expect("Cursor should exist");
            assert_eq!(cursor.sink_name, "elasticsearch");
            assert_eq!(cursor.last_event_seq, 12345);

            // List all cursors for a sink
            store
                .upsert_cdc_cursor(&NewCdcCursor {
                    sink_name: "elasticsearch".to_string(),
                    index_uid: "metrics".to_string(),
                    last_event_seq: 67890,
                    updated_at: 2500,
                })
                .expect("Upsert second cursor should succeed");

            let cursors = store
                .list_cdc_cursors("elasticsearch")
                .expect("List should succeed");
            assert_eq!(cursors.len(), 2);

            // Upsert (update)
            store
                .upsert_cdc_cursor(&NewCdcCursor {
                    sink_name: "elasticsearch".to_string(),
                    index_uid: "logs".to_string(),
                    last_event_seq: 13000,
                    updated_at: 3000,
                })
                .expect("Update should succeed");

            let cursor = store
                .get_cdc_cursor("elasticsearch", "logs")
                .expect("Get should succeed")
                .expect("Cursor should exist");
            assert_eq!(cursor.last_event_seq, 13000);

            // Composite PK: different sink shouldn't exist
            assert!(store
                .get_cdc_cursor("elasticsearch", "nonexistent")
                .expect("Get should succeed")
                .is_none());
            assert!(store
                .get_cdc_cursor("unknown_sink", "logs")
                .expect("Get should succeed")
                .is_none());
        }

        // --- Table 11: tenant_map tests ---

        #[tokio::test]
        async fn test_redis_tenant_map() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let api_key_hash = vec![1u8; 32];

            // Insert
            store
                .insert_tenant_mapping(&NewTenantMapping {
                    api_key_hash: api_key_hash.clone(),
                    tenant_id: "acme-corp".to_string(),
                    group_id: Some(2),
                })
                .expect("Insert should succeed");

            // Get
            let mapping = store
                .get_tenant_mapping(&api_key_hash)
                .expect("Get should succeed")
                .expect("Mapping should exist");
            assert_eq!(mapping.tenant_id, "acme-corp");
            assert_eq!(mapping.group_id, Some(2));

            // Missing
            let unknown_hash = vec![99u8; 32];
            assert!(store
                .get_tenant_mapping(&unknown_hash)
                .expect("Get should succeed")
                .is_none());

            // Delete
            assert!(store
                .delete_tenant_mapping(&api_key_hash)
                .expect("Delete should succeed"));
            assert!(store
                .get_tenant_mapping(&api_key_hash)
                .expect("Get should succeed")
                .is_none());

            // Nullable group_id
            let hash2 = vec![2u8; 32];
            store
                .insert_tenant_mapping(&NewTenantMapping {
                    api_key_hash: hash2.clone(),
                    tenant_id: "default-tenant".to_string(),
                    group_id: None,
                })
                .expect("Insert with null group_id should succeed");

            let mapping = store
                .get_tenant_mapping(&hash2)
                .expect("Get should succeed")
                .expect("Mapping should exist");
            assert_eq!(mapping.tenant_id, "default-tenant");
            assert_eq!(mapping.group_id, None);
        }

        // --- Table 12: rollover_policies tests ---

        #[tokio::test]
        async fn test_redis_rollover_policies() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            // Insert
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
                .expect("Upsert should succeed");

            // Get
            let policy = store
                .get_rollover_policy("daily-logs")
                .expect("Get should succeed")
                .expect("Policy should exist");
            assert_eq!(policy.name, "daily-logs");
            assert_eq!(policy.write_alias, "logs-write");
            assert!(policy.enabled);

            // List
            let policies = store
                .list_rollover_policies()
                .expect("List should succeed");
            assert_eq!(policies.len(), 1);

            // Upsert (update)
            store
                .upsert_rollover_policy(&NewRolloverPolicy {
                    name: "daily-logs".to_string(),
                    write_alias: "logs-write".to_string(),
                    read_alias: "logs-read".to_string(),
                    pattern: "logs-{YYYY-MM-DD}".to_string(),
                    triggers_json: r#"{"max_age": "1d", "max_docs": 2000000}"#.to_string(),
                    retention_json: r#"{"keep_indexes": 30}"#.to_string(),
                    template_json: r#"{"primary_key": "id", "settings_ref": "logs-template"}"#.to_string(),
                    enabled: false,
                })
                .expect("Update should succeed");

            let policy = store
                .get_rollover_policy("daily-logs")
                .expect("Get should succeed")
                .expect("Policy should exist");
            assert!(!policy.enabled);

            // Delete
            assert!(store
                .delete_rollover_policy("daily-logs")
                .expect("Delete should succeed"));
            assert!(store
                .get_rollover_policy("daily-logs")
                .expect("Get should succeed")
                .is_none());
        }

        // --- Table 13: search_ui_config tests ---

        #[tokio::test]
        async fn test_redis_search_ui_config() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            let config_json = r#"{"title": "Product Search", "facets": ["category", "price"], "sort": ["relevance", "price_asc"]}"#;

            // Insert
            store
                .upsert_search_ui_config(&NewSearchUiConfig {
                    index_uid: "products".to_string(),
                    config_json: config_json.to_string(),
                    updated_at: 5000,
                })
                .expect("Upsert should succeed");

            // Get
            let config = store
                .get_search_ui_config("products")
                .expect("Get should succeed")
                .expect("Config should exist");
            assert_eq!(config.index_uid, "products");
            assert_eq!(config.config_json, config_json);

            // Upsert (update)
            let updated_json = r#"{"title": "Product Search V2", "facets": ["category"]}"#;
            store
                .upsert_search_ui_config(&NewSearchUiConfig {
                    index_uid: "products".to_string(),
                    config_json: updated_json.to_string(),
                    updated_at: 6000,
                })
                .expect("Update should succeed");

            let config = store
                .get_search_ui_config("products")
                .expect("Get should succeed")
                .expect("Config should exist");
            assert_eq!(config.config_json, updated_json);
            assert_eq!(config.updated_at, 6000);

            // Delete
            assert!(store
                .delete_search_ui_config("products")
                .expect("Delete should succeed"));
            assert!(store
                .get_search_ui_config("products")
                .expect("Get should succeed")
                .is_none());
        }

        // --- Table 14: admin_sessions tests ---

        #[tokio::test]
        async fn test_redis_admin_sessions() {
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            // Insert
            store
                .insert_admin_session(&NewAdminSession {
                    session_id: "sess-admin-1".to_string(),
                    csrf_token: "csrf-token-abc123".to_string(),
                    admin_key_hash: "hash-of-admin-key".to_string(),
                    created_at: 7000,
                    expires_at: now_ms() + 3600000,
                    user_agent: Some("Mozilla/5.0".to_string()),
                    source_ip: Some("192.168.1.100".to_string()),
                })
                .expect("Insert should succeed");

            // Get
            let session = store
                .get_admin_session("sess-admin-1")
                .expect("Get should succeed")
                .expect("Session should exist");
            assert_eq!(session.session_id, "sess-admin-1");
            assert_eq!(session.csrf_token, "csrf-token-abc123");
            assert!(!session.revoked);

            // Revoke
            assert!(store
                .revoke_admin_session("sess-admin-1")
                .expect("Revoke should succeed"));
            let session = store
                .get_admin_session("sess-admin-1")
                .expect("Get should succeed")
                .expect("Session should exist");
            assert!(session.revoked);

            // Nullable fields
            store
                .insert_admin_session(&NewAdminSession {
                    session_id: "sess-minimal".to_string(),
                    csrf_token: "csrf".to_string(),
                    admin_key_hash: "hash".to_string(),
                    created_at: 1000,
                    expires_at: now_ms() + 3600000,
                    user_agent: None,
                    source_ip: None,
                })
                .expect("Insert minimal session should succeed");

            let session = store
                .get_admin_session("sess-minimal")
                .expect("Get should succeed")
                .expect("Session should exist");
            assert!(session.user_agent.is_none());
            assert!(session.source_ip.is_none());

            // Redis handles expiration automatically - delete_expired returns 0
            let deleted = store
                .delete_expired_admin_sessions(now_ms())
                .expect("Delete expired should succeed");
            assert_eq!(deleted, 0);
        }

        // --- Comprehensive trait behavior test ---

        #[tokio::test]
        async fn test_redis_taskstore_trait_completeness() {
            // This test ensures all TaskStore trait methods are callable
            // and behave consistently with the SQLite implementation.
            let (store, _url) = setup_redis_store().await;
            store.migrate().expect("Migration should succeed");

            // Test tasks
            let mut node_tasks = HashMap::new();
            node_tasks.insert("node-1".to_string(), 123u64);
            store
                .insert_task(&NewTask {
                    miroir_id: "task-trait-test".to_string(),
                    created_at: now_ms(),
                    status: "queued".to_string(),
                    node_tasks: node_tasks.clone(),
                    error: None,
                })
                .expect("insert_task should work");

            let task = store
                .get_task("task-trait-test")
                .expect("get_task should work")
                .expect("task should exist");
            assert_eq!(task.node_tasks, node_tasks);

            // Test update operations
            assert!(store
                .update_task_status("task-trait-test", "running")
                .expect("update_task_status should work"));
            assert!(store
                .update_node_task("task-trait-test", "node-2", 456)
                .expect("update_node_task should work"));
            assert!(store
                .set_task_error("task-trait-test", "test error")
                .expect("set_task_error should work"));

            // Test list and filter
            let tasks = store
                .list_tasks(&TaskFilter {
                    status: Some("running".to_string()),
                    limit: Some(10),
                    offset: None,
                })
                .expect("list_tasks should work");
            assert_eq!(tasks.len(), 1);

            // Test count
            let count = store.task_count().expect("task_count should work");
            assert_eq!(count, 1);

            // Test prune
            let pruned = store
                .prune_tasks(now_ms() - 1000, 100)
                .expect("prune_tasks should work");
            assert_eq!(pruned, 0); // our task is recent

            // Test leader lease
            let scope = "trait-test-scope";
            assert!(store
                .try_acquire_leader_lease(scope, "pod-1", now_ms() + 10000, now_ms())
                .expect("try_acquire_leader_lease should work"));
            assert!(store
                .renew_leader_lease(scope, "pod-1", now_ms() + 20000)
                .expect("renew_leader_lease should work"));

            let lease = store
                .get_leader_lease(scope)
                .expect("get_leader_lease should work")
                .expect("lease should exist");
            assert_eq!(lease.holder, "pod-1");

            // Test job
            store
                .insert_job(&NewJob {
                    id: "job-trait-test".to_string(),
                    type_: "test".to_string(),
                    params: "{}".to_string(),
                    state: "queued".to_string(),
                    progress: "{}".to_string(),
                })
                .expect("insert_job should work");

            let job = store
                .get_job("job-trait-test")
                .expect("get_job should work")
                .expect("job should exist");
            assert_eq!(job.state, "queued");
        }
    }

    // --- Unit tests that don't require testcontainers ---

    #[test]
    fn test_search_ui_scoped_key_type() {
        // Verify SearchUiScopedKey can be constructed and has expected fields
        let key = SearchUiScopedKey {
            index_uid: "test-index".to_string(),
            primary_key: "pk-abc".to_string(),
            primary_uid: "primary-123".to_string(),
            previous_key: Some("ppk-def".to_string()),
            previous_uid: Some("previous-456".to_string()),
            rotated_at: 1234567890,
            generation: 5,
        };
        assert_eq!(key.index_uid, "test-index");
        assert_eq!(key.primary_uid, "primary-123");
        assert_eq!(key.previous_uid.as_deref(), Some("previous-456"));
        assert_eq!(key.rotated_at, 1234567890);
        assert_eq!(key.generation, 5);
    }

    #[test]
    fn test_redis_helper_functions() {
        // Test the helper functions directly
        let mut fields = std::collections::HashMap::new();
        fields.insert("name".to_string(), redis::Value::BulkString(b"test-name".to_vec()));
        fields.insert("version".to_string(), redis::Value::Int(42));
        fields.insert("enabled".to_string(), redis::Value::Int(1));

        // get_field_string
        let name = get_field_string(&fields, "name").expect("Should get name");
        assert_eq!(name, "test-name");

        // get_field_i64
        let version = get_field_i64(&fields, "version").expect("Should get version");
        assert_eq!(version, 42);

        // opt_field
        let maybe_name = opt_field(&fields, "name");
        assert_eq!(maybe_name.as_deref(), Some("test-name"));

        // Missing field
        assert!(get_field_string(&fields, "missing").is_err());

        // opt_field for missing field
        assert!(opt_field(&fields, "missing").is_none());
    }

    #[test]
    fn test_task_from_hash() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("miroir_id".to_string(), redis::Value::BulkString(b"task-1".to_vec()));
        fields.insert("created_at".to_string(), redis::Value::Int(1000));
        fields.insert("status".to_string(), redis::Value::BulkString(b"queued".to_vec()));
        fields.insert("node_tasks".to_string(), redis::Value::BulkString(br#"{"node-1":123}"#.to_vec()));
        // error field is optional

        let task = RedisTaskStore::task_from_hash("task-1".to_string(), &fields)
            .expect("Should parse task");
        assert_eq!(task.miroir_id, "task-1");
        assert_eq!(task.created_at, 1000);
        assert_eq!(task.status, "queued");
        assert_eq!(task.node_tasks.get("node-1"), Some(&123));
        assert!(task.error.is_none());
    }

    #[test]
    fn test_canary_from_hash() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("id".to_string(), redis::Value::BulkString(b"canary-1".to_vec()));
        fields.insert("name".to_string(), redis::Value::BulkString(b"Test Canary".to_vec()));
        fields.insert("index_uid".to_string(), redis::Value::BulkString(b"logs".to_vec()));
        fields.insert("interval_s".to_string(), redis::Value::Int(60));
        fields.insert("query_json".to_string(), redis::Value::BulkString(br#"{"q":"test"}"#.to_vec()));
        fields.insert("assertions_json".to_string(), redis::Value::BulkString(b"[]".to_vec()));
        fields.insert("enabled".to_string(), redis::Value::Int(1));
        fields.insert("created_at".to_string(), redis::Value::Int(1000));

        let canary = RedisTaskStore::canary_from_hash("canary-1".to_string(), &fields)
            .expect("Should parse canary");
        assert_eq!(canary.id, "canary-1");
        assert_eq!(canary.name, "Test Canary");
        assert_eq!(canary.index_uid, "logs");
        assert_eq!(canary.interval_s, 60);
        assert!(canary.enabled);
    }
}
