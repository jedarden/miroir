//! Redis backend for the task store.

use super::error::{Result, TaskStoreError};
use super::schema::*;
use super::TaskStore;
use redis::AsyncCommands;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Hash an API key using SHA256 and return as hex string for Redis key.
#[allow(dead_code)]
fn hash_api_key(api_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Redis task store implementation.
pub struct RedisTaskStore {
    client: Arc<redis::Client>,
}

impl RedisTaskStore {
    /// Create a new Redis task store.
    pub async fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let store = Self {
            client: Arc::new(client),
        };
        Ok(store)
    }

    /// Get a connection from the pool.
    async fn get_conn(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(Into::into)
    }

    /// Generate a Redis key for a table.
    fn table_key(&self, table: &str, id: &str) -> String {
        format!("miroir:{table}:{id}")
    }

    /// Generate a Redis key for a table's index.
    fn index_key(&self, table: &str) -> String {
        format!("miroir:{table}:_index")
    }
}

#[async_trait::async_trait]
impl TaskStore for RedisTaskStore {
    async fn initialize(&self) -> Result<()> {
        let mut conn = self.get_conn().await?;

        // Set schema version
        let version_key = "miroir:schema_version";
        let current_version: Option<i64> = conn.get(version_key).await?;

        if current_version.is_none() {
            conn.set::<_, _, ()>(version_key, SCHEMA_VERSION).await?;
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
        let mut conn = self.get_conn().await?;
        let version: i64 = conn.get("miroir:schema_version").await?;
        Ok(version)
    }

    async fn task_insert(&self, task: &Task) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("tasks", &task.miroir_id);

        // Serialize task
        let data = serde_json::to_string(task)?;

        // Store task data
        conn.set::<_, _, ()>(&key, data).await?;

        // Add to index
        let index_key = self.index_key("tasks");
        conn.sadd::<_, _, ()>(&index_key, &task.miroir_id).await?;

        Ok(())
    }

    async fn task_get(&self, miroir_id: &str) -> Result<Option<Task>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("tasks", miroir_id);

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let task: Task = serde_json::from_str(&d)?;
                Ok(Some(task))
            }
            None => Ok(None),
        }
    }

    async fn task_update_status(&self, miroir_id: &str, status: TaskStatus) -> Result<()> {
        let mut task = self
            .task_get(miroir_id)
            .await?
            .ok_or_else(|| TaskStoreError::NotFound(miroir_id.to_string()))?;
        task.status = status;
        self.task_insert(&task).await
    }

    async fn task_update_node(&self, miroir_id: &str, node_id: &str, task_uid: u64) -> Result<()> {
        let mut task = self
            .task_get(miroir_id)
            .await?
            .ok_or_else(|| TaskStoreError::NotFound(miroir_id.to_string()))?;
        task.node_tasks.insert(node_id.to_string(), task_uid);
        self.task_insert(&task).await
    }

    async fn task_list(&self, filter: &TaskFilter) -> Result<Vec<Task>> {
        let mut conn = self.get_conn().await?;
        let index_key = self.index_key("tasks");

        // Get all task IDs from index
        let all_ids: Vec<String> = conn.smembers(&index_key).await?;

        let mut tasks = Vec::new();
        for id in all_ids {
            if let Some(task) = self.task_get(&id).await? {
                // Apply filters
                if let Some(status) = filter.status {
                    if task.status != status {
                        continue;
                    }
                }
                tasks.push(task);
            }
        }

        // Sort by created_at descending
        tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        // Apply limit/offset
        let offset = filter.offset.unwrap_or(0);
        let limit = filter.limit.unwrap_or(tasks.len());

        Ok(tasks.into_iter().skip(offset).take(limit).collect())
    }

    async fn node_settings_version_get(&self, index: &str, node_id: &str) -> Result<Option<i64>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("node_settings_version", &format!("{index}:{node_id}"));

        let version: Option<i64> = conn.get(&key).await?;
        Ok(version)
    }

    async fn node_settings_version_set(
        &self,
        index: &str,
        node_id: &str,
        version: i64,
    ) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("node_settings_version", &format!("{index}:{node_id}"));
        let now = chrono::Utc::now().timestamp_millis() as u64;

        // Store version with timestamp
        let data = serde_json::json!({
            "version": version,
            "updated_at": now,
        });
        conn.set::<_, _, ()>(&key, data.to_string()).await?;

        Ok(())
    }

    async fn alias_upsert(&self, alias: &Alias) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("aliases", &alias.name);

        let data = serde_json::to_string(alias)?;
        conn.set::<_, _, ()>(&key, data).await?;

        // Add to index
        let index_key = self.index_key("aliases");
        conn.sadd::<_, _, ()>(&index_key, &alias.name).await?;

        Ok(())
    }

    async fn alias_get(&self, name: &str) -> Result<Option<Alias>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("aliases", name);

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let alias: Alias = serde_json::from_str(&d)?;
                Ok(Some(alias))
            }
            None => Ok(None),
        }
    }

    async fn alias_delete(&self, name: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("aliases", name);
        let index_key = self.index_key("aliases");

        conn.del::<_, ()>(&key).await?;
        conn.srem::<_, _, ()>(&index_key, name).await?;

        Ok(())
    }

    async fn alias_list(&self) -> Result<Vec<Alias>> {
        let mut conn = self.get_conn().await?;
        let index_key = self.index_key("aliases");

        let all_names: Vec<String> = conn.smembers(&index_key).await?;

        let mut aliases = Vec::new();
        for name in all_names {
            if let Some(alias) = self.alias_get(&name).await? {
                aliases.push(alias);
            }
        }

        Ok(aliases)
    }

    async fn session_upsert(&self, session: &Session) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("sessions", &session.session_id);

        let data = serde_json::to_string(session)?;
        // Calculate TTL in seconds from the ttl field (Unix millis)
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let ttl_seconds = if session.ttl > now {
            (session.ttl - now) / 1000
        } else {
            1 // Minimum 1 second
        };
        conn.set_ex::<_, _, ()>(&key, data, ttl_seconds).await?;

        Ok(())
    }

    async fn session_get(&self, session_id: &str) -> Result<Option<Session>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("sessions", session_id);

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let session: Session = serde_json::from_str(&d)?;
                Ok(Some(session))
            }
            None => Ok(None),
        }
    }

    async fn session_delete(&self, session_id: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("sessions", session_id);
        conn.del::<_, ()>(&key).await?;
        Ok(())
    }

    async fn session_delete_by_index(&self, _index: &str) -> Result<()> {
        // This is expensive in Redis - we need to scan all sessions
        // For now, we'll return an error to discourage this pattern
        Err(TaskStoreError::InvalidData(
            "session_delete_by_index is not efficient in Redis mode".to_string(),
        ))
    }

    async fn idempotency_check(&self, key: &str) -> Result<Option<IdempotencyEntry>> {
        let mut conn = self.get_conn().await?;
        let redis_key = self.table_key("idempotency_cache", key);

        let data: Option<String> = conn.get(&redis_key).await?;
        match data {
            Some(d) => {
                let entry: IdempotencyEntry = serde_json::from_str(&d)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    async fn idempotency_record(&self, entry: &IdempotencyEntry) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let redis_key = self.table_key("idempotency_cache", &entry.key);

        let data = serde_json::to_string(entry)?;
        // Set with 1 hour expiration
        conn.set_ex::<_, _, ()>(&redis_key, data, 3600).await?;

        Ok(())
    }

    async fn idempotency_prune(&self, _before_ts: u64) -> Result<u64> {
        // Redis handles expiration automatically via TTL
        Ok(0)
    }

    async fn job_enqueue(&self, job: &Job) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("jobs", &job.id);

        let data = serde_json::to_string(job)?;
        conn.set::<_, _, ()>(&key, data).await?;

        // Add to enqueued queue
        conn.rpush::<_, _, ()>("miroir:jobs:enqueued", &job.id)
            .await?;

        Ok(())
    }

    async fn job_dequeue(&self, worker_id: &str) -> Result<Option<Job>> {
        let mut conn = self.get_conn().await?;

        // Pop from enqueued queue (pop single element)
        let job_id: Option<String> = conn.lpop("miroir:jobs:enqueued", None).await?;

        if let Some(job_id) = job_id {
            // Get the job
            let mut job = self
                .job_get(&job_id)
                .await?
                .ok_or_else(|| TaskStoreError::NotFound(job_id.clone()))?;

            // Update state
            job.state = JobState::InProgress;
            job.claimed_by = Some(worker_id.to_string());
            job.claim_expires_at = Some(chrono::Utc::now().timestamp_millis() as u64 + 300000); // 5 min lease

            // Save updated job
            self.job_enqueue(&job).await?;

            // Remove from enqueued queue (we already popped it)
            conn.lrem::<_, _, ()>("miroir:jobs:enqueued", 1, &job_id)
                .await?;

            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    async fn job_update_status(
        &self,
        job_id: &str,
        status: JobState,
        result: Option<&str>,
    ) -> Result<()> {
        let mut job = self
            .job_get(job_id)
            .await?
            .ok_or_else(|| TaskStoreError::NotFound(job_id.to_string()))?;

        job.state = status;

        // Update progress with result if provided
        if let Some(r) = result {
            job.progress = r.to_string();
        }

        // Clear claim when terminal
        if matches!(status, JobState::Completed | JobState::Failed) {
            job.claimed_by = None;
            job.claim_expires_at = None;
        }

        self.job_enqueue(&job).await?;
        Ok(())
    }

    async fn job_get(&self, job_id: &str) -> Result<Option<Job>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("jobs", job_id);

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let job: Job = serde_json::from_str(&d)?;
                Ok(Some(job))
            }
            None => Ok(None),
        }
    }

    async fn job_list(&self, status: Option<JobState>, limit: usize) -> Result<Vec<Job>> {
        // Get all job IDs from the enqueued queue
        let mut conn = self.get_conn().await?;
        let all_ids: Vec<String> = conn.lrange("miroir:jobs:enqueued", 0, -1).await?;

        let mut jobs = Vec::new();
        for id in all_ids {
            if let Some(job) = self.job_get(&id).await? {
                if status.is_none() || Some(job.state) == status {
                    jobs.push(job);
                }
            }
        }

        // Sort by ID (as proxy for time) and limit
        jobs.sort_by(|a, b| b.id.cmp(&a.id));
        jobs.truncate(limit);

        Ok(jobs)
    }

    async fn leader_lease_acquire(&self, lease: &LeaderLease) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        let key = "miroir:leader_lease";

        // Calculate TTL from expires_at to now
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let ttl = if lease.expires_at > now {
            (lease.expires_at - now) / 1000
        } else {
            1 // Minimum 1 second
        };
        #[allow(clippy::cast_possible_truncation)]
        let ttl_usize = ttl as usize;

        // Use the options API to set with NX and EX
        let acquired: bool = redis::cmd("SET")
            .arg(key)
            .arg(serde_json::to_string(lease)?)
            .arg("NX")
            .arg("EX")
            .arg(ttl_usize)
            .query_async(&mut conn)
            .await?;

        Ok(acquired)
    }

    async fn leader_lease_release(&self, _lease_id: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        conn.del::<_, ()>("miroir:leader_lease").await?;
        Ok(())
    }

    async fn leader_lease_get(&self) -> Result<Option<LeaderLease>> {
        let mut conn = self.get_conn().await?;
        let data: Option<String> = conn.get("miroir:leader_lease").await?;

        match data {
            Some(d) => {
                let lease: LeaderLease = serde_json::from_str(&d)?;
                Ok(Some(lease))
            }
            None => Ok(None),
        }
    }

    async fn canary_upsert(&self, canary: &Canary) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("canaries", &canary.name);

        let data = serde_json::to_string(canary)?;
        conn.set::<_, _, ()>(&key, data).await?;

        let index_key = self.index_key("canaries");
        conn.sadd::<_, _, ()>(&index_key, &canary.name).await?;

        Ok(())
    }

    async fn canary_get(&self, name: &str) -> Result<Option<Canary>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("canaries", name);

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let canary: Canary = serde_json::from_str(&d)?;
                Ok(Some(canary))
            }
            None => Ok(None),
        }
    }

    async fn canary_delete(&self, name: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("canaries", name);
        let index_key = self.index_key("canaries");

        conn.del::<_, ()>(&key).await?;
        conn.srem::<_, _, ()>(&index_key, name).await?;

        Ok(())
    }

    async fn canary_list(&self) -> Result<Vec<Canary>> {
        let mut conn = self.get_conn().await?;
        let index_key = self.index_key("canaries");

        let all_names: Vec<String> = conn.smembers(&index_key).await?;

        let mut canaries = Vec::new();
        for name in all_names {
            if let Some(canary) = self.canary_get(&name).await? {
                canaries.push(canary);
            }
        }

        Ok(canaries)
    }

    async fn canary_run_insert(&self, run: &CanaryRun) -> Result<()> {
        let mut conn = self.get_conn().await?;
        // Use canary_id:ran_at as the unique key for the run
        let run_key = format!("{}:{}", run.canary_id, run.ran_at);
        let key = self.table_key("canary_runs", &run_key);

        let data = serde_json::to_string(run)?;
        conn.set::<_, _, ()>(&key, data).await?;

        // Add to canary-specific runs list
        let canary_runs_key = format!("miroir:canary_runs:{}:index", run.canary_id);
        conn.lpush::<_, _, ()>(&canary_runs_key, &run_key).await?;

        Ok(())
    }

    async fn canary_run_list(&self, canary_name: &str, limit: usize) -> Result<Vec<CanaryRun>> {
        let mut conn = self.get_conn().await?;
        let canary_runs_key = format!("miroir:canary_runs:{canary_name}:index");

        let run_ids: Vec<String> = conn.lrange(&canary_runs_key, 0, limit as isize - 1).await?;

        let mut runs = Vec::new();
        for run_id in run_ids {
            let key = self.table_key("canary_runs", &run_id);
            let data: Option<String> = conn.get(&key).await?;

            if let Some(d) = data {
                if let Ok(run) = serde_json::from_str::<CanaryRun>(&d) {
                    runs.push(run);
                }
            }
        }

        Ok(runs)
    }

    async fn canary_run_prune(&self, _before_ts: u64) -> Result<u64> {
        // Redis would need a different approach for pruning
        // For now, rely on TTL
        Ok(0)
    }

    async fn cdc_cursor_get(&self, sink: &str, index: &str) -> Result<Option<CdcCursor>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("cdc_cursors", &format!("{sink}:{index}"));

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let cursor: CdcCursor = serde_json::from_str(&d)?;
                Ok(Some(cursor))
            }
            None => Ok(None),
        }
    }

    async fn cdc_cursor_set(&self, cursor: &CdcCursor) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key(
            "cdc_cursors",
            &format!("{}:{}", cursor.sink_name, cursor.index_uid),
        );

        let data = serde_json::to_string(cursor)?;
        conn.set::<_, _, ()>(&key, data).await?;

        Ok(())
    }

    async fn cdc_cursor_list(&self, _sink: &str) -> Result<Vec<CdcCursor>> {
        // This requires scanning, which is expensive
        // For now, return empty list
        Ok(Vec::new())
    }

    async fn tenant_upsert(&self, tenant: &Tenant) -> Result<()> {
        let mut conn = self.get_conn().await?;
        // Convert hash bytes to hex string for Redis key
        let key_hex = hex::encode(&tenant.api_key_hash);
        let key = self.table_key("tenant_map", &key_hex);

        let data = serde_json::to_string(tenant)?;
        conn.set::<_, _, ()>(&key, data).await?;

        let index_key = self.index_key("tenant_map");
        conn.sadd::<_, _, ()>(&index_key, &key_hex).await?;

        Ok(())
    }

    async fn tenant_get(&self, api_key: &str) -> Result<Option<Tenant>> {
        let mut conn = self.get_conn().await?;
        // Hash the API key and convert to hex for lookup
        use std::hash::Hasher;
        use twox_hash::XxHash64;
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(api_key.as_bytes());
        let hash = hasher.finish();
        let key_hex = format!("{hash:016x}");
        let key = self.table_key("tenant_map", &key_hex);

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let tenant: Tenant = serde_json::from_str(&d)?;
                Ok(Some(tenant))
            }
            None => Ok(None),
        }
    }

    async fn tenant_delete(&self, api_key: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        // Hash the API key and convert to hex for lookup
        use std::hash::Hasher;
        use twox_hash::XxHash64;
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(api_key.as_bytes());
        let hash = hasher.finish();
        let key_hex = format!("{hash:016x}");
        let key = self.table_key("tenant_map", &key_hex);
        let index_key = self.index_key("tenant_map");

        conn.del::<_, ()>(&key).await?;
        conn.srem::<_, _, ()>(&index_key, &key_hex).await?;

        Ok(())
    }

    async fn tenant_list(&self) -> Result<Vec<Tenant>> {
        let mut conn = self.get_conn().await?;
        let index_key = self.index_key("tenant_map");

        let all_keys: Vec<String> = conn.smembers(&index_key).await?;

        let mut tenants = Vec::new();
        for key in all_keys {
            if let Some(tenant) = self.tenant_get(&key).await? {
                tenants.push(tenant);
            }
        }

        Ok(tenants)
    }

    async fn rollover_policy_upsert(&self, policy: &RolloverPolicy) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("rollover_policies", &policy.name);

        let data = serde_json::to_string(policy)?;
        conn.set::<_, _, ()>(&key, data).await?;

        let index_key = self.index_key("rollover_policies");
        conn.sadd::<_, _, ()>(&index_key, &policy.name).await?;

        Ok(())
    }

    async fn rollover_policy_get(&self, name: &str) -> Result<Option<RolloverPolicy>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("rollover_policies", name);

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let policy: RolloverPolicy = serde_json::from_str(&d)?;
                Ok(Some(policy))
            }
            None => Ok(None),
        }
    }

    async fn rollover_policy_delete(&self, name: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("rollover_policies", name);
        let index_key = self.index_key("rollover_policies");

        conn.del::<_, ()>(&key).await?;
        conn.srem::<_, _, ()>(&index_key, name).await?;

        Ok(())
    }

    async fn rollover_policy_list(&self) -> Result<Vec<RolloverPolicy>> {
        let mut conn = self.get_conn().await?;
        let index_key = self.index_key("rollover_policies");

        let all_names: Vec<String> = conn.smembers(&index_key).await?;

        let mut policies = Vec::new();
        for name in all_names {
            if let Some(policy) = self.rollover_policy_get(&name).await? {
                policies.push(policy);
            }
        }

        Ok(policies)
    }

    async fn search_ui_config_upsert(&self, config: &SearchUiConfig) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("search_ui_config", &config.index_uid);

        let data = serde_json::to_string(config)?;
        conn.set::<_, _, ()>(&key, data).await?;

        let index_key = self.index_key("search_ui_config");
        conn.sadd::<_, _, ()>(&index_key, &config.index_uid).await?;

        Ok(())
    }

    async fn search_ui_config_get(&self, index: &str) -> Result<Option<SearchUiConfig>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("search_ui_config", index);

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let config: SearchUiConfig = serde_json::from_str(&d)?;
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    async fn search_ui_config_delete(&self, index: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("search_ui_config", index);
        let index_key = self.index_key("search_ui_config");

        conn.del::<_, ()>(&key).await?;
        conn.srem::<_, _, ()>(&index_key, index).await?;

        Ok(())
    }

    async fn search_ui_config_list(&self) -> Result<Vec<SearchUiConfig>> {
        let mut conn = self.get_conn().await?;
        let index_key = self.index_key("search_ui_config");

        let all_indices: Vec<String> = conn.smembers(&index_key).await?;

        let mut configs = Vec::new();
        for index in all_indices {
            if let Some(config) = self.search_ui_config_get(&index).await? {
                configs.push(config);
            }
        }

        Ok(configs)
    }

    async fn admin_session_upsert(&self, session: &AdminSession) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("admin_sessions", &session.session_id);

        let data = serde_json::to_string(session)?;
        conn.set_ex::<_, _, ()>(&key, data, (session.expires_at - session.created_at) / 1000)
            .await?;

        Ok(())
    }

    async fn admin_session_get(&self, session_id: &str) -> Result<Option<AdminSession>> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("admin_sessions", session_id);

        let data: Option<String> = conn.get(&key).await?;
        match data {
            Some(d) => {
                let session: AdminSession = serde_json::from_str(&d)?;
                Ok(Some(session))
            }
            None => Ok(None),
        }
    }

    async fn admin_session_delete(&self, session_id: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = self.table_key("admin_sessions", session_id);
        conn.del::<_, ()>(&key).await?;
        Ok(())
    }

    async fn admin_session_revoke(&self, session_id: &str) -> Result<()> {
        let mut session = self
            .admin_session_get(session_id)
            .await?
            .ok_or_else(|| TaskStoreError::NotFound(session_id.to_string()))?;

        session.revoked = 1;
        self.admin_session_upsert(&session).await?;

        // Publish to Pub/Sub for instant propagation
        let mut conn = self.get_conn().await?;
        let _: usize = conn
            .publish("miroir:admin_session:revoked", session_id)
            .await?;

        Ok(())
    }

    async fn admin_session_is_revoked(&self, session_id: &str) -> Result<bool> {
        if let Some(session) = self.admin_session_get(session_id).await? {
            Ok(session.revoked != 0)
        } else {
            Ok(false)
        }
    }

    // Redis-specific operations

    async fn ratelimit_increment(
        &self,
        key: &str,
        window_s: u64,
        _limit: u64,
    ) -> Result<(u64, u64)> {
        let mut conn = self.get_conn().await?;
        let redis_key = format!("miroir:ratelimit:{key}");

        // Increment and get TTL
        let count: u64 = conn.incr(&redis_key, 1).await?;

        if count == 1 {
            // First request in window - set expiration
            conn.expire::<_, ()>(&redis_key, window_s as i64).await?;
        }

        let ttl: i64 = conn.ttl(&redis_key).await?;

        Ok((count, ttl.max(0) as u64))
    }

    async fn ratelimit_set_backoff(&self, key: &str, duration_s: u64) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let redis_key = format!("miroir:ratelimit:backoff:{key}");
        conn.set_ex::<_, _, ()>(&redis_key, "1", duration_s).await?;
        Ok(())
    }

    async fn ratelimit_check_backoff(&self, key: &str) -> Result<Option<u64>> {
        let mut conn = self.get_conn().await?;
        let redis_key = format!("miroir:ratelimit:backoff:{key}");

        let exists: bool = conn.exists(&redis_key).await?;
        if exists {
            let ttl: i64 = conn.ttl(&redis_key).await?;
            Ok(Some(ttl.max(0) as u64))
        } else {
            Ok(None)
        }
    }

    async fn cdc_overflow_check(&self, sink: &str) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        let key = format!("miroir:cdc:overflow:{sink}");
        let exists: bool = conn.exists(&key).await?;
        Ok(exists)
    }

    async fn cdc_overflow_size(&self, sink: &str) -> Result<u64> {
        let mut conn = self.get_conn().await?;
        let key = format!("miroir:cdc:overflow:{sink}");
        let size: u64 = conn.strlen(&key).await?;
        Ok(size)
    }

    async fn cdc_overflow_append(&self, sink: &str, data: &[u8]) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = format!("miroir:cdc:overflow:{sink}");

        // Check if appending would exceed 1 GiB limit
        let current_size: u64 = conn.strlen(&key).await?;
        if current_size + data.len() as u64 > 1_073_741_824 {
            return Err(TaskStoreError::InvalidData(
                "CDC overflow buffer would exceed 1 GiB limit".to_string(),
            ));
        }

        conn.append::<_, _, ()>(&key, data).await?;
        Ok(())
    }

    async fn cdc_overflow_clear(&self, sink: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let key = format!("miroir:cdc:overflow:{sink}");
        conn.del::<_, ()>(&key).await?;
        Ok(())
    }

    async fn scoped_key_set(&self, index: &str, key: &str, expires_at: u64) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let redis_key = format!("miroir:search_ui_scoped_key:{index}");

        let ttl = (expires_at - chrono::Utc::now().timestamp_millis() as u64) / 1000;
        conn.set_ex::<_, _, ()>(&redis_key, key, ttl).await?;

        Ok(())
    }

    async fn scoped_key_get(&self, index: &str) -> Result<Option<String>> {
        let mut conn = self.get_conn().await?;
        let redis_key = format!("miroir:search_ui_scoped_key:{index}");

        let key: Option<String> = conn.get(&redis_key).await?;
        Ok(key)
    }

    async fn scoped_key_observe(&self, pod: &str, index: &str, key: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let redis_key = format!("miroir:search_ui_scoped_key_observed:{pod}:{index}");

        conn.set::<_, _, ()>(&redis_key, key).await?;
        Ok(())
    }

    async fn scoped_key_has_observed(&self, pod: &str, index: &str, key: &str) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        let redis_key = format!("miroir:search_ui_scoped_key_observed:{pod}:{index}");

        let current: Option<String> = conn.get(&redis_key).await?;
        Ok(current.as_deref() == Some(key))
    }

    async fn health_check(&self) -> Result<bool> {
        let mut conn = self.get_conn().await?;
        redis::cmd("PING").query_async::<_, ()>(&mut conn).await?;
        Ok(true)
    }
}
