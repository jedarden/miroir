//! Property tests and integration tests for the task store.
//! Phase 3 feature — not tested in Phase 0.

#![cfg(feature = "task-store")]

use miroir_core::task_store::*;
use miroir_core::task_store::{SqliteTaskStore, TaskStore};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::task::JoinSet;

/// Helper function to create a temporary SQLite store.
async fn create_temp_store() -> Arc<SqliteTaskStore> {
    let temp_file = NamedTempFile::new().unwrap();
    let store = SqliteTaskStore::new(temp_file.path()).await.unwrap();
    store.initialize().await.unwrap();
    Arc::new(store)
}

/// Property test: (insert, get) round-trip for tasks.
#[tokio::test]
async fn task_insert_get_roundtrip() {
    let store = create_temp_store().await;

    let task = Task {
        miroir_id: "test-1".to_string(),
        created_at: 1234567890,
        status: TaskStatus::Enqueued,
        node_tasks: HashMap::new(),
        error: None,
    };

    // Insert
    store.task_insert(&task).await.unwrap();

    // Get
    let retrieved = store.task_get("test-1").await.unwrap().unwrap();

    assert_eq!(retrieved.miroir_id, task.miroir_id);
    assert_eq!(retrieved.created_at, task.created_at);
    assert_eq!(retrieved.status, task.status);
    assert_eq!(retrieved.node_tasks, task.node_tasks);
    assert_eq!(retrieved.error, task.error);
}

/// Property test: upsert semantics for aliases.
#[tokio::test]
async fn alias_upsert_roundtrip() {
    let store = create_temp_store().await;

    let alias1 = Alias {
        name: "test-alias".to_string(),
        kind: AliasKind::Single,
        current_uid: Some("index-1".to_string()),
        target_uids: Some(vec!["index-1".to_string()]),
        version: 1,
        created_at: 1234567890,
        history: vec![],
    };

    // Insert
    store.alias_upsert(&alias1).await.unwrap();

    // Get
    let retrieved = store.alias_get("test-alias").await.unwrap().unwrap();

    assert_eq!(retrieved.name, alias1.name);
    assert_eq!(retrieved.kind, alias1.kind);
    assert_eq!(retrieved.current_uid, alias1.current_uid);

    // Update (upsert)
    let alias2 = Alias {
        version: 2,
        current_uid: Some("index-2".to_string()),
        ..alias1.clone()
    };

    store.alias_upsert(&alias2).await.unwrap();

    let retrieved2 = store.alias_get("test-alias").await.unwrap().unwrap();

    assert_eq!(retrieved2.version, 2);
    assert_eq!(retrieved2.current_uid, Some("index-2".to_string()));
}

/// Property test: idempotency cache semantics.
#[tokio::test]
async fn idempotency_cache_roundtrip() {
    let store = create_temp_store().await;

    let entry = IdempotencyEntry {
        key: "req-123".to_string(),
        body_sha256: vec![1, 2, 3],
        miroir_task_id: "task-123".to_string(),
        expires_at: 1234567890,
    };

    // Record
    store.idempotency_record(&entry).await.unwrap();

    // Check
    let retrieved = store.idempotency_check("req-123").await.unwrap().unwrap();

    assert_eq!(retrieved.key, entry.key);

    // Duplicate record (should work)
    store.idempotency_record(&entry).await.unwrap();

    // Prune old entries
    let pruned = store.idempotency_prune(2000000000).await.unwrap();
    assert_eq!(pruned, 1);

    // Check that entry is gone
    let retrieved = store.idempotency_check("req-123").await.unwrap();
    assert!(retrieved.is_none());
}

/// Property test: leader lease acquisition.
#[tokio::test]
async fn leader_lease_acquire_renew() {
    let store = create_temp_store().await;

    let now = chrono::Utc::now().timestamp_millis() as u64;

    let lease1 = LeaderLease {
        scope: "test-scope".to_string(),
        holder: "pod-1".to_string(),
        expires_at: now + 10_000, // 10 seconds later
    };

    // Acquire
    let acquired = store.leader_lease_acquire(&lease1).await.unwrap();
    assert!(acquired);

    // Get current lease
    let current = store.leader_lease_get().await.unwrap().unwrap();
    assert_eq!(current.holder, lease1.holder);

    // Try to acquire again (should fail - lease still valid)
    let lease2 = LeaderLease {
        scope: "test-scope".to_string(),
        holder: "pod-2".to_string(),
        expires_at: now + 15_000,
    };

    let acquired2 = store.leader_lease_acquire(&lease2).await.unwrap();
    assert!(!acquired2);

    // Release
    store.leader_lease_release("test-scope").await.unwrap();

    // Now acquisition should succeed
    let acquired3 = store.leader_lease_acquire(&lease2).await.unwrap();
    assert!(acquired3);
}

/// Integration test: restart survival.
#[tokio::test]
async fn restart_survival() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();

    // Create store and insert data
    {
        let store = SqliteTaskStore::new(&path).await.unwrap();
        store.initialize().await.unwrap();

        let task = Task {
            miroir_id: "restart-test".to_string(),
            created_at: 1234567890,
            status: TaskStatus::Processing,
            node_tasks: {
                let mut map = HashMap::new();
                map.insert("node-1".to_string(), 123u64);
                map
            },
            error: None,
        };

        store.task_insert(&task).await.unwrap();

        // Update status
        store
            .task_update_status("restart-test", TaskStatus::Succeeded)
            .await
            .unwrap();
    }

    // Simulate restart: close connection, reopen, and verify data survived
    {
        let store = SqliteTaskStore::new(&path).await.unwrap();
        store.initialize().await.unwrap();

        let retrieved = store.task_get("restart-test").await.unwrap().unwrap();

        assert_eq!(retrieved.miroir_id, "restart-test");
        assert_eq!(retrieved.status, TaskStatus::Succeeded);
        assert_eq!(retrieved.node_tasks.len(), 1);
        assert_eq!(retrieved.node_tasks["node-1"], 123);
    }
}

/// Integration test: schema version check.
#[tokio::test]
async fn schema_version_check() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();

    // Initialize store
    {
        let store = SqliteTaskStore::new(&path).await.unwrap();
        store.initialize().await.unwrap();

        let version = store.schema_version().await.unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    // Reopen and verify version
    {
        let store = SqliteTaskStore::new(&path).await.unwrap();
        store.initialize().await.unwrap();

        let version = store.schema_version().await.unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }
}

/// Property test: node settings version.
#[tokio::test]
async fn node_settings_version_roundtrip() {
    let store = create_temp_store().await;

    // Set version
    store
        .node_settings_version_set("test-index", "node-1", 5)
        .await
        .unwrap();

    // Get version
    let version = store
        .node_settings_version_get("test-index", "node-1")
        .await
        .unwrap();
    assert_eq!(version, Some(5));

    // Update version
    store
        .node_settings_version_set("test-index", "node-1", 10)
        .await
        .unwrap();
    let version2 = store
        .node_settings_version_get("test-index", "node-1")
        .await
        .unwrap();
    assert_eq!(version2, Some(10));

    // Different node
    let version3 = store
        .node_settings_version_get("test-index", "node-2")
        .await
        .unwrap();
    assert_eq!(version3, None);
}

/// Property test: CDC cursors.
#[tokio::test]
async fn cdc_cursor_roundtrip() {
    let store = create_temp_store().await;

    let cursor = CdcCursor {
        sink_name: "kafka".to_string(),
        index_uid: "test-index".to_string(),
        last_event_seq: 123,
        updated_at: 1234567890,
    };

    // Set cursor
    store.cdc_cursor_set(&cursor).await.unwrap();

    // Get cursor
    let retrieved = store
        .cdc_cursor_get("kafka", "test-index")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.sink_name, cursor.sink_name);
    assert_eq!(retrieved.index_uid, cursor.index_uid);
    assert_eq!(retrieved.last_event_seq, cursor.last_event_seq);
}

/// Property test: tenant map.
#[tokio::test]
async fn tenant_map_roundtrip() {
    let store = create_temp_store().await;

    // Use the actual API key (the implementation will hash it)
    let api_key = "my-secret-api-key";

    // Manually compute the hash to insert into the database
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(api_key.as_bytes());
    let api_key_hash: Vec<u8> = hasher.finalize().to_vec();

    let tenant = Tenant {
        api_key_hash: api_key_hash.clone(),
        tenant_id: "tenant-1".to_string(),
        group_id: Some(1),
    };

    // Insert tenant
    store.tenant_upsert(&tenant).await.unwrap();

    // Get tenant (using the actual API key)
    let retrieved = store.tenant_get(api_key).await.unwrap().unwrap();
    assert_eq!(retrieved.tenant_id, tenant.tenant_id);

    // Delete tenant
    store.tenant_delete(api_key).await.unwrap();
    let retrieved2 = store.tenant_get(api_key).await.unwrap();
    assert!(retrieved2.is_none());
}

/// Property test: sessions.
#[tokio::test]
async fn session_roundtrip() {
    let store = create_temp_store().await;

    let session = Session {
        session_id: "session-456".to_string(),
        last_write_mtask_id: Some("task-123".to_string()),
        last_write_at: Some(1234567890),
        pinned_group: Some(1),
        min_settings_version: 5,
        ttl: 1234654290,
    };

    // Insert session
    store.session_upsert(&session).await.unwrap();

    // Get session
    let retrieved = store.session_get("session-456").await.unwrap().unwrap();
    assert_eq!(retrieved.session_id, session.session_id);
    assert_eq!(retrieved.min_settings_version, session.min_settings_version);

    // Delete session
    store.session_delete("session-456").await.unwrap();

    let retrieved2 = store.session_get("session-456").await.unwrap();
    assert!(retrieved2.is_none());
}

/// Property test: jobs queue and dequeue.
#[tokio::test]
async fn job_queue_dequeue_roundtrip() {
    let store = create_temp_store().await;

    let job = Job {
        id: "job-1".to_string(),
        job_type: "test_job".to_string(),
        params: r#"{"param1": "value1"}"#.to_string(),
        state: JobState::Queued,
        claimed_by: None,
        claim_expires_at: None,
        progress: r#"{"status": "queued"}"#.to_string(),
    };

    // Enqueue job
    store.job_enqueue(&job).await.unwrap();

    // Get job
    let retrieved = store.job_get("job-1").await.unwrap().unwrap();
    assert_eq!(retrieved.id, job.id);
    assert_eq!(retrieved.state, JobState::Queued);

    // List jobs
    let jobs = store.job_list(Some(JobState::Queued), 10).await.unwrap();
    assert_eq!(jobs.len(), 1);

    // Dequeue job
    let claimed = store.job_dequeue("worker-1").await.unwrap().unwrap();
    assert_eq!(claimed.id, "job-1");
    assert_eq!(claimed.state, JobState::InProgress);
    assert_eq!(claimed.claimed_by, Some("worker-1".to_string()));

    // Update status
    store
        .job_update_status("job-1", JobState::Completed, Some(r#"{"status": "done"}"#))
        .await
        .unwrap();

    let final_job = store.job_get("job-1").await.unwrap().unwrap();
    assert_eq!(final_job.state, JobState::Completed);
}

/// Health check test.
#[tokio::test]
async fn health_check() {
    let store = create_temp_store().await;
    let healthy = store.health_check().await.unwrap();
    assert!(healthy);
}

/// Concurrent write test: verify WAL mode prevents deadlocks.
#[tokio::test]
async fn concurrent_writes_no_deadlock() {
    let temp_file = NamedTempFile::new().unwrap();
    let store = Arc::new(SqliteTaskStore::new(temp_file.path()).await.unwrap());
    store.initialize().await.unwrap();

    let mut join_set = JoinSet::new();

    // Spawn 10 concurrent tasks writing to the database
    for i in 0..10 {
        let store_clone = Arc::clone(&store);
        join_set.spawn(async move {
            let task = Task {
                miroir_id: format!("concurrent-{}", i),
                created_at: 1234567890 + i as u64,
                status: TaskStatus::Enqueued,
                node_tasks: HashMap::new(),
                error: None,
            };

            // Perform multiple operations
            store_clone.task_insert(&task).await.unwrap();
            store_clone
                .task_get(&format!("concurrent-{}", i))
                .await
                .unwrap();
            store_clone
                .task_update_status(&format!("concurrent-{}", i), TaskStatus::Processing)
                .await
                .unwrap();
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = join_set.join_next().await {
        result.unwrap();
    }

    // Verify all tasks were written
    for i in 0..10 {
        let task = store.task_get(&format!("concurrent-{}", i)).await.unwrap();
        assert!(task.is_some());
        assert_eq!(task.unwrap().status, TaskStatus::Processing);
    }
}
