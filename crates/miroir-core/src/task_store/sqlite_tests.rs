//! SQLite task store tests (tables 1-7 from plan §4).

use super::schema::*;
use super::sqlite::SqliteTaskStore;
use super::TaskStore;
use std::collections::HashMap;

#[tokio::test]
async fn test_initialize_schema() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Verify schema version is set
    let version = store.schema_version().await.unwrap();
    assert_eq!(version, SCHEMA_VERSION);

    // Re-initializing should be idempotent
    store.initialize().await.unwrap();
    let version2 = store.schema_version().await.unwrap();
    assert_eq!(version2, SCHEMA_VERSION);
}

#[tokio::test]
async fn test_tasks_crud() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Insert a task
    let mut node_tasks = HashMap::new();
    node_tasks.insert("node-0".to_string(), 42);
    node_tasks.insert("node-1".to_string(), 17);

    let task = Task {
        miroir_id: "task-1".to_string(),
        created_at: 12345,
        status: TaskStatus::Enqueued,
        node_tasks: node_tasks.clone(),
        error: None,
    };

    store.task_insert(&task).await.unwrap();

    // Get the task back
    let retrieved = store.task_get("task-1").await.unwrap().unwrap();
    assert_eq!(retrieved.miroir_id, "task-1");
    assert_eq!(retrieved.created_at, 12345);
    assert_eq!(retrieved.status, TaskStatus::Enqueued);
    assert_eq!(retrieved.node_tasks, node_tasks);
    assert!(retrieved.error.is_none());

    // Update status
    store
        .task_update_status("task-1", TaskStatus::Processing)
        .await
        .unwrap();
    let updated = store.task_get("task-1").await.unwrap().unwrap();
    assert_eq!(updated.status, TaskStatus::Processing);

    // Update node task
    store
        .task_update_node("task-1", "node-2", 99)
        .await
        .unwrap();
    let with_node = store.task_get("task-1").await.unwrap().unwrap();
    assert_eq!(with_node.node_tasks.get("node-2"), Some(&99));

    // List tasks
    let tasks = store
        .task_list(&TaskFilter {
            status: Some(TaskStatus::Processing),
            limit: Some(10),
            offset: None,
        })
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].miroir_id, "task-1");

    // Non-existent task returns None
    let missing = store.task_get("nonexistent").await.unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_node_settings_version() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Initially no version
    let version = store
        .node_settings_version_get("index-1", "node-0")
        .await
        .unwrap();
    assert!(version.is_none());

    // Set a version
    store
        .node_settings_version_set("index-1", "node-0", 42)
        .await
        .unwrap();

    // Get it back
    let version = store
        .node_settings_version_get("index-1", "node-0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(version, 42);

    // Update the version
    store
        .node_settings_version_set("index-1", "node-0", 43)
        .await
        .unwrap();
    let version = store
        .node_settings_version_get("index-1", "node-0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(version, 43);

    // Different (index, node) pair is independent
    let version = store
        .node_settings_version_get("index-1", "node-1")
        .await
        .unwrap();
    assert!(version.is_none());
}

#[tokio::test]
async fn test_aliases_single_target() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Create a single-target alias
    let alias = Alias {
        name: "my-alias".to_string(),
        kind: AliasKind::Single,
        current_uid: Some("index-1".to_string()),
        target_uids: None,
        version: 1,
        created_at: 12345,
        history: vec![],
    };

    store.alias_upsert(&alias).await.unwrap();

    // Get it back
    let retrieved = store.alias_get("my-alias").await.unwrap().unwrap();
    assert_eq!(retrieved.name, "my-alias");
    assert_eq!(retrieved.kind, AliasKind::Single);
    assert_eq!(retrieved.current_uid, Some("index-1".to_string()));
    assert_eq!(retrieved.version, 1);

    // Update with new target
    let updated = Alias {
        current_uid: Some("index-2".to_string()),
        version: 2,
        ..alias.clone()
    };
    store.alias_upsert(&updated).await.unwrap();

    let retrieved = store.alias_get("my-alias").await.unwrap().unwrap();
    assert_eq!(retrieved.current_uid, Some("index-2".to_string()));
    assert_eq!(retrieved.version, 2);

    // List aliases
    let aliases = store.alias_list().await.unwrap();
    assert_eq!(aliases.len(), 1);

    // Delete alias
    store.alias_delete("my-alias").await.unwrap();
    let missing = store.alias_get("my-alias").await.unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_aliases_multi_target() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Create a multi-target alias
    let alias = Alias {
        name: "multi-alias".to_string(),
        kind: AliasKind::Multi,
        current_uid: None,
        target_uids: Some(vec!["index-1".to_string(), "index-2".to_string()]),
        version: 1,
        created_at: 12345,
        history: vec![],
    };

    store.alias_upsert(&alias).await.unwrap();

    // Get it back
    let retrieved = store.alias_get("multi-alias").await.unwrap().unwrap();
    assert_eq!(retrieved.kind, AliasKind::Multi);
    assert_eq!(
        retrieved.target_uids,
        Some(vec!["index-1".to_string(), "index-2".to_string()])
    );
    assert!(retrieved.current_uid.is_none());
}

#[tokio::test]
async fn test_sessions() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Create a session
    let session = Session {
        session_id: "session-1".to_string(),
        last_write_mtask_id: Some("task-1".to_string()),
        last_write_at: Some(12345),
        pinned_group: Some(0),
        min_settings_version: 1,
        ttl: 67890,
    };

    store.session_upsert(&session).await.unwrap();

    // Get it back
    let retrieved = store.session_get("session-1").await.unwrap().unwrap();
    assert_eq!(retrieved.session_id, "session-1");
    assert_eq!(retrieved.last_write_mtask_id, Some("task-1".to_string()));
    assert_eq!(retrieved.last_write_at, Some(12345));
    assert_eq!(retrieved.pinned_group, Some(0));
    assert_eq!(retrieved.min_settings_version, 1);
    assert_eq!(retrieved.ttl, 67890);

    // Update session
    let updated = Session {
        last_write_mtask_id: Some("task-2".to_string()),
        last_write_at: Some(99999),
        pinned_group: None,
        ..session.clone()
    };
    store.session_upsert(&updated).await.unwrap();

    let retrieved = store.session_get("session-1").await.unwrap().unwrap();
    assert_eq!(retrieved.last_write_mtask_id, Some("task-2".to_string()));
    assert_eq!(retrieved.pinned_group, None);

    // Delete session
    store.session_delete("session-1").await.unwrap();
    let missing = store.session_get("session-1").await.unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_idempotency_cache() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Record an idempotency entry
    let entry = IdempotencyEntry {
        key: "key-1".to_string(),
        body_sha256: vec![1, 2, 3, 4],
        miroir_task_id: "task-1".to_string(),
        expires_at: 99999,
    };

    store.idempotency_record(&entry).await.unwrap();

    // Check it exists
    let retrieved = store.idempotency_check("key-1").await.unwrap().unwrap();
    assert_eq!(retrieved.key, "key-1");
    assert_eq!(retrieved.body_sha256, vec![1, 2, 3, 4]);
    assert_eq!(retrieved.miroir_task_id, "task-1".to_string());
    assert_eq!(retrieved.expires_at, 99999);

    // Update the entry
    let updated = IdempotencyEntry {
        miroir_task_id: "task-2".to_string(),
        ..entry.clone()
    };
    store.idempotency_record(&updated).await.unwrap();

    let retrieved = store.idempotency_check("key-1").await.unwrap().unwrap();
    assert_eq!(retrieved.miroir_task_id, "task-2".to_string());

    // Prune old entries
    store
        .idempotency_prune(50000)
        .await
        .unwrap();
    let retrieved = store.idempotency_check("key-1").await.unwrap();
    assert!(retrieved.is_some()); // Still there

    store
        .idempotency_prune(200000)
        .await
        .unwrap();
    let retrieved = store.idempotency_check("key-1").await.unwrap();
    assert!(retrieved.is_none()); // Pruned
}

#[tokio::test]
async fn test_jobs() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Enqueue a job
    let job = Job {
        id: "job-1".to_string(),
        job_type: "test_job".to_string(),
        params: r#"{"param": "value"}"#.to_string(),
        state: JobState::Queued,
        claimed_by: None,
        claim_expires_at: None,
        progress: r#"{"status": "starting"}"#.to_string(),
    };

    store.job_enqueue(&job).await.unwrap();

    // Get it back
    let retrieved = store.job_get("job-1").await.unwrap().unwrap();
    assert_eq!(retrieved.id, "job-1");
    assert_eq!(retrieved.job_type, "test_job");
    assert_eq!(retrieved.state, JobState::Queued);
    assert!(retrieved.claimed_by.is_none());

    // Dequeue the job
    let claimed = store.job_dequeue("worker-1").await.unwrap().unwrap();
    assert_eq!(claimed.id, "job-1");
    assert_eq!(claimed.state, JobState::InProgress);
    assert_eq!(claimed.claimed_by, Some("worker-1".to_string()));
    assert!(claimed.claim_expires_at.is_some());

    // Try to dequeue again - should get next job (none)
    let next = store.job_dequeue("worker-2").await.unwrap();
    assert!(next.is_none());

    // Update job status
    store
        .job_update_status("job-1", JobState::Completed, Some("done"))
        .await
        .unwrap();

    let completed = store.job_get("job-1").await.unwrap().unwrap();
    assert_eq!(completed.state, JobState::Completed);

    // List jobs by status
    let queued = store
        .job_list(Some(JobState::Queued), 10)
        .await
        .unwrap();
    assert_eq!(queued.len(), 0);

    let all = store.job_list(None, 10).await.unwrap();
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn test_leader_lease() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Acquire a lease with far-future expiration
    let now = chrono::Utc::now().timestamp_millis() as u64;
    let lease = LeaderLease {
        scope: "test-scope".to_string(),
        holder: "pod-1".to_string(),
        expires_at: now + 60_000, // 1 minute from now
    };

    let acquired = store.leader_lease_acquire(&lease).await.unwrap();
    assert!(acquired);

    // Get the lease
    let retrieved = store.leader_lease_get().await.unwrap().unwrap();
    assert_eq!(retrieved.scope, "test-scope");
    assert_eq!(retrieved.holder, "pod-1");

    // Try to acquire again - should fail (lease still valid)
    let lease2 = LeaderLease {
        holder: "pod-2".to_string(),
        expires_at: now + 90_000, // 90 seconds from now
        scope: "test-scope".to_string(),
    };
    let acquired = store.leader_lease_acquire(&lease2).await.unwrap();
    assert!(!acquired);

    // Release the lease
    store.leader_lease_release("test-scope").await.unwrap();

    // Now can acquire again
    let acquired = store.leader_lease_acquire(&lease2).await.unwrap();
    assert!(acquired);

    let retrieved = store.leader_lease_get().await.unwrap().unwrap();
    assert_eq!(retrieved.holder, "pod-2");
}

#[tokio::test]
async fn test_concurrent_writes() {
    let store = std::sync::Arc::new(SqliteTaskStore::new(":memory:").await.unwrap());
    store.initialize().await.unwrap();

    // Spawn multiple concurrent writes
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let store = std::sync::Arc::clone(&store);
            tokio::spawn(async move {
                let task = Task {
                    miroir_id: format!("task-{}", i),
                    created_at: 12345 + i as u64,
                    status: TaskStatus::Enqueued,
                    node_tasks: HashMap::new(),
                    error: None,
                };
                store.task_insert(&task).await.unwrap();
            })
        })
        .collect();

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all tasks were inserted
    let tasks = store
        .task_list(&TaskFilter {
            status: None,
            limit: Some(100),
            offset: None,
        })
        .await
        .unwrap();
    assert_eq!(tasks.len(), 10);
}

#[tokio::test]
async fn test_health_check() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    let healthy = store.health_check().await.unwrap();
    assert!(healthy);
}

#[tokio::test]
async fn test_persistence() {
    // Use a temporary file instead of :memory:
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Create and populate
    {
        let store = SqliteTaskStore::new(&db_path).await.unwrap();
        store.initialize().await.unwrap();

        let task = Task {
            miroir_id: "persistent-task".to_string(),
            created_at: 12345,
            status: TaskStatus::Enqueued,
            node_tasks: HashMap::new(),
            error: None,
        };
        store.task_insert(&task).await.unwrap();
    }

    // Reopen and verify
    {
        let store = SqliteTaskStore::new(&db_path).await.unwrap();
        store.initialize().await.unwrap();

        let retrieved = store.task_get("persistent-task").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().miroir_id, "persistent-task");
    }
}

#[tokio::test]
async fn test_task_with_error() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    let task = Task {
        miroir_id: "failed-task".to_string(),
        created_at: 12345,
        status: TaskStatus::Failed,
        node_tasks: HashMap::new(),
        error: Some("something went wrong".to_string()),
    };

    store.task_insert(&task).await.unwrap();

    let retrieved = store.task_get("failed-task").await.unwrap().unwrap();
    assert_eq!(retrieved.status, TaskStatus::Failed);
    assert_eq!(retrieved.error, Some("something went wrong".to_string()));
}

#[tokio::test]
async fn test_task_filter_by_status() {
    let store = SqliteTaskStore::new(":memory:").await.unwrap();
    store.initialize().await.unwrap();

    // Insert tasks with different statuses
    for (i, status) in [TaskStatus::Enqueued, TaskStatus::Processing, TaskStatus::Succeeded].iter().enumerate() {
        let task = Task {
            miroir_id: format!("task-{}", i),
            created_at: 12345 + i as u64,
            status: *status,
            node_tasks: HashMap::new(),
            error: None,
        };
        store.task_insert(&task).await.unwrap();
    }

    // Filter by enqueued
    let enqueued = store
        .task_list(&TaskFilter {
            status: Some(TaskStatus::Enqueued),
            limit: None,
            offset: None,
        })
        .await
        .unwrap();
    assert_eq!(enqueued.len(), 1);

    // Get all tasks
    let all = store
        .task_list(&TaskFilter {
            status: None,
            limit: None,
            offset: None,
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 3);

    // Test pagination
    let page1 = store
        .task_list(&TaskFilter {
            status: None,
            limit: Some(2),
            offset: Some(0),
        })
        .await
        .unwrap();
    assert_eq!(page1.len(), 2);

    let page2 = store
        .task_list(&TaskFilter {
            status: None,
            limit: Some(2),
            offset: Some(2),
        })
        .await
        .unwrap();
    assert_eq!(page2.len(), 1);
}
