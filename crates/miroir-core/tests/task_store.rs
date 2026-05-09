//! Property tests and integration tests for the task store.
//! Phase 3 feature — not tested in Phase 0.

#![cfg(feature = "task-store")]

use miroir_core::task_store::*;
use miroir_core::task_store::{SqliteTaskStore, TaskStore};
use proptest::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::NamedTempFile;

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
        target_uids: vec!["index-1".to_string()],
        version: 1,
        created_at: 1234567890,
        updated_at: 1234567890,
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
        response: "{\"result\":\"ok\"}".to_string(),
        status_code: 200,
        created_at: 1234567890,
    };

    // Record
    store.idempotency_record(&entry).await.unwrap();

    // Check
    let retrieved = store.idempotency_check("req-123").await.unwrap().unwrap();

    assert_eq!(retrieved.key, entry.key);
    assert_eq!(retrieved.response, entry.response);
    assert_eq!(retrieved.status_code, entry.status_code);

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
        lease_id: "lease-1".to_string(),
        holder: "pod-1".to_string(),
        acquired_at: now,
        expires_at: now + 10_000, // 10 seconds later
    };

    // Acquire
    let acquired = store.leader_lease_acquire(&lease1).await.unwrap();
    assert!(acquired);

    // Get current lease
    let current = store.leader_lease_get().await.unwrap().unwrap();
    assert_eq!(current.lease_id, lease1.lease_id);
    assert_eq!(current.holder, lease1.holder);

    // Try to acquire again (should fail - lease still valid)
    let lease2 = LeaderLease {
        lease_id: "lease-2".to_string(),
        holder: "pod-2".to_string(),
        acquired_at: now + 1000,
        expires_at: now + 15_000,
    };

    let acquired2 = store.leader_lease_acquire(&lease2).await.unwrap();
    assert!(!acquired2);

    // Release
    store.leader_lease_release("lease-1").await.unwrap();

    // Now acquisition should succeed
    let acquired3 = store.leader_lease_acquire(&lease2).await.unwrap();
    assert!(acquired3);
}

/// Property test: job enqueue/dequeue.
#[tokio::test]
async fn job_enqueue_dequeue() {
    let store = create_temp_store().await;

    let job1 = Job {
        job_id: "job-1".to_string(),
        job_type: "test".to_string(),
        parameters: "{}".to_string(),
        status: JobStatus::Enqueued,
        worker_id: None,
        result: None,
        error: None,
        created_at: 1234567890,
        started_at: None,
        completed_at: None,
    };

    let job2 = Job {
        job_id: "job-2".to_string(),
        job_type: "test".to_string(),
        parameters: "{}".to_string(),
        status: JobStatus::Enqueued,
        worker_id: None,
        result: None,
        error: None,
        created_at: 1234567891,
        started_at: None,
        completed_at: None,
    };

    // Enqueue
    store.job_enqueue(&job1).await.unwrap();
    store.job_enqueue(&job2).await.unwrap();

    // Dequeue (should get job-1 first)
    let dequeued = store.job_dequeue("worker-1").await.unwrap().unwrap();
    assert_eq!(dequeued.job_id, "job-1");
    assert_eq!(dequeued.status, JobStatus::Processing);
    assert_eq!(dequeued.worker_id, Some("worker-1".to_string()));

    // Dequeue again (should get job-2)
    let dequeued2 = store.job_dequeue("worker-1").await.unwrap().unwrap();
    assert_eq!(dequeued2.job_id, "job-2");

    // Update job status
    store
        .job_update_status("job-1", JobStatus::Succeeded, Some("{\"ok\":true}"))
        .await
        .unwrap();

    let updated = store.job_get("job-1").await.unwrap().unwrap();
    assert_eq!(updated.status, JobStatus::Succeeded);
    assert_eq!(updated.result, Some("{\"ok\":true}".to_string()));
    assert!(updated.completed_at.is_some());
}

/// Property test: canary run history.
#[tokio::test]
async fn canary_run_history() {
    let store = create_temp_store().await;

    let canary = Canary {
        name: "test-canary".to_string(),
        index: "test-index".to_string(),
        query: "*".to_string(),
        min_results: 1,
        max_results: 1000,
        interval_s: 60,
        enabled: true,
        created_at: 1234567890,
        updated_at: 1234567890,
    };

    store.canary_upsert(&canary).await.unwrap();

    // Insert runs
    let run1 = CanaryRun {
        run_id: "run-1".to_string(),
        canary_name: "test-canary".to_string(),
        ran_at: 1234567950,
        passed: true,
        result_count: 100,
        error: None,
        latency_ms: 50,
    };

    let run2 = CanaryRun {
        run_id: "run-2".to_string(),
        canary_name: "test-canary".to_string(),
        ran_at: 1234567890,
        passed: false,
        result_count: 0,
        error: Some("no results".to_string()),
        latency_ms: 45,
    };

    store.canary_run_insert(&run1).await.unwrap();
    store.canary_run_insert(&run2).await.unwrap();

    // List runs
    let runs = store.canary_run_list("test-canary", 10).await.unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].run_id, "run-1"); // Most recent first (ran_at: 1234567950)
    assert_eq!(runs[1].run_id, "run-2");
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
                map.insert(
                    "node-1".to_string(),
                    NodeTask {
                        task_uid: 123,
                        status: NodeTaskStatus::Processing,
                    },
                );
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
        assert_eq!(retrieved.node_tasks["node-1"].task_uid, 123);
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
        sink: "kafka".to_string(),
        index: "test-index".to_string(),
        cursor: "offset-123".to_string(),
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
    assert_eq!(retrieved.sink, cursor.sink);
    assert_eq!(retrieved.index, cursor.index);
    assert_eq!(retrieved.cursor, cursor.cursor);

    // Update cursor
    let cursor2 = CdcCursor {
        cursor: "offset-456".to_string(),
        ..cursor
    };
    store.cdc_cursor_set(&cursor2).await.unwrap();

    let retrieved2 = store
        .cdc_cursor_get("kafka", "test-index")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved2.cursor, "offset-456");
}

/// Property test: tenant map.
#[tokio::test]
async fn tenant_map_roundtrip() {
    let store = create_temp_store().await;

    let tenant = Tenant {
        api_key: "key-123".to_string(),
        tenant_id: "tenant-1".to_string(),
        name: "Test Tenant".to_string(),
        capabilities: "{\"max_qps\":100}".to_string(),
        created_at: 1234567890,
        updated_at: 1234567890,
    };

    // Insert tenant
    store.tenant_upsert(&tenant).await.unwrap();

    // Get tenant
    let retrieved = store.tenant_get("key-123").await.unwrap().unwrap();
    assert_eq!(retrieved.api_key, tenant.api_key);
    assert_eq!(retrieved.tenant_id, tenant.tenant_id);
    assert_eq!(retrieved.name, tenant.name);

    // List tenants
    let tenants = store.tenant_list().await.unwrap();
    assert_eq!(tenants.len(), 1);
    assert_eq!(tenants[0].api_key, "key-123");

    // Delete tenant
    store.tenant_delete("key-123").await.unwrap();
    let retrieved2 = store.tenant_get("key-123").await.unwrap();
    assert!(retrieved2.is_none());
}

/// Property test: rollover policies.
#[tokio::test]
async fn rollover_policy_roundtrip() {
    let store = create_temp_store().await;

    let policy = RolloverPolicy {
        name: "daily-rollover".to_string(),
        index_pattern: "logs-*".to_string(),
        max_age_days: Some(7),
        max_size_bytes: Some(10_737_418_240), // 10 GiB
        max_docs: None,
        enabled: true,
        created_at: 1234567890,
        updated_at: 1234567890,
    };

    // Insert policy
    store.rollover_policy_upsert(&policy).await.unwrap();

    // Get policy
    let retrieved = store
        .rollover_policy_get("daily-rollover")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.name, policy.name);
    assert_eq!(retrieved.max_age_days, policy.max_age_days);

    // List policies
    let policies = store.rollover_policy_list().await.unwrap();
    assert_eq!(policies.len(), 1);
}

/// Property test: search UI config.
#[tokio::test]
async fn search_ui_config_roundtrip() {
    let store = create_temp_store().await;

    let config = SearchUiConfig {
        index: "products".to_string(),
        config: "{\"theme\":\"dark\",\"facets\":[\"category\",\"price\"]}".to_string(),
        created_at: 1234567890,
        updated_at: 1234567890,
    };

    // Insert config
    store.search_ui_config_upsert(&config).await.unwrap();

    // Get config
    let retrieved = store
        .search_ui_config_get("products")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.index, config.index);
    assert_eq!(retrieved.config, config.config);

    // List configs
    let configs = store.search_ui_config_list().await.unwrap();
    assert_eq!(configs.len(), 1);
}

/// Property test: admin sessions.
#[tokio::test]
async fn admin_session_roundtrip() {
    let store = create_temp_store().await;

    let session = AdminSession {
        session_id: "session-123".to_string(),
        user_id: "user-1".to_string(),
        created_at: 1234567890,
        expires_at: 1234654290, // 24 hours later
        revoked: false,
    };

    // Insert session
    store.admin_session_upsert(&session).await.unwrap();

    // Get session
    let retrieved = store
        .admin_session_get("session-123")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retrieved.session_id, session.session_id);
    assert_eq!(retrieved.user_id, session.user_id);
    assert!(!retrieved.revoked);

    // Revoke session
    store.admin_session_revoke("session-123").await.unwrap();

    let is_revoked = store.admin_session_is_revoked("session-123").await.unwrap();
    assert!(is_revoked);
}

/// Property test: sessions.
#[tokio::test]
async fn session_roundtrip() {
    let store = create_temp_store().await;

    let session = Session {
        session_id: "session-456".to_string(),
        index: "products".to_string(),
        settings_version: 5,
        created_at: 1234567890,
        expires_at: 1234654290,
    };

    // Insert session
    store.session_upsert(&session).await.unwrap();

    // Get session
    let retrieved = store.session_get("session-456").await.unwrap().unwrap();
    assert_eq!(retrieved.session_id, session.session_id);
    assert_eq!(retrieved.index, session.index);
    assert_eq!(retrieved.settings_version, session.settings_version);

    // Delete session
    store.session_delete("session-456").await.unwrap();

    let retrieved2 = store.session_get("session-456").await.unwrap();
    assert!(retrieved2.is_none());
}

/// Proptest: task list with filtering.
fn task_list_strategy() -> impl Strategy<Value = Vec<Task>> {
    let task_status_strategy = prop_oneof![
        Just(TaskStatus::Enqueued),
        Just(TaskStatus::Processing),
        Just(TaskStatus::Succeeded),
        Just(TaskStatus::Failed),
        Just(TaskStatus::Canceled),
    ];

    // Use safe u64 values that fit in SQLite's signed 64-bit integer
    let created_at_strategy = 0u64..9223372036854775807u64;

    prop::collection::vec(
        (created_at_strategy, task_status_strategy),
        0..100,
    )
    .prop_map(|items| {
        items
            .into_iter()
            .enumerate()
            .map(|(i, (created_at, status))| Task {
                // Always use enumerated index to ensure unique IDs
                miroir_id: format!("task-{i}"),
                created_at,
                status,
                node_tasks: HashMap::new(),
                error: None,
            })
            .collect()
    })
}

proptest! {
    #[test]
    fn prop_task_list_filter_by_status(tasks in task_list_strategy()) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = rt.block_on(create_temp_store());

        // Insert all tasks
        for task in &tasks {
            rt.block_on(store.task_insert(task)).unwrap();
        }

        // List all tasks
        let filter = TaskFilter::default();
        let all_tasks = rt.block_on(store.task_list(&filter)).unwrap();
        assert_eq!(all_tasks.len(), tasks.len());

        // Filter by Succeeded status
        let filter = TaskFilter {
            status: Some(TaskStatus::Succeeded),
            ..Default::default()
        };
        let succeeded_tasks = rt.block_on(store.task_list(&filter)).unwrap();
        let expected_count = tasks.iter().filter(|t| t.status == TaskStatus::Succeeded).count();
        assert_eq!(succeeded_tasks.len(), expected_count);
    }
}

/// Health check test.
#[tokio::test]
async fn health_check() {
    let store = create_temp_store().await;
    let healthy = store.health_check().await.unwrap();
    assert!(healthy);
}
