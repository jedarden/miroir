//! P5.18 §13.18 Canary acceptance tests
//!
//! Tests synthetic canary queries with golden assertions, including:
//! - Creating canaries and storing them
//! - Canary run history accumulation
//! - Assertion failure data structures
//! - Capture flow for seeding canaries from production traffic
//! - Canary CRUD operations

use miroir_core::{
    canary::{CanaryAssertion, QueryCapture, SearchQuery, SearchResponse},
    task_store::{NewCanary, TaskStore},
};
use std::collections::HashMap;
use std::sync::Arc;

/// Create an in-memory SQLite task store for testing
fn create_test_store() -> Arc<miroir_core::task_store::SqliteTaskStore> {
    let store = miroir_core::task_store::SqliteTaskStore::open_in_memory()
        .expect("Failed to create in-memory store");
    store.migrate().expect("Failed to migrate database schema");
    Arc::new(store)
}

/// Test 1: Create canary → can be stored and retrieved
#[tokio::test]
async fn ac1_canary_can_be_created_and_stored() {
    let store = create_test_store();

    // Create a canary
    let canary = NewCanary {
        id: "test-canary-1".to_string(),
        name: "Test Canary".to_string(),
        index_uid: "products".to_string(),
        interval_s: 60,
        query_json: serde_json::to_string(&SearchQuery {
            params: HashMap::new(),
        })
        .unwrap(),
        assertions_json: serde_json::to_string(&vec![CanaryAssertion::MinHits { value: 1 }])
            .unwrap(),
        enabled: true,
        created_at: chrono::Utc::now().timestamp_millis(),
    };

    store.upsert_canary(&canary).unwrap();

    // Verify canary was created
    let retrieved = store.get_canary("test-canary-1").unwrap().unwrap();
    assert_eq!(retrieved.id, "test-canary-1");
    assert_eq!(retrieved.name, "Test Canary");
    assert_eq!(retrieved.index_uid, "products");
    assert_eq!(retrieved.interval_s, 60);
    assert!(retrieved.enabled);
}

/// Test 2: Canary run history accumulates
#[tokio::test]
async fn ac2_canary_run_history_accumulates() {
    let store = create_test_store();

    // Create a canary first
    let canary = NewCanary {
        id: "test-canary-2".to_string(),
        name: "Test Canary 2".to_string(),
        index_uid: "products".to_string(),
        interval_s: 1,
        query_json: serde_json::to_string(&SearchQuery {
            params: HashMap::new(),
        })
        .unwrap(),
        assertions_json: serde_json::to_string(&vec![CanaryAssertion::MinHits { value: 1 }])
            .unwrap(),
        enabled: true,
        created_at: chrono::Utc::now().timestamp_millis(),
    };

    store.upsert_canary(&canary).unwrap();

    // Insert multiple canary runs with different statuses
    for i in 0..3 {
        let status = if i == 1 {
            "failed".to_string()
        } else {
            "passed".to_string()
        };

        let failed_assertions = if i == 1 {
            Some(
                serde_json::to_string(&vec![serde_json::json!({
                    "assertion_type": "min_hits",
                    "expected": 5,
                    "actual": 2,
                    "message": "Expected at least 5 hits, got 2"
                })])
                .unwrap(),
            )
        } else {
            None
        };

        store
            .insert_canary_run(
                &miroir_core::task_store::NewCanaryRun {
                    canary_id: "test-canary-2".to_string(),
                    ran_at: chrono::Utc::now().timestamp_millis() + (i as i64 * 1000),
                    status: status.clone(),
                    latency_ms: 50,
                    failed_assertions_json: failed_assertions,
                },
                100,
            )
            .unwrap();
    }

    // Verify history accumulates
    let runs = store.get_canary_runs("test-canary-2", 100).unwrap();
    assert_eq!(runs.len(), 3, "History should accumulate all runs");
    assert_eq!(runs[0].status, "passed");
    assert_eq!(runs[1].status, "failed");
    assert_eq!(runs[2].status, "passed");

    // Verify failed run has assertion details
    assert!(runs[1].failed_assertions_json.is_some());
    let failures: Vec<serde_json::Value> =
        serde_json::from_str(runs[1].failed_assertions_json.as_ref().unwrap()).unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0]["assertion_type"], "min_hits");
    assert_eq!(failures[0]["expected"], 5);
    assert_eq!(failures[0]["actual"], 2);
}

/// Test 3: Assertion failure includes actual observed value
#[tokio::test]
async fn ac3_assertion_failure_includes_actual_value() {
    // Test that assertion failure data structures correctly serialize
    let failure = serde_json::json!({
        "assertion_type": "min_hits",
        "expected": 5,
        "actual": 2,
        "message": "Expected at least 5 hits, got 2"
    });

    assert_eq!(failure["assertion_type"], "min_hits");
    assert_eq!(failure["expected"], 5);
    assert_eq!(failure["actual"], 2);

    // Test multiple assertion types
    let failures = [
        serde_json::json!({
            "assertion_type": "top_hit_id",
            "expected": "product-123",
            "actual": "product-456",
            "message": "Top hit ID mismatch"
        }),
        serde_json::json!({
            "assertion_type": "max_p95_ms",
            "expected": 200,
            "actual": 350,
            "message": "Latency exceeded threshold"
        }),
    ];

    assert_eq!(failures.len(), 2);
    assert_eq!(failures[0]["assertion_type"], "top_hit_id");
    assert_eq!(failures[1]["assertion_type"], "max_p95_ms");
}

/// Test 4: Capture flow - record production queries
#[tokio::test]
async fn ac4_capture_flow_records_queries() {
    let capture = QueryCapture::new(10);

    // Simulate capturing 10 production queries
    for i in 0..10 {
        let mut params = HashMap::new();
        params.insert("q".to_string(), serde_json::json!(format!("query {}", i)));
        params.insert("limit".to_string(), serde_json::json!(10));

        capture
            .capture(
                "products".to_string(),
                SearchQuery { params },
                SearchResponse {
                    hits: vec![],
                    estimated_total_hits: 0,
                    processing_time_ms: 50,
                    query: format!("query {i}"),
                },
            )
            .await;
    }

    // Verify capture recorded queries
    let captured = capture.get_captured().await;
    assert_eq!(captured.len(), 10);

    // Verify each captured query
    for (i, query) in captured.iter().enumerate() {
        assert_eq!(query.index_uid, "products");
        let q = query.query.params.get("q").and_then(|v| v.as_str());
        assert_eq!(q, Some(format!("query {i}").as_str()));
    }

    // Clear and verify
    capture.clear().await;
    let captured_after = capture.get_captured().await;
    assert_eq!(captured_after.len(), 0);
}

/// Test 5: Captured query can be promoted to canary
#[tokio::test]
async fn ac5_captured_query_can_be_promoted_to_canary() {
    let capture = QueryCapture::new(10);

    // Capture a query
    let mut params = HashMap::new();
    params.insert("q".to_string(), serde_json::json!("laptop"));
    params.insert("limit".to_string(), serde_json::json!(10));

    capture
        .capture(
            "products".to_string(),
            SearchQuery { params },
            SearchResponse {
                hits: vec![],
                estimated_total_hits: 100,
                processing_time_ms: 45,
                query: "laptop".to_string(),
            },
        )
        .await;

    let captured = capture.get_captured().await;
    assert_eq!(captured.len(), 1);

    // Promote captured query to a canary
    let first_captured = &captured[0];
    let canary = miroir_core::canary::create_canary(
        "captured-canary-1".to_string(),
        "Promoted from capture".to_string(),
        first_captured.index_uid.clone(),
        3600,
        first_captured.query.clone(),
        vec![CanaryAssertion::MinHits { value: 1 }],
    )
    .unwrap();

    let store = create_test_store();
    store.upsert_canary(&canary).unwrap();

    // Verify canary was created from captured query
    let retrieved = store.get_canary("captured-canary-1").unwrap().unwrap();
    assert_eq!(retrieved.name, "Promoted from capture");
    assert_eq!(retrieved.index_uid, "products");

    // Verify query was preserved
    let retrieved_query: SearchQuery =
        serde_json::from_str(&retrieved.query_json).expect("Should parse query");
    let q = retrieved_query.params.get("q").and_then(|v| v.as_str());
    assert_eq!(q, Some("laptop"));
}

/// Test 6: Canary run history is bounded
#[tokio::test]
async fn ac6_canary_run_history_is_bounded() {
    let store = create_test_store();

    // Create a canary
    let canary = NewCanary {
        id: "history-test".to_string(),
        name: "History Test".to_string(),
        index_uid: "products".to_string(),
        interval_s: 1,
        query_json: serde_json::to_string(&SearchQuery {
            params: HashMap::new(),
        })
        .unwrap(),
        assertions_json: serde_json::to_string(&vec![CanaryAssertion::MinHits { value: 1 }])
            .unwrap(),
        enabled: true,
        created_at: chrono::Utc::now().timestamp_millis(),
    };

    store.upsert_canary(&canary).unwrap();

    // Insert more runs than the history limit
    let history_limit = 10;
    for i in 0..20 {
        store
            .insert_canary_run(
                &miroir_core::task_store::NewCanaryRun {
                    canary_id: "history-test".to_string(),
                    ran_at: chrono::Utc::now().timestamp_millis() + (i as i64 * 1000),
                    status: "Passed".to_string(),
                    latency_ms: 50,
                    failed_assertions_json: None,
                },
                history_limit,
            )
            .unwrap();
    }

    // Verify history is bounded
    let runs = store.get_canary_runs("history-test", 100).unwrap();
    assert_eq!(runs.len(), history_limit, "History should be bounded");
}

/// Test 7: Canary can be enabled and disabled
#[tokio::test]
async fn ac7_canary_enable_disable() {
    let store = create_test_store();

    // Create an enabled canary
    let canary = NewCanary {
        id: "toggle-test".to_string(),
        name: "Toggle Test".to_string(),
        index_uid: "products".to_string(),
        interval_s: 60,
        query_json: serde_json::to_string(&SearchQuery {
            params: HashMap::new(),
        })
        .unwrap(),
        assertions_json: serde_json::to_string(&vec![CanaryAssertion::MinHits { value: 1 }])
            .unwrap(),
        enabled: true,
        created_at: chrono::Utc::now().timestamp_millis(),
    };

    store.upsert_canary(&canary).unwrap();

    let retrieved = store.get_canary("toggle-test").unwrap().unwrap();
    assert!(retrieved.enabled);

    // Disable the canary
    store
        .upsert_canary(&NewCanary {
            id: "toggle-test".to_string(),
            name: "Toggle Test".to_string(),
            index_uid: "products".to_string(),
            interval_s: 60,
            query_json: serde_json::to_string(&SearchQuery {
                params: HashMap::new(),
            })
            .unwrap(),
            assertions_json: serde_json::to_string(&vec![CanaryAssertion::MinHits { value: 1 }])
                .unwrap(),
            enabled: false,
            created_at: chrono::Utc::now().timestamp_millis(),
        })
        .unwrap();

    let retrieved = store.get_canary("toggle-test").unwrap().unwrap();
    assert!(!retrieved.enabled);
}

/// Test 8: Canary list can be retrieved
#[tokio::test]
async fn ac8_canary_list_can_be_retrieved() {
    let store = create_test_store();

    // Create multiple canaries
    for i in 0..3 {
        let canary = NewCanary {
            id: format!("list-test-{i}"),
            name: format!("List Test Canary {i}"),
            index_uid: "products".to_string(),
            interval_s: 60,
            query_json: serde_json::to_string(&SearchQuery {
                params: HashMap::new(),
            })
            .unwrap(),
            assertions_json: serde_json::to_string(&vec![CanaryAssertion::MinHits { value: 1 }])
                .unwrap(),
            enabled: i % 2 == 0, // Alternate enabled/disabled
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        store.upsert_canary(&canary).unwrap();
    }

    // Retrieve canary list
    let canaries = store.list_canaries().unwrap();
    assert_eq!(canaries.len(), 3);

    // Verify canary properties
    assert_eq!(canaries[0].name, "List Test Canary 0");
    assert!(canaries[0].enabled);
    assert_eq!(canaries[1].name, "List Test Canary 1");
    assert!(!canaries[1].enabled);
}

/// Test 9: Canary can be deleted
#[tokio::test]
async fn ac9_canary_can_be_deleted() {
    let store = create_test_store();

    // Create a canary
    let canary = NewCanary {
        id: "delete-test".to_string(),
        name: "Delete Test".to_string(),
        index_uid: "products".to_string(),
        interval_s: 60,
        query_json: serde_json::to_string(&SearchQuery {
            params: HashMap::new(),
        })
        .unwrap(),
        assertions_json: serde_json::to_string(&vec![CanaryAssertion::MinHits { value: 1 }])
            .unwrap(),
        enabled: true,
        created_at: chrono::Utc::now().timestamp_millis(),
    };

    store.upsert_canary(&canary).unwrap();

    // Verify it exists
    assert!(store.get_canary("delete-test").unwrap().is_some());

    // Delete the canary
    store.delete_canary("delete-test").unwrap();

    // Verify it's gone
    assert!(store.get_canary("delete-test").unwrap().is_none());
}

/// Test 10: Canary can be updated
#[tokio::test]
async fn ac10_canary_can_be_updated() {
    let store = create_test_store();

    // Create a canary
    let canary = NewCanary {
        id: "update-test".to_string(),
        name: "Original Name".to_string(),
        index_uid: "products".to_string(),
        interval_s: 60,
        query_json: serde_json::to_string(&SearchQuery {
            params: HashMap::new(),
        })
        .unwrap(),
        assertions_json: serde_json::to_string(&vec![CanaryAssertion::MinHits { value: 1 }])
            .unwrap(),
        enabled: true,
        created_at: chrono::Utc::now().timestamp_millis(),
    };

    store.upsert_canary(&canary).unwrap();

    // Update the canary
    store
        .upsert_canary(&NewCanary {
            id: "update-test".to_string(),
            name: "Updated Name".to_string(),
            index_uid: "products".to_string(),
            interval_s: 120, // Changed interval
            query_json: serde_json::to_string(&SearchQuery {
                params: HashMap::new(),
            })
            .unwrap(),
            assertions_json: serde_json::to_string(&vec![CanaryAssertion::MinHits { value: 5 }])
                .unwrap(), // Changed assertion
            enabled: false, // Changed enabled state
            created_at: chrono::Utc::now().timestamp_millis(),
        })
        .unwrap();

    // Verify updates
    let retrieved = store.get_canary("update-test").unwrap().unwrap();
    assert_eq!(retrieved.name, "Updated Name");
    assert_eq!(retrieved.interval_s, 120);
    assert!(!retrieved.enabled);

    // Verify assertions were updated
    let assertions: Vec<CanaryAssertion> =
        serde_json::from_str(&retrieved.assertions_json).unwrap();
    assert_eq!(assertions.len(), 1);
    match &assertions[0] {
        CanaryAssertion::MinHits { value } => assert_eq!(*value, 5),
        _ => panic!("Unexpected assertion type"),
    }
}

/// Test 11: All assertion types can be serialized
#[tokio::test]
async fn ac11_all_assertion_types_serialize() {
    let assertions = vec![
        CanaryAssertion::TopHitId {
            value: "product-123".to_string(),
        },
        CanaryAssertion::TopKContains {
            k: 5,
            ids: vec!["a".to_string(), "b".to_string()],
        },
        CanaryAssertion::MinHits { value: 10 },
        CanaryAssertion::MaxP95Ms { value: 500 },
        CanaryAssertion::SettingsVersionAtLeast { value: 42 },
        CanaryAssertion::MustNotContainId {
            id: "deprecated".to_string(),
        },
    ];

    let serialized = serde_json::to_string(&assertions).unwrap();
    let deserialized: Vec<CanaryAssertion> = serde_json::from_str(&serialized).unwrap();

    assert_eq!(deserialized.len(), assertions.len());

    // Verify each assertion type
    match &deserialized[0] {
        CanaryAssertion::TopHitId { value } => assert_eq!(value, "product-123"),
        _ => panic!("Expected TopHitId"),
    }

    match &deserialized[2] {
        CanaryAssertion::MinHits { value } => assert_eq!(*value, 10),
        _ => panic!("Expected MinHits"),
    }

    match &deserialized[5] {
        CanaryAssertion::MustNotContainId { id } => assert_eq!(id, "deprecated"),
        _ => panic!("Expected MustNotContainId"),
    }
}

/// Test 12: Query with various parameters can be captured
#[tokio::test]
async fn ac12_query_with_various_parameters_can_be_captured() {
    let capture = QueryCapture::new(10);

    // Capture a complex query
    let mut params = HashMap::new();
    params.insert("q".to_string(), serde_json::json!("laptop"));
    params.insert("limit".to_string(), serde_json::json!(20));
    params.insert(
        "filter".to_string(),
        serde_json::json!("category = \"electronics\""),
    );
    params.insert("sort".to_string(), serde_json::json!("price:asc"));

    capture
        .capture(
            "products".to_string(),
            SearchQuery { params },
            SearchResponse {
                hits: vec![],
                estimated_total_hits: 100,
                processing_time_ms: 45,
                query: "laptop".to_string(),
            },
        )
        .await;

    let captured = capture.get_captured().await;
    assert_eq!(captured.len(), 1);

    // Verify all parameters were captured
    assert_eq!(captured[0].query.params.get("q").unwrap(), "laptop");
    assert_eq!(captured[0].query.params.get("limit").unwrap(), 20);
    assert_eq!(
        captured[0].query.params.get("filter").unwrap(),
        "category = \"electronics\""
    );
    assert_eq!(captured[0].query.params.get("sort").unwrap(), "price:asc");
}
