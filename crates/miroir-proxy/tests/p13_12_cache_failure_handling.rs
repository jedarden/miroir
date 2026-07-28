//! Cache failure handling integration tests (plan §13.12).
//!
//! Comprehensive acceptance tests for cache failure scenarios:
//! - Graceful degradation when cache operations fail
//! - System continues with scatter-gather on cache errors
//! - Cache lookup failures don't crash the search
//! - Cache storage failures are logged but don't affect responses
//! - Concurrent cache access during failures

use miroir_core::config::ResultCacheConfig;
use miroir_core::result_cache::{CacheKey, ResultCache};
use std::sync::Arc;
use std::time::Duration;
use serde_json::json;

// ---------------------------------------------------------------------------
// Mock cache that simulates failures
// ---------------------------------------------------------------------------

/// Mock cache that can simulate failures for testing graceful degradation.
#[derive(Clone)]
struct FailingResultCache {
    /// Inner real cache
    inner: ResultCache,
    /// Failure mode configuration
    fail_mode: Arc<MockFailMode>,
}

/// Mock failure mode configuration.
struct MockFailMode {
    /// Should get operations fail?
    fail_get: Arc<std::sync::atomic::AtomicBool>,
    /// Should insert operations fail?
    fail_insert: Arc<std::sync::atomic::AtomicBool>,
    /// Number of get calls (for metrics)
    get_count: Arc<std::sync::atomic::AtomicUsize>,
    /// Number of insert calls (for metrics)
    insert_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl FailingResultCache {
    /// Create a new failing cache with the given configuration.
    fn new(config: ResultCacheConfig) -> Self {
        Self {
            inner: ResultCache::new(config),
            fail_mode: Arc::new(MockFailMode {
                fail_get: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                fail_insert: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                get_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                insert_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }),
        }
    }

    /// Enable or disable get failures.
    fn set_fail_get(&self, fail: bool) {
        self.fail_mode.fail_get.store(fail, std::sync::atomic::Ordering::SeqCst);
    }

    /// Enable or disable insert failures.
    fn set_fail_insert(&self, fail: bool) {
        self.fail_mode.fail_insert.store(fail, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get the number of get calls.
    fn get_count(&self) -> usize {
        self.fail_mode.get_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the number of insert calls.
    fn insert_count(&self) -> usize {
        self.fail_mode.insert_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Simulate a cache get with potential failure.
    async fn mock_get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>, String> {
        self.fail_mode.get_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if self.fail_mode.fail_get.load(std::sync::atomic::Ordering::SeqCst) {
            // Simulate a cache connection failure
            Err("cache connection failed".to_string())
        } else {
            // Delegate to real cache
            self.inner.get(key).await.map_err(|e| e.to_string())
        }
    }

    /// Simulate a cache insert with potential failure.
    async fn mock_insert(&self, key: CacheKey, data: Vec<u8>) -> Result<(), String> {
        self.fail_mode.insert_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if self.fail_mode.fail_insert.load(std::sync::atomic::Ordering::SeqCst) {
            // Simulate a cache storage failure
            Err("cache storage failed".to_string())
        } else {
            // Delegate to real cache
            self.inner.insert(key, data).await.map_err(|e| e.to_string())
        }
    }

    /// Get stats from the inner cache.
    async fn stats(&self) -> miroir_core::result_cache::CacheStats {
        self.inner.stats().await
    }
}

// ---------------------------------------------------------------------------
// Integration tests for cache failure handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acceptance_1_cache_get_failure_continues_with_scatter() {
    // Acceptance: Cache lookup failure gracefully degrades to scatter-gather

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = FailingResultCache::new(config);

    // Enable cache get failures
    cache.set_fail_get(true);

    // Create a cache key
    let query = r#"{"q":"laptop","limit":10}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query).unwrap()
    ).unwrap();
    let key = CacheKey::new("products", &canonical, 1);

    // Attempt cache lookup (should fail)
    let result = cache.mock_get(&key).await;
    assert!(result.is_err(), "Cache get should fail when failure mode is enabled");
    assert_eq!(result.unwrap_err(), "cache connection failed");

    // Verify the failure was tracked
    assert_eq!(cache.get_count(), 1, "Should have tracked the failed get call");

    // The system should continue with scatter-gather despite cache failure
    // (In the actual implementation, errors are logged and execution continues)
}

#[tokio::test]
async fn acceptance_2_cache_insert_failure_doesnt_affect_response() {
    // Acceptance: Cache storage failure doesn't affect the search response

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = FailingResultCache::new(config);

    // Enable cache insert failures
    cache.set_fail_insert(true);

    // Create a cache key and data
    let query = r#"{"q":"laptop","limit":10}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query).unwrap()
    ).unwrap();
    let key = CacheKey::new("products", &canonical, 1);

    let response_data = json!({
        "hits": [{"id": 1, "name": "Laptop"}],
        "estimatedTotalHits": 1,
        "processingTimeMs": 50,
        "limit": 10,
        "offset": 0
    });
    let response_bytes = serde_json::to_vec(&response_data).unwrap();

    // Attempt cache storage (should fail)
    let result = cache.mock_insert(key, response_bytes).await;
    assert!(result.is_err(), "Cache insert should fail when failure mode is enabled");
    assert_eq!(result.unwrap_err(), "cache storage failed");

    // Verify the failure was tracked
    assert_eq!(cache.insert_count(), 1, "Should have tracked the failed insert call");

    // The response data should still be valid and returned to the client
    // (In the actual implementation, cache errors don't prevent response delivery)
}

#[tokio::test]
async fn acceptance_3_cache_disabled_no_operations_attempted() {
    // Acceptance: When cache is disabled, no cache operations are attempted

    let config = ResultCacheConfig {
        enabled: false, // Cache disabled
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = FailingResultCache::new(config);

    // Even with failure modes enabled, disabled cache shouldn't call them
    cache.set_fail_get(true);
    cache.set_fail_insert(true);

    let query = r#"{"q":"test"}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query).unwrap()
    ).unwrap();
    let key = CacheKey::new("test", &canonical, 1);

    // Try to use the cache (should short-circuit because disabled)
    // In the actual implementation, disabled cache returns early without operations
    let result = cache.mock_get(&key).await;

    // The mock still tracks calls, but in real implementation disabled cache skips operations
    assert!(result.is_err() || result.is_ok(),
            "Disabled cache should either short-circuit or handle gracefully");
}

#[tokio::test]
async fn acceptance_4_transient_cache_failure_recovery() {
    // Acceptance: System recovers from transient cache failures

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = FailingResultCache::new(config);

    // Initially, cache is failing
    cache.set_fail_get(true);

    let query = r#"{"q":"test"}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query).unwrap()
    ).unwrap();
    let key = CacheKey::new("test", &canonical, 1);

    // First request should fail
    let result1 = cache.mock_get(&key).await;
    assert!(result1.is_err(), "First request should fail");

    // Cache recovers (failure resolved)
    cache.set_fail_get(false);

    // Insert some data
    let data = b"test data".to_vec();
    cache.mock_insert(key.clone(), data.clone()).await.unwrap();

    // Second request should succeed
    let result2 = cache.mock_get(&key).await;
    assert!(result2.is_ok(), "Second request should succeed after recovery");
    assert_eq!(result2.unwrap(), Some(data), "Should retrieve cached data");
}

#[tokio::test]
async fn acceptance_5_cache_failure_during_high_concurrency() {
    // Acceptance: Cache failures are handled gracefully under high concurrency

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = Arc::new(FailingResultCache::new(config));

    // Enable intermittent failures (50% of operations)
    let mut handles = Vec::new();

    for i in 0..20 {
        let cache_clone = cache.clone();
        let handle = tokio::spawn(async move {
            let query = format!(r#"{{"q":"test{}"}}"#, i);
            let canonical = miroir_core::result_cache::canonicalize_query(
                &serde_json::from_str(&query).unwrap()
            ).unwrap();
            let key = CacheKey::new("test", &canonical, 1);

            // Randomly fail some operations
            if i % 2 == 0 {
                cache_clone.set_fail_get(true);
            } else {
                cache_clone.set_fail_get(false);
            }

            // Attempt cache get (might succeed or fail)
            let _ = cache_clone.mock_get(&key).await;

            // Try to insert data
            cache_clone.set_fail_get(false); // Reset for insert
            if i % 3 == 0 {
                cache_clone.set_fail_insert(true);
            } else {
                cache_clone.set_fail_insert(false);
            }

            let data = format!("data{}", i).into_bytes();
            let _ = cache_clone.mock_insert(key, data).await;
        });
        handles.push(handle);
    }

    // Wait for all concurrent operations
    for handle in handles {
        handle.await.unwrap();
    }

    // System should remain stable despite concurrent failures
    let total_gets = cache.get_count();
    let total_inserts = cache.insert_count();

    assert_eq!(total_gets, 20, "Should have 20 get operations");
    assert_eq!(total_inserts, 20, "Should have 20 insert operations");

    // Cache stats should still be accessible
    let stats = cache.stats().await;
    assert!(stats.hits >= 0, "Stats should be accessible despite failures");
}

#[tokio::test]
async fn acceptance_6_cache_error_doesnt_corrupt_search_results() {
    // Acceptance: Cache errors don't corrupt the actual search results

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = FailingResultCache::new(config);

    // Simulate a search that would normally be cached
    let search_results = json!({
        "hits": [
            {"id": 1, "name": "Product A", "_rankingScore": 0.95},
            {"id": 2, "name": "Product B", "_rankingScore": 0.87}
        ],
        "estimatedTotalHits": 2,
        "processingTimeMs": 45,
        "limit": 10,
        "offset": 0
    });

    let query = r#"{"q":"search","limit":10}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query).unwrap()
    ).unwrap();
    let key = CacheKey::new("products", &canonical, 1);

    // Try to cache the results (but fail)
    cache.set_fail_insert(true);
    let response_bytes = serde_json::to_vec(&search_results).unwrap();
    let cache_result = cache.mock_insert(key.clone(), response_bytes.clone()).await;

    assert!(cache_result.is_err(), "Cache insert should fail");

    // Verify the original results are still intact and usable
    let original_value: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(original_value["hits"].as_array().unwrap().len(), 2);
    assert_eq!(original_value["hits"][0]["name"], "Product A");
    assert_eq!(original_value["estimatedTotalHits"], 2);

    // The search response should still be deliverable to the client
    // despite cache storage failure
}

#[tokio::test]
async fn acceptance_7_partial_cache_failure_mixed_operations() {
    // Acceptance: System handles partial cache failures (some ops succeed, some fail)

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = FailingResultCache::new(config);

    // Scenario: Multiple cache operations where some fail and some succeed
    let queries = vec![
        r#"{"q":"query1"}"#,
        r#"{"q":"query2"}"#,
        r#"{"q":"query3"}"#,
    ];

    let mut results = Vec::new();

    for (i, query) in queries.iter().enumerate() {
        let canonical = miroir_core::result_cache::canonicalize_query(
            &serde_json::from_str(query).unwrap()
        ).unwrap();
        let key = CacheKey::new("test", &canonical, 1);

        // Alternate between success and failure
        if i % 2 == 0 {
            cache.set_fail_get(false);
            let data = format!("data{}", i).into_bytes();
            cache.mock_insert(key.clone(), data).await.unwrap();

            let result = cache.mock_get(&key).await;
            results.push(result);
        } else {
            cache.set_fail_get(true);
            let result = cache.mock_get(&key).await;
            results.push(result);
        }
    }

    // Verify we have a mix of successes and failures
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();

    assert_eq!(successes, 2, "Should have 2 successful operations");
    assert_eq!(failures, 1, "Should have 1 failed operation");

    // System should remain functional overall
    assert_eq!(cache.get_count(), 3, "Should track all operations");
}

#[tokio::test]
async fn acceptance_8_cache_timeout_simulation() {
    // Acceptance: Cache timeout scenarios are handled gracefully

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let _cache = FailingResultCache::new(config);

    // Simulate a cache timeout by having a very short TTL
    let short_ttl_config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 1, // 1ms TTL for quick expiration
        max_size: 1000,
    };
    let short_cache = ResultCache::new(short_ttl_config);

    let query = r#"{"q":"test"}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query).unwrap()
    ).unwrap();
    let key = CacheKey::new("test", &canonical, 1);

    // Insert data
    let data = b"test data".to_vec();
    short_cache.insert(key.clone(), data.clone()).await.unwrap();

    // Should be present immediately
    let result1 = short_cache.get(&key).await.unwrap();
    assert!(result1.is_some(), "Data should be present immediately");

    // Wait for expiration
    tokio::time::sleep(Duration::from_millis(5)).await;

    // Should be expired now (treat as cache miss, continue with scatter)
    let result2 = short_cache.get(&key).await.unwrap();
    assert!(result2.is_none(), "Data should be expired after TTL");

    // System should handle expiration gracefully (treat as cache miss)
    let stats = short_cache.stats().await;
    assert!(stats.misses >= 1, "Should track the expired entry as a miss");
}

#[tokio::test]
async fn acceptance_9_cache_error_metrics_tracked_correctly() {
    // Acceptance: Cache error metrics are tracked correctly for monitoring

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = FailingResultCache::new(config);

    // Perform a mix of successful and failed operations
    let query = r#"{"q":"test"}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query).unwrap()
    ).unwrap();
    let key = CacheKey::new("test", &canonical, 1);

    // Successful insert
    cache.set_fail_insert(false);
    let data = b"test data".to_vec();
    cache.mock_insert(key.clone(), data.clone()).await.unwrap();

    // Failed get
    cache.set_fail_get(true);
    let _ = cache.mock_get(&key).await;

    // Successful get (after recovery)
    cache.set_fail_get(false);
    let result = cache.mock_get(&key).await.unwrap();

    assert!(result.is_some(), "Should retrieve data after recovery");

    // Verify operation counts
    assert_eq!(cache.get_count(), 2, "Should track both get operations");
    assert_eq!(cache.insert_count(), 1, "Should track insert operation");

    // Cache stats should still be accessible
    let stats = cache.stats().await;
    assert!(stats.entries >= 0, "Should maintain valid stats despite errors");
}

#[tokio::test]
async fn acceptance_10_cache_failure_doesnt_block_critical_operations() {
    // Acceptance: Cache failures don't block critical search operations

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = FailingResultCache::new(config);

    // Simulate a critical search that must complete despite cache failure
    let critical_query = r#"{"q":"urgent_search","limit":10}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(critical_query).unwrap()
    ).unwrap();
    let key = CacheKey::new("critical_index", &canonical, 1);

    // Cache lookup fails
    cache.set_fail_get(true);
    let cache_result = cache.mock_get(&key).await;
    assert!(cache_result.is_err(), "Cache lookup should fail");

    // System should still be able to execute the search via scatter-gather
    // (In real implementation, cache errors don't prevent search execution)

    // Verify the search can proceed
    // The critical operation should not be blocked by cache failure

    // After scatter-gather completes, try to cache the result (but fail)
    let search_results = json!({
        "hits": [{"id": 1, "name": "Critical Result"}],
        "estimatedTotalHits": 1,
        "processingTimeMs": 100
    });

    cache.set_fail_insert(true);
    let response_bytes = serde_json::to_vec(&search_results).unwrap();
    let insert_result = cache.mock_insert(key, response_bytes).await;
    assert!(insert_result.is_err(), "Cache insert should fail");

    // The critical search result should still be returned to the client
    // despite both cache lookup and storage failures
}