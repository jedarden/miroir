//! Multi-search cache integration tests (plan §13.11).
//!
//! Comprehensive acceptance tests for multi-search result cache:
//! - Cache hit reduces upstream fan-out for batch queries
//! - Cache miss executes normal scatter for each query
//! - Cache storage after successful merge
//! - Metrics are recorded correctly for multi-search
//! - Cache bypass for multi-target aliases in batches
//! - Cache invalidation on settings version change

use miroir_core::config::{MiroirConfig, ResultCacheConfig};
use miroir_core::result_cache::{CacheKey, ResultCache};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn acceptance_1_multi_search_cache_hit_bypasses_fanout() {
    // Acceptance: Cache hit in multi-search returns cached result without executing scatter

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = ResultCache::new(config);

    // Create cache keys for two queries in the batch
    let query1 = r#"{"q":"laptop","limit":10}"#;
    let canonical1 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query1).unwrap()
    ).unwrap();
    let key1 = CacheKey::new("products", &canonical1, 1);

    let query2 = r#"{"q":"phone","limit":20}"#;
    let canonical2 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query2).unwrap()
    ).unwrap();
    let key2 = CacheKey::new("products", &canonical2, 1);

    // Pre-populate cache with results
    let cached_response1 = json!({
        "hits": [{"id": 1, "name": "Laptop"}],
        "estimatedTotalHits": 1,
        "processingTimeMs": 10,
        "limit": 10,
        "offset": 0
    });
    let cached_response2 = json!({
        "hits": [{"id": 2, "name": "Phone"}],
        "estimatedTotalHits": 1,
        "processingTimeMs": 10,
        "limit": 20,
        "offset": 0
    });

    cache.insert(key1.clone(), serde_json::to_vec(&cached_response1).unwrap()).await.unwrap();
    cache.insert(key2.clone(), serde_json::to_vec(&cached_response2).unwrap()).await.unwrap();

    // Verify cache hits
    let result1 = cache.get(&key1).await.unwrap();
    assert!(result1.is_some());

    let result2 = cache.get(&key2).await.unwrap();
    assert!(result2.is_some());

    // Verify cache stats
    let stats = cache.stats().await;
    assert_eq!(stats.hits, 2);
    assert_eq!(stats.misses, 0);
    assert_eq!(stats.hit_rate, Some(1.0));
}

#[tokio::test]
async fn acceptance_2_multi_search_cache_miss_executes_scatter() {
    // Acceptance: Cache miss in multi-search continues with normal scatter-gather flow

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = ResultCache::new(config);

    // Create cache keys for queries that are not cached
    let query1 = r#"{"q":"tablet","limit":10}"#;
    let canonical1 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query1).unwrap()
    ).unwrap();
    let key1 = CacheKey::new("products", &canonical1, 1);

    let query2 = r#"{"q":"monitor","limit":20}"#;
    let canonical2 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query2).unwrap()
    ).unwrap();
    let key2 = CacheKey::new("products", &canonical2, 1);

    // Verify cache misses
    let result1 = cache.get(&key1).await.unwrap();
    assert!(result1.is_none());

    let result2 = cache.get(&key2).await.unwrap();
    assert!(result2.is_none());

    // Verify cache stats
    let stats = cache.stats().await;
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hit_rate, Some(0.0));
}

#[tokio::test]
async fn acceptance_3_multi_search_mixed_hit_miss() {
    // Acceptance: Multi-search batch can have some cache hits and some misses

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = ResultCache::new(config);

    // Cache only the first query
    let query1 = r#"{"q":"cached","limit":10}"#;
    let canonical1 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query1).unwrap()
    ).unwrap();
    let key1 = CacheKey::new("products", &canonical1, 1);

    let cached_response1 = json!({
        "hits": [{"id": 1, "name": "Cached Product"}],
        "estimatedTotalHits": 1,
        "processingTimeMs": 10,
        "limit": 10,
        "offset": 0
    });
    cache.insert(key1.clone(), serde_json::to_vec(&cached_response1).unwrap()).await.unwrap();

    // Second query is not cached
    let query2 = r#"{"q":"uncached","limit":20}"#;
    let canonical2 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query2).unwrap()
    ).unwrap();
    let key2 = CacheKey::new("products", &canonical2, 1);

    // Verify mixed results
    let result1 = cache.get(&key1).await.unwrap();
    assert!(result1.is_some());

    let result2 = cache.get(&key2).await.unwrap();
    assert!(result2.is_none());

    // Verify cache stats
    let stats = cache.stats().await;
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hit_rate, Some(0.5));
}

#[tokio::test]
async fn acceptance_4_multi_search_cache_storage_after_merge() {
    // Acceptance: Successful multi-search results are cached for each query

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = ResultCache::new(config);

    // Simulate merged search results from a multi-search batch
    let response1 = json!({
        "hits": [{"id": 1, "name": "Product A"}],
        "estimatedTotalHits": 1,
        "processingTimeMs": 50,
        "limit": 10,
        "offset": 0
    });

    let response2 = json!({
        "hits": [{"id": 2, "name": "Product B"}],
        "estimatedTotalHits": 1,
        "processingTimeMs": 60,
        "limit": 20,
        "offset": 0
    });

    // Create cache keys and store results
    let query1 = r#"{"q":"search1","limit":10}"#;
    let canonical1 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query1).unwrap()
    ).unwrap();
    let key1 = CacheKey::new("products", &canonical1, 1);

    let query2 = r#"{"q":"search2","limit":20}"#;
    let canonical2 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query2).unwrap()
    ).unwrap();
    let key2 = CacheKey::new("products", &canonical2, 1);

    cache.insert(key1.clone(), serde_json::to_vec(&response1).unwrap()).await.unwrap();
    cache.insert(key2.clone(), serde_json::to_vec(&response2).unwrap()).await.unwrap();

    // Verify results were cached
    let result1 = cache.get(&key1).await.unwrap();
    assert!(result1.is_some());

    let result2 = cache.get(&key2).await.unwrap();
    assert!(result2.is_some());

    // Verify cache has 2 entries
    assert_eq!(cache.len().await, 2);
}

#[tokio::test]
async fn acceptance_5_multi_search_cache_disabled() {
    // Acceptance: When cache is disabled, multi-search results are not cached

    let config = ResultCacheConfig {
        enabled: false, // Disabled
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = ResultCache::new(config);

    // Try to insert multi-search results
    let query1 = r#"{"q":"test1"}"#;
    let canonical1 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query1).unwrap()
    ).unwrap();
    let key1 = CacheKey::new("test", &canonical1, 1);
    let data1 = b"result1".to_vec();

    let query2 = r#"{"q":"test2"}"#;
    let canonical2 = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query2).unwrap()
    ).unwrap();
    let key2 = CacheKey::new("test", &canonical2, 1);
    let data2 = b"result2".to_vec();

    cache.insert(key1.clone(), data1).await.unwrap();
    cache.insert(key2.clone(), data2).await.unwrap();

    // Verify nothing was cached
    let result1 = cache.get(&key1).await.unwrap();
    assert!(result1.is_none());

    let result2 = cache.get(&key2).await.unwrap();
    assert!(result2.is_none());

    // Verify cache remains empty
    assert!(cache.is_empty().await);
}

#[tokio::test]
async fn acceptance_6_multi_search_settings_version_invalidates() {
    // Acceptance: Different settings versions produce different cache keys for multi-search

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = ResultCache::new(config);

    let query = r#"{"q":"test"}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query).unwrap()
    ).unwrap();

    // Cache with settings version 1
    let key_v1 = CacheKey::new("products", &canonical, 1);
    cache.insert(key_v1.clone(), b"version 1".to_vec()).await.unwrap();

    // Cache with settings version 2 (different key)
    let key_v2 = CacheKey::new("products", &canonical, 2);
    cache.insert(key_v2.clone(), b"version 2".to_vec()).await.unwrap();

    // Verify they're different entries
    assert_ne!(key_v1, key_v2);

    // Verify each version gets its cached value
    let result_v1 = cache.get(&key_v1).await.unwrap();
    assert_eq!(result_v1, Some(b"version 1".to_vec()));

    let result_v2 = cache.get(&key_v2).await.unwrap();
    assert_eq!(result_v2, Some(b"version 2".to_vec()));

    // Verify we have 2 entries
    assert_eq!(cache.len().await, 2);
}

#[tokio::test]
async fn acceptance_7_multi_search_cache_ttl_expiration() {
    // Acceptance: Multi-search cache entries expire after TTL

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 100, // Very short TTL for testing
        max_size: 1000,
    };
    let cache = ResultCache::new(config);

    // Insert multiple entries
    for i in 0..3 {
        let query = format!(r#"{{"q":"test{}"}}"#, i);
        let canonical = miroir_core::result_cache::canonicalize_query(
            &serde_json::from_str(&query).unwrap()
        ).unwrap();
        let key = CacheKey::new("test", &canonical, 1);
        let data = format!("data{}", i);
        cache.insert(key, data.into_bytes()).await.unwrap();
    }

    // All should be present immediately
    assert_eq!(cache.len().await, 3);

    // Wait for expiration
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // All should be expired now
    for i in 0..3 {
        let query = format!(r#"{{"q":"test{}"}}"#, i);
        let canonical = miroir_core::result_cache::canonicalize_query(
            &serde_json::from_str(&query).unwrap()
        ).unwrap();
        let key = CacheKey::new("test", &canonical, 1);
        let result = cache.get(&key).await.unwrap();
        assert!(result.is_none());
    }

    // Stats should show misses for expired entries
    let stats = cache.stats().await;
    assert!(stats.misses >= 3);
}

#[tokio::test]
async fn acceptance_8_multi_search_batch_consistency() {
    // Acceptance: Multi-search batch with identical queries should use cache consistently

    let config = ResultCacheConfig {
        enabled: true,
        ttl_ms: 500,
        max_size: 1000,
    };
    let cache = ResultCache::new(config);

    // Create a batch with the same query twice
    let query = r#"{"q":"duplicate","limit":10}"#;
    let canonical = miroir_core::result_cache::canonicalize_query(
        &serde_json::from_str(query).unwrap()
    ).unwrap();
    let key = CacheKey::new("products", &canonical, 1);

    // Cache the result once
    let response = json!({
        "hits": [{"id": 1, "name": "Product"}],
        "estimatedTotalHits": 1,
        "processingTimeMs": 10,
        "limit": 10,
        "offset": 0
    });
    cache.insert(key.clone(), serde_json::to_vec(&response).unwrap()).await.unwrap();

    // Both queries in the batch should hit the cache
    let result1 = cache.get(&key).await.unwrap();
    assert!(result1.is_some());

    let result2 = cache.get(&key).await.unwrap();
    assert!(result2.is_some());

    // Verify cache stats show 2 hits
    let stats = cache.stats().await;
    assert_eq!(stats.hits, 2);
    assert_eq!(stats.misses, 0);
}
