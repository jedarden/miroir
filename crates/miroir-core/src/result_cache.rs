//! LRU cache with TTL for merged search results (plan §13.10).
//!
//! Caches completed scatter-gather results to reduce redundant fan-out under
//! bursty repeat-query traffic (pagination, retries, facet-count polling).
//!
//! # Cache Key
//!
//! - `index_name`: Target index UID
//! - `canonicalized_query_body`: Query JSON sorted deterministically
//! - `settings_version`: Index settings version (invalidates on schema change)
//!
//! # Cache Value
//!
//! - Serialized `MergedSearchResult` as JSON bytes
//!
//! # Expiration
//!
//! - TTL-based expiration (configurable via `ttl_ms`, default 500ms)
//! - LRU eviction when `max_size` is exceeded (default 1000 entries)

use crate::config::ResultCacheConfig;
use crate::Result;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, trace};

// ---------------------------------------------------------------------------
// Cache entry with timestamp for TTL
// ---------------------------------------------------------------------------

/// Single cache entry with expiration timestamp.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Cached JSON bytes (serialized MergedSearchResult).
    data: Vec<u8>,
    /// Expiration timestamp.
    expires_at: Instant,
}

impl CacheEntry {
    /// Create a new cache entry with TTL.
    fn new(data: Vec<u8>, ttl: Duration) -> Self {
        Self {
            data,
            expires_at: Instant::now() + ttl,
        }
    }

    /// Check if this entry has expired.
    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

// ---------------------------------------------------------------------------
// Cache key structure
// ---------------------------------------------------------------------------

/// Cache key combining index, query, and settings version.
///
/// Uses hash-based equality for efficient comparison while maintaining
/// deterministic key generation for identical queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// Hash of the canonicalized components.
    hash: u64,
}

impl CacheKey {
    /// Create a new cache key from the components.
    ///
    /// # Arguments
    ///
    /// * `index_name` - Target index UID
    /// * `canonicalized_query_body` - Query JSON sorted deterministically
    /// * `settings_version` - Index settings version (invalidates on schema change)
    pub fn new(index_name: &str, canonicalized_query_body: &str, settings_version: u64) -> Self {
        let mut hasher = DefaultHasher::new();
        index_name.hash(&mut hasher);
        canonicalized_query_body.hash(&mut hasher);
        settings_version.hash(&mut hasher);
        Self {
            hash: hasher.finish(),
        }
    }

    /// Create a cache key for testing or special cases.
    pub fn from_hash(hash: u64) -> Self {
        Self { hash }
    }
}

// ---------------------------------------------------------------------------
// Result cache implementation
// ---------------------------------------------------------------------------

/// LRU cache with TTL for merged search results.
pub struct ResultCache {
    /// LRU cache backing store.
    cache: Arc<RwLock<lru::LruCache<CacheKey, CacheEntry>>>,
    /// Cache configuration.
    config: ResultCacheConfig,
    /// Cache hit count (for metrics).
    hits: Arc<RwLock<u64>>,
    /// Cache miss count (for metrics).
    misses: Arc<RwLock<u64>>,
}

impl ResultCache {
    /// Create a new result cache with the given configuration.
    pub fn new(config: ResultCacheConfig) -> Self {
        use std::num::NonZeroUsize;
        let capacity = NonZeroUsize::new(config.max_size).unwrap_or_else(|| NonZeroUsize::new(1).unwrap());
        let cache = lru::LruCache::new(capacity);
        Self {
            cache: Arc::new(RwLock::new(cache)),
            config,
            hits: Arc::new(RwLock::new(0)),
            misses: Arc::new(RwLock::new(0)),
        }
    }

    /// Get a cached result if available and not expired.
    ///
    /// Returns `Ok(Some(data))` on cache hit, `Ok(None)` on miss,
    /// or `Err` if the cache is disabled.
    pub async fn get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>> {
        if !self.config.enabled {
            return Ok(None);
        }

        let mut cache = self.cache.write().await;

        if let Some(entry) = cache.get(key) {
            if entry.is_expired() {
                // Entry expired - remove it and return miss
                cache.pop(key);
                *self.misses.write().await += 1;
                trace!("Cache expired for key {:?}", key);
                return Ok(None);
            }

            // Cache hit
            *self.hits.write().await += 1;
            debug!("Cache hit for key {:?}", key);
            Ok(Some(entry.data.clone()))
        } else {
            // Cache miss
            *self.misses.write().await += 1;
            trace!("Cache miss for key {:?}", key);
            Ok(None)
        }
    }

    /// Insert a result into the cache.
    ///
    /// Returns `Ok(())` on success, or `Err` if the cache is disabled.
    pub async fn insert(&self, key: CacheKey, data: Vec<u8>) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let ttl = Duration::from_millis(self.config.ttl_ms);
        let entry = CacheEntry::new(data, ttl);

        let mut cache = self.cache.write().await;
        debug!("Cache insert for key {:?} (TTL: {:?})", key, ttl);
        cache.put(key, entry);

        Ok(())
    }

    /// Invalidate a specific cache entry.
    pub async fn invalidate(&self, key: &CacheKey) {
        let mut cache = self.cache.write().await;
        cache.pop(key);
        debug!("Cache invalidated for key {:?}", key);
    }

    /// Clear all entries from the cache.
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        debug!("Cache cleared");
    }

    /// Get the current number of entries in the cache.
    pub async fn len(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }

    /// Check if the cache is empty.
    pub async fn is_empty(&self) -> bool {
        let cache = self.cache.read().await;
        cache.is_empty()
    }

    /// Get cache statistics (hits, misses, hit rate).
    pub async fn stats(&self) -> CacheStats {
        let hits = *self.hits.read().await;
        let misses = *self.misses.read().await;
        let total = hits + misses;
        let hit_rate = if total > 0 {
            Some(hits as f64 / total as f64)
        } else {
            None
        };

        CacheStats {
            hits,
            misses,
            hit_rate,
            entries: self.len().await,
        }
    }

    /// Create a cache key from query components.
    ///
    /// This is a convenience method for generating cache keys.
    pub fn make_key(
        index_name: &str,
        canonicalized_query_body: &str,
        settings_version: u64,
    ) -> CacheKey {
        CacheKey::new(index_name, canonicalized_query_body, settings_version)
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Hit rate (0.0 to 1.0), or None if no requests.
    pub hit_rate: Option<f64>,
    /// Current number of entries in the cache.
    pub entries: usize,
}

/// Clone implementation for ResultCache.
///
/// This creates a shallow clone that shares the underlying cache data
/// and statistics. This is safe because all operations are synchronized
/// via `Arc<RwLock>`.
impl Clone for ResultCache {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            config: self.config.clone(),
            hits: self.hits.clone(),
            misses: self.misses.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper function for canonicalizing query JSON
// ---------------------------------------------------------------------------

/// Canonicalize a query JSON object for consistent hashing.
///
/// This sorts object keys alphabetically to ensure that semantically
/// identical queries (e.g., `{"q":"a","limit":10}` vs `{"limit":10,"q":"a"}`)
/// produce the same cache key.
pub fn canonicalize_query(query: &serde_json::Value) -> Result<String> {
    match query {
        serde_json::Value::Object(map) => {
            // Sort keys and recursively canonicalize values
            let mut sorted_map = serde_json::Map::new();
            let mut sorted_keys: Vec<&String> = map.keys().collect();
            sorted_keys.sort();

            for key in sorted_keys {
                if let Some(value) = map.get(key) {
                    sorted_map.insert(
                        key.clone(),
                        serde_json::from_str(&canonicalize_query(value)?)?
                    );
                }
            }

            Ok(serde_json::to_string(&sorted_map)?)
        }
        serde_json::Value::Array(arr) => {
            // Canonicalize array elements
            let canonicalized: Result<Vec<String>> = arr
                .iter()
                .map(|v| canonicalize_query(v))
                .collect();
            let canonical_arr: Vec<serde_json::Value> = canonicalized?
                .iter()
                .map(|s| serde_json::from_str(s).unwrap())
                .collect();
            Ok(serde_json::to_string(&canonical_arr)?)
        }
        _ => {
            // Primitive values (string, number, bool, null) are already canonical
            Ok(serde_json::to_string(query)?)
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ResultCacheConfig;
    use serde_json::json;

    fn make_test_config() -> ResultCacheConfig {
        ResultCacheConfig {
            enabled: true,
            ttl_ms: 500,
            max_size: 10,
        }
    }

    #[test]
    fn test_cache_key_creation() {
        let key1 = CacheKey::new("test_index", "{\"q\":\"test\"}", 1);
        let key2 = CacheKey::new("test_index", "{\"q\":\"test\"}", 1);

        // Same inputs should produce same key
        assert_eq!(key1, key2);

        // Different settings version should produce different key
        let key3 = CacheKey::new("test_index", "{\"q\":\"test\"}", 2);
        assert_ne!(key1, key3);

        // Different index should produce different key
        let key4 = CacheKey::new("other_index", "{\"q\":\"test\"}", 1);
        assert_ne!(key1, key4);
    }

    #[test]
    fn test_cache_key_from_hash() {
        let key1 = CacheKey::new("test", "query", 1);
        let hash = key1.hash;
        let key2 = CacheKey::from_hash(hash);

        assert_eq!(key1, key2);
    }

    #[test]
    fn test_canonicalize_query_object() {
        let query1 = json!({"q": "test", "limit": 10});
        let query2 = json!({"limit": 10, "q": "test"});

        let canon1 = canonicalize_query(&query1).unwrap();
        let canon2 = canonicalize_query(&query2).unwrap();

        assert_eq!(canon1, canon2);
    }

    #[test]
    fn test_canonicalize_query_nested() {
        let query1 = json!({
            "filter": {"age": 25, "city": "NYC"},
            "q": "test"
        });
        let query2 = json!({
            "q": "test",
            "filter": {"city": "NYC", "age": 25}
        });

        let canon1 = canonicalize_query(&query1).unwrap();
        let canon2 = canonicalize_query(&query2).unwrap();

        assert_eq!(canon1, canon2);
    }

    #[test]
    fn test_canonicalize_query_array() {
        let query1 = json!(["b", "a", "c"]);
        let query2 = json!(["b", "a", "c"]);

        let canon1 = canonicalize_query(&query1).unwrap();
        let canon2 = canonicalize_query(&query2).unwrap();

        assert_eq!(canon1, canon2);
    }

    #[test]
    fn test_canonicalize_query_primitive() {
        assert_eq!(canonicalize_query(&json!("test")).unwrap(), r#""test""#);
        assert_eq!(canonicalize_query(&json!(42)).unwrap(), "42");
        assert_eq!(canonicalize_query(&json!(true)).unwrap(), "true");
        assert_eq!(canonicalize_query(&json!(null)).unwrap(), "null");
    }

    #[tokio::test]
    async fn test_cache_insert_and_get() {
        let cache = ResultCache::new(make_test_config());
        let key = CacheKey::new("test", "query", 1);
        let data = b"test data".to_vec();

        // Insert
        cache.insert(key.clone(), data.clone()).await.unwrap();

        // Get should return the data
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, Some(data));
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache = ResultCache::new(make_test_config());
        let key = CacheKey::new("test", "query", 1);

        // Get should return None for non-existent key
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_cache_disabled() {
        let config = ResultCacheConfig {
            enabled: false,
            ttl_ms: 500,
            max_size: 10,
        };
        let cache = ResultCache::new(config);
        let key = CacheKey::new("test", "query", 1);
        let data = b"test data".to_vec();

        // Insert should be no-op when disabled
        cache.insert(key.clone(), data).await.unwrap();

        // Get should always return None when disabled
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_cache_invalidate() {
        let cache = ResultCache::new(make_test_config());
        let key = CacheKey::new("test", "query", 1);
        let data = b"test data".to_vec();

        // Insert
        cache.insert(key.clone(), data.clone()).await.unwrap();
        assert_eq!(cache.get(&key).await.unwrap(), Some(data));

        // Invalidate
        cache.invalidate(&key).await;

        // Should return None after invalidation
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let cache = ResultCache::new(make_test_config());

        // Insert multiple entries
        for i in 0..5 {
            let key = CacheKey::new("test", &format!("query_{}", i), 1);
            let data = format!("data_{}", i).into_bytes();
            cache.insert(key, data).await.unwrap();
        }

        assert_eq!(cache.len().await, 5);

        // Clear all
        cache.clear().await;

        assert_eq!(cache.len().await, 0);
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let cache = ResultCache::new(make_test_config());
        let key = CacheKey::new("test", "query", 1);
        let data = b"test data".to_vec();

        // Initial stats
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.hit_rate, None);

        // Insert
        cache.insert(key.clone(), data).await.unwrap();

        // Cache miss
        let wrong_key = CacheKey::new("test", "wrong", 1);
        cache.get(&wrong_key).await.unwrap();

        let stats = cache.stats().await;
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hit_rate, Some(0.0));

        // Cache hit
        cache.get(&key).await.unwrap();

        let stats = cache.stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hit_rate, Some(0.5));
    }

    #[tokio::test]
    async fn test_cache_lru_eviction() {
        // Create a cache with max_size = 3
        let config = ResultCacheConfig {
            enabled: true,
            ttl_ms: 500,
            max_size: 3,
        };
        let cache = ResultCache::new(config);

        // Insert 4 entries (should evict the first)
        for i in 0..4 {
            let key = CacheKey::new("test", &format!("query_{}", i), 1);
            let data = format!("data_{}", i).into_bytes();
            cache.insert(key, data).await.unwrap();
        }

        // Cache should only have 3 entries
        assert_eq!(cache.len().await, 3);

        // First entry should have been evicted
        let first_key = CacheKey::new("test", "query_0", 1);
        assert_eq!(cache.get(&first_key).await.unwrap(), None);

        // Last entry should still be present
        let last_key = CacheKey::new("test", "query_3", 1);
        assert_eq!(cache.get(&last_key).await.unwrap(), Some(b"data_3".to_vec()));
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        // Create a cache with very short TTL (10ms)
        let config = ResultCacheConfig {
            enabled: true,
            ttl_ms: 10,
            max_size: 10,
        };
        let cache = ResultCache::new(config);
        let key = CacheKey::new("test", "query", 1);
        let data = b"test data".to_vec();

        // Insert
        cache.insert(key.clone(), data.clone()).await.unwrap();

        // Should be present immediately
        assert_eq!(cache.get(&key).await.unwrap(), Some(data));

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Should be expired now
        let result = cache.get(&key).await.unwrap();
        assert_eq!(result, None);

        // Stats should show a miss for the expired entry
        let stats = cache.stats().await;
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn test_cache_key_collisions() {
        let cache = ResultCache::new(make_test_config());

        // Create two different queries that might hash to the same value
        // (extremely unlikely, but we test the cache behavior)
        let key1 = CacheKey::new("index1", "query1", 1);
        let data1 = b"data1".to_vec();

        let key2 = CacheKey::new("index2", "query2", 2);
        let data2 = b"data2".to_vec();

        // Insert both
        cache.insert(key1.clone(), data1.clone()).await.unwrap();
        cache.insert(key2.clone(), data2.clone()).await.unwrap();

        // Both should be retrievable
        assert_eq!(cache.get(&key1).await.unwrap(), Some(data1));
        assert_eq!(cache.get(&key2).await.unwrap(), Some(data2));
    }

    #[tokio::test]
    async fn test_cache_clone() {
        let cache = ResultCache::new(make_test_config());
        let key = CacheKey::new("test", "query", 1);
        let data = b"test data".to_vec();

        // Insert into original
        cache.insert(key.clone(), data.clone()).await.unwrap();

        // Clone the cache
        let cache_clone = cache.clone();

        // Both should have the same data
        assert_eq!(cache.get(&key).await.unwrap(), Some(data.clone()));
        assert_eq!(cache_clone.get(&key).await.unwrap(), Some(data));

        // Stats should be shared (both get() calls increment the shared hit counter)
        let stats = cache.stats().await;
        assert_eq!(stats.hits, 2);
    }

    #[tokio::test]
    async fn test_cache_concurrent_access() {
        let cache = Arc::new(ResultCache::new(make_test_config()));
        let mut handles = Vec::new();

        // Spawn multiple concurrent tasks
        for i in 0..10 {
            let cache_clone = cache.clone();
            let handle = tokio::spawn(async move {
                let key = CacheKey::new("test", &format!("query_{}", i), 1);
                let data = format!("data_{}", i).into_bytes();

                // Insert
                cache_clone.insert(key.clone(), data).await.unwrap();

                // Get
                cache_clone.get(&key).await.unwrap()
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Cache should have 10 entries
        assert_eq!(cache.len().await, 10);
    }

    #[test]
    fn test_cache_entry_expiration() {
        let data = b"test".to_vec();
        let entry = CacheEntry::new(data.clone(), Duration::from_millis(100));

        // Should not be expired immediately
        assert!(!entry.is_expired());

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(110));

        // Should be expired now
        assert!(entry.is_expired());
    }
}
