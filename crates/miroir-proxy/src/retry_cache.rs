//! Orchestrator-side retry cache for idempotency (plan §4).
//!
//! Key: sha256(batch || target_node || idempotency_key_or_mtask)
//!
//! This cache prevents duplicate writes when retrying timed-out requests
//! to nodes. It stores terminal responses (success or definitive failure)
//! for a configurable TTL.

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Cached response from a node.
#[derive(Debug, Clone)]
pub struct CachedResponse {
    /// Response body.
    pub body: Vec<u8>,
    /// HTTP status code.
    pub status: u16,
    /// When this cache entry was created.
    pub cached_at: Instant,
}

impl CachedResponse {
    /// Check if this entry is still valid (not expired).
    pub fn is_valid(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() < ttl
    }
}

/// Retry cache for idempotency.
#[derive(Clone)]
pub struct RetryCache {
    entries: Arc<RwLock<HashMap<String, CachedResponse>>>,
    default_ttl: Duration,
}

impl RetryCache {
    /// Create a new retry cache with the specified default TTL.
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            default_ttl,
        }
    }

    /// Generate cache key from request components.
    ///
    /// Key format: sha256(batch || target_node || idempotency_key_or_mtask)
    pub fn cache_key(
        batch: &[u8],
        target_node: &str,
        idempotency_key: Option<&str>,
    ) -> String {
        let mut hasher = Sha256::new();

        // Include batch
        hasher.update(batch);

        // Include target node
        hasher.update(target_node.as_bytes());

        // Include idempotency key or use empty string
        if let Some(key) = idempotency_key {
            hasher.update(key.as_bytes());
        }

        let result = hasher.finalize();
        hex::encode(result)
    }

    /// Get a cached response if it exists and is still valid.
    pub async fn get(&self, key: &str) -> Option<CachedResponse> {
        let cache = self.entries.read().await;
        cache.get(key).and_then(|entry| {
            if entry.is_valid(self.default_ttl) {
                Some(entry.clone())
            } else {
                None
            }
        })
    }

    /// Store a response in the cache.
    pub async fn put(&self, key: String, response: CachedResponse) {
        let mut cache = self.entries.write().await;
        cache.insert(key, response);
    }

    /// Remove expired entries from the cache.
    pub async fn prune(&self) {
        let mut cache = self.entries.write().await;
        let now = Instant::now();

        cache.retain(|_, entry| {
            now.duration_since(entry.cached_at) < self.default_ttl
        });
    }

    /// Clear all entries from the cache.
    pub async fn clear(&self) {
        let mut cache = self.entries.write().await;
        cache.clear();
    }

    /// Get the number of entries in the cache.
    pub async fn len(&self) -> usize {
        let cache = self.entries.read().await;
        cache.len()
    }

    /// Check if the cache is empty.
    pub async fn is_empty(&self) -> bool {
        let cache = self.entries.read().await;
        cache.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_generation() {
        let batch = b"test_batch";
        let target_node = "node1";
        let idempotency_key = Some("key123");

        let key1 = RetryCache::cache_key(batch, target_node, idempotency_key);
        let key2 = RetryCache::cache_key(batch, target_node, idempotency_key);

        // Same inputs should produce same key
        assert_eq!(key1, key2);

        // Different inputs should produce different keys
        let key3 = RetryCache::cache_key(batch, "node2", idempotency_key);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_cache_key_without_idempotency() {
        let batch = b"test_batch";
        let target_node = "node1";

        let key1 = RetryCache::cache_key(batch, target_node, None);
        let key2 = RetryCache::cache_key(batch, target_node, None);

        // Same inputs should produce same key
        assert_eq!(key1, key2);

        // With and without idempotency key should be different
        let key3 = RetryCache::cache_key(batch, target_node, Some("key123"));
        assert_ne!(key1, key3);
    }

    #[tokio::test]
    async fn test_cache_put_get() {
        let cache = RetryCache::new(Duration::from_secs(60));

        let response = CachedResponse {
            body: b"test_response".to_vec(),
            status: 200,
            cached_at: Instant::now(),
        };

        let key = "test_key".to_string();
        cache.put(key.clone(), response.clone()).await;

        let retrieved = cache.get(&key).await;
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.body, b"test_response");
        assert_eq!(retrieved.status, 200);
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let cache = RetryCache::new(Duration::from_millis(100));

        let response = CachedResponse {
            body: b"test_response".to_vec(),
            status: 200,
            cached_at: Instant::now(),
        };

        let key = "test_key".to_string();
        cache.put(key.clone(), response).await;

        // Should be valid immediately
        let retrieved = cache.get(&key).await;
        assert!(retrieved.is_some());

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should be expired now
        let retrieved = cache.get(&key).await;
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_cache_prune() {
        let cache = RetryCache::new(Duration::from_millis(100));

        let response1 = CachedResponse {
            body: b"test_response1".to_vec(),
            status: 200,
            cached_at: Instant::now(),
        };

        let response2 = CachedResponse {
            body: b"test_response2".to_vec(),
            status: 200,
            cached_at: Instant::now(),
        };

        cache.put("key1".to_string(), response1).await;
        cache.put("key2".to_string(), response2).await;

        assert_eq!(cache.len().await, 2);

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        cache.prune().await;

        // Should be empty after pruning
        assert_eq!(cache.len().await, 0);
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let cache = RetryCache::new(Duration::from_secs(60));

        let response = CachedResponse {
            body: b"test_response".to_vec(),
            status: 200,
            cached_at: Instant::now(),
        };

        cache.put("key1".to_string(), response.clone()).await;
        cache.put("key2".to_string(), response).await;

        assert_eq!(cache.len().await, 2);

        cache.clear().await;

        assert_eq!(cache.len().await, 0);
    }

    #[test]
    fn test_cached_response_is_valid() {
        let response = CachedResponse {
            body: vec![],
            status: 200,
            cached_at: Instant::now(),
        };

        assert!(response.is_valid(Duration::from_secs(60)));
        assert!(!response.is_valid(Duration::from_millis(10)));
    }

    #[tokio::test]
    async fn test_cache_is_empty() {
        let cache = RetryCache::new(Duration::from_secs(60));

        assert!(cache.is_empty().await);

        let response = CachedResponse {
            body: vec![],
            status: 200,
            cached_at: Instant::now(),
        };

        cache.put("key1".to_string(), response).await;

        assert!(!cache.is_empty().await);
    }
}
