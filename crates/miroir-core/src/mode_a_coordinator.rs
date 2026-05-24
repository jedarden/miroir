//! Mode A shard-partitioned ownership coordinator (plan §14.5 Mode A).
//!
//! Each pod owns a subset of shards for background processing. Assignment uses
//! rendezvous hashing over the current peer set:
//!
//! ```text
//! peers      = discover_peers()           // headless-Service DNS lookup
//! owns(s, p) = p == top1_by_score(hash(s || pid) for pid in peers)
//! ```
//!
//! Applies to:
//! - Anti-entropy reconciler (§13.8) — each pod fingerprints and repairs the shards it owns
//! - Settings drift check (§13.5) — each pod polls a subset of (index, node) settings-hash pairs
//! - Task registry pruner — each pod prunes tasks where it wins the rendezvous score
//! - TTL sweeper (§13.14) — each pod sweeps only its rendezvous-owned shards
//! - Canary runner (§13.18) — each canary ID is rendezvous-owned by exactly one pod
//!
//! When the peer set changes (scale event, pod restart), rendezvous redistributes
//! ownership with minimal reshuffling. No explicit handoff — the new owner runs
//! the next scheduled pass. Transient double-work during a 15-second discovery
//! window is harmless: all operations are idempotent.

use crate::peer_discovery::{PeerDiscovery, PeerId, PeerSet};
use std::hash::Hasher;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use twox_hash::XxHash64;

/// Error type for Mode A coordination.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ModeAError {
    #[error("no peers discovered")]
    NoPeers,

    #[error("empty shard identifier")]
    EmptyShardId,

    #[error("empty peer identifier")]
    EmptyPeerId,
}

/// Result type for Mode A operations.
pub type Result<T> = std::result::Result<T, ModeAError>;

/// Mode A coordinator for shard-partitioned ownership.
///
/// Uses rendezvous hashing over the current peer set to determine which pod
/// owns a given shard or task.
pub struct ModeACoordinator {
    /// Our own pod ID (from POD_NAME env var).
    pod_id: PeerId,
    /// Peer discovery service.
    peer_discovery: Arc<PeerDiscovery>,
    /// Cached peer set (refreshed periodically).
    cached_peer_set: Arc<RwLock<PeerSet>>,
}

impl ModeACoordinator {
    /// Create a new Mode A coordinator.
    ///
    /// # Arguments
    ///
    /// * `pod_id` - Our pod ID (from `POD_NAME` env var)
    /// * `peer_discovery` - Peer discovery service
    pub fn new(pod_id: PeerId, peer_discovery: Arc<PeerDiscovery>) -> Self {
        let peer_set = PeerSet::new(vec![pod_id.clone()]);
        Self {
            pod_id,
            peer_discovery,
            cached_peer_set: Arc::new(RwLock::new(peer_set)),
        }
    }

    /// Refresh the peer set from DNS SRV records.
    ///
    /// Should be called periodically (e.g., every 15s per plan §14.5).
    pub async fn refresh_peers(&self) -> Result<usize> {
        let new_peer_set = self.peer_discovery.refresh().await.map_err(|e| {
            warn!("peer discovery failed: {}", e);
            ModeAError::NoPeers
        })?;

        let peer_count = new_peer_set.peers.len();
        if peer_count == 0 {
            warn!("peer discovery returned empty peer set");
            return Err(ModeAError::NoPeers);
        }

        // Update cached peer set
        let mut cached = self.cached_peer_set.write().await;
        *cached = new_peer_set;

        debug!(
            pod_id = %self.pod_id,
            peer_count,
            "refreshed Mode A peer set"
        );

        Ok(peer_count)
    }

    /// Get the current peer set.
    pub async fn peer_set(&self) -> PeerSet {
        self.cached_peer_set.read().await.clone()
    }

    /// Compute the rendezvous score for a shard-peer pair.
    ///
    /// Higher score = higher ownership priority.
    /// Uses xxhash (twox-hash) for consistency with router.
    fn rendezvous_score(shard_id: &str, peer_id: &str) -> u64 {
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(shard_id.as_bytes());
        hasher.write(b"||");
        hasher.write(peer_id.as_bytes());
        hasher.finish()
    }

    /// Find the peer that owns a given shard via rendezvous hashing.
    ///
    /// Returns the peer ID with the highest rendezvous score for the shard.
    pub async fn owner_for_shard(&self, shard_id: &str) -> Result<PeerId> {
        if shard_id.is_empty() {
            return Err(ModeAError::EmptyShardId);
        }

        let peer_set = self.peer_set().await;

        if peer_set.peers.is_empty() {
            return Err(ModeAError::NoPeers);
        }

        let mut best_peer = None;
        let mut best_score = 0u64;

        for peer in &peer_set.peers {
            let score = Self::rendezvous_score(shard_id, peer);
            if score > best_score {
                best_score = score;
                best_peer = Some(peer.clone());
            }
        }

        best_peer.ok_or(ModeAError::NoPeers)
    }

    /// Check if this pod owns a given shard.
    ///
    /// Returns true if this pod has the highest rendezvous score for the shard.
    pub async fn owns_shard(&self, shard_id: &str) -> Result<bool> {
        if shard_id.is_empty() {
            return Err(ModeAError::EmptyShardId);
        }

        let owner = self.owner_for_shard(shard_id).await?;
        Ok(owner == self.pod_id)
    }

    /// Check if this pod owns a task (by miroir_id).
    ///
    /// Uses the same rendezvous hashing as shard ownership.
    /// Task registry pruner uses this to partition task cleanup.
    pub async fn owns_task(&self, miroir_id: &str) -> Result<bool> {
        if miroir_id.is_empty() {
            return Err(ModeAError::EmptyShardId);
        }

        let peer_set = self.peer_set().await;

        if peer_set.peers.is_empty() {
            return Err(ModeAError::NoPeers);
        }

        let mut best_score = 0u64;
        let mut is_owner = false;

        for peer in &peer_set.peers {
            let score = Self::rendezvous_score(miroir_id, peer);
            if score > best_score {
                best_score = score;
                is_owner = (peer == &self.pod_id);
            }
        }

        Ok(is_owner)
    }

    /// Check if this pod owns a canary (by canary ID).
    ///
    /// Canary runner uses this to partition canary execution.
    pub async fn owns_canary(&self, canary_id: &str) -> Result<bool> {
        self.owns_task(canary_id).await
    }

    /// Check if this pod owns an (index, node) pair for settings drift checking.
    ///
    /// Combines index and node into a single key for rendezvous hashing.
    pub async fn owns_settings_check(&self, index_uid: &str, node_id: &str) -> Result<bool> {
        let key = format!("{}:{}", index_uid, node_id);
        self.owns_task(&key).await
    }

    /// Get the list of shards owned by this pod.
    ///
    /// Computes ownership for all shards and returns the ones this pod owns.
    pub async fn owned_shards(&self, all_shards: &[u32]) -> Result<Vec<u32>> {
        let peer_set = self.peer_set().await;

        if peer_set.peers.is_empty() {
            return Err(ModeAError::NoPeers);
        }

        let mut owned = Vec::new();

        for &shard_id in all_shards {
            let shard_str = shard_id.to_string();
            if self.owns_shard(&shard_str).await? {
                owned.push(shard_id);
            }
        }

        Ok(owned)
    }

    /// Get the fraction of shards owned by this pod.
    ///
    /// Returns a value between 0.0 and 1.0.
    pub async fn ownership_fraction(&self, all_shards: &[u32]) -> Result<f64> {
        let owned = self.owned_shards(all_shards).await?;
        let total = all_shards.len() as f64;
        let owned_count = owned.len() as f64;

        if total > 0.0 {
            Ok(owned_count / total)
        } else {
            Ok(0.0)
        }
    }

    /// Get the current peer count.
    pub async fn peer_count(&self) -> usize {
        self.peer_set().await.peers.len()
    }

    /// Check if we are the only peer (single-pod deployment).
    pub async fn is_single_pod(&self) -> bool {
        self.peer_count().await <= 1
    }

    /// Get our pod ID.
    pub fn pod_id(&self) -> &str {
        &self.pod_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rendezvous_score_deterministic() {
        // Same inputs should produce same score
        let score1 = ModeACoordinator::rendezvous_score("shard-42", "pod-1");
        let score2 = ModeACoordinator::rendezvous_score("shard-42", "pod-1");
        assert_eq!(score1, score2);
    }

    #[test]
    fn test_rendezvous_score_different_peers() {
        // Different peers should produce different scores
        let score1 = ModeACoordinator::rendezvous_score("shard-42", "pod-1");
        let score2 = ModeACoordinator::rendezvous_score("shard-42", "pod-2");
        assert_ne!(score1, score2);
    }

    #[test]
    fn test_rendezvous_score_different_shards() {
        // Different shards should produce different scores
        let score1 = ModeACoordinator::rendezvous_score("shard-1", "pod-1");
        let score2 = ModeACoordinator::rendezvous_score("shard-2", "pod-1");
        assert_ne!(score1, score2);
    }

    #[test]
    fn test_owns_shard_empty_id() {
        let coordinator = test_coordinator();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let result = coordinator.owns_shard("").await;
            assert!(matches!(result, Err(ModeAError::EmptyShardId)));
        });
    }

    #[test]
    fn test_owns_task_by_miroir_id() {
        let coordinator = test_coordinator();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // With single pod, we own everything
            let result = coordinator.owns_task("miroir-task-123").await;
            assert!(result.unwrap());
        });
    }

    #[test]
    fn test_owns_settings_check() {
        let coordinator = test_coordinator();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // With single pod, we own everything
            let result = coordinator.owns_settings_check("my-index", "node-1").await;
            assert!(result.unwrap());
        });
    }

    #[test]
    fn test_owned_shards() {
        let coordinator = test_coordinator();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let all_shards: Vec<u32> = (0..10).collect();
            let owned = coordinator.owned_shards(&all_shards).await.unwrap();

            // With single pod, we own all shards
            assert_eq!(owned.len(), 10);
        });
    }

    #[test]
    fn test_ownership_fraction() {
        let coordinator = test_coordinator();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let all_shards: Vec<u32> = (0..10).collect();
            let fraction = coordinator.ownership_fraction(&all_shards).await.unwrap();

            // With single pod, we own 100% of shards
            assert_eq!(fraction, 1.0);
        });
    }

    #[test]
    fn test_is_single_pod() {
        let coordinator = test_coordinator();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            assert!(coordinator.is_single_pod().await);
        });
    }

    #[test]
    fn test_no_peers_error() {
        use tokio::sync::RwLock;

        // Create a coordinator with an empty peer set
        let peer_discovery = Arc::new(PeerDiscovery::new(
            "test-pod".to_string(),
            "default".to_string(),
            "miroir-headless".to_string(),
        ));

        let coordinator = ModeACoordinator::new("test-pod".to_string(), peer_discovery);

        // Manually set empty peer set
        let empty_set = PeerSet::new(vec![]);
        *coordinator.cached_peer_set.write().await = empty_set;

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let result = coordinator.owns_shard("shard-1").await;
            assert!(matches!(result, Err(ModeAError::NoPeers)));
        });
    }

    fn test_coordinator() -> ModeACoordinator {
        use std::net::{Ipv4Addr, SocketAddr};

        // Create a mock peer discovery with our pod
        let peer_discovery = Arc::new(PeerDiscovery::new(
            "test-pod".to_string(),
            "default".to_string(),
            "miroir-headless".to_string(),
        ));

        ModeACoordinator::new("test-pod".to_string(), peer_discovery)
    }
}
