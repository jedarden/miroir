//! Peer discovery via Kubernetes headless Service SRV records (plan §14.5).
//!
//! This module provides zero-config peer discovery for Miroir pods in the same
//! Deployment. Each pod periodically performs an SRV lookup against the headless
//! Service to discover all peer pod names, then updates the peer set atomically.
//!
//! # Peer Identity
//!
//! - `PeerId = POD_NAME` (the pod name injected via Downward API)
//! - The headless Service SRV record returns a list of `{target, port}` entries
//! - The `target` field contains the pod DNS name (e.g., `miroir-miroir-0.miroir-headless.default.svc.cluster.local`)
//! - We extract the pod name from the first component of the target
//!
//! # Usage
//!
//! ```no_run
//! use miroir_core::peer_discovery::{PeerDiscovery, PeerId};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pod_name = std::env::var("POD_NAME").unwrap();
//!     let namespace = std::env::var("POD_NAMESPACE").unwrap();
//!     let service_name = "miroir-headless";
//!
//!     let discovery = PeerDiscovery::new(
//!         pod_name,
//!         namespace,
//!         service_name.to_string(),
//!     );
//!
//!     // Refresh peers
//!     let peers = discovery.refresh().await;
//!     println!("Discovered {} peers", peers.peers.len());
//! }
//! ```

use crate::error::{MiroirError, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Unique identifier for a peer pod.
///
/// This is simply the pod name (e.g., `miroir-miroir-0`).
pub type PeerId = String;

/// The current set of discovered peers with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSet {
    /// List of peer pod names (including self).
    pub peers: Vec<PeerId>,
    /// Instant when this peer set was last refreshed.
    #[serde(skip, default = "Instant::now")]
    pub refreshed_at: Instant,
}

impl PeerSet {
    /// Create a new peer set.
    pub fn new(peers: Vec<PeerId>) -> Self {
        Self {
            peers,
            refreshed_at: Instant::now(),
        }
    }

    /// Count of peers in the set.
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Whether the peer set is empty.
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

/// Peer discovery via Kubernetes headless Service.
pub struct PeerDiscovery {
    /// Our own pod name (injected via Downward API).
    pod_name: PeerId,
    /// Kubernetes namespace (injected via Downward API).
    namespace: String,
    /// Headless Service name (e.g., "miroir-headless").
    service_name: String,
    /// Current peer set.
    peer_set: Arc<RwLock<PeerSet>>,
}

impl PeerDiscovery {
    /// Create a new peer discovery instance.
    ///
    /// # Arguments
    ///
    /// * `pod_name` - Our pod name (from `POD_NAME` env var)
    /// * `namespace` - Kubernetes namespace (from `POD_NAMESPACE` env var)
    /// * `service_name` - Headless Service name (e.g., "miroir-headless")
    pub fn new(pod_name: String, namespace: String, service_name: String) -> Self {
        Self {
            pod_name,
            namespace,
            service_name,
            peer_set: Arc::new(RwLock::new(PeerSet::new(Vec::new()))),
        }
    }

    /// Get the current peer set.
    pub async fn peers(&self) -> Vec<PeerId> {
        self.peer_set.read().await.peers.clone()
    }

    /// Get the peer set count.
    pub async fn peer_count(&self) -> usize {
        self.peer_set.read().await.len()
    }

    /// Refresh the peer set by performing an SRV lookup.
    ///
    /// This resolves `_miroir._tcp.<service>.<namespace>.svc.cluster.local`
    /// and extracts pod names from the returned targets.
    ///
    /// Returns the updated peer set.
    #[cfg(feature = "peer-discovery")]
    pub async fn refresh(&self) -> Result<PeerSet> {
        let srv_name = format!(
            "_miroir._tcp.{}.{}.svc.cluster.local",
            self.service_name, self.namespace
        );

        // Perform SRV lookup using blocking task
        // Use trust-dns-resolver with a config that works in Kubernetes
        // In Kubernetes, we use the cluster DNS server (typically at 10.96.0.10:53)
        use trust_dns_resolver::config::{ResolverConfig, ResolverOpts};
        use trust_dns_resolver::Resolver;
        use trust_dns_resolver::config::NameServerConfig;
        use std::net::{IpAddr, Ipv4Addr};

        let lookup = tokio::task::spawn_blocking(move || {
            // Create a resolver config pointing to Kubernetes DNS
            let ns = NameServerConfig {
                socket_addr: (IpAddr::V4(Ipv4Addr::new(10, 96, 0, 10)), 53).into(),
                protocol: trust_dns_resolver::config::Protocol::Udp,
                tls_dns_name: None,
            };
            let config = ResolverConfig::from_parts::<Vec<NameServerConfig>>(
                None,
                vec![],
                vec![ns].into(),
            );

            let resolver = Resolver::new(config, ResolverOpts::default())
                .map_err(|e| MiroirError::Discovery(format!("failed to create DNS resolver: {}", e)))?;

            resolver.srv_lookup(&srv_name)
                .map_err(|e| MiroirError::Discovery(format!("SRV lookup failed for {}: {}", srv_name, e)))
        })
        .await
        .map_err(|e| MiroirError::Discovery(format!("SRV lookup task failed: {}", e)))??;

        // Extract pod names from SRV targets
        // Each SRV record has a target like "miroir-miroir-0.miroir-headless.default.svc.cluster.local"
        // We extract the first component as the pod name.
        let mut peers: Vec<PeerId> = lookup
            .iter()
            .filter_map(|srv| {
                let target = srv.target().to_string();
                // Remove trailing dot if present
                let target = target.strip_suffix('.').unwrap_or(&target);
                // Split and take first component
                target.split('.').next().map(|s| s.to_string())
            })
            .collect();

        // Sort for deterministic ordering
        peers.sort();

        // Update peer set
        let new_peer_set = PeerSet::new(peers);
        *self.peer_set.write().await = new_peer_set.clone();

        Ok(new_peer_set)
    }

    /// Refresh the peer set (fallback when peer-discovery feature is disabled).
    #[cfg(not(feature = "peer-discovery"))]
    pub async fn refresh(&self) -> Result<PeerSet> {
        Err(MiroirError::Discovery(
            "peer-discovery feature is disabled".to_string(),
        ))
    }

    /// Get our own pod name.
    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_set_empty() {
        let set = PeerSet::new(vec![]);
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_peer_set_with_peers() {
        let set = PeerSet::new(vec!["pod-1".into(), "pod-2".into(), "pod-3".into()]);
        assert!(!set.is_empty());
        assert_eq!(set.len(), 3);
    }
}
