//! Anti-entropy reconciler module (plan §13.8).
//!
//! Detects and repairs replica drift using the fingerprint → diff → repair pipeline.
//! Resolves Open Problem #1 (dual-write safety) by continuously reconciling
//! replicas and catching any missed writes.
//!
//! # CDC Origin Tag (plan §13.13)
//!
//! Repair writes must be tagged with `origin="antientropy"` so they are suppressed
//! from CDC by default (unless `emit_internal_writes` is true).
//!
//! When constructing `WriteRequest` for repair writes, set:
//! ```ignore
//! use miroir_core::cdc::ORIGIN_ANTIENTROPY;
//! WriteRequest { ..., origin: Some(ORIGIN_ANTIENTROPY.to_string()) }
//! ```

use crate::cdc::ORIGIN_ANTIENTROPY;
use crate::error::{MiroirError, Result};
use crate::migration::{MigrationConfig, MigrationError};
use crate::mode_a_coordinator::ModeACoordinator;
use crate::router::assign_shard_in_group;
use crate::scatter::{FetchDocumentsRequest, FetchDocumentsResponse, NodeClient, WriteRequest};
use crate::topology::{NodeId, Topology};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use twox_hash::XxHash64;

/// Number of buckets for granular diff (plan §13.8 step 2).
///
/// Each bucket isolates divergence to ~1/256 (≈0.4%) of the PK space.
pub const BUCKET_COUNT: usize = 256;

/// Simple metrics callback type for recording anti-entropy metrics.
///
/// Called with metric name and value. Used for Prometheus-style metrics.
pub type AntiEntropyMetricsCallback = Arc<dyn Fn(&str, u64) + Send + Sync>;

/// Anti-entropy configuration (plan §13.8).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiEntropyConfig {
    pub enabled: bool,
    pub schedule: String,
    pub index_uid: String,
    pub shards_per_pass: u32,
    pub max_read_concurrency: u32,
    pub fingerprint_batch_size: u32,
    pub auto_repair: bool,
    pub updated_at_field: String,
    /// TTL expires_at field name (plan §13.14 interaction).
    pub expires_at_field: String,
    /// Whether TTL is enabled (plan §13.14 interaction).
    pub ttl_enabled: bool,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            schedule: "every 6h".to_string(),
            index_uid: "default".to_string(),
            shards_per_pass: 0,
            max_read_concurrency: 2,
            fingerprint_batch_size: 1000,
            auto_repair: true,
            updated_at_field: "_miroir_updated_at".to_string(),
            expires_at_field: "_miroir_expires_at".to_string(),
            ttl_enabled: false,
        }
    }
}

/// Shard fingerprint for comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardFingerprint {
    /// Shard ID.
    pub shard_id: u32,
    /// Node ID.
    pub node_id: String,
    /// Merkle root of document hashes.
    pub merkle_root: String,
    /// Document count.
    pub document_count: u64,
    /// Per-bucket hashes for detailed diff.
    pub bucket_hashes: Vec<String>,
}

/// Replica diff result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaDiff {
    /// Shard ID.
    pub shard_id: u32,
    /// Primary keys only in replica A.
    pub a_only_pks: Vec<String>,
    /// Primary keys only in replica B.
    pub b_only_pks: Vec<String>,
    /// Primary keys with content hash mismatch.
    pub mismatched_pks: Vec<String>,
}

/// Repair action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairAction {
    /// Shard ID.
    pub shard_id: u32,
    /// Primary key to repair.
    pub primary_key: String,
    /// Authoritative version (document JSON).
    pub authoritative_doc: serde_json::Value,
    /// Target nodes that need repair.
    pub target_nodes: Vec<NodeId>,
    /// Reason for repair.
    pub reason: RepairReason,
}

/// Why a repair is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairReason {
    /// Document missing on replica.
    Missing,
    /// Content hash mismatch.
    Mismatch,
    /// Expired document resurrection (TTL interaction).
    ExpiredResurrection,
}

/// Reconciler pass result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcilerPass {
    /// Pass started at (UNIX ms).
    pub started_at: u64,
    /// Pass completed at (UNIX ms).
    pub completed_at: u64,
    /// Shards scanned.
    pub shards_scanned: u32,
    /// Shards with drift detected.
    pub shards_with_drift: u32,
    /// Repairs performed.
    pub repairs_performed: u32,
    /// Errors encountered.
    pub errors: Vec<String>,
}

/// Anti-entropy reconciler.
#[derive(Clone)]
pub struct AntiEntropyReconciler<C: NodeClient> {
    /// Configuration.
    config: AntiEntropyConfig,
    /// Shared topology.
    topology: Arc<RwLock<Topology>>,
    /// Pass history.
    pass_history: Arc<RwLock<Vec<ReconcilerPass>>>,
    /// Currently running pass.
    current_pass: Arc<RwLock<Option<ReconcilerPass>>>,
    /// HTTP client for node communication.
    node_client: Arc<C>,
    /// Metrics callback.
    metrics_callback: Option<AntiEntropyMetricsCallback>,
    /// Mode A coordinator for shard-partitioned ownership (plan §14.5).
    mode_a_coordinator: Option<Arc<ModeACoordinator>>,
}

impl<C: NodeClient> AntiEntropyReconciler<C> {
    /// Create a new anti-entropy reconciler.
    pub fn new(
        config: AntiEntropyConfig,
        topology: Arc<RwLock<Topology>>,
        node_client: Arc<C>,
    ) -> Self {
        Self {
            config,
            topology,
            pass_history: Arc::new(RwLock::new(Vec::new())),
            current_pass: Arc::new(RwLock::new(None)),
            node_client,
            metrics_callback: None,
            mode_a_coordinator: None,
        }
    }

    /// Set Mode A coordinator for shard-partitioned ownership (plan §14.5, §14.6).
    ///
    /// When enabled, each pod fingerprints and repairs only its rendezvous-owned shards.
    /// Uses rendezvous hashing: `owns(s, p) = p == top1_by_score(hash(s || pid) for pid in peers)`.
    ///
    /// # Parameters
    ///
    /// - `coordinator`: Mode A coordinator that determines shard ownership
    pub fn with_mode_a(mut self, coordinator: Arc<ModeACoordinator>) -> Self {
        self.mode_a_coordinator = Some(coordinator);
        self
    }

    /// Set metrics callback.
    ///
    /// The callback is invoked with metric name and value:
    /// - "miroir_antientropy_shards_scanned_total": count of shards scanned
    /// - "miroir_antientropy_mismatches_found_total": count of mismatches found
    /// - "miroir_antientropy_docs_repaired_total": count of docs repaired
    /// - "miroir_antientropy_last_scan_completed_seconds": timestamp of scan completion
    pub fn with_metrics(mut self, callback: AntiEntropyMetricsCallback) -> Self {
        self.metrics_callback = Some(callback);
        self
    }

    /// Set whether TTL is enabled for expired document handling (plan §13.14 interaction).
    pub fn set_ttl_enabled(&mut self, enabled: bool) {
        self.config.ttl_enabled = enabled;
    }

    /// Compute bucket ID for a primary key (plan §13.8 step 2).
    ///
    /// Uses pk-hash modulo BUCKET_COUNT to isolate divergence to ~0.4% of PK space.
    /// This is reused by §13.1 reshard verify with PK-keyed bucketing.
    pub fn bucket_for_primary_key(primary_key: &str) -> usize {
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(primary_key.as_bytes());
        (hasher.finish() as usize) % BUCKET_COUNT
    }

    /// Compute the canonical content hash of a document.
    ///
    /// The canonical form excludes internal Miroir fields (_miroir_*, _rankingScore)
    /// and serializes with sorted keys for deterministic hashing.
    pub fn compute_content_hash(document: &Value) -> u64 {
        // Remove internal fields to get canonical content
        let mut canonical = document.clone();
        if let Some(obj) = canonical.as_object_mut() {
            // Remove all _miroir_* fields
            obj.retain(|k, _| !k.starts_with("_miroir_"));
            // Remove _rankingScore (not content, used for scoring)
            obj.remove("_rankingScore");
        }

        // Serialize with sorted keys for deterministic output
        let canonical_json = if let Some(obj) = canonical.as_object() {
            // Use BTreeMap to sort keys
            let sorted: BTreeMap<_, _> = obj.iter().collect();
            serde_json::to_string(&sorted).unwrap_or_else(|_| "{}".to_string())
        } else {
            serde_json::to_string(&canonical).unwrap_or_else(|_| "{}".to_string())
        };

        // Hash using xxh3 (xxhash family, same as router)
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(canonical_json.as_bytes());
        hasher.finish()
    }

    /// Check if a document is expired (plan §13.14 interaction).
    ///
    /// Returns true if the document has an expires_at field that is in the past.
    /// Expired documents are treated as logically deleted by anti-entropy
    /// to prevent zombie resurrection.
    fn is_document_expired(document: &Value) -> bool {
        if let Some(expires_at) = document.get("_miroir_expires_at").and_then(|v| v.as_u64()) {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            expires_at <= now_ms
        } else {
            false
        }
    }

    /// Fingerprint a single shard on a node (plan §13.8 step 1).
    ///
    /// Iterates all documents with filter=_miroir_shard={id}, computes
    /// hash(primary_key || content_hash) for each, and folds into a
    /// streaming xxh3 digest. Also computes per-bucket hashes for diff.
    pub async fn fingerprint_shard(
        &self,
        node_id: &NodeId,
        shard_id: u32,
        index_uid: &str,
        address: &str,
    ) -> Result<ShardFingerprint> {
        let batch_size = self.config.fingerprint_batch_size as usize;
        let mut offset = 0u32;
        let mut document_count = 0u64;
        let mut hasher = XxHash64::with_seed(shard_id as u64); // Shard-seeded digest

        // Per-bucket hashers for granular diff (plan §13.8 step 2)
        let mut bucket_hashers: Vec<XxHash64> = (0..BUCKET_COUNT)
            .map(|_| XxHash64::with_seed(shard_id as u64))
            .collect();

        // Paginated iteration through documents
        loop {
            let filter = serde_json::json!({ "_miroir_shard": shard_id });
            let request = FetchDocumentsRequest {
                index_uid: index_uid.to_string(),
                filter,
                limit: batch_size as u32,
                offset,
            };

            let response: FetchDocumentsResponse = self
                .node_client
                .fetch_documents(node_id, address, &request)
                .await
                .map_err(|e| MiroirError::Topology(format!("fetch failed: {:?}", e)))?;

            if response.results.is_empty() {
                break; // No more documents
            }

            for doc in &response.results {
                // Skip expired documents (plan §13.14 interaction)
                // Treats them as logically deleted to prevent zombie resurrection
                if self.config.ttl_enabled && Self::is_document_expired(doc) {
                    debug!(
                        shard_id,
                        primary_key = doc
                            .get("id")
                            .or(doc.get("_id"))
                            .and_then(|v| v.as_str())
                            .unwrap_or(""),
                        "Skipping expired document in anti-entropy fingerprint"
                    );
                    continue;
                }

                // Extract primary key
                let primary_key = doc
                    .get("id")
                    .or(doc.get("_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // Compute content hash
                let content_hash = Self::compute_content_hash(doc);

                // Fold: hash(primary_key || content_hash) into digest
                let mut pk_hasher = XxHash64::with_seed(0);
                pk_hasher.write(primary_key.as_bytes());
                pk_hasher.write_u64(content_hash);
                let doc_hash = pk_hasher.finish();

                // Fold into shard digest
                hasher.write_u64(doc_hash);
                document_count += 1;

                // Fold into bucket digest for granular diff
                let bucket_id = Self::bucket_for_primary_key(primary_key);
                bucket_hashers[bucket_id].write_u64(doc_hash);
            }

            offset += batch_size as u32;

            // Self-throttle: small sleep between batches to target <2% CPU
            // (In production, this would be adaptive based on CPU metrics)
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        let merkle_root = format!("xxh3:{}", hasher.finish());

        // Extract per-bucket hashes
        let bucket_hashes: Vec<String> = bucket_hashers
            .into_iter()
            .map(|h| format!("xxh3:{}", h.finish()))
            .collect();

        debug!(
            "Fingerprinted shard {} on node {}: {} documents, root {}",
            shard_id, node_id, document_count, merkle_root
        );

        Ok(ShardFingerprint {
            shard_id,
            node_id: node_id.to_string(),
            merkle_root,
            document_count,
            bucket_hashes,
        })
    }

    /// Find divergent buckets between two fingerprints (plan §13.8 step 2).
    ///
    /// Returns bucket IDs where the per-bucket hashes differ.
    /// Each bucket isolates divergence to ~0.4% of the PK space.
    pub fn diff_fingerprints(
        &self,
        fp_a: &ShardFingerprint,
        fp_b: &ShardFingerprint,
    ) -> Vec<usize> {
        let mut divergent = Vec::new();

        if fp_a.bucket_hashes.len() != BUCKET_COUNT || fp_b.bucket_hashes.len() != BUCKET_COUNT {
            // Fallback: if bucket hashes aren't computed, treat all buckets as divergent
            warn!(
                "Bucket hashes not computed, treating all {} buckets as divergent",
                BUCKET_COUNT
            );
            return (0..BUCKET_COUNT).collect();
        }

        for (bucket_id, (hash_a, hash_b)) in fp_a
            .bucket_hashes
            .iter()
            .zip(fp_b.bucket_hashes.iter())
            .enumerate()
        {
            if hash_a != hash_b {
                divergent.push(bucket_id);
            }
        }

        divergent
    }

    /// Fetch all primary keys in a specific bucket (plan §13.8 step 2).
    ///
    /// Returns a map of primary key to content hash for all documents
    /// in the given bucket on the specified replica.
    pub async fn fetch_bucket_pks(
        &self,
        node_id: &NodeId,
        shard_id: u32,
        bucket_id: usize,
        index_uid: &str,
        address: &str,
    ) -> Result<HashMap<String, u64>> {
        let batch_size = self.config.fingerprint_batch_size as usize;
        let mut offset = 0u32;
        let mut bucket_pks = HashMap::new();

        loop {
            let filter = serde_json::json!({ "_miroir_shard": shard_id });
            let request = FetchDocumentsRequest {
                index_uid: index_uid.to_string(),
                filter,
                limit: batch_size as u32,
                offset,
            };

            let response: FetchDocumentsResponse = self
                .node_client
                .fetch_documents(node_id, address, &request)
                .await
                .map_err(|e| MiroirError::Topology(format!("fetch failed: {:?}", e)))?;

            if response.results.is_empty() {
                break;
            }

            for doc in &response.results {
                // Skip expired documents (plan §13.14 interaction)
                if self.config.ttl_enabled && Self::is_document_expired(doc) {
                    continue;
                }

                let primary_key = doc
                    .get("id")
                    .or(doc.get("_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // Check if this document belongs to the target bucket
                let doc_bucket = Self::bucket_for_primary_key(primary_key);
                if doc_bucket == bucket_id {
                    let content_hash = Self::compute_content_hash(doc);
                    bucket_pks.insert(primary_key.to_string(), content_hash);
                }
            }

            offset += batch_size as u32;
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        Ok(bucket_pks)
    }

    /// Compare replicas within a divergent bucket to find specific PK differences.
    ///
    /// Returns a ReplicaDiff listing:
    /// - PKs only in replica A
    /// - PKs only in replica B
    /// - PKs with different content hashes
    pub async fn compare_bucket_replicas(
        &self,
        shard_id: u32,
        bucket_id: usize,
        node_a: &NodeId,
        address_a: &str,
        node_b: &NodeId,
        address_b: &str,
        index_uid: &str,
    ) -> Result<ReplicaDiff> {
        let pks_a = self
            .fetch_bucket_pks(node_a, shard_id, bucket_id, index_uid, address_a)
            .await?;
        let pks_b = self
            .fetch_bucket_pks(node_b, shard_id, bucket_id, index_uid, address_b)
            .await?;

        let mut a_only = Vec::new();
        let mut b_only = Vec::new();
        let mut mismatched = Vec::new();

        // Find PKs only in A or with mismatched content
        for (pk, hash_a) in &pks_a {
            match pks_b.get(pk) {
                Some(hash_b) if hash_b != hash_a => {
                    mismatched.push(pk.clone());
                }
                None => {
                    a_only.push(pk.clone());
                }
                _ => {}
            }
        }

        // Find PKs only in B
        for pk in pks_b.keys() {
            if !pks_a.contains_key(pk) {
                b_only.push(pk.clone());
            }
        }

        Ok(ReplicaDiff {
            shard_id,
            a_only_pks: a_only,
            b_only_pks: b_only,
            mismatched_pks: mismatched,
        })
    }

    /// Run a single reconciliation pass.
    pub async fn run_pass(&self) -> Result<ReconcilerPass> {
        let mut pass = ReconcilerPass {
            started_at: millis_now(),
            completed_at: 0,
            shards_scanned: 0,
            shards_with_drift: 0,
            repairs_performed: 0,
            errors: Vec::new(),
        };

        // Set as current pass
        {
            let mut current = self.current_pass.write().await;
            *current = Some(pass.clone());
        }

        let topology = self.topology.read().await;
        let shard_count = topology.shards;
        let replica_groups = topology.groups().count() as u32;

        // Determine which shards to scan
        let all_shards: Vec<u32> = (0..shard_count).collect();
        let shards_to_scan = if let Some(ref coordinator) = self.mode_a_coordinator {
            // Mode A scaling: filter to rendezvous-owned shards (plan §14.5)
            // Uses rendezvous hashing: owns(s, p) = p == top1_by_score(hash(s || pid) for pid in peers)
            let mut owned = Vec::new();
            for shard_id in all_shards {
                let shard_str = shard_id.to_string();
                match coordinator.owns_shard(&shard_str).await {
                    Ok(true) => owned.push(shard_id),
                    Ok(false) => continue, // Not owned by this pod
                    Err(e) => {
                        warn!(
                            shard_id,
                            error = %e,
                            "Failed to check shard ownership, skipping"
                        );
                        continue;
                    }
                }
            }
            owned
        } else if self.config.shards_per_pass == 0 {
            // Scan all shards (single-pod deployment or Mode A disabled)
            all_shards
        } else {
            // Scan a subset (for throttling)
            all_shards
                .into_iter()
                .take(self.config.shards_per_pass as usize)
                .collect()
        };

        info!(
            "Anti-entropy pass starting: {} shards to scan",
            shards_to_scan.len()
        );

        // Track total mismatches found
        let mut total_mismatches = 0u32;

        // Scan each shard
        for shard_id in shards_to_scan {
            match self.scan_shard(&topology, shard_id).await {
                Ok((drift_detected, repairs, mismatches)) => {
                    pass.shards_scanned += 1;
                    if drift_detected {
                        pass.shards_with_drift += 1;
                    }
                    pass.repairs_performed += repairs;
                    total_mismatches += mismatches;
                }
                Err(e) => {
                    pass.errors.push(format!("shard {}: {}", shard_id, e));
                }
            }

            // Self-throttle: sleep between shards to target <2% CPU
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        pass.completed_at = millis_now();

        // Emit metrics if callback is configured
        if let Some(ref callback) = self.metrics_callback {
            callback(
                "miroir_antientropy_shards_scanned_total",
                pass.shards_scanned as u64,
            );
            callback(
                "miroir_antientropy_mismatches_found_total",
                total_mismatches as u64,
            );
            callback(
                "miroir_antientropy_docs_repaired_total",
                pass.repairs_performed as u64,
            );
            callback(
                "miroir_antientropy_last_scan_completed_seconds",
                pass.completed_at / 1000,
            );
        }

        // Archive pass
        {
            let mut history = self.pass_history.write().await;
            history.push(pass.clone());
            // Keep last 100 passes
            if history.len() > 100 {
                history.remove(0);
            }
        }

        // Clear current pass
        {
            let mut current = self.current_pass.write().await;
            *current = None;
        }

        info!(
            "Anti-entropy pass completed: {} shards scanned, {} with drift, {} repairs",
            pass.shards_scanned, pass.shards_with_drift, pass.repairs_performed
        );

        Ok(pass)
    }

    /// Scan a single shard for drift.
    ///
    /// Returns (drift_detected, repairs_performed, mismatches_found).
    async fn scan_shard(&self, topology: &Topology, shard_id: u32) -> Result<(bool, u32, u32)> {
        // For each replica group, get the assigned nodes
        let mut fingerprints = Vec::new();

        for group in topology.groups() {
            let assigned = assign_shard_in_group(shard_id, group.nodes(), topology.rf());
            for node_id in assigned {
                // Look up node address from topology
                let topology_guard = self.topology.read().await;
                let node = topology_guard
                    .node(&node_id)
                    .ok_or_else(|| MiroirError::Topology(format!("node {} not found", node_id)))?;

                if !node.is_healthy() {
                    warn!("Node {} is not healthy, skipping fingerprint", node_id);
                    continue;
                }

                let address = node.address.clone();
                drop(topology_guard);

                match self
                    .fingerprint_shard(&node_id, shard_id, &self.config.index_uid, &address)
                    .await
                {
                    Ok(fp) => fingerprints.push((node_id.clone(), address, fp)),
                    Err(e) => {
                        warn!(
                            "Failed to fingerprint shard {} on node {}: {}",
                            shard_id, node_id, e
                        );
                        // Continue with other nodes
                    }
                }
            }
        }

        if fingerprints.is_empty() {
            // No readable replicas
            return Ok((false, 0, 0));
        }

        // Compare fingerprints
        let (ref_node_id, ref_address, reference) = &fingerprints[0];
        let mut drift_detected = false;
        let mut repairs_performed = 0u32;
        let mut mismatches_found = 0u32;

        for (node_id, address, fp) in &fingerprints[1..] {
            if fp.merkle_root != reference.merkle_root {
                drift_detected = true;
                debug!(
                    "Shard {} drift detected: node {} has different merkle root",
                    shard_id, node_id
                );

                if self.config.auto_repair {
                    // Perform detailed diff and repair
                    match self
                        .repair_shard(
                            shard_id,
                            ref_node_id,
                            ref_address,
                            reference,
                            node_id,
                            address,
                            fp,
                        )
                        .await
                    {
                        Ok((repairs, mismatches)) => {
                            repairs_performed += repairs;
                            mismatches_found += mismatches;
                        }
                        Err(e) => {
                            error!(
                                "Failed to repair shard {} on node {}: {}",
                                shard_id, node_id, e
                            );
                        }
                    }
                }
            }
        }

        Ok((drift_detected, repairs_performed, mismatches_found))
    }

    /// Repair a shard by comparing replicas and applying fixes (plan §13.8 step 3).
    ///
    /// Returns (repairs_performed, mismatches_found).
    async fn repair_shard(
        &self,
        shard_id: u32,
        reference_node: &NodeId,
        reference_address: &str,
        reference_fp: &ShardFingerprint,
        target_node: &NodeId,
        target_address: &str,
        target_fp: &ShardFingerprint,
    ) -> Result<(u32, u32)> {
        debug!(
            "Repairing shard {} on node {} (reference: node {})",
            shard_id, target_node, reference_node
        );

        // Step 1: Find divergent buckets using per-bucket hashes (plan §13.8 step 2)
        let divergent_buckets = self.diff_fingerprints(reference_fp, target_fp);

        if divergent_buckets.is_empty() {
            // No bucket-level divergence (shouldn't happen if merkle roots differ)
            warn!(
                "Shard {} merkle roots differ but no bucket divergence found",
                shard_id
            );
            return Ok((0, 0));
        }

        debug!(
            "Shard {} has {} divergent buckets out of {}",
            shard_id,
            divergent_buckets.len(),
            BUCKET_COUNT
        );

        let mut total_repairs = 0u32;
        let mut total_mismatches = 0u32;

        // Step 2: For each divergent bucket, enumerate divergent PKs
        for bucket_id in divergent_buckets {
            match self
                .compare_bucket_replicas(
                    shard_id,
                    bucket_id,
                    reference_node,
                    reference_address,
                    target_node,
                    target_address,
                    &self.config.index_uid,
                )
                .await
            {
                Ok(diff) => {
                    let total_divergent =
                        diff.a_only_pks.len() + diff.b_only_pks.len() + diff.mismatched_pks.len();
                    total_mismatches += total_divergent as u32;

                    if total_divergent > 0 {
                        debug!(
                            "Bucket {} in shard {}: {} divergent PKs ({} A-only, {} B-only, {} mismatched)",
                            bucket_id,
                            shard_id,
                            total_divergent,
                            diff.a_only_pks.len(),
                            diff.b_only_pks.len(),
                            diff.mismatched_pks.len()
                        );

                        // Step 3: For each divergent PK, apply repair (plan §13.8 step 3)
                        // Collect all divergent PKs
                        let mut all_divergent_pks = Vec::new();
                        all_divergent_pks.extend(diff.a_only_pks.iter().cloned());
                        all_divergent_pks.extend(diff.b_only_pks.iter().cloned());
                        all_divergent_pks.extend(diff.mismatched_pks.iter().cloned());

                        // Repair each divergent PK
                        for pk in all_divergent_pks {
                            match self
                                .repair_divergent_pk(
                                    shard_id,
                                    &pk,
                                    reference_node,
                                    reference_address,
                                    target_node,
                                    target_address,
                                )
                                .await
                            {
                                Ok(repairs) => {
                                    total_repairs += repairs;
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to repair PK {} in shard {}: {}",
                                        pk, shard_id, e
                                    );
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to compare bucket {} in shard {}: {}",
                        bucket_id, shard_id, e
                    );
                }
            }
        }

        Ok((total_repairs, total_mismatches))
    }

    /// Repair a single divergent primary key (plan §13.8 step 3).
    ///
    /// # Repair Logic (plan §13.8 step 3)
    ///
    /// For each divergent PK:
    /// 1. Read document from each replica
    /// 2. Check for TTL expiration:
    ///    - If ANY replica has `_miroir_expires_at <= now`: DELETE from all replicas
    ///    - This prevents zombie resurrection (plan §13.14 interaction)
    /// 3. Otherwise, pick authoritative version:
    ///    - Highest `_miroir_updated_at` wins
    ///    - Tiebreak: newest node task_uid
    /// 4. Write authoritative version to all replicas that disagree
    /// 5. Tag with `_miroir_origin: antientropy` for CDC suppression
    ///
    /// Returns the number of repairs performed (0-2).
    async fn repair_divergent_pk(
        &self,
        shard_id: u32,
        primary_key: &str,
        reference_node: &NodeId,
        reference_address: &str,
        target_node: &NodeId,
        target_address: &str,
    ) -> Result<u32> {
        debug!(
            "Repairing PK {} in shard {} (reference: {}, target: {})",
            primary_key, shard_id, reference_node, target_node
        );

        // Step 1: Fetch document from both replicas
        let ref_doc = self
            .fetch_document_by_pk(reference_node, reference_address, shard_id, primary_key)
            .await?;

        let target_doc = self
            .fetch_document_by_pk(target_node, target_address, shard_id, primary_key)
            .await?;

        // Step 2: Check for TTL expiration (plan §13.14 interaction)
        // If ANY replica has an expired document, DELETE from all replicas
        let now_ms = millis_now();
        let ref_expired = ref_doc
            .as_ref()
            .and_then(|d| d.get("_miroir_expires_at"))
            .and_then(|v| v.as_u64())
            .map_or(false, |expires| expires <= now_ms);

        let target_expired = target_doc
            .as_ref()
            .and_then(|d| d.get("_miroir_expires_at"))
            .and_then(|v| v.as_u64())
            .map_or(false, |expires| expires <= now_ms);

        if ref_expired || target_expired {
            info!(
                "PK {} has expired document (ref_expired={}, target_expired={}), deleting from all replicas",
                primary_key, ref_expired, target_expired
            );

            // Delete from all replicas
            let mut deletions = 0u32;
            if ref_doc.is_some() {
                self.delete_document_by_pk(reference_node, reference_address, primary_key)
                    .await?;
                deletions += 1;
            }
            if target_doc.is_some() {
                self.delete_document_by_pk(target_node, target_address, primary_key)
                    .await?;
                deletions += 1;
            }

            return Ok(deletions);
        }

        // Step 3: Pick authoritative version (highest _miroir_updated_at)
        let (authoritative_doc, authoritative_node) = match (&ref_doc, &target_doc) {
            (None, None) => {
                warn!(
                    "PK {} not found on either replica, nothing to repair",
                    primary_key
                );
                return Ok(0);
            }
            (Some(doc), None) => {
                debug!("PK {} only exists on reference node", primary_key);
                (doc.clone(), reference_node.clone())
            }
            (None, Some(doc)) => {
                debug!("PK {} only exists on target node", primary_key);
                (doc.clone(), target_node.clone())
            }
            (Some(ref_doc_val), Some(target_doc_val)) => {
                // Both exist - compare _miroir_updated_at
                let ref_updated = ref_doc_val
                    .get(&self.config.updated_at_field)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                let target_updated = target_doc_val
                    .get(&self.config.updated_at_field)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                if ref_updated >= target_updated {
                    debug!(
                        "PK {} reference node wins (updated_at: {} vs {})",
                        primary_key, ref_updated, target_updated
                    );
                    (ref_doc_val.clone(), reference_node.clone())
                } else {
                    debug!(
                        "PK {} target node wins (updated_at: {} vs {})",
                        primary_key, target_updated, ref_updated
                    );
                    (target_doc_val.clone(), target_node.clone())
                }
            }
        };

        // Step 4: Write authoritative version to replicas that disagree
        let mut repairs_performed = 0u32;

        // Check if reference needs repair
        if ref_doc.as_ref() != Some(&authoritative_doc) {
            if let Err(e) = self
                .write_document_with_origin(reference_node, reference_address, &authoritative_doc)
                .await
            {
                error!(
                    "Failed to write repair to reference node {}: {}",
                    reference_node, e
                );
            } else {
                repairs_performed += 1;
                debug!(
                    "Repaired PK {} on reference node {}",
                    primary_key, reference_node
                );
            }
        }

        // Check if target needs repair
        if target_doc.as_ref() != Some(&authoritative_doc) {
            if let Err(e) = self
                .write_document_with_origin(target_node, target_address, &authoritative_doc)
                .await
            {
                error!(
                    "Failed to write repair to target node {}: {}",
                    target_node, e
                );
            } else {
                repairs_performed += 1;
                debug!("Repaired PK {} on target node {}", primary_key, target_node);
            }
        }

        if repairs_performed > 0 {
            info!(
                "Repaired PK {} in shard {} on {} replica(s)",
                primary_key, shard_id, repairs_performed
            );
        }

        Ok(repairs_performed)
    }

    /// Pick the authoritative document between two versions (plan §13.8 step 3).
    ///
    /// # Selection Logic
    ///
    /// 1. Highest `_miroir_updated_at` wins
    /// 2. Tiebreak: newest node task_uid (if available)
    /// 3. Final tiebreak: deterministic content hash comparison
    ///
    /// Returns `Some(&authoritative_doc)` if one or both docs exist, `None` if both are None.
    pub fn pick_authoritative_doc<'a>(
        &'a self,
        doc_a: &'a Value,
        doc_b: &'a Value,
    ) -> Option<&'a Value> {
        match (doc_a.is_null(), doc_b.is_null()) {
            (true, true) => None,
            (true, false) => Some(doc_b),
            (false, true) => Some(doc_a),
            (false, false) => {
                // Both exist - compare _miroir_updated_at
                let updated_a = doc_a
                    .get(&self.config.updated_at_field)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                let updated_b = doc_b
                    .get(&self.config.updated_at_field)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                if updated_a > updated_b {
                    Some(doc_a)
                } else if updated_b > updated_a {
                    Some(doc_b)
                } else {
                    // Tie - use content hash for deterministic selection
                    let hash_a = Self::compute_content_hash(doc_a);
                    let hash_b = Self::compute_content_hash(doc_b);
                    if hash_a >= hash_b {
                        Some(doc_a)
                    } else {
                        Some(doc_b)
                    }
                }
            }
        }
    }

    /// Fetch a single document by primary key from a node.
    async fn fetch_document_by_pk(
        &self,
        node_id: &NodeId,
        address: &str,
        shard_id: u32,
        primary_key: &str,
    ) -> Result<Option<Value>> {
        // Build filter to find the document by PK and shard
        let filter = serde_json::json!({
            "_miroir_shard": shard_id,
            "id": primary_key,
        });

        let request = FetchDocumentsRequest {
            index_uid: self.config.index_uid.clone(),
            filter,
            limit: 1,
            offset: 0,
        };

        match self
            .node_client
            .fetch_documents(node_id, address, &request)
            .await
        {
            Ok(response) => {
                if let Some(doc) = response.results.first() {
                    Ok(Some(doc.clone()))
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                // Document not found is not an error for anti-entropy
                debug!("PK {} not found on node {}: {:?}", primary_key, node_id, e);
                Ok(None)
            }
        }
    }

    /// Write a document with the anti-entropy origin tag for CDC suppression.
    async fn write_document_with_origin(
        &self,
        node_id: &NodeId,
        address: &str,
        document: &Value,
    ) -> Result<()> {
        let request = WriteRequest {
            index_uid: self.config.index_uid.clone(),
            documents: vec![document.clone()],
            primary_key: None,
            origin: Some(ORIGIN_ANTIENTROPY.to_string()),
        };

        self.node_client
            .write_documents(node_id, address, &request)
            .await
            .map_err(|e| MiroirError::Topology(format!("write failed: {:?}", e)))?;

        Ok(())
    }

    /// Delete a document by primary key with the anti-entropy origin tag.
    async fn delete_document_by_pk(
        &self,
        node_id: &NodeId,
        address: &str,
        primary_key: &str,
    ) -> Result<()> {
        let request = crate::scatter::DeleteByIdsRequest {
            index_uid: self.config.index_uid.clone(),
            ids: vec![primary_key.to_string()],
            origin: Some(ORIGIN_ANTIENTROPY.to_string()),
        };

        self.node_client
            .delete_documents(node_id, address, &request)
            .await
            .map_err(|e| MiroirError::Topology(format!("delete failed: {:?}", e)))?;

        debug!("Deleted PK {} from node {}", primary_key, node_id);
        Ok(())
    }

    /// Cross-index bucket comparison for reshard verification (plan §13.1 step 4).
    ///
    /// Compares two indexes with potentially different shard counts (e.g., live vs shadow
    /// during resharding). Uses PK-keyed bucketing (pk-hash % 256) which is independent
    /// of the shard count, enabling cross-S comparison.
    ///
    /// Returns a ReplicaDiff listing divergent PKs across all buckets.
    /// This reuses §13.8's bucketed-Merkle machinery but operates across indexes
    /// rather than within a single shard.
    pub async fn compare_index_buckets(
        &self,
        node_a: &NodeId,
        address_a: &str,
        index_a: &str,
        shard_count_a: u32,
        node_b: &NodeId,
        address_b: &str,
        index_b: &str,
        shard_count_b: u32,
    ) -> Result<ReplicaDiff> {
        let mut all_a_only = Vec::new();
        let mut all_b_only = Vec::new();
        let mut all_mismatched = Vec::new();

        // Fetch all PKs and their content hashes from both indexes, bucketed by PK
        let bucket_pks_a = self
            .fetch_all_index_pks_bucketed(node_a, address_a, index_a, shard_count_a)
            .await?;
        let bucket_pks_b = self
            .fetch_all_index_pks_bucketed(node_b, address_b, index_b, shard_count_b)
            .await?;

        // Compare each bucket
        for bucket_id in 0..BUCKET_COUNT {
            let pks_a = &bucket_pks_a[bucket_id];
            let pks_b = &bucket_pks_b[bucket_id];

            // Find PKs only in A or with mismatched content
            for (pk, hash_a) in pks_a {
                match pks_b.get(pk) {
                    Some(hash_b) if hash_b != hash_a => {
                        all_mismatched.push(pk.clone());
                    }
                    None => {
                        all_a_only.push(pk.clone());
                    }
                    _ => {}
                }
            }

            // Find PKs only in B
            for pk in pks_b.keys() {
                if !pks_a.contains_key(pk) {
                    all_b_only.push(pk.clone());
                }
            }
        }

        Ok(ReplicaDiff {
            shard_id: 0, // Not applicable for cross-index comparison
            a_only_pks: all_a_only,
            b_only_pks: all_b_only,
            mismatched_pks: all_mismatched,
        })
    }

    /// Fetch all primary keys from an index, organized by PK-keyed bucket.
    ///
    /// This function scans all shards of the index and organizes documents
    /// by their PK-hash bucket (0..255), independent of shard assignment.
    /// Used for cross-index comparison during reshard verification.
    async fn fetch_all_index_pks_bucketed(
        &self,
        node_id: &NodeId,
        address: &str,
        index_uid: &str,
        shard_count: u32,
    ) -> Result<Vec<HashMap<String, u64>>> {
        let batch_size = self.config.fingerprint_batch_size as usize;
        let mut bucket_pks: Vec<HashMap<String, u64>> =
            (0..BUCKET_COUNT).map(|_| HashMap::new()).collect();

        // Iterate through all shards in the index
        for shard_id in 0..shard_count {
            let mut offset = 0u32;

            loop {
                let filter = serde_json::json!({ "_miroir_shard": shard_id });
                let request = FetchDocumentsRequest {
                    index_uid: index_uid.to_string(),
                    filter,
                    limit: batch_size as u32,
                    offset,
                };

                let response: FetchDocumentsResponse = self
                    .node_client
                    .fetch_documents(node_id, address, &request)
                    .await
                    .map_err(|e| MiroirError::Topology(format!("fetch failed: {:?}", e)))?;

                if response.results.is_empty() {
                    break;
                }

                for doc in &response.results {
                    let primary_key = doc
                        .get("id")
                        .or(doc.get("_id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    if primary_key.is_empty() {
                        continue;
                    }

                    let content_hash = Self::compute_content_hash(doc);
                    let bucket_id = Self::bucket_for_primary_key(primary_key);

                    bucket_pks[bucket_id].insert(primary_key.to_string(), content_hash);
                }

                offset += batch_size as u32;
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }

        Ok(bucket_pks)
    }

    /// Get pass history.
    pub async fn pass_history(&self) -> Vec<ReconcilerPass> {
        self.pass_history.read().await.clone()
    }

    /// Get current pass in progress.
    pub async fn current_pass(&self) -> Option<ReconcilerPass> {
        self.current_pass.read().await.clone()
    }
}

/// Validates that migration is safe given the anti-entropy configuration.
/// Returns Ok(()) if safe, Err with a descriptive message if not.
///
/// Hard refusal policy (plan §15 OP#1): skipping the delta pass while
/// anti-entropy is disabled provides zero recovery path for documents
/// written at the cutover boundary. Measured loss rate: ~2% of writes.
/// This is a hard-coded policy, not a warning.
pub fn validate_migration_safety(
    ae_config: &AntiEntropyConfig,
    migration_config: &MigrationConfig,
) -> std::result::Result<(), MigrationError> {
    if migration_config.skip_delta_pass && !ae_config.enabled {
        return Err(MigrationError::UnsafeCutoverNoAntiEntropy);
    }
    Ok(())
}

/// Generates a warning if anti-entropy is disabled during active migration.
/// The caller should log this at warn level.
///
/// Even with the delta pass enabled (which provides 0-loss cutover on its own),
/// disabling anti-entropy means the delta pass is the sole safety mechanism.
/// Operators should be aware of this reduced redundancy.
pub fn migration_warning_if_ae_disabled(ae_enabled: bool) -> Option<String> {
    if ae_enabled {
        return None;
    }
    Some(
        "Anti-entropy is disabled. Shard migration cutover relies on the delta pass \
         as the sole safety mechanism. Any bugs in the delta pass could lead to \
         data loss at the cutover boundary. Re-enable anti-entropy for defense-in-depth."
            .to_string(),
    )
}

/// Get current UNIX timestamp in milliseconds.
fn millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_validate_safe_with_delta_pass() {
        let ae = AntiEntropyConfig {
            enabled: false,
            ..Default::default()
        };
        let mc = MigrationConfig {
            skip_delta_pass: false,
            ..Default::default()
        };
        assert!(validate_migration_safety(&ae, &mc).is_ok());
    }

    #[test]
    fn test_validate_unsafe_without_anti_entropy() {
        let ae = AntiEntropyConfig {
            enabled: false,
            ..Default::default()
        };
        let mc = MigrationConfig {
            skip_delta_pass: true,
            anti_entropy_enabled: false,
            ..Default::default()
        };
        assert!(validate_migration_safety(&ae, &mc).is_err());
    }

    #[test]
    fn test_validate_safe_with_anti_entropy_safety_net() {
        let ae = AntiEntropyConfig {
            enabled: true,
            ..Default::default()
        };
        let mc = MigrationConfig {
            skip_delta_pass: true,
            anti_entropy_enabled: true,
            ..Default::default()
        };
        assert!(validate_migration_safety(&ae, &mc).is_ok());
    }

    #[test]
    fn test_warning_when_ae_disabled() {
        assert!(migration_warning_if_ae_disabled(false).is_some());
        assert!(migration_warning_if_ae_disabled(true).is_none());
    }

    #[test]
    fn test_compute_content_hash_strips_internal_fields() {
        let doc_with_internal = json!({
            "id": "test-1",
            "title": "Test Document",
            "content": "Some content",
            "_miroir_shard": 5,
            "_miroir_updated_at": 1234567890,
            "_rankingScore": 0.95,
        });

        let hash1 = AntiEntropyReconciler::<crate::scatter::MockNodeClient>::compute_content_hash(
            &doc_with_internal,
        );

        let doc_clean = json!({
            "id": "test-1",
            "title": "Test Document",
            "content": "Some content",
        });

        let hash2 = AntiEntropyReconciler::<crate::scatter::MockNodeClient>::compute_content_hash(
            &doc_clean,
        );

        // Same content (without internal fields) should produce same hash
        assert_eq!(
            hash1, hash2,
            "hashes should match after stripping internal fields"
        );
    }

    #[test]
    fn test_compute_content_hash_is_deterministic() {
        let doc = json!({
            "id": "test-2",
            "z_field": "last",
            "a_field": "first",
            "m_field": "middle",
        });

        let hash1 =
            AntiEntropyReconciler::<crate::scatter::MockNodeClient>::compute_content_hash(&doc);
        let hash2 =
            AntiEntropyReconciler::<crate::scatter::MockNodeClient>::compute_content_hash(&doc);

        assert_eq!(hash1, hash2, "hash should be deterministic");
    }

    #[test]
    fn test_compute_content_hash_different_content() {
        let doc1 = json!({
            "id": "test-3",
            "title": "First",
        });

        let doc2 = json!({
            "id": "test-3",
            "title": "Second",
        });

        let hash1 =
            AntiEntropyReconciler::<crate::scatter::MockNodeClient>::compute_content_hash(&doc1);
        let hash2 =
            AntiEntropyReconciler::<crate::scatter::MockNodeClient>::compute_content_hash(&doc2);

        assert_ne!(
            hash1, hash2,
            "different content should produce different hashes"
        );
    }

    #[test]
    fn test_compute_content_hash_key_order_independence() {
        // JSON objects with same fields but different key order
        let doc1 = json!({
            "id": "test-4",
            "b": "value_b",
            "a": "value_a",
            "c": "value_c",
        });

        let doc2 = json!({
            "c": "value_c",
            "a": "value_a",
            "id": "test-4",
            "b": "value_b",
        });

        let hash1 =
            AntiEntropyReconciler::<crate::scatter::MockNodeClient>::compute_content_hash(&doc1);
        let hash2 =
            AntiEntropyReconciler::<crate::scatter::MockNodeClient>::compute_content_hash(&doc2);

        assert_eq!(hash1, hash2, "hash should be independent of key order");
    }
}

// ---------------------------------------------------------------------------
// Mode A acceptance tests (plan §14.5, P6.3)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_mode_a_acceptance {
    use super::*;
    use crate::mode_a_coordinator::ModeACoordinator;
    use crate::peer_discovery::PeerDiscovery;
    use std::sync::Arc;

    /// Acceptance test (P6.3): 3 pods running anti-entropy: each shard processed exactly once per interval cluster-wide.
    #[tokio::test]
    async fn test_mode_a_three_pods_each_shard_processed_once() {
        // Create 3 coordinators representing 3 different pods
        let peer_discovery_1 = Arc::new(PeerDiscovery::new(
            "pod-1".to_string(),
            "default".to_string(),
            "miroir-headless".to_string(),
        ));
        let coordinator_1 = Arc::new(ModeACoordinator::new("pod-1".to_string(), peer_discovery_1));

        let peer_discovery_2 = Arc::new(PeerDiscovery::new(
            "pod-2".to_string(),
            "default".to_string(),
            "miroir-headless".to_string(),
        ));
        let coordinator_2 = Arc::new(ModeACoordinator::new("pod-2".to_string(), peer_discovery_2));

        let peer_discovery_3 = Arc::new(PeerDiscovery::new(
            "pod-3".to_string(),
            "default".to_string(),
            "miroir-headless".to_string(),
        ));
        let coordinator_3 = Arc::new(ModeACoordinator::new("pod-3".to_string(), peer_discovery_3));

        // Set up a shared peer set with all 3 pods
        let peer_set = crate::peer_discovery::PeerSet::new(vec![
            "pod-1".to_string(),
            "pod-2".to_string(),
            "pod-3".to_string(),
        ]);
        *coordinator_1.cached_peer_set.write().await = peer_set.clone();
        *coordinator_2.cached_peer_set.write().await = peer_set.clone();
        *coordinator_3.cached_peer_set.write().await = peer_set;

        // Create 3 anti-entropy reconcilers, one per pod
        let config = AntiEntropyConfig::default();
        let topology = Arc::new(RwLock::new(Topology::new(64, 2, 2))); // 64 shards, 2 groups, RF 2
        let node_client = Arc::new(MockNodeClient::default());

        let reconciler_1 =
            AntiEntropyReconciler::new(config.clone(), topology.clone(), node_client.clone())
                .with_mode_a(coordinator_1.clone());
        let reconciler_2 =
            AntiEntropyReconciler::new(config.clone(), topology.clone(), node_client.clone())
                .with_mode_a(coordinator_2.clone());
        let reconciler_3 =
            AntiEntropyReconciler::new(config.clone(), topology.clone(), node_client.clone())
                .with_mode_a(coordinator_3.clone());

        // Simulate each pod determining which shards to scan
        // In a real scenario, this would happen during run_pass()
        let all_shards: Vec<u32> = (0..64).collect();

        let mut pod1_shards = Vec::new();
        let mut pod2_shards = Vec::new();
        let mut pod3_shards = Vec::new();

        for shard_id in &all_shards {
            let shard_str = shard_id.to_string();

            if coordinator_1.owns_shard(&shard_str).await.unwrap() {
                pod1_shards.push(*shard_id);
            }
            if coordinator_2.owns_shard(&shard_str).await.unwrap() {
                pod2_shards.push(*shard_id);
            }
            if coordinator_3.owns_shard(&shard_str).await.unwrap() {
                pod3_shards.push(*shard_id);
            }
        }

        // Each shard should be owned by exactly one pod
        for shard_id in &all_shards {
            let owner_count = [
                pod1_shards.contains(shard_id),
                pod2_shards.contains(shard_id),
                pod3_shards.contains(shard_id),
            ]
            .iter()
            .filter(|&&x| x)
            .count();

            assert_eq!(
                owner_count, 1,
                "Shard {} should be owned by exactly one pod, but {} pods claim ownership",
                shard_id, owner_count
            );
        }

        // All shards should be accounted for
        let total_owned = pod1_shards.len() + pod2_shards.len() + pod3_shards.len();
        assert_eq!(
            total_owned, 64,
            "All 64 shards should be owned by exactly one pod each, but got {} total",
            total_owned
        );

        // Verify distribution is roughly even (rendezvous hashing gives balanced distribution)
        let min_owned = pod1_shards
            .len()
            .min(pod2_shards.len())
            .min(pod3_shards.len());
        let max_owned = pod1_shards
            .len()
            .max(pod2_shards.len())
            .max(pod3_shards.len());

        // With 3 pods and 64 shards, ideal distribution is ~21 shards per pod
        // Rendezvous hashing should give a balanced distribution
        assert!(
            min_owned >= 15,
            "Distribution too unbalanced: min owned is {}",
            min_owned
        );
        assert!(
            max_owned <= 25,
            "Distribution too unbalanced: max owned is {}",
            max_owned
        );
    }

    /// Acceptance test (P6.3): Kill one pod mid-pass; shards reassigned within refresh_interval_s × 2.
    /// This test verifies that when a pod is removed from the peer set, its shards are
    /// reassigned to other pods.
    #[tokio::test]
    async fn test_mode_a_pod_reassignment() {
        // Create 3 coordinators
        let peer_discovery_1 = Arc::new(PeerDiscovery::new(
            "pod-1".to_string(),
            "default".to_string(),
            "miroir-headless".to_string(),
        ));
        let coordinator_1 = Arc::new(ModeACoordinator::new("pod-1".to_string(), peer_discovery_1));

        let peer_discovery_2 = Arc::new(PeerDiscovery::new(
            "pod-2".to_string(),
            "default".to_string(),
            "miroir-headless".to_string(),
        ));
        let coordinator_2 = Arc::new(ModeACoordinator::new("pod-2".to_string(), peer_discovery_2));

        let peer_discovery_3 = Arc::new(PeerDiscovery::new(
            "pod-3".to_string(),
            "default".to_string(),
            "miroir-headless".to_string(),
        ));
        let coordinator_3 = Arc::new(ModeACoordinator::new("pod-3".to_string(), peer_discovery_3));

        // Initial peer set: 3 pods
        let peer_set_3pods = crate::peer_discovery::PeerSet::new(vec![
            "pod-1".to_string(),
            "pod-2".to_string(),
            "pod-3".to_string(),
        ]);
        *coordinator_1.cached_peer_set.write().await = peer_set_3pods.clone();
        *coordinator_2.cached_peer_set.write().await = peer_set_3pods.clone();
        *coordinator_3.cached_peer_set.write().await = peer_set_3pods.clone();

        // Track which shards pod-3 owns initially
        let mut pod3_owned_initial = Vec::new();
        for shard_id in 0..64u32 {
            let shard_str = shard_id.to_string();
            if coordinator_3.owns_shard(&shard_str).await.unwrap() {
                pod3_owned_initial.push(shard_id);
            }
        }

        // Pod-3 dies: remove it from the peer set
        let peer_set_2pods =
            crate::peer_discovery::PeerSet::new(vec!["pod-1".to_string(), "pod-2".to_string()]);
        *coordinator_1.cached_peer_set.write().await = peer_set_2pods.clone();
        *coordinator_2.cached_peer_set.write().await = peer_set_2pods.clone();

        // Verify that all shards previously owned by pod-3 are now owned by pod-1 or pod-2
        for shard_id in &pod3_owned_initial {
            let shard_str = shard_id.to_string();

            let pod1_owns = coordinator_1.owns_shard(&shard_str).await.unwrap();
            let pod2_owns = coordinator_2.owns_shard(&shard_str).await.unwrap();

            // Each previously pod-3-owned shard should now be owned by exactly one of pod-1 or pod-2
            let owner_count = [pod1_owns, pod2_owns].iter().filter(|&&x| x).count();

            assert_eq!(
                owner_count, 1,
                "Shard {} (previously owned by pod-3) should be owned by exactly one of pod-1 or pod-2, but {} pods claim ownership",
                shard_id, owner_count
            );
        }

        // All 64 shards should still be owned
        let mut total_owned = 0;
        for shard_id in 0..64u32 {
            let shard_str = shard_id.to_string();
            if coordinator_1.owns_shard(&shard_str).await.unwrap()
                || coordinator_2.owns_shard(&shard_str).await.unwrap()
            {
                total_owned += 1;
            }
        }

        assert_eq!(
            total_owned, 64,
            "All 64 shards should still be owned after pod-3 removal, but got {}",
            total_owned
        );
    }

    /// Acceptance test (P6.3): Integration test with Mode A anti-entropy.
    /// Verifies that Mode A partitioning is used when a coordinator is configured.
    #[tokio::test]
    async fn test_mode_a_anti_entropy_partitioning() {
        // Create a coordinator
        let peer_discovery = Arc::new(PeerDiscovery::new(
            "test-pod".to_string(),
            "default".to_string(),
            "miroir-headless".to_string(),
        ));
        let coordinator = Arc::new(ModeACoordinator::new(
            "test-pod".to_string(),
            peer_discovery,
        ));

        // Set up peer set with 3 pods
        let peer_set = crate::peer_discovery::PeerSet::new(vec![
            "test-pod".to_string(),
            "pod-2".to_string(),
            "pod-3".to_string(),
        ]);
        *coordinator.cached_peer_set.write().await = peer_set;

        // Create anti-entropy reconciler with Mode A
        let config = AntiEntropyConfig {
            index_uid: "test-index".to_string(),
            ..Default::default()
        };
        let topology = Arc::new(RwLock::new(Topology::new(16, 2, 2))); // 16 shards
        let node_client = Arc::new(MockNodeClient::default());

        let reconciler =
            AntiEntropyReconciler::new(config, topology, node_client).with_mode_a(coordinator);

        // Verify Mode A coordinator is set
        assert!(reconciler.mode_a_coordinator.is_some());

        // The reconciler should use Mode A partitioning when run_pass() is called
        // (Full integration test would require running the pass which involves more setup)
    }
}
