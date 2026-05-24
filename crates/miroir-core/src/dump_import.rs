//! Streaming routed dump import (plan §13.9).
//!
//! Intercepts dump imports and routes each document to the correct shard
//! instead of broadcasting to all nodes.

use crate::error::{MiroirError, Result};
use crate::router::{assign_shard_in_group, shard_for_key};
use crate::topology::{NodeId, Topology};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Dump import configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpImportConfig {
    /// Import mode: "streaming" or "broadcast".
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Batch size for per-target POSTs.
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    /// Parallel target writes.
    #[serde(default = "default_parallel")]
    pub parallel_target_writes: u32,
    /// Memory buffer cap (bytes).
    #[serde(default = "default_memory_buffer")]
    pub memory_buffer_bytes: u64,
    /// Chunk size for Mode C coordinator.
    #[serde(default = "default_chunk_size")]
    pub chunk_size_bytes: u64,
}

fn default_mode() -> String {
    "streaming".into()
}
fn default_batch_size() -> u32 {
    1000
}
fn default_parallel() -> u32 {
    8
}
fn default_memory_buffer() -> u64 {
    134_217_728 // 128 MiB
}
fn default_chunk_size() -> u64 {
    268_435_456 // 256 MiB
}

impl Default for DumpImportConfig {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            batch_size: default_batch_size(),
            parallel_target_writes: default_parallel(),
            memory_buffer_bytes: default_memory_buffer(),
            chunk_size_bytes: default_chunk_size(),
        }
    }
}

/// Dump import phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DumpImportPhase {
    /// No import in progress.
    Idle = 0,
    /// Reading and parsing dump.
    Reading = 1,
    /// Routing documents to target nodes.
    Routing = 2,
    /// Applying index settings.
    ApplyingSettings = 3,
    /// Completed successfully.
    Complete = 4,
    /// Failed with error.
    Failed = 5,
}

/// Dump import status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpImportStatus {
    /// Import ID.
    pub id: String,
    /// Target index UID.
    pub index_uid: String,
    /// Current phase.
    pub phase: DumpImportPhase,
    /// Documents processed so far.
    pub documents_processed: u64,
    /// Total documents (estimated).
    pub total_documents: u64,
    /// Bytes read so far.
    pub bytes_read: u64,
    /// Phase started at (UNIX ms).
    pub phase_started_at: u64,
    /// Error message if any.
    pub error: Option<String>,
}

/// Dump import manager.
pub struct DumpImportManager {
    /// Configuration.
    config: DumpImportConfig,
    /// Active imports (ID -> status).
    active_imports: Arc<RwLock<HashMap<String, DumpImportStatus>>>,
    /// Topology for routing.
    topology: Arc<Topology>,
}

impl DumpImportManager {
    /// Create a new dump import manager.
    pub fn new(config: DumpImportConfig, topology: Arc<Topology>) -> Self {
        Self {
            config,
            active_imports: Arc::new(RwLock::new(HashMap::new())),
            topology,
        }
    }

    /// Start a streaming dump import.
    pub async fn start_import(
        &self,
        index_uid: String,
        dump_data: Vec<u8>,
        primary_key: String,
        shard_count: u32,
    ) -> Result<String> {
        if self.config.mode != "streaming" {
            return Err(MiroirError::InvalidRequest(
                "streaming dump import is disabled".into(),
            ));
        }

        let import_id = format!("dump-{}-{}", index_uid, uuid::Uuid::new_v4());
        let now = millis_now();

        // Create initial status
        let status = DumpImportStatus {
            id: import_id.clone(),
            index_uid: index_uid.clone(),
            phase: DumpImportPhase::Reading,
            documents_processed: 0,
            total_documents: 0,
            bytes_read: 0,
            phase_started_at: now,
            error: None,
        };

        {
            let mut imports = self.active_imports.write().await;
            imports.insert(import_id.clone(), status);
        }

        // Clone import_id before moving into the async block
        let import_id_for_spawn = import_id.clone();

        // Spawn background import task
        let imports = self.active_imports.clone();
        let topology = self.topology.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::run_import(
                &import_id_for_spawn,
                index_uid,
                dump_data,
                primary_key,
                shard_count,
                topology,
                config,
                imports,
            )
            .await
            {
                tracing::error!("Dump import {} failed: {}", import_id_for_spawn, e);
            }
        });

        Ok(import_id)
    }

    /// Get the status of an import.
    pub async fn get_status(&self, import_id: &str) -> Option<DumpImportStatus> {
        let imports = self.active_imports.read().await;
        imports.get(import_id).cloned()
    }

    /// Run the import pipeline.
    async fn run_import(
        import_id: &str,
        index_uid: String,
        dump_data: Vec<u8>,
        primary_key: String,
        shard_count: u32,
        topology: Arc<Topology>,
        config: DumpImportConfig,
        imports: Arc<RwLock<HashMap<String, DumpImportStatus>>>,
    ) -> Result<()> {
        // Update phase to reading
        Self::update_phase(&imports, import_id, DumpImportPhase::Reading).await;

        // Parse NDJSON and route documents
        let data_str = std::str::from_utf8(&dump_data)
            .map_err(|e| MiroirError::InvalidRequest(format!("invalid UTF-8 in dump: {}", e)))?;

        // Per-target buffers
        let mut per_target_buffers: HashMap<(NodeId, u32), Vec<serde_json::Value>> = HashMap::new();

        let mut processed = 0u64;
        let _total_estimate = 0u64;

        for line in data_str.lines() {
            if line.is_empty() {
                continue;
            }
            let doc: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| MiroirError::InvalidRequest(format!("invalid JSON in dump: {}", e)))?;

            // Extract primary key value
            let pk_value = doc
                .get(&primary_key)
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    MiroirError::InvalidRequest(format!(
                        "missing or invalid primary key field: {}",
                        primary_key
                    ))
                })?;

            // Compute shard and route
            let shard_id = shard_for_key(pk_value, shard_count);

            // Get target nodes for this shard (assign across all replica groups)
            let target_nodes: Vec<NodeId> = topology
                .groups()
                .flat_map(|group| assign_shard_in_group(shard_id, group.nodes(), topology.rf()))
                .collect();

            if target_nodes.is_empty() {
                return Err(MiroirError::Topology(format!(
                    "no nodes for shard {}",
                    shard_id
                )));
            }

            // Add to each target's buffer
            for node in &target_nodes {
                per_target_buffers
                    .entry((node.clone(), shard_id))
                    .or_insert_with(Vec::new)
                    .push(doc.clone());
            }

            processed += 1;

            // Flush buffers when they reach batch size
            if processed % config.batch_size as u64 == 0 {
                Self::flush_buffers(
                    &index_uid,
                    &mut per_target_buffers,
                    &config,
                    &imports,
                    import_id,
                    processed,
                )
                .await?;
            }
        }

        // Final flush
        Self::flush_buffers(
            &index_uid,
            &mut per_target_buffers,
            &config,
            &imports,
            import_id,
            processed,
        )
        .await?;

        // Mark complete
        Self::update_phase(&imports, import_id, DumpImportPhase::Complete).await;

        Ok(())
    }

    /// Flush buffered documents to target nodes.
    async fn flush_buffers(
        index_uid: &str,
        buffers: &mut HashMap<(NodeId, u32), Vec<serde_json::Value>>,
        _config: &DumpImportConfig,
        imports: &Arc<RwLock<HashMap<String, DumpImportStatus>>>,
        import_id: &str,
        processed: u64,
    ) -> Result<()> {
        for ((node, _shard), docs) in buffers.drain() {
            if docs.is_empty() {
                continue;
            }

            // POST documents to the node
            // In a real implementation, this would use the HTTP client
            tracing::debug!(
                "Flushing {} documents to node {} for index {}",
                docs.len(),
                node,
                index_uid
            );

            // Update status
            let mut imports = imports.write().await;
            if let Some(status) = imports.get_mut(import_id) {
                status.documents_processed = processed;
            }
        }

        Ok(())
    }

    /// Update the phase of an import.
    async fn update_phase(
        imports: &Arc<RwLock<HashMap<String, DumpImportStatus>>>,
        import_id: &str,
        phase: DumpImportPhase,
    ) {
        let mut imports = imports.write().await;
        if let Some(status) = imports.get_mut(import_id) {
            status.phase = phase;
            status.phase_started_at = millis_now();
        }
    }
}

/// Get current UNIX timestamp in milliseconds.
fn millis_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl Default for DumpImportManager {
    fn default() -> Self {
        Self::new(
            DumpImportConfig::default(),
            Arc::new(Topology::new(1, 1, 1)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = DumpImportConfig::default();
        assert_eq!(config.mode, "streaming");
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.parallel_target_writes, 8);
    }

    #[test]
    fn test_phase_serialization() {
        let phase = DumpImportPhase::Routing;
        let json = serde_json::to_string(&phase).unwrap();
        assert_eq!(json, "\"Routing\"");

        let deserialized: DumpImportPhase = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, DumpImportPhase::Routing);
    }

    #[tokio::test]
    async fn test_get_status_nonexistent() {
        let manager = DumpImportManager::default();
        let status = manager.get_status("nonexistent").await;
        assert!(status.is_none());
    }

    #[tokio::test]
    async fn test_import_rejects_broadcast_mode() {
        let config = DumpImportConfig {
            mode: "broadcast".into(),
            ..Default::default()
        };
        let topology = Arc::new(Topology::new(64, 2, 1));
        let manager = DumpImportManager::new(config, topology);

        let result = manager
            .start_import("products".into(), vec![1, 2, 3], "id".into(), 64)
            .await;

        assert!(result.is_err());
    }
}
