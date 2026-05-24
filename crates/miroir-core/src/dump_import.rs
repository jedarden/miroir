//! Streaming routed dump import (plan §13.9).
//!
//! Intercepts dump imports and routes each document to the correct shard
//! instead of broadcasting to all nodes.

use crate::error::{MiroirError, Result};
use crate::router::assign_shard_in_group;
use crate::router::shard_for_key;
use crate::scatter::{NodeClient, WriteRequest};
use crate::topology::{NodeId, Topology};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

impl DumpImportPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Reading => "reading",
            Self::Routing => "routing",
            Self::ApplyingSettings => "applying_settings",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "idle" => Some(Self::Idle),
            "reading" => Some(Self::Reading),
            "routing" => Some(Self::Routing),
            "applying_settings" => Some(Self::ApplyingSettings),
            "complete" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Dump import status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpImportStatus {
    /// Import ID.
    pub id: String,
    /// Target index UID.
    pub index_uid: String,
    /// Current phase.
    pub phase: String,
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
pub struct DumpImportManager<C: NodeClient + Send + Sync + 'static> {
    /// Configuration.
    config: DumpImportConfig,
    /// Active imports (ID -> status).
    active_imports: Arc<RwLock<HashMap<String, DumpImportStatus>>>,
    /// Topology for routing.
    topology: Arc<Topology>,
    /// HTTP client for posting documents.
    client: Arc<C>,
}

impl<C: NodeClient + Send + Sync + 'static> DumpImportManager<C> {
    /// Create a new dump import manager.
    pub fn new(config: DumpImportConfig, topology: Arc<Topology>, client: C) -> Self {
        Self {
            config,
            active_imports: Arc::new(RwLock::new(HashMap::new())),
            topology,
            client: Arc::new(client),
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
            phase: DumpImportPhase::Reading.as_str().to_string(),
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

        // Run the import directly (for now, can be made background later)
        let result = Self::run_import(
            &import_id,
            index_uid,
            dump_data,
            primary_key,
            shard_count,
            self.topology.clone(),
            self.config.clone(),
            self.active_imports.clone(),
            self.client.clone(),
        )
        .await;

        if let Err(e) = result {
            tracing::error!("Dump import {} failed: {}", import_id, e);

            // Update status to failed
            let mut imports = self.active_imports.write().await;
            if let Some(status) = imports.get_mut(&import_id) {
                status.phase = DumpImportPhase::Failed.as_str().to_string();
                status.error = Some(e.to_string());
                status.phase_started_at = millis_now();
            }
        }

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
        client: Arc<C>,
    ) -> Result<()> {
        // Update phase to reading
        Self::update_phase(&imports, import_id, DumpImportPhase::Reading).await;

        // Parse NDJSON and route documents
        let data_str = std::str::from_utf8(&dump_data)
            .map_err(|e| MiroirError::InvalidRequest(format!("invalid UTF-8 in dump: {}", e)))?;

        // Per-target buffers: (node_id, shard_id) -> Vec<documents>
        let mut per_target_buffers: HashMap<(NodeId, u32), Vec<Value>> = HashMap::new();

        let mut processed = 0u64;
        let bytes_read = dump_data.len() as u64;

        for line in data_str.lines() {
            if line.is_empty() {
                continue;
            }
            let mut doc: Value = serde_json::from_str(line)
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

            // Inject _miroir_shard into the document
            doc["_miroir_shard"] = serde_json::json!(shard_id);

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
                    bytes_read,
                    &client,
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
            bytes_read,
            &client,
        )
        .await?;

        // Mark complete
        Self::update_phase(&imports, import_id, DumpImportPhase::Complete).await;

        Ok(())
    }

    /// Flush buffered documents to target nodes.
    async fn flush_buffers(
        index_uid: &str,
        buffers: &mut HashMap<(NodeId, u32), Vec<Value>>,
        _config: &DumpImportConfig,
        imports: &Arc<RwLock<HashMap<String, DumpImportStatus>>>,
        import_id: &str,
        processed: u64,
        bytes_read: u64,
        client: &Arc<C>,
    ) -> Result<()> {
        // Build write requests for each target
        let mut write_tasks = Vec::new();

        for ((node, _shard), docs) in buffers.drain() {
            if docs.is_empty() {
                continue;
            }

            let node_id = node.clone();
            let index = index_uid.to_string();
            let documents = docs;
            let client_ref = client;

            write_tasks.push(async move {
                let write_req = WriteRequest {
                    index_uid: index.clone(),
                    documents: documents.clone(),
                    primary_key: None,
                    origin: None,
                };

                let result = client_ref.write_documents(&node_id, "", &write_req).await;

                (node_id, documents.len(), result)
            });
        }

        // Execute all writes in parallel (with concurrency limit)
        let results = futures_util::stream::iter(write_tasks)
            .buffer_unordered(_config.parallel_target_writes as usize)
            .collect::<Vec<_>>()
            .await;

        // Check for errors
        for (node, doc_count, result) in results {
            let _total_docs = doc_count as u64;

            match result {
                Ok(resp) if resp.success => {
                    tracing::debug!(
                        "Flushed {} documents to node {} for index {}",
                        doc_count,
                        node,
                        index_uid
                    );
                }
                Ok(resp) => {
                    tracing::warn!(
                        "Failed to flush {} documents to node {} for index {}: {}",
                        doc_count,
                        node,
                        index_uid,
                        resp.message.unwrap_or_default()
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Error flushing documents to node {} for index {}: {:?}",
                        node,
                        index_uid,
                        e
                    );
                }
            }
        }

        // Update status
        let mut imports_guard = imports.write().await;
        if let Some(status) = imports_guard.get_mut(import_id) {
            status.documents_processed = processed;
            status.bytes_read = bytes_read;
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
            status.phase = phase.as_str().to_string();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scatter::MockNodeClient;
    use crate::topology::Node;

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
        assert_eq!(phase.as_str(), "routing");

        let deserialized = DumpImportPhase::from_str("routing").unwrap();
        assert_eq!(deserialized, DumpImportPhase::Routing);
    }

    #[test]
    fn test_phase_roundtrip() {
        for phase in [
            DumpImportPhase::Idle,
            DumpImportPhase::Reading,
            DumpImportPhase::Routing,
            DumpImportPhase::ApplyingSettings,
            DumpImportPhase::Complete,
            DumpImportPhase::Failed,
        ] {
            let s = phase.as_str();
            let parsed = DumpImportPhase::from_str(s).unwrap();
            assert_eq!(parsed, phase);
        }
    }

    #[tokio::test]
    async fn test_get_status_nonexistent() {
        let topology = Arc::new(Topology::new(64, 2, 1));
        let client = MockNodeClient::default();
        let manager = DumpImportManager::new(DumpImportConfig::default(), topology, client);

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
        let client = MockNodeClient::default();
        let manager = DumpImportManager::new(config, topology, client);

        let result = manager
            .start_import("products".into(), vec![1, 2, 3], "id".into(), 64)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_import_success() {
        // Create topology with nodes
        let mut topology = Topology::new(64, 2, 1);
        topology.add_node(Node::new(
            NodeId::new("node-0".into()),
            "http://node-0:7700".into(),
            0,
        ));
        topology.add_node(Node::new(
            NodeId::new("node-1".into()),
            "http://node-1:7700".into(),
            0,
        ));

        let topology = Arc::new(topology);

        // Create mock client
        let mut client = MockNodeClient::default();
        client.write_responses.insert(
            NodeId::new("node-0".into()),
            WriteResponse {
                success: true,
                task_uid: Some(1),
                message: None,
                code: None,
                error_type: None,
            },
        );
        client.write_responses.insert(
            NodeId::new("node-1".into()),
            WriteResponse {
                success: true,
                task_uid: Some(2),
                message: None,
                code: None,
                error_type: None,
            },
        );

        let manager = DumpImportManager::new(DumpImportConfig::default(), topology, client);

        // Create test dump data
        let dump_data = r#"{"id": "1", "name": "Product 1"}
{"id": "2", "name": "Product 2"}
{"id": "3", "name": "Product 3"}"#
            .as_bytes()
            .to_vec();

        let import_id = manager
            .start_import("products".into(), dump_data, "id".into(), 64)
            .await
            .expect("Import should succeed");

        // Wait a bit for the import to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Check final status
        let status = manager.get_status(&import_id).await;
        assert!(status.is_some());

        let status = status.unwrap();
        assert_eq!(status.index_uid, "products");
        assert_eq!(status.documents_processed, 3);
    }

    #[tokio::test]
    async fn test_import_invalid_json() {
        let topology = Arc::new(Topology::new(64, 2, 1));
        let client = MockNodeClient::default();
        let manager = DumpImportManager::new(DumpImportConfig::default(), topology, client);

        let dump_data = b"invalid json".to_vec();

        let result = manager
            .start_import("products".into(), dump_data, "id".into(), 64)
            .await;

        // Should return an ID but the import will fail in the background
        assert!(result.is_ok());

        let import_id = result.unwrap();

        // Wait for background task to fail
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let status = manager.get_status(&import_id).await;
        assert!(status.is_some());

        let status = status.unwrap();
        assert_eq!(status.phase, "failed");
        assert!(status.error.is_some());
    }
}
