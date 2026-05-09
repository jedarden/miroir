//! Write path: document routing with hash-based sharding and quorum.

use crate::client::NodeClient;
use crate::error_response::ErrorResponse;
use crate::scatter::HttpScatter;
use crate::state::ProxyState;
use miroir_core::config::UnavailableShardPolicy;
use miroir_core::router;
use miroir_core::scatter::ScatterRequest;
use miroir_core::topology::Topology;
use miroir_core::{MiroirError, Result};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use uuid::Uuid;

/// Write path executor for document batches.
pub struct WriteExecutor {
    state: ProxyState,
    scatter: HttpScatter,
}

impl WriteExecutor {
    pub fn new(state: ProxyState) -> Self {
        let node_timeout_ms = state.config.scatter.node_timeout_ms;
        let scatter = HttpScatter::new(state.client.clone(), node_timeout_ms);

        Self { state, scatter }
    }

    /// Execute a document write (add/replace) for an index.
    pub async fn write_documents(
        &self,
        index: &str,
        documents: Vec<Value>,
        primary_key: Option<&str>,
    ) -> Result<WriteResult> {
        // Validate primary key is known
        let pk = self.resolve_primary_key(index, primary_key).await?;

        // Hash documents by shard and group by target nodes
        let topology = self.state.topology().await;
        let shard_count = self.state.config.shards;
        let rf = self.state.config.replication_factor as usize;

        let mut shard_groups: HashMap<u32, Vec<Value>> = HashMap::new();
        let mut reserved_field_errors = Vec::new();

        for (idx, doc) in documents.iter().enumerate() {
            // Check for reserved fields
            if let Some(obj) = doc.as_object() {
                if obj.contains_key("_miroir_shard") {
                    reserved_field_errors.push(idx);
                    continue;
                }
            }

            // Extract primary key value
            let pk_value = self.extract_pk_value(doc, &pk)?;
            let shard_id = router::shard_for_key(&pk_value, shard_count);

            // Inject _miroir_shard
            let mut doc_with_shard = doc.clone();
            if let Some(obj) = doc_with_shard.as_object_mut() {
                obj.insert("_miroir_shard".to_string(), json!(shard_id));
            }

            shard_groups
                .entry(shard_id)
                .or_insert_with(Vec::new)
                .push(doc_with_shard);
        }

        if !reserved_field_errors.is_empty() {
            return Err(MiroirError::Routing(format!(
                "{} documents contain reserved field _miroir_shard: {:?}",
                reserved_field_errors.len(),
                reserved_field_errors
            )));
        }

        // For each shard, compute write targets and group by node
        let mut node_batches: HashMap<String, Vec<Value>> = HashMap::new();

        for (shard_id, docs) in shard_groups {
            let targets = router::write_targets(shard_id, &topology);

            for target in targets {
                let node = topology
                    .node(&target)
                    .ok_or_else(|| MiroirError::Routing(format!("node {} not found", target.as_str())))?;

                node_batches
                    .entry(node.id.as_str().to_string())
                    .or_insert_with(Vec::new)
                    .extend(docs.clone());
            }
        }

        // Fan out writes to all nodes
        let miroir_task_id = format!("mtask-{}", Uuid::new_v4());

        let mut node_tasks: HashMap<String, u64> = HashMap::new();
        let mut group_quorum: HashMap<u32, GroupQuorum> = HashMap::new();
        let mut failed_nodes = Vec::new();

        for (node_id, docs) in node_batches {
            let body = serde_json::to_vec(&docs).unwrap();
            let path = format!("/indexes/{}/documents", index);

            let request = ScatterRequest {
                body,
                headers: vec![],
                method: "POST".to_string(),
                path,
            };

            // Send to this node
            let result = self
                .scatter
                .scatter(&topology, vec![node_id.clone().into()], request, UnavailableShardPolicy::Partial)
                .await?;

            if let Some(resp) = result.responses.first() {
                // Parse response to get task UID
                if let Some(task_uid) = resp.body.get("taskUid").and_then(|v| v.as_u64()) {
                    node_tasks.insert(node_id.clone(), task_uid);

                    // Track per-group quorum
                    if let Some(node) = topology.node(&node_id.clone().into()) {
                        let group_id = node.replica_group;
                        let quorum = group_quorum.entry(group_id).or_insert_with(|| {
                            GroupQuorum {
                                group_id,
                                rf,
                                acked: HashSet::new(),
                            }
                        });
                        quorum.acked.insert(node_id.clone());
                    }
                } else {
                    failed_nodes.push(node_id);
                }
            } else {
                failed_nodes.push(node_id);
            }
        }

        // Check quorum - write succeeds if at least one group met quorum
        let degraded_groups = self.check_quorum(&group_quorum, &topology);
        let any_group_met_quorum = group_quorum.values().any(|q| q.met_quorum());

        if !any_group_met_quorum {
            return Err(MiroirError::Routing("No replica group met quorum".to_string()));
        }

        Ok(WriteResult {
            miroir_task_id,
            node_tasks,
            degraded_groups,
        })
    }

    async fn resolve_primary_key(&self, index: &str, primary_key: Option<&str>) -> Result<String> {
        if let Some(pk) = primary_key {
            return Ok(pk.to_string());
        }

        // Query index to get primary key
        let topology = self.state.topology().await;
        let first_node = topology.nodes().next();

        if let Some(node) = first_node {
            let resp = self
                .state
                .client
                .send_to_node(&topology, &node.id, "GET", &format!("/indexes/{}", index), None, &[])
                .await?;

            if let Some(pk) = resp.body.get("primaryKey").and_then(|v| v.as_str()) {
                return Ok(pk.to_string());
            }
        }

        Err(MiroirError::Routing(format!(
            "Index {} does not have a primary key",
            index
        )))
    }

    fn extract_pk_value(&self, doc: &Value, pk: &str) -> Result<String> {
        let obj = doc
            .as_object()
            .ok_or_else(|| MiroirError::Routing("Document is not an object".to_string()))?;

        let value = obj.get(pk).ok_or_else(|| {
            MiroirError::Routing(format!("Primary key '{}' not found in document", pk))
        })?;

        Ok(value.to_string())
    }

    fn check_quorum(&self, group_quorum: &HashMap<u32, GroupQuorum>, topology: &Topology) -> Vec<u32> {
        let mut degraded = Vec::new();

        for (group_id, quorum) in group_quorum {
            if !quorum.met_quorum() {
                degraded.push(*group_id);
            }
        }

        degraded
    }
}

/// Result of a document write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    pub miroir_task_id: String,
    pub node_tasks: HashMap<String, u64>,
    pub degraded_groups: Vec<u32>,
}

/// Quorum tracking for a replica group.
#[derive(Debug)]
struct GroupQuorum {
    group_id: u32,
    rf: usize,
    acked: HashSet<String>,
}

impl GroupQuorum {
    fn met_quorum(&self) -> bool {
        let required = (self.rf / 2) + 1;
        self.acked.len() >= required
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miroir_core::config::{MiroirConfig, NodeConfig, ServerConfig};
    use miroir_core::topology::{Node, NodeId, Topology};

    #[tokio::test]
    async fn test_extract_pk_value() {
        let doc = json!({"id": "test123", "name": "foo"});
        let executor = create_test_executor();

        let result = executor.extract_pk_value(&doc, "id").unwrap();
        assert_eq!(result, "\"test123\"");
    }

    #[tokio::test]
    async fn test_extract_pk_value_missing() {
        let doc = json!({"name": "foo"});
        let executor = create_test_executor();

        let result = executor.extract_pk_value(&doc, "id");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_group_quorum_met() {
        let quorum = GroupQuorum {
            group_id: 0,
            rf: 3,
            acked: HashSet::from(["node1".to_string(), "node2".to_string()]),
        };

        assert!(quorum.met_quorum()); // 2 >= (3/2)+1 = 2
    }

    #[tokio::test]
    async fn test_group_quorum_not_met() {
        let quorum = GroupQuorum {
            group_id: 0,
            rf: 3,
            acked: HashSet::from(["node1".to_string()]),
        };

        assert!(!quorum.met_quorum()); // 1 < 2
    }

    fn create_test_executor() -> WriteExecutor {
        let config = MiroirConfig {
            shards: 64,
            replication_factor: 2,
            ..Default::default()
        };

        let state = ProxyState::new(config).unwrap();
        WriteExecutor::new(state)
    }
}
