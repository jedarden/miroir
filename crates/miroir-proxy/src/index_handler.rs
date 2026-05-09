//! Index lifecycle operations: create, delete, stats.

use crate::state::ProxyState;
use miroir_core::topology::Topology;
use miroir_core::{MiroirError, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

/// Index lifecycle executor.
pub struct IndexExecutor {
    state: ProxyState,
}

impl IndexExecutor {
    pub fn new(state: ProxyState) -> Self {
        Self { state }
    }

    /// Create an index on all nodes.
    pub async fn create_index(&self, uid: &str, primary_key: Option<&str>) -> Result<IndexResult> {
        let topology = self.state.topology().await;

        // Prepare request body
        let mut body = json!({
            "uid": uid,
        });

        if let Some(pk) = primary_key {
            body["primaryKey"] = json!(pk);
        }

        let body_bytes = serde_json::to_vec(&body).unwrap();

        // Broadcast to all nodes
        let mut node_tasks = HashMap::new();
        let mut failed_nodes = Vec::new();

        for node in topology.nodes() {
            match self
                .state
                .client
                .send_to_node(
                    &topology,
                    &node.id,
                    "POST",
                    "/indexes",
                    Some(&body_bytes),
                    &[],
                )
                .await
            {
                Ok(resp) if (200..300).contains(&resp.status) => {
                    if let Some(task_uid) = resp.body.get("taskUid").and_then(|v| v.as_u64()) {
                        node_tasks.insert(node.id.as_str().to_string(), task_uid);
                    }
                }
                _ => {
                    failed_nodes.push(node.id.as_str().to_string());
                }
            }
        }

        if !failed_nodes.is_empty() {
            // Rollback: delete from successful nodes
            for node_id in node_tasks.keys() {
                let _ = self
                    .state
                    .client
                    .send_to_node(
                        &topology,
                        &node_id.clone().into(),
                        "DELETE",
                        &format!("/indexes/{}", uid),
                        None,
                        &[],
                    )
                    .await;
            }

            return Err(MiroirError::Routing(format!(
                "Failed to create index on nodes: {:?}",
                failed_nodes
            )));
        }

        // Add _miroir_shard to filterable attributes
        self.add_miroir_shard_filterable(uid).await?;

        let miroir_task_id = format!("mtask-{}", Uuid::new_v4());

        Ok(IndexResult {
            miroir_task_id,
            node_tasks,
        })
    }

    /// Delete an index from all nodes.
    pub async fn delete_index(&self, uid: &str) -> Result<IndexResult> {
        let topology = self.state.topology().await;

        let mut node_tasks = HashMap::new();
        let mut failed_nodes = Vec::new();

        for node in topology.nodes() {
            match self
                .state
                .client
                .send_to_node(
                    &topology,
                    &node.id,
                    "DELETE",
                    &format!("/indexes/{}", uid),
                    None,
                    &[],
                )
                .await
            {
                Ok(resp) if (200..300).contains(&resp.status) => {
                    if let Some(task_uid) = resp.body.get("taskUid").and_then(|v| v.as_u64()) {
                        node_tasks.insert(node.id.as_str().to_string(), task_uid);
                    }
                }
                _ => {
                    failed_nodes.push(node.id.as_str().to_string());
                }
            }
        }

        if !failed_nodes.is_empty() {
            return Err(MiroirError::Routing(format!(
                "Failed to delete index on nodes: {:?}",
                failed_nodes
            )));
        }

        let miroir_task_id = format!("mtask-{}", Uuid::new_v4());

        Ok(IndexResult {
            miroir_task_id,
            node_tasks,
        })
    }

    /// Get aggregated stats for an index.
    pub async fn get_stats(&self, uid: &str) -> Result<Value> {
        let topology = self.state.topology().await;

        let mut total_documents = 0u64;
        let mut field_distribution: HashMap<String, u64> = HashMap::new();
        let mut failed_nodes = Vec::new();

        for node in topology.nodes() {
            match self
                .state
                .client
                .send_to_node(
                    &topology,
                    &node.id,
                    "GET",
                    &format!("/indexes/{}/stats", uid),
                    None,
                    &[],
                )
                .await
            {
                Ok(resp) if (200..300).contains(&resp.status) => {
                    // Sum numberOfDocuments
                    if let Some(count) = resp.body.get("numberOfDocuments").and_then(|v| v.as_u64()) {
                        total_documents += count;
                    }

                    // Merge fieldDistribution
                    if let Some(fields) = resp.body.get("fieldDistribution").and_then(|v| v.as_object()) {
                        for (field, count) in fields {
                            let count_val = count.as_u64().unwrap_or(0);
                            *field_distribution.entry(field.clone()).or_insert(0) += count_val;
                        }
                    }
                }
                _ => {
                    failed_nodes.push(node.id.as_str().to_string());
                }
            }
        }

        if failed_nodes.len() > topology.nodes().count() / 2 {
            return Err(MiroirError::Routing(format!(
                "Failed to get stats from majority of nodes: {:?}",
                failed_nodes
            )));
        }

        Ok(json!({
            "numberOfDocuments": total_documents,
            "fieldDistribution": field_distribution,
        }))
    }

    /// Add _miroir_shard to filterable attributes.
    async fn add_miroir_shard_filterable(&self, uid: &str) -> Result<()> {
        let topology = self.state.topology().await;

        // Get current settings
        let first_node = topology.nodes().next();
        if let Some(node) = first_node {
            if let Ok(resp) = self
                .state
                .client
                .send_to_node(
                    &topology,
                    &node.id,
                    "GET",
                    &format!("/indexes/{}/settings/filterable-attributes", uid),
                    None,
                    &[],
                )
                .await
            {
                if let Some(attrs) = resp.body.as_array() {
                    let mut attrs_vec: Vec<String> = attrs
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();

                    if !attrs_vec.contains(&"_miroir_shard".to_string()) {
                        attrs_vec.push("_miroir_shard".to_string());

                        let body = serde_json::to_vec(&attrs_vec).unwrap();

                        // Broadcast to all nodes
                        for node in topology.nodes() {
                            let _ = self
                                .state
                                .client
                                .send_to_node(
                                    &topology,
                                    &node.id,
                                    "PUT",
                                    &format!("/indexes/{}/settings/filterable-attributes", uid),
                                    Some(&body),
                                    &[],
                                )
                                .await;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Result of an index operation.
#[derive(Debug, Clone)]
pub struct IndexResult {
    pub miroir_task_id: String,
    pub node_tasks: HashMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use miroir_core::config::MiroirConfig;

    #[tokio::test]
    async fn test_index_result_creation() {
        let result = IndexResult {
            miroir_task_id: "mtask-123".to_string(),
            node_tasks: HashMap::new(),
        };

        assert_eq!(result.miroir_task_id, "mtask-123");
    }

    fn create_test_executor() -> IndexExecutor {
        let config = MiroirConfig {
            shards: 64,
            replication_factor: 2,
            ..Default::default()
        };

        let state = ProxyState::new(config).unwrap();
        IndexExecutor::new(state)
    }
}
