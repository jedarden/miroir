//! Scatter orchestration: fan-out logic and covering set builder.

use crate::config::UnavailableShardPolicy;
use crate::topology::{NodeId, Topology};
use crate::Result;

/// Scatter orchestrator: fans out requests to the covering set.
pub trait Scatter: Send + Sync {
    /// Execute a scatter request to multiple nodes.
    ///
    /// Returns a map of node ID to response. Failed nodes are omitted
    /// based on the unavailable shard policy.
    fn scatter(
        &self,
        topology: &Topology,
        nodes: Vec<NodeId>,
        request: ScatterRequest,
        policy: UnavailableShardPolicy,
    ) -> Result<ScatterResponse>;
}

/// A scatter request to be sent to each node.
#[derive(Debug, Clone)]
pub struct ScatterRequest {
    /// Request body (JSON or raw bytes).
    pub body: Vec<u8>,

    /// Request headers.
    pub headers: Vec<(String, String)>,

    /// HTTP method.
    pub method: String,

    /// Request path.
    pub path: String,
}

/// Response from a scatter operation.
#[derive(Debug, Clone)]
pub struct ScatterResponse {
    /// Responses from successful nodes.
    pub responses: Vec<NodeResponse>,

    /// Nodes that failed or timed out.
    pub failed: Vec<NodeId>,
}

/// Response from a single node.
#[derive(Debug, Clone)]
pub struct NodeResponse {
    /// Node that responded.
    pub node_id: NodeId,

    /// Response body.
    pub body: Vec<u8>,

    /// HTTP status code.
    pub status: u16,

    /// Response headers.
    pub headers: Vec<(String, String)>,
}

/// Default stub implementation of Scatter.
#[derive(Debug, Clone, Default)]
pub struct StubScatter;

impl Scatter for StubScatter {
    fn scatter(
        &self,
        _topology: &Topology,
        _nodes: Vec<NodeId>,
        _request: ScatterRequest,
        _policy: UnavailableShardPolicy,
    ) -> Result<ScatterResponse> {
        Ok(ScatterResponse {
            responses: Vec::new(),
            failed: Vec::new(),
        })
    }
}
