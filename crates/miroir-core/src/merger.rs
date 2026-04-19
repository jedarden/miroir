//! Result merger: combines shard results into a single response.

use crate::Result;
use serde_json::Value;

/// Result merger: combines responses from multiple shards.
pub trait Merger: Send + Sync {
    /// Merge search results from multiple shards.
    ///
    /// Takes the raw JSON responses from each shard and produces
    /// a merged result with global sorting, offset/limit applied,
    /// and facet aggregation.
    fn merge(
        &self,
        shard_responses: Vec<ShardResponse>,
        offset: usize,
        limit: usize,
        client_requested_score: bool,
    ) -> Result<MergedResult>;
}

/// Response from a single shard.
#[derive(Debug, Clone)]
pub struct ShardResponse {
    /// Shard identifier.
    pub shard_id: u32,

    /// Raw JSON response from the node.
    pub body: Value,

    /// Whether this shard succeeded.
    pub success: bool,
}

/// Merged search result.
#[derive(Debug, Clone)]
pub struct MergedResult {
    /// Merged hits (globally sorted, offset/limit applied).
    pub hits: Vec<Value>,

    /// Aggregated facets.
    pub facets: Value,

    /// Estimated total hits (sum of shard totals).
    pub total_hits: u64,

    /// Processing time in milliseconds.
    pub processing_time_ms: u64,

    /// Whether the response is degraded (some shards failed).
    pub degraded: bool,
}

/// Default stub implementation of Merger.
#[derive(Debug, Clone, Default)]
pub struct StubMerger;

impl Merger for StubMerger {
    fn merge(
        &self,
        _shard_responses: Vec<ShardResponse>,
        _offset: usize,
        _limit: usize,
        _client_requested_score: bool,
    ) -> Result<MergedResult> {
        Ok(MergedResult {
            hits: Vec::new(),
            facets: serde_json::json!({}),
            total_hits: 0,
            processing_time_ms: 0,
            degraded: false,
        })
    }
}
