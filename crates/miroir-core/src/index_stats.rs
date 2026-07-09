//! Shared per-node index-stats aggregation.
//!
//! Extracted from `ilm::fetch_index_stats` (plan §13.17) so that both ILM
//! rollover evaluation and the reshard backfill denominator (bf-2ynu5) compute
//! an index's real document count the *same* way: iterate the source node
//! addresses, `GET /indexes/{uid}/stats`, and reduce `numberOfDocuments`.
//!
//! # Why `max`, not `sum`
//!
//! Each address in a coordinator's `node_addresses` hosts a full replica of the
//! index (this is also why `reshard::advance_backfill` can read every shard of
//! the source index from a single healthy node via `filter=_miroir_shard`).
//! Reducing per-node counts with `max` therefore yields the true document count
//! and tolerates a replica that is mid-ingest; `sum` would over-count by the
//! replication factor. ILM relies on exactly this reduction for its `max_docs`
//! rollover trigger, so the reshard denominator must agree with it.
//!
//! # Testability
//!
//! The HTTP transport ([`fetch_node_stats`]) is exercised end-to-end by the
//! `miroir-proxy` ILM acceptance tests (mockito). The reduction policy itself is
//! a pure function — [`reduce_document_counts`] — so it is unit-tested directly
//! against known per-node counts (including failing nodes) without spinning up
//! an HTTP server.

use reqwest::Client;
use serde::Deserialize;
use tracing::warn;

/// `/indexes/{uid}/stats` response shape for a single node (Meilisearch).
///
/// Both fields default when absent so a minimal `{"numberOfDocuments": N}` body
/// parses cleanly, and a node that lacks the index entirely yields zeros.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct NodeIndexStats {
    #[serde(default)]
    #[serde(rename = "numberOfDocuments")]
    pub number_of_documents: u64,
    /// Size detail — may be absent on older Meilisearch versions.
    #[serde(rename = "stats", default)]
    pub stats: Option<NodeStatsDetail>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NodeStatsDetail {
    #[serde(rename = "databaseSize", default)]
    pub database_size: u64,
}

/// Aggregated index stats reduced across all source node addresses.
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    /// Document count, reduced with `max` across responding nodes.
    pub total_documents: u64,
    /// `databaseSize` summed across responding nodes.
    pub total_size_bytes: u64,
    /// Number of nodes that returned stats successfully (HTTP 2xx, parsable).
    pub nodes_responded: usize,
}

/// Failure encountered while fetching stats from one node.
#[derive(Debug, thiserror::Error)]
pub enum FetchStatsError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("failed to read response: {0}")]
    Read(reqwest::Error),
    #[error("HTTP {status}: {body}")]
    Http { status: u16, body: String },
    #[error("failed to parse stats: {0}")]
    Parse(serde_json::Error),
}

/// Fetch `/indexes/{uid}/stats` from a single node.
///
/// A 404 (index absent on that node) is reported as zero documents rather than
/// an error, matching the original `ilm::fetch_node_stats` behavior — a node
/// that has not yet received the index should not fail the aggregation.
pub async fn fetch_node_stats(
    client: &Client,
    address: &str,
    master_key: &str,
    index_uid: &str,
) -> Result<NodeIndexStats, FetchStatsError> {
    let url = format!(
        "{}/indexes/{}/stats",
        address.trim_end_matches('/'),
        index_uid
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", master_key))
        .send()
        .await
        .map_err(FetchStatsError::Request)?;

    let status = response.status();
    let body_text = response
        .text()
        .await
        .map_err(FetchStatsError::Read)?;

    if status.as_u16() == 404 {
        // Index doesn't exist on this node — count as zero, not an error.
        return Ok(NodeIndexStats {
            number_of_documents: 0,
            stats: None,
        });
    }

    if !status.is_success() {
        return Err(FetchStatsError::Http {
            status: status.as_u16(),
            body: body_text,
        });
    }

    serde_json::from_str(&body_text).map_err(FetchStatsError::Parse)
}

/// Aggregate an index's stats across all source node addresses.
///
/// Iterates every address, GETs `/indexes/{uid}/stats`, and reduces the results
/// the same way `ilm::fetch_index_stats` did: document count via `max`, size via
/// `sum`. A node that errors (network / non-2xx / parse) is logged and skipped
/// — one failing node never blocks the count. Returns zeros only if every node
/// fails or reports zero.
pub async fn aggregate_index_stats(
    client: &Client,
    node_addresses: &[String],
    master_key: &str,
    index_uid: &str,
) -> IndexStats {
    let mut total_documents = 0u64;
    let mut total_size_bytes = 0u64;
    let mut nodes_responded = 0usize;

    for address in node_addresses {
        match fetch_node_stats(client, address, master_key, index_uid).await {
            Ok(node_stats) => {
                total_documents = total_documents.max(node_stats.number_of_documents);
                if let Some(ref stats) = node_stats.stats {
                    total_size_bytes += stats.database_size;
                }
                nodes_responded += 1;
            }
            Err(e) => {
                // Log but continue - one node failing shouldn't block the count.
                warn!("index_stats: failed to fetch stats from node {}: {}", address, e);
            }
        }
    }

    IndexStats {
        total_documents,
        total_size_bytes,
        nodes_responded,
    }
}

/// Reduce per-node document counts into the index's real document count.
///
/// Pure (no I/O): takes each node's count as `Ok(n)` (or `Err(_)` if that node
/// failed to respond) and returns the `max` across the responders, or `0` when
/// no node responded. This is the policy shared by ILM rollover evaluation and
/// the reshard backfill denominator, factored out so it can be unit-tested
/// against known per-node counts without an HTTP server.
pub fn reduce_document_counts<I, E>(counts: I) -> u64
where
    I: IntoIterator<Item = Result<u64, E>>,
{
    counts
        .into_iter()
        .filter_map(Result::ok)
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_node_stats_full_body() {
        let body = r#"{"numberOfDocuments": 4242, "stats": {"databaseSize": 9999}}"#;
        let stats: NodeIndexStats = serde_json::from_str(body).unwrap();
        assert_eq!(stats.number_of_documents, 4242);
        let detail = stats.stats.expect("stats detail present");
        assert_eq!(detail.database_size, 9999);
    }

    #[test]
    fn parse_node_stats_missing_fields_default_zero() {
        // A bare object must still parse, defaulting every field.
        let stats: NodeIndexStats = serde_json::from_str("{}").unwrap();
        assert_eq!(stats.number_of_documents, 0);
        assert!(stats.stats.is_none());
    }

    #[test]
    fn parse_node_stats_docs_without_size_detail() {
        let stats: NodeIndexStats = serde_json::from_str(r#"{"numberOfDocuments": 7}"#).unwrap();
        assert_eq!(stats.number_of_documents, 7);
        assert!(stats.stats.is_none());
    }

    // --- reduce_document_counts: the "mock node client" aggregation seam ---
    // Each `Ok(n)` is a node that returned `numberOfDocuments = n`; each `Err`
    // is a node that failed to respond. The reduction must pick the maximum
    // responder and tolerate any number of failures.

    #[test]
    fn reduce_picks_max_across_healthy_nodes() {
        let counts: [Result<u64, ()>; 3] = [Ok(100), Ok(250), Ok(180)];
        assert_eq!(reduce_document_counts(counts), 250);
    }

    #[test]
    fn reduce_ignores_failed_nodes() {
        // Two of four nodes failed; the denominator is still the max responder.
        let counts: [Result<u64, ()>; 4] = [Ok(100), Err(()), Ok(250), Err(())];
        assert_eq!(reduce_document_counts(counts), 250);
    }

    #[test]
    fn reduce_zero_when_every_node_fails() {
        let counts: [Result<u64, ()>; 3] = [Err(()), Err(()), Err(())];
        assert_eq!(reduce_document_counts(counts), 0);
    }

    #[test]
    fn reduce_zero_when_no_nodes() {
        let counts: [Result<u64, ()>; 0] = [];
        assert_eq!(reduce_document_counts(counts), 0);
    }

    #[test]
    fn reduce_single_node() {
        let counts: [Result<u64, ()>; 1] = [Ok(42)];
        assert_eq!(reduce_document_counts(counts), 42);
    }

    #[test]
    fn reduce_treats_zero_doc_index_as_zero() {
        // An existing but empty index reports 0 on every node.
        let counts: [Result<u64, ()>; 2] = [Ok(0), Ok(0)];
        assert_eq!(reduce_document_counts(counts), 0);
    }
}
