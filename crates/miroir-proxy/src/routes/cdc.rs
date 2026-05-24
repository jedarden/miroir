//! CDC (Change Data Capture) routes — plan §13.13.
//!
//! Provides the `GET /_miroir/changes` endpoint for querying CDC events
//! via long-polling.

use axum::{
    extract::{FromRef, Query, State},
    http::StatusCode,
    Json,
};
use miroir_core::cdc::CdcManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Query parameters for GET /_miroir/changes.
#[derive(Debug, Deserialize)]
pub struct ChangesQueryParams {
    /// Cursor to start from (exclusive). Returns events with sequence > cursor.
    pub since: Option<u64>,
    /// Index UID to query.
    pub index: String,
    /// Maximum number of events to return (default 100).
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Long-poll timeout in seconds (default 30, 0 = return immediately).
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_limit() -> usize {
    100
}

fn default_timeout() -> u64 {
    30
}

/// Response body for GET /_miroir/changes.
#[derive(Debug, Serialize)]
pub struct ChangesResponse {
    /// CDC events since the cursor.
    pub events: Vec<serde_json::Value>,
    /// Current maximum sequence number for this index.
    pub max_sequence: u64,
}

/// GET /_miroir/changes — CDC event stream (plan §13.13, P5.13.d).
///
/// Query parameters:
/// - `since`: Cursor to start from (exclusive). Defaults to 0.
/// - `index`: Index UID to query (required).
/// - `limit`: Maximum events to return (default 100, max 1000).
/// - `timeout`: Long-poll timeout in seconds (default 30, 0 = return immediately).
///
/// Returns events with sequence > `since`, up to `limit` events.
/// Waits up to `timeout` seconds for new events if none are immediately available.
/// Use the returned `max_sequence` as the next `since` value for pagination.
pub async fn get_changes<S>(
    Query(params): Query<ChangesQueryParams>,
    State(state): State<S>,
) -> Result<Json<ChangesResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    std::sync::Arc<miroir_core::cdc::CdcManager>: FromRef<S>,
{
    // Extract CDC manager from state
    let cdc_manager_arc = std::sync::Arc::<miroir_core::cdc::CdcManager>::from_ref(&state);
    let cdc_manager = cdc_manager_arc.as_ref();

    // Cap limit at 1000 to prevent large responses
    let limit = params.limit.min(1000);

    // Default cursor to 0 if not provided
    let cursor = params.since.unwrap_or(0);

    // Determine timeout: 0 means return immediately (no long-poll)
    let timeout = if params.timeout == 0 {
        None
    } else {
        Some(params.timeout)
    };

    // Get events since cursor with long-poll support
    let events = cdc_manager
        .get_changes_long_poll(&params.index, cursor, limit, timeout)
        .await;

    // Get current max sequence
    let max_sequence = cdc_manager.max_sequence(&params.index).await;

    // Convert events to JSON values
    let events_json: Vec<serde_json::Value> = events
        .into_iter()
        .filter_map(|event| serde_json::to_value(event).ok())
        .collect();

    Ok(Json(ChangesResponse {
        events: events_json,
        max_sequence,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use miroir_core::cdc::{CdcConfig, CdcEvent, CdcOperation};

    #[test]
    fn test_changes_query_params_default_limit() {
        let params: ChangesQueryParams = serde_urlencoded::from_str("index=products").unwrap();
        assert_eq!(params.limit, 100);
        assert_eq!(params.since, None);
        assert_eq!(params.index, "products");
        assert_eq!(params.timeout, 30); // Default timeout
    }

    #[test]
    fn test_changes_query_params_with_values() {
        let params: ChangesQueryParams =
            serde_urlencoded::from_str("index=products&since=100&limit=50&timeout=60").unwrap();
        assert_eq!(params.limit, 50);
        assert_eq!(params.since, Some(100));
        assert_eq!(params.index, "products");
        assert_eq!(params.timeout, 60);
    }

    #[test]
    fn test_changes_query_params_zero_timeout() {
        let params: ChangesQueryParams =
            serde_urlencoded::from_str("index=products&timeout=0").unwrap();
        assert_eq!(params.timeout, 0); // No long-poll
        assert_eq!(params.limit, 100);
    }
}
