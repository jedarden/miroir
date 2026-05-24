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
}

fn default_limit() -> usize {
    100
}

/// Response body for GET /_miroir/changes.
#[derive(Debug, Serialize)]
pub struct ChangesResponse {
    /// CDC events since the cursor.
    pub events: Vec<serde_json::Value>,
    /// Current maximum sequence number for this index.
    pub max_sequence: u64,
}

/// GET /_miroir/changes — CDC event stream (plan §13.13).
///
/// Query parameters:
/// - `since`: Cursor to start from (exclusive). Defaults to 0.
/// - `index`: Index UID to query (required).
/// - `limit`: Maximum events to return (default 100, max 1000).
///
/// Returns events with sequence > `since`, up to `limit` events.
/// Use the returned `max_sequence` as the next `since` value for pagination.
pub async fn get_changes<S>(
    Query(params): Query<ChangesQueryParams>,
    State(state): State<S>,
) -> Result<Json<ChangesResponse>, StatusCode>
where
    S: Clone + Send + Sync + 'static,
    miroir_core::cdc::CdcManager: FromRef<S>,
{
    // Extract CDC manager from state
    let cdc_manager = miroir_core::cdc::CdcManager::from_ref(&state);

    // Cap limit at 1000 to prevent large responses
    let limit = params.limit.min(1000);

    // Default cursor to 0 if not provided
    let cursor = params.since.unwrap_or(0);

    // Get events since cursor
    let events = cdc_manager.get_changes(&params.index, cursor, limit).await;

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
    }

    #[test]
    fn test_changes_query_params_with_values() {
        let params: ChangesQueryParams =
            serde_urlencoded::from_str("index=products&since=100&limit=50").unwrap();
        assert_eq!(params.limit, 50);
        assert_eq!(params.since, Some(100));
        assert_eq!(params.index, "products");
    }
}
