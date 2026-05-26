# Search UI Analytics Beacon (plan §13.21)

## Overview

The Search UI Analytics Beacon is an HTTP endpoint that collects end-user interaction data from the embedded search UI. Beacon events are published to the CDC (Change Data Capture) system for downstream analytics processing.

## Beacon Endpoint

**URL:** `POST /_miroir/ui/search/{index}/beacon`

**Authentication:** JWT session token (from `/ui/search/{index}/session`)

**Request Body:** JSON `BeaconRequest`

**Response:** `202 Accepted` (event processed) or `403 Forbidden` (invalid session)

## Beacon Request Schema

```json
{
  "event_id": "string",           // Client-generated unique identifier for idempotency
  "event_type": "string",         // Event type: "click", "latency", "impression"
  "index_uid": "string",          // Index UID
  "query": "string (optional)",   // Query string for search/latency events
  "result_count": "number (optional)", // Number of results for search events
  "latency_ms": "number (optional)",   // Latency in milliseconds
  "document_id": "string (optional)",  // Primary key of clicked document (for click events)
  "position": "number (optional)"      // Click position in results (for click events)
}
```

### Event Types

| Event Type | Description | Required Fields | Optional Fields |
|------------|-------------|-----------------|-----------------|
| `click` | User clicked a search result | `document_id`, `position` | `query`, `result_count` |
| `latency` | Search latency measurement | `latency_ms` | `query`, `result_count` |
| `impression` | Search results impression | `result_count` | `query` |

## Idempotency

Beacon events are idempotent based on the `event_id` field:

1. The orchestrator checks if `event_id` was already processed (Redis-backed)
2. Duplicate events are rejected with `202 Accepted` (no-op)
3. Each unique `event_id` is processed exactly once

For clients that cannot generate `event_id`, the orchestrator generates a deterministic hash from:
- Session ID
- Query string
- Document ID (if present)
- Position (if present)
- Minute-bucketed latency (for latency events)

## CDC Integration

### Publishing to CDC

When `search_ui.analytics.enabled: true`, beacon events are published to the CDC internal queue:

1. Beacon event received at `/ui/search/{index}/beacon`
2. Event converted to `AnalyticsEvent` with `event_type` mapped:
   - `"click"` → `"click_through"` in CDC
   - `"latency"` → `"latency"` in CDC
   - `"impression"` → `"impression"` in CDC
3. Event stored in CDC internal queue with monotonically increasing sequence number
4. Event available via `GET /_miroir/changes?since={cursor}&index={uid}`

### CDC Event Schema

Analytics events in the CDC stream have this structure:

```json
{
  "mtask_id": "analytics:{event_id}",      // Prefixed with "analytics:"
  "index": "products",                     // Index UID
  "operation": "ClickThrough",             // CdcOperation enum
  "primary_keys": ["doc-123"],             // Clicked document ID
  "shard_ids": [],                         // Empty for analytics events
  "settings_version": 0,                   // Not applicable
  "timestamp": 1234567890,                 // UNIX timestamp (ms)
  "document": {                            // Original analytics event
    "event_type": "click_through",
    "event_id": "evt-abc-123",
    "session_id": "session-xyz-789",
    "index": "products",
    "query": "laptop",
    "result_id": "doc-123",
    "result_position": 3,
    "latency_ms": null,
    "timestamp": 1234567890
  },
  "origin": null,                          // No origin for user events
  "event_id": "evt-abc-123"                // Stable deduplication key
}
```

### CDC Operation Types

| Operation | Description | When Emitted |
|-----------|-------------|--------------|
| `ClickThrough` | User clicked a search result | `event_type: "click"` |
| `Latency` | Search latency measurement | `event_type: "latency"` |
| `Add` | Document added | Document write |
| `Update` | Document updated | Document write |
| `Delete` | Document deleted | Document delete |

### CDC Configuration

Analytics events respect the `cdc.emit_internal_writes` configuration:

- **Click events** (`click_through`): Always emitted (user-initiated)
- **Latency events** (`latency`): Subject to `emit_internal_writes` flag

```yaml
cdc:
  enabled: true
  emit_internal_writes: false  # Suppresses latency events if false
  sinks: []
```

## Configuration

### Enable Analytics

```yaml
search_ui:
  enabled: true
  analytics:
    enabled: true              # Enable beacon collection
    sink: "cdc"                # Publish to CDC internal queue
```

### Disable Analytics

```yaml
search_ui:
  enabled: true
  analytics:
    enabled: false             # Disable beacon collection
    sink: "cdc"
```

## Client Integration

### JavaScript Example

```javascript
// After obtaining JWT session token from /ui/search/{index}/session
const token = sessionStorage.getItem('miroir_search_token');

// Send click-through event
fetch(`/_miroir/ui/search/products/beacon`, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`
  },
  body: JSON.stringify({
    event_id: crypto.randomUUID(),
    event_type: 'click',
    index_uid: 'products',
    query: 'laptop',
    document_id: 'prod-123',
    position: 3
  })
});
```

### Latency Measurement

```javascript
// Measure search latency
const searchStart = performance.now();

// Perform search...
const results = await search(query);

const searchEnd = performance.now();
const latencyMs = Math.round(searchEnd - searchStart);

// Send latency beacon
fetch(`/_miroir/ui/search/products/beacon`, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`
  },
  body: JSON.stringify({
    event_id: crypto.randomUUID(),
    event_type: 'latency',
    index_uid: 'products',
    query: query,
    result_count: results.hits.length,
    latency_ms: latencyMs
  })
});
```

## Metrics

The following Prometheus metrics are exposed for beacon events:

- `miroir_search_ui_sessions_total` - Total sessions minted
- `miroir_search_ui_queries_total{index}` - Total search queries
- `miroir_search_ui_zero_hits_total{index}` - Total zero-result queries
- `miroir_search_ui_click_through_total{index}` - Total click events (when analytics enabled)
- `miroir_search_ui_p95_ms{index}` - P95 end-user latency

## Rate Limiting

The beacon endpoint is rate-limited per source IP:

- **Default:** 60 requests per minute
- **Backend:** Redis (distributed) or local (per-pod)
- **Scope:** Per-IP limit, not per-session

Rate limit configuration:

```yaml
search_ui:
  rate_limit:
    per_ip: "60/minute"
    backend: "redis"           # or "local"
    redis_key_prefix: "miroir:ratelimit:searchui:"
    redis_ttl_s: 60
```

## Security Considerations

1. **Authentication Required:** Beacon endpoint requires valid JWT session token
2. **No PII in Events:** Beacon events should not contain personally identifiable information
3. **Idempotency:** Duplicate events are ignored to prevent replay attacks
4. **Origin Validation:** Session token `idx` claim must match request `index_uid`
5. **Scope Validation:** JWT `scope` claim must include `beacon` action

## Troubleshooting

### Events Not Appearing in CDC

1. Check `search_ui.analytics.enabled: true`
2. Verify CDC is enabled: `cdc.enabled: true`
3. Check `cdc.emit_internal_writes` for latency events
4. Verify JWT token includes `beacon` in scope
5. Check browser console for network errors

### Duplicate Events

If duplicate events appear in CDC:
1. Verify `event_id` is unique per event
2. Check Redis idempotency cache is functioning
3. Ensure client is not retrying failed requests

### High Cardinality

To reduce CDC stream cardinality:
1. Avoid including unique user IDs in beacon events
2. Use session IDs instead of user IDs
3. Aggregate high-frequency events before sending

## References

- Plan §13.21: Default search interface (end-user search UI)
- Plan §13.13: CDC (Change Data Capture)
- `crates/miroir-proxy/src/routes/search_ui.rs` - Beacon endpoint implementation
- `crates/miroir-core/src/cdc.rs` - CDC manager and analytics event types
