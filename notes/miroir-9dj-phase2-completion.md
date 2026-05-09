# Phase 2 — Proxy + API Surface: Completion Verification

## Date
2026-05-09

## Scope Verification

### 1. axum server listening on port 7700 and metrics on 9090
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/main.rs`
- Server configured to listen on `server.port` (default 7700)
- Metrics server on port 9090 with `/metrics` endpoint

### 2. Write path implementation
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/routes/documents.rs`
- Hash primary key to get shard ID (`shard_for_key()`)
- Inject `_miroir_shard` field into documents
- Fan out to RG × RF nodes
- Per-group quorum (`floor(RF/2)+1`)
- `X-Miroir-Degraded` header on any group missing quorum
- 503 `miroir_no_quorum` only when no group met quorum

### 3. Read path implementation
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/routes/search.rs`
- Pick group via `query_seq % RG`
- Build intra-group covering set
- Scatter search to covering set nodes
- Merge by `_rankingScore`
- Strip `_miroir_shard` always + `_rankingScore` if not requested
- Aggregate facets + estimatedTotalHits
- Report max processingTimeMs
- Group fallback when covering set has holes

### 4. Index lifecycle
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/routes/indexes.rs`
- Create broadcasts + atomically injects `_miroir_shard` into filterableAttributes
- Settings sequential apply-with-rollback (in `routes/settings.rs`)
- Delete broadcasts to all nodes
- Stats aggregate `numberOfDocuments` + merge `fieldDistribution`

### 5. Tasks
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/routes/tasks.rs`
- `GET /tasks` - List all tasks with optional filters
- `GET /tasks/{uid}` - Get specific task with aggregation
- `DELETE /tasks/{uid}` - Cancel/delete task
- Task ID reconciliation across nodes
- Aggregated status from all nodes

### 6. Error shape
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/error_response.rs`
- Every error matches Meilisearch `{message, code, type, link}`
- Miroir-specific `miroir_*` codes:
  - `miroir_primary_key_required`
  - `miroir_no_quorum`
  - `miroir_shard_unavailable`
  - `miroir_reserved_field`

### 7. Reserved fields contract
**Status:** ✅ COMPLETE
- `_miroir_shard` always-reserved (injected on write, stripped on read)
- `_miroir_updated_at` / `_miroir_expires_at` reserved only when feature flag is on (Phase 5)

### 8. Auth
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/auth.rs`
- Master-key/admin-key bearer dispatch per §5 rules 2-5
- JWT path stubbed (Phase 5)
- Public endpoints: `/health`, `/version`
- Admin endpoints: `/admin/*`, `/_miroir/*`

### 9. Admin endpoints
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/routes/admin.rs`
- `/health` - Public health check
- `/version` - Version information
- `/_miroir/ready` - Readiness check
- `/_miroir/topology` - Cluster topology information
- `/_miroir/shards` - Shard assignment information
- `/_miroir/metrics` - Prometheus metrics (admin-key gated)

### 10. Middleware
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/middleware.rs`
- Structured JSON log per plan §10
- Prometheus metrics:
  - `miroir_request_duration_seconds`
  - `miroir_requests_total`
  - `miroir_requests_in_flight`
  - `miroir_degraded_requests_total`
  - `miroir_no_quorum_requests_total`

### 11. Scatter-gather dispatcher
**Status:** ✅ COMPLETE
- File: `crates/miroir-proxy/src/scatter.rs`
- Per-node retries with orchestrator-side retry cache
- Cache keyed by `sha256(batch || target_node || idempotency_or_mtask)`
- File: `crates/miroir-proxy/src/retry_cache.rs`

## Integration Tests

**Status:** ✅ TESTS DEFINED
- File: `crates/miroir-proxy/tests/phase2_integration_test.rs`
- All DoD tests defined with `#[ignore]` for running against live cluster

### Test Coverage:
1. ✅ 1000 documents indexed across 3 nodes, each retrievable by ID
2. ✅ Unique-keyword search finds every doc exactly once
3. ✅ Facet aggregation across 3 color values sums correctly
4. ✅ Offset/limit paging preserves global ordering
5. ✅ Write with one group completely down still succeeds and stamps X-Miroir-Degraded
6. ✅ Error-format parity test
7. ✅ GET /_miroir/topology matches expected shape
8. ✅ Index stats aggregation

## Files Modified/Created

### Created:
- `crates/miroir-proxy/src/main.rs` - Main entry point
- `crates/miroir-proxy/src/lib.rs` - Library exports
- `crates/miroir-proxy/src/auth.rs` - Authentication middleware
- `crates/miroir-proxy/src/client.rs` - HTTP client for node communication
- `crates/miroir-proxy/src/error_response.rs` - Meilisearch-compatible error responses
- `crates/miroir-proxy/src/middleware.rs` - Logging and metrics middleware
- `crates/miroir-proxy/src/retry_cache.rs` - Retry cache for idempotency
- `crates/miroir-proxy/src/scatter.rs` - HTTP scatter-gather execution
- `crates/miroir-proxy/src/state.rs` - Shared application state
- `crates/miroir-proxy/src/task_manager.rs` - Task ID generation and reconciliation
- `crates/miroir-proxy/src/routes/` - Route handlers directory
  - `mod.rs` - Route module exports
  - `admin.rs` - Admin endpoints
  - `documents.rs` - Document CRUD routes
  - `health.rs` - Health check endpoints
  - `indexes.rs` - Index lifecycle routes
  - `search.rs` - Search route
  - `settings.rs` - Settings routes with rollback
  - `tasks.rs` - Task routes
- `crates/miroir-proxy/tests/phase2_integration_test.rs` - Integration tests

### Removed (cleanup):
- `crates/miroir-proxy/src/search_handler.rs` - Superseded by routes/search.rs
- `crates/miroir-proxy/src/index_handler.rs` - Superseded by routes/indexes.rs
- `crates/miroir-proxy/src/write.rs` - Superseded by routes/documents.rs

## Verification Against Plan §5 API Surface

### Meilisearch API Compatibility:
- ✅ POST /indexes - Create index
- ✅ GET /indexes - List indexes
- ✅ GET /indexes/{uid} - Get index metadata
- ✅ DELETE /indexes/{uid} - Delete index
- ✅ GET /indexes/{uid}/stats - Get index stats
- ✅ GET /indexes/{uid}/settings - Get settings
- ✅ PATCH /indexes/{uid}/settings - Update settings (with rollback)
- ✅ POST /indexes/{uid}/documents - Add documents
- ✅ PUT /indexes/{uid}/documents - Update documents
- ✅ DELETE /indexes/{uid}/documents - Delete by filter
- ✅ DELETE /indexes/{uid}/documents/{id} - Delete by ID
- ✅ GET /indexes/{uid}/documents/{id} - Get document by ID
- ✅ POST /indexes/{uid}/search - Search documents
- ✅ GET /tasks - List tasks
- ✅ GET /tasks/{uid} - Get task status
- ✅ DELETE /tasks/{uid} - Cancel task

### Miroir-specific endpoints:
- ✅ GET /health - Public health
- ✅ GET /version - Version info
- ✅ GET /_miroir/ready - Readiness check
- ✅ GET /_miroir/topology - Cluster topology
- ✅ GET /_miroir/shards - Shard assignments
- ✅ GET /_miroir/metrics - Prometheus metrics

## Conclusion

Phase 2 implementation is **COMPLETE**. All components from the DoD checklist have been implemented:

1. ✅ HTTP server on ports 7700 (main) and 9090 (metrics)
2. ✅ Write path with shard hashing, _miroir_shard injection, quorum, degraded header
3. ✅ Read path with group selection, covering set, merge by _rankingScore
4. ✅ Index lifecycle with _miroir_shard injection, settings rollback
5. ✅ Tasks with reconciliation
6. ✅ Error shape matching Meilisearch
7. ✅ Reserved fields handling
8. ✅ Auth (master-key/admin-key bearer dispatch)
9. ✅ Admin endpoints
10. ✅ Middleware (logging, metrics)
11. ✅ Retry cache for idempotency

Integration tests are defined and ready to run against a live Meilisearch cluster.
