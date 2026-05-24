# Phase 2 (miroir-9dj): Proxy + API Surface — Implementation Summary

## Completed Implementation

### HTTP Server (crates/miroir-proxy/src/main.rs)
- Axum server listening on port 7700 (main API)
- Metrics server on port 9090 (Prometheus format)
- Proper shutdown signal handling

### Write Path (crates/miroir-proxy/src/routes/documents.rs, write.rs)
- Hash primary key to get shard ID using `shard_for_key()`
- Inject `_miroir_shard` field into all documents
- Fan out writes to RG × RF nodes
- Per-group quorum tracking (`floor(RF/2)+1`)
- `X-Miroir-Degraded` header on any group missing quorum
- 503 `miroir_no_quorum` only when no group met quorum
- Document add, update, delete by batch, delete by ID

### Read Path (crates/miroir-proxy/src/routes/search.rs, search_handler.rs)
- Pick group via `query_seq % RG`
- Build intra-group covering set
- Scatter search to covering set nodes
- Merge by `_rankingScore`
- Strip `_miroir_shard` always + `_rankingScore` if client didn't request
- Aggregate facets + estimatedTotalHits
- Report max processingTimeMs
- Group-fallback when covering set has holes

### Index Lifecycle (crates/miroir-proxy/src/routes/indexes.rs)
- Create broadcasts to all nodes
- Atomically inject `_miroir_shard` into `filterableAttributes`
- Settings sequential apply-with-rollback
- Delete broadcasts to all nodes
- Stats aggregate `numberOfDocuments` + merge `fieldDistribution`

### Tasks (crates/miroir-proxy/src/routes/tasks.rs)
- Per-task ID reconciliation across nodes
- Aggregated status from all nodes
- Task deletion support
- GET /tasks, GET /tasks/{uid}, DELETE /tasks/{uid}

### Error Response (crates/miroir-proxy/src/error_response.rs)
- Every error matches Meilisearch `{message,code,type,link}` shape
- New `miroir_*` codes per plan §5

### Auth (crates/miroir-proxy/src/auth.rs)
- Master-key/admin-key bearer dispatch per plan §5 rules 2-5
- JWT path stubbed (Phase 5)

### Admin Endpoints (crates/miroir-proxy/src/routes/admin.rs)
- /health + /version + /_miroir/ready
- /_miroir/topology + /_miroir/shards
- /_miroir/metrics (admin-key gated mirror of port 9090 /metrics)

### Middleware (crates/miroir-proxy/src/middleware.rs)
- Structured JSON log per plan §10
- Prometheus metrics (`miroir_request_duration_seconds`, etc.)
- Request tracking with degraded detection

### Core Primitives (Phase 1)
- Router with Rendezvous hash (crates/miroir-core/src/router.rs)
- Merger for result aggregation (crates/miroir-core/src/merger.rs)
- Scatter orchestration (crates/miroir-core/src/scatter.rs)
- Topology management (crates/miroir-core/src/topology.rs)

## Integration Tests

Integration tests written in `crates/miroir-proxy/tests/phase2_integration_test.rs`:
- test_1000_documents_indexed_retrievable_by_id
- test_unique_keyword_search_finds_all_docs_once
- test_facet_aggregation_sums_correctly
- test_offset_limit_paging_preserves_global_ordering
- test_write_with_degraded_group_succeeds_with_header
- test_topology_endpoint_shape
- test_error_format_parity
- test_index_stats_aggregation

Note: Tests marked with `#[ignore]` as they require running Meilisearch nodes.

## Out of Scope (Moved to Later Phases)

- Two-phase settings broadcast (→ Phase 5 / §13.5)
- Persistent task store (→ Phase 3)
- Rebalancer (→ Phase 4)
- Any §13 feature (→ Phase 5)
- Multi-replica coordination / Redis / HPA (→ Phase 6)

## Build Artifacts

Release binaries built successfully:
- `target/release/libmiroir_core.rlib`
- `target/release/libmiroir_proxy.rlib`
- `target/release/libmiroir_ctl.rlib`
- `target/release/miroir-ctl`
