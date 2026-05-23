# P2.3 Search Read Path Implementation Summary

## Overview
The search read path with scatter-gather + merge + group selection is fully implemented and tested.

## Implementation Components

### 1. Group Selection (`query_group`)
- **Location**: `crates/miroir-core/src/router.rs:98-103`
- **Logic**: `query_seq % RG` (round-robin through replica groups)
- **Status**: ✅ Implemented

### 2. Intra-group Covering Set (`covering_set`)
- **Location**: `crates/miroir-core/src/router.rs:105-116`
- **Logic**: Uses rendezvous hash to select RF replicas per shard
- **Rotation**: Rotates through replicas using `query_seq`
- **Status**: ✅ Implemented

### 3. Scatter Planning (`plan_search_scatter`)
- **Location**: `crates/miroir-core/src/scatter.rs:387-431`
- **Logic**: Builds `ScatterPlan` mapping shards to nodes
- **Features**:
  - Supports replica_selector for adaptive selection
  - Version floor filtering (plan §13.5)
  - Session pinning support (plan §13.6)
- **Status**: ✅ Implemented

### 4. Execute Scatter (`execute_scatter`)
- **Location**: `crates/miroir-core/src/scatter.rs:598-742`
- **Logic**: Fans out search requests to all nodes in covering set
- **Features**:
  - Parallel requests with `join_all`
  - Unavailable shard policies: Partial, Error, Fallback
  - Group fallback on failure (plan §2)
- **Status**: ✅ Implemented

### 5. Request Transformation (`SearchRequest::to_node_body`)
- **Location**: `crates/miroir-core/src/scatter.rs:341-376`
- **Logic**: Injects `showRankingScore: true` unconditionally
- **Pagination**: Sets `limit = offset + limit`, `offset = 0`
- **Status**: ✅ Implemented

### 6. Merge (`scatter_gather_search`, `merge`)
- **Location**: `crates/miroir-core/src/scatter.rs:745-785`, `crates/miroir-core/src/merger.rs`
- **Strategies**:
  - RRF (Reciprocal Rank Fusion) - default
  - Score-based (for OP#4 global-IDF)
- **Features**:
  - Deduplication by primary key
  - Facet aggregation (sum across shards)
  - `estimatedTotalHits` sum
  - `processingTimeMs` max
- **Status**: ✅ Implemented

### 7. HTTP Endpoint (`search_handler`)
- **Location**: `crates/miroir-proxy/src/routes/search.rs:163-686`
- **Route**: `POST /search/:index`
- **Features**:
  - DFS query-then-fetch (OP#4 global-IDF preflight)
  - Session pinning (plan §13.6)
  - Query coalescing (plan §13.10)
  - X-Miroir-Degraded header
  - Settings version headers
- **Status**: ✅ Implemented

### 8. HTTP Client (`HttpClient::search_node`)
- **Location**: `crates/miroir-proxy/src/client.rs:64-136`
- **Logic**: POST to `/indexes/{uid}/search` on Meilisearch nodes
- **Features**:
  - Master key authentication
  - Timeout handling
  - Error handling
- **Status**: ✅ Implemented

## Acceptance Tests (10/10 Passed)

- ✅ Unique-keyword search returns exactly 1 hit (deduplication)
- ✅ Facet counts sum correctly across shards
- ✅ Paging: 5 pages of 10 = single limit=50 order, no dupes/gaps
- ✅ Node down with RF=2: search still covers all shards
- ✅ Group down: search uses fallback, not degraded
- ✅ X-Miroir-Degraded header includes actual shard IDs
- ✅ Full integration test (hits, facets, metadata)
- ✅ showRankingScore injected unconditionally
- ✅ limit is offset + limit for coordinator pagination
- ✅ Degraded header format

## Integration with Phase 5 Features

The scatter plan cleanly separates routing decisions for future Phase 5 features:
- **Hedging** (§13.2): `ScatterPlan.hedging_eligible` flag
- **Adaptive replica selection** (§13.3): `replica_selector` parameter
- **Query coalescing** (§13.10): Integrated in search_handler
