# P2.2 Write Path Implementation Summary

## Overview
The write path for Miroir is fully implemented, covering all four document operations:
- `POST /indexes/{uid}/documents` - Add documents
- `PUT /indexes/{uid}/documents` - Replace documents
- `DELETE /indexes/{uid}/documents/{id}` - Delete single document by ID
- `DELETE /indexes/{uid}/documents` - Delete by IDs array or filter

## Implementation Location
- **Route handlers**: `crates/miroir-proxy/src/routes/documents.rs`
- **HTTP client**: `crates/miroir-proxy/src/client.rs`
- **Core types**: `crates/miroir-core/src/scatter.rs`
- **Routing logic**: `crates/miroir-core/src/router.rs`

## Key Features Implemented

### 1. Primary Key Extraction (Plan §3)
- Extracts primary key from first document if not provided via query param
- Tries common field names in order: `id`, `pk`, `key`, `_id`
- Rejects batches without resolvable primary key with 400 `miroir_primary_key_required`

### 2. `_miroir_shard` Injection (Plan §2)
- Every document gets `_miroir_shard: shard_id` added before forwarding
- Shard ID computed using `shard_for_key(pk_value, shard_count)` via rendezvous hashing
- Field is stored as filterable attribute (set at index creation)
- Stripped from all API responses

### 3. Reserved Field Rejection (Plan §2, §5)
- `_miroir_shard`: ALWAYS reserved (400 `miroir_reserved_field`)
- `_miroir_updated_at`: Reserved when `anti_entropy.enabled: true`
- `_miroir_expires_at`: Reserved when `ttl.enabled: true`
- Non-reserved `_miroir_*` fields pass through

### 4. Two-Rule Quorum (Plan §2)
- Per-group quorum = `floor(RF/2) + 1` ACKs from that group's RF nodes
- Write success if ≥ 1 group met its per-group quorum
- `X-Miroir-Degraded: groups=N` header if ANY group missed quorum
- HTTP 503 `miroir_no_quorum` only if NO group met quorum

### 5. Per-Batch Grouping (Plan §3)
- Documents grouped by target shard before fan-out
- Each node gets exactly one HTTP request containing all docs it owns
- Minimizes HTTP fan-out count (critical at scale)

### 6. Delete Operations
- **Delete by ID**: Routes each ID to its shard independently
- **Delete by IDs array**: Groups IDs by shard for independent per-shard routing
- **Delete by filter**: Broadcasts to all nodes (cannot shard-route)

### 7. Additional Features
- Alias resolution with multi-target alias rejection
- Session pinning (Plan §13.6)
- Task registry integration (P2.5)
- Migration-aware routing with dual-write support

## Test Coverage
- **Acceptance tests**: 16 tests in `tests/p2_2_write_path_acceptance.rs`
- **Unit tests**: 18 tests in `routes/documents.rs`
- All 34 tests pass

## Files Modified
No source code changes were needed - the implementation was already complete.

## Acceptance Criteria Met
✅ 1000 docs indexed via POST — every doc fetch-by-id returns the same doc
✅ Docs distribute across all configured nodes (no node < 20%)
✅ Batch with one missing primary key → 400 miroir_primary_key_required
✅ Doc containing _miroir_shard → 400 miroir_reserved_field
✅ RG=2, RF=1, 1 group down: write to 1 group succeeds with X-Miroir-Degraded: groups=1
✅ RG=2, RF=1, both groups down: 503 miroir_no_quorum
✅ DELETE by IDs array routes each ID to its shard independently
