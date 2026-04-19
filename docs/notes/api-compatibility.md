# API Compatibility

## Goal

Miroir must be a **drop-in replacement** for a single Meilisearch instance. A client configured to talk to a Meilisearch host should work identically when pointed at Miroir instead — no SDK changes, no query changes, no schema changes.

## What This Means

- Miroir exposes the **Meilisearch REST API verbatim** on its inbound interface
- All request and response shapes, status codes, error formats, and headers must match the Meilisearch spec exactly
- The `X-Meili-API-Key` / `Authorization` header must be accepted and forwarded (or validated at the orchestrator boundary)
- Async task responses (`taskUid`, `GET /tasks/{uid}`) must work — the orchestrator must track and reconcile task states across nodes

## The Core Value Proposition

A single Meilisearch instance is limited by the RAM of one server. Miroir removes that ceiling:

- The logical index can be arbitrarily large — sharded across N nodes, each holding a fraction
- Replication factor is tunable: RF=1 for maximum capacity, RF=2+ for resilience and read throughput
- Adding nodes expands capacity; removing nodes contracts it — the rebalancer handles redistribution
- From the client's perspective, none of this is visible

## API Surface Categories

**Pass-through (broadcast to all nodes):**
- Index create/update/delete
- Settings (ranking rules, synonyms, stop words, filterable/sortable attributes, etc.)
- Index stats (`GET /indexes/{uid}/stats`) — aggregate across nodes

**Shard-routed writes:**
- `POST /indexes/{uid}/documents` — route each document by hash(primary key) to RF nodes
- `PUT /indexes/{uid}/documents` — same
- `DELETE /indexes/{uid}/documents/{id}` — route to owning shard's RF nodes
- `DELETE /indexes/{uid}/documents` by filter — must broadcast to all nodes

**Scatter-gather reads:**
- `POST /indexes/{uid}/search` — fan out to one replica per shard, merge results
- `GET /indexes/{uid}/documents` — fan out, merge, paginate globally
- `GET /indexes/{uid}/documents/{id}` — route to any replica of owning shard

**Orchestrator-local:**
- `GET /health` — orchestrator health (not proxied)
- `GET /version` — return Meilisearch version of the backing nodes
- `GET /tasks`, `GET /tasks/{uid}` — orchestrator maintains a unified task registry

## Task ID Reconciliation

Meilisearch returns a `taskUid` for every write operation. Nodes have independent task ID sequences. Miroir must:

1. Issue writes to RF nodes
2. Collect the per-node `taskUid` for each operation
3. Generate a **Miroir task ID** that maps to the set of per-node task IDs
4. Return the Miroir task ID to the client
5. When polled (`GET /tasks/{uid}`), aggregate node task statuses — report `succeeded` only when all RF nodes report `succeeded`

## Non-Goals

- Miroir does not expose any Miroir-specific management API on the Meilisearch port — topology management (add/remove nodes, rebalance) is handled out-of-band via config or a separate admin interface
- Miroir does not need to implement Meilisearch's tenant tokens or fine-grained key management — it can forward the master key to nodes and let them validate
