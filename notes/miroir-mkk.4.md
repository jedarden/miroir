# P4.4 Replica Group Addition: Initializing → Active

## Summary

This bead implements the "Adding a new replica group" flow from plan §2, enabling horizontal query scaling by adding new replica groups without interrupting existing queries.

## Implementation

### Core Components

1. **`GroupAdditionCoordinator`** (`crates/miroir-core/src/group_addition.rs`)
   - State machine for group addition: Initializing → Syncing → SyncComplete → Active
   - Per-shard sync state tracking with round-robin source group selection
   - Progress tracking and timeout handling

2. **`GroupSyncWorker`** (`crates/miroir-core/src/group_sync_worker.rs`)
   - Background worker that copies documents from existing groups to new group
   - Paginated sync using `filter=_miroir_shard={id}`
   - Handles source unavailability gracefully (pauses and resumes)

3. **Admin API** (`crates/miroir-proxy/src/routes/admin_endpoints.rs`)
   - `POST /_miroir/replica_groups` - Add new replica group
   - `GET /_miroir/replica_groups/{id}/status` - Check sync progress
   - `POST /_miroir/replica_groups/{id}/activate` - Mark group as active

### Key Behaviors

- **Query routing**: During sync, queries only route to `Active` groups (not `Initializing`)
- **Write fan-out**: New writes immediately fan out to all groups including the new one
- **Zero read interruption**: Existing groups continue serving queries throughout
- **Round-robin source selection**: Spreads read load during sync across source groups

## Acceptance Tests

All 4 acceptance tests pass:
1. ✓ During sync, query throughput on original group unchanged
2. ✓ After `active`, queries distribute round-robin between groups
3. ✓ Mid-sync writes present on both groups after sync
4. ✓ Failed sync pauses and resumes when source returns

## Files Modified

- `crates/miroir-core/src/group_addition.rs` - New file
- `crates/miroir-core/src/group_sync_worker.rs` - New file
- `crates/miroir-core/tests/p44_replica_group_addition.rs` - New test file
- `crates/miroir-proxy/src/routes/admin_endpoints.rs` - Added group addition endpoints
- `crates/miroir-proxy/src/routes/admin.rs` - Added routes
- `crates/miroir-proxy/src/main.rs` - Added sync worker background task
