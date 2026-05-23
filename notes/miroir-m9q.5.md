# P6.5 Mode C: Work-Queued Chunked Jobs - Verification Summary

## Task Completion Status

P6.5 Mode C work-queued chunked jobs (plan §14.5) is **fully implemented** and all acceptance tests pass.

## Implementation Details (from commits 8b1cf42, cff90a3)

### Core Components
1. **mode_c_coordinator.rs** - Job coordination with:
   - `claim_job()` - atomic compare-and-swap for job claiming
   - `renew_claim()` - heartbeat to extend claim TTL
   - `reclaim_expired_claims()` - release claims from crashed pods
   - `split_job_into_chunks()` - chunk large jobs on input boundaries
   - `queue_depth()` - HPA metric support

2. **mode_c_worker/mod.rs** - Worker loop for processing:
   - Poll for queued jobs and claim them
   - Heartbeat to renew claims every 10s
   - Process dump import chunks (NDJSON line boundaries)
   - Process reshard backfill chunks (shard-id ranges)
   - Handle idempotent resume from `last_cursor`

3. **dump_chunking.rs** - Split NDJSON dumps on line boundaries (256 MiB default)

4. **reshard_chunking.rs** - Split reshard backfill by shard-id ranges

### Database Schema
Migration 005_jobs_chunking.sql adds:
- `parent_job_id` - Link chunks to parent job
- `chunk_index` - Chunk position (0-based)
- `total_chunks` - Total number of chunks
- `created_at` - Job creation timestamp
- Indexes for efficient queries

### Acceptance Tests (22 tests pass)
- ✅ 1 GB dump splits into 4× 256 MiB chunks
- ✅ 3 pods claim chunks in parallel
- ✅ Claim expires in 30s; another pod resumes at last_cursor
- ✅ HPA queue depth metric drives scaling
- ✅ Two concurrent dumps interleave without starvation
- ✅ Reshard backfill splits by shard-id range
- ✅ Heartbeat renews claim; missed heartbeat expires

## Configuration

```yaml
dump_import:
  chunk_size_bytes: 268435456  # 256 MiB per §14.5 Mode C chunk-parallel coordinator
```

## HPA Integration

Queue depth metric: `miroir_background_queue_depth` (Prometheus GaugeVec with `job_type` label)

```yaml
# Example HPA configuration
metrics:
- type: External
  external:
    metric:
      name: miroir_background_queue_depth
    target:
      type: AverageValue
      averageValue: 10
```

## Verified

- All 22 Mode C acceptance tests pass
- Jobs table with states: `queued | in_progress | completed | failed`
- Claim TTL: 30s default, heartbeat every 10s
- Chunking on input boundaries (NDJSON lines for dump, shard-id for reshard)
- Per-chunk progress for idempotent resume
- Queue depth metric for HPA scaling
