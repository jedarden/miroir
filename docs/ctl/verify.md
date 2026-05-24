# `miroir-ctl verify`

## Purpose
Check data integrity across replica groups and detect drift.

## Preconditions
- Admin API key configured
- Cluster healthy (degraded nodes can cause false positives)

## Examples

```bash
# Verify all indexes
miroir-ctl verify

# Verify specific index
miroir-ctl verify --index myindex

# Verbose output (shows per-shard details)
miroir-ctl verify --verbose
```

## Gotchas
- Verification is read-only — it won't fix drift, only report it
- Large indexes take time to verify — progress is not streamed
- Use `miroir-ctl task status` to check async verification jobs
- Drift is automatically repaired by anti-entropy workers — see `miroir-ctl task status`

## See also
- Plan §13.8 — anti-entropy and drift detection
- Plan §10 — observability and metrics
