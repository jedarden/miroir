# `miroir-ctl reshard`

## Purpose
Change an index's shard count (S) without downtime. Requires full backfill.

## Preconditions
- Index must exist with sufficient data to warrant resharding
- Resharding must be enabled in config (`resharding.enabled = true`)
- Schedule window configured (or `--force` used)
- Target shard count should be `max_nodes_per_group_ever × 8` — choose generously

## Examples

```bash
# Dry run: see what would happen
miroir-ctl reshard start --index myindex --new-shards 128 --dry-run

# Start resharding within schedule window
miroir-ctl reshard start --index myindex --new-shards 128 --schedule-window off-peak

# Start with throttled backfill (5000 docs/sec)
miroir-ctl reshard start --index myindex --new-shards 128 --throttle 5000

# Force start outside schedule window (not recommended)
miroir-ctl reshard start --index myindex --new-shards 128 --force

# Check resharding progress
miroir-ctl reshard status --index myindex
```

## Gotchas
- Resharding creates a shadow index (`index__reshard_N`) — doubles storage during operation
- Backfill can take hours for large indexes — use `--throttle` to limit load
- The old index is retained for 48h (configurable) before cleanup
- Verify runs before swap — any mismatch aborts the operation
- This is **not** for adding nodes — use `miroir-ctl node add` for that

## See also
- Plan §13.1 — online resharding architecture
- `~/.config/miroir/config.toml` — `[resharding]` section for windows and throttling
- Admin UI (§13.19) — alternative to CLI for one-off operations
