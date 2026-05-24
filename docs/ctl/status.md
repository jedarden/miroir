# `miroir-ctl status`

## Purpose
Show cluster health, node status, and ongoing rebalancing operations.

## Preconditions
- Admin API key configured (`MIROIR_ADMIN_API_KEY` env var, `~/.config/miroir/credentials`, or `--admin-key` flag)
- Miroir orchestrator reachable at `--api-url` (default: `http://localhost:8080`)

## Examples

```bash
# One-time status snapshot
miroir-ctl status

# Continuously refresh every 2 seconds (use during operations)
miroir-ctl status --watch
```

## Gotchas
- `--watch` mode clears the terminal each refresh — use `Ctrl+C` to exit
- Degraded nodes show `⚠` but don't necessarily mean downtime — check individual node errors
- `fully_covered: false` means some shards have fewer than RF replicas — check `degraded_node_count`

## See also
- Plan §10 — health metrics and degraded node detection
- `miroir-ctl node list` — detailed node topology
