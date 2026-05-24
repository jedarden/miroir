# `miroir-ctl rebalance`

## Purpose
Monitor and manage shard migrations triggered by topology changes.

## Preconditions
- Rebalancing runs automatically after `node add` / `node drain` — no manual start needed
- Admin API key for status queries

## Examples

```bash
# Show current rebalance progress
miroir-ctl rebalance status

# Watch migrations in real-time (use during node operations)
miroir-ctl rebalance status --watch
```

## Gotchas
- Rebalancing is automatic — there's no `rebalance start` command
- Migrations are rate-limited to avoid overwhelming nodes — large topology changes may take hours
- Each migration moves one shard from source to destination; a node add triggers ~S/Ng migrations
- Queries continue serving during rebalance — old and new locations are both valid

## See also
- Plan §13.2 — automatic rebalancing on topology change
- Plan §13.3 — drain-triggered rebalancing
- `miroir-ctl status --watch` — cluster-wide health during migrations
