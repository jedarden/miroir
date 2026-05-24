# `miroir-ctl node`

## Purpose
Manage cluster topology — add, remove, drain, and list nodes.

## Preconditions
- Target node must be running and reachable from the orchestrator
- Node's Meilisearch instance should be healthy before adding
- For removal: node should be drained first to avoid data loss

## Examples

```bash
# Add a new node to replica group 0
miroir-ctl node add --id node-3 --address http://node-3:7700 --replica-group 0

# List all nodes with status
miroir-ctl node list

# Drain a node before removal (migrates its shards elsewhere)
miroir-ctl node drain node-2

# Remove a node after draining completes
miroir-ctl node remove node-2

# Force remove (dangerous — skips drain, may lose data)
miroir-ctl node remove node-2 --force --yes
```

## Gotchas
- `node add` triggers automatic rebalancing — use `miroir-ctl rebalance status --watch` to track
- `node drain` is async — the command returns immediately but migrations continue in background
- Never use `--force` unless the node is permanently dead — you'll lose RF replicas
- `node list` shows the same info as `miroir-ctl status` but in table format

## See also
- Plan §13.2 — node addition and rebalancing
- Plan §13.3 — node drain and removal
- `miroir-ctl rebalance status` — track migration progress
