# `miroir-ctl task`

## Purpose
Monitor and manage background tasks (resharding, rebalancing, verification, etc.).

## Preconditions
- Admin API key configured

## Examples

```bash
# List all background tasks
miroir-ctl task list

# Show status of a specific task
miroir-ctl task status --id abc123

# Show status of all tasks (no ID filter)
miroir-ctl task status

# Cancel a running task
miroir-ctl task cancel --id abc123
```

## Gotchas
- **Not yet implemented** — see bead miroir-qon for tracking
- Task IDs are returned by async commands (`node add`, `reshard start`, etc.)
- Cancellation is best-effort — some tasks complete their current unit before stopping
- Tasks that fail are retried automatically — check `task status` for retry count

## See also
- Plan §13 — task management across operations
- `miroir-ctl rebalance status` — shortcut for rebalancing tasks
- `miroir-ctl reshard status` — shortcut for resharding tasks
