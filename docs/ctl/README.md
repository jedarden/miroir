# miroir-ctl Runbooks

This directory contains runbooks for each `miroir-ctl` subcommand. Each runbook includes:

- Purpose (one sentence)
- Preconditions (what must be true before running)
- Examples (common invocations)
- Gotchas (edge cases and warnings)
- See also (plan section references)

## Runbooks

| Command | Runbook | Description |
|---------|---------|-------------|
| `status` | [status.md](status.md) | Show cluster health and node status |
| `node` | [node.md](node.md) | Add, remove, drain, and list nodes |
| `rebalance` | [rebalance.md](rebalance.md) | Monitor shard migrations |
| `reshard` | [reshard.md](reshard.md) | Change index shard count |
| `verify` | [verify.md](verify.md) | Check data integrity across replicas |
| `task` | [task.md](task.md) | Monitor background tasks |
| `dump` | [dump.md](dump.md) | Export and import index data |
| `alias` | [alias.md](alias.md) | Manage index aliases |
| `canary` | [canary.md](canary.md) | Control canary deployments |
| `ttl` | [ttl.md](ttl.md) | Manage document expiration policies |
| `cdc` | [cdc.md](cdc.md) | Configure change data capture |
| `shadow` | [shadow.md](shadow.md) | Manage shadow indexing |
| `ui` | [ui.md](ui.md) | Launch the Admin UI |
| `tenant` | [tenant.md](tenant.md) | Manage multi-tenancy |
| `explain` | [explain.md](explain.md) | Analyze query plans |
| `key` | [key.md](key.md) | Manage API keys |

## Quick Reference

```bash
# Cluster health
miroir-ctl status --watch

# Node operations
miroir-ctl node add --id node-3 --address http://node-3:7700 --replica-group 0
miroir-ctl node drain node-2
miroir-ctl node remove node-2

# Monitor operations
miroir-ctl rebalance status --watch
miroir-ctl task status

# Data operations
miroir-ctl dump export --index myindex --output myindex.dump
miroir-ctl verify --index myindex

# Admin UI
miroir-ctl ui
```

## Authentication

All commands require an admin API key. Set it via:

1. Environment variable: `export MIROIR_ADMIN_API_KEY=...`
2. Credentials file: `~/.config/miroir/credentials` with `[default].admin_api_key`
3. Command line flag: `--admin-key ...` (WARNING: visible in process list)

## See also

- [Plan §11](../plan/plan.md#11-onboarding) — common operations
- [Plan §4](../plan/plan.md#4-implementation) — crate layout
- [Troubleshooting](../troubleshooting.md) — common issues
