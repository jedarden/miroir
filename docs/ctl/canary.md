# `miroir-ctl canary`

## Purpose
Control canary deployments for rolling updates and validation.

## Preconditions
- Admin API key configured
- Canary feature enabled in config

## Examples

```bash
# Start a canary deployment (10% of traffic to new version)
miroir-ctl canary start --version v2.0.0 --percentage 10

# Increase canary traffic
miroir-ctl canary set --percentage 25

# Check canary status
miroir-ctl canary status

# Promote canary to 100% (complete rollout)
miroir-ctl canary promote

# Abort canary and rollback
miroir-ctl canary abort
```

## Gotchas
- **Not yet implemented** — see tracking bead for details
- Canary splits traffic at the orchestrator level — not per-node
- Metrics are compared between canary and baseline — automatic abort on regression
- Use `miroir-ctl status` to monitor during canary
- Aborting restores previous routing immediately

## See also
- Plan §13.18 — canary deployment architecture
- Plan §10 — metrics and observability for canary validation
