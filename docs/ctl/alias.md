# `miroir-ctl alias`

## Purpose
Manage index aliases for zero-downtime index swaps (e.g., for resharding or blue-green deployments).

## Preconditions
- Source and target indexes must exist
- Admin API key configured

## Examples

```bash
# Create an alias pointing to an index
miroir-ctl alias create --name prod --target myindex_v1

# List all aliases
miroir-ctl alias list

# Update an alias to point to a new index (zero-downtime swap)
miroir-ctl alias update --name prod --target myindex_v2

# Delete an alias
miroir-ctl alias delete --name prod
```

## Gotchas
- **Not yet fully implemented** — see bead miroir-qon for tracking
- Aliases are resolved at query time — no data is copied
- Use aliases for A/B testing: same queries hit different indexes
- Resharding uses aliases internally for the final swap (see `miroir-ctl reshard`)
- Deleting an alias does not delete the underlying index

## See also
- Plan §13.7 — alias management and atomic swaps
- Plan §13.1 — resharding alias swap phase
- `miroir-ctl reshard` — uses aliases for zero-downtime migration
